package util

import (
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/ordishs/go-utils/expiringmap"
)

type ExpiringConcurrentCache[K comparable, V any] struct {
	mu        sync.RWMutex
	cache     *expiringmap.ExpiringMap[K, V]
	wg        map[K]*sync.WaitGroup
	ZeroValue V
}

func NewExpiringConcurrentCache[K comparable, V any](expiration time.Duration) *ExpiringConcurrentCache[K, V] {
	return &ExpiringConcurrentCache[K, V]{
		cache: expiringmap.New[K, V](expiration),
		wg:    make(map[K]*sync.WaitGroup),
	}
}

func (c *ExpiringConcurrentCache[K, V]) GetOrSet(key K, fetchFunc func() (V, error)) (V, error) {
	var (
		val   V
		found bool
		err   error
		wg    *sync.WaitGroup
	)

	// Start by acquiring a read lock
	c.mu.RLock()

	// Check if the value is already in the cache
	if val, found = c.cache.Get(key); found {
		c.mu.RUnlock()
		return val, nil
	}

	// Upgrade to a write lock if the value is not found
	c.mu.RUnlock()
	c.mu.Lock()

	// Check again to avoid race conditions
	if val, found = c.cache.Get(key); found {
		c.mu.Unlock()
		return val, nil
	}

	// If not, check if there is an ongoing request
	if wg, found = c.wg[key]; found {
		c.mu.Unlock()
		wg.Wait() // Wait for the other goroutine to finish

		if val, found = c.cache.Get(key); found {
			return val, nil
		}

		return c.ZeroValue, errors.NewProcessingError("cache: failed to get value after waiting")
	}

	// Create a new WaitGroup for the key
	wg = &sync.WaitGroup{}
	wg.Add(1)
	c.wg[key] = wg

	// Release the global lock, for others to wait on the wait group
	c.mu.Unlock()

	// Perform the fetch, with a lock on the cache
	c.mu.Lock()

	defer func() {
		wg.Done()         // Mark the wait group as done
		delete(c.wg, key) // Remove it from the map

		c.mu.Unlock()
	}()

	// Perform the fetch
	result, err := fetchFunc()
	if err != nil {
		return c.ZeroValue, err
	}

	// Cache the result
	c.cache.Set(key, result)

	return result, nil
}
