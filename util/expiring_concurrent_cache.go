package util

import (
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/ordishs/go-utils/expiringmap"
)

type expiringConcurrentCacheWait[V any] struct {
	wg     *sync.WaitGroup
	result *V
}

type ExpiringConcurrentCache[K comparable, V any] struct {
	mu        sync.RWMutex
	cache     *expiringmap.ExpiringMap[K, V]
	wg        map[K]*expiringConcurrentCacheWait[V]
	ZeroValue V
}

func NewExpiringConcurrentCache[K comparable, V any](expiration time.Duration) *ExpiringConcurrentCache[K, V] {
	return &ExpiringConcurrentCache[K, V]{
		cache: expiringmap.New[K, V](expiration),
		wg:    make(map[K]*expiringConcurrentCacheWait[V]),
	}
}

func (c *ExpiringConcurrentCache[K, V]) GetOrSet(key K, fetchFunc func() (V, bool, error)) (V, error) {
	var (
		val        V
		found      bool
		allowCache bool
		err        error
		wg         *sync.WaitGroup
		wgw        *expiringConcurrentCacheWait[V]
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
	if wgw, found = c.wg[key]; found {
		c.mu.Unlock()
		wgw.wg.Wait() // Wait for the other goroutine to finish

		if val, found = c.cache.Get(key); found {
			return val, nil
		}

		// check the result in the wait group
		if wgw.result != nil {
			return *wgw.result, nil
		}

		return c.ZeroValue, errors.NewProcessingError("cache: failed to get value after waiting")
	}

	// Create a new WaitGroup for the key
	wg = &sync.WaitGroup{}
	wg.Add(1)
	c.wg[key] = &expiringConcurrentCacheWait[V]{
		wg: wg,
	}

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
	val, allowCache, err = fetchFunc()
	if err != nil {
		return c.ZeroValue, err
	}

	// Cache the result
	if allowCache {
		c.cache.Set(key, val)
	}

	c.wg[key].result = &val

	return val, nil
}
