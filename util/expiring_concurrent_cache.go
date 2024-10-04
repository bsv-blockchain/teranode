package util

import (
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/ordishs/go-utils/expiringmap"
)

type ExpiringConcurrentCache[K comparable, V any] struct {
	mu    sync.Mutex
	cache *expiringmap.ExpiringMap[K, V]
	wg    map[K]*sync.WaitGroup
}

func NewExpiringConcurrentCache[K comparable, V any](expiration time.Duration) *ExpiringConcurrentCache[K, V] {
	return &ExpiringConcurrentCache[K, V]{
		cache: expiringmap.New[K, V](expiration),
		wg:    make(map[K]*sync.WaitGroup),
	}
}

func (c *ExpiringConcurrentCache[K, V]) GetOrSet(key K, fetchFunc func() (V, error)) (V, error) {
	var zeroValue V

	c.mu.Lock()

	// Check if the value is already in the cache
	if val, found := c.cache.Get(key); found {
		c.mu.Unlock()
		return val, nil
	}

	// If not, check if there is an ongoing request
	if wg, found := c.wg[key]; found {
		c.mu.Unlock()
		wg.Wait()

		if val, found := c.cache.Get(key); found {
			return val, nil
		}

		return zeroValue, errors.NewProcessingError("cache: failed to get value after waiting")
	}

	// Otherwise, create a new WaitGroup for the key
	wg := &sync.WaitGroup{}
	wg.Add(1)
	c.wg[key] = wg
	c.mu.Unlock()

	// Do the actual work
	result, err := fetchFunc()
	if err != nil {
		return zeroValue, err
	}

	c.mu.Lock()

	// Cache the result
	c.cache.Set(key, result)

	// Mark the wait group as done and remove it
	wg.Done()
	delete(c.wg, key)

	c.mu.Unlock()

	return result, nil
}
