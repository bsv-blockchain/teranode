package blockvalidation

import (
	"sync"
	"time"
)

// cacheEntry is a generic structure to hold a resource, its loading state, and expiry time.
type cacheEntry[V any] struct {
	asset  V
	err    error
	cond   *sync.Cond
	ready  bool
	expiry time.Time
}

// AssetFetcher is a generic structure for fetching assets with caching and expiry.
type AssetFetcher[K comparable, V any] struct {
	mu             sync.Mutex
	expiryDuration time.Duration
	cache          map[K]*cacheEntry[V]
	builder        func(K) (V, error)
}

// NewAssetFetcher creates a new instance of a generic asset fetcher with expiry.
func NewAssetFetcher[K comparable, V any](builder func(K) (V, error), expiryDuration time.Duration) *AssetFetcher[K, V] {
	af := &AssetFetcher[K, V]{
		cache:          make(map[K]*cacheEntry[V]),
		expiryDuration: expiryDuration,
		builder:        builder,
	}

	go func() {
		for {
			time.Sleep(expiryDuration / 2) // Adjust the duration as needed
			af.mu.Lock()

			for key, entry := range af.cache {
				if time.Now().After(entry.expiry) {
					delete(af.cache, key)
				}
			}

			af.mu.Unlock()
		}
	}()

	return af
}

// GetResource retrieves or builds a resource, with expiry handling.
func (af *AssetFetcher[K, V]) GetAsset(key K) (V, error) {
	af.mu.Lock()

	// Check if resource is in cache and not expired
	if entry, found := af.cache[key]; found {
		for !entry.ready {
			entry.cond.Wait()
		}

		af.mu.Unlock()
		return entry.asset, nil
	}

	// Create a new cache entry
	cond := sync.NewCond(&af.mu)

	af.cache[key] = &cacheEntry[V]{cond: cond, expiry: time.Now().Add(af.expiryDuration)}
	af.mu.Unlock()

	// Build the resource
	resource, err := af.builder(key)
	if err != nil {
		af.mu.Lock()

		// Update the cache entry
		af.cache[key].err = err
		af.cache[key].ready = true
	} else {
		af.mu.Lock()

		// Update the cache entry
		af.cache[key].asset = resource
		af.cache[key].ready = true
	}

	cond.Broadcast()

	af.mu.Unlock()

	return resource, err
}
