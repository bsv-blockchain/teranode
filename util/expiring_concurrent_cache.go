package util

import (
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
)

type expiringConcurrentCacheWait[V any] struct {
	wg     *sync.WaitGroup
	result *V
	err    error // leader's fetch error, shared verbatim with waiters on this flight
}

// testHookWaiterAboutToWait, when non-nil, is invoked by a waiting goroutine immediately after it
// has captured the in-flight entry and released c.mu, just before it blocks on the flight's
// WaitGroup. It is nil in production (a single nil check on the already-contended waiter path, no
// behavior change) and is set only by tests needing deterministic leader/waiter ordering.
var testHookWaiterAboutToWait func()

type ExpiringConcurrentCache[K comparable, V any] struct {
	mu        sync.RWMutex
	cache     *expiringmap.ExpiringMap[K, V]
	wg        map[K]*expiringConcurrentCacheWait[V]
	ZeroValue V
}

// NewExpiringConcurrentCache creates a new thread-safe cache with automatic expiration.
// Items expire after the specified duration and are automatically cleaned up.
func NewExpiringConcurrentCache[K comparable, V any](expiration time.Duration) *ExpiringConcurrentCache[K, V] {
	return &ExpiringConcurrentCache[K, V]{
		cache: expiringmap.New[K, V](expiration),
		wg:    make(map[K]*expiringConcurrentCacheWait[V]),
	}
}

// Stop stops the background cleanup goroutine of the underlying ExpiringMap.
func (c *ExpiringConcurrentCache[K, V]) Stop() {
	c.cache.Stop()
}

// GetOrSet retrieves a value from the cache or fetches it using the provided function.
// If multiple goroutines request the same key simultaneously, only one fetch operation occurs.
// The fetchFunc returns (value, shouldCache, error) where shouldCache determines if the result is cached.
//
// fetchFunc must not call GetOrSet for the SAME key it is fetching — neither directly nor from a
// goroutine it then waits on. That key's fetch is already in flight, so the nested call would block
// until the outer fetch completes, which cannot happen while the outer fetch is waiting on it: a
// deadlock. This is unsupported by design and is not detected at runtime. Re-entrancy for DIFFERENT
// keys is fully supported (see TestGetOrSetReentrantFetchDoesNotDeadlock).
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

		if testHookWaiterAboutToWait != nil {
			testHookWaiterAboutToWait()
		}

		wgw.wg.Wait() // Wait for the other goroutine to finish

		if val, found = c.cache.Get(key); found {
			return val, nil
		}

		// check the result in the wait group
		if wgw.result != nil {
			return *wgw.result, nil
		}

		// share the leader's real error with waiters, if the fetch failed
		if wgw.err != nil {
			return c.ZeroValue, wgw.err
		}

		return c.ZeroValue, errors.NewProcessingError("cache: failed to get value after waiting")
	}

	// Create a new WaitGroup for the key
	wg = &sync.WaitGroup{}
	wg.Add(1)
	wgw = &expiringConcurrentCacheWait[V]{
		wg: wg,
	}
	c.wg[key] = wgw

	// Release the global lock, for others to wait on the wait group.
	c.mu.Unlock()

	// Publish the result and clean up under the lock, but run fetchFunc() WITHOUT the lock.
	fetchOK := false

	defer func() {
		c.mu.Lock()

		if fetchOK {
			if err == nil {
				// Cache the successful result
				if allowCache {
					c.cache.Set(key, val)
				}

				wgw.result = &val
			} else {
				// Share the leader's real error with any waiters on this flight
				wgw.err = err
			}
		}

		wg.Done()         // after publish so waiters observe result via Done/Wait
		delete(c.wg, key) // remove in-flight entry, still under c.mu

		c.mu.Unlock()
	}()

	// Perform the fetch WITHOUT holding c.mu.
	val, allowCache, err = fetchFunc()
	fetchOK = true // reached only if fetchFunc returned (did not panic)

	if err != nil {
		return c.ZeroValue, err
	}

	return val, nil
}
