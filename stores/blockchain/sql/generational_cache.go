package sql

import (
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/jellydator/ttlcache/v3"
)

// GenerationalCache wraps ttlcache with generation-based invalidation tracking.
// This prevents stale query results from being cached after invalidation occurs.
//
// Race condition without generational tracking:
// 1. Thread A: cache miss, starts DB query
// 2. Cache is invalidated (DeleteAll called, for example block added to chain)
// 3. Thread A: completes query with now-stale result
// 4. Thread A: writes stale result to cache ❌
// 5. Future reads return stale data instead of fresh data
//
// With generation tracking:
// - Begin() captures the current generation in a CacheOperation object
// - DeleteAll() increments the generation
// - CacheOperation.Set() only writes if generation matches (query wasn't invalidated)
// - This ensures stale results from pre-invalidation queries aren't cached
type GenerationalCache struct {
	ttlCache   *ttlcache.Cache[chainhash.Hash, any]
	generation atomic.Uint64
	stopped    atomic.Bool
}

// NewGenerationalCache creates a new generational cache instance.
// The cache is automatically started and begins cleanup of expired items.
func NewGenerationalCache() *GenerationalCache {
	gc := &GenerationalCache{
		ttlCache: ttlcache.New[chainhash.Hash, any](
			ttlcache.WithDisableTouchOnHit[chainhash.Hash, any](),
		),
	}
	// Auto-start the cache cleanup goroutine
	go gc.ttlCache.Start()
	return gc
}

// Begin starts a cache-safe operation by capturing the current generation.
// Use this for Get→work→Set patterns to prevent stale writes after cache invalidation.
func (gc *GenerationalCache) Begin(key chainhash.Hash) *CacheOperation {
	return &CacheOperation{
		generationalCache: gc,
		key:               key,
		generation:        gc.generation.Load(),
	}
}

// DeleteAll clears all cached entries and increments the generation.
// This invalidates any in-flight operations, preventing them from caching stale results.
func (gc *GenerationalCache) DeleteAll() {
	gc.ttlCache.DeleteAll()
	gc.generation.Add(1)
}

// Stop halts automatic cleanup.
// It is safe to call Stop multiple times.
func (gc *GenerationalCache) Stop() {
	if gc.stopped.CompareAndSwap(false, true) {
		gc.ttlCache.Stop()
	}
}

// CacheOperation represents a scoped cache operation that captures generation at operation start.
// This provides a cleaner API than token passing - the generation is encapsulated in the object.
//
// Usage pattern:
//
//	op := cache.Begin(key)
//	if item := op.Get(); item != nil {
//	    return item.Value()
//	}
//	result := doExpensiveWork()
//	op.Set(result, ttl)  // Only caches if no invalidation occurred
type CacheOperation struct {
	generationalCache *GenerationalCache
	key               chainhash.Hash
	generation        uint64 // captured at Begin time
}

// Get retrieves the cached Item if present, or nil on miss.
// Returns *ttlcache.Item to maintain API compatibility - call .Value() on result.
func (co *CacheOperation) Get() *ttlcache.Item[chainhash.Hash, any] {
	return co.generationalCache.ttlCache.Get(co.key)
}

// Set writes a value to the cache only if generation hasn't changed since Begin.
// Returns true if cached, false if generation changed (cache was invalidated during operation).
func (co *CacheOperation) Set(value any, ttl time.Duration) bool {
	// Only cache if generation matches (cache wasn't invalidated during operation)
	if co.generation == co.generationalCache.generation.Load() {
		co.generationalCache.ttlCache.Set(co.key, value, ttl)
		return true
	}
	// Generation changed - skip caching stale result
	return false
}
