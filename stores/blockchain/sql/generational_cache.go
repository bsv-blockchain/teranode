package sql

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/jellydator/ttlcache/v3"
)

// Header query results have a separate retained-payload budget. Entry count
// alone permits thousands of copies of whole-chain header responses.
const maxCachedHeaderBytes uint64 = 64 << 20

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
// - NewOp() captures the current generation in a CacheOperation object
// - DeleteAll() increments the generation
// - CacheOperation.Set() only writes if generation matches (query wasn't invalidated)
// - This ensures stale results from pre-invalidation queries aren't cached
type GenerationalCache struct {
	writeMu     sync.Mutex
	headerBytes uint64
	ttlCache    *ttlcache.Cache[chainhash.Hash, any]
	generation  atomic.Uint64
	stopped     atomic.Bool
}

// NewGenerationalCache creates a new generational cache instance.
// The cache is automatically started and begins cleanup of expired items.
//
// The accessors in this package build their cache keys by hashing a format string
// that includes caller-supplied values (see GetBlockHeaders and the sibling header
// accessors, which all follow the same pattern), so a caller varying those values
// produces a distinct entry per value. Without a capacity limit an unauthenticated
// caller could pin an unbounded number of entries this way; capacity caps that at
// the LRU-evicted maximum regardless of TTL. Pass 0 (or a negative value) for no
// capacity limit (only the per-entry TTL evicts) - used by tests that do not need
// the bound.
func NewGenerationalCache(capacity int) *GenerationalCache {
	opts := []ttlcache.Option[chainhash.Hash, any]{
		ttlcache.WithDisableTouchOnHit[chainhash.Hash, any](),
	}
	if capacity > 0 {
		opts = append(opts, ttlcache.WithCapacity[chainhash.Hash, any](uint64(capacity)))
	}

	gc := &GenerationalCache{
		ttlCache: ttlcache.New[chainhash.Hash, any](opts...),
	}
	// Auto-start the cache cleanup goroutine
	go gc.ttlCache.Start()
	return gc
}

// NewOp starts a cache-safe operation by capturing the current generation.
// Use this for Get→work→Set patterns to prevent stale writes after cache invalidation.
func (gc *GenerationalCache) NewOp(key chainhash.Hash) *CacheOperation {
	return &CacheOperation{
		generationalCache: gc,
		key:               key,
		generation:        gc.generation.Load(),
	}
}

// DeleteAll clears all cached entries and increments the generation.
// This invalidates any in-flight operations, preventing them from caching stale results.
func (gc *GenerationalCache) DeleteAll() {
	gc.writeMu.Lock()
	defer gc.writeMu.Unlock()
	gc.headerBytes = 0
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
//	op := cache.NewOp(key)
//	if item := op.Get(); item != nil {
//	    return item.Value()
//	}
//	result := doExpensiveWork()
//	op.Set(result, ttl)  // Only caches if no invalidation occurred
type CacheOperation struct {
	generationalCache *GenerationalCache
	key               chainhash.Hash
	generation        uint64 // captured at NewOp time
}

// Get retrieves the cached Item if present, or nil on miss.
// Returns *ttlcache.Item to maintain API compatibility - call .Value() on result.
func (co *CacheOperation) Get() *ttlcache.Item[chainhash.Hash, any] {
	return co.generationalCache.ttlCache.Get(co.key)
}

// Set writes a value to the cache only if generation hasn't changed since NewOp.
// Returns false if invalidation overtook the query or its header payload exceeds
// the cache budget; either case still permits returning the result to the caller.
func (co *CacheOperation) Set(value any, ttl time.Duration) bool {
	cost := cachedHeaderBytes(value)
	if cost > maxCachedHeaderBytes {
		return false
	}
	gc := co.generationalCache
	gc.writeMu.Lock()
	defer gc.writeMu.Unlock()
	// Only cache if generation matches (cache wasn't invalidated during operation)
	if co.generation == co.generationalCache.generation.Load() {
		// Conservatively charge replacements and expired entries until the next
		// clear. On budget exhaustion evict cached responses before admitting more.
		// This is eviction, not a chain change, so in-flight queries remain valid.
		if gc.headerBytes+cost > maxCachedHeaderBytes {
			gc.ttlCache.DeleteAll()
			gc.headerBytes = 0
		}
		gc.headerBytes += cost
		gc.ttlCache.Set(co.key, value, ttl)
		return true
	}
	// Generation changed - skip caching stale result
	return false
}

// cachedHeaderBytes estimates retained header payload conservatively, including
// slice backing arrays and variable-length metadata. It is not a process RSS
// limit: other cached value types and in-flight query results are not included.
func cachedHeaderBytes(value any) uint64 {
	if ids, ok := value.([]uint32); ok {
		return uint64(cap(ids)) * 4
	}
	pair, ok := value.([2]interface{})
	if !ok {
		return 0
	}
	headers, ok := pair[0].([]*model.BlockHeader)
	if !ok {
		return 0
	}
	metas, ok := pair[1].([]*model.BlockHeaderMeta)
	if !ok {
		return 0
	}
	// 256 bytes per slot covers pointers, fixed structs and header hashes on
	// supported architectures, even when distinct slots share an object.
	cost := (uint64(cap(headers)) + uint64(cap(metas))) * 256
	if cost > maxCachedHeaderBytes {
		return cost
	}
	for _, meta := range metas {
		if meta != nil {
			cost += uint64(len(meta.Miner)) + uint64(len(meta.PeerID)) + uint64(cap(meta.ChainWork))
			if cost > maxCachedHeaderBytes {
				return cost
			}
		}
	}
	return cost
}
