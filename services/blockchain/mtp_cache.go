package blockchain

import (
	"sync"
	"sync/atomic"
)

// mtpCache holds Median Time Past values indexed by block height. It is owned by
// the Blockchain service and serves GetMedianTimePastForHeights /
// GetMedianTimePastRange without round-tripping through the storage layer's
// per-StoreBlock cache invalidation.
//
// MTP for height h depends on block_time of heights [h-11, h-1] (BIP113).
// The values for committed blocks are immutable until a chain reorganisation
// changes which block sits at a given height — at that point we truncate from
// the affected height onward and let subsequent queries repopulate.
//
// Lookups are O(1); the cache is a contiguous slice indexed by height. Memory
// is bounded by chain length (~4 bytes per block, ~5 MB at 1.27M heights).
//
// A generation counter guards against a TOCTOU race between a Get* path that
// reads from the store and a concurrent reorg that truncates / resets the
// cache. Callers snapshot the generation before the store read via
// generation(), and write back via putRangeIfGen which only commits if the
// snapshot still matches the current generation. truncate / reset bump the
// generation atomically so any in-flight putRangeIfGen call from a stale
// view is rejected.
type mtpCache struct {
	mu   sync.RWMutex
	mtps []uint32
	gen  atomic.Uint64
}

// newMTPCache returns an empty cache. Entries are added lazily on cache
// misses.
func newMTPCache() *mtpCache {
	return &mtpCache{}
}

// getRange returns cached MTP values for [fromHeight, toHeight] or false if
// the cache does not fully cover the range. A zero entry is treated as "not
// covered" so genuine zero MTP values (e.g. heights below MedianTimeBlocks)
// are re-fetched and confirmed instead of silently served.
func (c *mtpCache) getRange(fromHeight, toHeight uint32) ([]uint32, bool) {
	if toHeight < fromHeight {
		return []uint32{}, true
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if uint32(len(c.mtps)) <= toHeight {
		if prometheusBlockchainMTPCacheMisses != nil {
			prometheusBlockchainMTPCacheMisses.Inc()
		}
		return nil, false
	}

	out := make([]uint32, toHeight-fromHeight+1)
	for h := fromHeight; h <= toHeight; h++ {
		mtp := c.mtps[h]
		if mtp == 0 && h >= uint32(MedianTimeBlocks) {
			// Sentinel: never populated for a height where MTP could be
			// non-zero. Force a miss so the caller refetches.
			if prometheusBlockchainMTPCacheMisses != nil {
				prometheusBlockchainMTPCacheMisses.Inc()
			}
			return nil, false
		}
		out[h-fromHeight] = mtp
	}
	if prometheusBlockchainMTPCacheHits != nil {
		prometheusBlockchainMTPCacheHits.Inc()
	}
	return out, true
}

// get returns the cached MTP for a single height, or false on miss. Same
// zero-as-miss semantics as getRange for heights >= MedianTimeBlocks.
func (c *mtpCache) get(height uint32) (uint32, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if uint32(len(c.mtps)) <= height {
		if prometheusBlockchainMTPCacheMisses != nil {
			prometheusBlockchainMTPCacheMisses.Inc()
		}
		return 0, false
	}
	mtp := c.mtps[height]
	if mtp == 0 && height >= uint32(MedianTimeBlocks) {
		if prometheusBlockchainMTPCacheMisses != nil {
			prometheusBlockchainMTPCacheMisses.Inc()
		}
		return 0, false
	}
	if prometheusBlockchainMTPCacheHits != nil {
		prometheusBlockchainMTPCacheHits.Inc()
	}
	return mtp, true
}

// generation returns the current cache generation. Snapshot this before
// reading from the store, then pass the snapshot to putRangeIfGen. If a
// concurrent truncate or reset bumps the generation between the snapshot and
// the write, putRangeIfGen will discard the write so stale (pre-reorg) values
// cannot poison the cache.
func (c *mtpCache) generation() uint64 {
	return c.gen.Load()
}

// putRange stores MTP values for [fromHeight, fromHeight+len(mtps)-1].
// Slots are grown as needed; existing entries are overwritten so reorg-driven
// repopulations replace stale values cleanly.
//
// This unguarded variant is used by tests and other callers that already hold
// some external synchronisation. Production Get* paths must use putRangeIfGen
// instead, with a generation snapshot taken before the upstream store read.
func (c *mtpCache) putRange(fromHeight uint32, mtps []uint32) {
	if len(mtps) == 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.putRangeLocked(fromHeight, mtps)
}

// putRangeIfGen commits the write only if the cache generation still matches
// the snapshot. Returns true if the write was applied, false if it was
// rejected because a truncate or reset bumped the generation since the
// snapshot. Used by Get* paths to close the read-store/write-cache TOCTOU
// window with concurrent reorg invalidations.
func (c *mtpCache) putRangeIfGen(fromHeight uint32, mtps []uint32, snapshot uint64) bool {
	if len(mtps) == 0 {
		return true
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.gen.Load() != snapshot {
		return false
	}
	c.putRangeLocked(fromHeight, mtps)
	return true
}

// putRangeLocked is the shared write body. Caller must hold c.mu for writing.
func (c *mtpCache) putRangeLocked(fromHeight uint32, mtps []uint32) {
	end := fromHeight + uint32(len(mtps))
	if uint32(len(c.mtps)) < end {
		grow := end - uint32(len(c.mtps))
		c.mtps = append(c.mtps, make([]uint32, grow)...)
	}
	for i, mtp := range mtps {
		c.mtps[fromHeight+uint32(i)] = mtp
	}
}

// truncate drops all cached entries with height >= fromHeight. Callers invoke
// this after StoreBlock (to discard any speculative top-of-chain entry) and on
// chain reorganisation paths (InvalidateBlock, RevalidateBlock) where blocks
// from fromHeight upward may move chains. Bumps the generation so any
// in-flight putRangeIfGen call from a stale view is rejected.
func (c *mtpCache) truncate(fromHeight uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if uint32(len(c.mtps)) > fromHeight {
		c.mtps = c.mtps[:fromHeight]
	}
	c.gen.Add(1)
	if prometheusBlockchainMTPCacheTruncations != nil {
		prometheusBlockchainMTPCacheTruncations.Inc()
	}
}

// reset clears the entire cache. Used on store-wide events (e.g. test resets)
// where the safest action is to refetch everything. Bumps the generation so
// any in-flight putRangeIfGen call from a stale view is rejected.
func (c *mtpCache) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.mtps = c.mtps[:0]
	c.gen.Add(1)
	if prometheusBlockchainMTPCacheResets != nil {
		prometheusBlockchainMTPCacheResets.Inc()
	}
}
