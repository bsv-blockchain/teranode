package sql

import "sync"

// blockTimestampCacheCapacity is the maximum number of block timestamps to keep
// in the sliding-window cache. Only the most recent entries are retained; older
// entries are pruned on each Add. 50 is well above the 11 needed for a single
// MTP calculation and provides headroom for minor out-of-order inserts.
const blockTimestampCacheCapacity = 50

// blockTimestampCache is a concurrency-safe, bounded cache of block timestamps
// keyed by height. It eliminates per-block SQL queries in
// calculateMedianTimePastForHeight during sequential block processing (seeder,
// catchup) where the previous 11 blocks are always already in the cache.
//
// Fork safety: when a block is stored at a height that already has a cached
// entry, all entries from that height onward are evicted so that stale
// timestamps from the old chain are never served. InvalidateBlock and
// RevalidateBlock clear the entire cache as a conservative safety net.
type blockTimestampCache struct {
	mu         sync.RWMutex
	timestamps map[uint32]uint32 // height → block_time (unix)
}

func newBlockTimestampCache() *blockTimestampCache {
	return &blockTimestampCache{
		timestamps: make(map[uint32]uint32, blockTimestampCacheCapacity),
	}
}

// Add stores a block timestamp for the given height.
//
// If the height already exists in the cache (fork), all entries at that height
// and above are evicted before the new value is stored. This guarantees that
// subsequent MTP lookups never mix timestamps from different chain branches.
func (c *blockTimestampCache) Add(height, blockTime uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Detect fork: a different block being stored at an already-cached height.
	if _, exists := c.timestamps[height]; exists {
		c.invalidateFromLocked(height)
	}

	c.timestamps[height] = blockTime

	// Keep the cache bounded by pruning entries that are too far behind.
	if len(c.timestamps) > blockTimestampCacheCapacity {
		c.pruneLocked(height)
	}
}

// GetRange returns the block timestamps for every height in [startHeight, endHeight]
// (inclusive) in ascending order. If any height in the range is missing from the
// cache, nil is returned to signal a cache miss.
func (c *blockTimestampCache) GetRange(startHeight, endHeight uint32) []uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	count := endHeight - startHeight + 1
	result := make([]uint32, 0, count)

	for h := startHeight; h <= endHeight; h++ {
		ts, ok := c.timestamps[h]
		if !ok {
			return nil
		}
		result = append(result, ts)
	}

	return result
}

// InvalidateFrom removes all cached entries at heights >= fromHeight.
func (c *blockTimestampCache) InvalidateFrom(fromHeight uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.invalidateFromLocked(fromHeight)
}

// Clear removes all cached entries.
func (c *blockTimestampCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.timestamps = make(map[uint32]uint32, blockTimestampCacheCapacity)
}

// Len returns the number of entries in the cache (used for testing).
func (c *blockTimestampCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.timestamps)
}

func (c *blockTimestampCache) invalidateFromLocked(fromHeight uint32) {
	for h := range c.timestamps {
		if h >= fromHeight {
			delete(c.timestamps, h)
		}
	}
}

func (c *blockTimestampCache) pruneLocked(currentHeight uint32) {
	minKeep := uint32(0)
	if currentHeight > blockTimestampCacheCapacity {
		minKeep = currentHeight - blockTimestampCacheCapacity
	}
	for h := range c.timestamps {
		if h < minKeep {
			delete(c.timestamps, h)
		}
	}
}
