package sql

import "sync"

// blockTimestampCacheCapacity is how many blocks the median-time cache holds. When it is full,
// the block added first is dropped first. Side branches take entries too, so it holds far more
// than the 11 one median reads; each entry is about 16 bytes.
const blockTimestampCacheCapacity = 1000

// blockTimestampCache holds recent blocks' timestamps keyed by block ID, each with its parent's
// ID, so calculateMedianTimePastForHeight follows one block's own parent links in memory, as SV
// Node's GetMedianTimePast follows pprev. It saves a SQL parent walk per stored block during
// sync. An entry never goes stale, because a block's timestamp and parent never change, so a
// fork, an invalidation or a revalidation leaves it in place; a side block has its own key and
// cannot overwrite the main chain's.
type blockTimestampCache struct {
	mu     sync.RWMutex
	blocks map[uint64]cachedBlockTime
	order  [blockTimestampCacheCapacity]uint64 // a ring of IDs in the order they were added
	next   int                                 // the ring slot the next Add writes, holding the oldest ID once full
}

type cachedBlockTime struct {
	blockTime uint32
	parentID  uint64
}

func newBlockTimestampCache() *blockTimestampCache {
	return &blockTimestampCache{blocks: make(map[uint64]cachedBlockTime, blockTimestampCacheCapacity)}
}

// Add records block id's timestamp and parent. An id already present is left as it is.
func (c *blockTimestampCache) Add(id, parentID uint64, blockTime uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.blocks[id]; exists {
		return
	}

	if len(c.blocks) == blockTimestampCacheCapacity {
		delete(c.blocks, c.order[c.next])
	}

	c.blocks[id] = cachedBlockTime{blockTime: blockTime, parentID: parentID}
	c.order[c.next] = id
	c.next = (c.next + 1) % blockTimestampCacheCapacity
}

// Ancestors returns the timestamps of block id and its count-1 nearest ancestors, oldest first,
// or nil when any of them is not in the cache.
func (c *blockTimestampCache) Ancestors(id uint64, count int) []uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	times := make([]uint32, count)

	for i := count - 1; i >= 0; i-- {
		entry, ok := c.blocks[id]
		if !ok {
			return nil
		}

		times[i] = entry.blockTime
		id = entry.parentID
	}

	return times
}

// Clear removes every entry.
func (c *blockTimestampCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.blocks = make(map[uint64]cachedBlockTime, blockTimestampCacheCapacity)
	c.next = 0
}

// Len returns the number of entries.
func (c *blockTimestampCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.blocks)
}
