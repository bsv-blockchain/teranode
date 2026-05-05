package blockchain

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMTPCache_EmptyMisses(t *testing.T) {
	c := newMTPCache()

	// Single get on empty cache misses
	_, ok := c.get(5)
	assert.False(t, ok)

	// Range get on empty cache misses
	out, ok := c.getRange(0, 10)
	assert.False(t, ok)
	assert.Nil(t, out)

	// Empty range (toHeight < fromHeight) returns empty + ok=true
	out, ok = c.getRange(10, 5)
	assert.True(t, ok)
	assert.Empty(t, out)
}

func TestMTPCache_PutGetRange(t *testing.T) {
	c := newMTPCache()

	// Heights 0..14 all populated; below MedianTimeBlocks (11) we still
	// expect cache hits because the entries were explicitly stored, but
	// store non-zero values to avoid the zero-as-miss sentinel.
	values := []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	c.putRange(0, values)

	got, ok := c.getRange(0, 14)
	require.True(t, ok)
	assert.Equal(t, values, got)

	got, ok = c.getRange(5, 10)
	require.True(t, ok)
	assert.Equal(t, []uint32{6, 7, 8, 9, 10, 11}, got)

	mtp, ok := c.get(11)
	require.True(t, ok)
	assert.Equal(t, uint32(12), mtp)
}

func TestMTPCache_RangeBeyondCacheMisses(t *testing.T) {
	c := newMTPCache()
	c.putRange(0, []uint32{1, 2, 3})

	_, ok := c.getRange(0, 5)
	assert.False(t, ok, "range exceeding cache should miss")
}

func TestMTPCache_ZeroSentinelAtMTPHeight(t *testing.T) {
	c := newMTPCache()
	// Slot 11 left as zero — at or above MedianTimeBlocks, treat as miss so
	// caller refetches and confirms.
	c.putRange(0, []uint32{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})

	_, ok := c.get(11)
	assert.False(t, ok, "zero at height >= 11 must be treated as miss")

	// Below MedianTimeBlocks, zero is valid (genuine MTP=0 for early heights)
	mtp, ok := c.get(5)
	require.True(t, ok)
	assert.Equal(t, uint32(0), mtp)
}

func TestMTPCache_Truncate(t *testing.T) {
	c := newMTPCache()
	c.putRange(0, []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})

	c.truncate(10)

	_, ok := c.get(10)
	assert.False(t, ok, "height >= truncate point must miss")

	mtp, ok := c.get(9)
	require.True(t, ok)
	assert.Equal(t, uint32(10), mtp)

	// Truncate beyond current length is a no-op
	c.truncate(100)
	_, ok = c.get(9)
	assert.True(t, ok)
}

func TestMTPCache_Reset(t *testing.T) {
	c := newMTPCache()
	c.putRange(0, []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
	c.reset()

	_, ok := c.get(0)
	assert.False(t, ok, "after reset, all entries miss")
}

func TestMTPCache_Overwrite(t *testing.T) {
	c := newMTPCache()
	c.putRange(0, []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
	// Overwrite with new values (e.g. reorg-driven repopulation)
	c.putRange(5, []uint32{100, 101, 102})

	got, ok := c.getRange(4, 8)
	require.True(t, ok)
	assert.Equal(t, []uint32{5, 100, 101, 102, 9}, got)
}

func TestMTPCache_ConcurrentReadWrite(t *testing.T) {
	c := newMTPCache()
	c.putRange(0, []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})

	var wg sync.WaitGroup
	const readers = 16
	const iters = 1000

	wg.Add(readers + 1)
	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				_, _ = c.getRange(0, 19)
			}
		}()
	}
	go func() {
		defer wg.Done()
		for j := 0; j < iters; j++ {
			c.putRange(0, []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
		}
	}()

	wg.Wait()
}
