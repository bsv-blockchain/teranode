package netsync

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// chargedTotal is what the byte counter should be at any moment: the sum of what
// every block still on the books has been billed.
func chargedTotal(park *blockPark) int64 {
	park.mu.Lock()
	defer park.mu.Unlock()

	var total int64

	for _, size := range park.charged {
		total += size
	}

	return total
}

// TestBlockPark_SettlingABlockTwiceDoesNotMoveTheCounterTwice is the drift that
// billing per hash exists to make impossible.
//
// Delete used to subtract a size the CALLER supplied, so a block settled twice —
// which several rows of the disposition table can reach — was subtracted twice,
// and the floor at zero swallowed the evidence. On mainnet the counter read
// 14.7 GB against 9.2 GB actually on disk. Anything reading that counter, and
// the read-ahead ceiling now does, was reading fiction.
func TestBlockPark_SettlingABlockTwiceDoesNotMoveTheCounterTwice(t *testing.T) {
	park, _ := newTestPark(t, "")

	prevs := []chainhash.Hash{{0x31}, {0x32}}
	entries := make([]parkedBlock, 0, len(prevs))

	for i, prev := range prevs {
		hash := parkedRecord(t, park, prev, byte(i+1))

		size, exists, err := park.convertedRecordSize(context.Background(), hash)
		require.NoError(t, err)
		require.True(t, exists)

		entry := parkedBlock{hash: hash, prevBlock: prev, size: size}
		require.True(t, park.AdoptWritten(entry))

		entries = append(entries, entry)
	}

	full := park.Bytes()
	require.Positive(t, full)
	require.Equal(t, full, chargedTotal(park), "the counter is the sum of what is billed")

	taken, ok := park.Take(entries[0].hash)
	require.True(t, ok)
	require.Equal(t, full, park.Bytes(), "a taken block keeps its blob and its charge")

	park.Delete(context.Background(), taken)

	settled := park.Bytes()
	require.Less(t, settled, full, "settling it gives the bytes back")
	require.Equal(t, settled, chargedTotal(park))

	// The same block settled again, which is what a second disposition on one
	// block does. Under the old accounting this subtracted its size a second
	// time from blocks that still have it.
	park.Delete(context.Background(), taken)
	require.Equal(t, settled, park.Bytes(), "settling the same block twice must not move the counter twice")

	park.Delete(context.Background(), entries[0])
	require.Equal(t, settled, park.Bytes(), "nor does settling a stale copy of it")

	require.Equal(t, chargedTotal(park), park.Bytes())
}

// TestBlockPark_ARestoredBlockIsStillBilled covers the other direction.
//
// A block taken and then given back keeps its blob on disk, so it must keep its
// charge. Restore used to re-index it without re-billing, which was correct only
// for the paths that had not released the charge, and silently wrong for the
// ones that had.
func TestBlockPark_ARestoredBlockIsStillBilled(t *testing.T) {
	park, _ := newTestPark(t, "")

	prev := chainhash.Hash{0x35}
	hash := parkedRecord(t, park, prev, 1)

	size, exists, err := park.convertedRecordSize(context.Background(), hash)
	require.NoError(t, err)
	require.True(t, exists)

	entry := parkedBlock{hash: hash, prevBlock: prev, size: size}
	require.True(t, park.AdoptWritten(entry))

	full := park.Bytes()

	taken, ok := park.Take(entry.hash)
	require.True(t, ok)

	park.Restore(taken)

	require.Equal(t, 1, park.Len())
	require.Equal(t, full, park.Bytes(), "a block that is back in the index is still on disk and still billed")
	require.Equal(t, full, chargedTotal(park))

	// And it can still be settled exactly once afterwards.
	settled, ok := park.Take(entry.hash)
	require.True(t, ok)

	park.Delete(context.Background(), settled)

	require.Zero(t, park.Bytes())
	require.Zero(t, chargedTotal(park))
}
