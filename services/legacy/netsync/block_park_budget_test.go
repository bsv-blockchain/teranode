package netsync

import (
	"context"
	"testing"

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

	blocks := minedBlocks(t, 2)

	entries := make([]parkedBlock, 0, len(blocks))

	for _, b := range blocks {
		msgBlock := b.MsgBlock()

		entry := parkedBlock{hash: msgBlock.BlockHash(), prevBlock: msgBlock.Header.PrevBlock}

		require.Equal(t, parkAccepted, park.Park(context.Background(), entry, msgBlock))

		entry.size = int64(msgBlock.SerializeSize())
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

	msgBlock := minedBlocks(t, 1)[0].MsgBlock()

	entry := parkedBlock{hash: msgBlock.BlockHash(), prevBlock: msgBlock.Header.PrevBlock}
	require.Equal(t, parkAccepted, park.Park(context.Background(), entry, msgBlock))

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

// TestReadAhead_TheWalkStopsAtItsBlockDepth is the brake, and it counts blocks.
//
// There used to be a byte budget here instead, and it was removed. A block's
// size is not known until it has been downloaded, so a byte bound cannot stop
// the bandwidth being spent, only refuse a block already paid for; its default
// matched the park's own ceiling so the walk filled the park to exactly its
// refusal point; and the round it abandoned included the header at the front of
// the list, which is the one block whose arrival would have freed the bytes.
// Mainnet discarded 1.02 TB of block bytes in two days under it.
//
// What is left is the depth, in blocks, checked before a request goes out. That
// is what SV Node bounds by and all it bounds by.
func TestReadAhead_TheWalkStopsAtItsBlockDepth(t *testing.T) {
	h := newParkWiringHarness(t, true)

	h.sm.headerMu.Lock()
	ceiling, ok := h.sm.lookaheadCeilingLocked()
	h.sm.headerMu.Unlock()

	require.True(t, ok, "with a header list and a configured lower window there is a ceiling")

	h.sm.headerMu.Lock()
	front := h.sm.headerList.Front().Value.(*headerNode).height
	h.sm.headerMu.Unlock()

	require.Equal(t, int64(front)+int64(h.sm.settings.Legacy.BlockDownloadLowerWindow), ceiling,
		"the ceiling is the front of the header list plus the configured depth, in blocks")

	// Zero disables it, which is the compiled default and means no limit.
	h.sm.settings.Legacy.BlockDownloadLowerWindow = 0

	h.sm.headerMu.Lock()
	_, ok = h.sm.lookaheadCeilingLocked()
	h.sm.headerMu.Unlock()

	require.False(t, ok, "a lower window of zero is no ceiling at all")
}
