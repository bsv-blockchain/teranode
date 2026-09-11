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

// TestReadAhead_TheWalkStopsAtTheByteBudget is the brake.
//
// The block-count window beside it says nothing about disk, because a window of
// 128 is 1.4 GB of small blocks and 190 GB of 2022-era ones. What bounds the
// park is the byte budget, and it has to count the bytes already owed by peers
// as well as the bytes already on disk: a round that ignores what is coming
// requests far past the budget and only finds out on delivery, after the merkle
// rebuild has been paid for.
func TestReadAhead_TheWalkStopsAtTheByteBudget(t *testing.T) {
	h := newParkWiringHarness(t, true)

	h.sm.blockSizeTracker = newBlockSizeTracker(10)

	require.False(t, h.sm.readAheadBudgetExhausted(), "an empty park with nothing owed is not at its budget")

	// Two blocks owed, at the average size the tracker has learned.
	h.sm.blockSizeTracker.addBlockSize(4 * 1024 * 1024)
	h.sm.blockDownloads.Add(h.peer, chainhash.Hash{0x01})
	h.sm.blockDownloads.Add(h.peer, chainhash.Hash{0x02})

	h.sm.settings.Legacy.BlockDownloadMaxBytes = 16 * 1024 * 1024
	require.False(t, h.sm.readAheadBudgetExhausted(), "8 MiB owed against a 16 MiB budget leaves room")

	h.sm.settings.Legacy.BlockDownloadMaxBytes = 8 * 1024 * 1024
	require.True(t, h.sm.readAheadBudgetExhausted(),
		"the bytes peers already owe count against the budget, not just the bytes on disk")

	h.sm.settings.Legacy.BlockDownloadMaxBytes = 0
	require.False(t, h.sm.readAheadBudgetExhausted(), "zero disables the ceiling")
}
