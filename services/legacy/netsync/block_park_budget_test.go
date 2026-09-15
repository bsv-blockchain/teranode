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

	// lookaheadCeilingLocked takes the committed height as a parameter now,
	// read by its caller before headerMu is taken (see wantedBlocks); any
	// value stands in for it here, since the point under test is the
	// arithmetic, not where the height comes from.
	best := int32(0)

	h.sm.headerMu.Lock()
	ceiling, ok := h.sm.lookaheadCeilingLocked(best)
	h.sm.headerMu.Unlock()

	require.True(t, ok, "with a configured lower window there is a ceiling")

	require.Equal(t, int64(best)+int64(h.sm.settings.Legacy.BlockDownloadLowerWindow), ceiling,
		"the ceiling is the committed height plus the configured depth, in blocks — "+
			"not the front of the header list, which this harness never commits past")

	// Zero disables it, which is the compiled default and means no limit.
	h.sm.settings.Legacy.BlockDownloadLowerWindow = 0

	h.sm.headerMu.Lock()
	_, ok = h.sm.lookaheadCeilingLocked(best)
	h.sm.headerMu.Unlock()

	require.False(t, ok, "a lower window of zero is no ceiling at all")
}

// TestReadAhead_IsAnchoredToTheCommittedBlock pins the rule that keeps the park
// self-limiting: never ask for a block more than the read-ahead depth above the
// last block this node has actually validated.
//
// The ceiling used to be anchored to the front of the header list, which
// advances when a block ARRIVES rather than when it commits. That made it a
// ratchet driven by downloads with no coupling to the committer, so the park
// grew until it hit its entry cap, started refusing blocks, and punched holes in
// the run waiting to commit. Measured on mainnet during a genesis resync on
// 2026-09-12: front at 4877, chain settled at 868, park full at 4096, and 1.8
// blocks a minute against a 23ms commit path.
//
// That front-of-list fallback is gone as of this fix round, not merely
// superseded: lookaheadCeilingLocked no longer reads the header list at all, so
// there is nothing left to fall back onto or seed a header list against here.
func TestReadAhead_IsAnchoredToTheCommittedBlock(t *testing.T) {
	h := newParkWiringHarness(t, true)

	// An arbitrary baseline standing in for wherever a real node's committed
	// height happens to be; the point under test is that the ceiling follows
	// this number when it moves, not what the number itself is.
	front := int32(4877)

	depth := int64(h.sm.settings.Legacy.BlockDownloadLowerWindow)
	require.Positive(t, depth, "the harness must configure a read-ahead depth or this test proves nothing")

	// Committing moves the ceiling, because the committer is what the read-ahead
	// is ahead OF. best is passed straight in now (see wantedBlocks, which reads
	// it from the chain before taking headerMu and hands it down), so moving it
	// is just passing a different value.
	h.sm.headerMu.Lock()
	ceiling, ok := h.sm.lookaheadCeilingLocked(front + 500)
	h.sm.headerMu.Unlock()

	require.True(t, ok)
	require.Equal(t, int64(front)+500+depth, ceiling,
		"the ceiling follows the committed block, not the front of the header list")

	// Nothing committed in this process yet is a real state — genesis, or a
	// struct-literal test harness — and it is no longer a special case: with the
	// header-list fallback removed, a height of zero here answers zero plus
	// depth, the same arithmetic as any other height.
	h.sm.headerMu.Lock()
	atStart, ok := h.sm.lookaheadCeilingLocked(0)
	h.sm.headerMu.Unlock()

	require.True(t, ok)
	require.Equal(t, depth, atStart,
		"with nothing committed the ceiling is the depth alone, not the depth above wherever the header list happens to start")
}
