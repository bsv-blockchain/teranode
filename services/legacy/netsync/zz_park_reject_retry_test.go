package netsync

import (
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// TestParkRejectionLeavesTheBlockRequestable is the gate on removing the park's
// merkle check.
//
// The park verifies a downloaded block against its header before writing it,
// which costs a merkle rebuild over every transaction: 13.7 seconds for a
// 100,001-transaction mainnet block, paid on 91% of blocks because that is how
// many arrive out of order. The same verification then runs again during normal
// block processing, which is where it has to run anyway, so the park's copy is
// redundant for correctness and only earlier.
//
// What makes removing it safe is what happens when the later check fails, and
// that is what this test pins. A rejected parked block must have its bytes
// deleted, be put back on the download walk, and be re-requestable — because if
// a rejection is terminal then moving the check turns a caught attack into a
// stalled sync, which is exactly the failure the park's check was added to
// prevent.
//
// The specific hazard is the cascade mark. The rejection sets a flag that
// suppresses a failed block's descendants, which is right when a commit failure
// means the block is suspect. If a verification failure becomes the ordinary way
// a bad park is caught, that mark must not suppress the retry of the block
// itself.
func TestParkRejectionLeavesTheBlockRequestable(t *testing.T) {
	h := newParkWiringHarness(t, true)

	msgBlock := h.blocks[1].MsgBlock()
	hash := msgBlock.BlockHash()

	require.Equal(t, parkAccepted,
		h.sm.blockPark.Park(h.sm.ctx, parkedBlock{hash: hash, prevBlock: msgBlock.Header.PrevBlock}, msgBlock))
	require.Equal(t, 1, h.sm.blockPark.Len())

	entry, ok := h.sm.blockPark.Take(hash)
	require.True(t, ok, "the drain claims the entry before committing it")

	// What the drain does when normal verification refuses the block. A merkle
	// mismatch is none of the special cases parkCommitFailure recognises, so it
	// takes the default arm.
	h.sm.parkedBlockFailed(entry, errors.NewBlockInvalidError("merkle root mismatch"))

	require.Zero(t, h.sm.blockPark.Len(), "a rejected block must not stay parked")
	require.Empty(t, parkDirEntries(t, h.parkDir), "and its bytes must be deleted, not leaked")

	// The mark is set, which is correct: until this block is re-obtained, none of
	// its descendants can commit, so dropping them saves work.
	_, marked := h.sm.recentlyFailedBlocks.Get(hash)
	require.True(t, marked, "the rejection marks the block, which is what suppresses its descendants")

	// The gate. The mark is keyed on the hash of a block whose CHILDREN should be
	// suppressed, and the arrival path tests it against a block's PARENT, so a
	// block's own mark must not suppress the block itself. If this inverted, a
	// rejected block could never be re-obtained and moving the check would trade
	// a caught attack for a permanent stall.
	parentOfThisBlock := msgBlock.Header.PrevBlock

	_, parentMarked := h.sm.recentlyFailedBlocks.Get(parentOfThisBlock)
	require.False(t, parentMarked,
		"the rejection must not mark this block's parent, or the retry is suppressed as its own descendant")

	// And re-admission clears the mark, so the descendants become admissible
	// again the moment a good copy arrives rather than waiting for it to expire.
	h.sm.recentlyFailedBlocks.Delete(hash)

	_, stillMarked := h.sm.recentlyFailedBlocks.Get(hash)
	require.False(t, stillMarked)

	require.Equal(t, parkAccepted,
		h.sm.blockPark.Park(h.sm.ctx, parkedBlock{hash: hash, prevBlock: msgBlock.Header.PrevBlock}, msgBlock),
		"a good copy of a previously rejected block must be parkable again")
}
