package netsync

import (
	"testing"

	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestSyncManager_AParkedBlockRecordsAUsableHeight is the prerequisite for
// judging a parked block by its height, and it is delivered through the shape
// production actually uses.
//
// The queue message's height comes from the decoded block, and a legacy block is
// built by bsvutil.NewBlockFromBlockAndBytes with no height set, so it arrives as
// BlockHeightUnknown. Any rule that reads the recorded height would therefore
// read nothing. The harness's own deliver helper supplies a positive height that
// production never supplies, so a test written through it would pass whether the
// height was sourced or not.
func TestSyncManager_AParkedBlockRecordsAUsableHeight(t *testing.T) {
	h := newParkWiringHarness(t, true)

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	msgBlock := h.blocks[0].MsgBlock()

	require.Equal(t, int32(-1), bsvutil.BlockHeightUnknown,
		"the premise: a legacy-delivered block reports no height at all")

	require.NoError(t, h.deliverBlock(t, msgBlock, bsvutil.BlockHeightUnknown))
	require.Equal(t, 1, h.sm.blockPark.Len())

	entry, ok := h.sm.blockPark.Take(msgBlock.BlockHash())
	require.True(t, ok)
	require.Equal(t, int32(1), entry.height,
		"the height comes from the header node the arrival took off the front, since the block itself has none")
}

// TestSyncManager_AParkedBlockWithNoHeaderKeepsWhatWasReported covers the case
// that has no answer.
//
// Restart recovery rebuilds entries from disk with no header list behind them.
// A height guessed for one of those would be worse than none, so whatever was
// reported stands and the rules that read a height skip it.
func TestSyncManager_AParkedBlockWithNoHeaderKeepsWhatWasReported(t *testing.T) {
	h := newParkWiringHarness(t, true)

	unknownHash := h.blocks[2].MsgBlock().BlockHash()

	// Nothing in the header list and no removed front: the shape a recovered
	// entry is in.
	h.sm.headerMu.Lock()
	delete(h.sm.headerIndex, unknownHash)
	h.sm.headerMu.Unlock()

	require.Equal(t, bsvutil.BlockHeightUnknown,
		h.sm.parkedBlockHeight(bsvutil.BlockHeightUnknown, unknownHash, nil),
		"with no header to read, the reported height stands rather than being invented")

	require.Equal(t, int32(7), h.sm.parkedBlockHeight(7, unknownHash, nil),
		"a height the block did report is always preferred")
}
