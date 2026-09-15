package netsync

import (
	"testing"

	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestSyncManager_AParkedBlockRecordsWhateverHeightWasReported pins the parked
// entry's height field to the one source left once the header list is gone:
// whatever height the queue message itself reported, unconditionally.
//
// The queue message's height comes from the decoded block, and a legacy block
// is built by bsvutil.NewBlockFromBlockAndBytes with no height set, so it
// arrives as BlockHeightUnknown. There used to be a fallback here that read a
// height out of the in-flight header list for exactly that case. That list is
// gone, and nothing replaced the fallback: a block parked with no reported
// height now simply carries none, which is the same honest state restart
// recovery has always left those entries in, extended to every block whose
// wire message did not report one.
func TestSyncManager_AParkedBlockRecordsWhateverHeightWasReported(t *testing.T) {
	h := newParkWiringHarness(t, true)

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	msgBlock := h.blocks[0].MsgBlock()

	require.Equal(t, int32(-1), bsvutil.BlockHeightUnknown,
		"the premise: a legacy-delivered block reports no height at all")

	require.NoError(t, h.deliverBlock(t, msgBlock, bsvutil.BlockHeightUnknown))
	require.Equal(t, 1, h.sm.blockPark.Len())

	entry, ok := h.sm.blockPark.Take(msgBlock.BlockHash())
	require.True(t, ok)
	require.Equal(t, bsvutil.BlockHeightUnknown, entry.height,
		"with no header list to recover a height from, the reported height stands even when it is unknown")
}

// TestSyncManager_AParkedBlockKeepsAReportedHeight is the other half: a height
// the block DID report is what the entry records, exactly as reported.
func TestSyncManager_AParkedBlockKeepsAReportedHeight(t *testing.T) {
	h := newParkWiringHarness(t, true)

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	msgBlock := h.blocks[2].MsgBlock()

	require.NoError(t, h.deliverBlock(t, msgBlock, 7))

	entry, ok := h.sm.blockPark.Take(msgBlock.BlockHash())
	require.True(t, ok)
	require.Equal(t, int32(7), entry.height, "a height the block did report is recorded as reported")
}
