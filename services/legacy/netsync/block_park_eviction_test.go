package netsync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestSyncManager_ABlockTheChainHasGonePastIsDropped is the first of the two
// rules that replaced the thirty-minute timer.
//
// A block below a block this node has already committed cannot be a link in any
// chain it is building, and its parent is missing so it cannot be a sibling
// either. Nothing will ever ask for it, so it goes, and the walk is deliberately
// NOT rewound onto it: re-requesting a block the chain no longer needs is the
// waste the timer used to generate.
func TestSyncManager_ABlockTheChainHasGonePastIsDropped(t *testing.T) {
	h := newParkWiringHarness(t, true)

	child := h.blocks[1].MsgBlock().BlockHash()

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())
	require.Positive(t, h.sm.blockPark.Bytes())

	// Nothing has been committed yet, so the node cannot say what it has gone
	// past and keeps everything.
	h.sm.sweepParkedBlocks(time.Now().Add(parkStuckThreshold + time.Second))
	require.Equal(t, 1, h.sm.blockPark.Len(),
		"a node that has committed nothing must keep what it has downloaded")

	// The chain moves past it. deliver stamps the entry at height 2.
	h.sm.noteCommittedHeight(5)

	h.sm.sweepParkedBlocks(time.Now().Add(parkStuckThreshold + time.Second))

	require.Zero(t, h.sm.blockPark.Len(), "a block the chain has gone past must be dropped")
	require.Zero(t, h.sm.blockPark.Bytes(), "and give its bytes back")
	require.NotContains(t, parkDirEntries(t, h.parkDir), child.String()+".msgBlock",
		"and its blob must go, or the byte budget is holding disk nothing tracks")

	h.sm.headerMu.Lock()
	startHeader := h.sm.startHeader
	h.sm.headerMu.Unlock()

	require.Nil(t, startHeader,
		"the walk must NOT be rewound onto a block the chain has gone past; asking for it again is the waste this replaces")
}

// TestSyncManager_ABlockBehindAnInvalidParentIsDroppedNotCommitted is the second
// rule, and it closes a hole rather than only replacing the timer.
//
// The sweep used to ask whether a parent was STORED. Invalidation is a flag on
// the row and not a delete, so a parent this node has rejected still exists, and
// the sweep would commit its descendant on the strength of that. Asking for the
// header answers both questions in one round trip.
func TestSyncManager_ABlockBehindAnInvalidParentIsDroppedNotCommitted(t *testing.T) {
	h := newParkWiringHarness(t, true)

	child := h.blocks[1].MsgBlock().BlockHash()

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	// The parent turns up, stored and rejected.
	h.chainHoldsInvalid(t, h.blocks[1].MsgBlock().Header.PrevBlock)

	h.sm.sweepParkedBlocks(time.Now().Add(parkStuckThreshold + time.Second))

	require.Zero(t, h.sm.blockPark.Len(), "a block behind a rejected parent can never be committed, so it must not be held")
	require.Zero(t, h.sm.blockPark.Bytes())
	require.NotContains(t, parkDirEntries(t, h.parkDir), child.String()+".msgBlock")

	_, failed := h.sm.recentlyFailedBlocks.Get(child)
	require.True(t, failed,
		"and it must be written off, so its own descendants are short-circuited rather than each discovering this separately")

	require.False(t, h.rec.wasRejected(child),
		"the peer sent a block whose parent WE rejected, which says nothing about the peer")

	h.sm.headerMu.Lock()
	startHeader := h.sm.startHeader
	h.sm.headerMu.Unlock()

	require.Nil(t, startHeader, "nor is it worth asking for again")
}

// TestSyncManager_TheSweepStillCommitsBehindAValidParent is the control for the
// test above: the validity check must not have turned the commit path off.
func TestSyncManager_TheSweepStillCommitsBehindAValidParent(t *testing.T) {
	h := newParkWiringHarness(t, true)

	child := h.blocks[1].MsgBlock().BlockHash()

	h.client.On("GetBlockExists", mock.Anything, &child).Return(false, nil).Once()

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)
	h.chainHolds(t, h.blocks[1].MsgBlock().Header.PrevBlock)

	h.sm.sweepParkedBlocks(time.Now().Add(parkStuckThreshold + time.Second))

	require.Zero(t, h.sm.blockPark.Len(), "a valid parent still means the parked block is committed")

	_, failed := h.sm.recentlyFailedBlocks.Get(child)
	require.False(t, failed, "committed, not written off")
}
