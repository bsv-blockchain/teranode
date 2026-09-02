package netsync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestSyncManager_ARedeliveredCopyThatWasTheFrontKeepsTheBlockReachable is
// about the one header node a parked block may be the last holder of.
//
// The header list is the only thing that fetches anything in headers-first mode:
// the getblocks the drop paths send is thrown away by processInvMsg while that
// mode is on. So a block whose header has left the list, and which is then given
// up on, is only ever asked for again because the park kept that header node and
// the rewind puts it back.
//
// The awkward case is a block delivered twice. The first copy arrives while the
// block is somewhere in the middle of the list, so it takes no header off the
// front and the park records nothing. Then the blocks in front of it are
// committed, it becomes the front, and a second copy turns up — a peer racing
// the frontier, or a getdata answered twice. That copy DOES take the header off
// the front, and because the block is already parked the second Park call is the
// only place that node can be recorded. Drop it there and the node is
// unreachable: not in the list, not in the index, and not on the entry, so when
// the block is later given up on there is nothing to rewind to and nothing ever
// asks for it again.
//
// Asserted as the end state that matters — after the block is given up on it is
// back in the download walk — rather than as the contents of the entry.
func TestSyncManager_ARedeliveredCopyThatWasTheFrontKeepsTheBlockReachable(t *testing.T) {
	h := newParkWiringHarness(t, true)

	first := h.blocks[0].MsgBlock().BlockHash()
	child := h.blocks[1].MsgBlock().BlockHash()

	// Nothing is in the chain, so every delivery below is an orphan and parks.
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	// The first copy arrives while the block is behind another header, so it
	// takes nothing off the front and the park has no header node for it.
	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	h.sm.blockPark.mu.Lock()
	require.Nil(t, h.sm.blockPark.entries[child].removedFront,
		"the first copy was not the front, so there is no header node to record yet")
	h.sm.blockPark.mu.Unlock()

	// The block in front of it arrives and moves the list on, so our block is
	// now the front.
	h.sm.advanceHeaderListFor(first)

	h.sm.headerMu.Lock()
	front := h.sm.headerList.Front().Value.(*headerNode)
	h.sm.headerMu.Unlock()

	require.Equal(t, child.String(), front.hash.String())

	// A second copy of the same block turns up. It takes the header off the
	// front, and the park is where that node has to be kept.
	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len(), "a re-delivered copy of a parked block is not a second block")

	h.sm.headerMu.Lock()
	_, indexed := h.sm.headerIndex[child]
	listLen := h.sm.headerList.Len()
	h.sm.headerMu.Unlock()

	require.False(t, indexed, "the second copy took the header out of the list and out of the index")
	require.Equal(t, 1, listLen, "only the block after it is left, so nothing but the carried node can bring this one back")

	// The parent never arrives and the block is given up on.
	h.sm.sweepParkedBlocks(time.Now().Add(parkEntryTTL + time.Second))

	require.Zero(t, h.sm.blockPark.Len())

	h.sm.headerMu.Lock()
	startHeader := h.sm.startHeader
	h.sm.headerMu.Unlock()

	require.NotNil(t, startHeader,
		"a block given up on must be back in the download walk; without the header node the second copy took off the front there is nothing to rewind to and nothing ever asks for it again")
	require.Equal(t, child.String(), startHeader.Value.(*headerNode).hash.String())
}

// TestSyncManager_ALaterCopyOfAParkedFrontBlockDoesNotWipeItsHeaderNode is the
// same rule in the other order, and the order is the whole of it.
//
// The test above has the first copy arrive from the middle of the list and the
// second one as the front, so the second Park call is where the header node has
// to be recorded. Reverse them and the danger reverses with them: the FIRST copy
// was the front and carried the only header node anybody holds, and the second
// copy — a peer racing the frontier, or a getdata answered twice — arrives when
// the block is no longer the front and so brings no node with it. Refreshing the
// parked entry from that copy must not overwrite what the first one recorded.
//
// Wiping it costs the block for good: not in the list, not in the index, and
// nothing on the entry, so when the parent never comes and the block is given up
// on there is nothing to rewind to and nothing ever asks for it again.
func TestSyncManager_ALaterCopyOfAParkedFrontBlockDoesNotWipeItsHeaderNode(t *testing.T) {
	h := newParkWiringHarness(t, true)

	front := h.blocks[0].MsgBlock().BlockHash()

	// Nothing is in the chain, so every delivery below is an orphan and parks.
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	// The first copy IS the front, so its header is taken off the list and
	// unindexed before the park sees it, and the parked entry is the only thing
	// still holding that node.
	require.Equal(t, front, h.parkFrontBlock(t))

	h.sm.blockPark.mu.Lock()
	require.NotNil(t, h.sm.blockPark.entries[front].removedFront,
		"the first copy was the front, so the park has to be holding its header node")
	h.sm.blockPark.mu.Unlock()

	h.sm.headerMu.Lock()
	nowTheFront := h.sm.headerList.Front().Value.(*headerNode)
	h.sm.headerMu.Unlock()

	require.Equal(t, h.blocks[1].MsgBlock().BlockHash().String(), nowTheFront.hash.String(),
		"the block after it is the front now, so a second copy takes nothing off the list")

	getDataBefore := h.rec.getDataCount()

	// The second copy turns up. It is not the front, so there is no header node
	// to carry, and the refresh must leave the recorded one alone.
	require.NoError(t, h.deliver(t, 0))
	require.Equal(t, 1, h.sm.blockPark.Len(), "a re-delivered copy of a parked block is not a second block")

	// The parent never arrives and the block is given up on.
	h.sm.sweepParkedBlocks(time.Now().Add(parkEntryTTL + time.Second))

	require.Zero(t, h.sm.blockPark.Len())

	h.requireBackInTheWalk(t, front, getDataBefore)
}
