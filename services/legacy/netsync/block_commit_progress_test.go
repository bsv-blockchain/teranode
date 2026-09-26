package netsync

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestSyncManager_ADrainCommitIsChainProgress covers the commit that never
// passes through a decoded block message at all any more: a node working
// through a backlog of parked blocks commits them from the drain, and that
// must still register as progress (noteChainProgress -> commitRate), or a node
// making real progress purely off its park would look stalled for as long as
// its park kept it busy.
//
// This used to be two more tests apart from this one: a decoded arrival
// (consumeQueuedBlock, reading a field called lastChainProgress) not
// refreshing progress while parking, and a decoded arrival refreshing it once
// committed. Both are gone along with the decoded-block consumer itself
// (manager.go) — every arrival now goes through pipelineBlockSink and
// handleBlockOnDiskMsg, whose only commit route is the same parkedTail this
// test already drives, so there is no separate "message finished" path left
// to pin apart from the drain one below.
func TestSyncManager_ADrainCommitIsChainProgress(t *testing.T) {
	h := newParkWiringHarness(t, true)
	h.sm.commitRate = newCommitRateTracker()

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len(), "the drain needs something parked to commit")

	// The block is now in the chain, which HandleBlockDirect answers with a nil
	// error, so the drain counts it as committed. That is the same route the
	// park's own commit-walk test uses to drive a successful drain.
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

	before := h.sm.commitRate.count

	h.sm.drainParkedDescendants(h.blocks[1].MsgBlock().Header.PrevBlock)

	require.Zero(t, h.sm.blockPark.Len(), "the drain committed the parked block")
	require.Greater(t, h.sm.commitRate.count, before,
		"a parked block joining the chain is progress even though no queue message finished")
}
