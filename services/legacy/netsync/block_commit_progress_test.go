package netsync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestSyncManager_AParkedBlockIsNotChainProgress is the liveness half of taking
// the park off the commit goroutine.
//
// localReadBackpressured suppresses the sync-peer stall check while the pipeline
// is still producing, and it used to read "a queue message finished" as
// producing. Once the park started deferring blocks rather than committing them,
// a node doing nothing but parking looked exactly like a node committing
// steadily. The stamp is shared across peers, so one peer's out-of-order blocks
// held that suppression open over another peer's silence.
func TestSyncManager_AParkedBlockIsNotChainProgress(t *testing.T) {
	h := newParkWiringHarness(t, true)

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	msgBlock := h.blocks[0].MsgBlock()
	hash := msgBlock.BlockHash()

	h.sm.blockDownloads.Add(h.peer, hash)

	stale := time.Now().Add(-time.Hour).UnixNano()
	h.sm.lastChainProgress.Store(stale)

	h.sm.blockBacklog.Add(1)

	h.sm.consumeQueuedBlock(&blockQueueMsg{
		block:       msgBlock,
		blockHash:   hash,
		blockHeight: 1,
		peer:        h.peer,
	})

	require.Equal(t, 1, h.sm.blockPark.Len(), "the block parked, which is the case under test")
	require.Zero(t, h.sm.blockBacklog.Load(), "the queue slot is still given back")
	require.Equal(t, stale, h.sm.lastChainProgress.Load(),
		"parking a block must not refresh the signal that suppresses the stall check")
}

// TestSyncManager_ACommittedBlockIsChainProgress is the other half: a block that
// actually joins the chain has to refresh the signal, or a node committing one
// slow block would rotate a peer that is doing nothing wrong.
func TestSyncManager_ACommittedBlockIsChainProgress(t *testing.T) {
	h := newParkWiringHarness(t, true)

	h.sm.lastChainProgress.Store(time.Now().Add(-time.Hour).UnixNano())

	before := h.sm.lastChainProgress.Load()

	// committed is what handleBlockMsg sets when HandleBlockDirect returns
	// without error, and it is the only thing consumeQueuedBlock reads.
	h.sm.blockBacklog.Add(1)
	h.sm.consumeQueuedBlock(&blockQueueMsg{
		block:     h.blocks[0].MsgBlock(),
		blockHash: h.blocks[0].MsgBlock().BlockHash(),
		peer:      h.peer,
		committed: true,
	})

	require.Greater(t, h.sm.lastChainProgress.Load(), before,
		"a block joining the chain is the one thing that counts as progress")
}

// TestSyncManager_ADrainCommitIsChainProgress covers the commit that never
// passes through the block queue.
//
// A node working through a backlog of parked blocks commits them from the drain,
// not from an arrival. Without a stamp there, a node making real progress would
// look stalled for as long as its park kept it busy.
func TestSyncManager_ADrainCommitIsChainProgress(t *testing.T) {
	h := newParkWiringHarness(t, true)

	child := h.blocks[1].MsgBlock().BlockHash()

	h.client.On("GetBlockExists", mock.Anything, &child).Return(false, nil).Once()

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len(), "the drain needs something parked to commit")

	// The block is now in the chain, which HandleBlockDirect answers with a nil
	// error, so the drain counts it as committed. That is the same route the
	// park's own commit-walk test uses to drive a successful drain.
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

	stale := time.Now().Add(-time.Hour).UnixNano()
	h.sm.lastChainProgress.Store(stale)

	h.sm.drainParkedDescendants(h.blocks[1].MsgBlock().Header.PrevBlock)

	require.Zero(t, h.sm.blockPark.Len(), "the drain committed the parked block")
	require.Greater(t, h.sm.lastChainProgress.Load(), stale,
		"a parked block joining the chain is progress even though no queue message finished")
}
