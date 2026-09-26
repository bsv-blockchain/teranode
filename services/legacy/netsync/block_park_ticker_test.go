package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// runBlockHandlerWithAFastParkSweep starts the real block handler with the park
// sweep's ticker turned down from thirty seconds to something a test can wait
// for, and stops it again at the end of the test.
//
// The interval is the only thing shortened. Everything else on that ticker —
// which calls it makes, in which order, on which goroutine — is exactly what a
// running node does, which is the whole point of these two tests.
func runBlockHandlerWithAFastParkSweep(t *testing.T, sm *SyncManager) {
	t.Helper()

	previous := parkSweepInterval
	parkSweepInterval = 10 * time.Millisecond

	sm.quit = make(chan struct{})
	sm.handlerDone = make(chan struct{})
	sm.msgChan = make(chan interface{}, 1)

	go sm.blockHandler()

	t.Cleanup(func() {
		close(sm.quit)
		<-sm.handlerDone

		parkSweepInterval = previous
	})
}

// TestSyncManager_TheBlockHandlerRunsTheParkSweep proves the sweep is reachable
// in a running node rather than only when a test calls it directly.
//
// The sweep is the ONLY thing that ever commits a block recovered from disk
// after a restart whose parent was already in the chain when the node started:
// that block never sees a commit event for its parent, so no drain is ever
// triggered for it. Unwired, every such block sits until its TTL evicts it and
// the whole download is thrown away — and nothing in the suite noticed, because
// every sweep test supplied its own call.
func TestSyncManager_TheBlockHandlerRunsTheParkSweep(t *testing.T) {
	h := newParkWiringHarness(t, true)

	child := h.blocks[1].MsgBlock().BlockHash()

	// The child arrives before its parent and parks. The streaming route
	// (handleBlockOnDiskMsg) never calls GetBlockExists while parking — only a
	// real commit attempt does — so nothing needs scripting for the arrival
	// itself; the parent is in the chain, but nothing in this node committed
	// it, so no drain was ever triggered — the state a restart leaves behind.
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	// And usable, not merely present: the sweep asks that as one question now,
	// because invalidation is a flag on the row rather than a delete, so a
	// parent this node has rejected still exists.
	h.chainHolds(t, h.blocks[1].MsgBlock().Header.PrevBlock)

	// And the block has been waiting long enough for the sweep to spend a chain
	// lookup on it. The ticker passes the real clock, so the age has to be real.
	h.sm.blockPark.mu.Lock()
	for _, entry := range h.sm.blockPark.entries {
		entry.parkedAt = time.Now().Add(-parkStuckThreshold - time.Second)
	}
	h.sm.blockPark.mu.Unlock()

	runBlockHandlerWithAFastParkSweep(t, h.sm)

	require.True(t, WaitUntil(func() bool { return h.sm.blockPark.Len() == 0 }, 5*time.Second),
		"the block handler's own ticker must run the park sweep, or a restart-recovered block is never committed by anything")

	_, failed := h.sm.recentlyFailedBlocks.Get(child)
	require.False(t, failed, "the sweep must have committed the block, not given up on it")
}

// TestSyncManager_TheBlockHandlerAsksForAGivenUpBlockAgain proves the other
// call on that ticker is reachable too.
//
// Nothing puts a given-up block back anywhere any more — the wanted-range pass
// recomputes what it wants and who owes it from the committed tip on every
// call, so a block that is still in range and unowed is simply found again the
// next time a pass runs. Everything else that issues a getdata does so because
// sync is moving — a block arrived, a headers message arrived, a block
// committed — and in the regime this covers, sync is not moving: the block that
// was given up on is the one everything else was queued behind. So the park
// sweep's ticker is what carries a fresh pass out on its own, and unwired the
// node would sit still until the stall detector rotated the peer.
//
// There is no park in this manager, so the sweep on the adjacent line cannot be
// what asks for the block.
func TestSyncManager_TheBlockHandlerAsksForAGivenUpBlockAgain(t *testing.T) {
	var nonce uint32

	anchor := chainhash.Hash{0xc9}
	msg, hashes := linkedHeaders(anchor, 3, &nonce)

	sm := schedulerManager(t)

	peer, _ := schedulerPeer(t, sm, 66, 1000)
	sm.storeSyncPeer(peer, &syncPeerState{})

	seedFetchHeaders(t, sm, peer, anchor, msg)

	require.Nil(t, sm.blockPark, "the park sweep must not be able to account for the request below")

	runBlockHandlerWithAFastParkSweep(t, sm)

	require.True(t, WaitUntil(func() bool { return sm.blockDownloads.RequestedWithin(hashes[0], time.Minute) }, 5*time.Second),
		"the block handler's own ticker must ask for the block again, or nothing ever does once sync has stopped moving")
}
