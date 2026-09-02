package netsync

import (
	"testing"
	"time"

	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
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

	// The child arrives before its parent and parks.
	h.client.On("GetBlockExists", mock.Anything, &child).Return(false, nil).Once()

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	// The parent is in the chain, but nothing in this node committed it, so no
	// drain was ever triggered — the state a restart leaves behind.
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

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

// TestSyncManager_TheBlockHandlerCarriesARewoundCursorForward proves the other
// call on that ticker is reachable too.
//
// A rewind moves the download cursor back and sends nothing. Everything else
// that issues a getdata does so because sync is moving — a block arrived, a
// headers message arrived, a block committed — and in the regime a rewind exists
// for, sync is not moving: the block that was given up on is the one everything
// else was queued behind. So the ticker is the only thing that carries the
// cursor out again, and unwired the node sits still until the stall detector
// rotates the peer and throws the cursor away.
//
// There is no park in this manager, so the sweep on the adjacent line cannot be
// what asks for the block.
func TestSyncManager_TheBlockHandlerCarriesARewoundCursorForward(t *testing.T) {
	r := newRefusedBlock(t, 66, time.Millisecond)

	require.Nil(t, r.sm.blockPark, "the park sweep must not be able to account for the request below")

	runBlockHandlerWithAFastParkSweep(t, r.sm)

	require.True(t, WaitUntil(func() bool { return r.sm.blockDownloads.RequestedWithin(r.hash, time.Minute) }, 5*time.Second),
		"the block handler's own ticker must carry a rewound cursor forward, or nothing ever asks for the block again")
}

// TestSyncManager_TheBlockHandlerSamplesEveryPeersThroughput proves the sampling
// the frontier race depends on is actually driven in a running node.
//
// isPullingBytes needs two samples a tick apart before it will answer anything
// but "not downloading". If nothing takes those samples the answer is false for
// every peer for ever, the race's "is this owner actually sending it?" test can
// never veto anything, and the veto is dead code that no unit test calling
// frontierRaceTarget directly would notice — because those tests supply the
// samples themselves.
func TestSyncManager_TheBlockHandlerSamplesEveryPeersThroughput(t *testing.T) {
	sm := newRaceManager(t)

	first, _, _ := connectRacePeer(t, 90, 1000)
	second, _, _ := connectRacePeer(t, 91, 1000)

	registerRacePeer(sm, first)
	registerRacePeer(sm, second)
	sm.storeSyncPeer(first, &syncPeerState{})

	sm.quit = make(chan struct{})
	sm.handlerDone = make(chan struct{})
	sm.msgChan = make(chan interface{}, 1)

	go sm.blockHandler()

	t.Cleanup(func() {
		close(sm.quit)
		<-sm.handlerDone
	})

	ticks := func(p *peerpkg.Peer) uint64 {
		state, exists := sm.peerStates.Get(p)
		require.True(t, exists)

		return state.throughputTicks.Load()
	}

	// Two, not one: one sample is a reading with nothing to subtract from, and
	// isPullingBytes refuses to answer until there are two.
	require.True(t, WaitUntil(func() bool { return ticks(first) >= 2 && ticks(second) >= 2 }, 4*frontierCheckInterval),
		"every registered peer must be sampled by the block handler, not only the sync peer")
}
