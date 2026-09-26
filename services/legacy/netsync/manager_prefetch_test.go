package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

// newPrefetchManager builds a minimal SyncManager wired only for the block
// prefetch admission gate, at the given slot capacity.
func newPrefetchManager(capacity int64) *SyncManager {
	sm := &SyncManager{logger: ulogger.TestLogger{}}
	sm.blockPrefetchBudgetSlots = capacity
	sm.blockPrefetchBudget = semaphore.NewWeighted(capacity)
	sm.inFlightBlocks = make(map[chainhash.Hash]*inFlightBlock)

	return sm
}

// TestPeerStateResolvingPrimary covers the stream→primary resolution walk shared
// by handleBlockMsg/handleHeadersMsg/handleInvMsg/BlockRequested: a registered
// peer resolves to itself, an unregistered stream sub-peer resolves to its
// association's registered primary, and an unknown peer resolves to nothing.
func TestPeerStateResolvingPrimary(t *testing.T) {
	newSM := func() *SyncManager {
		return &SyncManager{
			logger:     ulogger.TestLogger{},
			peerStates: txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		}
	}

	t.Run("registered primary resolves to itself", func(t *testing.T) {
		sm := newSM()
		primary := &peerpkg.Peer{}
		want := &peerSyncState{}
		sm.peerStates.Set(primary, want)

		state, resolved, exists := sm.peerStateResolvingPrimary(primary)
		require.True(t, exists)
		require.Same(t, primary, resolved)
		require.Same(t, want, state)
	})

	t.Run("stream sub-peer resolves to its registered primary", func(t *testing.T) {
		sm := newSM()
		primary := &peerpkg.Peer{}
		want := &peerSyncState{}
		sm.peerStates.Set(primary, want)

		stream := &peerpkg.Peer{}
		stream.SetAssociation(peerpkg.NewAssociation([]byte{0x01}, primary))

		state, resolved, exists := sm.peerStateResolvingPrimary(stream)
		require.True(t, exists)
		require.Same(t, primary, resolved)
		require.Same(t, want, state)
	})

	t.Run("unknown peer resolves to nothing", func(t *testing.T) {
		sm := newSM()
		peer := &peerpkg.Peer{}

		state, resolved, exists := sm.peerStateResolvingPrimary(peer)
		require.False(t, exists)
		require.Same(t, peer, resolved)
		require.Nil(t, state)
	})
}

// TestAcquireBlockPrefetch_BlocksUntilReleaseAndCountsWaiter proves the gate
// backpressures the read-loop when the budget is full, registers a waiter while
// blocked (so the stall detector can tell self-backpressure from a slow peer),
// and unblocks on release.
func TestAcquireBlockPrefetch_BlocksUntilReleaseAndCountsWaiter(t *testing.T) {
	sm := newPrefetchManager(1)

	require.NoError(t, sm.AcquireBlockPrefetch(context.Background(), chainhash.Hash{0x01})) // fills the budget

	acquired := make(chan struct{}, 1)

	go func() {
		if err := sm.AcquireBlockPrefetch(context.Background(), chainhash.Hash{0x02}); err == nil {
			acquired <- struct{}{}
		}
	}()

	// The second acquire must block on the full budget and register as a waiter.
	require.Eventually(t, func() bool { return sm.blockPrefetchWaiters.Load() == 1 },
		time.Second, 5*time.Millisecond)
	require.True(t, sm.localReadBackpressured())

	select {
	case <-acquired:
		t.Fatal("second acquire returned before the budget was released")
	case <-time.After(50 * time.Millisecond):
	}

	sm.ReleaseBlockPrefetch(chainhash.Hash{0x01})

	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("second acquire did not unblock after release")
	}

	require.Eventually(t, func() bool { return sm.blockPrefetchWaiters.Load() == 0 },
		time.Second, 5*time.Millisecond)
	require.False(t, sm.localReadBackpressured())
}

// TestAcquireBlockPrefetch_CtxCancel proves a read-loop blocked on the budget is
// released (with nothing reserved) when its context is cancelled on shutdown.
func TestAcquireBlockPrefetch_CtxCancel(t *testing.T) {
	sm := newPrefetchManager(1)

	require.NoError(t, sm.AcquireBlockPrefetch(context.Background(), chainhash.Hash{0x01})) // fills the budget

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		done <- sm.AcquireBlockPrefetch(ctx, chainhash.Hash{0x02})
	}()

	require.Eventually(t, func() bool { return sm.blockPrefetchWaiters.Load() == 1 },
		time.Second, 5*time.Millisecond)

	cancel()

	select {
	case e := <-done:
		require.Error(t, e)
	case <-time.After(time.Second):
		t.Fatal("acquire did not return after context cancellation")
	}

	require.Eventually(t, func() bool { return sm.blockPrefetchWaiters.Load() == 0 },
		time.Second, 5*time.Millisecond)

	// A cancelled acquire reserved nothing, so it must not leak its hash in the
	// dedup set: the two halves of the gate stay paired even on the failure path.
	sm.inFlightBlocksMu.Lock()
	_, leaked := sm.inFlightBlocks[chainhash.Hash{0x02}]
	sm.inFlightBlocksMu.Unlock()
	require.False(t, leaked, "a cancelled acquire must remove the hash it inserted before parking")
}

// TestAcquireBlockPrefetch_DeduplicatesInFlight proves the in-flight-by-hash set
// is the dedup half of the admission gate: a second acquire of a hash already in
// flight is dropped with the benign ErrDuplicateBlockInFlight sentinel WITHOUT
// reserving a slot, the set keeps exactly one copy, and the hash becomes
// acquirable again only after release.
func TestAcquireBlockPrefetch_DeduplicatesInFlight(t *testing.T) {
	sm := newPrefetchManager(4)
	h := chainhash.Hash{0x01}

	// First copy of H is admitted and reserves a slot.
	require.NoError(t, sm.AcquireBlockPrefetch(context.Background(), h))

	// Second copy of H is a duplicate: benign sentinel, nothing reserved.
	dupErr := sm.AcquireBlockPrefetch(context.Background(), h)
	require.ErrorIs(t, dupErr, ErrDuplicateBlockInFlight)

	// The duplicate reserved no slot: everything but the single admitted copy is
	// still free (probe with TryAcquire, then hand it straight back).
	require.True(t, sm.blockPrefetchBudget.TryAcquire(3))
	sm.blockPrefetchBudget.Release(3)

	// The set still holds exactly one copy of H.
	sm.inFlightBlocksMu.Lock()
	_, present := sm.inFlightBlocks[h]
	require.True(t, present)
	require.Len(t, sm.inFlightBlocks, 1)
	sm.inFlightBlocksMu.Unlock()

	// Release H: the hash leaves the set alongside the budget, so a fresh copy of
	// H is admissible again.
	sm.ReleaseBlockPrefetch(h)

	sm.inFlightBlocksMu.Lock()
	require.Empty(t, sm.inFlightBlocks)
	sm.inFlightBlocksMu.Unlock()

	require.NoError(t, sm.AcquireBlockPrefetch(context.Background(), h))
	sm.ReleaseBlockPrefetch(h)
}

// TestAcquireBlockPrefetch_DuplicateDoesNotConsumeBudget proves a duplicate does
// not eat budget: a DIFFERENT hash G can still be admitted for the slot the
// duplicate of H would have (wrongly) consumed. This is the regression the dedup
// set exists to prevent — N copies of one requested block filling the whole
// budget and parking every peer's read-loop.
func TestAcquireBlockPrefetch_DuplicateDoesNotConsumeBudget(t *testing.T) {
	sm := newPrefetchManager(2)
	h := chainhash.Hash{0x0a}
	g := chainhash.Hash{0x0b}

	// H takes one slot.
	require.NoError(t, sm.AcquireBlockPrefetch(context.Background(), h))

	// A duplicate of H must not reserve the remaining slot.
	dupErr := sm.AcquireBlockPrefetch(context.Background(), h)
	require.ErrorIs(t, dupErr, ErrDuplicateBlockInFlight)

	// So a different hash G can still be admitted against the remaining slot.
	require.NoError(t, sm.AcquireBlockPrefetch(context.Background(), g))

	sm.ReleaseBlockPrefetch(h)
	sm.ReleaseBlockPrefetch(g)
}

// TestLocalReadBackpressured proves the suppression is exactly a read-loop
// parked on the prefetch budget: false with no waiter, true while one is
// parked, false again once it clears.
func TestLocalReadBackpressured(t *testing.T) {
	sm := newPrefetchManager(100)
	require.False(t, sm.localReadBackpressured())

	sm.blockPrefetchWaiters.Add(1)
	require.True(t, sm.localReadBackpressured())

	sm.blockPrefetchWaiters.Add(-1)
	require.False(t, sm.localReadBackpressured())
}

// TestHandleCheckSyncPeer_PrefetchBackpressure proves the stall detector
// suppresses rotation while the node is backpressured by its own block
// processing — a read-loop parked on the prefetch budget — so a healthy peer is
// not rotated merely because a block is slow to validate. It still rotates a
// genuinely idle stalled peer once that self-backpressure clears.
//
// This used to also cover a queued / mid-validation backlog suppressing
// rotation on its own, gated on a lastChainProgress staleness check; both are
// gone with the decoded block queue (blockBacklog, lastChainProgress), so the
// only self-backpressure signal left is the prefetch waiter count.
func TestHandleCheckSyncPeer_PrefetchBackpressure(t *testing.T) {
	newStalledState := func() *syncPeerState {
		return &syncPeerState{
			lastBlockTime: time.Now().Add(-10 * time.Minute),
			ticks:         1,
			violations:    maxNetworkViolations - 1,
		}
	}

	newSyncManager := func(sp *peerpkg.Peer, sps *syncPeerState) *SyncManager {
		sm := &SyncManager{
			logger:                   ulogger.TestLogger{},
			peerStates:               txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
			minSyncPeerNetworkSpeed:  51200,
			blockPrefetchBudgetSlots: 100,
			blockPrefetchBudget:      semaphore.NewWeighted(100),
		}
		sm.storeSyncPeer(sp, sps)
		sm.headersFirstMode.Store(false)
		sm.peerStates.Set(sp, &peerSyncState{})

		return sm
	}

	t.Run("keeps sync peer while a read-loop is blocked on prefetch budget", func(t *testing.T) {
		sp := &peerpkg.Peer{}
		sm := newSyncManager(sp, newStalledState())

		sm.blockPrefetchWaiters.Add(1) // read-loop parked in AcquireBlockPrefetch

		require.NotPanics(t, func() { sm.handleCheckSyncPeer() })
		require.Equal(t, sp, sm.loadSyncPeer())
	})

	t.Run("rotates a genuinely idle stalled peer (no waiters)", func(t *testing.T) {
		sp := &peerpkg.Peer{}
		sm := newSyncManager(sp, newStalledState())

		// Nothing queued and no read-loop parked: the stale last-block-time is the
		// peer's fault, so rotation runs (and panics in this minimal SyncManager,
		// which proves it ran rather than being suppressed).
		require.Panics(t, func() { sm.handleCheckSyncPeer() })
	})
}
