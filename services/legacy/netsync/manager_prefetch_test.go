package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

// newPrefetchManager builds a minimal SyncManager wired only for the block
// prefetch gate. A budget of 0 leaves prefetch disabled.
func newPrefetchManager(budget int64) *SyncManager {
	sm := &SyncManager{logger: ulogger.TestLogger{}}
	if budget > 0 {
		sm.blockPrefetchBudgetBytes = budget
		sm.blockPrefetchBudget = semaphore.NewWeighted(budget)
	}

	return sm
}

// TestBlockRequested covers the pre-admission gate that stops a misbehaving
// peer from flooding unrequested blocks into the prefetch budget: only blocks we
// actually have an outstanding getdata for are admitted (regtest excepted).
func TestBlockRequested(t *testing.T) {
	hash := chainhash.Hash{0x01}

	newSM := func(params *chaincfg.Params) *SyncManager {
		return &SyncManager{
			logger:      ulogger.TestLogger{},
			chainParams: params,
			peerStates:  txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		}
	}

	t.Run("regtest admits any block", func(t *testing.T) {
		sm := newSM(&chaincfg.RegressionNetParams)
		require.True(t, sm.BlockRequested(&peerpkg.Peer{}, &hash))
	})

	t.Run("requested block is admitted", func(t *testing.T) {
		sm := newSM(&chaincfg.MainNetParams)
		p := &peerpkg.Peer{}
		reqd := expiringmap.New[chainhash.Hash, struct{}](time.Minute)
		reqd.Set(hash, struct{}{})
		sm.peerStates.Set(p, &peerSyncState{requestedBlocks: reqd})

		require.True(t, sm.BlockRequested(p, &hash))
	})

	t.Run("unrequested block from a known peer is rejected", func(t *testing.T) {
		sm := newSM(&chaincfg.MainNetParams)
		p := &peerpkg.Peer{}
		sm.peerStates.Set(p, &peerSyncState{
			requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		})

		require.False(t, sm.BlockRequested(p, &hash))
	})

	t.Run("block from an unknown peer is rejected", func(t *testing.T) {
		sm := newSM(&chaincfg.MainNetParams)
		require.False(t, sm.BlockRequested(&peerpkg.Peer{}, &hash))
	})
}

func TestBlockPrefetchEnabled(t *testing.T) {
	require.False(t, newPrefetchManager(0).BlockPrefetchEnabled())
	require.True(t, newPrefetchManager(100).BlockPrefetchEnabled())
}

// TestUsePrefetchIngestion proves OnBlock's gate: prefetch ingestion is used only
// with a configured budget and off regression net, so regtest keeps the
// synchronous submit-then-query ordering the acceptance tooling depends on.
func TestUsePrefetchIngestion(t *testing.T) {
	withBudget := func(params *chaincfg.Params) *SyncManager {
		return &SyncManager{
			chainParams:              params,
			blockPrefetchBudgetBytes: 100,
			blockPrefetchBudget:      semaphore.NewWeighted(100),
		}
	}

	// Budget disabled → synchronous regardless of network.
	require.False(t, (&SyncManager{chainParams: &chaincfg.MainNetParams}).UsePrefetchIngestion())

	// Budget enabled off regtest → prefetch path.
	require.True(t, withBudget(&chaincfg.MainNetParams).UsePrefetchIngestion())

	// Budget enabled on regtest → synchronous path.
	require.False(t, withBudget(&chaincfg.RegressionNetParams).UsePrefetchIngestion())
}

func TestAcquireBlockPrefetch_Disabled(t *testing.T) {
	sm := newPrefetchManager(0)

	w, err := sm.AcquireBlockPrefetch(context.Background(), nil, 999)
	require.NoError(t, err)
	require.Equal(t, int64(0), w)

	// Release of a zero reservation must be a no-op, not a panic.
	require.NotPanics(t, func() { sm.ReleaseBlockPrefetch(w) })
}

// TestAcquireBlockPrefetch_FloorsTinyBlocks proves a block smaller than the
// per-in-flight floor is charged the floor, not its serialized size, so a flood
// of minimal blocks cannot admit an unbounded number of goroutines within the
// byte budget.
func TestAcquireBlockPrefetch_FloorsTinyBlocks(t *testing.T) {
	sm := newPrefetchManager(4 * minInFlightBlockWeight)

	w, err := sm.AcquireBlockPrefetch(context.Background(), nil, 81) // minimal zero-tx block
	require.NoError(t, err)
	require.Equal(t, int64(minInFlightBlockWeight), w, "a tiny block must be charged the floor weight")
	sm.ReleaseBlockPrefetch(w)
}

// TestAcquireBlockPrefetch_OversizedAdmittedAlone proves a block larger than the
// whole budget is admitted (weight clamped to the budget) rather than
// deadlocking, and that it then consumes the entire budget until released —
// i.e. huge blocks process one at a time, preserving the original backpressure.
func TestAcquireBlockPrefetch_OversizedAdmittedAlone(t *testing.T) {
	const budget = 2 * minInFlightBlockWeight
	sm := newPrefetchManager(budget)

	w, err := sm.AcquireBlockPrefetch(context.Background(), nil, budget*100)
	require.NoError(t, err)
	require.Equal(t, int64(budget), w, "oversized weight must clamp to the budget")

	// Budget is now fully consumed: nothing else can be admitted until release.
	require.False(t, sm.blockPrefetchBudget.TryAcquire(1))

	sm.ReleaseBlockPrefetch(w)
	require.True(t, sm.blockPrefetchBudget.TryAcquire(1))
}

// TestAcquireBlockPrefetch_BlocksUntilReleaseAndCountsWaiter proves the gate
// backpressures the read-loop when the budget is full, registers a waiter while
// blocked (so the stall detector can tell self-backpressure from a slow peer),
// and unblocks on release.
func TestAcquireBlockPrefetch_BlocksUntilReleaseAndCountsWaiter(t *testing.T) {
	const budget = 2 * minInFlightBlockWeight
	sm := newPrefetchManager(budget)

	first, err := sm.AcquireBlockPrefetch(context.Background(), nil, budget) // fills the budget
	require.NoError(t, err)

	acquired := make(chan int64, 1)

	go func() {
		w, e := sm.AcquireBlockPrefetch(context.Background(), nil, minInFlightBlockWeight)
		if e == nil {
			acquired <- w
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

	sm.ReleaseBlockPrefetch(first)

	select {
	case w := <-acquired:
		require.Equal(t, int64(minInFlightBlockWeight), w)
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
	const budget = 2 * minInFlightBlockWeight
	sm := newPrefetchManager(budget)

	_, err := sm.AcquireBlockPrefetch(context.Background(), nil, budget) // fills the budget
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		_, e := sm.AcquireBlockPrefetch(ctx, nil, minInFlightBlockWeight)
		done <- e
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
}

// TestAcquireBlockPrefetch_QuitAbort proves a budget-parked read-loop unblocks on
// peer teardown (its quit channel closing), not only on ctx cancellation —
// mirroring awaitBlockResult, since sp.ctx is the long-lived Init context that
// Stop() does not cancel.
func TestAcquireBlockPrefetch_QuitAbort(t *testing.T) {
	const budget = 2 * minInFlightBlockWeight
	sm := newPrefetchManager(budget)

	_, err := sm.AcquireBlockPrefetch(context.Background(), nil, budget) // fills the budget
	require.NoError(t, err)

	quit := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		// Non-cancellable ctx: only quit can unblock this, proving quit is honored.
		_, e := sm.AcquireBlockPrefetch(context.Background(), quit, minInFlightBlockWeight)
		done <- e
	}()

	require.Eventually(t, func() bool { return sm.blockPrefetchWaiters.Load() == 1 },
		time.Second, 5*time.Millisecond)

	close(quit) // peer torn down

	select {
	case e := <-done:
		require.Error(t, e)
	case <-time.After(time.Second):
		t.Fatal("acquire did not return after the peer quit channel closed")
	}

	require.Eventually(t, func() bool { return sm.blockPrefetchWaiters.Load() == 0 },
		time.Second, 5*time.Millisecond)
}

func TestLocalReadBackpressured(t *testing.T) {
	t.Run("disabled: tracks the block backlog", func(t *testing.T) {
		sm := newPrefetchManager(0)
		require.False(t, sm.localReadBackpressured())

		sm.blockBacklog.Add(1)
		require.True(t, sm.localReadBackpressured())

		sm.blockBacklog.Add(-1)
		require.False(t, sm.localReadBackpressured())
	})

	t.Run("enabled: suppresses on backlog or on a budget waiter", func(t *testing.T) {
		sm := newPrefetchManager(100)
		require.False(t, sm.localReadBackpressured())

		// A queued / mid-validation backlog is self-backpressure: a stale
		// last-block-time then reflects our validation speed, not the peer, so the
		// stall check must be suppressed (a genuinely stalled peer drains the
		// backlog and the check resumes).
		sm.blockBacklog.Add(5)
		require.True(t, sm.localReadBackpressured())
		sm.blockBacklog.Add(-5)
		require.False(t, sm.localReadBackpressured())

		// A read-loop parked in AcquireBlockPrefetch is also self-backpressure.
		sm.blockPrefetchWaiters.Add(1)
		require.True(t, sm.localReadBackpressured())

		sm.blockPrefetchWaiters.Add(-1)
		require.False(t, sm.localReadBackpressured())
	})
}

// TestHandleCheckSyncPeer_PrefetchBackpressure proves the stall detector
// suppresses rotation while the node is backpressured by its own block
// processing — either a read-loop parked on the prefetch budget OR any queued /
// mid-validation backlog — so a healthy peer is not rotated merely because a
// block is slow to validate. It still rotates a genuinely idle stalled peer once
// that self-backpressure clears.
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
			blockPrefetchBudgetBytes: 100,
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

	t.Run("keeps sync peer while a backlog is draining (slow local validation)", func(t *testing.T) {
		sp := &peerpkg.Peer{}
		sm := newSyncManager(sp, newStalledState())

		// Blocks queued / mid-validation with no read-loop parked: a stale
		// last-block-time reflects our validation speed, not the peer. The healthy
		// peer must be kept (rotation would panic in this minimal SyncManager).
		sm.blockBacklog.Add(3)

		require.NotPanics(t, func() { sm.handleCheckSyncPeer() })
		require.Equal(t, sp, sm.loadSyncPeer())
	})

	t.Run("rotates a genuinely idle stalled peer (no backlog, no waiters)", func(t *testing.T) {
		sp := &peerpkg.Peer{}
		sm := newSyncManager(sp, newStalledState())

		// Nothing queued and no read-loop parked: the stale last-block-time is the
		// peer's fault, so rotation runs (and panics in this minimal SyncManager,
		// which proves it ran rather than being suppressed).
		require.Panics(t, func() { sm.handleCheckSyncPeer() })
	})
}
