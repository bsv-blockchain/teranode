package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
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
		sm.inFlightBlocks = make(map[chainhash.Hash]struct{})
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

// TestUsePrefetchIngestion proves OnBlock's gate across the full budget {0,
// positive} × net {mainnet, testnet, regtest} matrix: prefetch ingestion is used
// only with a configured budget and off regression net, so regtest keeps the
// synchronous submit-then-query ordering the acceptance tooling depends on. It
// asserts the gate tracks the shared peerpkg.UseBlockPrefetchIngestion predicate
// so the sync manager and the read-loop's shouldArmProcessingTimer cannot drift.
func TestUsePrefetchIngestion(t *testing.T) {
	budgets := []int64{0, 100}
	params := []*chaincfg.Params{&chaincfg.MainNetParams, &chaincfg.TestNetParams, &chaincfg.RegressionNetParams}

	for _, budget := range budgets {
		for _, p := range params {
			sm := &SyncManager{chainParams: p}
			if budget > 0 {
				sm.blockPrefetchBudgetBytes = budget
				sm.blockPrefetchBudget = semaphore.NewWeighted(budget)
			}

			want := budget > 0 && p.Net != wire.RegTestNet

			// The shared predicate is true exactly for a positive budget off regtest.
			require.Equal(t, want, peerpkg.UseBlockPrefetchIngestion(budget, p.Net))

			// The gate tracks the shared predicate for the manager's own budget/net.
			require.Equal(t, peerpkg.UseBlockPrefetchIngestion(budget, p.Net), sm.UsePrefetchIngestion())
		}
	}
}

func TestAcquireBlockPrefetch_Disabled(t *testing.T) {
	sm := newPrefetchManager(0)

	w, err := sm.AcquireBlockPrefetch(context.Background(), nil, chainhash.Hash{0x01}, 999)
	require.NoError(t, err)
	require.Equal(t, int64(0), w)

	// Release of a zero reservation must be a no-op, not a panic.
	require.NotPanics(t, func() { sm.ReleaseBlockPrefetch(chainhash.Hash{0x01}, w) })
}

// TestAcquireBlockPrefetch_FloorsTinyBlocks proves a block smaller than the
// per-in-flight floor is charged the floor, not its serialized size, so a flood
// of minimal blocks cannot admit an unbounded number of goroutines within the
// byte budget.
func TestAcquireBlockPrefetch_FloorsTinyBlocks(t *testing.T) {
	sm := newPrefetchManager(4 * minInFlightBlockWeight)

	w, err := sm.AcquireBlockPrefetch(context.Background(), nil, chainhash.Hash{0x01}, 81) // minimal zero-tx block
	require.NoError(t, err)
	require.Equal(t, int64(minInFlightBlockWeight), w, "a tiny block must be charged the floor weight")
	sm.ReleaseBlockPrefetch(chainhash.Hash{0x01}, w)
}

// TestAcquireBlockPrefetch_OversizedAdmittedAlone proves a block larger than the
// whole budget is admitted (weight clamped to the budget) rather than
// deadlocking, and that it then consumes the entire budget until released —
// i.e. huge blocks process one at a time, preserving the original backpressure.
func TestAcquireBlockPrefetch_OversizedAdmittedAlone(t *testing.T) {
	const budget = 2 * minInFlightBlockWeight
	sm := newPrefetchManager(budget)

	w, err := sm.AcquireBlockPrefetch(context.Background(), nil, chainhash.Hash{0x01}, budget*100)
	require.NoError(t, err)
	require.Equal(t, int64(budget), w, "oversized weight must clamp to the budget")

	// Budget is now fully consumed: nothing else can be admitted until release.
	require.False(t, sm.blockPrefetchBudget.TryAcquire(1))

	sm.ReleaseBlockPrefetch(chainhash.Hash{0x01}, w)
	require.True(t, sm.blockPrefetchBudget.TryAcquire(1))
}

// TestAcquireBlockPrefetch_BlocksUntilReleaseAndCountsWaiter proves the gate
// backpressures the read-loop when the budget is full, registers a waiter while
// blocked (so the stall detector can tell self-backpressure from a slow peer),
// and unblocks on release.
func TestAcquireBlockPrefetch_BlocksUntilReleaseAndCountsWaiter(t *testing.T) {
	const budget = 2 * minInFlightBlockWeight
	sm := newPrefetchManager(budget)

	first, err := sm.AcquireBlockPrefetch(context.Background(), nil, chainhash.Hash{0x01}, budget) // fills the budget
	require.NoError(t, err)

	acquired := make(chan int64, 1)

	go func() {
		w, e := sm.AcquireBlockPrefetch(context.Background(), nil, chainhash.Hash{0x02}, minInFlightBlockWeight)
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

	sm.ReleaseBlockPrefetch(chainhash.Hash{0x01}, first)

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

	_, err := sm.AcquireBlockPrefetch(context.Background(), nil, chainhash.Hash{0x01}, budget) // fills the budget
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		_, e := sm.AcquireBlockPrefetch(ctx, nil, chainhash.Hash{0x02}, minInFlightBlockWeight)
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

	// A cancelled acquire reserved nothing, so it must not leak its hash in the
	// dedup set: the two halves of the gate stay paired even on the failure path.
	sm.inFlightBlocksMu.Lock()
	_, leaked := sm.inFlightBlocks[chainhash.Hash{0x02}]
	sm.inFlightBlocksMu.Unlock()
	require.False(t, leaked, "a cancelled acquire must remove the hash it inserted before parking")
}

// TestAcquireBlockPrefetch_QuitAbort proves a budget-parked read-loop unblocks on
// peer teardown (its quit channel closing), not only on ctx cancellation —
// mirroring awaitBlockResult, since sp.ctx is the long-lived Init context that
// Stop() does not cancel.
func TestAcquireBlockPrefetch_QuitAbort(t *testing.T) {
	const budget = 2 * minInFlightBlockWeight
	sm := newPrefetchManager(budget)

	_, err := sm.AcquireBlockPrefetch(context.Background(), nil, chainhash.Hash{0x01}, budget) // fills the budget
	require.NoError(t, err)

	quit := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		// Non-cancellable ctx: only quit can unblock this, proving quit is honored.
		_, e := sm.AcquireBlockPrefetch(context.Background(), quit, chainhash.Hash{0x02}, minInFlightBlockWeight)
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

// TestAcquireBlockPrefetch_DeduplicatesInFlight proves the in-flight-by-hash set
// is the dedup half of the admission gate: a second acquire of a hash already in
// flight is dropped with the benign ErrDuplicateBlockInFlight sentinel WITHOUT
// reserving budget, the set keeps exactly one copy, and the hash becomes
// acquirable again only after release (paired 1:1 with the budget weight).
func TestAcquireBlockPrefetch_DeduplicatesInFlight(t *testing.T) {
	const budget = 4 * minInFlightBlockWeight
	sm := newPrefetchManager(budget)
	h := chainhash.Hash{0x01}

	// First copy of H is admitted and reserves the floor weight.
	w, err := sm.AcquireBlockPrefetch(context.Background(), nil, h, minInFlightBlockWeight)
	require.NoError(t, err)
	require.Equal(t, int64(minInFlightBlockWeight), w)

	// Second copy of H is a duplicate: benign sentinel, nothing reserved.
	dupW, dupErr := sm.AcquireBlockPrefetch(context.Background(), nil, h, minInFlightBlockWeight)
	require.ErrorIs(t, dupErr, ErrDuplicateBlockInFlight)
	require.Equal(t, int64(0), dupW)

	// The duplicate reserved no budget: everything but the single admitted copy is
	// still free (probe with TryAcquire, then hand it straight back).
	require.True(t, sm.blockPrefetchBudget.TryAcquire(budget-minInFlightBlockWeight))
	sm.blockPrefetchBudget.Release(budget - minInFlightBlockWeight)

	// The set still holds exactly one copy of H.
	sm.inFlightBlocksMu.Lock()
	_, present := sm.inFlightBlocks[h]
	require.True(t, present)
	require.Len(t, sm.inFlightBlocks, 1)
	sm.inFlightBlocksMu.Unlock()

	// Release H: the hash leaves the set alongside the budget, so a fresh copy of
	// H is admissible again.
	sm.ReleaseBlockPrefetch(h, w)

	sm.inFlightBlocksMu.Lock()
	require.Empty(t, sm.inFlightBlocks)
	sm.inFlightBlocksMu.Unlock()

	w2, err := sm.AcquireBlockPrefetch(context.Background(), nil, h, minInFlightBlockWeight)
	require.NoError(t, err)
	require.Equal(t, int64(minInFlightBlockWeight), w2)
	sm.ReleaseBlockPrefetch(h, w2)
}

// TestAcquireBlockPrefetch_DuplicateDoesNotConsumeBudget proves a duplicate does
// not eat budget: a DIFFERENT hash G can still be admitted for the budget the
// duplicate of H would have (wrongly) consumed. This is the regression the dedup
// set exists to prevent — N copies of one requested, near-budget-sized block
// filling the whole budget and parking every peer's read-loop.
func TestAcquireBlockPrefetch_DuplicateDoesNotConsumeBudget(t *testing.T) {
	const budget = 2 * minInFlightBlockWeight
	sm := newPrefetchManager(budget)
	h := chainhash.Hash{0x0a}
	g := chainhash.Hash{0x0b}

	// H takes half the budget.
	wH, err := sm.AcquireBlockPrefetch(context.Background(), nil, h, minInFlightBlockWeight)
	require.NoError(t, err)

	// A duplicate of H must not reserve the remaining half.
	_, dupErr := sm.AcquireBlockPrefetch(context.Background(), nil, h, minInFlightBlockWeight)
	require.ErrorIs(t, dupErr, ErrDuplicateBlockInFlight)

	// So a different hash G can still be admitted against the remaining budget.
	wG, err := sm.AcquireBlockPrefetch(context.Background(), nil, g, minInFlightBlockWeight)
	require.NoError(t, err)
	require.Equal(t, int64(minInFlightBlockWeight), wG)

	sm.ReleaseBlockPrefetch(h, wH)
	sm.ReleaseBlockPrefetch(g, wG)
}

// TestAcquireBlockPrefetch_DisabledSkipsDedup proves the synchronous/kill-switch
// path (nil budget) neither dedups nor touches the (nil) in-flight set: repeated
// acquires of the same hash all return (0, nil), matching the one-block-per-peer
// backpressure the synchronous path already provides.
func TestAcquireBlockPrefetch_DisabledSkipsDedup(t *testing.T) {
	sm := newPrefetchManager(0)
	require.Nil(t, sm.inFlightBlocks)
	h := chainhash.Hash{0x01}

	for i := 0; i < 3; i++ {
		w, err := sm.AcquireBlockPrefetch(context.Background(), nil, h, 999)
		require.NoError(t, err)
		require.Equal(t, int64(0), w)
	}

	require.Nil(t, sm.inFlightBlocks)
	require.NotPanics(t, func() { sm.ReleaseBlockPrefetch(h, 0) })
}

func TestLocalReadBackpressured(t *testing.T) {
	// stale backdates the progress stamp well past the stall timeout so a
	// non-empty backlog reads as a hung pipeline rather than progressing work.
	stale := func(sm *SyncManager) {
		sm.lastBacklogProgress.Store(time.Now().Add(-time.Hour).UnixNano())
	}

	t.Run("kill switch (budget nil): suppression is unconditional on any backlog", func(t *testing.T) {
		sm := newPrefetchManager(0)
		require.False(t, sm.localReadBackpressured())

		// A queued / mid-validation backlog is self-backpressure: suppress the stall
		// check. On the kill-switch path the per-message watchdog is still armed for
		// blocks and owns processing-stall liveness, so suppression here stays
		// UNCONDITIONAL — exactly as pre-prefetch.
		sm.blockBacklog.Add(1)
		sm.noteBacklogProgress()
		require.True(t, sm.localReadBackpressured())

		// Even a stale progress stamp must NOT lift suppression on the kill switch:
		// timeout-gating would rotate a healthy sync peer on a legitimately slow
		// block, churn the "proven synchronous" path never produced.
		stale(sm)
		require.True(t, sm.localReadBackpressured())

		sm.blockBacklog.Add(-1)
		require.False(t, sm.localReadBackpressured())
	})

	t.Run("enabled: suppresses on a progressing backlog or a budget waiter", func(t *testing.T) {
		sm := newPrefetchManager(100)
		require.False(t, sm.localReadBackpressured())

		// A progressing backlog is self-backpressure: a stale last-block-time
		// then reflects our validation speed, not the peer.
		sm.blockBacklog.Add(5)
		sm.noteBacklogProgress()
		require.True(t, sm.localReadBackpressured())

		// Progress has stalled past the timeout — a genuine hang. Deliberately do
		// NOT fall through to the waiter signal: a hung pipeline with a full budget
		// accumulates waiters, and we WANT rotation then.
		stale(sm)
		require.False(t, sm.localReadBackpressured())
		sm.blockPrefetchWaiters.Add(1)
		require.False(t, sm.localReadBackpressured())
		sm.blockPrefetchWaiters.Add(-1)

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

	t.Run("keeps sync peer while a backlog is draining (slow but progressing validation)", func(t *testing.T) {
		sp := &peerpkg.Peer{}
		sm := newSyncManager(sp, newStalledState())

		// Blocks queued / mid-validation with a fresh progress stamp and no
		// read-loop parked: the backlog is advancing, so a stale last-block-time
		// reflects our validation speed, not the peer. The healthy peer must be
		// kept (rotation would panic in this minimal SyncManager).
		sm.blockBacklog.Add(3)
		sm.noteBacklogProgress()

		require.NotPanics(t, func() { sm.handleCheckSyncPeer() })
		require.Equal(t, sp, sm.loadSyncPeer())
	})

	t.Run("rotates when the backlog has stalled past the processing timeout", func(t *testing.T) {
		sp := &peerpkg.Peer{}
		sm := newSyncManager(sp, newStalledState())

		// A non-empty backlog whose progress stamp predates the stall timeout is a
		// hung pipeline, not slow-but-progressing validation: suppression lifts and
		// the rotation path runs (panicking in this minimal SyncManager, which
		// proves it ran rather than being suppressed).
		sm.blockBacklog.Add(3)
		sm.lastBacklogProgress.Store(time.Now().Add(-time.Hour).UnixNano())

		require.Panics(t, func() { sm.handleCheckSyncPeer() })
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

// TestHandleBlockMsg_SkipsDisconnectedPeer proves the async prefetch path stops
// validating a peer's queued blocks once that peer is disconnected: after
// awaitBlockResult disconnects on the first bad block, the remaining FIFO tail
// must be skipped rather than fully validated. The skip must be a benign
// ServiceError (not disconnect-worthy) so it only releases budget and logs, and
// it must return before any FSM/blockchain work (blockchainClient is nil here,
// so reaching that code would panic).
func TestHandleBlockMsg_SkipsDisconnectedPeer(t *testing.T) {
	sm := &SyncManager{
		logger:                   ulogger.TestLogger{},
		chainParams:              &chaincfg.MainNetParams,
		peerStates:               txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		blockPrefetchBudgetBytes: 100,
		blockPrefetchBudget:      semaphore.NewWeighted(100),
	}

	// A zero-value Peer has never been marked connected, so Connected() is false,
	// standing in for a peer awaitBlockResult has just disconnected.
	p := &peerpkg.Peer{}
	require.False(t, p.Connected())
	sm.peerStates.Set(p, &peerSyncState{
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	})

	err := sm.handleBlockMsg(&blockQueueMsg{
		blockHash: chainhash.Hash{0x01},
		peer:      p,
	})

	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrServiceError), "skip must be benign to shouldDisconnectOnBlockErr")

	// Sanity: the skip is only reached because prefetch ingestion is active; the
	// synchronous/regtest path is gated out and would proceed toward validation.
	require.True(t, sm.UsePrefetchIngestion())
}
