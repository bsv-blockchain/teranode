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
	"github.com/stretchr/testify/assert"
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

func TestAcquireBlockPrefetch_Disabled(t *testing.T) {
	sm := newPrefetchManager(0)

	w, err := sm.AcquireBlockPrefetch(context.Background(), 999)
	require.NoError(t, err)
	require.Equal(t, int64(0), w)

	// Release of a zero reservation must be a no-op, not a panic.
	require.NotPanics(t, func() { sm.ReleaseBlockPrefetch(w) })
}

func TestAcquireBlockPrefetch_MinWeight(t *testing.T) {
	sm := newPrefetchManager(100)

	// A zero/negative serialized size still reserves at least one unit so the
	// semaphore accounting stays consistent.
	w, err := sm.AcquireBlockPrefetch(context.Background(), 0)
	require.NoError(t, err)
	require.Equal(t, int64(1), w)
	sm.ReleaseBlockPrefetch(w)
}

// TestAcquireBlockPrefetch_OversizedAdmittedAlone proves a block larger than the
// whole budget is admitted (weight clamped to the budget) rather than
// deadlocking, and that it then consumes the entire budget until released —
// i.e. huge blocks process one at a time, preserving the original backpressure.
func TestAcquireBlockPrefetch_OversizedAdmittedAlone(t *testing.T) {
	sm := newPrefetchManager(100)

	w, err := sm.AcquireBlockPrefetch(context.Background(), 1_000)
	require.NoError(t, err)
	require.Equal(t, int64(100), w, "oversized weight must clamp to the budget")

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
	sm := newPrefetchManager(100)

	first, err := sm.AcquireBlockPrefetch(context.Background(), 100) // fills the budget
	require.NoError(t, err)

	acquired := make(chan int64, 1)

	go func() {
		w, e := sm.AcquireBlockPrefetch(context.Background(), 30)
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
		require.Equal(t, int64(30), w)
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
	sm := newPrefetchManager(100)

	_, err := sm.AcquireBlockPrefetch(context.Background(), 100) // fills the budget
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		_, e := sm.AcquireBlockPrefetch(ctx, 50)
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

func TestLocalReadBackpressured(t *testing.T) {
	t.Run("disabled: tracks the block backlog", func(t *testing.T) {
		sm := newPrefetchManager(0)
		require.False(t, sm.localReadBackpressured())

		sm.blockBacklog.Add(1)
		require.True(t, sm.localReadBackpressured())

		sm.blockBacklog.Add(-1)
		require.False(t, sm.localReadBackpressured())
	})

	t.Run("enabled: tracks waiters and ignores the backlog", func(t *testing.T) {
		sm := newPrefetchManager(100)
		require.False(t, sm.localReadBackpressured())

		// Under prefetch a non-empty backlog is the normal steady state and must
		// NOT count as self-backpressure — otherwise the stall detector would be
		// suppressed on nearly every tick and never rotate a slow sync peer.
		sm.blockBacklog.Add(5)
		require.False(t, sm.localReadBackpressured())

		// Only an actually-blocked read-loop (a waiter) is self-backpressure.
		sm.blockPrefetchWaiters.Add(1)
		require.True(t, sm.localReadBackpressured())

		sm.blockPrefetchWaiters.Add(-1)
		require.False(t, sm.localReadBackpressured())
	})
}

// TestHandleCheckSyncPeer_PrefetchBackpressure is the prefetch analogue of
// TestHandleCheckSyncPeer_LocalBacklog: it proves the stall detector skips while
// a read-loop is blocked acquiring budget, but — unlike the disabled path — does
// NOT let a non-empty block backlog suppress rotation of a genuinely stalled
// sync peer.
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

		sm.blockPrefetchWaiters.Add(1) // read-loop backpressured by our own processing

		require.NotPanics(t, func() { sm.handleCheckSyncPeer() })
		assert.Equal(t, sp, sm.loadSyncPeer())
	})

	t.Run("still rotates a stalled peer despite a non-empty backlog (no waiters)", func(t *testing.T) {
		sp := &peerpkg.Peer{}
		sm := newSyncManager(sp, newStalledState())

		// A backlog is normal under prefetch; with no read-loop actually blocked
		// the zero-throughput stalled peer must still be rotated (rotation panics
		// in this minimal SyncManager, which proves it ran).
		sm.blockBacklog.Add(3)

		assert.Panics(t, func() { sm.handleCheckSyncPeer() })
	})
}
