package blockvalidation

// Tests for the per-block BLOCK_INCOMPLETE retry cap.
//
// A block whose transactions can never be blessed (missing parents, lost UTXO
// store partition) used to be retried forever by every block-processing worker
// while the chain tip stayed frozen behind it. The cap bounds that grinding:
// after IncompleteBlockMaxRetriesPerBlock counted failures, re-announcements
// from sources that already failed the block are dropped at enqueue (untried
// sources always pass — one data-poor peer must not exhaust a block's budget
// for everyone), one manual_intervention_required escalation is emitted, and a
// once-per-window self-requeue chain keeps retrying until the block validates.
// The cap only engages when the FSM is RUNNING, but an undeterminable FSM
// state keeps it engaged (a degraded blockchain service is exactly the
// condition that produces BLOCK_INCOMPLETE storms).

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jellydator/ttlcache/v3"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newIncompleteCapServer builds a Server with just the BLOCK_INCOMPLETE
// attempt-cap machinery wired.
func newIncompleteCapServer(t *testing.T, cap int) *Server {
	t.Helper()

	return newIncompleteCapServerWithCooldown(t, cap, incompleteBlockCooldown)
}

// newIncompleteCapServerWithCooldown is newIncompleteCapServer with the cap's
// window shrunk. The cache TTL and the Server's cooldown are set from the SAME
// value, exactly as production wires them, so tests that exercise
// window-expiry behaviour reproduce the real pairing rather than a hand-picked
// delay against a 10-minute TTL.
func newIncompleteCapServerWithCooldown(t *testing.T, cap int, cooldown time.Duration) *Server {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.IncompleteBlockMaxRetriesPerBlock = cap

	u := &Server{
		settings:           tSettings,
		logger:             ulogger.TestLogger{},
		incompleteCooldown: cooldown,
		blockIncompleteAttempts: ttlcache.New[chainhash.Hash, *incompleteBlockState](
			ttlcache.WithTTL[chainhash.Hash, *incompleteBlockState](cooldown),
			ttlcache.WithDisableTouchOnHit[chainhash.Hash, *incompleteBlockState](),
		),
	}

	go u.blockIncompleteAttempts.Start()
	t.Cleanup(u.blockIncompleteAttempts.Stop)

	return u
}

func bfFrom(h *chainhash.Hash, source string) processBlockFound {
	return processBlockFound{hash: h, baseURL: source, peerID: source}
}

func TestIncompleteBlockAttemptCap(t *testing.T) {
	u := newIncompleteCapServer(t, 3)
	u.blockPriorityQueue = NewBlockPriorityQueue(ulogger.TestLogger{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := chainhash.HashH([]byte("incomplete-blk-cap"))
	bf := bfFrom(&h, "http://peer-a")

	require.False(t, u.incompleteAttemptsExhausted(&h), "zero attempts is not exhausted")

	u.recordIncompleteBlockFailure(ctx, bf)
	require.False(t, u.incompleteAttemptsExhausted(&h), "1/3 is below the cap")

	u.recordIncompleteBlockFailure(ctx, bf)
	require.False(t, u.incompleteAttemptsExhausted(&h), "2/3 is below the cap")

	u.recordIncompleteBlockFailure(ctx, bf)
	require.True(t, u.incompleteAttemptsExhausted(&h), "3/3 reaches the cap -> cooldown")

	// A different block has its own independent budget.
	other := chainhash.HashH([]byte("incomplete-blk-other"))
	require.False(t, u.incompleteAttemptsExhausted(&other))

	// Success clears the state so a recovered block starts fresh.
	u.clearIncompleteAttempts(&h)
	require.False(t, u.incompleteAttemptsExhausted(&h), "cleared block is retriable again")

	// Cap disabled (<= 0) never exhausts, regardless of failure count.
	u.settings.BlockValidation.IncompleteBlockMaxRetriesPerBlock = 0
	for range 10 {
		u.recordIncompleteBlockFailure(ctx, bf)
	}
	require.False(t, u.incompleteAttemptsExhausted(&h), "cap <= 0 disables the bound")
}

func TestIncompleteBlockAttemptCap_NilCacheSafe(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	u := &Server{settings: tSettings, logger: ulogger.TestLogger{}} // blockIncompleteAttempts == nil

	h := chainhash.HashH([]byte("incomplete-blk-nil"))
	require.NotPanics(t, func() {
		require.False(t, u.incompleteAttemptsExhausted(&h))
		require.False(t, u.incompleteSourceSuppressed(&h, "http://peer"))
		u.clearIncompleteAttempts(&h)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		u.recordIncompleteBlockFailure(ctx, bfFrom(&h, "http://peer"))
	})
}

// TestIncompleteBlockSourceSuppression pins the source-awareness of the
// enqueue gate: once capped, sources that already failed the block are
// suppressed, but an UNTRIED source must always pass — one data-poor peer must
// not exhaust the block's budget for everyone.
func TestIncompleteBlockSourceSuppression(t *testing.T) {
	u := newIncompleteCapServer(t, 2)
	u.blockPriorityQueue = NewBlockPriorityQueue(ulogger.TestLogger{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := chainhash.HashH([]byte("incomplete-blk-sources"))

	u.recordIncompleteBlockFailure(ctx, bfFrom(&h, "http://peer-a"))
	u.recordIncompleteBlockFailure(ctx, bfFrom(&h, "http://peer-b"))
	require.True(t, u.incompleteAttemptsExhausted(&h))

	require.True(t, u.incompleteSourceSuppressed(&h, "http://peer-a"), "a source that failed the block is suppressed")
	require.True(t, u.incompleteSourceSuppressed(&h, "http://peer-b"), "a source that failed the block is suppressed")
	require.False(t, u.incompleteSourceSuppressed(&h, "http://peer-c"), "an untried source must pass the gate")
	require.True(t, u.incompleteSourceSuppressed(&h, ""), "a sourceless announcement of a capped block is suppressed")
	require.True(t, u.incompleteSourceSuppressed(&h, SourceTypeRetry), "a bare retry marker is suppressed")

	// Below the cap nothing is suppressed, tried or not.
	fresh := chainhash.HashH([]byte("incomplete-blk-fresh"))
	u.recordIncompleteBlockFailure(ctx, bfFrom(&fresh, "http://peer-a"))
	require.False(t, u.incompleteSourceSuppressed(&fresh, "http://peer-a"))
}

// TestRecordIncompleteBlockFailure_EscalatesOnce verifies the escalation fires
// exactly once per block — latched by the escalated flag, so failures past the
// cap (including overshoot from workers racing the enqueue gate) never
// re-escalate.
func TestRecordIncompleteBlockFailure_EscalatesOnce(t *testing.T) {
	initPrometheusMetrics()

	u := newIncompleteCapServer(t, 2)
	u.blockPriorityQueue = NewBlockPriorityQueue(ulogger.TestLogger{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := chainhash.HashH([]byte("incomplete-blk-escalate"))
	bf := bfFrom(&h, "http://peer-1")

	retriesBefore := testutil.ToFloat64(prometheusBlockValidationIncompleteBlockRetries)
	escalationsBefore := testutil.ToFloat64(prometheusBlockValidationIncompleteBlockEscalations)

	u.recordIncompleteBlockFailure(ctx, bf)
	require.False(t, u.incompleteAttemptsExhausted(&h), "1/2 must not escalate")
	require.InDelta(t, escalationsBefore, testutil.ToFloat64(prometheusBlockValidationIncompleteBlockEscalations), 0.001)

	u.recordIncompleteBlockFailure(ctx, bf)
	require.True(t, u.incompleteAttemptsExhausted(&h), "2/2 reaches the cap")
	require.InDelta(t, escalationsBefore+1, testutil.ToFloat64(prometheusBlockValidationIncompleteBlockEscalations), 0.001)

	u.recordIncompleteBlockFailure(ctx, bf)
	u.recordIncompleteBlockFailure(ctx, bf)
	require.InDelta(t, escalationsBefore+1, testutil.ToFloat64(prometheusBlockValidationIncompleteBlockEscalations), 0.001, "failures past the cap must not re-escalate")

	require.InDelta(t, retriesBefore+4, testutil.ToFloat64(prometheusBlockValidationIncompleteBlockRetries), 0.001, "every failure counts toward the retry total")
}

// TestRecordIncompleteBlockFailure_FSMGate pins the caught-up gating: failures
// are not counted while the node is syncing (CATCHINGBLOCKS), are counted when
// RUNNING, and — critically — are still counted when the FSM state CANNOT BE
// DETERMINED: a degraded blockchain service must not silently disable the cap.
func TestRecordIncompleteBlockFailure_FSMGate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	newServerWithFSM := func(state blockchain_api.FSMStateType, stateErr error) *Server {
		u := newIncompleteCapServer(t, 1)
		u.blockPriorityQueue = NewBlockPriorityQueue(ulogger.TestLogger{})

		bcMock := &blockchain.Mock{}
		if stateErr != nil {
			bcMock.On("GetFSMCurrentState", mock.Anything).Return(nil, stateErr)
		} else {
			st := state
			bcMock.On("GetFSMCurrentState", mock.Anything).Return(&st, nil)
		}

		u.blockValidation = &BlockValidation{blockchainClient: bcMock}

		return u
	}

	t.Run("not counted while catching up", func(t *testing.T) {
		u := newServerWithFSM(blockchain_api.FSMStateType_CATCHINGBLOCKS, nil)
		h := chainhash.HashH([]byte("fsm-catchingblocks"))

		u.recordIncompleteBlockFailure(ctx, bfFrom(&h, "http://peer"))
		require.False(t, u.incompleteAttemptsExhausted(&h), "failures during catchup must not count toward the cap")
	})

	t.Run("counted when running", func(t *testing.T) {
		u := newServerWithFSM(blockchain_api.FSMStateType_RUNNING, nil)
		h := chainhash.HashH([]byte("fsm-running"))

		u.recordIncompleteBlockFailure(ctx, bfFrom(&h, "http://peer"))
		require.True(t, u.incompleteAttemptsExhausted(&h), "failures while RUNNING must count toward the cap")
	})

	t.Run("counted when FSM state cannot be determined", func(t *testing.T) {
		u := newServerWithFSM(0, errors.NewServiceError("blockchain unavailable"))
		h := chainhash.HashH([]byte("fsm-error"))

		u.recordIncompleteBlockFailure(ctx, bfFrom(&h, "http://peer"))
		require.True(t, u.incompleteAttemptsExhausted(&h), "an undeterminable FSM state must keep the cap engaged")
	})
}

// TestRecordIncompleteBlockFailure_RequeueCarriesSource verifies the
// cooldown-expiry self-requeue re-enters the priority queue with the failing
// announcement's ORIGINAL source. A stripped SourceTypeRetry marker is
// unfetchable: by requeue time the queue's alternative-source index for the
// hash is gone, so processBlockWithPriority would fail with "no sources
// available" and the retry would be a silent no-op.
func TestRecordIncompleteBlockFailure_RequeueCarriesSource(t *testing.T) {
	initPrometheusMetrics()

	u := newIncompleteCapServerWithCooldown(t, 1, 200*time.Millisecond)
	u.blockPriorityQueue = NewBlockPriorityQueue(ulogger.TestLogger{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := chainhash.HashH([]byte("incomplete-blk-requeue"))
	bf := bfFrom(&h, "http://origin-peer")

	u.recordIncompleteBlockFailure(ctx, bf)

	// Peek takes the queue lock (Len does not), so it is the race-safe way to
	// poll for the AfterFunc's requeue.
	require.Eventually(t, func() bool {
		_, _, ok := u.blockPriorityQueue.Peek()
		return ok
	}, 2*time.Second, 10*time.Millisecond, "cooldown expiry must requeue the block")

	queued, _, ok := u.blockPriorityQueue.Peek()
	require.True(t, ok)
	require.True(t, queued.hash.IsEqual(&h))
	require.Equal(t, "http://origin-peer", queued.baseURL, "requeue must carry the original source, not a bare retry marker")
	require.Equal(t, "http://origin-peer", queued.peerID, "requeue must carry the original peer")
}

// TestIncompleteRequeueChainStopsOnRecovery verifies the once-per-window retry
// chain terminates once the block validates (state cleared): the timer fires,
// finds no capped state, and requeues nothing.
func TestIncompleteRequeueChainStopsOnRecovery(t *testing.T) {
	u := newIncompleteCapServerWithCooldown(t, 1, 200*time.Millisecond)
	u.blockPriorityQueue = NewBlockPriorityQueue(ulogger.TestLogger{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := chainhash.HashH([]byte("incomplete-blk-recovered"))

	u.recordIncompleteBlockFailure(ctx, bfFrom(&h, "http://peer"))
	require.True(t, u.incompleteAttemptsExhausted(&h))

	// Block validates before the window expires: success clears the state, and
	// the armed chain must observe that and stop instead of re-queueing.
	u.clearIncompleteAttempts(&h)

	time.Sleep(600 * time.Millisecond)

	_, _, ok := u.blockPriorityQueue.Peek()
	require.False(t, ok, "a recovered block must not be requeued")
}

// TestIncompleteRequeueFiresAfterWindowExpiry is the regression test for the
// self-requeue that never fired: the cap re-Sets the cache entry with the
// cooldown TTL and then arms the retry timer with the SAME duration, so the
// timer necessarily fires after the entry it used to look up had expired —
// the chain read a nil entry, concluded "recovered", and stopped without ever
// re-queueing. The chain must survive its own window and retry the block.
func TestIncompleteRequeueFiresAfterWindowExpiry(t *testing.T) {
	initPrometheusMetrics()

	u := newIncompleteCapServerWithCooldown(t, 1, 200*time.Millisecond)
	u.blockPriorityQueue = NewBlockPriorityQueue(ulogger.TestLogger{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := chainhash.HashH([]byte("incomplete-blk-window-expiry"))

	u.recordIncompleteBlockFailure(ctx, bfFrom(&h, "http://origin-peer"))
	require.True(t, u.incompleteAttemptsExhausted(&h), "the failure must reach the cap and arm the chain")

	require.Eventually(t, func() bool {
		_, _, ok := u.blockPriorityQueue.Peek()
		return ok
	}, 3*time.Second, 10*time.Millisecond, "the armed chain must re-queue the block when its own window expires")

	queued, _, ok := u.blockPriorityQueue.Peek()
	require.True(t, ok)
	require.True(t, queued.hash.IsEqual(&h))

	// …and it must keep going: the cap state is revived each window, so the
	// block stays capped (no re-page) and the chain re-arms.
	require.True(t, u.incompleteAttemptsExhausted(&h), "the cap state must survive the retry")

	escalations := testutil.ToFloat64(prometheusBlockValidationIncompleteBlockEscalations)

	time.Sleep(500 * time.Millisecond)

	require.InDelta(t, escalations, testutil.ToFloat64(prometheusBlockValidationIncompleteBlockEscalations), 0.001, "re-queueing every window must not re-page the operator")
}

// TestIncompleteRequeueReleasesFailedSources pins that a source judged unable
// to serve a block is re-tested on the next window rather than suppressed for
// the whole extended lifetime of the cap state. The reason a peer failed
// (block not absorbed yet, pruned body) is usually temporary, and the
// cooldown-expiry retry is exactly the moment every source deserves another
// turn.
func TestIncompleteRequeueReleasesFailedSources(t *testing.T) {
	initPrometheusMetrics()

	u := newIncompleteCapServerWithCooldown(t, 1, 500*time.Millisecond)
	u.blockPriorityQueue = NewBlockPriorityQueue(ulogger.TestLogger{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := chainhash.HashH([]byte("incomplete-blk-source-release"))

	u.recordIncompleteBlockFailure(ctx, bfFrom(&h, "http://peer-a"))
	require.True(t, u.incompleteSourceSuppressed(&h, "http://peer-a"), "the failing source is suppressed inside the window")

	// The cap must still be in force after the window's retry (state revived),
	// with the previously-failed source released rather than judged once and
	// suppressed for the whole extended lifetime of the state.
	require.Eventually(t, func() bool {
		return u.incompleteAttemptsExhausted(&h) && !u.incompleteSourceSuppressed(&h, "http://peer-a")
	}, 3*time.Second, 10*time.Millisecond, "the window's retry must give a previously-failed source another turn")
	require.True(t, u.incompleteSourceSuppressed(&h, ""), "a sourceless announcement of a capped block is still suppressed")
}

// TestIncompleteBlockWindowSlidesOnFailure pins the deliberate divergence from
// the catchup cap's fixed window: the cooldown window slides on each COUNTED
// failure, so a slow, expensive block that fails every few minutes still
// accumulates to the cap instead of the window expiring under it.
func TestIncompleteBlockWindowSlidesOnFailure(t *testing.T) {
	u := newIncompleteCapServer(t, 5)
	u.blockPriorityQueue = NewBlockPriorityQueue(ulogger.TestLogger{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := chainhash.HashH([]byte("incomplete-blk-window"))
	bf := bfFrom(&h, "http://peer")

	u.recordIncompleteBlockFailure(ctx, bf)
	firstExpiry := u.blockIncompleteAttempts.Get(h).ExpiresAt()

	time.Sleep(20 * time.Millisecond)

	u.recordIncompleteBlockFailure(ctx, bf)
	secondExpiry := u.blockIncompleteAttempts.Get(h).ExpiresAt()

	require.True(t, secondExpiry.After(firstExpiry), "a counted failure must slide the window so slow blocks still reach the cap")
}
