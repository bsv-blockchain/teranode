package bridge

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// admitResult carries an Acquire outcome back from a goroutine. Acquire is
// tested from other goroutines (that is the whole point of a budget), and
// require.* must never run off the test goroutine, so the goroutines only
// deliver values and the test goroutine asserts on them.
type admitResult struct {
	weight int64
	err    error
}

func newTestAdmission(t *testing.T, budget int64, base, maxWindow time.Duration) *Admission {
	t.Helper()

	tSettings := &settings.Settings{}
	tSettings.Legacy.BlockPrefetchBufferBytes = budget
	tSettings.Legacy.BlockFailureBackoffBase = base
	tSettings.Legacy.BlockFailureBackoffMaxDuration = maxWindow
	tSettings.Legacy.PeerProcessingTimeout = 3 * time.Minute

	a := NewAdmission(ulogger.TestLogger{}, tSettings)
	t.Cleanup(a.Stop)

	return a
}

func admissionHash(n byte) chainhash.Hash {
	var h chainhash.Hash
	h[0] = n

	return h
}

// TestAdmission_BudgetAdmitsConcurrentBlocksUpToCap proves the byte budget lets
// several blocks in at once and stops exactly at the cap, then releases the
// waiter as soon as one in-flight block drains. This is the netsync prefetch
// semantic (services/legacy/netsync/manager.go AcquireBlockPrefetch, PR 1190).
func TestAdmission_BudgetAdmitsConcurrentBlocksUpToCap(t *testing.T) {
	t.Parallel()

	const slots = 4

	a := newTestAdmission(t, slots*minInFlightBlockWeight, 0, 0)
	require.True(t, a.Enabled(), "a positive budget must enable admission")

	held := make([]int64, slots)

	for i := 0; i < slots; i++ {
		weight, err := a.Acquire(context.Background(), nil, admissionHash(byte(i)), minInFlightBlockWeight)
		require.NoError(t, err, "block %d must be admitted while budget remains", i)
		require.Equal(t, int64(minInFlightBlockWeight), weight)

		held[i] = weight
	}

	// The budget is now exactly full. One more block must park.
	parked := make(chan admitResult, 1)

	go func() {
		weight, err := a.Acquire(context.Background(), nil, admissionHash(slots), minInFlightBlockWeight)
		parked <- admitResult{weight: weight, err: err}
	}()

	select {
	case r := <-parked:
		t.Fatalf("block admitted past the budget cap: weight=%d err=%v", r.weight, r.err)
	case <-time.After(200 * time.Millisecond):
	}

	require.Eventually(t, func() bool { return a.Waiters() == 1 }, 2*time.Second, 5*time.Millisecond,
		"a budget-parked caller must be counted as a waiter")

	a.Release(admissionHash(0), held[0])

	select {
	case r := <-parked:
		require.NoError(t, r.err)
		require.Equal(t, int64(minInFlightBlockWeight), r.weight)
	case <-time.After(5 * time.Second):
		t.Fatal("parked block was never admitted after a slot drained")
	}

	for i := 1; i < slots; i++ {
		a.Release(admissionHash(byte(i)), held[i])
	}

	a.Release(admissionHash(slots), minInFlightBlockWeight)
	require.Zero(t, a.Waiters())
}

// TestAdmission_OversizedBlockIsAdmittedAlone covers the bigger-than-budget
// rule: the weight is clamped to the whole budget, so the block waits for every
// other in-flight block to drain and then holds the budget by itself.
func TestAdmission_OversizedBlockIsAdmittedAlone(t *testing.T) {
	t.Parallel()

	budget := int64(2 * minInFlightBlockWeight)
	a := newTestAdmission(t, budget, 0, 0)

	small, err := a.Acquire(context.Background(), nil, admissionHash(1), minInFlightBlockWeight)
	require.NoError(t, err)
	require.Equal(t, int64(minInFlightBlockWeight), small)

	oversized := make(chan admitResult, 1)

	go func() {
		weight, aerr := a.Acquire(context.Background(), nil, admissionHash(2), budget*10)
		oversized <- admitResult{weight: weight, err: aerr}
	}()

	select {
	case r := <-oversized:
		t.Fatalf("oversized block entered while another block held budget: weight=%d err=%v", r.weight, r.err)
	case <-time.After(200 * time.Millisecond):
	}

	a.Release(admissionHash(1), small)

	var big admitResult

	select {
	case big = <-oversized:
		require.NoError(t, big.err)
		require.Equal(t, budget, big.weight, "an oversized block's weight is clamped to the whole budget")
	case <-time.After(5 * time.Second):
		t.Fatal("oversized block was never admitted after the budget drained")
	}

	// It now owns the entire budget: nothing else may enter alongside it.
	third := make(chan admitResult, 1)

	go func() {
		weight, aerr := a.Acquire(context.Background(), nil, admissionHash(3), minInFlightBlockWeight)
		third <- admitResult{weight: weight, err: aerr}
	}()

	select {
	case r := <-third:
		t.Fatalf("block admitted alongside an oversized block: weight=%d err=%v", r.weight, r.err)
	case <-time.After(200 * time.Millisecond):
	}

	a.Release(admissionHash(2), big.weight)

	select {
	case r := <-third:
		require.NoError(t, r.err)

		a.Release(admissionHash(3), r.weight)
	case <-time.After(5 * time.Second):
		t.Fatal("block never admitted after the oversized block drained")
	}
}

// TestAdmission_BudgetDisabled proves the 0 kill switch is a clean no-op: no
// reservation, no dedup, no waiter accounting.
func TestAdmission_BudgetDisabled(t *testing.T) {
	a := newTestAdmission(t, 0, 5*time.Second, 150*time.Second)
	require.False(t, a.Enabled(), "a zero budget must disable prefetch admission")

	hash := admissionHash(7)

	weight, err := a.Acquire(context.Background(), nil, hash, 1<<20)
	require.NoError(t, err)
	require.Zero(t, weight, "a disabled budget reserves nothing")

	// Dedup is skipped too: the synchronous path already keeps one block in
	// flight per peer, so the same hash must not be rejected here.
	weight, err = a.Acquire(context.Background(), nil, hash, 1<<20)
	require.NoError(t, err)
	require.Zero(t, weight)

	a.Release(hash, weight)
	require.Zero(t, a.Waiters())
}

// TestAdmission_DuplicateHashRejected covers the dedup half of the gate: one
// copy of a hash at a time, released in lockstep with the byte reservation.
func TestAdmission_DuplicateHashRejected(t *testing.T) {
	a := newTestAdmission(t, 8*minInFlightBlockWeight, 0, 0)

	hash := admissionHash(9)

	weight, err := a.Acquire(context.Background(), nil, hash, minInFlightBlockWeight)
	require.NoError(t, err)

	_, err = a.Acquire(context.Background(), nil, hash, minInFlightBlockWeight)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrDuplicateBlockInFlight)

	a.Release(hash, weight)

	weight, err = a.Acquire(context.Background(), nil, hash, minInFlightBlockWeight)
	require.NoError(t, err, "the hash must be admissible again once it has drained")

	a.Release(hash, weight)
}

// TestAdmission_ReleaseAfterRejectedAcquireKeepsDedupEntry pins the hardening
// on Release: a caller that pairs Release with a REJECTED Acquire must not
// evict the dedup entry belonging to the copy still being ingested. If it did,
// a third copy of the same hash would be admitted against the same budget
// while the first is still in flight.
func TestAdmission_ReleaseAfterRejectedAcquireKeepsDedupEntry(t *testing.T) {
	a := newTestAdmission(t, 8*minInFlightBlockWeight, 0, 0)
	hash := admissionHash(5)

	admitted, err := a.Acquire(context.Background(), nil, hash, minInFlightBlockWeight)
	require.NoError(t, err)
	require.Positive(t, admitted)

	duplicateWeight, err := a.Acquire(context.Background(), nil, hash, minInFlightBlockWeight)
	require.ErrorIs(t, err, ErrDuplicateBlockInFlight)
	require.Zero(t, duplicateWeight, "a rejected acquire reserves nothing")

	// The mistake: releasing what the rejected acquire returned.
	a.Release(hash, duplicateWeight)

	_, err = a.Acquire(context.Background(), nil, hash, minInFlightBlockWeight)
	require.ErrorIs(t, err, ErrDuplicateBlockInFlight,
		"the in-flight copy's dedup entry must survive a release paired with a rejected acquire")

	// The real owner's release still frees both halves.
	a.Release(hash, admitted)

	reacquired, err := a.Acquire(context.Background(), nil, hash, minInFlightBlockWeight)
	require.NoError(t, err, "the hash must be admissible once its real owner releases it")

	a.Release(hash, reacquired)
}

// TestAdmission_CancelledAcquireDropsDedupSlot proves a cancelled wait reserves
// nothing and leaves no stale hash behind.
func TestAdmission_CancelledAcquireDropsDedupSlot(t *testing.T) {
	a := newTestAdmission(t, minInFlightBlockWeight, 0, 0)

	first, err := a.Acquire(context.Background(), nil, admissionHash(1), minInFlightBlockWeight)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	parked := make(chan admitResult, 1)

	go func() {
		weight, aerr := a.Acquire(ctx, nil, admissionHash(2), minInFlightBlockWeight)
		parked <- admitResult{weight: weight, err: aerr}
	}()

	require.Eventually(t, func() bool { return a.Waiters() == 1 }, 2*time.Second, 5*time.Millisecond)
	cancel()

	select {
	case r := <-parked:
		require.Error(t, r.err)
		require.Zero(t, r.weight, "a cancelled acquire must reserve nothing")
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled acquire never returned")
	}

	a.Release(admissionHash(1), first)

	weight, err := a.Acquire(context.Background(), nil, admissionHash(2), minInFlightBlockWeight)
	require.NoError(t, err, "the cancelled hash must not linger in the dedup set")

	a.Release(admissionHash(2), weight)
}

// TestAdmission_FailureBackoffWindow is the table for the per-block failure
// backoff (services/legacy/netsync/manager.go recordBlockFailureBackoff,
// PR 1192): linear ramp of base per consecutive failure, capped at the max
// window, disabled when either knob is 0.
func TestAdmission_FailureBackoffWindow(t *testing.T) {
	const base = 5 * time.Second

	tests := []struct {
		name       string
		base       time.Duration
		maxWindow  time.Duration
		failures   int
		wantSkip   bool
		wantWindow time.Duration
	}{
		{name: "base 0 disables the backoff", base: 0, maxWindow: 150 * time.Second, failures: 3},
		{name: "max window 0 disables the backoff", base: base, maxWindow: 0, failures: 3},
		{name: "negative base disables the backoff", base: -1, maxWindow: 150 * time.Second, failures: 3},
		{name: "one failure waits one base", base: base, maxWindow: 150 * time.Second, failures: 1, wantSkip: true, wantWindow: base},
		{name: "two failures ramp linearly", base: base, maxWindow: 150 * time.Second, failures: 2, wantSkip: true, wantWindow: 2 * base},
		{name: "three failures ramp linearly", base: base, maxWindow: 150 * time.Second, failures: 3, wantSkip: true, wantWindow: 3 * base},
		{name: "ramp is capped at the max window", base: base, maxWindow: 12 * time.Second, failures: 5, wantSkip: true, wantWindow: 12 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := newTestAdmission(t, 1<<20, tt.base, tt.maxWindow)
			hash := admissionHash(1)

			var window time.Duration
			for i := 0; i < tt.failures; i++ {
				window = a.RecordFailure(hash)
			}

			require.Equal(t, tt.wantWindow, window, "recorded backoff window")

			remaining, attempts, skip := a.BackoffRemaining(hash)
			require.Equal(t, tt.wantSkip, skip, "skip decision")

			if !tt.wantSkip {
				require.NoError(t, a.SkipForBackoff(hash), "a disabled backoff must never skip a block")
				return
			}

			require.Equal(t, tt.failures, attempts, "consecutive failure count")
			require.Positive(t, remaining)
			require.LessOrEqual(t, remaining, tt.wantWindow)

			err := a.SkipForBackoff(hash)
			require.Error(t, err, "a backed-off block must be skipped without blocking")
			require.True(t, errors.Is(err, errors.ErrServiceUnavailable),
				"the skip must be the retryable local-fault error netsync returns, got %v", err)
		})
	}
}

// TestAdmission_BackoffWindowElapses proves the skip is time-bounded: once the
// window passes, the same hash is admitted again.
func TestAdmission_BackoffWindowElapses(t *testing.T) {
	a := newTestAdmission(t, 1<<20, 30*time.Millisecond, 5*time.Second)
	hash := admissionHash(3)

	require.Equal(t, 30*time.Millisecond, a.RecordFailure(hash))
	require.Error(t, a.SkipForBackoff(hash))

	require.Eventually(t, func() bool { return a.SkipForBackoff(hash) == nil }, 5*time.Second, 5*time.Millisecond,
		"the block must become admissible once its backoff window elapses")
}

// TestAdmission_ClearFailureResetsTheRamp proves a successful block forgets its
// failure history, so a later failure starts at the base again.
func TestAdmission_ClearFailureResetsTheRamp(t *testing.T) {
	const base = 5 * time.Second

	a := newTestAdmission(t, 1<<20, base, 150*time.Second)
	hash := admissionHash(4)

	require.Equal(t, base, a.RecordFailure(hash))
	require.Equal(t, 2*base, a.RecordFailure(hash))

	a.ClearFailure(hash)
	require.NoError(t, a.SkipForBackoff(hash), "a cleared hash must not be skipped")

	require.Equal(t, base, a.RecordFailure(hash), "the ramp must restart at the base after a success")
}

// TestAdmission_PreAdmitContext covers the pre-admission timeout that protocol
// turns into a sync-peer rotation (services/legacy/peer_server.go OnBlock,
// PR 1281): a real deadline, a timeout distinguishable from a parent cancel,
// and the 3 minute fallback when the setting is unset.
func TestAdmission_PreAdmitContext(t *testing.T) {
	tSettings := &settings.Settings{}
	tSettings.Legacy.PeerProcessingTimeout = 40 * time.Millisecond

	a := NewAdmission(ulogger.TestLogger{}, tSettings)
	t.Cleanup(a.Stop)

	ctx, cancel := a.PreAdmitContext(context.Background())
	defer cancel()

	_, ok := ctx.Deadline()
	require.True(t, ok, "the pre-admission context must carry a deadline")
	require.False(t, PreAdmitTimedOut(ctx), "no timeout before the deadline fires")

	<-ctx.Done()
	require.True(t, PreAdmitTimedOut(ctx), "an expired pre-admission context must rotate the sync peer")

	// A parent cancel is teardown, not a wedged local round-trip: it must NOT
	// look like a timeout, so the caller drops the block instead of rotating.
	parent, cancelParent := context.WithCancel(context.Background())

	teardown, cancelTeardown := a.PreAdmitContext(parent)
	defer cancelTeardown()

	cancelParent()
	<-teardown.Done()
	require.False(t, PreAdmitTimedOut(teardown), "a parent cancel must not be reported as a pre-admission timeout")

	// Unset setting falls back to 3 minutes so zeroing it cannot reintroduce
	// an unbounded pre-admission phase.
	unset := NewAdmission(ulogger.TestLogger{}, &settings.Settings{})
	t.Cleanup(unset.Stop)

	fallbackCtx, cancelFallback := unset.PreAdmitContext(context.Background())
	defer cancelFallback()

	deadline, ok := fallbackCtx.Deadline()
	require.True(t, ok)
	require.InDelta(t, float64(3*time.Minute), float64(time.Until(deadline)), float64(5*time.Second))
}
