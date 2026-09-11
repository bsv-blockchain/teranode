package aerospike

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// newTestStoreForBatchWait builds the minimum Store the two ctx-less batch
// producers touch. Both are wired to okBatcher, which accepts the item and
// never completes it — "the dispatcher never signals" — so the only ways out of
// group.Wait are the context arm and the timer arm, which is exactly what these
// tests discriminate. No container is involved.
func newTestStoreForBatchWait(t *testing.T) *Store {
	t.Helper()

	InitPrometheusMetrics()

	tSettings := &settings.Settings{}
	tSettings.Aerospike.UseDefaultPolicies = true

	return &Store{
		ctx:              context.Background(),
		namespace:        "test-ns",
		setName:          "test-set",
		logger:           ulogger.TestLogger{},
		settings:         tSettings,
		incrementBatcher: okBatcher[batchIncrement]{},
		setDAHBatcher:    okBatcher[batchDAH]{},
	}
}

func batchTimeoutCounter(op string) float64 {
	return testutil.ToFloat64(prometheusUtxoMapErrors.WithLabelValues(op, "BatchTimeout"))
}

// TestIncrementSpentRecords_ContextCancelIsNotATimeout pins that a cancelled
// store context is reported as cancellation, not as an elapsed timeout. The
// spend wait is 30s and the call must return far sooner, which is the proof
// that the context arm — not the timer arm — released it.
func TestIncrementSpentRecords_ContextCancelIsNotATimeout(t *testing.T) {
	s := newTestStoreForBatchWait(t)
	s.settings.UtxoStore.SpendWaitTimeout = 30 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx

	before := batchTimeoutCounter("IncrementSpentRecords")

	txid := chainhash.HashH([]byte("increment-cancel"))
	errCh := make(chan error, 1)

	go func() {
		_, err := s.IncrementSpentRecords(&txid, 1, 100)
		errCh <- err
	}()

	cancel()

	var err error

	select {
	case err = <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("IncrementSpentRecords did not return on a cancelled store context")
	}

	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrContextCanceled), "a cancelled store context must be classified as cancellation: %s", err)
	require.NotContains(t, err.Error(), "timed out")
	require.Equal(t, before, batchTimeoutCounter("IncrementSpentRecords"),
		"a cancellation must not bump the alert-grade BatchTimeout counter")
}

// TestIncrementSpentRecords_GenuineTimeoutStillReportsTimeout is the other half:
// the timer arm must keep reporting a timeout, with the counter bump intact.
func TestIncrementSpentRecords_GenuineTimeoutStillReportsTimeout(t *testing.T) {
	s := newTestStoreForBatchWait(t)
	s.settings.UtxoStore.SpendWaitTimeout = 20 * time.Millisecond

	before := batchTimeoutCounter("IncrementSpentRecords")

	txid := chainhash.HashH([]byte("increment-timeout"))

	_, err := s.IncrementSpentRecords(&txid, 1, 100)
	require.Error(t, err)
	require.Contains(t, err.Error(), "timed out after")
	require.True(t, errors.Is(err, errors.ErrServiceUnavailable))
	require.Equal(t, before+1, batchTimeoutCounter("IncrementSpentRecords"),
		"a genuine timeout must still bump BatchTimeout exactly once")
}

// TestSetDAHForChildRecords_ContextCancelIsNotATimeout mirrors the increment
// case for the other ctx-less producer. This path has no metric of its own, so
// the assertion is purely on the classification and the wording.
func TestSetDAHForChildRecords_ContextCancelIsNotATimeout(t *testing.T) {
	s := newTestStoreForBatchWait(t)
	s.batcherWait = 30 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx

	txid := chainhash.HashH([]byte("setdah-cancel"))
	errCh := make(chan error, 1)

	go func() { errCh <- s.SetDAHForChildRecords(&txid, 2, 500) }()

	cancel()

	var err error

	select {
	case err = <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("SetDAHForChildRecords did not return on a cancelled store context")
	}

	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrContextCanceled), "a cancelled store context must be classified as cancellation: %s", err)
	require.NotContains(t, err.Error(), "did not complete within")
}

// TestSetDAHForChildRecords_GenuineTimeoutStillReportsTimeout keeps the timer
// arm reporting an elapsed timeout.
func TestSetDAHForChildRecords_GenuineTimeoutStillReportsTimeout(t *testing.T) {
	s := newTestStoreForBatchWait(t)
	s.batcherWait = 20 * time.Millisecond

	txid := chainhash.HashH([]byte("setdah-timeout"))

	err := s.SetDAHForChildRecords(&txid, 2, 500)
	require.Error(t, err)
	require.Contains(t, err.Error(), "did not complete within")
	require.True(t, errors.Is(err, errors.ErrServiceUnavailable))
}

// TestIsContextWaitErr_ClassifiesByReturnedError pins the classification
// policy: the verdict comes from the error Wait RETURNED.
//
// This is deliberately NOT a "both arms fire" test. Group.Wait's select is
// three-way, so when ctx.Done() and timer.C are both ready Go picks
// pseudo-randomly and no timing test can make that deterministic. The policy is
// therefore pinned where it actually lives — in the mapping from returned error
// to verdict. The stronger guarantee, that no s.ctx.Err() recheck can creep back
// in, is enforced by the helper's signature taking no context at all, not by a
// timing test.
//
// The two-branch test is exhaustive because Wait returns only those two
// classes, so "not a ctx error" IS the timeout.
func TestIsContextWaitErr_ClassifiesByReturnedError(t *testing.T) {
	tests := []struct {
		name    string
		waitErr error
		expect  bool
	}{
		{name: "context canceled", waitErr: context.Canceled, expect: true},
		{name: "context deadline exceeded", waitErr: context.DeadlineExceeded, expect: true},
		{
			name:    "timer arm",
			waitErr: fmt.Errorf("%w after %s", completion.ErrWaitTimeout, 30*time.Second),
			expect:  false,
		},
		{name: "unrelated error", waitErr: errors.NewStorageError("aerospike record unreadable"), expect: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expect, isContextWaitErr(tc.waitErr))
		})
	}
}
