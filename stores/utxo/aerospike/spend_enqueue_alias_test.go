package aerospike

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// recordingSpendBatcher captures the exact slice Spend hands to the batcher and
// then completes every item it accepted. Completing matters: without it Spend
// parks on group.Wait for the full SpendWaitTimeout. Results are benign (nil) so
// Spend stays out of the rollback / Unspend / external-storage paths, and the
// assertions are on the CAPTURED slice, not on Spend's return value.
type recordingSpendBatcher struct {
	mu       sync.Mutex
	captured [][]*batchSpend
}

func (r *recordingSpendBatcher) record(items []*batchSpend) {
	r.mu.Lock()
	r.captured = append(r.captured, items)
	r.mu.Unlock()

	for _, it := range items {
		it.complete(nil)
	}
}

// enqueued returns the single slice handed to the batcher, failing the test if
// the enqueue did not happen exactly once.
func (r *recordingSpendBatcher) enqueued(t *testing.T) []*batchSpend {
	t.Helper()

	r.mu.Lock()
	defer r.mu.Unlock()

	require.Len(t, r.captured, 1, "Spend must enqueue the whole tx in exactly one PutBatchCtx")

	return r.captured[0]
}

func (r *recordingSpendBatcher) Put(*batchSpend, ...int)                     {}
func (r *recordingSpendBatcher) PutCtx(context.Context, *batchSpend, ...int) {}
func (r *recordingSpendBatcher) PutBatch(items []*batchSpend, _ ...int)      { r.record(items) }
func (r *recordingSpendBatcher) PutBatchCtx(_ context.Context, items []*batchSpend, _ ...int) {
	r.record(items)
}
func (r *recordingSpendBatcher) Trigger()                      {}
func (r *recordingSpendBatcher) SetDrainMode(bool)             {}
func (r *recordingSpendBatcher) SetTickInterval(time.Duration) {}
func (r *recordingSpendBatcher) Close()                        {}

// newTestStoreForSpendEnqueue builds the minimum Store (*Store).Spend touches.
// No container is involved: the spend batcher is always a test double.
func newTestStoreForSpendEnqueue(t *testing.T) *Store {
	t.Helper()

	InitPrometheusMetrics()

	tSettings := &settings.Settings{}
	tSettings.Aerospike.UseDefaultPolicies = true
	// Safety net only: every test double completes its items, so a healthy run
	// never reaches this bound.
	tSettings.UtxoStore.SpendWaitTimeout = 5 * time.Second

	chainParams := chaincfg.RegressionNetParams
	tSettings.ChainCfgParams = &chainParams

	return &Store{
		ctx:       context.Background(),
		namespace: "test-ns",
		setName:   "test-set",
		logger:    ulogger.TestLogger{},
		settings:  tSettings,
	}
}

// txWithInputs builds a transaction with n spendable inputs, each on its own
// parent txid so the spends are distinct.
func txWithInputs(t *testing.T, n int) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()

	for i := 0; i < n; i++ {
		require.NoError(t, tx.From(
			fmt.Sprintf("%064x", i+1),
			uint32(i), //nolint:gosec // small loop counter
			"76a914000000000000000000000000000000000000000088ac",
			1000,
		))
	}

	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 500))

	return tx
}

// TestSpend_EnqueuesEveryItemWhenNoCircuitBreaker guards the alias path: with no
// breaker, the slice handed to the batcher is the items slice itself and must
// still carry every input, in order.
func TestSpend_EnqueuesEveryItemWhenNoCircuitBreaker(t *testing.T) {
	const inputs = 5

	s := newTestStoreForSpendEnqueue(t)
	rec := &recordingSpendBatcher{}
	s.spendBatcher = rec
	s.spendCircuitBreaker = nil

	tx := txWithInputs(t, inputs)

	spends, err := s.Spend(context.Background(), tx, 100)
	require.NoError(t, err)
	require.Len(t, spends, inputs)

	enqueued := rec.enqueued(t)
	require.Len(t, enqueued, inputs)

	for i, item := range enqueued {
		require.Same(t, spends[i], item.spend, "enqueued item %d must be the spend for input %d", i, i)
	}
}

// TestSpend_EnqueuesOnlyAcceptedItemsWhenBreakerHalfOpen exercises the prefix
// copy and the mid-batch boundary. A fully-open breaker cannot produce an
// accept-then-reject shape (Allow returns false for EVERY call before
// nextAttempt); half-open can, because the first Allow after the cooldown flips
// the state and returns true, and subsequent calls return true only while
// halfOpenAttempts < halfOpenMax. With halfOpenMax = 1, input 0 is accepted and
// the rest are rejected.
func TestSpend_EnqueuesOnlyAcceptedItemsWhenBreakerHalfOpen(t *testing.T) {
	const inputs = 4

	s := newTestStoreForSpendEnqueue(t)
	rec := &recordingSpendBatcher{}
	s.spendBatcher = rec

	cb := newCircuitBreaker(1, 1, time.Millisecond)
	require.NotNil(t, cb)

	cb.RecordFailure() // trips the breaker open

	time.Sleep(20 * time.Millisecond) // let the cooldown elapse so the next Allow half-opens

	s.spendCircuitBreaker = cb

	tx := txWithInputs(t, inputs)

	spends, _ := s.Spend(context.Background(), tx, 100)
	require.Len(t, spends, inputs)

	enqueued := rec.enqueued(t)
	require.Len(t, enqueued, 1, "only the half-open allowance may be enqueued")
	require.Same(t, spends[0], enqueued[0].spend)

	for i := 1; i < inputs; i++ {
		require.Error(t, spends[i].Err, "fast-failed input %d must carry the breaker error", i)
	}
}

// TestSpend_FiltersFromTheFirstInputWhenBreakerFullyOpen exercises the filtered
// bool against the non-nil-empty-slice trap: make([]T, 0, n) is non-nil, so a
// nil test would misbehave when the FIRST input is rejected.
func TestSpend_FiltersFromTheFirstInputWhenBreakerFullyOpen(t *testing.T) {
	const inputs = 3

	s := newTestStoreForSpendEnqueue(t)
	rec := &recordingSpendBatcher{}
	s.spendBatcher = rec

	cb := newCircuitBreaker(1, 1, time.Hour)
	require.NotNil(t, cb)

	cb.RecordFailure() // trips the breaker open, cooldown will not elapse

	s.spendCircuitBreaker = cb

	tx := txWithInputs(t, inputs)

	spends, _ := s.Spend(context.Background(), tx, 100)
	require.Len(t, spends, inputs)

	require.Empty(t, rec.enqueued(t), "an entirely rejected tx must enqueue nothing")

	for i := 0; i < inputs; i++ {
		require.Error(t, spends[i].Err, "fast-failed input %d must carry the breaker error", i)
	}
}

// TestSpend_CompletesEveryEnqueuedItemWhenSendRejected is the regression guard
// for the rejected-send loop: it ranges over the slice that was actually handed
// to the batcher, which is now the items slice itself on the common path. Every
// item must end up completed rather than parked.
func TestSpend_CompletesEveryEnqueuedItemWhenSendRejected(t *testing.T) {
	const inputs = 4

	s := newTestStoreForSpendEnqueue(t)
	s.spendBatcher = sendOnClosedBatcher[batchSpend]{}
	s.spendCircuitBreaker = nil

	tx := txWithInputs(t, inputs)

	// A rejected send fails the whole tx, so Spend returns an error; what
	// matters is that no input was left parked on the completion group.
	spends, err := s.Spend(context.Background(), tx, 100)
	require.Error(t, err)
	require.Len(t, spends, inputs)

	for i, spend := range spends {
		require.Error(t, spend.Err, "input %d must be completed with the shutdown error, not parked", i)
		require.Contains(t, spend.Err.Error(), "shutting down")
	}
}
