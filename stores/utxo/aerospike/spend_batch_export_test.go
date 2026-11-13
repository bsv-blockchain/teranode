package aerospike

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// SendSpendBatchForTest drives an exact batch through the real backend. It is
// compiled only for tests so external-package integration tests need no sleeps
// to establish batch membership or ordering.
func (s *Store) SendSpendBatchForTest(t *testing.T, spends []*utxo.Spend, height uint32) {
	t.Helper()
	group := completion.NewGroup(int32(len(spends)))
	batch := make([]*batchSpend, len(spends))
	for i, spend := range spends {
		batch[i] = &batchSpend{spend: spend, blockHeight: height, group: group}
	}
	s.sendSpendBatchLua(batch)
	require.NoError(t, group.Wait(context.Background(), time.Second), "all batch claims must complete")
}

// SpendHeldForTest runs Spend for tx with a batcher that hands the store's claim
// back, then dispatches before followed by that claim as one batch. The part that
// carries the claim closes entered once it started and then waits for release.
// It returns the channel that receives Spend's error and the channel closed when
// the batch has finished.
func (s *Store) SpendHeldForTest(ctx context.Context, tx *bt.Tx, height uint32, before []*utxo.Spend, entered chan<- struct{}, release <-chan struct{}) (<-chan error, <-chan struct{}, error) {
	queued := make(chan *batchSpend, 1)
	s.spendBatcher = batcher.NewWithPool(1, time.Millisecond, func(items []*batchSpend) { queued <- items[0] }, false)

	result := make(chan error, 1)
	go func() { _, err := s.Spend(ctx, tx, height); result <- err }()

	var claim *batchSpend

	select {
	case claim = <-queued:
	case <-time.After(3 * time.Second):
		return nil, nil, errors.NewProcessingError("claim not queued")
	}

	group := completion.NewGroup(int32(len(before)))
	batch := make([]*batchSpend, 0, len(before)+1)

	for _, spend := range before {
		batch = append(batch, &batchSpend{spend: spend, blockHeight: height, group: group})
	}

	batch = append(batch, claim)

	s.beforeSpendPart = func(part []*batchSpend) {
		for _, item := range part {
			if item == claim {
				close(entered)
				<-release

				return
			}
		}
	}

	done := make(chan struct{})
	go func() { defer close(done); s.sendSpendBatchLua(batch) }()

	return result, done, nil
}

// SpendAbortInFlightForTest reports the in-flight abort counter.
func SpendAbortInFlightForTest(t *testing.T) float64 {
	t.Helper()

	var metric dto.Metric
	require.NoError(t, prometheusUtxoSpendAbortInFlight.Write(&metric))

	return metric.GetCounter().GetValue()
}

// SpendPublishedForTest spends one claim through the real backend, as a claim
// that has already published its success, and returns a func that marks the
// claim orphaned, as a caller that gave up after the publish would.
func (s *Store) SpendPublishedForTest(t *testing.T, spend *utxo.Spend, height uint32) (orphan func()) {
	t.Helper()

	group := completion.NewGroup(1)
	item := &batchSpend{spend: spend, blockHeight: height, group: group, done: make(chan struct{}), onOrphan: s.rollbackOrphanedSpend}

	s.sendSpendBatchLua([]*batchSpend{item})
	require.NoError(t, group.Wait(context.Background(), time.Second), "the claim must have completed")
	require.NoError(t, item.err, "the claim must have succeeded for this test to exercise the orphan path")

	return item.markOrphaned
}

// SpendSaturatedForTest runs Spend for tx against a spend batcher whose channel
// is full and whose callback blocks until release is closed, then dispatches
// every real claim through the backend. It returns the channel that receives
// Spend's error.
func (s *Store) SpendSaturatedForTest(ctx context.Context, tx *bt.Tx, height uint32, release <-chan struct{}) <-chan error {
	s.spendBatcher = batcher.NewWithPool(1, time.Millisecond, func(items []*batchSpend) {
		<-release

		real := make([]*batchSpend, 0, len(items))

		for _, item := range items {
			if item.group != nil {
				real = append(real, item)
			}
		}

		if len(real) > 0 {
			s.sendSpendBatchLua(real)
		}
	}, false)

	for i := 0; i < 1000; i++ {
		go s.spendBatcher.Put(&batchSpend{spend: &utxo.Spend{}})
	}

	time.Sleep(300 * time.Millisecond)

	result := make(chan error, 1)
	go func() { _, err := s.Spend(ctx, tx, height); result <- err }()

	return result
}

// SpendHeldAfterFailureForTest runs Spend for a tx of two inputs. The first
// input's claim is dispatched alone and completes at once, and the second
// input's claim is dispatched alone with its part held until release is closed.
// The part closes entered once it started. It returns the channel that receives
// Spend's error and the channel closed when the held claim has finished.
func (s *Store) SpendHeldAfterFailureForTest(ctx context.Context, tx *bt.Tx, height uint32, entered chan<- struct{}, release <-chan struct{}) (<-chan error, <-chan struct{}, error) {
	queued := make(chan *batchSpend, 4)
	s.spendBatcher = batcher.NewWithPool(2, time.Millisecond, func(items []*batchSpend) {
		for _, item := range items {
			queued <- item
		}
	}, false)

	result := make(chan error, 1)
	go func() { _, err := s.Spend(ctx, tx, height); result <- err }()

	var first, claim *batchSpend

	for i := 0; i < 2; i++ {
		select {
		case item := <-queued:
			if first == nil {
				first = item
			} else {
				claim = item
			}
		case <-time.After(3 * time.Second):
			return nil, nil, errors.NewProcessingError("claims not queued")
		}
	}

	s.sendSpendBatchLua([]*batchSpend{first})

	s.beforeSpendPart = func(part []*batchSpend) {
		for _, item := range part {
			if item == claim {
				close(entered)
				<-release

				return
			}
		}
	}

	done := make(chan struct{})
	go func() { defer close(done); s.sendSpendBatchLua([]*batchSpend{claim}) }()

	return result, done, nil
}

// SpendRetryForTest runs Spend for tx against a spend batcher that dispatches
// every claim through the real backend at once, with a wait budget of wait. It
// returns the channel that receives Spend's error.
func (s *Store) SpendRetryForTest(ctx context.Context, tx *bt.Tx, height uint32, wait time.Duration) <-chan error {
	s.settings.UtxoStore.SpendWaitTimeout = wait
	s.spendBatcher = batcher.NewWithPool(1, time.Millisecond, func(items []*batchSpend) { s.sendSpendBatchLua(items) }, false)

	result := make(chan error, 1)
	go func() { _, err := s.Spend(ctx, tx, height); result <- err }()

	return result
}
