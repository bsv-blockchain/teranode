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
