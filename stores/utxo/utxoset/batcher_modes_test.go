package utxoset

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"

	batcher "github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// batcherModeCallers is the concurrent SpendAndCreate load both tests drive. batcherModeSize is
// the batch cap configured for the store/create batcher, chosen small enough that the load
// forces several flushes rather than fitting in one.
const (
	batcherModeCallers = 2000
	batcherModeSize    = 500
)

// batcherFlushCount reads the current value of teranode_batcher_batches_total, summed across
// every trigger reason, for the named batcher.
//
// The counter lives on the process-wide registry every utxoset batcher reports through (see
// util/batchermetrics: one shared prometheus.Metrics provider, registered once). It is
// cumulative and shared with every other test in the package, so a test reads it before and
// after its own load and asserts on the delta rather than the absolute value.
func batcherFlushCount(t *testing.T, name string) float64 {
	t.Helper()

	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	var total float64

	for _, mf := range mfs {
		if mf.GetName() != "teranode_batcher_batches_total" {
			continue
		}

		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "batcher" && lp.GetValue() == name {
					total += m.GetCounter().GetValue()
				}
			}
		}
	}

	return total
}

// batcherReasonCounts reads teranode_batcher_batches_total for the named batcher, broken out by
// the "reason" label go-batcher stamps on every flush (batcher.ReasonSize, ReasonTimeout,
// ReasonManual, ReasonDrain, ReasonShutdown). Cumulative and process-wide like
// batcherFlushCount, so callers diff two snapshots rather than reading one in isolation.
func batcherReasonCounts(t *testing.T, name string) map[string]float64 {
	t.Helper()

	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	counts := make(map[string]float64)

	for _, mf := range mfs {
		if mf.GetName() != "teranode_batcher_batches_total" {
			continue
		}

		for _, m := range mf.GetMetric() {
			var batcherName, reason string

			for _, lp := range m.GetLabel() {
				switch lp.GetName() {
				case "batcher":
					batcherName = lp.GetValue()
				case "reason":
					reason = lp.GetValue()
				}
			}

			if batcherName == name {
				counts[reason] += m.GetCounter().GetValue()
			}
		}
	}

	return counts
}

// batcherBatchSizeStats reads the sum and count of teranode_batcher_batch_size, a histogram of
// dispatched batch widths, for the named batcher. A caller wanting the average batch size over
// a window takes this before and after the window and divides the two deltas -- the average of
// an already-observed histogram cannot be read any other way, and a delta is required for the
// same reason batcherFlushCount's caller takes one: the series is shared and cumulative.
func batcherBatchSizeStats(t *testing.T, name string) (sum float64, count uint64) {
	t.Helper()

	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, mf := range mfs {
		if mf.GetName() != "teranode_batcher_batch_size" {
			continue
		}

		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "batcher" && lp.GetValue() == name {
					h := m.GetHistogram()
					sum += h.GetSampleSum()
					count += h.GetSampleCount()
				}
			}
		}
	}

	return sum, count
}

// driveSpendAndCreateLoad fires n concurrent CreateOnly SpendAndCreate calls through s, each its
// own transaction, and waits for every one of them to land.
//
// CreateOnly skips the spend phase entirely (see runSpendAndCreateBatch), so every call can
// share mkTx's fixed input without any of them contending on it; only their outputs, and so
// their ids, need to differ, which distinct satoshis values already give them.
func driveSpendAndCreateLoad(t *testing.T, s *Store, ctx context.Context, n int, height uint32, baseSats uint64) {
	t.Helper()

	var wg sync.WaitGroup

	errs := make(chan error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			tx := mkTx(t, 1, baseSats+uint64(i)) //nolint:gosec // test id spread, no overflow at this scale

			if _, _, err := s.SpendAndCreate(ctx, tx, height, utxo.WithCreateOnly()); err != nil {
				errs <- err
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
}

// TestBatcherModeGreedyAccumulateBoundsFlushCount is task 24's proof that
// StoreBatcherGreedyAccumulate, not a forced drain mode, now governs the store/create batcher:
// with drain off and greedy on, 2,000 concurrent creates against a batch cap of 500 must flush
// close to the theoretical minimum, because greedy only pulls what is already queued into the
// batch faster and never fires one early.
//
// Run five times (see the t.Run loop) because a bound this close to the minimum is the first
// place a scheduling fluke would show up as a flake.
func TestBatcherModeGreedyAccumulateBoundsFlushCount(t *testing.T) {
	bound := math.Ceil(float64(batcherModeCallers)/float64(batcherModeSize)) + 2

	for attempt := 1; attempt <= 5; attempt++ {
		t.Run(fmt.Sprintf("run%d", attempt), func(t *testing.T) {
			s, ctx := newTestStoreWith(t, func(st *settings.Settings) {
				st.UtxoStore.StoreBatcherSize = batcherModeSize
				st.UtxoStore.StoreBatcherDurationMillis = 50
				st.UtxoStore.StoreBatcherDrainMode = false
				st.UtxoStore.StoreBatcherGreedyAccumulate = true
				st.UtxoStore.BatcherMaxConcurrent = 8
				st.BatcherBackground = true
			})

			before := batcherFlushCount(t, "utxoset_spend_and_create")

			driveSpendAndCreateLoad(t, s, ctx, batcherModeCallers, 100, uint64(attempt)*10_000_000)

			flushes := batcherFlushCount(t, "utxoset_spend_and_create") - before

			t.Logf("greedy accumulate: %v flushes for %d callers (bound %v)", flushes, batcherModeCallers, bound)

			require.LessOrEqualf(t, flushes, bound,
				"greedy accumulate should keep flushes near the size-driven minimum: got %v flushes for %d callers at batch size %d",
				flushes, batcherModeCallers, batcherModeSize)
		})
	}
}

// TestBatcherModeDrainProducesAdaptiveSmallBatches is the other half of the same proof: drain
// mode still exists and still behaves as before when an operator asks for it.
//
// This does NOT assert a flush count against the greedy bound. Drain fires as soon as a worker
// is free with whatever has queued, but nothing stops it from also draining a full 500-item
// batch when 2,000 callers all land inside the same instant -- a slow or contended CI box
// stretches exactly that window, so a count-based assertion can converge on the greedy shape by
// coincidence and either flake red on a fast box or pass green while hiding a real regression
// on a slow one. The two properties that are true of drain mode REGARDLESS of timing are: every
// flush it makes is stamped reason=drain (see the drain branch in go-batcher's worker loop,
// which never falls through to the size- or timeout-triggered paths), and because it fires on
// whatever is queued rather than waiting for the queue to fill, its batches average well under
// the cap even when some individual ones happen to hit it.
func TestBatcherModeDrainProducesAdaptiveSmallBatches(t *testing.T) {
	for attempt := 1; attempt <= 5; attempt++ {
		t.Run(fmt.Sprintf("run%d", attempt), func(t *testing.T) {
			s, ctx := newTestStoreWith(t, func(st *settings.Settings) {
				st.UtxoStore.StoreBatcherSize = batcherModeSize
				st.UtxoStore.StoreBatcherDurationMillis = 50
				st.UtxoStore.StoreBatcherDrainMode = true
				st.UtxoStore.StoreBatcherGreedyAccumulate = false
				st.UtxoStore.BatcherMaxConcurrent = 8
				st.BatcherBackground = true
			})

			reasonsBefore := batcherReasonCounts(t, "utxoset_spend_and_create")
			sumBefore, countBefore := batcherBatchSizeStats(t, "utxoset_spend_and_create")

			driveSpendAndCreateLoad(t, s, ctx, batcherModeCallers, 100, uint64(1000+attempt)*10_000_000)

			reasonsAfter := batcherReasonCounts(t, "utxoset_spend_and_create")
			sumAfter, countAfter := batcherBatchSizeStats(t, "utxoset_spend_and_create")

			drainFlushes := reasonsAfter[batcher.ReasonDrain] - reasonsBefore[batcher.ReasonDrain]
			sizeFlushes := reasonsAfter[batcher.ReasonSize] - reasonsBefore[batcher.ReasonSize]
			timeoutFlushes := reasonsAfter[batcher.ReasonTimeout] - reasonsBefore[batcher.ReasonTimeout]

			flushCount := countAfter - countBefore
			require.Positive(t, flushCount, "the load must have produced at least one flush")
			avgBatchSize := (sumAfter - sumBefore) / float64(flushCount)

			t.Logf("drain mode: %d flushes reason=drain, %d reason=size, %d reason=timeout, avg batch size %.1f (cap %d)",
				int64(drainFlushes), int64(sizeFlushes), int64(timeoutFlushes), avgBatchSize, batcherModeSize)

			require.Positivef(t, drainFlushes, "drain mode must report at least one flush with reason=drain")
			require.Zerof(t, sizeFlushes, "drain mode must never report a size-triggered flush, got %v", sizeFlushes)
			require.Zerof(t, timeoutFlushes, "drain mode must never report a timeout-triggered flush, got %v", timeoutFlushes)
			require.Lessf(t, avgBatchSize, float64(batcherModeSize)/2,
				"drain mode should adapt the batch to the queue rather than converge on the cap: avg batch size %.1f, cap %d",
				avgBatchSize, batcherModeSize)
		})
	}
}
