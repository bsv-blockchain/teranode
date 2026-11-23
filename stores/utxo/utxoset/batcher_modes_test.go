package utxoset

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

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

// settledBatchSizeStats is batcherBatchSizeStats taken only once every triggered batch has been
// observed in the histogram.
//
// go-batcher bumps teranode_batcher_batches_total before it runs the batch fn, but observes
// teranode_batcher_batch_size in a defer after the fn returns, and utxoset answers each caller
// from inside the fn. So a caller can return before its batch reaches the histogram: a read
// taken right after a load can miss its last batches, and a later window can pick them up.
// Every dispatch records both, so the histogram has caught up once its count equals the
// counter's total; polling for that makes the window boundaries exact.
func settledBatchSizeStats(t *testing.T, name string) (sum float64, count uint64) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)

	for {
		sum, count = batcherBatchSizeStats(t, name)
		triggered := batcherFlushCount(t, name)

		if float64(count) == triggered {
			return sum, count
		}

		if time.Now().After(deadline) {
			require.Failf(t, "batch_size histogram never caught up with batches_total",
				"batcher %q: %d batches observed, %v triggered", name, count, triggered)
		}

		time.Sleep(5 * time.Millisecond)
	}
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
// Neither the flush count nor the batch size under the concurrent burst says anything about
// drain mode: when 2,000 callers all land inside the same instant, drain legitimately takes
// full 500-item batches, so a count- or average-based bound converges on the greedy shape on a
// slow or contended CI box and flakes. Two properties hold regardless of timing, and the test
// asserts only those:
//
//   - under the concurrent burst, every flush is stamped reason=drain (the drain branch in
//     go-batcher's worker loop never falls through to the size- or timeout-triggered paths);
//   - with one caller at a time, the queue never holds more than one item, so drain must fire
//     each lone item immediately as a batch of exactly one rather than wait for the cap or the
//     timer. That is the adaptive behaviour, measured without depending on scheduling.
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
			sumBefore, countBefore := settledBatchSizeStats(t, "utxoset_spend_and_create")

			driveSpendAndCreateLoad(t, s, ctx, batcherModeCallers, 100, uint64(1000+attempt)*10_000_000)

			reasonsAfter := batcherReasonCounts(t, "utxoset_spend_and_create")
			sumAfter, countAfter := settledBatchSizeStats(t, "utxoset_spend_and_create")

			drainFlushes := reasonsAfter[batcher.ReasonDrain] - reasonsBefore[batcher.ReasonDrain]
			sizeFlushes := reasonsAfter[batcher.ReasonSize] - reasonsBefore[batcher.ReasonSize]
			timeoutFlushes := reasonsAfter[batcher.ReasonTimeout] - reasonsBefore[batcher.ReasonTimeout]

			t.Logf("drain mode burst: %d flushes reason=drain, %d reason=size, %d reason=timeout, %d dispatched batches, %.0f items",
				int64(drainFlushes), int64(sizeFlushes), int64(timeoutFlushes), countAfter-countBefore, sumAfter-sumBefore)

			require.Positivef(t, drainFlushes, "drain mode must report at least one flush with reason=drain")
			require.Zerof(t, sizeFlushes, "drain mode must never report a size-triggered flush, got %v", sizeFlushes)
			require.Zerof(t, timeoutFlushes, "drain mode must never report a timeout-triggered flush, got %v", timeoutFlushes)

			// Sequential callers: each SpendAndCreate blocks until its batch lands, so only one
			// item is ever queued and drain must dispatch it on its own.
			const sequentialCallers = 20

			seqSumBefore, seqCountBefore := settledBatchSizeStats(t, "utxoset_spend_and_create")
			seqReasonsBefore := batcherReasonCounts(t, "utxoset_spend_and_create")

			for i := 0; i < sequentialCallers; i++ {
				driveSpendAndCreateLoad(t, s, ctx, 1, 100, uint64(2000+attempt)*10_000_000+uint64(i)) //nolint:gosec // test id spread, no overflow at this scale
			}

			seqSumAfter, seqCountAfter := settledBatchSizeStats(t, "utxoset_spend_and_create")
			seqReasonsAfter := batcherReasonCounts(t, "utxoset_spend_and_create")

			require.Equalf(t, uint64(sequentialCallers), seqCountAfter-seqCountBefore,
				"drain mode must flush each lone queued item as its own batch")
			require.InDeltaf(t, float64(sequentialCallers), seqSumAfter-seqSumBefore, 0,
				"every sequential drain batch must hold exactly one item")
			require.InDeltaf(t, float64(sequentialCallers),
				seqReasonsAfter[batcher.ReasonDrain]-seqReasonsBefore[batcher.ReasonDrain], 0,
				"every sequential flush must be stamped reason=drain")
		})
	}
}
