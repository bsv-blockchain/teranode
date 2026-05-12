package subtreeprocessor

import (
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// The queue-filter contract at queue.go:96 is:
//
//	if validFromMillis > 0 && next.time >= validFromMillis {
//	    return nil, false
//	}
//
// which is equivalent to: admit iff (validFromMillis <= 0) OR
// (batch.time < validFromMillis). The properties in this file pin that
// contract directly and at the SubtreeProcessor integration level so a
// future change to either the filter or the validFromMillis formulas
// trips a generated counter-example rather than waiting for a wall-clock
// flake to surface it.

const (
	realisticMinMillis = int64(1_500_000_000_000) // 2017-07-14
	realisticMaxMillis = int64(2_000_000_000_000) // 2033-05-18
)

// Test_propertyDequeueBatchAdmitPredicate asserts that LockFreeQueue's
// dequeueBatch admit/reject decision matches the documented predicate
// for any (batch_time, validFromMillis) pair, including negative and
// zero validFromMillis (the short-circuit case) and the boundary
// batch.time == validFromMillis (inclusive reject).
func Test_propertyDequeueBatchAdmitPredicate(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		batchTimeMs := rapid.Int64Range(realisticMinMillis, realisticMaxMillis).Draw(t, "batchTimeMs")
		batchTime := time.UnixMilli(batchTimeMs).UTC()

		// Cover negative, zero, and positive validFromMillis around the
		// batch time so the boundary cases are hit frequently.
		validFromMillis := rapid.OneOf(
			rapid.Int64Range(-1000, 0),                               // bypass region
			rapid.Int64Range(batchTimeMs-10, batchTimeMs+10),         // boundary region
			rapid.Int64Range(realisticMinMillis, realisticMaxMillis), // unrelated cutoffs
		).Draw(t, "validFromMillis")

		q := NewLockFreeQueue()
		q.clock = fixedClock{t: batchTime}
		q.enqueueBatch(
			[]subtreepkg.Node{{Hash: chainhash.Hash{}, Fee: 1, SizeInBytes: 0}},
			[]*subtreepkg.TxInpoints{{}},
		)

		_, admitted := q.dequeueBatch(validFromMillis)
		want := validFromMillis <= 0 || batchTimeMs < validFromMillis
		require.Equal(t, want, admitted,
			"admit/reject diverged from queue.go:96 spec: batch_time=%d, validFromMillis=%d",
			batchTimeMs, validFromMillis)
	})
}

// Test_propertyDrainAdmitInvariant drives dequeueDuringBlockMovement
// with random (window, enqueue_clock, drain_clock) and asserts the
// integration-level outcome matches the expected admit predicate. This
// catches regressions in either the validFromMillis formula at
// SubtreeProcessor.go:3789-3796 or the queue filter at queue.go:96.
//
// The expected predicate composes both call sites:
//
//	validFromMillis = 0                                      if window == 0
//	validFromMillis = (drain_clock - window).UnixMilli()     otherwise
//	admit iff validFromMillis <= 0 OR batch.time < validFromMillis
func Test_propertyDrainAdmitInvariant(outerT *testing.T) {
	rapid.Check(outerT, func(t *rapid.T) {
		windowMs := rapid.Int64Range(0, 10_000).Draw(t, "windowMs")
		window := time.Duration(windowMs) * time.Millisecond

		enqueueMs := rapid.Int64Range(realisticMinMillis, realisticMaxMillis).Draw(t, "enqueueMs")
		enqueueTime := time.UnixMilli(enqueueMs).UTC()

		// Drain clock relative to enqueue: covers backward jumps,
		// same-instant, and arbitrary forward advances.
		deltaMs := rapid.Int64Range(-10_000, 10_000).Draw(t, "deltaMs")
		drainTime := enqueueTime.Add(time.Duration(deltaMs) * time.Millisecond)

		// newTestProcessorNoStart takes *testing.T; cleanups accumulate
		// across rapid iterations and are released when the outer test
		// finishes. Acceptable at the default ~100 iterations.
		stp := newTestProcessorNoStart(outerT)
		stp.settings.BlockAssembly.DoubleSpendWindow = window
		stp.queue.clock = fixedClock{t: enqueueTime}
		stp.clock = fixedClock{t: drainTime}

		txHash := chainhash.HashH([]byte("rapid-drain"))
		stp.queue.enqueueBatch(
			[]subtreepkg.Node{{Hash: txHash, Fee: 1, SizeInBytes: 220}},
			[]*subtreepkg.TxInpoints{{}},
		)
		require.Equal(t, int64(1), stp.queue.length())

		require.NoError(t, stp.dequeueDuringBlockMovement(nil, nil, nil, true))

		var validFromMillis int64
		if window > 0 {
			validFromMillis = drainTime.Add(-window).UnixMilli()
		}
		wantAdmit := validFromMillis <= 0 || enqueueMs < validFromMillis

		ctx := fmt.Sprintf("window=%v, enqueueMs=%d, drainMs=%d, validFromMillis=%d",
			window, enqueueMs, drainTime.UnixMilli(), validFromMillis)

		if wantAdmit {
			require.Equal(t, int64(0), stp.queue.length(),
				"expected admit but batch still queued: %s", ctx)
			require.Contains(t, collectSubtreeHashes(stp), txHash,
				"admitted batch must appear in subtree: %s", ctx)
		} else {
			require.Equal(t, int64(1), stp.queue.length(),
				"expected reject but batch drained: %s", ctx)
			require.NotContains(t, collectSubtreeHashes(stp), txHash,
				"rejected batch must not appear in subtree: %s", ctx)
		}
	})
}

// Test_propertyAgingAlwaysAdmits asserts that for any
// (window, enqueue_time), advancing the drain clock past
// (enqueue_time + window) admits the batch. This is the liveness
// counterpart to the boundary tests: anything that ages past its
// window must eventually drain, no matter the window length.
func Test_propertyAgingAlwaysAdmits(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		windowMs := rapid.Int64Range(0, 10_000).Draw(t, "windowMs")
		window := time.Duration(windowMs) * time.Millisecond
		enqueueMs := rapid.Int64Range(realisticMinMillis, realisticMaxMillis).Draw(t, "enqueueMs")
		enqueueTime := time.UnixMilli(enqueueMs).UTC()

		// Fully age the batch: drain at enqueue + window + 1ms.
		drainTime := enqueueTime.Add(window).Add(time.Millisecond)

		q := NewLockFreeQueue()
		q.clock = fixedClock{t: enqueueTime}
		q.enqueueBatch(
			[]subtreepkg.Node{{Hash: chainhash.Hash{}, Fee: 1, SizeInBytes: 0}},
			[]*subtreepkg.TxInpoints{{}},
		)

		var validFromMillis int64
		if window > 0 {
			validFromMillis = drainTime.Add(-window).UnixMilli()
		}

		_, admitted := q.dequeueBatch(validFromMillis)
		require.True(t, admitted,
			"fully-aged batch failed to admit: window=%v, enqueueMs=%d, drainMs=%d, validFromMillis=%d",
			window, enqueueMs, drainTime.UnixMilli(), validFromMillis)
	})
}
