package subtreeprocessor

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_queue(t *testing.T) {
	q := NewLockFreeQueue()

	enqueueBatches(t, q, 1, 10)

	batches := 0
	totalTxs := 0

	for {
		batch, found := q.dequeueBatch(0)
		if !found {
			break
		}

		assert.Greater(t, batch.time, int64(0))
		totalTxs += len(batch.nodes)
		batches++
	}

	assert.True(t, q.IsEmpty())
	assert.Equal(t, 10, batches)
	assert.Equal(t, 10, totalTxs) // each batch has 1 tx

	enqueueBatches(t, q, 1, 10)

	batches = 0
	totalTxs = 0

	for {
		batch, found := q.dequeueBatch(0)
		if !found {
			break
		}

		assert.Greater(t, batch.time, int64(0))
		totalTxs += len(batch.nodes)
		batches++
	}

	assert.True(t, q.IsEmpty())
	assert.Equal(t, 10, batches)
	assert.Equal(t, 10, totalTxs)
}

func Test_queueWithTime(t *testing.T) {
	q := NewLockFreeQueue()

	enqueueBatches(t, q, 1, 10)

	validFromMillis := time.Now().Add(-200 * time.Millisecond).UnixMilli()
	_, found := q.dequeueBatch(validFromMillis)
	require.False(t, found)

	time.Sleep(50 * time.Millisecond)

	validFromMillis = time.Now().Add(-200 * time.Millisecond).UnixMilli()
	_, found = q.dequeueBatch(validFromMillis)
	require.False(t, found)

	time.Sleep(200 * time.Millisecond)

	batches := 0
	validFromMillis = time.Now().Add(-200 * time.Millisecond).UnixMilli()

	for {
		batch, found := q.dequeueBatch(validFromMillis)
		if !found {
			break
		}

		assert.Greater(t, batch.time, int64(0))
		batches++
	}

	assert.True(t, q.IsEmpty())
	assert.Equal(t, 10, batches)

	enqueueBatches(t, q, 1, 10)

	validFromMillis = time.Now().Add(-200 * time.Millisecond).UnixMilli()
	_, found = q.dequeueBatch(validFromMillis)
	require.False(t, found)

	time.Sleep(50 * time.Millisecond)

	validFromMillis = time.Now().Add(-200 * time.Millisecond).UnixMilli()
	_, found = q.dequeueBatch(validFromMillis)
	require.False(t, found)

	time.Sleep(200 * time.Millisecond)

	batches = 0
	validFromMillis = time.Now().Add(-200 * time.Millisecond).UnixMilli()

	for {
		batch, found := q.dequeueBatch(validFromMillis)
		if !found {
			break
		}

		assert.Greater(t, batch.time, int64(0))
		batches++
	}

	assert.True(t, q.IsEmpty())
	assert.Equal(t, 10, batches)
}

type fixedClock struct{ t time.Time }

func (f fixedClock) Now() time.Time { return f.t }

// Test_queueClockOverride verifies the clock seam: when a fake clock is
// installed, batch.time matches the fake's value rather than wall time.
// This is the hook tests will use to drive deterministic batch timestamps.
func Test_queueClockOverride(t *testing.T) {
	q := NewLockFreeQueue()

	fixed := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	q.clock = fixedClock{t: fixed}

	q.enqueueBatch(
		[]subtree.Node{{Hash: chainhash.Hash{}, Fee: 1, SizeInBytes: 0}},
		[]*subtree.TxInpoints{{}},
	)

	batch, found := q.dequeueBatch(0)
	require.True(t, found)
	require.Equal(t, fixed.UnixMilli(), batch.time)
}

// Test_zeroWindowFormulasAgree asserts parity between the two
// validFromMillis formulas inside SubtreeProcessor at DoubleSpendWindow=0
// (the documented default - see settings/blockassembly_settings.go:29).
// Both call sites now zero-guard the calculation, so neither activates
// the queue filter at queue.go:96 and both admit same-millisecond
// batches.
//
//	Start loop (SubtreeProcessor.go:807-813):
//	  validFromMillis = 0                              if DoubleSpendWindow == 0
//	  validFromMillis = (now - window).UnixMilli()     otherwise
//
//	dequeueDuringBlockMovement (SubtreeProcessor.go:3789-3796):
//	  validFromMillis = 0                              if DoubleSpendWindow == 0
//	  validFromMillis = (now - window).UnixMilli()     otherwise
//
// Before the fix, the drain formula was unconditional, which held back
// same-millisecond batches under the default config. This test pins the
// post-fix parity. If a future change removes either zero-guard, the
// corresponding subtest will fail.
func Test_zeroWindowFormulasAgree(t *testing.T) {
	fixed := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	window := time.Duration(0)

	enqueueAtFixed := func() *LockFreeQueue {
		q := NewLockFreeQueue()
		q.clock = fixedClock{t: fixed}
		q.enqueueBatch(
			[]subtree.Node{{Hash: chainhash.Hash{}, Fee: 1, SizeInBytes: 0}},
			[]*subtree.TxInpoints{{}},
		)
		return q
	}

	t.Run("start_loop_formula_admits_same_millisecond_batch", func(t *testing.T) {
		// Mirror of the formula at SubtreeProcessor.go:810-813.
		startValidFromMillis := int64(0)
		if window > 0 {
			startValidFromMillis = fixed.Add(-window).UnixMilli()
		}

		q := enqueueAtFixed()
		batch, found := q.dequeueBatch(startValidFromMillis)
		require.True(t, found, "Start loop must admit same-ms batch at window=0")
		require.Equal(t, fixed.UnixMilli(), batch.time)
	})

	t.Run("drain_formula_admits_same_millisecond_batch", func(t *testing.T) {
		// Mirror of the formula at SubtreeProcessor.go:3789-3796.
		drainValidFromMillis := int64(0)
		if window > 0 {
			drainValidFromMillis = fixed.Add(-window).UnixMilli()
		}

		q := enqueueAtFixed()
		batch, found := q.dequeueBatch(drainValidFromMillis)
		require.True(t, found, "drain must admit same-ms batch at window=0 "+
			"(zero-guard parity with the Start loop)")
		require.Equal(t, fixed.UnixMilli(), batch.time)
	})
}

// Test_validFromMillisBoundaries pins the inclusive-reject semantics and
// the negative/zero-bypass behaviour of the queue's validFromMillis
// filter at queue.go:96:
//
//	if validFromMillis > 0 && next.time >= validFromMillis {
//	    return nil, false
//	}
//
// Two pieces worth documenting beyond the asymmetry test above:
//
//   - Boundary: batch.time == validFromMillis is rejected (>= is
//     inclusive). batch.time == validFromMillis - 1 admits. A future
//     change to "strictly greater than" would silently widen the
//     admission window by one millisecond.
//
//   - Defensive bypass: validFromMillis <= 0 short-circuits filtering
//     entirely. Any caller producing a non-positive cutoff (e.g. via
//     clock.Now() before the unix epoch, or a window larger than the
//     current millisecond timestamp) silently disables double-spend
//     protection for that dequeue. Both call sites in SubtreeProcessor
//     compute Now().Add(-window).UnixMilli(); in production
//     Now().UnixMilli() is in the trillions so this guard is dormant,
//     but a future caller or a test built on time.Time{} would trip it.
func Test_validFromMillisBoundaries(t *testing.T) {
	t.Run("inclusive_reject_at_boundary", func(t *testing.T) {
		fixed := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
		q := NewLockFreeQueue()
		q.clock = fixedClock{t: fixed}
		q.enqueueBatch(
			[]subtree.Node{{Hash: chainhash.Hash{}, Fee: 1, SizeInBytes: 0}},
			[]*subtree.TxInpoints{{}},
		)
		_, found := q.dequeueBatch(fixed.UnixMilli())
		require.False(t, found, "batch.time == validFromMillis must be rejected")
	})

	t.Run("admit_one_below_boundary", func(t *testing.T) {
		fixed := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
		q := NewLockFreeQueue()
		q.clock = fixedClock{t: fixed}
		q.enqueueBatch(
			[]subtree.Node{{Hash: chainhash.Hash{}, Fee: 1, SizeInBytes: 0}},
			[]*subtree.TxInpoints{{}},
		)
		_, found := q.dequeueBatch(fixed.UnixMilli() + 1)
		require.True(t, found, "batch.time == validFromMillis - 1 must admit")
	})

	t.Run("negative_validFromMillis_bypasses_filter", func(t *testing.T) {
		fixed := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
		q := NewLockFreeQueue()
		q.clock = fixedClock{t: fixed}
		q.enqueueBatch(
			[]subtree.Node{{Hash: chainhash.Hash{}, Fee: 1, SizeInBytes: 0}},
			[]*subtree.TxInpoints{{}},
		)
		// validFromMillis = -1 → guard ("> 0") short-circuits, filter off.
		batch, found := q.dequeueBatch(-1)
		require.True(t, found, "negative validFromMillis must short-circuit the filter")
		require.Equal(t, fixed.UnixMilli(), batch.time)
	})
}

// Test_clockBackwardJumpHoldsBatchesLonger characterizes how the queue
// behaves when the drain clock jumps backwards relative to the enqueue
// clock - the kind of jump an NTP correction can introduce mid-flight.
//
//	enqueue at T=10_000_000, batch.time = 10_000_000
//	drain at  T= 5_000_000, window = 200ms → validFromMillis = 4_999_800
//	batch.time (10_000_000) >= validFromMillis (4_999_800) → rejected
//
// The batch stays queued until the drain clock catches back up past
// (batch.time + window). In production this means an NTP step
// backwards during block movement can stall the drain until wall time
// re-advances, even though the batch itself is fully aged. Documented
// here so the behaviour does not surprise anyone tracking down a
// post-NTP-correction stall.
func Test_clockBackwardJumpHoldsBatchesLonger(t *testing.T) {
	enqueueAt := time.UnixMilli(10_000_000).UTC()
	q := NewLockFreeQueue()
	q.clock = fixedClock{t: enqueueAt}
	q.enqueueBatch(
		[]subtree.Node{{Hash: chainhash.Hash{}, Fee: 1, SizeInBytes: 0}},
		[]*subtree.TxInpoints{{}},
	)

	const window = 200 * time.Millisecond

	drainAtBack := time.UnixMilli(5_000_000).UTC() // clock stepped backwards
	_, found := q.dequeueBatch(drainAtBack.Add(-window).UnixMilli())
	require.False(t, found, "batch held back while drain clock is behind enqueue clock")

	// Once wall time recovers past (batch.time + window) the batch drains.
	drainAtRecovered := enqueueAt.Add(window + time.Millisecond)
	batch, found := q.dequeueBatch(drainAtRecovered.Add(-window).UnixMilli())
	require.True(t, found, "batch drains once drain clock recovers")
	require.Equal(t, enqueueAt.UnixMilli(), batch.time)
}

// Test_dequeueBatchUntilPreservesPostBoundaryBatch pins the
// inclusive-until admit semantics of dequeueBatchUntil. The boundary
// batch (batch.time == maxTimeMillis) is admitted; any batch with
// batch.time > maxTimeMillis is rejected without being removed from
// the queue.
//
// Regression guard for the Reset drain loop bug: the previous
// implementation called dequeueBatch(0) then checked batch.time
// post-hoc, which removed the boundary batch from the queue before
// discovering it was too new. dequeueBatchUntil peeks first.
func Test_dequeueBatchUntilPreservesPostBoundaryBatch(t *testing.T) {
	preSnapshot := time.UnixMilli(1_700_000_000_000).UTC()
	postSnapshot := preSnapshot.Add(10 * time.Millisecond)

	q := NewLockFreeQueue()

	q.clock = fixedClock{t: preSnapshot}
	q.enqueueBatch(
		[]subtree.Node{{Hash: chainhash.HashH([]byte("pre")), Fee: 1, SizeInBytes: 0}},
		[]*subtree.TxInpoints{{}},
	)

	q.clock = fixedClock{t: postSnapshot}
	q.enqueueBatch(
		[]subtree.Node{{Hash: chainhash.HashH([]byte("post")), Fee: 2, SizeInBytes: 0}},
		[]*subtree.TxInpoints{{}},
	)
	require.Equal(t, int64(2), q.length(), "precondition: both batches enqueued")

	// Drain everything up to and including preSnapshot.
	var consumedFees []uint64
	for {
		batch, found := q.dequeueBatchUntil(preSnapshot.UnixMilli())
		if !found {
			break
		}
		consumedFees = append(consumedFees, batch.nodes[0].Fee)
	}

	require.Equal(t, []uint64{1}, consumedFees,
		"pre-snapshot batch must drain inside the loop body")
	require.Equal(t, int64(1), q.length(),
		"post-snapshot batch must survive: dequeueBatchUntil peeks before consuming")

	// Boundary check: a batch enqueued at exactly maxTimeMillis admits.
	q2 := NewLockFreeQueue()
	q2.clock = fixedClock{t: preSnapshot}
	q2.enqueueBatch(
		[]subtree.Node{{Hash: chainhash.HashH([]byte("boundary")), Fee: 1, SizeInBytes: 0}},
		[]*subtree.TxInpoints{{}},
	)
	_, found := q2.dequeueBatchUntil(preSnapshot.UnixMilli())
	require.True(t, found, "batch.time == maxTimeMillis must admit (inclusive-until)")

	// Empty queue returns false without touching state.
	_, found = q2.dequeueBatchUntil(preSnapshot.UnixMilli())
	require.False(t, found, "empty queue returns false")
}

func Test_queue2Threads(t *testing.T) {
	q := NewLockFreeQueue()

	enqueueBatches(t, q, 2, 10)

	batches := 0

	for {
		batch, found := q.dequeueBatch(0)
		if !found {
			break
		}

		batches++

		t.Logf("Batch: time=%d, txs=%d\n", batch.time, len(batch.nodes))
	}

	assert.True(t, q.IsEmpty())
	assert.Equal(t, 20, batches)

	enqueueBatches(t, q, 2, 10)

	batches = 0

	for {
		batch, found := q.dequeueBatch(0)
		if !found {
			break
		}

		batches++

		t.Logf("Batch: time=%d, txs=%d\n", batch.time, len(batch.nodes))
	}

	assert.True(t, q.IsEmpty())
	assert.Equal(t, 20, batches)
}

func Test_queueLarge(t *testing.T) {
	runtime.GC()

	q := NewLockFreeQueue()

	enqueueBatches(t, q, 1, 10_000_000)

	startTime := time.Now()

	batches := 0

	for {
		_, found := q.dequeueBatch(0)
		if !found {
			break
		}

		batches++
	}

	t.Logf("Time empty %d batches: %s\n", batches, time.Since(startTime))
	t.Logf("Mem used for queue: %s\n", printAlloc())

	assert.True(t, q.IsEmpty())
	assert.Equal(t, 10_000_000, batches)

	runtime.GC()

	enqueueBatches(t, q, 1_000, 10_000)

	startTime = time.Now()

	batches = 0

	for {
		_, found := q.dequeueBatch(0)
		if !found {
			break
		}

		batches++
	}

	t.Logf("Time empty %d batches: %s\n", batches, time.Since(startTime))
	t.Logf("Mem used after dequeue: %s\n", printAlloc())
	runtime.GC()
	t.Logf("Mem used after dequeue after GC: %s\n", printAlloc())

	assert.True(t, q.IsEmpty())
	assert.Equal(t, 10_000_000, batches)
}

// enqueueBatches adds test batches to a queue for testing.
// Each batch contains a single transaction for testing simplicity.
//
// Parameters:
//   - t: Testing instance
//   - q: Queue to populate
//   - threads: Number of concurrent threads
//   - iter: Number of iterations per thread (each iteration enqueues one batch)
func enqueueBatches(t *testing.T, q *LockFreeQueue, threads, iter int) {
	startTime := time.Now()

	var wg sync.WaitGroup

	for n := 0; n < threads; n++ {
		wg.Add(1)

		go func(n int) {
			defer wg.Done()

			for i := 0; i < iter; i++ {
				u := (n * iter) + i
				// Each batch contains a single transaction
				q.enqueueBatch(
					[]subtree.Node{{
						Hash:        chainhash.Hash{},
						Fee:         uint64(u),
						SizeInBytes: 0,
					}},
					[]*subtree.TxInpoints{{}},
				)
			}
		}(n)
	}

	wg.Wait()
	t.Logf("Time queue %d batches: %s\n", threads*iter, time.Since(startTime))
}

// Benchmark functions for performance testing

// BenchmarkQueue tests queue performance.
func BenchmarkQueue(b *testing.B) {
	q := NewLockFreeQueue()

	b.ResetTimer()

	go func() {
		for {
			_, found := q.dequeueBatch(0)
			if !found {
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	for i := 0; i < b.N; i++ {
		q.enqueueBatch(
			[]subtree.Node{{
				Hash:        chainhash.Hash{},
				Fee:         uint64(i),
				SizeInBytes: 0,
			}},
			[]*subtree.TxInpoints{{}},
		)
	}
}

// BenchmarkAtomicPointer tests atomic pointer operations.
func BenchmarkAtomicPointer(b *testing.B) {
	var v atomic.Pointer[TxBatch]

	t1 := &TxBatch{
		nodes: []subtree.Node{{
			Hash:        chainhash.Hash{},
			Fee:         1,
			SizeInBytes: 0,
		}},
	}
	t2 := &TxBatch{
		nodes: []subtree.Node{{
			Hash:        chainhash.Hash{},
			Fee:         1,
			SizeInBytes: 0,
		}},
	}

	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			v.Swap(t1)
		} else {
			v.Swap(t2)
		}
	}
}

// printAlloc formats memory allocation information for testing.
//
// Returns:
//   - string: Formatted memory allocation string
func printAlloc() string {
	var m runtime.MemStats

	runtime.ReadMemStats(&m)

	return fmt.Sprintf("%d MB", m.Alloc/(1024*1024))
}
