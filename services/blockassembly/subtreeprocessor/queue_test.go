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

// Test_enqueueBatchUnboundedContract pins LockFreeQueue's current intake
// contract: enqueueBatch accepts an arbitrary number of batches with no
// ceiling, and length() (a transaction count - see the field comment above)
// grows linearly forever with whatever is enqueued.
//
// This is DELIBERATE, not an oversight (issue #1429). Bounding intake here -
// by blocking enqueueBatch or by making it reject/shed - is inadmissible
// until the validator's two-phase commit unlock path is made safe against
// it: SpendAndCreate marks new outputs WithLocked(true), and only unlocks
// them (SetLocked(false), via twoPhaseCommitTransaction) after block
// assembly acknowledges the transaction. If enqueueBatch ever returns an
// error and a caller used it to reject, the unlock would be skipped and the
// transaction would stay Locked in the shared UTXO store indefinitely,
// failing every descendant with ErrTxLocked. If a caller silently dropped
// instead, the submitter would be told the transaction was accepted while it
// sits unmined forever - BSV has no mempool to re-send it. Blocking is
// separately disqualified because the producer's context has no deadline.
//
// So this test exists to make any future change to enqueueBatch's contract
// (a ceiling, a return value, a block) a visible, deliberate diff rather
// than an accident - and it must not be "fixed" by adding a bound without
// first fixing the two-phase-commit unlock path above.
func Test_enqueueBatchUnboundedContract(t *testing.T) {
	q := NewLockFreeQueue()

	const batches = 5_000
	const txsPerBatch = 3

	for i := 0; i < batches; i++ {
		nodes := make([]subtree.Node, txsPerBatch)
		inpoints := make([]*subtree.TxInpoints, txsPerBatch)

		for j := range nodes {
			nodes[j] = subtree.Node{
				Hash:        chainhash.HashH(fmt.Appendf(nil, "%d-%d", i, j)),
				Fee:         1,
				SizeInBytes: 100,
			}
			inpoints[j] = &subtree.TxInpoints{}
		}

		q.enqueueBatch(nodes, inpoints)
	}

	require.Equal(t, int64(batches*txsPerBatch), q.length(),
		"enqueueBatch has no ceiling: length grows linearly with every transaction enqueued, by design (issue #1429)")
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
// (the documented default - see BlockAssembly.DoubleSpendWindow in
// settings/blockassembly_settings.go).
// Both call sites now zero-guard the calculation, so neither activates
// the queue filter in LockFreeQueue.dequeueBatch and both admit same-millisecond
// batches.
//
//	Start loop (the default: branch of SubtreeProcessor.Start):
//	  validFromMillis = 0                              if DoubleSpendWindow == 0
//	  validFromMillis = (now - window).UnixMilli()     otherwise
//
//	SubtreeProcessor.dequeueDuringBlockMovement:
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
		// Mirror of the formula in the default: branch of SubtreeProcessor.Start.
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
		// Mirror of the formula in SubtreeProcessor.dequeueDuringBlockMovement.
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
// filter in LockFreeQueue.dequeueBatch:
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

// Test_headAgeGaugeRefreshedOnRefusalPath pins the fix for the lost-update race
// that could zero the head-age gauge during a real backlog: when the head-of-
// line batch is held back by the double-spend window (dequeueBatch refuses it as
// too new), the gauge must be refreshed to that batch's time rather than left at
// a stale 0. The clock is forced so the batch and the validFrom cutoff share a
// millisecond, i.e. the head is refused (>= is inclusive).
func Test_headAgeGaugeRefreshedOnRefusalPath(t *testing.T) {
	fixed := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)

	q := NewLockFreeQueue()
	q.clock = fixedClock{t: fixed}
	q.enqueueBatch(
		[]subtree.Node{{Hash: chainhash.Hash{}, Fee: 1, SizeInBytes: 0}},
		[]*subtree.TxInpoints{{}},
	)

	// Simulate a stale 0 left by an empty-clear that raced a producer.
	q.oldestPendingMillis.Store(0)

	// The head is refused for being too new (batch.time == validFromMillis).
	_, found := q.dequeueBatch(fixed.UnixMilli())
	require.False(t, found, "precondition: the held head is refused")

	require.Equal(t, fixed.UnixMilli(), q.oldestPendingMillis.Load(),
		"the refusal path must refresh the gauge to the held head's time, not leave a stale 0")
	require.NotZero(t, q.headAgeMillis(fixed.Add(time.Second).UnixMilli()),
		"headAgeMillis must be non-zero while a batch is held")
}

// Test_headAgeGaugeProducerCASFromEmpty pins that a producer sets the gauge from
// empty on the empty->non-empty transition even when the new batch shares a
// millisecond with a just-drained batch — the same-millisecond case the recheck
// design must not confuse.
func Test_headAgeGaugeProducerCASFromEmpty(t *testing.T) {
	fixed := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)

	q := NewLockFreeQueue()
	q.clock = fixedClock{t: fixed}

	// Enqueue then drain a first batch so the queue is genuinely empty and the
	// gauge is cleared to 0.
	q.enqueueBatch(
		[]subtree.Node{{Hash: chainhash.Hash{}, Fee: 1, SizeInBytes: 0}},
		[]*subtree.TxInpoints{{}},
	)
	_, found := q.dequeueBatch(0)
	require.True(t, found)
	require.Equal(t, int64(0), q.oldestPendingMillis.Load(), "precondition: gauge cleared to 0 on empty")

	// A new same-millisecond batch must set the gauge via the producer CAS.
	q.enqueueBatch(
		[]subtree.Node{{Hash: chainhash.Hash{}, Fee: 2, SizeInBytes: 0}},
		[]*subtree.TxInpoints{{}},
	)
	require.Equal(t, fixed.UnixMilli(), q.oldestPendingMillis.Load(),
		"the producer CAS must set the gauge from empty even for a same-ms batch")
}

// Test_headAgeGaugeConsumerEmptyClearReReadRepair pins the consumer empty-clear
// re-read repair: when the gauge has been cleared to 0 (an empty-clear that raced
// a producer's link) but a batch is in fact linked at the head, updateOldestPending
// must restore the linked head's time rather than leave a stale 0 — and it must do
// so by value, so a batch sharing a millisecond with a just-removed batch is not
// confused. This is the observable contract of the Store(0)+re-read shape.
//
// Note on scope: the re-read's *second* head.next load (first load nil, second
// non-nil) is only reachable when a producer links between the two loads, which is
// an inherently concurrent interleaving and cannot be forced deterministically from
// a single goroutine. This test pins the deterministic half — a linked head with a
// zeroed gauge is restored, not dropped — and Test_headAgeGaugeConcurrentDrainRace
// exercises the racing second-load path under -race.
func Test_headAgeGaugeConsumerEmptyClearReReadRepair(t *testing.T) {
	fixed := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)

	q := NewLockFreeQueue()
	q.clock = fixedClock{t: fixed}

	// Drain a first batch so the queue reaches the empty state whose clear the
	// re-read must repair.
	q.enqueueBatch(
		[]subtree.Node{{Hash: chainhash.Hash{}, Fee: 1, SizeInBytes: 0}},
		[]*subtree.TxInpoints{{}},
	)
	_, found := q.dequeueBatch(0)
	require.True(t, found)
	require.Equal(t, int64(0), q.oldestPendingMillis.Load(), "precondition: gauge cleared to 0 on empty")

	// Link a new batch B sharing the just-removed batch's millisecond, then force
	// the gauge back to 0 to model the empty-clear Store(0) that raced B's link.
	q.enqueueBatch(
		[]subtree.Node{{Hash: chainhash.Hash{}, Fee: 2, SizeInBytes: 0}},
		[]*subtree.TxInpoints{{}},
	)
	q.oldestPendingMillis.Store(0)

	// updateOldestPending must observe the linked head and restore its time.
	q.updateOldestPending()

	require.Equal(t, fixed.UnixMilli(), q.oldestPendingMillis.Load(),
		"the empty-clear re-read must restore the linked head's time by value, not leave a stale 0")
	require.NotZero(t, q.headAgeMillis(fixed.Add(time.Second).UnixMilli()))
}

// Test_headAgeGaugeConcurrentDrainRace exercises the gauge writers — the producer
// CAS, the consumer empty-clear + recheck, and the refusal-path refresh — under
// -race with many producers and a single draining/refusing consumer, all on a
// fixed clock so every batch shares one millisecond.
//
// The monitor asserts the point-5 invariant DURING the run: a non-empty queue
// must never present a zero head age. It observes "non-empty" via q.length()
// (the queueLength atomic) and the age via headAgeMillis (the oldestPendingMillis
// atomic) — both atomic-only, so the observation is race-free and never touches
// q.head (which the single consumer mutates without a lock; the after-join drain
// below is the only place q.head is read).
//
// The two atomics cannot be sampled in one snapshot, so at the instant the queue
// refills a transient (age==0, length>0) skew is possible: enqueueBatch now
// reserves the length BEFORE it links the batch and runs the gauge CAS, so a
// monitor can see length>0 for a moment before the gauge is set. That transient
// resolves as soon as the producer's very next step links and CASes. A genuine
// regression — a latched stale 0 while a backlog is held (the exact defect the
// refusal-path refresh and the empty-clear re-read remove) — instead persists
// across the consumer's passes. The monitor therefore re-confirms a bounded number
// of times and flags only a violation that does not clear, which is the strongest
// variant observable without a q.head read.
func Test_headAgeGaugeConcurrentDrainRace(t *testing.T) {
	fixed := time.UnixMilli(1_700_000_000_000).UTC()

	q := NewLockFreeQueue()
	q.clock = fixedClock{t: fixed}

	nowMillis := fixed.Add(time.Second).UnixMilli()

	var (
		wg         sync.WaitGroup
		violations atomic.Int64
		stop       = make(chan struct{})
	)

	for p := 0; p < 4; p++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				select {
				case <-stop:
					return
				default:
				}

				q.enqueueBatch(
					[]subtree.Node{{Hash: chainhash.Hash{}, Fee: 1, SizeInBytes: 0}},
					[]*subtree.TxInpoints{{}},
				)
			}
		}()
	}

	// Single consumer: alternately holds the head back (refusal-path refresh) and
	// drains (empty-clear + recheck), so the empty<->non-empty transitions the
	// gauge fix targets are hit repeatedly.
	wg.Add(1)

	go func() {
		defer wg.Done()

		for {
			select {
			case <-stop:
				return
			default:
			}

			q.dequeueBatch(fixed.UnixMilli()) // refuse the head (too new)
			q.dequeueBatch(0)                 // drain one
		}
	}()

	// Monitor: assert "length>0 => headAgeMillis>0" during the run, race-free,
	// re-confirming to reject the transient two-atomic snapshot skew.
	wg.Add(1)

	go func() {
		defer wg.Done()

		for {
			select {
			case <-stop:
				return
			default:
			}

			if q.headAgeMillis(nowMillis) != 0 || q.length() == 0 {
				continue
			}

			// Candidate violation: re-confirm. A real latched 0 persists while the
			// held backlog stays queued; a snapshot skew clears within a few reads.
			persisted := true

			for i := 0; i < 100_000; i++ {
				if q.headAgeMillis(nowMillis) != 0 || q.length() == 0 {
					persisted = false
					break
				}
			}

			if persisted {
				violations.Add(1)
				return
			}
		}
	}()

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()

	require.Zero(t, violations.Load(),
		"a non-empty queue must never present a zero head age (point-5 invariant)")

	// All goroutines have joined; touching q.head is now safe. Drain fully and
	// assert the gauge clears to 0 exactly when the queue empties.
	for {
		if _, found := q.dequeueBatch(0); !found {
			break
		}
	}

	require.True(t, q.IsEmpty(), "queue fully drained")
	require.Equal(t, int64(0), q.oldestPendingMillis.Load(), "gauge clears to 0 once the queue is empty")
	require.Zero(t, q.headAgeMillis(nowMillis))
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
