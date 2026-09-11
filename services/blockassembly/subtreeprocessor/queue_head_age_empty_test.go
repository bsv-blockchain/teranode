package subtreeprocessor

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

// oneNodeBatch is the smallest publishable batch.
func oneNodeBatch(fee uint64) ([]subtree.Node, []*subtree.TxInpoints) {
	return []subtree.Node{{Hash: chainhash.Hash{}, Fee: fee, SizeInBytes: 0}}, []*subtree.TxInpoints{{}}
}

// TestQueue_HeadAgeClearedWhenProducerCASLandsAfterDrain reproduces the A5 defect
// deterministically and pins its fix.
//
// publish links a batch and only THEN runs CompareAndSwap(0, batch.time). A drain
// landing inside that window advances head past the batch and clears the gauge to
// 0, after which the producer's CAS succeeds — leaving a non-zero head age on a
// queue with nothing left to dequeue. Nothing used to clear it: every later
// dequeueBatch took the head.next == nil early return, so the gauge grew without
// bound. With the default DoubleSpendWindow of 0 the hold-back refresh (the only
// other writer) is never reached either, so this is the DEFAULT configuration's
// behaviour, not an exotic one.
//
// The consequences were real in both directions: the stall alert fires on an idle
// node, and with backpressure enabled the same stale value crosses the pause
// watermark, pauses ingest, and — since no new batch will arrive to be dequeued —
// keeps the value stale, producing a self-sustaining pause/cooldown oscillation on
// an empty queue.
//
// The interleaving is reproduced by its OUTCOME rather than by racing goroutines:
// an empty queue whose gauge holds a stale non-zero timestamp is exactly the state
// step 3 leaves behind, and it is that state the empty observation must clear.
func TestQueue_HeadAgeClearedWhenProducerCASLandsAfterDrain(t *testing.T) {
	fixed := time.UnixMilli(1_700_000_000_000).UTC()

	q := NewLockFreeQueue()
	q.clock = fixedClock{t: fixed}

	nodes, inpoints := oneNodeBatch(1)
	q.enqueueBatch(nodes, inpoints)

	// The drain empties the queue.
	_, found := q.dequeueBatch(0)
	require.True(t, found)
	require.True(t, q.IsEmpty(), "precondition: the queue is empty")

	// The producer's late CAS lands on the now-empty queue, latching a stale
	// timestamp. headAgeMillis no longer surfaces it (the empty-queue clamp reads
	// queueLength==0 and returns 0), so the stale value is observed on the raw
	// latch — it is that latch the empty observation must still clear.
	q.oldestPendingMillis.Store(fixed.UnixMilli())
	require.NotZero(t, q.oldestPendingMillis.Load(),
		"precondition: the latch holds a stale timestamp on an empty queue")
	require.Zero(t, q.headAgeMillis(fixed.Add(time.Minute).UnixMilli()),
		"the empty-queue clamp already masks the stale latch from the head-age read")

	// The next drain-loop pass observes the empty queue and must clear it. Before
	// the fix this pass returned without touching the gauge, and so did every pass
	// after it.
	_, found = q.dequeueBatch(0)
	require.False(t, found)

	require.Equal(t, int64(0), q.oldestPendingMillis.Load(), "the empty observation must clear the gauge")
	require.Equal(t, int64(0), q.headAgeMillis(fixed.Add(time.Minute).UnixMilli()),
		"an empty queue must not present a head age, however long ago the stale timestamp was")

	// Same for the until-variant, which the state-transition drain uses.
	q.oldestPendingMillis.Store(fixed.UnixMilli())

	_, found = q.dequeueBatchUntil(fixed.Add(time.Hour).UnixMilli())
	require.False(t, found)
	require.Equal(t, int64(0), q.oldestPendingMillis.Load(),
		"dequeueBatchUntil's empty observation must clear the gauge too")
}

// TestQueue_HeadAgeStaysAccurateUnderConcurrentPublishAndDrain runs the gauge's
// writers concurrently — the producer CAS, the consumer's empty-clear and recheck,
// and the empty observation — and asserts convergence after quiescing: an empty
// queue presents a zero head age.
//
// This is a genuine concurrency test, so the goroutine fan-out is the point; it is
// meant to be run under -race.
func TestQueue_HeadAgeStaysAccurateUnderConcurrentPublishAndDrain(t *testing.T) {
	fixed := time.UnixMilli(1_700_000_000_000).UTC()

	q := NewLockFreeQueue()
	q.clock = fixedClock{t: fixed}

	var (
		wg        sync.WaitGroup
		published atomic.Int64
		stop      = make(chan struct{})
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

				nodes, inpoints := oneNodeBatch(1)
				q.enqueueBatch(nodes, inpoints)
				published.Add(1)
			}
		}()
	}

	// Single consumer, as the queue's contract requires.
	wg.Add(1)

	go func() {
		defer wg.Done()

		for {
			select {
			case <-stop:
				return
			default:
			}

			q.dequeueBatch(0)
		}
	}()

	require.Eventually(t, func() bool { return published.Load() > 500 }, 5*time.Second, time.Millisecond,
		"producers should make progress")

	close(stop)
	wg.Wait()

	// Quiesce: drain whatever the producers left behind, then assert the gauge
	// agrees with the queue being empty. Before the fix a producer CAS that landed
	// after the final drain latched here permanently.
	for {
		if _, found := q.dequeueBatch(0); !found {
			break
		}
	}

	require.True(t, q.IsEmpty())
	require.Equal(t, int64(0), q.headAgeMillis(fixed.Add(time.Minute).UnixMilli()),
		"after quiescing, an empty queue must present a zero head age")
}

// TestQueue_HoldBackKeepsHeadAgeAccurate pins the other half of the change: the
// empty observation must not be confused with a HELD-BACK head. When
// dequeueBatchUntil declines the head for being past the caller's time boundary the
// batch is still queued, so the gauge must read that batch's real age rather than
// being cleared to 0 — otherwise the fix for the empty case would blind the stall
// alert and the backpressure control during a hold-back.
func TestQueue_HoldBackKeepsHeadAgeAccurate(t *testing.T) {
	fixed := time.UnixMilli(1_700_000_000_000).UTC()

	q := NewLockFreeQueue()
	q.clock = fixedClock{t: fixed}

	nodes, inpoints := oneNodeBatch(1)
	q.enqueueBatch(nodes, inpoints)

	// Model a stale 0 (an earlier empty-clear that raced this producer) so the
	// refresh is observable rather than a no-op.
	q.oldestPendingMillis.Store(0)

	// The head is beyond the boundary, so it is declined but stays queued.
	_, found := q.dequeueBatchUntil(fixed.Add(-time.Second).UnixMilli())
	require.False(t, found, "precondition: the head is past the caller's boundary")

	require.Equal(t, fixed.UnixMilli(), q.oldestPendingMillis.Load(),
		"a held-back head must refresh the gauge to its own time, not clear it")

	now := fixed.Add(2 * time.Second)
	require.Equal(t, int64(2000), q.headAgeMillis(now.UnixMilli()),
		"the gauge must report the held head's true age")
	require.False(t, q.IsEmpty(), "the held batch is still queued")
}

// TestQueue_HeadAgeClampedToZeroOnEmptyQueueDuringStateTransition pins the
// read-side clamp that keeps an empty queue reporting a zero head age. A producer
// CAS that latched a stale non-zero oldestPendingMillis on an empty queue must
// not surface as a non-zero head age: the only clear runs on the dequeue paths,
// which a long reorg/move-forward never takes, so the latch would otherwise
// persist for the whole transition. The clamp keys off queueLength, which
// reserve-first accounting keeps a safe empty oracle, and a real backlog must
// still report its true age.
func TestQueue_HeadAgeClampedToZeroOnEmptyQueueDuringStateTransition(t *testing.T) {
	fixed := time.UnixMilli(1_700_000_000_000).UTC()

	q := NewLockFreeQueue()
	q.clock = fixedClock{t: fixed}

	// Empty queue with a stale latch, as if a producer CAS raced a drain just
	// before a state transition that never dequeues.
	require.Zero(t, q.length(), "precondition: the queue is empty")
	q.oldestPendingMillis.Store(fixed.UnixMilli())

	require.Equal(t, int64(0), q.headAgeMillis(fixed.Add(time.Hour).UnixMilli()),
		"an empty queue must clamp head age to 0 however stale the latch")

	// A real backlog must still report its true age (the clamp fires only on an
	// empty queue). Clear the latch first so the producer CAS sets it cleanly.
	q.oldestPendingMillis.Store(0)

	nodes, inpoints := oneNodeBatch(1)
	q.enqueueBatch(nodes, inpoints)

	require.Positive(t, q.length(), "precondition: a batch is queued")
	require.Equal(t, int64(2000), q.headAgeMillis(fixed.Add(2*time.Second).UnixMilli()),
		"a non-empty queue must report the head's true age, not the clamp")
}

// TestQueue_PublishNeverUnderReportsUnderProducerRace pins the monotone (min)
// producer update that closes the multi-producer ordering hazard: with the latch
// holding a LATER timestamp — the state a plain CAS-from-0 leaves when the
// producer that linked BEHIND the true head happens to win the store first — a
// producer whose batch is OLDER must pull the latch back to its own time, so the
// gauge converges to the oldest pending time and never under-reports the backlog
// age (the dangerous direction, where the backpressure controller would fail to
// pause). A subsequent newer batch must never raise the latch.
//
// The interleaving is reproduced by its OUTCOME rather than by racing goroutines:
// a latch holding the later producer's time is exactly the state the losing
// (older) producer must correct on publish.
func TestQueue_PublishNeverUnderReportsUnderProducerRace(t *testing.T) {
	older := time.UnixMilli(1_700_000_000_000).UTC()
	later := older.Add(50 * time.Millisecond)

	q := NewLockFreeQueue()

	// The latch already holds the LATER producer's time (the under-report a plain
	// CAS-from-0 would leave once the later-linked producer stores first).
	q.oldestPendingMillis.Store(later.UnixMilli())

	// The older producer publishes; the min update must lower the latch to its
	// own, older time rather than leave the later one.
	q.clock = fixedClock{t: older}
	nodes, inpoints := oneNodeBatch(1)
	q.enqueueBatch(nodes, inpoints)
	require.Equal(t, older.UnixMilli(), q.oldestPendingMillis.Load(),
		"publish must lower the latch to the oldest pending time, never leave a later one")

	// A subsequent newer batch must not raise the latch off the true head.
	q.clock = fixedClock{t: later.Add(time.Second)}
	newerNodes, newerInpoints := oneNodeBatch(2)
	q.enqueueBatch(newerNodes, newerInpoints)
	require.Equal(t, older.UnixMilli(), q.oldestPendingMillis.Load(),
		"a newer batch never raises the head-age latch")
}

// TestQueue_EnqueueBatchReservesBeforePublish pins the reserve-first ordering of
// the unbounded path: queueLength is accounted BEFORE the batch is linked, so a
// monitor can only ever see the counter OVER-count the linked backlog, never read
// below it. That direction is what makes queueLength==0 a safe empty oracle for
// the head-age clamp. Producers only (no consumer) so the head chain is stable
// and the monitor can walk it race-free against the producers' atomic appends;
// this is a genuine concurrency test, meant to be run under -race.
func TestQueue_EnqueueBatchReservesBeforePublish(t *testing.T) {
	q := NewLockFreeQueue()

	var (
		wg    sync.WaitGroup
		stop  = make(chan struct{})
		under atomic.Int64
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

				nodes, inpoints := oneNodeBatch(1)
				q.enqueueBatch(nodes, inpoints)
			}
		}()
	}

	// Monitor: count the items reachable on the linked chain, then read the
	// counter. Reading length() AFTER the walk is sound under Go's sequentially
	// consistent atomics: any batch observed linked had its reserve Add ordered
	// before its link store, which was observed before this later length load, so
	// the load must include it. A counter reading below the linked count is
	// therefore a genuine under-count regression, not a two-read skew.
	wg.Add(1)

	go func() {
		defer wg.Done()

		for {
			select {
			case <-stop:
				return
			default:
			}

			var linked int64
			for n := q.head.next.Load(); n != nil; n = n.next.Load() {
				linked += int64(len(n.nodes))
			}

			if q.length() < linked {
				under.Add(1)
			}
		}
	}()

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()

	require.Zero(t, under.Load(),
		"queueLength must never read below the linked backlog (reserve-first over-counts only)")
}

// TestSubtreeProcessor_QueueHeadAgeUsesInjectableClock pins that QueueHeadAge
// derives the age from the queue's injectable clock — the same source publish
// stamps batches with — not wall time. With a fake clock advanced by a known
// delta the reported age is exactly that delta; a wall-clock read would instead
// return the years since the fixed batch timestamp.
func TestSubtreeProcessor_QueueHeadAgeUsesInjectableClock(t *testing.T) {
	stp := newTestProcessorNoStart(t)

	t0 := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	stp.queue.clock = fixedClock{t: t0}

	nodes, inpoints := oneNodeBatch(1)
	stp.queue.enqueueBatch(nodes, inpoints)

	// Advance only the injectable clock; wall time is unaffected.
	stp.queue.clock = fixedClock{t: t0.Add(1500 * time.Millisecond)}

	require.Equal(t, 1500*time.Millisecond, stp.QueueHeadAge(),
		"QueueHeadAge must measure against the injectable clock, not wall time")
}
