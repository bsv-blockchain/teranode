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

	// The producer's late CAS lands on the now-empty queue.
	q.oldestPendingMillis.Store(fixed.UnixMilli())
	require.NotZero(t, q.headAgeMillis(fixed.Add(time.Minute).UnixMilli()),
		"precondition: the gauge holds a stale age on an empty queue")

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
