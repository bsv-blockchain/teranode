package subtreeprocessor

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// mkNodes builds n distinct transaction nodes for queue tests.
func mkNodes(n int) []subtree.Node {
	nodes := make([]subtree.Node, n)
	for i := range nodes {
		nodes[i] = subtree.Node{Hash: chainhash.Hash{}, Fee: uint64(i), SizeInBytes: 0} // gosec:nolint
	}

	return nodes
}

// mkInpoints builds n empty inpoints to pair with mkNodes.
func mkInpoints(n int) []*subtree.TxInpoints {
	ips := make([]*subtree.TxInpoints, n)
	for i := range ips {
		ips[i] = &subtree.TxInpoints{}
	}

	return ips
}

// Test 1 — enqueueBatchIfRoom refuses a batch that would cross maxItems without
// publishing or changing length, and admits a batch that lands exactly on the
// limit.
func Test_enqueueBatchIfRoom_refusesPastLimit(t *testing.T) {
	q := NewLockFreeQueueWithLimit(100)

	// A batch that lands exactly on the limit is admitted (100 is not > 100).
	require.True(t, q.enqueueBatchIfRoom(mkNodes(100), mkInpoints(100)))
	require.Equal(t, int64(100), q.length())

	// A further batch would cross the limit: refused, no accounting change.
	require.False(t, q.enqueueBatchIfRoom(mkNodes(1), mkInpoints(1)))
	require.Equal(t, int64(100), q.length(), "refused batch must not change length")

	// Only the first batch was ever published: draining yields exactly one.
	batch, found := q.dequeueBatch(0)
	require.True(t, found)
	require.Equal(t, 100, len(batch.nodes))

	_, found = q.dequeueBatch(0)
	require.False(t, found, "the refused batch was never published")
}

// Test 2 — maxItems = 0 disables the bound entirely; enqueueBatchIfRoom never
// refuses. Regression guard for the opt-out path and the rollback plan.
func Test_enqueueBatchIfRoom_unboundedNeverRefuses(t *testing.T) {
	q := NewLockFreeQueueWithLimit(0)

	for i := 0; i < 1000; i++ {
		require.True(t, q.enqueueBatchIfRoom(mkNodes(100), mkInpoints(100)))
	}

	require.Equal(t, int64(100_000), q.length(), "unbounded queue accounts every item")
}

// Test 3 — accounting is in items, not batches: one 100-node batch is length 100.
func Test_enqueueBatchIfRoom_accountsInItems(t *testing.T) {
	q := NewLockFreeQueueWithLimit(1000)

	require.True(t, q.enqueueBatchIfRoom(mkNodes(100), mkInpoints(100)))
	require.Equal(t, int64(100), q.length(), "length counts items, not batches")
}

// Test 4 — headAgeMillis is 0 when empty, tracks the oldest pending batch across
// enqueues, and resets on drain. Driven through the clock seam, no sleeps.
func Test_headAgeMillis_tracksOldestPending(t *testing.T) {
	q := NewLockFreeQueue()

	require.Equal(t, int64(0), q.headAgeMillis(time.Now().UnixMilli()), "empty queue reports age 0")

	t0 := time.UnixMilli(1_000_000).UTC()
	q.clock = fixedClock{t: t0}
	q.enqueueBatch(mkNodes(1), mkInpoints(1))

	now := t0.Add(500 * time.Millisecond).UnixMilli()
	require.Equal(t, int64(500), q.headAgeMillis(now))

	// A newer batch does not move the head-of-line age.
	t1 := t0.Add(100 * time.Millisecond)
	q.clock = fixedClock{t: t1}
	q.enqueueBatch(mkNodes(1), mkInpoints(1))
	require.Equal(t, int64(500), q.headAgeMillis(now), "oldest pending is still the first batch")

	// Draining the first batch advances the head-of-line to the second.
	_, found := q.dequeueBatch(0)
	require.True(t, found)
	require.Equal(t, now-t1.UnixMilli(), q.headAgeMillis(now))

	// Draining the last batch empties the queue: age resets to 0.
	_, found = q.dequeueBatch(0)
	require.True(t, found)
	require.Equal(t, int64(0), q.headAgeMillis(now), "drained-empty queue resets age to 0")
}

// Test 5 — concurrency / plateau. Many producers race a stalled consumer with a
// bound set. The number of published items never exceeds the cap, length()
// plateaus rather than tracking offered load, and offered == accepted +
// rejected. Proves the reservation is a hard bound. Run under -race.
func Test_enqueueBatchIfRoom_plateauUnderContention(t *testing.T) {
	t.Parallel()

	const (
		producers   = 8
		perProducer = 5000
		batchItems  = 10
		maxItems    = 1000
	)

	q := NewLockFreeQueueWithLimit(maxItems)

	var accepted, rejected atomic.Int64

	var wg sync.WaitGroup

	for p := 0; p < producers; p++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := 0; i < perProducer; i++ {
				if q.enqueueBatchIfRoom(mkNodes(batchItems), mkInpoints(batchItems)) {
					accepted.Add(1)
				} else {
					rejected.Add(1)
				}
			}
		}()
	}

	wg.Wait()

	require.LessOrEqual(t, q.length(), int64(maxItems), "length plateaus at or below the cap")

	// Drain and count published items: the true published total never exceeds
	// the cap even though offered load was far larger.
	var published int64

	for {
		batch, found := q.dequeueBatch(0)
		if !found {
			break
		}

		published += int64(len(batch.nodes))
	}

	require.LessOrEqual(t, published, int64(maxItems), "published items never exceed the cap")
	require.Equal(t, accepted.Load()*batchItems, published, "every accepted batch was published exactly once")

	offered := int64(producers * perProducer)
	require.Equal(t, offered, accepted.Load()+rejected.Load(), "offered == accepted + rejected")
}

// Test 6 — interleaved reservation and rollback under contention does not
// corrupt the counter: with a concurrent consumer, the queue drains to exactly
// zero and every accepted item is dequeued exactly once. Run under -race.
func Test_enqueueBatchIfRoom_rollbackDrainsToZero(t *testing.T) {
	t.Parallel()

	const (
		producers   = 8
		perProducer = 3000
		batchItems  = 7
		maxItems    = 500
	)

	q := NewLockFreeQueueWithLimit(maxItems)

	stop := make(chan struct{})

	var drained atomic.Int64

	var consumerWg sync.WaitGroup

	consumerWg.Add(1)

	go func() {
		defer consumerWg.Done()

		for {
			select {
			case <-stop:
				return
			default:
				if batch, found := q.dequeueBatch(0); found {
					drained.Add(int64(len(batch.nodes)))
				}
			}
		}
	}()

	var accepted atomic.Int64

	var wg sync.WaitGroup

	for p := 0; p < producers; p++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := 0; i < perProducer; i++ {
				if q.enqueueBatchIfRoom(mkNodes(batchItems), mkInpoints(batchItems)) {
					accepted.Add(1)
				}
			}
		}()
	}

	wg.Wait()
	close(stop)
	consumerWg.Wait()

	// Drain whatever the consumer left behind.
	for {
		batch, found := q.dequeueBatch(0)
		if !found {
			break
		}

		drained.Add(int64(len(batch.nodes)))
	}

	require.Equal(t, int64(0), q.length(), "counter returns to exactly zero after rollbacks and drains")
	require.Equal(t, accepted.Load()*batchItems, drained.Load(), "every accepted item is drained exactly once")
}

// Test 7 — reserved-vs-published invariant (on the bounded reservation path) and
// the drain-snapshot race. Every publish here goes through enqueueBatchIfRoom,
// which reserves before publishing; the invariant it asserts is scoped to that
// path (the unbounded enqueueBatch is documented not to uphold it). A loop of
// always-failing over-cap reservations (add-then-rollback, never publish) races
// a dequeueDuringBlockMovement-shaped drain. A single genuine post-entry batch is
// published (admissible by count, held back by the validFromMillis filter) so
// the "no chasing ingest" property is exercised concretely, not just
// structurally. Asserts: (a) length() never under-reports the pre-snapshot
// published-outstanding count at any sample; (b) the drain removes exactly the
// pre-snapshot batches; (c) the drain never dequeues the post-entry batch, which
// remains queued. Driven through the clock seam; run under -race.
func Test_enqueueBatchIfRoom_reservedVsPublishedInvariant(t *testing.T) {
	t.Parallel()

	const (
		preBatches = 50
		batchItems = 10
		// Room for the pre-fill plus exactly one post-entry batch.
		maxItems = (preBatches + 1) * batchItems
	)

	q := NewLockFreeQueueWithLimit(maxItems)

	// Publish the pre-snapshot batches at tPre.
	tPre := time.UnixMilli(1_000_000).UTC()
	q.clock = fixedClock{t: tPre}

	for i := 0; i < preBatches; i++ {
		require.True(t, q.enqueueBatchIfRoom(mkNodes(batchItems), mkInpoints(batchItems)))
	}

	const preItems = int64(preBatches * batchItems)

	require.Equal(t, preItems, q.length(), "precondition: pre-snapshot batches published")

	// validFromMillis is sampled strictly after the pre-snapshot time, so every
	// pre-snapshot batch is admissible and anything published at/after entry is
	// held back by the queue filter.
	tEntry := tPre.UnixMilli() + 1

	// A genuine post-entry publish: it has room (so it succeeds and is linked),
	// its timestamp is >= validFromMillis, so the drain must hold it back rather
	// than chase it. This is the concrete "no chasing ingest" case.
	tPost := time.UnixMilli(tEntry).UTC()
	q.clock = fixedClock{t: tPost}
	require.True(t, q.enqueueBatchIfRoom(mkNodes(batchItems), mkInpoints(batchItems)),
		"post-entry publish must have room")

	// Churn: always-failing reservations (n > maxItems can never fit, even from
	// empty), so they only add-then-rollback the counter and never publish.
	// This is what makes length() transiently overshoot during the drain.
	stop := make(chan struct{})

	var (
		churnWg        sync.WaitGroup
		churnPublished atomic.Bool
	)

	churnWg.Add(1)

	go func() {
		defer churnWg.Done()

		for {
			select {
			case <-stop:
				return
			default:
				if q.enqueueBatchIfRoom(mkNodes(maxItems+1), mkInpoints(maxItems+1)) {
					churnPublished.Store(true)
				}
			}
		}
	}()

	// dequeueDuringBlockMovement-shaped drain: snapshot the (possibly inflated)
	// length, then drain up to that bound, stopping at the validFromMillis filter.
	snapshot := q.length()
	require.GreaterOrEqual(t, snapshot, preItems, "snapshot never reads below the published count")

	var itemsProcessed int64

	for itemsProcessed < snapshot {
		// Property (a): the counter never under-reports the still-published
		// pre-snapshot items.
		remainingPublished := preItems - itemsProcessed
		require.GreaterOrEqual(t, q.length(), remainingPublished,
			"length must never read below published-outstanding")

		batch, found := q.dequeueBatch(tEntry)
		if !found {
			break
		}

		// Property (c): a dequeued batch was always published before entry.
		require.Less(t, batch.time, tEntry, "drain must not dequeue a batch published at/after entry")

		itemsProcessed += int64(len(batch.nodes))
	}

	close(stop)
	churnWg.Wait()

	require.False(t, churnPublished.Load(), "an over-cap reservation must always fail")

	// Property (b): the drain removed exactly the pre-snapshot batches — not more
	// (it did not chase the post-entry ingest) and not fewer (the inflated
	// snapshot did not strand work).
	require.Equal(t, preItems, itemsProcessed, "drain removed exactly the pre-snapshot batches")

	// Property (c), concretely: the post-entry batch was held back, not chased,
	// so it is still queued and is exactly the batch published at tPost.
	batch, found := q.dequeueBatch(0)
	require.True(t, found, "the post-entry batch remained queued")
	require.Equal(t, tPost.UnixMilli(), batch.time, "the surviving batch is the post-entry publish")
}

// Test — normalizeMaxQueueItems clamps out-of-range values with a warning,
// following the clamp-and-warn convention. A negative cap disables the bound; a
// positive cap below one drain pass is raised to that floor; a healthy value and
// the explicit-disable 0 pass through unchanged.
func Test_normalizeMaxQueueItems(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)

	const sendBatchSize = 100

	floor := int64(maxBatchesPerIteration) * int64(sendBatchSize)

	t.Run("negative_disables", func(t *testing.T) {
		require.Equal(t, int64(0), normalizeMaxQueueItems(logger, -5, sendBatchSize))
	})

	t.Run("zero_stays_disabled", func(t *testing.T) {
		require.Equal(t, int64(0), normalizeMaxQueueItems(logger, 0, sendBatchSize))
	})

	t.Run("sub_drain_pass_clamps_up", func(t *testing.T) {
		require.Equal(t, floor, normalizeMaxQueueItems(logger, 1, sendBatchSize))
		require.Equal(t, floor, normalizeMaxQueueItems(logger, floor-1, sendBatchSize))
	})

	t.Run("healthy_value_passes_through", func(t *testing.T) {
		require.Equal(t, int64(16_777_216), normalizeMaxQueueItems(logger, 16_777_216, sendBatchSize))
	})
}
