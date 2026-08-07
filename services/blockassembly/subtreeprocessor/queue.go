// Package subtreeprocessor provides functionality for processing transaction subtrees in Teranode.
package subtreeprocessor

import (
	"sync/atomic"

	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// normalizeMaxQueueItems validates and normalizes the configured ingest-queue
// item cap, following the clamp-and-warn convention used for other out-of-range
// settings so a fat-fingered value is corrected loudly rather than silently.
//
// A negative cap is a misconfiguration, not a tiny cap, and is treated as
// disabled (0). A positive cap below one full drain pass
// (maxBatchesPerIteration x sendBatchSize items) would refuse every batch and
// wedge ingest, so it is clamped up to that floor.
//
// Parameters:
//   - logger: used to warn when a value is clamped
//   - maxItems: the configured item cap (0 disables the bound)
//   - sendBatchSize: the configured per-batch item count
//
// Returns:
//   - int64: the normalized cap to hand to NewLockFreeQueueWithLimit
func normalizeMaxQueueItems(logger ulogger.Logger, maxItems int64, sendBatchSize int) int64 {
	if maxItems < 0 {
		logger.Warnf("BlockAssembly.MaxQueueItems=%d is negative; clamping to 0 (queue bound disabled)", maxItems)
		return 0
	}

	if maxItems == 0 {
		return 0
	}

	floor := int64(maxBatchesPerIteration) * int64(sendBatchSize)
	if floor > 0 && maxItems < floor {
		logger.Warnf("BlockAssembly.MaxQueueItems=%d is below one drain pass (%d items = %d batches x %d); clamping up to %d",
			maxItems, floor, maxBatchesPerIteration, sendBatchSize, floor)

		return floor
	}

	return maxItems
}

// LockFreeQueue represents a FIFO structure for batches of transactions.
// This implementation is concurrent safe for queueing, but not for dequeueing.
// Reference: https://www.cs.rochester.edu/research/synchronization/pseudocode/queues.html
//
// The queue stores batches of transactions rather than individual transactions,
// significantly reducing atomic operations and improving throughput. Multiple
// producer threads can concurrently enqueue batches while a single consumer
// thread dequeues them.
//
// The atomic operations used ensure memory visibility across threads without
// requiring explicit locking mechanisms, improving performance in high-concurrency
// scenarios.
type LockFreeQueue struct {
	head                *TxBatch                // Points to the head of the queue (sentinel node)
	tail                atomic.Pointer[TxBatch] // Atomic pointer to the tail
	queueLength         atomic.Int64            // Tracks the current number of TRANSACTIONS reserved-or-published (see below), not batches
	oldestPendingMillis atomic.Int64            // Timestamp of the oldest pending batch; 0 when the queue is empty
	clock               clock                   // Source of batch timestamps; replaced in tests
	maxItems            int64                   // Hard ceiling on published-or-reserved items; <= 0 disables the bound
}

// NewLockFreeQueue creates and initializes a new, unbounded LockFreeQueue instance.
//
// Returns:
//   - *LockFreeQueue: A new, initialized queue with no capacity limit
func NewLockFreeQueue() *LockFreeQueue {
	return NewLockFreeQueueWithLimit(0)
}

// NewLockFreeQueueWithLimit creates a LockFreeQueue bounded to maxItems items
// (transactions, not batches). A value <= 0 leaves the queue unbounded, exactly
// matching NewLockFreeQueue.
//
// Parameters:
//   - maxItems: Hard ceiling on the number of published-or-reserved items
//
// Returns:
//   - *LockFreeQueue: A new, initialized queue
func NewLockFreeQueueWithLimit(maxItems int64) *LockFreeQueue {
	return &LockFreeQueue{
		head:     &TxBatch{},
		tail:     atomic.Pointer[TxBatch]{},
		clock:    realClock{},
		maxItems: maxItems,
	}
}

// length returns the current number of transactions queued, not the number
// of batches. enqueueBatch/dequeueBatch/dequeueBatchUntil all add or
// subtract len(nodes) (the transaction count of a batch), never 1 per
// batch. This distinction bit us once already: the drain bound at
// dequeueDuringBlockMovement compared its transaction budget against a
// variable it believed counted batches, so with ~1k transactions/batch the
// bound admitted ~1000x more work than intended and a pod sat 35+ minutes
// at 558 GB RSS before anyone noticed (see that function's docstring for the
// full account). Read this as "queued transactions", never "queued
// batches".
//
// On the bounded reservation path the count includes items that are reserved
// but not yet published, so it never reads below the published-outstanding
// count.
//
// Returns:
//   - int64: The current queue length (number of transactions)
//
//go:inline
func (q *LockFreeQueue) length() int64 {
	return q.queueLength.Load()
}

// publish links a batch into the queue in a thread-safe manner, WITHOUT
// mutating queueLength. It is the shared tail-swap-and-link core of both
// enqueueBatch (which accounts for the length afterwards) and
// enqueueBatchIfRoom (which reserves the length beforehand). The entire batch
// receives a single timestamp when published.
//
// Parameters:
//   - nodes: The transaction nodes to add
//   - txInpoints: Parent transaction references for each node
func (q *LockFreeQueue) publish(nodes []subtree.Node, txInpoints []*subtree.TxInpoints) {
	batch := &TxBatch{
		nodes:      nodes,
		txInpoints: txInpoints,
		time:       q.clock.Now().UnixMilli(),
	}
	batch.next.Store(nil)

	prev := q.tail.Swap(batch)
	if prev == nil {
		q.head.next.Store(batch)
	} else {
		prev.next.Store(batch)
	}

	// Mark the oldest pending timestamp on the empty->non-empty transition so
	// the head-age gauge reads a real age rather than 0. The consumer resets it
	// to 0 when it drains the last batch, so a CAS from 0 succeeds exactly for
	// the batch that becomes the new head-of-line.
	//
	// The gauge is now a control input (the validator's ingest backpressure
	// decides on it) as well as the stall alert, so it must not read 0 while a
	// backlog is present. Two mechanisms keep it consistent under the drain race
	// without a lock or a timestamp comparison: this CAS sets it whenever the
	// queue was empty, and the consumer's empty-clear re-reads head.next after
	// storing 0 (updateOldestPending) so a batch linked during that window is
	// picked up by value. Every interleaving converges to the linked head's time.
	q.oldestPendingMillis.CompareAndSwap(0, batch.time)
}

// enqueueBatch adds a batch of transactions to the queue unconditionally,
// accounting for its items in queueLength AFTER publishing. It is the unbounded
// path and remains byte-for-byte equivalent to the original queue.
//
// Because it publishes before accounting, this path does NOT uphold the
// queueLength >= published-outstanding invariant: for the brief window between
// publish and Add the counter reads below the published count. It also ignores
// maxItems entirely. Both are acceptable: this path feeds only the internal
// AddBatch/AddDirectly callers, never the bounded ingest handlers, and the sole
// consumer of the directional guarantee (the state-transition drain snapshot)
// is fed only by the reservation path in production. Callers that need the
// bound or the invariant must use enqueueBatchIfRoom.
//
// Parameters:
//   - nodes: The transaction nodes to add
//   - txInpoints: Parent transaction references for each node
func (q *LockFreeQueue) enqueueBatch(nodes []subtree.Node, txInpoints []*subtree.TxInpoints) {
	q.publish(nodes, txInpoints)
	q.queueLength.Add(int64(len(nodes))) // gosec:nolint
}

// enqueueBatchIfRoom adds a batch only if doing so would not push the number of
// published-or-reserved items past maxItems, and reports whether it did.
//
// The bound is enforced by an atomic reservation, not a load-then-compare: the
// batch's items are added to queueLength first, and only published if the
// post-add value is within the limit; otherwise the reservation is rolled back
// and nothing is published. Because reservations are totally ordered by the
// atomic and the counter decreases only by dequeue of published items or by
// rollback of a failed reservation, the published-but-not-yet-dequeued item
// count can never exceed maxItems even with many concurrent producers. A
// load-then-compare would not be a bound under the multi-producer contract,
// because two producers could both observe room and both publish.
//
// This reservation-first ordering is also what upholds the
// queueLength >= published-outstanding invariant: the counter is incremented
// before the batch is linked, so it never reads below the published count. The
// unbounded enqueueBatch path does not share that ordering (see its comment).
//
// When maxItems <= 0 the queue is unbounded and this degrades to enqueueBatch.
//
// Parameters:
//   - nodes: The transaction nodes to add
//   - txInpoints: Parent transaction references for each node
//
// Returns:
//   - bool: true if the batch was enqueued, false if it was refused for room
func (q *LockFreeQueue) enqueueBatchIfRoom(nodes []subtree.Node, txInpoints []*subtree.TxInpoints) bool {
	n := int64(len(nodes)) // gosec:nolint

	if q.maxItems <= 0 {
		q.enqueueBatch(nodes, txInpoints)
		return true
	}

	if q.queueLength.Add(n) > q.maxItems { // reserve
		q.queueLength.Add(-n) // roll back
		return false
	}

	q.publish(nodes, txInpoints) // reservation already accounted

	return true
}

// dequeueBatch removes and returns the next batch from the queue.
// NOTE - This operation is not thread-safe and should only be called from a single thread.
// The dequeued batch's memory will be eligible for garbage collection.
//
// Parameters:
//   - validFromMillis: Optional timestamp to filter batches - batches with time >= this value won't be dequeued
//
// Returns:
//   - *TxBatch: The batch of transactions
//   - bool: True if a batch was dequeued, false if queue empty or batch not valid
func (q *LockFreeQueue) dequeueBatch(validFromMillis int64) (*TxBatch, bool) {
	next := q.head.next.Load()

	if next == nil {
		return nil, false
	}

	if validFromMillis > 0 && next.time >= validFromMillis {
		// The head-of-line batch is queued but held back by the double-spend
		// window (not yet drainable). Refresh the gauge to its true age so a
		// transient stale 0 (from an earlier empty-clear that raced a producer)
		// cannot persist for the whole hold-back: the drain loop calls
		// dequeueBatch every idle-sleep, so this collapses any stale 0 to at most
		// one loop iteration. next is the current head-of-line, so its time is
		// the authoritative oldest-pending value.
		q.oldestPendingMillis.Store(next.time)
		return nil, false
	}

	q.head = next
	q.queueLength.Add(-int64(len(next.nodes))) // gosec:nolint
	q.updateOldestPending()

	return next, true
}

// dequeueBatchUntil removes and returns the next batch only if its time
// is at or before maxTimeMillis. The "inclusive-until" semantics
// (batch.time <= maxTimeMillis admits) complement dequeueBatch's
// "exclusive-from" semantics (batch.time < validFromMillis admits).
//
// Unlike dequeueBatch, this method peeks at batch.time BEFORE removing
// the batch from the queue, so callers that want to stop at a time
// boundary do not lose the boundary batch on the floor.
//
// NOTE - This operation is not thread-safe and should only be called
// from a single thread.
//
// Parameters:
//   - maxTimeMillis: Inclusive upper bound on batch.time for admission.
//
// Returns:
//   - *TxBatch: The dequeued batch, or nil if the queue is empty or the
//     head batch's time exceeds maxTimeMillis.
//   - bool: True iff a batch was dequeued.
func (q *LockFreeQueue) dequeueBatchUntil(maxTimeMillis int64) (*TxBatch, bool) {
	next := q.head.next.Load()
	if next == nil || next.time > maxTimeMillis {
		return nil, false
	}

	q.head = next
	q.queueLength.Add(-int64(len(next.nodes))) // gosec:nolint
	q.updateOldestPending()

	return next, true
}

// updateOldestPending refreshes the oldest-pending timestamp after the consumer
// advances head. It reads q.head, so it MUST only be called from the single
// consumer goroutine (the same constraint as dequeue). A monitoring goroutine
// must read headAgeMillis, never q.head.
//
// On the empty-clear it stores 0 and then re-reads head.next: a producer may
// have linked a new head between the two loads, and this recheck picks it up by
// value so the gauge never latches a stale 0 while a batch is present. The
// recheck compares no timestamps, so two batches sharing a millisecond can never
// be confused; if the producer has not linked yet, head.next is still nil
// (correctly 0) and the producer's own CAS(0, time) sets the gauge when it links.
func (q *LockFreeQueue) updateOldestPending() {
	if following := q.head.next.Load(); following != nil {
		q.oldestPendingMillis.Store(following.time)
		return
	}

	q.oldestPendingMillis.Store(0)

	// Re-read: a producer linking a new head between the load above and the
	// Store(0) would otherwise leave the gauge at 0 with a batch present.
	if following := q.head.next.Load(); following != nil {
		q.oldestPendingMillis.Store(following.time)
	}
}

// headAgeMillis returns how long the oldest pending batch has waited, in
// milliseconds, given the current wall-clock time in Unix millis. It returns 0
// when the queue is empty. It reads only an atomic and is therefore safe to
// call from a monitoring goroutine.
//
// Parameters:
//   - now: The current time in Unix milliseconds
//
// Returns:
//   - int64: The age of the oldest pending batch in milliseconds, or 0 if empty
func (q *LockFreeQueue) headAgeMillis(now int64) int64 {
	oldest := q.oldestPendingMillis.Load()
	if oldest == 0 {
		return 0
	}

	if age := now - oldest; age > 0 {
		return age
	}

	return 0
}

// MaxItems returns the enforced (normalized) hard ceiling on published-or-
// reserved items, or a value <= 0 when the queue is unbounded. It is the cap the
// ingest reservation actually enforces, which the shed message reports so
// operators see the real limit rather than the raw configured value.
//
// Returns:
//   - int64: The enforced item cap (<= 0 when unbounded)
func (q *LockFreeQueue) MaxItems() int64 {
	return q.maxItems
}

// IsEmpty checks if the queue contains any batches.
//
// Returns:
//   - bool: true if the queue is empty, false otherwise
//
//go:inline
func (q *LockFreeQueue) IsEmpty() bool {
	return q.head.next.Load() == nil
}
