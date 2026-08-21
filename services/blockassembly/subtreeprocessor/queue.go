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
// enqueueBatch and enqueueBatchIfRoom, each of which reserves the batch's items
// in queueLength before calling it. The entire batch receives a single timestamp
// when published.
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

	// Move the oldest-pending timestamp EARLIER toward this batch's time, never
	// later. A producer sets it from 0 (the empty->non-empty transition) or lowers
	// it when this batch predates the current latch, but it NEVER raises it. That
	// one-directional rule is what makes the multi-producer race safe: with the
	// latch at 0, producer A can link the true (older) head yet producer B links
	// behind it and wins the store first; a plain CAS-from-0 would then leave B's
	// LATER time and under-report the backlog age (the dangerous direction — the
	// backpressure controller would fail to pause). Here A's corrective CAS(tB, tA)
	// still lowers the latch to its older time, so it converges to the OLDEST
	// pending time. A read landing in the gap between B's store and A's corrective
	// CAS still sees the younger tB — a transient under-report — but that gap is one
	// producer's own straight-line path from link to this loop (no I/O, lock or
	// early return) and self-heals on A's very next CAS: a different class from the
	// persistent, consumer-cadence-bounded under-report a plain CAS-from-0 leaves.
	//
	// batch.time is stamped before tail.Swap, so a stalled producer can carry a
	// time earlier than the batch actually linked ahead of it; pulling the latch to
	// that earlier time is an OVER-report (age reads older than the true head),
	// bounded by the producers' inter-arrival gap and corrected by the consumer's
	// next updateOldestPending. Over-report is the safe direction.
	//
	// The loop is lock-free and terminates: the common case (non-empty queue, this
	// batch newer than the head) breaks on the first load with no store; a store
	// runs only on the empty transition or a rarer out-of-order timestamp, and each
	// failed CAS re-reads a latch another writer already moved toward this target or
	// to 0, so progress is bounded by the finite set of concurrent writers.
	//
	// An empty queue is covered separately by the headAgeMillis clamp, which reads
	// 0 whenever queueLength is 0, so a stale non-zero latch left on an empty queue
	// (e.g. a CAS that landed just after a drain) is never observed while it stays
	// empty — including for the whole of a dequeue-free state transition. Should a
	// batch then arrive while such a stale OLDER latch is still set, min-CAS leaves
	// it (it only lowers), so the age reads slightly old: an OVER-report, the safe
	// direction, bounded and cleared by the consumer's next updateOldestPending.
	for {
		cur := q.oldestPendingMillis.Load()
		if cur != 0 && batch.time >= cur {
			break
		}

		if q.oldestPendingMillis.CompareAndSwap(cur, batch.time) {
			break
		}
	}
}

// enqueueBatch adds a batch of transactions to the queue unconditionally,
// accounting for its items in queueLength BEFORE publishing. It is the unbounded
// path; it ignores maxItems but otherwise mirrors enqueueBatchIfRoom's
// reserve-first ordering.
//
// Reserving before publishing upholds the queueLength >= published-outstanding
// invariant on this path too: the counter is bumped before the batch is linked,
// so its only transient miscount is an OVER-count (counter high for the brief
// window before the link lands), never an under-count (counter reading below the
// published count). That direction is load-bearing because queueLength is read as
// an empty oracle — headAgeMillis clamps to 0 when it reads 0 — so it must never
// read 0 while a batch is linked. Every other consumer (the state-transition
// drain snapshot, the diagnostic gauges and RPCs) already tolerates a transient
// one-batch over-count. Callers that need the bound must use enqueueBatchIfRoom.
//
// Parameters:
//   - nodes: The transaction nodes to add
//   - txInpoints: Parent transaction references for each node
func (q *LockFreeQueue) enqueueBatch(nodes []subtree.Node, txInpoints []*subtree.TxInpoints) {
	q.queueLength.Add(int64(len(nodes))) // gosec:nolint  reserve before publishing
	q.publish(nodes, txInpoints)
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
// unbounded enqueueBatch path now shares that ordering (see its comment).
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
		// Make the empty observation authoritative. publish links a batch BEFORE
		// its CAS(0, time), so a drain landing in that window clears the gauge and
		// the producer's CAS then sets it with nothing left to dequeue — a non-zero
		// head age latched on an empty queue, which no later dequeue would clear
		// because they all take this early return. updateOldestPending stores 0 and
		// re-reads head.next, so every interleaving converges: a batch already
		// linked is picked up by value, and one not yet linked is set by the
		// producer's own CAS.
		q.updateOldestPending()

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

	if next == nil {
		// Empty observation: clear the gauge with the store-then-recheck, exactly as
		// dequeueBatch does. See its comment for the latched-stale-age race.
		q.updateOldestPending()

		return nil, false
	}

	if next.time > maxTimeMillis {
		// The head is queued but beyond the caller's time boundary. next is the
		// authoritative oldest-pending batch, so refresh the gauge to its true age
		// rather than leaving a stale value — mirroring dequeueBatch's hold-back arm.
		q.oldestPendingMillis.Store(next.time)

		return nil, false
	}

	q.head = next
	q.queueLength.Add(-int64(len(next.nodes))) // gosec:nolint
	q.updateOldestPending()

	return next, true
}

// updateOldestPending refreshes the oldest-pending timestamp after the consumer
// advances head, and is also the consumer's EMPTY-OBSERVATION clear: both
// dequeue paths call it when they find head.next == nil, which is what stops a
// producer CAS that landed just after a drain from latching a non-zero age on an
// empty queue. It reads q.head, so it MUST only be called from the single
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
	// An empty queue has no pending batch, so report age 0 even when a stale
	// non-zero oldestPendingMillis is still latched — e.g. a producer CAS that
	// landed just before a long state transition (reorg/move-forward), whose only
	// clear runs on the dequeue paths that transition never takes. Reserve-first
	// accounting (see enqueueBatch) makes queueLength a safe empty oracle: it can
	// only over-count a linked batch, never read 0 while one is present, so this
	// clamp can never mask a real backlog. Reads only atomics, never q.head, so it
	// stays safe to call from a monitoring goroutine.
	if q.queueLength.Load() == 0 {
		return 0
	}

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
