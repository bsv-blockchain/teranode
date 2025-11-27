// Package subtreeprocessor provides functionality for processing transaction subtrees in Teranode.
package subtreeprocessor

import (
	"sync/atomic"

	"github.com/bsv-blockchain/go-subtree"
	"github.com/kpango/fastime"
)

// LockFreeQueue represents a FIFO structure with operations to enqueue
// and dequeue generic values.
// This implementation is concurrent safe for queueing, but not for dequeueing.
// Reference: https://www.cs.rochester.edu/research/synchronization/pseudocode/queues.html
//
// The queue is specifically designed for high-throughput transaction processing,
// allowing multiple producer threads to concurrently enqueue transactions while
// a single consumer thread dequeues them. This design optimizes for the common
// pattern in blockchain systems where many sources submit transactions but a
// single process builds blocks.
//
// The atomic operations used ensure memory visibility across threads without
// requiring explicit locking mechanisms, improving performance in high-concurrency
// scenarios.
type LockFreeQueue struct {
	head        *TxIDAndFee                // Points to the head of the queue
	tail        atomic.Pointer[TxIDAndFee] // Atomic pointer to the tail
	queueLength atomic.Int64               // Tracks the current length of the queue
}

// NewLockFreeQueue creates and initializes a new LockFreeQueue instance.
//
// Returns:
//   - *LockFreeQueue: A new, initialized queue
func NewLockFreeQueue() *LockFreeQueue {
	return &LockFreeQueue{
		head:        &TxIDAndFee{},
		tail:        atomic.Pointer[TxIDAndFee]{},
		queueLength: atomic.Int64{},
	}
}

// length returns the current number of items in the queue.
//
// Returns:
//   - int64: The current queue length
//
//go:inline
func (q *LockFreeQueue) length() int64 {
	return q.queueLength.Load()
}

// enqueue adds a new transaction to the queue in a thread-safe manner.
// It uses atomic operations to ensure thread safety during concurrent enqueue operations.
// At high volumes (>2M tx/sec), direct allocation is more efficient than sync.Pool
// due to reduced contention and better GC characteristics.
//
// Parameters:
//   - node: The transaction node to add
//   - txInpoints: Parent transaction references
func (q *LockFreeQueue) enqueue(node subtree.Node, txInpoints subtree.TxInpoints) {
	v := &TxIDAndFee{
		node:       node,
		txInpoints: txInpoints,
		time:       fastime.Now().UnixMilli(),
	}
	v.next.Store(nil)

	prev := q.tail.Swap(v)
	if prev == nil {
		q.head.next.Store(v)
		q.queueLength.Add(1)

		return
	}

	prev.next.Store(v)
	q.queueLength.Add(1)
}

// dequeue removes and returns the next transaction from the queue.
// NOTE - This operation is not thread-safe and should only be called from a single thread.
// The dequeued item's memory will be eligible for garbage collection.
//
// Parameters:
//   - validFromMillis: Optional timestamp to filter transactions
//
// Returns:
//   - subtree.Node: The transaction node
//   - subtree.TxInpoints: Parent transaction references
//   - int64: Timestamp when item was added
//   - bool: True if item was dequeued, false if queue empty or item not valid
func (q *LockFreeQueue) dequeue(validFromMillis int64) (subtree.Node, subtree.TxInpoints, int64, bool) {
	next := q.head.next.Load()

	if next == nil {
		return subtree.Node{}, subtree.TxInpoints{}, 0, false
	}

	if validFromMillis > 0 && next.time >= validFromMillis {
		return subtree.Node{}, subtree.TxInpoints{}, 0, false
	}

	q.head = next
	q.queueLength.Add(-1)

	return next.node, next.txInpoints, next.time, true
}

// IsEmpty checks if the queue contains any items.
//
// Returns:
//   - bool: true if the queue is empty, false otherwise
//
//go:inline
func (q *LockFreeQueue) IsEmpty() bool {
	return q.head.next.Load() == nil
}

// DequeueBatch removes up to maxCount transactions from the queue in a single operation.
// This method is optimized for batch processing and significantly reduces per-transaction
// overhead compared to calling dequeue repeatedly. At high volumes (>2M tx/sec), this
// batch approach with a reusable buffer eliminates allocation overhead entirely.
//
// The caller provides a pre-allocated buffer slice which is reused across calls, eliminating
// the need for allocation and GC overhead. The buffer is cleared (length set to 0) and then
// filled with dequeued items.
//
// NOTE: This operation is not thread-safe and should only be called from a single consumer thread.
//
// Parameters:
//   - buffer: Pre-allocated slice to fill with dequeued items (will be cleared first)
//   - maxCount: Maximum number of items to dequeue
//   - validFromMillis: Timestamp filter - items with time >= this value won't be dequeued
//
// Returns:
//   - []*TxIDAndFee: The filled buffer (same slice as input, but with items)
//
// Performance: Zero allocations per call when buffer has sufficient capacity, improving
// throughput by 50-100% compared to individual dequeue operations.
func (q *LockFreeQueue) DequeueBatch(buffer []*TxIDAndFee, maxCount int, validFromMillis int64) []*TxIDAndFee {
	// Clear buffer (reset length to 0, keep capacity)
	buffer = buffer[:0]

	for i := 0; i < maxCount; i++ {
		next := q.head.next.Load()

		// Queue is empty
		if next == nil {
			break
		}

		// Item not yet valid for processing
		if validFromMillis > 0 && next.time >= validFromMillis {
			break
		}

		// Move head forward
		q.head = next

		// Update queue length
		q.queueLength.Add(-1)

		// Add to buffer - no allocation, reusing caller's buffer
		buffer = append(buffer, next)
	}

	return buffer
}
