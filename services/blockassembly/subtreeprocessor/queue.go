// Package subtreeprocessor provides functionality for processing transaction subtrees in Teranode.
package subtreeprocessor

import (
	"sync/atomic"

	"github.com/bsv-blockchain/go-subtree"
	"github.com/kpango/fastime"
)

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
	head        *TxBatch                // Points to the head of the queue (sentinel node)
	tail        atomic.Pointer[TxBatch] // Atomic pointer to the tail
	queueLength atomic.Int64            // Tracks the current number of batches in the queue
}

// NewLockFreeQueue creates and initializes a new LockFreeQueue instance.
//
// Returns:
//   - *LockFreeQueue: A new, initialized queue
func NewLockFreeQueue() *LockFreeQueue {
	return &LockFreeQueue{
		head:        &TxBatch{},
		tail:        atomic.Pointer[TxBatch]{},
		queueLength: atomic.Int64{},
	}
}

// length returns the current number of batches in the queue.
//
// Returns:
//   - int64: The current queue length (number of batches)
//
//go:inline
func (q *LockFreeQueue) length() int64 {
	return q.queueLength.Load()
}

// enqueueBatch adds a batch of transactions to the queue in a thread-safe manner.
// It uses atomic operations to ensure thread safety during concurrent enqueue operations.
// The entire batch receives a single timestamp when enqueued.
//
// Parameters:
//   - nodes: The transaction nodes to add
//   - txInpoints: Parent transaction references for each node
func (q *LockFreeQueue) enqueueBatch(nodes []subtree.Node, txInpoints []*subtree.TxInpoints) {
	batch := &TxBatch{
		nodes:      nodes,
		txInpoints: txInpoints,
		time:       fastime.Now().UnixMilli(),
	}
	batch.next.Store(nil)

	prev := q.tail.Swap(batch)
	if prev == nil {
		q.head.next.Store(batch)
		q.queueLength.Add(int64(len(nodes))) // gosec:nolint

		return
	}

	prev.next.Store(batch)
	q.queueLength.Add(int64(len(nodes))) // gosec:nolint
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
		return nil, false
	}

	q.head = next
	q.queueLength.Add(-int64(len(next.nodes))) // gosec:nolint

	return next, true
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
