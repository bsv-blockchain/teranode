// Package subtreeprocessor provides functionality for processing transaction subtrees in Teranode.
package subtreeprocessor

import (
	"sync"
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

var txIDAndFeePool = sync.Pool{
	New: func() any {
		return &TxIDAndFee{}
	},
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
//
// Parameters:
//   - v: The transaction to add to the queue
func (q *LockFreeQueue) enqueue(node subtree.Node, txInpoints subtree.TxInpoints) {
	v := txIDAndFeePool.Get().(*TxIDAndFee)

	v.node = node
	v.txInpoints = txInpoints
	v.time = fastime.Now().UnixMilli()
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
//
// Parameters:
//   - validFromMillis: Optional timestamp to filter transactions
//
// Returns:
//   - *TxIDAndFee: The next transaction in the queue, or nil if empty
func (q *LockFreeQueue) dequeue(validFromMillis int64) (subtree.Node, subtree.TxInpoints, int64, bool) {
	next := q.head.next.Load()

	if next == nil {
		return subtree.Node{}, subtree.TxInpoints{}, 0, false
	}

	if validFromMillis > 0 && next.time >= validFromMillis {
		return subtree.Node{}, subtree.TxInpoints{}, 0, false
	}

	oldItem := q.head
	q.head = next

	// return the dequeued to the pool for reuse
	txIDAndFeePool.Put(oldItem)

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
