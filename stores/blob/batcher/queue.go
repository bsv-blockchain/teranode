package batcher

import (
	"sync/atomic"
)

// LockFreeQueue represents a FIFO structure with operations to enqueue
// and dequeue generic values.
// This implementation is concurrent safe for queueing, but not for dequeueing.
// Reference: https://www.cs.rochester.edu/research/synchronization/pseudocode/queues.html
type LockFreeQueue struct {
	head         atomic.Pointer[BatchItem]
	tail         *BatchItem
	previousTail *BatchItem
}

// NewLockFreeQueue creates and initializes a LockFreeQueue
func NewLockFreeQueue() *LockFreeQueue {
	firstTail := &BatchItem{}
	lf := &LockFreeQueue{
		head:         atomic.Pointer[BatchItem]{},
		tail:         firstTail,
		previousTail: firstTail,
	}

	lf.head.Store(nil)

	return lf
}

// Enqueue adds a series of Request to the queue
// enqueue is thread safe, it uses atomic operations to add to the queue
func (q *LockFreeQueue) enqueue(v *BatchItem) {
	prev := q.head.Swap(v)
	if prev == nil {
		q.tail.next.Store(v)
		return
	}
	prev.next.Store(v)
}

// Dequeue removes a Request from the queue
// dequeue is not thread safe, it should only be called from a single thread !!!
func (q *LockFreeQueue) dequeue() *BatchItem {
	next := q.tail.next.Load()
	if next == nil || next == q.previousTail {
		return nil
	}

	if next != nil {
		q.tail = next
	}

	q.previousTail = next
	return next
}

// IsEmpty Checks if the queue is empty.
func (q *LockFreeQueue) IsEmpty() bool {
	return q.previousTail == q.tail
}
