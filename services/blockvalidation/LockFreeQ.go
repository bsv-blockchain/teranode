package blockvalidation

import (
	"sync/atomic"
)

type node[T any] struct {
	value T
	next  atomic.Pointer[node[T]]
}

// LockFreeQueue represents a FIFO structure with operations to enqueue
// and dequeue generic values.
// This implementation is concurrent safe for queueing, but not for dequeueing.
// Reference: https://www.cs.rochester.edu/research/synchronization/pseudocode/queues.html
type LockFreeQ[T any] struct {
	head         atomic.Pointer[node[T]]
	tail         *node[T]
	previousTail *node[T]
}

// NewLockFreeQueue creates and initializes a LockFreeQueue
func NewLockFreeQ[T any]() *LockFreeQ[T] {
	firstTail := &node[T]{}
	lf := &LockFreeQ[T]{
		head:         atomic.Pointer[node[T]]{},
		tail:         firstTail,
		previousTail: firstTail,
	}

	lf.head.Store(nil)

	return lf
}

// Enqueue adds a series of Request to the queue
// enqueue is thread safe, it uses atomic operations to add to the queue
func (q *LockFreeQ[T]) enqueue(v T) {
	node := &node[T]{value: v}
	prev := q.head.Swap(node)
	if prev == nil {
		q.tail.next.Store(node)
		return
	}
	prev.next.Store(node)
}

// Dequeue removes a Request from the queue
// dequeue is not thread safe, it should only be called from a single thread !!!
func (q *LockFreeQ[T]) dequeue() *T {
	next := q.tail.next.Load()

	if next == nil || next == q.previousTail {
		return nil
	}

	if next != nil {
		q.tail = next
	}

	q.previousTail = next
	return &next.value
}

// IsEmpty Checks if the queue is empty.
func (q *LockFreeQ[T]) IsEmpty() bool {
	return q.previousTail == q.tail
}
