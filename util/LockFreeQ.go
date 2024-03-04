package util

import (
	"sync/atomic"
)

type node[T any] struct {
	value T
	next  atomic.Pointer[node[T]]
}

// LockFreeQ represents a FIFO structure with operations to enqueue
// and dequeue generic values.
// This implementation is concurrent safe for queueing, but not for dequeueing.
// Reference: https://www.cs.rochester.edu/research/synchronization/pseudocode/queues.html
type LockFreeQ[T any] struct {
	tail atomic.Pointer[node[T]]
	head *node[T]
}

// NewLockFreeQ creates and initializes a LockFreeQueue
func NewLockFreeQ[T any]() *LockFreeQ[T] {
	return &LockFreeQ[T]{
		tail: atomic.Pointer[node[T]]{},
		head: &node[T]{},
	}
}

// Enqueue adds a series of Request to the queue
// Enqueue is thread safe, it uses atomic operations to add to the queue
func (q *LockFreeQ[T]) Enqueue(v T) {
	node := &node[T]{value: v}
	prev := q.tail.Swap(node)

	if prev == nil {
		q.head.next.Store(node)
		return
	}

	prev.next.Store(node)
}

// Dequeue removes a Request from the queue
// Dequeue is not thread safe, it should only be called from a single thread !!!
func (q *LockFreeQ[T]) Dequeue() *T {
	next := q.head.next.Load()

	if next == nil {
		return nil
	}

	q.head = next
	return &next.value
}

func (q *LockFreeQ[T]) IsEmpty() bool {
	return q.head.next.Load() == nil
}
