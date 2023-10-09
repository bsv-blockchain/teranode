package subtreeprocessor

import (
	"sync/atomic"
)

// LockFreeQueue represents a FIFO structure with operations to enqueue
// and dequeue generic values.
// Reference: https://www.cs.rochester.edu/research/synchronization/pseudocode/queues.html
type LockFreeQueue[T any] struct {
	head atomic.Pointer[node[T]]
	tail atomic.Pointer[node[T]]
}

// node represents a node in the queue
type node[T any] struct {
	value T
	next  atomic.Pointer[node[T]]
}

// newNode creates and initializes a node
func newNode[T any](v T) *node[T] {
	return &node[T]{value: v}
}

// NewLockFreeQueue creates and initializes a LockFreeQueue
func NewLockFreeQueue[T any]() *LockFreeQueue[T] {
	var n = node[T]{}
	lf := &LockFreeQueue[T]{
		head: atomic.Pointer[node[T]]{},
		tail: atomic.Pointer[node[T]]{},
	}

	lf.head.Store(&n)
	lf.tail.Store(&n)

	return lf
}

// Enqueue adds a series of Request to the queue
func (q *LockFreeQueue[T]) Enqueue(v T) {
	n := newNode(v)
	for {
		tail := q.tail.Load()
		next := tail.next.Load()
		if tail == q.tail.Load() {
			if next == nil {
				if tail.next.CompareAndSwap(next, n) {
					q.tail.CompareAndSwap(tail, n)
					return
				}
			} else {
				q.tail.CompareAndSwap(tail, next)
			}
		}
	}
}

// Dequeue removes a Request from the queue
func (q *LockFreeQueue[T]) Dequeue() T {
	var t T
	for {
		head := q.head.Load()
		tail := q.tail.Load()
		next := head.next.Load()
		if head == q.head.Load() {
			if head == tail {
				if next == nil {
					return t
				}
				q.tail.CompareAndSwap(tail, next)
			} else {
				v := next.value
				if q.head.CompareAndSwap(head, next) {
					return v
				}
			}
		}
	}
}

// IsEmpty Checks if the queue is empty.
func (q *LockFreeQueue[T]) IsEmpty() bool {
	return q.head.Load() == q.tail.Load()
}
