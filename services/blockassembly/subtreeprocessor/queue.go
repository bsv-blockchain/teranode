package subtreeprocessor

import (
	"sync/atomic"

	"github.com/kpango/fastime"
)

// LockFreeQueue represents a FIFO structure with operations to enqueue
// and dequeue generic values.
// This implementation is concurrent safe for queueing, but not for dequeueing.
// Reference: https://www.cs.rochester.edu/research/synchronization/pseudocode/queues.html
type LockFreeQueue struct {
	head        *TxIDAndFee
	tail        atomic.Pointer[TxIDAndFee]
	queueLength atomic.Int64
}

// NewLockFreeQueue creates and initializes a LockFreeQueue
func NewLockFreeQueue() *LockFreeQueue {
	return &LockFreeQueue{
		head:        &TxIDAndFee{},
		tail:        atomic.Pointer[TxIDAndFee]{},
		queueLength: atomic.Int64{},
	}
}

func (q *LockFreeQueue) length() int64 {
	return q.queueLength.Load()
}

// Enqueue adds a series of Request to the queue
// enqueue is thread safe, it uses atomic operations to add to the queue
func (q *LockFreeQueue) enqueue(v *TxIDAndFee) {
	v.time = fastime.Now().UnixMilli()
	prev := q.tail.Swap(v)
	if prev == nil {
		q.head.next.Store(v)
		return
	}
	prev.next.Store(v)
	q.queueLength.Add(1)
}

// Dequeue removes a Request from the queue
// dequeue is not thread safe, it should only be called from a single thread !!!
func (q *LockFreeQueue) dequeue(validFromMillis int64) *TxIDAndFee {
	next := q.head.next.Load()

	if next == nil {
		return nil
	}

	if validFromMillis > 0 && next.time >= validFromMillis {
		return nil
	}

	q.head = next
	q.queueLength.Add(-1)
	return next
}

// IsEmpty Checks if the queue is empty.
func (q *LockFreeQueue) IsEmpty() bool {
	return q.head.next.Load() == nil
}
