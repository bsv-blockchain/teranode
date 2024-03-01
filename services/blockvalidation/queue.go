package blockvalidation

import (
	"github.com/libsv/go-bt/v2/chainhash"
	"sync/atomic"
	"time"
)

type subtreeFound struct {
	hash    chainhash.Hash
	baseURL string
	time    int64
	next    atomic.Pointer[subtreeFound]
}

// LockFreeQueue represents a FIFO structure with operations to enqueue
// and dequeue generic values.
// This implementation is concurrent safe for queueing, but not for dequeueing.
// Reference: https://www.cs.rochester.edu/research/synchronization/pseudocode/queues.html
type LockFreeQueue struct {
	head         atomic.Pointer[subtreeFound]
	tail         *subtreeFound
	previousTail *subtreeFound
	queueLength  atomic.Int64
}

// NewLockFreeQueue creates and initializes a LockFreeQueue
func NewLockFreeQueue() *LockFreeQueue {
	firstTail := &subtreeFound{}
	lf := &LockFreeQueue{
		head:         atomic.Pointer[subtreeFound]{},
		tail:         firstTail,
		previousTail: firstTail,
		queueLength:  atomic.Int64{},
	}

	lf.head.Store(nil)

	return lf
}

func (q *LockFreeQueue) length() int64 {
	return q.queueLength.Load()
}

// Enqueue adds a series of Request to the queue
// enqueue is thread safe, it uses atomic operations to add to the queue
func (q *LockFreeQueue) enqueue(v *subtreeFound) {
	v.time = time.Now().UnixMilli()
	prev := q.head.Swap(v)
	if prev == nil {
		q.tail.next.Store(v)
		return
	}
	prev.next.Store(v)
	q.queueLength.Add(1)
}

// Dequeue removes a Request from the queue
// dequeue is not thread safe, it should only be called from a single thread !!!
func (q *LockFreeQueue) dequeue() *subtreeFound {
	next := q.tail.next.Load()

	if next == nil || next == q.previousTail {
		return nil
	}

	if next != nil {
		q.tail = next
	}

	q.previousTail = next
	q.queueLength.Add(-1)
	return next
}

// IsEmpty Checks if the queue is empty.
func (q *LockFreeQueue) IsEmpty() bool {
	return q.previousTail == q.tail
}
