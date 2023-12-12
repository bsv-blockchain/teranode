package txmetacache

import (
	"sync/atomic"
	"time"

	"github.com/libsv/go-bt/v2/chainhash"
)

type ttlQueueItem struct {
	hash *chainhash.Hash
	time int64
	next atomic.Pointer[ttlQueueItem]
}

// LockFreeTTLQueue represents a FIFO structure with operations to enqueue
// and dequeue generic values.
// This implementation is concurrent safe for queueing, but not for dequeueing.
// Reference: https://www.cs.rochester.edu/research/synchronization/pseudocode/queues.html
type LockFreeTTLQueue struct {
	head         atomic.Pointer[ttlQueueItem]
	tail         *ttlQueueItem
	previousTail *ttlQueueItem
	queueLength  atomic.Int64
}

// NewLockFreeTTLQueue creates and initializes a LockFreeQueue
func NewLockFreeTTLQueue() *LockFreeTTLQueue {
	firstTail := &ttlQueueItem{}
	lf := &LockFreeTTLQueue{
		head:         atomic.Pointer[ttlQueueItem]{},
		tail:         firstTail,
		previousTail: firstTail,
		queueLength:  atomic.Int64{},
	}

	lf.head.Store(nil)

	return lf
}

func (q *LockFreeTTLQueue) length() int64 {
	return q.queueLength.Load()
}

// Enqueue adds a series of Request to the queue
// enqueue is thread safe, it uses atomic operations to add to the queue
func (q *LockFreeTTLQueue) enqueue(v *ttlQueueItem) {
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
func (q *LockFreeTTLQueue) dequeue(upTill int64) *ttlQueueItem {
	next := q.tail.next.Load()
	if next == nil || next == q.previousTail || (upTill > 0 && next.time > upTill) {
		return nil
	}

	q.tail = next
	q.previousTail = next
	q.queueLength.Add(-1)

	return next
}

// IsEmpty Checks if the queue is empty.
func (q *LockFreeTTLQueue) IsEmpty() bool {
	return q.previousTail == q.tail
}
