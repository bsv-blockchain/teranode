package subtreeprocessor

import (
	"context"
	"slices"
	"sync"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
)

// StoreClass is the kind of a store request. A chain request stores a subtree of the chain and may
// be dropped once its subtree has left it; a snapshot request stores a copy of the open subtree for
// a caller that waits for it, and is never dropped and never waits for room.
type StoreClass uint8

const (
	StoreClassChain StoreClass = iota
	StoreClassSnapshot
)

// Estimated bytes a request holds per leaf of its subtree until its store settles: the serialized
// subtree (32 byte hash, fee and size), and the meta, which is built for a request with a
// ParentTxMap (up to about 120 bytes per leaf).
const (
	subtreeBytesPerLeaf = 48
	metaBytesPerLeaf    = 128
)

// StoreRequestBytes is what a request is charged: its subtree's serialization and, when it has a
// ParentTxMap, its meta. A request that only announces holds nothing.
func StoreRequestBytes(req NewSubtreeRequest) uint64 {
	if req.AnnounceOnly || req.Subtree == nil {
		return 0
	}

	leaves := uint64(req.Subtree.Length()) //nolint:gosec // not negative
	bytes := leaves * subtreeBytesPerLeaf

	if req.ParentTxMap != nil {
		bytes += leaves * metaBytesPerLeaf
	}

	return bytes
}

// StoreQueue is the store-side queue behind blockassembly_storedPrefixTemplate (issue 2269). The
// subtree processor hands it every store request and never waits: Push only appends. The workers
// Take from it.
//
// It enforces a byte limit. A request is charged from the moment it is admitted until its store is
// settled (Settled), so the bytes of a request that is queued, being written, waiting for a retry
// or abandoned by a write that does not return are all counted. A request that does not fit waits,
// not admitted, in this queue and not in the processor. At the limit, chain requests whose subtree
// has left the chain (Stale) are dropped, queued and waiting alike; a snapshot is never dropped.
// A request larger than the limit is admitted when nothing is held.
//
// Workers take snapshots first, then the chain requests that are still wanted in the order they
// were sent, then the stale ones. A sequence number is given when a request is taken, so a request
// removed before that leaves no gap for the in-order notification sender.
type StoreQueue struct {
	mu    sync.Mutex
	wake  chan struct{} // closed, and replaced, when a worker may find something to take
	limit uint64
	held  uint64
	seq   uint64

	snapshots []*storeItem
	ready     []*storeItem // admitted and charged, not yet taken
	waiting   []*storeItem // not admitted: no room
	byTree    map[*subtreepkg.Subtree][]*storeItem
}

// storeItem is a request in the queue, until its store is settled or it is dropped.
type storeItem struct {
	req     NewSubtreeRequest
	bytes   uint64
	stale   bool
	charged bool
	done    bool

	// first is closed once ErrChan was answered, with the error in firstErr; result is the request's
	// Result. A reorg that finds its subtree already sent waits on these instead of sending it again.
	first    chan struct{}
	firstErr error
	result   chan StoreResult
}

// NewStoreQueue returns a queue that holds at most maxBytes of requests.
func NewStoreQueue(maxBytes uint64) *StoreQueue {
	return &StoreQueue{limit: maxBytes, wake: make(chan struct{}), byTree: make(map[*subtreepkg.Subtree][]*storeItem)}
}

// Push hands a request to the queue. It never blocks.
func (q *StoreQueue) Push(req NewSubtreeRequest) {
	it := &storeItem{bytes: StoreRequestBytes(req), first: make(chan struct{})}

	if req.Result == nil && !req.AnnounceOnly {
		req.Result = make(chan StoreResult, 1)
	}

	it.result = req.Result
	req.answered = func(err error) { q.answered(it, err) }
	req.settled = func() { q.settled(it) }
	it.req = req

	q.mu.Lock()

	if req.Class == StoreClassSnapshot {
		it.charged = true
		q.held += it.bytes
		q.snapshots = append(q.snapshots, it)
	} else {
		if !req.AnnounceOnly {
			q.byTree[req.Subtree] = append(q.byTree[req.Subtree], it)
		}

		q.waiting = append(q.waiting, it)
	}

	dropped := q.admitLocked()
	q.signalLocked()
	q.mu.Unlock()

	finishDropped(dropped)
}

// Take returns the next request and its sequence number, waiting until there is one. It reports
// false when ctx ends first.
func (q *StoreQueue) Take(ctx context.Context) (NewSubtreeRequest, uint64, bool) {
	for {
		q.mu.Lock()

		if it := q.pickLocked(); it != nil {
			seq := q.seq
			q.seq++
			q.mu.Unlock()

			return it.req, seq, true
		}

		wake := q.wake
		q.mu.Unlock()

		select {
		case <-wake:
		case <-ctx.Done():
			return NewSubtreeRequest{}, 0, false
		}
	}
}

// Stale marks the requests for subtrees that have left the chain: they are taken last, and dropped
// when room is needed.
func (q *StoreQueue) Stale(subtrees ...*subtreepkg.Subtree) {
	q.mu.Lock()

	for _, st := range subtrees {
		for _, it := range q.byTree[st] {
			it.stale = true
		}
	}

	dropped := q.admitLocked()
	q.signalLocked()
	q.mu.Unlock()

	finishDropped(dropped)
}

// Wanted undoes Stale for subtrees that are back in the chain, after a block movement failed and
// restored the chain it started from.
func (q *StoreQueue) Wanted(subtrees ...*subtreepkg.Subtree) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, st := range subtrees {
		for _, it := range q.byTree[st] {
			it.stale = false
		}
	}
}

// Held is the bytes charged to requests that are queued, in flight or waiting for a retry.
func (q *StoreQueue) Held() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.held
}

// Len is the number of requests not yet taken, admitted or not.
func (q *StoreQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return len(q.snapshots) + len(q.ready) + len(q.waiting)
}

// entry is the newest request of st whose store is not settled, or nil.
func (q *StoreQueue) entry(st *subtreepkg.Subtree) *storeItem {
	q.mu.Lock()
	defer q.mu.Unlock()

	items := q.byTree[st]
	if len(items) == 0 {
		return nil
	}

	return items[len(items)-1]
}

// pickLocked removes and returns the request a worker takes next.
func (q *StoreQueue) pickLocked() *storeItem {
	if len(q.snapshots) > 0 {
		it := q.snapshots[0]
		q.snapshots = removeAt(q.snapshots, 0)

		return it
	}

	if len(q.ready) == 0 {
		return nil
	}

	i := slices.IndexFunc(q.ready, func(it *storeItem) bool { return !it.stale })
	if i < 0 {
		i = 0
	}

	it := q.ready[i]
	q.ready = removeAt(q.ready, i)

	return it
}

// admitLocked moves waiting requests to ready while they fit, wanted ones first. When the next one
// does not fit it drops the stale requests; with none to drop it leaves it waiting. It returns the
// dropped requests, which the caller finishes after unlocking.
func (q *StoreQueue) admitLocked() (dropped []*storeItem) {
	for len(q.waiting) > 0 {
		i := slices.IndexFunc(q.waiting, func(it *storeItem) bool { return !it.stale })
		if i < 0 {
			i = 0
		}

		it := q.waiting[i]
		if q.held == 0 || q.held+it.bytes <= q.limit {
			q.waiting = removeAt(q.waiting, i)
			it.charged = true
			q.held += it.bytes
			q.ready = append(q.ready, it)

			continue
		}

		stale := q.dropStaleLocked()
		if len(stale) == 0 {
			break
		}

		dropped = append(dropped, stale...)
	}

	return dropped
}

// dropStaleLocked removes every stale chain request that has not been taken.
func (q *StoreQueue) dropStaleLocked() (dropped []*storeItem) {
	isStale := func(it *storeItem) bool { return it.stale }

	for _, it := range q.ready {
		if it.stale {
			q.held -= it.bytes
			it.charged = false
		}
	}

	for _, list := range []*[]*storeItem{&q.ready, &q.waiting} {
		for _, it := range *list {
			if it.stale {
				dropped = append(dropped, it)
			}
		}

		*list = slices.DeleteFunc(*list, isStale)
	}

	for _, it := range dropped {
		it.done = true
		q.unregisterLocked(it)
		close(it.first)
	}

	return dropped
}

func (q *StoreQueue) unregisterLocked(it *storeItem) {
	if it.req.AnnounceOnly {
		return
	}

	items := slices.DeleteFunc(q.byTree[it.req.Subtree], func(other *storeItem) bool { return other == it })
	if len(items) == 0 {
		delete(q.byTree, it.req.Subtree)
	} else {
		q.byTree[it.req.Subtree] = items
	}
}

func (q *StoreQueue) signalLocked() {
	close(q.wake)
	q.wake = make(chan struct{})
}

// answered notes that a worker answered the request's ErrChan, which can come after the request
// settled: a store that aborted settles before its worker answers.
func (q *StoreQueue) answered(it *storeItem, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	select {
	case <-it.first:
	default:
		it.firstErr = err

		close(it.first)
	}
}

// settled ends the request's charge, and admits what now fits.
func (q *StoreQueue) settled(it *storeItem) {
	q.mu.Lock()

	if it.done {
		q.mu.Unlock()

		return
	}

	it.done = true

	if it.charged {
		q.held -= it.bytes
		it.charged = false
	}

	q.unregisterLocked(it)

	dropped := q.admitLocked()
	q.signalLocked()
	q.mu.Unlock()

	finishDropped(dropped)
}

// finishDropped answers the callers of dropped requests: ErrChan with nil, as a retry that is still
// going would, and Result as cancelled, which is neither stored nor failed.
func finishDropped(dropped []*storeItem) {
	for _, it := range dropped {
		if it.result != nil {
			select {
			case it.result <- StoreResult{Cancelled: true, Err: errors.NewStorageError("store request dropped: its subtree left the chain")}:
			default:
			}
		}

		if it.req.ErrChan != nil {
			select {
			case it.req.ErrChan <- nil:
			default:
			}
		}
	}
}

func removeAt[T any](s []T, i int) []T {
	if i == 0 {
		var zero T

		s[0] = zero

		return s[1:]
	}

	return slices.Delete(s, i, i+1)
}

// Answered is called by the worker after it answered the request's ErrChan, so a reorg waiting for
// the first attempt to end sees it. Without a queue it does nothing.
func (r NewSubtreeRequest) Answered(err error) {
	if r.answered != nil {
		r.answered(err)
	}
}

// Settled is called when the request's store is settled, stored or failed for good: the queue
// stops counting its bytes. Without a queue it does nothing.
func (r NewSubtreeRequest) Settled() {
	if r.settled != nil {
		r.settled()
	}
}
