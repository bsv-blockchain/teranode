package txmetacache

import (
	"sync"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/cespare/xxhash/v2"
)

// writeBackLanes partitions the cache buckets for the BatchDecorate write-back
// the way the validator partitions the txmeta Kafka topic (see
// sendTxMetaBatchV2 in services/validator): each lane owns a contiguous,
// disjoint range of buckets. With 256 lanes the ranges match the 256 txmeta
// partitions, so a write-back lane competes for bucket locks with at most one
// Kafka partition writer.
//
// Each lane has at most one writer at a time. Under a miss burst thousands of
// BatchDecorate calls complete together; without lanes every one of them
// wrote into the cache at once and the Kafka txmeta handler, one waiter among
// thousands on every bucket lock it needed, fell behind for good.
const writeBackLanes = min(256, BucketsCount)

const bucketsPerWriteBackLane = BucketsCount / writeBackLanes

// maxRetainedLaneScratch caps the merge scratch a lane keeps between writes, so
// one burst does not pin its largest-ever batch for the life of the process.
const maxRetainedLaneScratch = 4 * 1024

// write stores the merged batch. A panic is turned into an error: otherwise the
// lane would stay marked as writing and every later caller hashing into it
// would block forever.
func (l *writeBackLane) write(t *TxMetaCache) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.NewProcessingError("txmeta cache write-back panicked: %v", r)
		}
	}()

	return t.SetCacheMultiValuesRaw(l.keys, l.values)
}

func writeBackLaneOf(key []byte) int {
	return int(xxhash.Sum64(key) % BucketsCount / bucketsPerWriteBackLane)
}

// writeBackEvent tells the caller that owns a request either that its entries
// are written (write == false) or that it now owns the lane and must write the
// lane's queue (write == true).
type writeBackEvent struct {
	lane  *writeBackLane
	write bool
}

// writeBackRequest is one caller's share of a lane: a contiguous slice of the
// caller's keys/values that all hash into the lane.
type writeBackRequest struct {
	keys   [][]byte
	values [][]byte
	err    error
	events chan<- writeBackEvent
}

// writeBackLane group-commits the requests queued for its bucket range. A
// writer takes everything queued, writes it as one batch, then hands the lane
// to the owner of the oldest request that arrived meanwhile, so no caller
// writes other callers' entries for more than one batch. No goroutines are
// started: the writer is always a caller.
type writeBackLane struct {
	mu      sync.Mutex
	pending []*writeBackRequest
	writing bool

	// Merge scratch, only touched by the lane's current writer.
	keys   [][]byte
	values [][]byte
}

// writeBack stores the BatchDecorate entries, batched per lane, and returns
// once every entry has been written into the cache. Callers rely on that: the
// values live in pooled buffers that are recycled after BatchDecorate returns.
func (t *TxMetaCache) writeBack(keys, values [][]byte) error {
	laneOf := make([]uint16, len(keys))

	var counts [writeBackLanes]int

	for i, key := range keys {
		lane := writeBackLaneOf(key)
		laneOf[i] = uint16(lane) //nolint:gosec // lane < writeBackLanes <= 256
		counts[lane]++
	}

	// Reorder the entries by lane so each request is a sub-slice.
	var starts [writeBackLanes]int

	nonEmpty := 0
	offset := 0

	for lane, n := range counts {
		starts[lane] = offset
		offset += n

		if n > 0 {
			nonEmpty++
		}
	}

	sortedKeys := make([][]byte, len(keys))
	sortedValues := make([][]byte, len(values))
	next := starts

	for i, lane := range laneOf {
		sortedKeys[next[lane]] = keys[i]
		sortedValues[next[lane]] = values[i]
		next[lane]++
	}

	// At most two events per request (lane handed over, then written), so
	// senders never block on this channel.
	events := make(chan writeBackEvent, 2*nonEmpty)
	requests := make([]writeBackRequest, 0, nonEmpty)

	for lane, n := range counts {
		if n == 0 {
			continue
		}

		requests = append(requests, writeBackRequest{
			keys:   sortedKeys[starts[lane] : starts[lane]+n],
			values: sortedValues[starts[lane] : starts[lane]+n],
			events: events,
		})

		l := &t.lanes[lane]
		if l.enqueue(&requests[len(requests)-1]) {
			events <- writeBackEvent{lane: l, write: true}
		}
	}

	for written := 0; written < len(requests); {
		ev := <-events
		if ev.write {
			ev.lane.writeOnce(t)
			continue
		}

		written++
	}

	for i := range requests {
		if requests[i].err != nil {
			return requests[i].err
		}
	}

	return nil
}

// enqueue queues r and reports whether the caller became the lane's writer.
func (l *writeBackLane) enqueue(r *writeBackRequest) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.pending = append(l.pending, r)

	if l.writing {
		return false
	}

	l.writing = true

	return true
}

// writeOnce writes everything queued on the lane as one batch, then hands the
// lane to the next queued request's owner, or releases it.
func (l *writeBackLane) writeOnce(t *TxMetaCache) {
	l.mu.Lock()
	batch := l.pending
	l.pending = nil
	l.mu.Unlock()

	l.keys = l.keys[:0]
	l.values = l.values[:0]

	for _, req := range batch {
		l.keys = append(l.keys, req.keys...)
		l.values = append(l.values, req.values...)
	}

	err := l.write(t)

	// Drop references to the callers' (pooled) buffers before they are recycled.
	clear(l.keys)
	clear(l.values)

	if cap(l.keys) > maxRetainedLaneScratch {
		l.keys, l.values = nil, nil
	}

	for _, req := range batch {
		req.err = err
		req.events <- writeBackEvent{lane: l}
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.pending) == 0 {
		l.writing = false
		return
	}

	// Stay marked as writing; the next owner writes. Its events channel is
	// buffered for this, so the hand-off never blocks.
	l.pending[0].events <- writeBackEvent{lane: l, write: true}
}
