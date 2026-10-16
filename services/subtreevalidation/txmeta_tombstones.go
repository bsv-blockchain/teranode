package subtreevalidation

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// txMetaTombstoneWindow is how long a txmeta DELETE keeps blocking a late ADD
// for the same tx. Entries live between one and two windows. It is generous
// because it only has to outlast consumer lag on the partition carrying the ADD.
const txMetaTombstoneWindow = 30 * time.Minute

// txMetaTombstones remembers recently deleted txmeta hashes so txmetaHandler can
// drop an ADD that arrives after its DELETE.
//
// Block assembly's DELETE for a demoted tx and the validator's ADD for the same
// tx travel on different Kafka partitions, so the DELETE can be consumed first.
// Without this the late ADD re-caches the tx as non-conflicting, and the fresh
// subtree path then skips the counter-spend check for it. Dropping an ADD only
// costs a cache miss, which falls through to the store.
//
// Two generations rotate every window. While both are empty — nearly always,
// demotions are rare — contains is a single atomic load, so the ADD hot path
// pays nothing. A nil *txMetaTombstones is valid and empty.
type txMetaTombstones struct {
	window time.Duration
	now    func() time.Time

	// size is len(current)+len(previous), read without the lock.
	size atomic.Int64

	mu        sync.RWMutex
	current   map[chainhash.Hash]struct{}
	previous  map[chainhash.Hash]struct{}
	rotatedAt time.Time
}

func newTxMetaTombstones(window time.Duration, now func() time.Time) *txMetaTombstones {
	return &txMetaTombstones{
		window:    window,
		now:       now,
		current:   map[chainhash.Hash]struct{}{},
		previous:  map[chainhash.Hash]struct{}{},
		rotatedAt: now(),
	}
}

// add records hashes as deleted now.
func (t *txMetaTombstones) add(hashes []chainhash.Hash) {
	if t == nil || len(hashes) == 0 {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.rotateLocked()

	for _, h := range hashes {
		if _, ok := t.current[h]; !ok {
			t.current[h] = struct{}{}
		}
	}

	t.size.Store(int64(len(t.current) + len(t.previous)))
}

// empty reports whether no deletes are remembered, with one atomic load.
func (t *txMetaTombstones) empty() bool {
	return t == nil || t.size.Load() == 0
}

// contains reports whether key, a 32-byte tx hash, was deleted within the window.
func (t *txMetaTombstones) contains(key []byte) bool {
	if t.empty() {
		return false
	}

	h := chainhash.Hash(key)

	t.mu.RLock()
	due := t.now().Sub(t.rotatedAt) >= t.window
	_, inCurrent := t.current[h]
	_, inPrevious := t.previous[h]
	t.mu.RUnlock()

	if !due {
		return inCurrent || inPrevious
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.rotateLocked()

	_, inCurrent = t.current[h]
	_, inPrevious = t.previous[h]

	return inCurrent || inPrevious
}

// rotateLocked ages the generations by however many windows have passed.
func (t *txMetaTombstones) rotateLocked() {
	elapsed := t.now().Sub(t.rotatedAt)

	switch {
	case elapsed < t.window:
		return
	case elapsed < 2*t.window:
		t.previous = t.current
	default:
		t.previous = map[chainhash.Hash]struct{}{}
	}

	t.current = map[chainhash.Hash]struct{}{}
	t.rotatedAt = t.now()
	t.size.Store(int64(len(t.current) + len(t.previous)))
}
