package txmetacache

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// tombstones remembers the txs DeleteDemoted evicted, so that a write landing
// after the eviction cannot cache them again (see DeleteDemoted).
//
// The window is subtreevalidation_txMetaTombstoneWindow: it has to outlast
// consumer lag on the txmeta partition carrying a late ADD. Entries live between
// one and two windows. Two generations rotate every window. While both are
// empty — nearly always, demotions are rare — empty and contains are a single
// atomic load, so the write paths pay nothing. A nil *tombstones is valid and
// empty.
type tombstones struct {
	window time.Duration
	now    func() time.Time

	// size is len(current)+len(previous), read without the lock.
	size atomic.Int64

	mu        sync.RWMutex
	current   map[chainhash.Hash]struct{}
	previous  map[chainhash.Hash]struct{}
	rotatedAt time.Time
}

func newTombstones(window time.Duration, now func() time.Time) *tombstones {
	return &tombstones{
		window:    window,
		now:       now,
		current:   map[chainhash.Hash]struct{}{},
		previous:  map[chainhash.Hash]struct{}{},
		rotatedAt: now(),
	}
}

// add records hashes as deleted now.
func (t *tombstones) add(hashes []chainhash.Hash) {
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
func (t *tombstones) empty() bool {
	return t == nil || t.size.Load() == 0
}

// contains reports whether key, a 32-byte tx hash, was deleted within the window.
func (t *tombstones) contains(key []byte) bool {
	if t.empty() || len(key) != chainhash.HashSize {
		return false
	}

	t.rLockRotated()
	defer t.mu.RUnlock()

	return t.holdsLocked(chainhash.Hash(key))
}

// matches appends to dst the index of every key, a 32-byte tx hash, deleted
// within the window. A batch takes the lock and reads the clock once, not once
// per key, so writers sharing the lock while tombstones exist stay cheap.
func (t *tombstones) matches(keys [][]byte, dst []int) []int {
	if t.empty() {
		return dst
	}

	t.rLockRotated()
	defer t.mu.RUnlock()

	for i, key := range keys {
		if len(key) == chainhash.HashSize && t.holdsLocked(chainhash.Hash(key)) {
			dst = append(dst, i)
		}
	}

	return dst
}

// matchesHashes is matches for tx hashes.
func (t *tombstones) matchesHashes(hashes []chainhash.Hash, dst []int) []int {
	if t.empty() {
		return dst
	}

	t.rLockRotated()
	defer t.mu.RUnlock()

	for i := range hashes {
		if t.holdsLocked(hashes[i]) {
			dst = append(dst, i)
		}
	}

	return dst
}

// rLockRotated takes the read lock, rotating the generations first if a window
// has passed.
func (t *tombstones) rLockRotated() {
	t.mu.RLock()

	if t.now().Sub(t.rotatedAt) >= t.window {
		t.mu.RUnlock()
		t.mu.Lock()
		t.rotateLocked()
		t.mu.Unlock()
		t.mu.RLock()
	}
}

func (t *tombstones) holdsLocked(h chainhash.Hash) bool {
	if _, ok := t.current[h]; ok {
		return true
	}

	_, ok := t.previous[h]

	return ok
}

// rotateLocked ages the generations by however many windows have passed.
func (t *tombstones) rotateLocked() {
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
