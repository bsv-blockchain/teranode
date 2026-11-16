package file

import (
	"context"
	"sync"
	"time"
)

// blobKeyLocks serialises, per blob, the steps that decide its delete-at-height: an exclusive
// publish (its schedule and the publish itself) and SetDAH. It is per process and keyed by the
// blob's file name, so every File instance over the same directory shares it.
//
// The scheduler that holds delete-at-heights is last-writer-wins, and nothing in it knows
// whether the blob is on disk. So the order in which a publish and a SetDAH reach it is the
// whole answer, and it is decided here. Under this lock:
//
//   - an exclusive publish checks the name is free, schedules the blob's deletion, and then
//     publishes it, so a blob is never visible without its delete-at-height already scheduled;
//   - a publish that finds the name taken schedules nothing;
//   - SetDAH changes the delete-at-height of a blob only once no exclusive publish of it is in
//     flight.
//
// A promotion (SetDAH 0) can therefore only follow the publish's schedule, never precede it,
// and it is never undone by the publish it overlapped. A plain write (no exclusive publish) is
// not covered: it schedules before it publishes and takes no lock, as it always has, and a
// plain write over an existing blob still replaces that blob's delete-at-height.
//
// keyLockedSchedulerTimeout bounds each blob-deletion scheduler call made while a blob's key lock
// is held. The scheduler is a call to the blockchain service; one that hangs would otherwise hold
// the key, and every writer waiting on it, for as long as it hangs. 30 s is far above a healthy
// call.
var keyLockedSchedulerTimeout = 30 * time.Second

// One lock per name in use, created on demand and dropped when its last holder or waiter leaves,
// so a slow scheduler call on one blob never holds up another. A waiter gives up when its context
// ends: every caller holds a file-store write permit while it waits, and a wait with no way out
// let one hung holder pin, through its waiters, the whole permit pool (review of 76be891d6).
var blobKeyLocks = keyLocks{locks: make(map[string]*keyLock)}

type keyLocks struct {
	mu    sync.Mutex
	locks map[string]*keyLock
}

// keyLock is a one-slot channel: holding the lock is having sent into it.
type keyLock struct {
	slot chan struct{}
	refs int
}

// lock takes name's lock and returns the function that gives it back, or ctx's error if ctx ends
// first.
func (k *keyLocks) lock(ctx context.Context, name string) (func(), error) {
	k.mu.Lock()

	l, ok := k.locks[name]
	if !ok {
		l = &keyLock{slot: make(chan struct{}, 1)}
		k.locks[name] = l
	}

	l.refs++
	k.mu.Unlock()

	select {
	case l.slot <- struct{}{}:
	case <-ctx.Done():
		k.release(name, l)

		return nil, ctx.Err()
	}

	return func() {
		<-l.slot
		k.release(name, l)
	}, nil
}

// release drops one reference to name's lock, and the lock itself with the last.
func (k *keyLocks) release(name string, l *keyLock) {
	k.mu.Lock()
	defer k.mu.Unlock()

	l.refs--
	if l.refs == 0 {
		delete(k.locks, name)
	}
}
