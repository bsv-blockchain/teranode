package subtreeprocessor

import (
	"context"
	"sync/atomic"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
)

const (
	storageQueued uint32 = iota
	storageOwned
	storageFinished
)

type storageRequestLifecycle struct {
	storage *miningSnapshotStorage
	state   atomic.Uint32
}

// retainStorageRequest tracks metadata readers as well as mmap ownership. The
// processor must wait for these requests before clearing their ParentTxMap.
// Cancellation releases requests left in an abandoned queue.
func (stp *SubtreeProcessor) retainStorageRequest(ctx context.Context, req NewSubtreeRequest) NewSubtreeRequest {
	storage := &stp.miningSnapshots
	storage.mu.Lock()
	req.Lease = storage.retainLocked([]*subtreepkg.Subtree{req.Subtree})
	if storage.storagePending == 0 {
		storage.storageDone = make(chan struct{})
	}
	storage.storagePending++
	storage.mu.Unlock()
	req.storageLifecycle = &storageRequestLifecycle{storage: storage}
	lifecycle, lease := req.storageLifecycle, req.Lease
	req.stopLeaseRelease = context.AfterFunc(ctx, func() {
		if lifecycle.state.CompareAndSwap(storageQueued, storageFinished) {
			lifecycle.finish(lease)
		}
	})
	return req
}

// TakeStorageOwnership transfers a queued request to the listener. Cancellation
// and transfer compete on the request state: only an unclaimed request may be
// released by cancellation. A false result must be discarded without reading
// its subtree. The listener must release a successful transfer after all async
// storage readers and the completion callback have finished.
func (req NewSubtreeRequest) TakeStorageOwnership() (NewSubtreeRequest, bool) {
	if req.storageLifecycle != nil {
		if !req.storageLifecycle.state.CompareAndSwap(storageQueued, storageOwned) {
			return req, false
		}
		req.stopLeaseRelease()
		req.stopLeaseRelease = nil
		return req, true
	}
	// Requests supplied directly by callers have no queued lifecycle.
	lease, retained := req.Lease.Retain()
	req.Release()
	req.Lease = lease
	return req, retained
}

// Release releases request ownership, including the queue cancellation hook.
func (req NewSubtreeRequest) Release() {
	if req.stopLeaseRelease != nil {
		req.stopLeaseRelease()
	}
	if req.storageLifecycle == nil {
		req.Lease.Release()
		return
	}
	if req.storageLifecycle.state.Swap(storageFinished) != storageFinished {
		req.storageLifecycle.finish(req.Lease)
	}
}

func (lifecycle *storageRequestLifecycle) finish(lease *MiningSnapshotLease) {
	lease.Release()
	storage := lifecycle.storage
	storage.mu.Lock()
	defer storage.mu.Unlock()
	storage.storagePending--
	if storage.storagePending == 0 {
		close(storage.storageDone)
	}
}

// waitForSubtreeStorage runs in the processor dispatcher before a recovery
// rebuild. No further announcements can be admitted by that dispatcher while
// it waits, so completion protects ParentTxMap from being cleared under readers.
func (stp *SubtreeProcessor) waitForSubtreeStorage(ctx context.Context) error {
	storage := &stp.miningSnapshots
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		storage.mu.Lock()
		pending, done := storage.storagePending, storage.storageDone
		storage.mu.Unlock()
		if pending == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
		}
	}
}
