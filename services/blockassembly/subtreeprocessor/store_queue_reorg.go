package subtreeprocessor

import (
	"context"
	"time"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
)

// The two calls of a fork reorg to the store, with a StoreQueue (blockassembly_storedPrefixTemplate).
// Every subtree of the chain was sent to the queue as it was built, so the reorg does not send it
// again: it waits for the request that is there, and announces what is stored.

// awaitQueuedStores waits for the store of every chained subtree. A subtree whose request is no
// longer in the queue is looked up in the stored set, which a settled store fills; one that was
// never sent, or was dropped, is sent now, once. With a deadline the wait is for each request's
// typed result and a store that did not finish within it, failed, or left the meta unwritten fails
// the reorg; without one the wait is for ErrChan, as it always was, which a failed write answers
// with nil, and a store never fails the reorg. A request dropped while the reorg waits for it is sent
// again, once. Only the first wait subtrees are waited for (reorgAwaitedSubtrees): the others are
// already queued, and announceChainedSubtrees sees to the ones that are not.
func (stp *SubtreeProcessor) awaitQueuedStores(ctx context.Context, caller string, wait int, deadline time.Duration) error {
	q := stp.storeQueue
	waits := make([]*storeItem, min(wait, len(stp.chainedSubtrees)))

	var timer <-chan time.Time

	if deadline > 0 {
		t := time.NewTimer(deadline)
		defer t.Stop()

		timer = t.C
	}

	for i := range waits {
		st := stp.chainedSubtrees[i]

		if waits[i] = q.entry(st); waits[i] != nil {
			continue
		}

		root := *st.RootHash()
		if stp.storedSet != nil && stp.storedSet.Has(root) {
			continue
		}

		if stp.storedSet != nil && stp.storedSet.HasFailed(root) {
			if deadline == 0 {
				continue
			}

			return errors.NewStorageError("[%s] subtree %d was not stored", caller, i)
		}

		var err error
		if waits[i], err = stp.sendForAwait(ctx, caller, st); err != nil {
			return err
		}
	}

	for i, it := range waits {
		if it == nil {
			continue
		}

		for resent := false; ; {
			dropped, err := stp.awaitOne(ctx, caller, i, it, deadline, timer)
			if err != nil {
				return err
			}

			if !dropped {
				break
			}

			if resent {
				if deadline > 0 {
					return errors.NewStorageError("[%s] subtree %d was not stored: its request was dropped twice", caller, i)
				}

				break
			}

			resent = true

			if it, err = stp.sendForAwait(ctx, caller, stp.chainedSubtrees[i]); err != nil {
				return err
			}
		}
	}

	return nil
}

// sendForAwait sends st to the store, silently, for a reorg that waits for it.
func (stp *SubtreeProcessor) sendForAwait(ctx context.Context, caller string, st *subtreepkg.Subtree) (*storeItem, error) {
	if err := stp.sendNewSubtree(ctx, NewSubtreeRequest{
		Subtree:           st,
		ParentTxMap:       stp.currentTxMap,
		superseded:        stp.supersedeToken(),
		DeletedTxs:        stp.deletedTxs,
		SkipNotification:  true,
		ErrChan:           make(chan error, 1),
		OnStorageComplete: func() { stp.cleanupDeletedTxs(st) },
	}); err != nil {
		return nil, errors.NewProcessingError("[%s] context cancelled while sending subtree", caller, err)
	}

	return stp.storeQueue.entry(st), nil
}

// awaitOne waits for one request and reports whether the queue dropped it instead.
func (stp *SubtreeProcessor) awaitOne(ctx context.Context, caller string, i int, it *storeItem, deadline time.Duration, timer <-chan time.Time) (dropped bool, err error) {
	if it == nil {
		return false, nil
	}

	if deadline == 0 {
		select {
		case <-it.first:
			if stp.storeQueue.wasDropped(it) {
				return true, nil
			}

			if it.firstErr != nil {
				return false, errors.NewProcessingError("[%s] error sending subtree to the store queue", caller, it.firstErr)
			}
		case <-ctx.Done():
			return false, errors.NewProcessingError("[%s] context cancelled while awaiting subtree stores", caller, ctx.Err())
		}

		return false, nil
	}

	select {
	case r := <-it.result:
		if r.Cancelled && stp.storeQueue.wasDropped(it) {
			return true, nil
		}

		if !r.OK() {
			return false, errors.NewStorageError("[%s] subtree %d was not stored (cancelled %t)", caller, i, r.Cancelled, r.Err)
		}
	case <-timer:
		return false, errors.NewStorageError("[%s] subtree %d did not finish storing within %s", caller, i, deadline)
	case <-ctx.Done():
		return false, errors.NewProcessingError("[%s] context cancelled while awaiting subtree stores", caller, ctx.Err())
	}

	return false, nil
}

// announceChainedSubtrees announces the chain after the reorg's marks. A stored subtree is announced
// by a request that stores nothing. One whose request is still outstanding, queued or being retried,
// is told to announce when it is stored: the forward step sent it silently, and another full store
// would only add to the backlog. One with no request is sent to the store, which announces it.
func (stp *SubtreeProcessor) announceChainedSubtrees(ctx context.Context, caller string) error {
	for _, st := range stp.chainedSubtrees {
		stored := stp.storedSet != nil && stp.storedSet.Has(*st.RootHash())

		if !stored && stp.storeQueue.AnnounceOnSuccess(st) {
			continue // announced when it settles
		}

		// otherwise there is no request: stored, or settled since the check above
		stored = stored || (stp.storedSet != nil && stp.storedSet.Has(*st.RootHash()))

		req := NewSubtreeRequest{Subtree: st, AnnounceOnly: true}

		if !stored {
			req = NewSubtreeRequest{
				Subtree:           st,
				ParentTxMap:       stp.currentTxMap,
				superseded:        stp.supersedeToken(),
				DeletedTxs:        stp.deletedTxs,
				ErrChan:           make(chan error, 1),
				OnStorageComplete: func() { stp.cleanupDeletedTxs(st) },
			}
		}

		if err := stp.sendNewSubtree(ctx, req); err != nil {
			return errors.NewProcessingError("[%s] context cancelled while announcing subtrees", caller, err)
		}
	}

	return nil
}

// wantRestored is for a failed block movement that put its chain back. Retiring the chain it built
// on marked its requests stale, and a worker that came to one cancelled it (or the byte limit dropped
// it); the restored subtrees are wanted again, so the ones left are unmarked and the ones dropped are
// sent again, announcing if the dropped request would have: nothing else announces after a rollback.
func (stp *SubtreeProcessor) wantRestored(chained []*subtreepkg.Subtree) {
	stp.storeQueue.Wanted(chained...)

	for _, st := range chained {
		if stp.storeQueue.entry(st) != nil || (stp.storedSet != nil && stp.storedSet.Has(*st.RootHash())) {
			continue
		}

		stp.storeQueue.Push(NewSubtreeRequest{
			Subtree:           st,
			ParentTxMap:       stp.currentTxMap,
			superseded:        stp.supersedeToken(),
			DeletedTxs:        stp.deletedTxs,
			SkipNotification:  !stp.storeQueue.announceWasLost(st),
			ErrChan:           make(chan error, 1),
			OnStorageComplete: func() { stp.cleanupDeletedTxs(st) },
		})
	}
}

// runDroppedCleanups runs the deletedTxs cleanups of the requests the queue dropped. It runs on the
// processor goroutine, which is the one that unmaps a retired subtree, and closeRetiredSubtrees takes
// a subtree's queued requests out, and runs their cleanups, before it unmaps it: a request that no
// worker has taken holds no callback for a closed subtree. A request a worker has taken can still
// read its subtree after the close; that is the mmap hazard of bsv-blockchain/teranode#2287.
func (stp *SubtreeProcessor) runDroppedCleanups() {
	for _, cleanup := range stp.storeQueue.takeCleanups() {
		cleanup()
	}
}

// reorgAwaitedSubtrees is how many of the chained subtrees a fork reorg waits to have stored. With
// the stored-prefix cut on it is K0 = min(chain length, the leading subtrees that fit in a block,
// blockassembly_reorgAwaitedSubtrees), and at least one subtree of a chain: the rest store behind
// the template, which the cut keeps at the stored prefix, and a subtree beyond the block-size prefix
// is not in this block's template at all. With the cut off, or the setting 0, it is the whole chain:
// a template published over subtrees that are not stored would name files nothing can read.
func (stp *SubtreeProcessor) reorgAwaitedSubtrees() int {
	n := len(stp.chainedSubtrees)
	k0 := stp.settings.BlockAssembly.ReorgAwaitedSubtrees

	if k0 <= 0 || stp.storedSet == nil || stp.storeQueue == nil || n == 0 {
		return n
	}

	if budget, ok := stp.blockMaxSizeBudget(); ok {
		var total uint64

		fit := 0

		for _, st := range stp.chainedSubtrees {
			if total += st.SizeInBytes; total > budget {
				break
			}

			fit++
		}

		n = min(n, max(fit, 1))
	}

	return min(n, k0)
}
