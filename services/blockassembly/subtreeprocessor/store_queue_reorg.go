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
// with nil.
func (stp *SubtreeProcessor) awaitQueuedStores(ctx context.Context, caller string, deadline time.Duration) error {
	q := stp.storeQueue
	waits := make([]*storeItem, len(stp.chainedSubtrees))

	var timer <-chan time.Time

	if deadline > 0 {
		t := time.NewTimer(deadline)
		defer t.Stop()

		timer = t.C
	}

	for i, st := range stp.chainedSubtrees {
		if waits[i] = q.entry(st); waits[i] != nil {
			continue
		}

		root := *st.RootHash()
		if stp.storedSet != nil && stp.storedSet.Has(root) {
			continue
		}

		if stp.storedSet != nil && stp.storedSet.HasFailed(root) && deadline == 0 {
			continue
		}

		if stp.storedSet != nil && stp.storedSet.HasFailed(root) {
			return errors.NewStorageError("[%s] subtree %d was not stored", caller, i)
		}

		if err := stp.sendNewSubtree(ctx, NewSubtreeRequest{
			Subtree:           st,
			ParentTxMap:       stp.currentTxMap,
			superseded:        stp.supersedeToken(),
			DeletedTxs:        stp.deletedTxs,
			SkipNotification:  true,
			ErrChan:           make(chan error, 1),
			OnStorageComplete: func() { stp.cleanupDeletedTxs(st) },
		}); err != nil {
			return errors.NewProcessingError("[%s] context cancelled while sending subtree", caller, err)
		}

		waits[i] = q.entry(st)
	}

	for i, it := range waits {
		if it == nil {
			continue
		}

		if deadline == 0 {
			select {
			case <-it.first:
				if it.firstErr != nil {
					return errors.NewProcessingError("[%s] error sending subtree to the store queue", caller, it.firstErr)
				}
			case <-ctx.Done():
				return errors.NewProcessingError("[%s] context cancelled while awaiting subtree stores", caller, ctx.Err())
			}

			continue
		}

		select {
		case r := <-it.result:
			if !r.OK() {
				return errors.NewStorageError("[%s] subtree %d was not stored (cancelled %t)", caller, i, r.Cancelled, r.Err)
			}
		case <-timer:
			return errors.NewStorageError("[%s] subtree %d did not finish storing within %s", caller, i, deadline)
		case <-ctx.Done():
			return errors.NewProcessingError("[%s] context cancelled while awaiting subtree stores", caller, ctx.Err())
		}
	}

	return nil
}

// announceChainedSubtrees announces the chain after the reorg's marks. A stored subtree is announced
// by a request that stores nothing; one that is not stored yet is sent to the store again, which
// announces it when it is.
func (stp *SubtreeProcessor) announceChainedSubtrees(ctx context.Context, caller string) error {
	for _, st := range stp.chainedSubtrees {
		req := NewSubtreeRequest{Subtree: st, AnnounceOnly: true}

		if stp.storedSet == nil || !stp.storedSet.Has(*st.RootHash()) {
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
// on marked its requests stale, and at the byte limit that dropped them; the restored subtrees are
// wanted again, so the ones left are unmarked and the ones dropped are sent again.
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
			SkipNotification:  true,
			ErrChan:           make(chan error, 1),
			OnStorageComplete: func() { stp.cleanupDeletedTxs(st) },
		})
	}
}
