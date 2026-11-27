package subtreeprocessor

import (
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// Defaults of the stored-prefix watch; tests set the fields of storedPrefixWatch instead.
const (
	storedPrefixTickEvery    = 5 * time.Second
	storedPrefixStallFloor   = 150 * time.Second
	storedPrefixReenqueueGap = 30 * time.Second
)

// storedPrefixWatch is the state of maintainStoredPrefix, owned by the processor goroutine.
type storedPrefixWatch struct {
	tickEvery    time.Duration
	stallFloor   time.Duration // how long the first unstored subtree may stay first before it is sent again
	reenqueueGap time.Duration // the least time between two sends of one subtree

	firstSeen map[chainhash.Hash]time.Time        // when a chained subtree was first seen not stored
	lastSent  map[chainhash.Hash]time.Time        // when a subtree was last sent again
	pending   map[chainhash.Hash]chan StoreResult // the send of a subtree that has not settled

	// stuck is the first unstored chained root, and since when it has been the first: the stall is
	// the hole's, not the set's, because later subtrees keep storing behind a hole.
	stuck      chainhash.Hash
	stuckSince time.Time
}

func newStoredPrefixWatch() storedPrefixWatch {
	return storedPrefixWatch{
		tickEvery:    storedPrefixTickEvery,
		stallFloor:   storedPrefixStallFloor,
		reenqueueGap: storedPrefixReenqueueGap,
		firstSeen:    make(map[chainhash.Hash]time.Time),
		lastSent:     make(map[chainhash.Hash]time.Time),
		pending:      make(map[chainhash.Hash]chan StoreResult),
	}
}

// maintainStoredPrefix is the upkeep of the stored-prefix cut, run on a tick by the processor
// goroutine between handlers, so the chain it reads is whole and a rollback cannot restore a root it
// pruned. It runs on a timer, not on a worker's message: a worker never waits for the processor.
//
//   - It drops the roots of the set that are not chained, so the set cannot outgrow the chain.
//   - It exports the age of the oldest chained subtree that is not stored.
//   - It sends a chained subtree again from memory when its store ended failed for good (one a
//     tick), and the first unstored one when it has been the first for stallFloor with no retry of
//     its own in flight. A stall send that was not made (queue full, send outstanding, inside
//     reenqueueGap) is tried again on the next tick. A fixed 150 s stands for max(3 x the median store time, 150 s): the median would need
//     a timer on the store path, and a store that takes 50 s is already past any sane setting.
//     A subtree is sent again at most once per reenqueueGap, and not while its last send is
//     outstanding: in an outage each send holds its bytes in the retry channel for about 100 s.
func (stp *SubtreeProcessor) maintainStoredPrefix(now time.Time) {
	set, w := stp.storedSet, &stp.prefixWatch

	roots := make([]chainhash.Hash, len(stp.chainedSubtrees))
	keep := make(map[chainhash.Hash]struct{}, len(roots))

	for i, st := range stp.chainedSubtrees {
		roots[i] = *st.RootHash()
		keep[roots[i]] = struct{}{}
	}

	set.Retain(keep)

	if stp.storeQueue != nil {
		stp.storeQueue.retainAnnounceFailed(keep)
	}

	if stp.storeQueue != nil {
		stp.runDroppedCleanups()
	}

	for root := range w.firstSeen {
		if _, ok := keep[root]; !ok {
			delete(w.firstSeen, root)
		}
	}

	for root := range w.lastSent {
		if _, ok := keep[root]; !ok {
			delete(w.lastSent, root)
		}
	}

	for root, result := range w.pending {
		select {
		case <-result:
			delete(w.pending, root)
		default:
			if _, ok := keep[root]; !ok {
				delete(w.pending, root) // left the chain: its send settles on its own
			}
		}
	}

	first, oldest := -1, now

	for i, root := range roots {
		if set.Has(root) {
			delete(w.firstSeen, root)
			continue
		}

		if first < 0 {
			first = i
		}

		seen, ok := w.firstSeen[root]
		if !ok {
			seen = now
			w.firstSeen[root] = seen
		}

		if seen.Before(oldest) {
			oldest = seen
		}
	}

	prometheusSubtreeProcessorStoredPrefixOldestUnstored.Set(now.Sub(oldest).Seconds())

	if first < 0 {
		w.stuckSince = time.Time{}
	} else if roots[first] != w.stuck || w.stuckSince.IsZero() {
		w.stuck, w.stuckSince = roots[first], now
	}

	for i, root := range roots {
		if set.HasFailed(root) && stp.sendChainedSubtreeAgain(i, root, now) {
			break // one a tick: in a long outage every send holds its bytes in the retry channel
		}
	}

	if first >= 0 && !set.Retrying(roots[first]) && now.Sub(w.stuckSince) >= w.stallFloor {
		if stp.sendChainedSubtreeAgain(first, roots[first], now) {
			w.stuckSince = now
		}
	}
}

// sendChainedSubtreeAgain sends chained subtree i to the store without announcing it, unless it was
// sent again less than reenqueueGap ago or its last send is outstanding, and reports whether it sent.
// The send never blocks: a full store queue is exactly when the prefix is stuck, and the processor
// goroutine must not wait there; the next tick tries again.
//
// OnStorageComplete stays unset: the request that failed or stalled may still read DeletedTxs for
// its meta build, and cleans them up itself when it completes.
func (stp *SubtreeProcessor) sendChainedSubtreeAgain(i int, root chainhash.Hash, now time.Time) bool {
	w := &stp.prefixWatch

	if _, outstanding := w.pending[root]; outstanding {
		return false
	}

	if last, ok := w.lastSent[root]; ok && now.Sub(last) < w.reenqueueGap {
		return false
	}

	st := stp.chainedSubtrees[i]
	result := make(chan StoreResult, 1)

	// No OnStorageComplete: the subtree's first request cleans up the deleted transactions.
	req := NewSubtreeRequest{
		Subtree:          st,
		ParentTxMap:      stp.currentTxMap,
		superseded:       stp.supersedeToken(),
		DeletedTxs:       stp.deletedTxs,
		SkipNotification: true,
		Result:           result,
		ErrChan:          make(chan error, 1),
	}

	if stp.storeQueue != nil {
		// A request a worker has is not recovered by waiting: a write with no deadline can hang.
		if stp.storeQueue.queued(st) {
			return false
		}

		// a request that was to announce and failed for good announces when this one stores
		req.SkipNotification = !stp.storeQueue.announceWasLost(st)

		stp.storeQueue.Push(req)
	} else {
		select {
		case stp.newSubtreeChan <- req:
		default:
			return false
		}
	}

	w.lastSent[root] = now
	w.pending[root] = result

	stp.logger.Warnf("[SubtreeProcessor] chained subtree %d is not stored, sending it to the store again", i)

	return true
}
