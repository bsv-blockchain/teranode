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
	stallFloor   time.Duration // the least time the stored count may stand still before a re-enqueue
	reenqueueGap time.Duration // the least time between two sends of one subtree

	firstSeen   map[chainhash.Hash]time.Time // when a chained subtree was first seen not stored
	lastSent    map[chainhash.Hash]time.Time // when a subtree was last sent again
	lastAdded   uint64
	lastAdvance time.Time
}

func newStoredPrefixWatch() storedPrefixWatch {
	return storedPrefixWatch{
		tickEvery:    storedPrefixTickEvery,
		stallFloor:   storedPrefixStallFloor,
		reenqueueGap: storedPrefixReenqueueGap,
		firstSeen:    make(map[chainhash.Hash]time.Time),
		lastSent:     make(map[chainhash.Hash]time.Time),
	}
}

// maintainStoredPrefix is the upkeep of the stored-prefix cut, run on a tick by the processor
// goroutine between handlers, so the chain it reads is whole and a rollback cannot restore a root it
// pruned. It runs on a timer, not on a worker's message: a worker never waits for the processor.
//
//   - It drops the roots of the set that are not chained, so the set cannot outgrow the chain.
//   - It exports the age of the oldest chained subtree that is not stored.
//   - It sends a chained subtree again from memory when its store ended failed for good, and the
//     first unstored one when the stored count has not advanced for max(3 x the median store time,
//     stallFloor) with no retry in flight. A subtree is sent again at most once per reenqueueGap.
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

	if added := set.Added(); added != w.lastAdded || first < 0 || w.lastAdvance.IsZero() {
		w.lastAdded, w.lastAdvance = added, now
	}

	for i, root := range roots {
		if set.HasFailed(root) {
			stp.sendChainedSubtreeAgain(i, root, now)
		}
	}

	if first >= 0 && set.Retrying() == 0 && now.Sub(w.lastAdvance) >= max(3*set.MedianStore(), w.stallFloor) {
		stp.sendChainedSubtreeAgain(first, roots[first], now)

		w.lastAdvance = now
	}
}

// sendChainedSubtreeAgain sends chained subtree i to the store without announcing it, unless it was
// sent again less than reenqueueGap ago. The send never blocks: a full store queue is exactly when
// the prefix is stuck, and the processor goroutine must not wait there; the next tick tries again.
func (stp *SubtreeProcessor) sendChainedSubtreeAgain(i int, root chainhash.Hash, now time.Time) {
	w := &stp.prefixWatch

	if last, ok := w.lastSent[root]; ok && now.Sub(last) < w.reenqueueGap {
		return
	}

	st := stp.chainedSubtrees[i]

	req := NewSubtreeRequest{
		Subtree:           st,
		ParentTxMap:       stp.currentTxMap,
		superseded:        stp.supersedeToken(),
		DeletedTxs:        stp.deletedTxs,
		SkipNotification:  true,
		ErrChan:           make(chan error, 1),
		OnStorageComplete: func() { stp.cleanupDeletedTxs(st) },
	}

	if stp.storeQueue != nil {
		if stp.storeQueue.entry(st) != nil {
			return // still queued or being stored
		}

		stp.storeQueue.Push(req)
	} else {
		select {
		case stp.newSubtreeChan <- req:
		default:
			return
		}
	}

	w.lastSent[root] = now

	stp.logger.Warnf("[SubtreeProcessor] chained subtree %d is not stored, sending it to the store again", i)
}
