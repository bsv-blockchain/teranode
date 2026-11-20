package subtreeprocessor

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/pkg/txrules"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
)

// txEra is the inclusive range of block heights whose transactions are checked under the
// same consensus rules as the next block's (txrules.EraBounds).
type txEra struct {
	lo, hi uint32
}

// SetTxEra sets the rules the template is built under to those at nextHeight, the height
// of the next block. Block assembly mines a transaction without re-checking it, so one
// validated at a height under other rules (a side block below Genesis or Chronicle, or a
// transaction in flight while the tip crosses an activation height) must
// not reach the template. A network the script engine does not know clears the era, which
// admits everything.
func (stp *SubtreeProcessor) SetTxEra(nextHeight uint32) {
	var next *txEra

	if lo, hi, ok := txrules.EraBounds(stp.settings.ChainCfgParams, nextHeight); ok {
		next = &txEra{lo: lo, hi: hi}
	}

	stp.txEra.Store(next)
}

// TxEra returns the range of validated heights the template admits, ok false when it
// admits every height.
func (stp *SubtreeProcessor) TxEra() (lo, hi uint32, ok bool) {
	era := stp.txEra.Load()
	if era == nil {
		return 0, 0, false
	}

	return era.lo, era.hi, true
}

// admitTxLocked reports whether a transaction validated at height (0 when untagged) may
// enter the template: its height is under the next block's rules, its nLockTime (lockTime,
// 0 when it cannot hold the transaction back) does not hold it back at the candidate, and
// none of its parents was kept out. The caller holds droppedMu, and offers transactions in
// queue order, parents before children, so a refusal reaches every descendant.
//
// A transaction validated under other rules is dropped, recorded so its descendants are
// dropped too. A descendant dropped for its parent alone waits on that parent, and comes back
// if a block mines it (forgetMinedDroppedTxs). One its lock time holds back is held, with its descendants, and offered again
// when the candidate moves (tx_finality.go): it was validated for another candidate, by a
// validator ahead of block assembly or before a reorg, and becomes final as the chain grows.
// An admitted transaction that was dropped before is forgotten, so its own descendants are
// admitted again, and those dropped for it are held to be offered again.
func (stp *SubtreeProcessor) admitTxLocked(node subtreepkg.Node, inpoints *subtreepkg.TxInpoints, height, lockTime uint32) bool {
	hash := node.Hash

	if era := stp.txEra.Load(); era != nil && height != 0 && (height < era.lo || height > era.hi) {
		stp.releaseLockHeldLocked(hash)
		stp.recordDroppedLocked(hash)

		return false
	}

	if inpoints != nil && (len(stp.droppedTxs) != 0 || len(stp.lockHeldSet) != 0) {
		for _, parent := range inpoints.ParentTxHashes {
			if _, dropped := stp.droppedTxs[parent]; dropped {
				stp.releaseLockHeldLocked(hash)
				stp.recordDroppedLocked(hash)
				stp.waitOnDroppedParentLocked(parent, lockHeldTx{node: node, inpoints: inpoints, height: height, lockTime: lockTime})

				return false
			}
		}

		for _, parent := range inpoints.ParentTxHashes {
			if _, held := stp.lockHeldSet[parent]; held {
				stp.holdForLockTimeLocked(node, inpoints, height, lockTime)
				return false
			}
		}
	}

	if lockTime != 0 {
		if c := stp.freshTxCandidate(); c != nil && !stp.finalAtCandidate(lockTime, c) {
			stp.holdForLockTimeLocked(node, inpoints, height, lockTime)
			return false
		}
	}

	if _, dropped := stp.droppedTxs[hash]; dropped {
		delete(stp.droppedTxs, hash)
		stp.droppedCount.Add(-1)

		if waiting, ok := stp.droppedWaiters[hash]; ok {
			delete(stp.droppedWaiters, hash)
			stp.releaseDroppedWaitersLocked(waiting)
		}
	}

	stp.releaseLockHeldLocked(hash)
	stp.trackLockTimeLocked(hash, lockTime)

	return true
}

func (stp *SubtreeProcessor) recordDroppedLocked(hash chainhash.Hash) {
	if stp.droppedTxs == nil {
		stp.droppedTxs = make(map[chainhash.Hash]struct{})
	}

	if _, ok := stp.droppedTxs[hash]; !ok {
		stp.droppedTxs[hash] = struct{}{}
		stp.droppedCount.Add(1)
	}

	prometheusSubtreeProcessorTxEraDropped.Inc()
}

// waitOnDroppedParentLocked keeps tx, dropped because parent was, to bring back if a block
// mines parent. The caller holds droppedMu.
func (stp *SubtreeProcessor) waitOnDroppedParentLocked(parent chainhash.Hash, tx lockHeldTx) {
	if stp.droppedWaiters == nil {
		stp.droppedWaiters = make(map[chainhash.Hash][]lockHeldTx)
	}

	stp.droppedWaiters[parent] = append(stp.droppedWaiters[parent], tx)
}

// filterTxEra removes from the dequeued batches, in queue order, every transaction
// admitTxLocked refuses. It runs before any of them is added to the tx map.
func (stp *SubtreeProcessor) filterTxEra(batches []*TxBatch) {
	stp.droppedMu.Lock()
	defer stp.droppedMu.Unlock()

	for _, b := range batches {
		// One height and one lock time per node, or none: anything else is untagged.
		if len(b.validatedHeights) != len(b.nodes) {
			b.validatedHeights = nil
		}

		if len(b.lockTimes) != len(b.nodes) {
			b.lockTimes = nil
		}

		if b.validatedHeights == nil && b.lockTimes == nil && len(stp.droppedTxs) == 0 && len(stp.lockHeldSet) == 0 {
			continue
		}

		kept := 0

		for i := range b.nodes {
			var height, lockTime uint32
			if b.validatedHeights != nil {
				height = b.validatedHeights[i]
			}

			if b.lockTimes != nil {
				lockTime = b.lockTimes[i]
			}

			if !stp.admitTxLocked(b.nodes[i], b.txInpoints[i], height, lockTime) {
				continue
			}

			b.nodes[kept] = b.nodes[i]
			b.txInpoints[kept] = b.txInpoints[i]

			if b.validatedHeights != nil {
				b.validatedHeights[kept] = b.validatedHeights[i]
			}

			if b.lockTimes != nil {
				b.lockTimes[kept] = b.lockTimes[i]
			}

			kept++
		}

		b.nodes = b.nodes[:kept]
		b.txInpoints = b.txInpoints[:kept]

		if b.validatedHeights != nil {
			b.validatedHeights = b.validatedHeights[:kept]
		}

		if b.lockTimes != nil {
			b.lockTimes = b.lockTimes[:kept]
		}
	}
}

// FilterUnminedTxEra returns the reloaded transactions admitTxLocked accepts, in the order
// given. validatedHeights holds, per transaction, the height it was validated at (0 when
// unknown); block assembly derives it (see BlockAssembler.reloadValidatedHeights). The lock
// time is the store's nLockTime: the store does not keep the input sequences, so a
// transaction whose sequences are all final but whose nLockTime is not yet reached is kept
// out too.
func (stp *SubtreeProcessor) FilterUnminedTxEra(txs []*utxostore.UnminedTransaction, validatedHeights []uint32) []*utxostore.UnminedTransaction {
	if stp.txEra.Load() == nil && stp.droppedCount.Load() == 0 && stp.lockHeldCount.Load() == 0 && stp.txCandidate.Load() == nil {
		return txs
	}

	stp.droppedMu.Lock()
	defer stp.droppedMu.Unlock()

	var kept []*utxostore.UnminedTransaction

	for i, tx := range txs {
		var height uint32
		if i < len(validatedHeights) {
			height = validatedHeights[i]
		}

		admitted := stp.admitTxLocked(*tx.Node, tx.TxInpoints, height, tx.LockTime)

		switch {
		case admitted && kept != nil:
			kept = append(kept, tx)
		case !admitted && kept == nil:
			kept = make([]*utxostore.UnminedTransaction, i, len(txs))
			copy(kept, txs[:i])
		}
	}

	if kept == nil {
		return txs
	}

	return kept
}

// RestartDroppedTxs starts a reload's own record of dropped transactions. A reload
// re-derives every drop from the UTXO store, which no longer lists a dropped parent a block
// has mined since, so its records replace the old ones. The returned finish keeps the new
// records when the reload succeeded and puts the old ones back when it did not, so a
// failed reload cannot let a queued child of a dropped parent in. The processor does not
// dequeue while a reset reloads.
//
// The tracked lock times and the descendants waiting on a dropped parent are restarted the
// same way: the reload brings in, or drops, every one of them again.
func (stp *SubtreeProcessor) RestartDroppedTxs() (finish func(reloaded bool)) {
	stp.droppedMu.Lock()
	prev := stp.droppedTxs
	prevWaiters := stp.droppedWaiters
	prevLocked := stp.lockedTxs
	prevHeld := stp.lockHeldTxs
	prevHeldSet := stp.lockHeldSet
	stp.droppedTxs = nil
	stp.droppedCount.Store(0)
	stp.droppedWaiters = nil
	stp.lockedTxs = nil
	stp.lockedCount.Store(0)
	stp.clearLockHeldLocked()
	stp.droppedMu.Unlock()

	return func(reloaded bool) {
		if reloaded {
			return
		}

		stp.droppedMu.Lock()
		defer stp.droppedMu.Unlock()

		for hash, lockTime := range prevLocked {
			stp.trackLockTimeLocked(hash, lockTime)
		}

		for _, h := range prevHeld {
			stp.holdForLockTimeLocked(h.node, h.inpoints, h.height, h.lockTime)
		}

		// Offered ones are in the queue: they stay held until admitTxLocked decides them.
		for hash, inList := range prevHeldSet {
			if _, ok := stp.lockHeldSet[hash]; !ok && !inList {
				if stp.lockHeldSet == nil {
					stp.lockHeldSet = make(map[chainhash.Hash]bool)
				}

				stp.lockHeldSet[hash] = false
			}
		}

		stp.lockHeldCount.Store(int64(len(stp.lockHeldSet)))

		// Not through recordDroppedLocked: these drops were counted when they happened.
		if stp.droppedTxs == nil {
			stp.droppedTxs = make(map[chainhash.Hash]struct{}, len(prev))
		}

		for hash := range prev {
			if _, ok := stp.droppedTxs[hash]; !ok {
				stp.droppedTxs[hash] = struct{}{}
				stp.droppedCount.Add(1)
			}
		}

		for parent, waiters := range prevWaiters {
			for _, w := range waiters {
				stp.waitOnDroppedParentLocked(parent, w)
			}
		}
	}
}

// forgetMinedDroppedTxs forgets the dropped transactions a block mines. They are confirmed,
// so their descendants are validated against them under the next block's rules and must
// not stay out for the drop. A block made only of this node's own subtrees has no map, and
// mines nothing that was dropped: a dropped transaction never entered them.
//
// The descendants dropped for a parent it mines come back: they are held, with their own
// waiting descendants, for OfferLockHeldTxs to check against the store and queue again.
//
// The tracked lock times of the transactions it mines are forgotten too, since they left the
// template, and so are the held ones it mines, which must not be offered again.
func (stp *SubtreeProcessor) forgetMinedDroppedTxs(mined *SortedHashSet) {
	if mined == nil || (stp.droppedCount.Load() == 0 && stp.lockedCount.Load() == 0 && stp.lockHeldCount.Load() == 0) {
		return
	}

	stp.droppedMu.Lock()
	defer stp.droppedMu.Unlock()

	var (
		waiting  []lockHeldTx
		minedAny bool
	)

	for hash := range stp.droppedTxs {
		if mined.Exists(hash) {
			delete(stp.droppedTxs, hash)
			stp.droppedCount.Add(-1)

			minedAny = true
			waiting = append(waiting, stp.droppedWaiters[hash]...)
			delete(stp.droppedWaiters, hash)
		}
	}

	if waiting != nil {
		stp.releaseDroppedWaitersLocked(waiting)
	}

	if minedAny {
		stp.pruneDroppedWaitersLocked()
	}

	for hash := range stp.lockedTxs {
		if mined.Exists(hash) {
			delete(stp.lockedTxs, hash)
			stp.lockedCount.Add(-1)
		}
	}

	if len(stp.lockHeldSet) != 0 {
		kept := stp.lockHeldTxs[:0]

		for _, h := range stp.lockHeldTxs {
			if !mined.Exists(h.node.Hash) {
				kept = append(kept, h)
			}
		}

		clear(stp.lockHeldTxs[len(kept):])
		stp.lockHeldTxs = kept

		for hash := range stp.lockHeldSet {
			if mined.Exists(hash) {
				delete(stp.lockHeldSet, hash)
			}
		}

		stp.lockHeldCount.Store(int64(len(stp.lockHeldSet)))
	}
}

// releaseDroppedWaitersLocked holds the transactions that waited on a dropped parent that has
// left the dropped set (a block mined it, or it was admitted), and those that waited on them,
// breadth first. One no longer dropped (a block mined it too, or it was admitted since) is
// skipped. Each one leaves the dropped set as it is held, so a transaction that spends it is
// held behind it rather than dropped, and so is a released one that comes before a released
// parent: it waits for the next offer. Offered, one that is in a conflict, or has another
// parent still kept out, is dropped again. The caller holds droppedMu.
func (stp *SubtreeProcessor) releaseDroppedWaitersLocked(waiting []lockHeldTx) {
	for i := 0; i < len(waiting); i++ {
		w := waiting[i]

		if _, dropped := stp.droppedTxs[w.node.Hash]; !dropped {
			continue
		}

		delete(stp.droppedTxs, w.node.Hash)
		stp.droppedCount.Add(-1)

		waiting = append(waiting, stp.droppedWaiters[w.node.Hash]...)
		delete(stp.droppedWaiters, w.node.Hash)

		stp.holdForLockTimeLocked(w.node, w.inpoints, w.height, w.lockTime)
	}
}

// pruneDroppedWaitersLocked forgets the waiting records of transactions that left the dropped
// set while they waited: a block mined them. The caller holds droppedMu.
func (stp *SubtreeProcessor) pruneDroppedWaitersLocked() {
	for parent, waiters := range stp.droppedWaiters {
		kept := waiters[:0]

		for _, w := range waiters {
			if _, dropped := stp.droppedTxs[w.node.Hash]; dropped {
				kept = append(kept, w)
			}
		}

		clear(waiters[len(kept):])

		if len(kept) == 0 {
			delete(stp.droppedWaiters, parent)
		} else {
			stp.droppedWaiters[parent] = kept
		}
	}
}

// keepPreviousNodeFunc returns the check a moveBack applies to the template it rebuilds around
// the moved-back block: a node one of whose parents was kept out is kept out too, recorded so
// its own descendants follow, and taken out of the tx map. Only a drop from the moved-back
// block can orphan a template node, since every earlier drop already kept its descendants out
// on admission, so the check runs only when droppedFromBlock is set.
func (stp *SubtreeProcessor) keepPreviousNodeFunc(droppedFromBlock bool) func(node subtreepkg.Node) bool {
	if !droppedFromBlock {
		return func(subtreepkg.Node) bool { return true }
	}

	return func(node subtreepkg.Node) bool {
		hash := node.Hash

		inpoints, ok := stp.currentTxMap.Get(hash)
		if !ok || inpoints == nil {
			return true
		}

		stp.droppedMu.Lock()
		defer stp.droppedMu.Unlock()

		for _, parent := range inpoints.ParentTxHashes {
			if _, dropped := stp.droppedTxs[parent]; dropped {
				stp.recordDroppedLocked(hash)
				stp.currentTxMap.Delete(hash)

				// Untagged: it was admitted under the template's rules, and a rules change
				// reloads while anything is kept out or held (NothingToReload). A copy, so the
				// record does not keep the moved-back block's inpoints alive.
				kept := *inpoints
				stp.waitOnDroppedParentLocked(parent, lockHeldTx{node: node, inpoints: &kept, lockTime: stp.lockedTxs[hash]})

				return false
			}
		}

		return true
	}
}
