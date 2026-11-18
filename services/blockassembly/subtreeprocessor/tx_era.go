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
// validated at a height under other rules (issue 4940: a side block below Genesis or
// Chronicle, or a transaction in flight while the tip crosses an activation height) must
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
// none of its parents was kept out. A refused transaction is recorded so its descendants are
// refused too, which is why callers offer transactions in queue order, parents before
// children. An admitted transaction that was refused before is forgotten, so its own
// descendants are admitted again. The caller holds droppedMu.
//
// From the queue (fromQueue) the lock is checked only when the transaction was validated for
// the candidate's height or an earlier one, or the height is unknown. One validated for a
// later height comes from a validator ahead of block assembly, and is final once block
// assembly catches up. A reload runs at the chain tip, where a height above the candidate's is
// a reorg to a shorter chain, so it checks every lock.
func (stp *SubtreeProcessor) admitTxLocked(hash chainhash.Hash, inpoints *subtreepkg.TxInpoints, height, lockTime uint32, fromQueue bool) bool {
	if era := stp.txEra.Load(); era != nil && height != 0 && (height < era.lo || height > era.hi) {
		stp.recordDroppedLocked(hash)
		return false
	}

	if lockTime != 0 {
		if c := stp.freshTxCandidate(); c != nil && (!fromQueue || height == 0 || height <= c.height) && !stp.finalAtCandidate(lockTime, c) {
			stp.recordDroppedLocked(hash)
			return false
		}
	}

	if len(stp.droppedTxs) != 0 {
		if inpoints != nil {
			for _, parent := range inpoints.ParentTxHashes {
				if _, dropped := stp.droppedTxs[parent]; dropped {
					stp.recordDroppedLocked(hash)
					return false
				}
			}
		}

		if _, dropped := stp.droppedTxs[hash]; dropped {
			delete(stp.droppedTxs, hash)
			stp.droppedCount.Add(-1)
		}
	}

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

		if b.validatedHeights == nil && b.lockTimes == nil && len(stp.droppedTxs) == 0 {
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

			if !stp.admitTxLocked(b.nodes[i].Hash, b.txInpoints[i], height, lockTime, true) {
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
	if stp.txEra.Load() == nil && stp.droppedCount.Load() == 0 && stp.txCandidate.Load() == nil {
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

		admitted := stp.admitTxLocked(tx.Hash, tx.TxInpoints, height, tx.LockTime, false)

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
// The tracked lock times are restarted the same way: the reload tracks every transaction it
// brings in again.
func (stp *SubtreeProcessor) RestartDroppedTxs() (finish func(reloaded bool)) {
	stp.droppedMu.Lock()
	prev := stp.droppedTxs
	prevLocked := stp.lockedTxs
	stp.droppedTxs = nil
	stp.droppedCount.Store(0)
	stp.lockedTxs = nil
	stp.lockedCount.Store(0)
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
	}
}

// forgetMinedDroppedTxs forgets the dropped transactions a block mines. They are confirmed,
// so their descendants are validated against them under the next block's rules and must
// not stay out for the drop. A block made only of this node's own subtrees has no map, and
// mines nothing that was dropped: a dropped transaction never entered them.
//
// The tracked lock times of the transactions it mines are forgotten too: they left the
// template.
func (stp *SubtreeProcessor) forgetMinedDroppedTxs(mined *SortedHashSet) {
	if mined == nil || (stp.droppedCount.Load() == 0 && stp.lockedCount.Load() == 0) {
		return
	}

	stp.droppedMu.Lock()
	defer stp.droppedMu.Unlock()

	for hash := range stp.droppedTxs {
		if mined.Exists(hash) {
			delete(stp.droppedTxs, hash)
			stp.droppedCount.Add(-1)
		}
	}

	for hash := range stp.lockedTxs {
		if mined.Exists(hash) {
			delete(stp.lockedTxs, hash)
			stp.lockedCount.Add(-1)
		}
	}
}

// keepPreviousNodeFunc returns the check a moveBack applies to the template it rebuilds around
// the moved-back block: a node one of whose parents was kept out is kept out too, recorded so
// its own descendants follow, and taken out of the tx map. Only a drop from the moved-back
// block can orphan a template node, since every earlier drop already kept its descendants out
// on admission, so the check runs only when droppedFromBlock is set.
func (stp *SubtreeProcessor) keepPreviousNodeFunc(droppedFromBlock bool) func(hash chainhash.Hash) bool {
	if !droppedFromBlock {
		return func(chainhash.Hash) bool { return true }
	}

	return func(hash chainhash.Hash) bool {
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

				return false
			}
		}

		return true
	}
}
