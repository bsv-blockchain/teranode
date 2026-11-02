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
// enter the template: its height is under the next block's rules and none of its parents
// was kept out. A refused transaction is recorded so its descendants are refused too,
// which is why callers offer transactions in queue order, parents before children. An
// admitted transaction that was refused before is forgotten, so its own descendants are
// admitted again. The caller holds droppedMu.
func (stp *SubtreeProcessor) admitTxLocked(hash chainhash.Hash, inpoints *subtreepkg.TxInpoints, height uint32) bool {
	if era := stp.txEra.Load(); era != nil && height != 0 && (height < era.lo || height > era.hi) {
		stp.recordDroppedLocked(hash)
		return false
	}

	if len(stp.droppedTxs) == 0 {
		return true
	}

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
		// One height per node, or none: anything else is untagged.
		if len(b.validatedHeights) != len(b.nodes) {
			b.validatedHeights = nil
		}

		if b.validatedHeights == nil && len(stp.droppedTxs) == 0 {
			continue
		}

		kept := 0

		for i := range b.nodes {
			var height uint32
			if b.validatedHeights != nil {
				height = b.validatedHeights[i]
			}

			if !stp.admitTxLocked(b.nodes[i].Hash, b.txInpoints[i], height) {
				continue
			}

			b.nodes[kept] = b.nodes[i]
			b.txInpoints[kept] = b.txInpoints[i]

			if b.validatedHeights != nil {
				b.validatedHeights[kept] = b.validatedHeights[i]
			}

			kept++
		}

		b.nodes = b.nodes[:kept]
		b.txInpoints = b.txInpoints[:kept]

		if b.validatedHeights != nil {
			b.validatedHeights = b.validatedHeights[:kept]
		}
	}
}

// FilterUnminedTxEra returns the reloaded transactions admitTxLocked accepts, in the order
// given. validatedHeights holds, per transaction, the height it was validated at (0 when
// unknown); block assembly derives it (see BlockAssembler.reloadValidatedHeights).
func (stp *SubtreeProcessor) FilterUnminedTxEra(txs []*utxostore.UnminedTransaction, validatedHeights []uint32) []*utxostore.UnminedTransaction {
	if stp.txEra.Load() == nil && stp.droppedCount.Load() == 0 {
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

		admitted := stp.admitTxLocked(tx.Hash, tx.TxInpoints, height)

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
func (stp *SubtreeProcessor) RestartDroppedTxs() (finish func(reloaded bool)) {
	stp.droppedMu.Lock()
	prev := stp.droppedTxs
	stp.droppedTxs = nil
	stp.droppedCount.Store(0)
	stp.droppedMu.Unlock()

	return func(reloaded bool) {
		if reloaded {
			return
		}

		stp.droppedMu.Lock()
		defer stp.droppedMu.Unlock()

		for hash := range prev {
			stp.recordDroppedLocked(hash)
		}
	}
}

// forgetMinedDroppedTxs forgets the dropped transactions a block mines. They are confirmed,
// so their descendants are validated against them under the next block's rules and must
// not stay out for the drop. A block made only of this node's own subtrees has no map, and
// mines nothing that was dropped: a dropped transaction never entered them.
func (stp *SubtreeProcessor) forgetMinedDroppedTxs(mined *SortedHashSet) {
	if mined == nil || stp.droppedCount.Load() == 0 {
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
}
