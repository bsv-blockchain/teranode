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
// of the next block, and reports whether they differ from the ones set before. Block
// assembly mines a transaction without re-checking it, so one validated at a height under
// other rules (issue 4940: a side block below Genesis or Chronicle, or a transaction in
// flight while the tip crosses an activation height) must not reach the template. A
// network the script engine does not know clears the era, which admits everything.
func (stp *SubtreeProcessor) SetTxEra(nextHeight uint32) bool {
	var next *txEra

	if lo, hi, ok := txrules.EraBounds(stp.settings.ChainCfgParams, nextHeight); ok {
		next = &txEra{lo: lo, hi: hi}
	}

	prev := stp.txEra.Swap(next)

	return prev != nil && next != nil && *prev != *next
}

// ClearDroppedTxs forgets the transactions kept out of the template. Called when the
// template is rebuilt from the UTXO store, whose reload drops them again.
func (stp *SubtreeProcessor) ClearDroppedTxs() {
	stp.droppedMu.Lock()
	stp.droppedTxs = nil
	stp.droppedCount.Store(0)
	stp.droppedMu.Unlock()
}

// admitTx reports whether a transaction validated at height (0 when untagged) may enter the
// template: its height is under the next block's rules and none of its parents was kept
// out. A refused transaction is recorded so its descendants are refused too, which is why
// callers must offer transactions in queue order, parents before children.
func (stp *SubtreeProcessor) admitTx(hash chainhash.Hash, inpoints *subtreepkg.TxInpoints, height uint32) bool {
	if era := stp.txEra.Load(); era != nil && height != 0 && (height < era.lo || height > era.hi) {
		stp.recordDropped(hash)
		return false
	}

	if stp.droppedCount.Load() == 0 || inpoints == nil {
		return true
	}

	stp.droppedMu.Lock()
	defer stp.droppedMu.Unlock()

	for _, parent := range inpoints.ParentTxHashes {
		if _, dropped := stp.droppedTxs[parent]; dropped {
			stp.recordDroppedLocked(hash)
			return false
		}
	}

	return true
}

func (stp *SubtreeProcessor) recordDropped(hash chainhash.Hash) {
	stp.droppedMu.Lock()
	stp.recordDroppedLocked(hash)
	stp.droppedMu.Unlock()
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

// filterTxEra removes from the dequeued batches, in queue order, every transaction admitTx
// refuses. It runs before any of them is added to the tx map.
func (stp *SubtreeProcessor) filterTxEra(batches []*TxBatch) {
	for _, b := range batches {
		if b.validatedHeights == nil && stp.droppedCount.Load() == 0 {
			continue
		}

		kept := 0

		for i := range b.nodes {
			var height uint32
			if b.validatedHeights != nil {
				height = b.validatedHeights[i]
			}

			if !stp.admitTx(b.nodes[i].Hash, b.txInpoints[i], height) {
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

// filterUnminedTxEra returns the reloaded transactions admitTx accepts, tagged with the
// height they were created at (unmined_since), in the order given.
func (stp *SubtreeProcessor) filterUnminedTxEra(txs []*utxostore.UnminedTransaction) []*utxostore.UnminedTransaction {
	if stp.txEra.Load() == nil && stp.droppedCount.Load() == 0 {
		return txs
	}

	var kept []*utxostore.UnminedTransaction

	for i, tx := range txs {
		var height uint32
		if tx.UnminedSince > 0 {
			height = uint32(tx.UnminedSince) //nolint:gosec // a block height
		}

		admitted := stp.admitTx(tx.Hash, tx.TxInpoints, height)

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
