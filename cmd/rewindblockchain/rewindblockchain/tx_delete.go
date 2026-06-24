package rewindblockchain

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
)

// isNotFound returns true if err is a NotFound-family error that repair
// tooling should silently tolerate. SQL raises ErrNotFound for missing
// output rows; Aerospike raises ErrTxNotFound for missing records.
func isNotFound(err error) bool {
	return errors.Is(err, errors.ErrTxNotFound) || errors.Is(err, errors.ErrNotFound)
}

// removalCollector accumulates cross-record cleanup work so we can flush via
// Aerospike batch APIs. Callers add (parent, child) tuples and (tx, blockIDs)
// trims across many txs in a phase, then invoke flushCollector once per batch.
type removalCollector struct {
	conflictingChildren []utxo.ConflictingChildRemoval
	blockIDTrims        []utxo.BlockIDsRemoval
}

func newRemovalCollector() *removalCollector {
	return &removalCollector{
		conflictingChildren: make([]utxo.ConflictingChildRemoval, 0, 64),
		blockIDTrims:        make([]utxo.BlockIDsRemoval, 0, 16),
	}
}

// flushCollector dispatches the accumulated batch writes and resets the
// collector for the next batch.
func (e *env) flushCollector(ctx context.Context, c *removalCollector) error {
	if len(c.blockIDTrims) > 0 {
		if err := e.utxoStore.RemoveBlockIDs(ctx, c.blockIDTrims); err != nil {
			return errors.NewStorageError("failed to flush block id trims: %w", err)
		}
		e.stats.TxsBlockIDsTrimmed += len(c.blockIDTrims)
	}

	if len(c.conflictingChildren) > 0 {
		if err := e.utxoStore.RemoveFromConflictingChildren(ctx, c.conflictingChildren); err != nil {
			return errors.NewStorageError("failed to flush conflicting-child removals: %w", err)
		}
		e.stats.ParentConflictsCleaned += len(c.conflictingChildren)
	}

	c.blockIDTrims = c.blockIDTrims[:0]
	c.conflictingChildren = c.conflictingChildren[:0]
	return nil
}

// deleteTxWithParents implements the full helper described in the plan:
//   - Fetches tx + metadata.
//   - If the tx is still referenced by a surviving block, trims only the
//     deleted block IDs (queued onto the collector).
//   - Otherwise: builds Spend structs from inputs, Unspends, Deletes, and
//     queues parent conflictingChildren cleanups for the collector.
//
// Returns (acted, err) where `acted` is true iff the tx was deleted or
// trimmed.
func (e *env) deleteTxWithParents(ctx context.Context, txHash *chainhash.Hash, pf *preflightResult, c *removalCollector) (bool, error) {
	meta, err := e.utxoStore.Get(ctx, txHash,
		fields.Tx,
		fields.BlockIDs,
		fields.Conflicting,
	)
	if err != nil {
		if isNotFound(err) {
			// Already gone — idempotent.
			return false, nil
		}
		return false, errors.NewStorageError("failed to Get tx %s: %w", txHash.String(), err)
	}

	// Partition BlockIDs into surviving vs deleted.
	var (
		deletedHits []uint32
		survivors   []uint32
	)
	for _, id := range meta.BlockIDs {
		if _, ok := pf.deleteByID[id]; ok {
			deletedHits = append(deletedHits, id)
		} else {
			survivors = append(survivors, id)
		}
	}

	if len(survivors) > 0 {
		// Tx is still on at least one surviving block. Trim BlockIDs rather
		// than deleting.
		if len(deletedHits) > 0 {
			c.blockIDTrims = append(c.blockIDTrims, utxo.BlockIDsRemoval{
				TxHash:   txHash,
				BlockIDs: deletedHits,
			})
		}
		return true, nil
	}

	// Proceed with full deletion.
	if meta.Tx == nil {
		return false, errors.NewProcessingError("tx %s: Get returned nil Tx — cannot unspend inputs", txHash.String())
	}

	if err = e.utxoStore.PreviousOutputsDecorate(ctx, meta.Tx); err != nil {
		return false, errors.NewStorageError("PreviousOutputsDecorate %s: %w", txHash.String(), err)
	}

	spends := make([]*utxo.Spend, 0, len(meta.Tx.Inputs))
	for i, input := range meta.Tx.Inputs {
		utxoHash, hErr := util.UTXOHashFromInput(input)
		if hErr != nil {
			return false, errors.NewProcessingError("UTXOHashFromInput tx %s idx %d: %w", txHash.String(), i, hErr)
		}
		spends = append(spends, &utxo.Spend{
			TxID:         input.PreviousTxIDChainHash(),
			Vout:         input.PreviousTxOutIndex,
			UTXOHash:     utxoHash,
			SpendingData: spendpkg.NewSpendingData(meta.Tx.TxIDChainHash(), i),
		})
	}

	if len(spends) > 0 {
		if err = e.utxoStore.Unspend(ctx, spends); err != nil {
			// Tolerate NotFound: parent tx may have been DAH-reaped, already
			// deleted, or have had its outputs removed. SQL raises ErrNotFound
			// ("output X:Y not found") while Aerospike raises ErrTxNotFound.
			//
			// NOTE: tolerance is batch-wide. Aerospike short-circuits on the
			// first failing input (subsequent inputs are not unspent). SQL is
			// transactional and rolls back the entire batch on any failure —
			// no inputs are unspent. Acceptable here because the tolerated
			// outcome is "parent already gone": there is no UTXO row to
			// un-mark, so the no-op is correct. If a future store gains
			// per-input partial-success semantics, switch to single-input
			// calls and aggregate errors.
			if !isNotFound(err) {
				return false, errors.NewStorageError("Unspend tx %s: %w", txHash.String(), err)
			}
		}
	}

	if err = e.utxoStore.Delete(ctx, txHash); err != nil {
		if !isNotFound(err) {
			return false, errors.NewStorageError("Delete tx %s: %w", txHash.String(), err)
		}
	}

	// Parent conflictingChildren cleanup: only needed when the deleted tx was
	// marked conflicting (otherwise it was never appended to any parent's
	// list in the first place). We cannot cheaply tell which parents are
	// themselves slated for deletion; RemoveFromConflictingChildren is
	// idempotent on Aerospike/SQL, so we queue all of them and let the
	// batch API silently no-op on missing records.
	if meta.Conflicting {
		seen := make(map[chainhash.Hash]struct{}, len(meta.Tx.Inputs))
		for _, input := range meta.Tx.Inputs {
			parentHash := input.PreviousTxIDChainHash()
			if parentHash == nil {
				continue
			}
			if _, dup := seen[*parentHash]; dup {
				continue
			}
			seen[*parentHash] = struct{}{}

			c.conflictingChildren = append(c.conflictingChildren, utxo.ConflictingChildRemoval{
				ParentHash: parentHash,
				ChildHash:  txHash,
			})
		}
	}

	e.stats.TxsDeleted++
	return true, nil
}
