package teraslab

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	teraslab "github.com/icellan/teraslab/client/go"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
)

// SetConflicting marks transactions as conflicting or not conflicting and returns the affected spends.
func (s *Store) SetConflicting(ctx context.Context, txHashes []chainhash.Hash, value bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	if len(txHashes) == 0 {
		return nil, nil, nil
	}

	txids := make([]teraslab.TxID, len(txHashes))
	for i, h := range txHashes {
		txids[i] = hashToTxID(&h)
	}

	params := teraslab.SetConflictingParams{
		Value:                value,
		CurrentBlockHeight:   s.blockHeight.Load(),
		BlockHeightRetention: s.settings.GetUtxoStoreBlockHeightRetention(),
	}

	if _, err := s.client.SetConflictingBatch(ctx, params, txids); err != nil {
		if pe, ok := err.(*teraslab.PartialError); ok {
			// Build spend results for failed items
			var spendErrors []*utxo.Spend
			for _, itemErr := range pe.Errors {
				if int(itemErr.ItemIndex) < len(txHashes) {
					h := txHashes[itemErr.ItemIndex]
					spendErrors = append(spendErrors, &utxo.Spend{
						TxID: &h,
						Err:  mapErrorCode(itemErr.Code),
					})
				}
			}
			if len(spendErrors) > 0 {
				return spendErrors, nil, errors.ErrUtxoError
			}
		}
		return nil, nil, err
	}

	// Reconstruct the return contract that callers in the conflict-resolution
	// flow consume (process_conflicting.go uses both): the affected parent
	// spends (this tx's inputs) and the hashes of txs spending this tx's
	// outputs. Mirrors the Aerospike backend. The parent conflicting-children
	// linkage itself is applied server-side during SetConflictingBatch above.
	affectedParentSpends := make([]*utxo.Spend, 0)
	spendingTxHashes := make([]chainhash.Hash, 0)

	for i := range txHashes {
		txHash := txHashes[i]

		data, err := s.Get(ctx, &txHash, fields.Tx)
		if err != nil {
			return nil, nil, err
		}
		if data == nil || data.Tx == nil {
			continue
		}
		tx := data.Tx

		// This tx's inputs are the parent UTXOs affected by the conflict.
		for vin, input := range tx.Inputs {
			utxoHash, err := util.UTXOHashFromInput(input)
			if err != nil {
				return nil, nil, err
			}

			affectedParentSpends = append(affectedParentSpends, &utxo.Spend{
				TxID:         input.PreviousTxIDChainHash(),
				Vout:         input.PreviousTxOutIndex,
				UTXOHash:     utxoHash,
				SpendingData: spend.NewSpendingData(tx.TxIDChainHash(), vin),
			})
		}

		// Any tx spending this tx's outputs is a counter/spending child.
		for vout, output := range tx.Outputs {
			// Data outputs (OP_RETURN) are stored with a zero UTXO hash and are
			// never spendable, so they have no spending child. Skip them, mirroring
			// Create (txToCreateItem). Recomputing a hash here would mismatch the
			// stored zero hash and make GetSpend return ErrTxNotFound, aborting
			// SetConflicting for any tx with a data output.
			if output.LockingScript.IsData() {
				continue
			}

			vout32 := uint32(vout) //nolint:gosec // output count is bounded well below u32

			utxoHash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), output, vout32)
			if err != nil {
				return nil, nil, err
			}

			resp, err := s.GetSpend(ctx, &utxo.Spend{
				TxID:     tx.TxIDChainHash(),
				Vout:     vout32,
				UTXOHash: utxoHash,
			})
			if err != nil {
				return nil, nil, err
			}

			if resp != nil && resp.SpendingData != nil && resp.SpendingData.TxID != nil {
				spendingTxHashes = append(spendingTxHashes, *resp.SpendingData.TxID)
			}
		}
	}

	return affectedParentSpends, spendingTxHashes, nil
}

// RemoveFromConflictingChildren removes each (parent, child) pair from the
// parent's conflicting-children list — the inverse of the link established on
// Create/SetConflicting. Idempotent: a parent that is absent or a child not
// present is tolerated server-side (no-op). Each pair is routed to the node
// owning the parent record.
func (s *Store) RemoveFromConflictingChildren(ctx context.Context, removals []utxo.ConflictingChildRemoval) error {
	if len(removals) == 0 {
		return nil
	}

	pairs := make([]teraslab.ConflictingChildPair, 0, len(removals))
	for _, r := range removals {
		if r.ParentHash == nil || r.ChildHash == nil {
			return errors.NewInvalidArgumentError(
				"RemoveFromConflictingChildren: parent and child hashes must be non-nil")
		}
		pairs = append(pairs, teraslab.ConflictingChildPair{
			Parent: hashToTxID(r.ParentHash),
			Child:  hashToTxID(r.ChildHash),
		})
	}

	_, err := s.client.RemoveConflictingChildBatch(ctx, pairs)
	if err != nil {
		if pe, ok := err.(*teraslab.PartialError); ok {
			// The server already no-ops a missing parent / absent child, so a
			// per-item TxNotFound (parent absent on this node) is benign — the
			// removal is, by definition, satisfied. Any other per-item code is
			// a real failure.
			for i := range pe.Errors {
				if pe.Errors[i].Code != teraslab.ErrCodeTxNotFound {
					return errors.NewStorageError("RemoveFromConflictingChildren failed", err)
				}
			}
			return nil
		}
		return err
	}

	return nil
}

// GetCounterConflicting returns the counter-conflicting transactions for a given
// transaction hash — the sibling txs spending the same parent UTXOs as `txHash`.
// Delegates to the shared store-agnostic helper, exactly as the Aerospike
// backend does. (The previous hand-rolled version returned this record's own
// conflicting-children — the wrong category of hashes.)
func (s *Store) GetCounterConflicting(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	return utxo.GetCounterConflictingTxHashes(ctx, s, txHash)
}

// GetConflictingChildren returns the full set of conflicting descendant txs of
// `txHash` (recursive over conflicting-children + spending data, root excluded).
// Delegates to the shared store-agnostic helper, exactly as the Aerospike
// backend does. (The previous hand-rolled version returned only the direct
// children of a single record.)
func (s *Store) GetConflictingChildren(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	return utxo.GetConflictingChildren(ctx, s, txHash)
}
