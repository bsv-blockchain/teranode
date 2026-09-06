package teraslab

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	teraslab "github.com/icellan/teraslab/client/go"
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

	// Fetch every tx's record in a single batched read instead of one Get per tx
	// (the previous N+1 fan-out). FieldUtxoSlots is included so each output's
	// spend state — the spending children — comes from this same batch read
	// instead of a GetSpend RPC per output (the previous N*M fan-out). The records
	// are index-aligned with txids (same contract BatchDecorate relies on). A
	// missing record is a hard error here exactly as the per-tx Get path was.
	mask := buildFieldMask([]fields.FieldName{fields.Tx}) | teraslab.FieldUtxoSlots
	records, err := s.client.GetRecordBatch(ctx, mask, txids)
	if err != nil {
		if _, ok := err.(*teraslab.NotFoundError); ok {
			return nil, nil, errors.ErrTxNotFound
		}
		return nil, nil, err
	}

	for i := range txHashes {
		if i >= len(records) || !records[i].Found {
			return nil, nil, errors.ErrTxNotFound
		}

		data, err := recordToMetaData(records[i])
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

		// Any tx spending this tx's outputs is a counter/spending child. The
		// spending data was read from the record's slots into data.SpendingDatas
		// (FieldUtxoSlots above) — one batched read for all outputs, not a GetSpend
		// RPC per output. A non-spendable data output (zero stored hash) is never
		// spent, so its slot carries no spending data and is naturally skipped — no
		// era-dependent IsData() guess, which would disagree with Create's
		// ShouldStoreOutputAsUTXO storability rule.
		for _, sd := range data.SpendingDatas {
			if sd != nil && sd.TxID != nil {
				spendingTxHashes = append(spendingTxHashes, *sd.TxID)
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
	// Counter-conflict lookup is intentionally unbudgeted (maxNodes 0), matching
	// the Aerospike/SQL backends: the conflict-demotion path always walks to
	// completion.
	return utxo.GetCounterConflictingTxHashes(ctx, s, txHash, 0)
}

// GetConflictingChildren returns the full set of conflicting descendant txs of
// `txHash` (recursive over conflicting-children + spending data, root excluded).
// Delegates to the shared store-agnostic helper, exactly as the Aerospike
// backend does. (The previous hand-rolled version returned only the direct
// children of a single record.)
func (s *Store) GetConflictingChildren(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	// The conflicting-children BFS is budgeted by the configured node cap
	// (issue 1391 fail-closed guard), matching the Aerospike/SQL backends.
	return utxo.GetConflictingChildren(ctx, s, txHash, s.settings.UtxoStore.ConflictingChildrenMaxNodes)
}
