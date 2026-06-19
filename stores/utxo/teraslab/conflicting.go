package teraslab

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	teraslab "github.com/icellan/teraslab/client/go"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
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

	_, err := s.client.SetConflictingBatch(ctx, params, txids)
	if err != nil {
		if pe, ok := err.(*teraslab.PartialError); ok {
			// Build spend results for failed items
			var spendErrors []*utxo.Spend
			for _, itemErr := range pe.Errors {
				if int(itemErr.ItemIndex) < len(txHashes) {
					spendErrors = append(spendErrors, &utxo.Spend{
						TxID: func() *chainhash.Hash { h := txHashes[itemErr.ItemIndex]; return &h }(),
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

	// Parent conflicting-children updates are handled server-side by TeraSlab
	// during both Create (via parent_txids) and SetConflicting (via cold data parsing).

	return nil, nil, nil
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

// GetCounterConflicting returns the counter conflicting transactions for a given transaction hash.
func (s *Store) GetCounterConflicting(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	data, err := s.Get(ctx, &txHash, fields.ConflictingChildren)
	if err != nil {
		return nil, err
	}

	if data == nil {
		return nil, nil
	}

	return data.ConflictingChildren, nil
}

// GetConflictingChildren returns the children of the given conflicting transaction.
func (s *Store) GetConflictingChildren(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	data, err := s.Get(ctx, &txHash, fields.ConflictingChildren)
	if err != nil {
		return nil, err
	}

	if data == nil {
		return nil, nil
	}

	return data.ConflictingChildren, nil
}
