package teraslab

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teraslab "github.com/icellan/teraslab/client/go"
)

// RemoveBlockIDs trims the supplied block IDs from each transaction's block
// membership list without deleting the transaction record. It is the inverse
// of SetMinedMulti adding them, used by repair tooling when a transaction is
// referenced by multiple blocks and only a subset is being removed.
//
// Idempotent: removing a block ID that is not present, or targeting a txid that
// does not exist, is silently tolerated (matching the Aerospike backend's
// KEY_NOT_FOUND tolerance).
//
// TeraSlab's wire protocol removes one block ID (the shared BlockID parameter)
// across a list of txids per OP_SET_MINED_BATCH call with UnsetMined=true. A
// BlockIDsRemoval carries one txid but potentially many block IDs, so this
// inverts the grouping: removals are bucketed by block ID and one unset batch
// is issued per distinct block ID, with all txids needing that block removed
// batched together. This keeps the call count proportional to the number of
// distinct block IDs rather than the number of removals.
func (s *Store) RemoveBlockIDs(ctx context.Context, removals []utxo.BlockIDsRemoval) error {
	if len(removals) == 0 {
		return nil
	}

	// Bucket txids by the block ID that must be stripped from them. Dedupe
	// per (blockID, txid) so a repeated removal does not enqueue the same
	// txid twice in one batch.
	byBlock := make(map[uint32][]teraslab.TxID)
	seen := make(map[uint32]map[teraslab.TxID]struct{})

	for _, r := range removals {
		if r.TxHash == nil {
			return errors.NewInvalidArgumentError("txHash must be non-nil")
		}
		if len(r.BlockIDs) == 0 {
			continue
		}

		txid := hashToTxID(r.TxHash)
		for _, blockID := range r.BlockIDs {
			set, ok := seen[blockID]
			if !ok {
				set = make(map[teraslab.TxID]struct{})
				seen[blockID] = set
			}
			if _, dup := set[txid]; dup {
				continue
			}
			set[txid] = struct{}{}
			byBlock[blockID] = append(byBlock[blockID], txid)
		}
	}

	for blockID, txids := range byBlock {
		params := teraslab.SetMinedBatchParams{
			BlockID:              blockID,
			UnsetMined:           true,
			CurrentBlockHeight:   s.blockHeight.Load(),
			BlockHeightRetention: s.settings.GetUtxoStoreBlockHeightRetention(),
		}

		_, err := s.client.SetMinedBatch(ctx, params, txids)
		if err != nil {
			pe, ok := err.(*teraslab.PartialError)
			if !ok {
				return err
			}
			// Idempotency: a txid absent from the store surfaces as a
			// per-item TxNotFound. Tolerate it (the block ID is, by
			// definition, already not present on a record that does not
			// exist). Surface any other per-item failure.
			for i := range pe.Errors {
				if pe.Errors[i].Code != teraslab.ErrCodeTxNotFound {
					return errors.NewStorageError("failed to remove blockIDs", err)
				}
			}
		}
	}

	return nil
}
