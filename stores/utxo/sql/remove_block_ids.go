package sql

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// RemoveBlockIDs deletes block_ids rows for each removal in one SQL
// transaction. Idempotent.
func (s *Store) RemoveBlockIDs(ctx context.Context, removals []utxo.BlockIDsRemoval) error {
	if len(removals) == 0 {
		return nil
	}

	txn, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.NewStorageError("failed to begin tx", err)
	}

	defer func() {
		_ = txn.Rollback()
	}()

	q := `
		DELETE FROM block_ids
		WHERE block_id = $1
		  AND transaction_id = (SELECT id FROM transactions WHERE hash = $2)
	`

	for _, r := range removals {
		if r.TxHash == nil {
			return errors.NewInvalidArgumentError("txHash must be non-nil")
		}

		for _, blockID := range r.BlockIDs {
			if _, err = txn.ExecContext(ctx, q, blockID, r.TxHash[:]); err != nil {
				return errors.NewStorageError("failed to delete block_ids row", err)
			}
		}
	}

	if err = txn.Commit(); err != nil {
		return errors.NewStorageError("failed to commit remove block_ids tx", err)
	}

	return nil
}
