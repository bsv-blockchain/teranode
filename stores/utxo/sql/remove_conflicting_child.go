package sql

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// RemoveFromConflictingChildren deletes each (parent, child) pair from the
// conflicting_children table in a single SQL transaction. Idempotent.
func (s *Store) RemoveFromConflictingChildren(ctx context.Context, removals []utxo.ConflictingChildRemoval) error {
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
		DELETE FROM conflicting_children
		WHERE transaction_id       = (SELECT id FROM transactions WHERE hash = $1)
		  AND child_transaction_id = (SELECT id FROM transactions WHERE hash = $2)
	`

	for _, r := range removals {
		if r.ParentHash == nil || r.ChildHash == nil {
			return errors.NewInvalidArgumentError("parent and child hash must be non-nil")
		}

		if _, err = txn.ExecContext(ctx, q, r.ParentHash[:], r.ChildHash[:]); err != nil {
			return errors.NewStorageError("failed to remove from conflicting_children", err)
		}
	}

	if err = txn.Commit(); err != nil {
		return errors.NewStorageError("failed to commit conflicting_children removals", err)
	}

	return nil
}
