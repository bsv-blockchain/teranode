package postgres

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// RemoveFromConflictingChildren removes each child hash from the parent
// transaction's conflicting_children array. Idempotent: array_remove silently
// no-ops when the element is absent.
func (s *Store) RemoveFromConflictingChildren(ctx context.Context, removals []utxo.ConflictingChildRemoval) error {
	if len(removals) == 0 {
		return nil
	}

	txn, err := s.pool.Begin(ctx)
	if err != nil {
		return errors.NewStorageError("failed to begin tx", err)
	}

	defer func() {
		_ = txn.Rollback(ctx)
	}()

	const q = `UPDATE txs SET conflicting_children = array_remove(conflicting_children, $1) WHERE hash = $2`

	for _, r := range removals {
		if r.ParentHash == nil || r.ChildHash == nil {
			return errors.NewInvalidArgumentError("parent and child hash must be non-nil")
		}

		if _, err = txn.Exec(ctx, q, r.ChildHash[:], r.ParentHash[:]); err != nil {
			return errors.NewStorageError("failed to remove from conflicting_children", err)
		}
	}

	if err = txn.Commit(ctx); err != nil {
		return errors.NewStorageError("failed to commit conflicting_children removals", err)
	}

	return nil
}
