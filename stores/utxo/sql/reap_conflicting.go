package sql

import (
	"context"
	"database/sql"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	sqlpruner "github.com/bsv-blockchain/teranode/stores/utxo/sql/pruner"
)

// reapConflicting deletes one expired conflicting record and the references its parents hold to it in
// one transaction. It returns false, without changing anything, when the record is no longer eligible
// (for example a promotion cleared its conflicting flag) or when a conflict intent is pending: a
// promotion in flight may be about to make the record live, so its parents must keep naming it.
func (s *Store) reapConflicting(ctx context.Context, txHash chainhash.Hash, blockHeight, safetyWindow uint32) (bool, error) {
	txn, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, errors.NewStorageError("[reapConflicting] failed to begin transaction for %s", txHash.String(), err)
	}

	defer func() {
		_ = txn.Rollback()
	}()

	lockQuery := `SELECT t.id FROM transactions t WHERE ` + sqlpruner.DefensiveDeleteFilter + ` AND t.conflicting AND t.hash = $3`
	if s.engine == "postgres" {
		lockQuery += ` FOR UPDATE OF t`
	}

	var id int

	if err = txn.QueryRowContext(ctx, lockQuery, blockHeight, safetyWindow, txHash[:]).Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}

		return false, errors.NewStorageError("[reapConflicting] failed to lock %s", txHash.String(), err)
	}

	var pendingIntents int

	if err = txn.QueryRowContext(ctx, `SELECT COUNT(*) FROM conflict_intents`).Scan(&pendingIntents); err != nil {
		return false, errors.NewStorageError("[reapConflicting] failed to check conflict intents for %s", txHash.String(), err)
	}

	if pendingIntents > 0 {
		s.logger.Debugf("[reapConflicting] keeping %s, %d conflict intents are pending", txHash.String(), pendingIntents)

		return false, nil
	}

	rows, err := txn.QueryContext(ctx, `
		UPDATE outputs
		SET spending_data = NULL
		WHERE (transaction_id, idx) IN (
			SELECT p.id, i.previous_tx_idx
			FROM inputs i
			JOIN transactions p ON p.hash = i.previous_transaction_hash
			WHERE i.transaction_id = $1
		)
		AND substr(spending_data, 1, 32) = $2
		RETURNING transaction_id
	`, id, txHash[:])
	if err != nil {
		return false, errors.NewStorageError("[reapConflicting] failed to clear parent spends of %s", txHash.String(), err)
	}

	var parentIDs []int

	seen := make(map[int]struct{})

	for rows.Next() {
		var parentID int

		if err = rows.Scan(&parentID); err != nil {
			_ = rows.Close()

			return false, errors.NewStorageError("[reapConflicting] failed to scan parent of %s", txHash.String(), err)
		}

		if _, ok := seen[parentID]; !ok {
			seen[parentID] = struct{}{}
			parentIDs = append(parentIDs, parentID)
		}
	}

	if err = rows.Err(); err != nil {
		_ = rows.Close()

		return false, errors.NewStorageError("[reapConflicting] failed to read parents of %s", txHash.String(), err)
	}

	if err = rows.Close(); err != nil {
		return false, errors.NewStorageError("[reapConflicting] failed to close parents of %s", txHash.String(), err)
	}

	for _, parentID := range parentIDs {
		if err = s.setDAH(ctx, txn, parentID); err != nil {
			return false, errors.NewStorageError("[reapConflicting] failed to update delete height of a parent of %s", txHash.String(), err)
		}
	}

	statements := []string{
		`DELETE FROM conflicting_children WHERE child_transaction_id = $1`,
		`DELETE FROM conflicting_children WHERE transaction_id = $1`,
		`DELETE FROM block_ids WHERE transaction_id = $1`,
		`DELETE FROM outputs WHERE transaction_id = $1`,
		`DELETE FROM inputs WHERE transaction_id = $1`,
		`DELETE FROM transactions WHERE id = $1`,
	}

	for _, statement := range statements {
		if _, err = txn.ExecContext(ctx, statement, id); err != nil {
			return false, errors.NewStorageError("[reapConflicting] failed to delete %s", txHash.String(), err)
		}
	}

	if err = txn.Commit(); err != nil {
		return false, errors.NewStorageError("[reapConflicting] failed to commit %s", txHash.String(), err)
	}

	prometheusUtxoDelete.Inc()

	return true, nil
}
