package postgres

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// Delete removes a transaction and all its associated data in a single pgx
// transaction: its spend rows, the txs row, and any rows it left in the
// side-tables (pending_deletes, pending_unmined, dah_dirty_parents). There are
// no foreign keys, so these side-table rows are not cascaded — clean them here
// to match every other write path (spend.go, conflicting.go, mined.go) and
// avoid orphaned queue rows referencing a hash that no longer exists in txs.
func (s *Store) Delete(ctx context.Context, hash *chainhash.Hash) error {
	pgxTx, err := s.pool.Begin(ctx)
	if err != nil {
		return errors.NewStorageError("[Delete] begin", err)
	}
	defer pgxTx.Rollback(ctx) //nolint:errcheck

	// Delete in dependency order: children/side tables first, then parent table.
	deleteStatements := []string{
		`DELETE FROM spends WHERE prev_tx_hash = $1`,
		`DELETE FROM pending_deletes WHERE hash = $1`,
		`DELETE FROM pending_unmined WHERE hash = $1`,
		`DELETE FROM dah_dirty_parents WHERE hash = $1`,
		`DELETE FROM txs WHERE hash = $1`,
	}

	for _, stmt := range deleteStatements {
		if _, err = pgxTx.Exec(ctx, stmt, hash[:]); err != nil {
			return errors.NewStorageError("[Delete] failed for %s", hash, err)
		}
	}

	if err = pgxTx.Commit(ctx); err != nil {
		return errors.NewStorageError("[Delete] commit", err)
	}

	return nil
}
