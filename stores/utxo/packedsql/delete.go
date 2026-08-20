package packedsql

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5"
)

func (s *Store) Delete(ctx context.Context, hash *chainhash.Hash) error {
	dbTx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return errors.NewStorageError("packedsql: failed to begin delete transaction", err)
	}

	defer func() { _ = dbTx.Rollback(ctx) }()

	statements := []string{
		`DELETE FROM packed_tx_pages WHERE hash = $1`,
		`DELETE FROM utxo_overrides WHERE hash = $1`,
		`DELETE FROM conflicting_children WHERE hash = $1 OR child_hash = $1`,
		`DELETE FROM packed_txs WHERE hash = $1`,
	}

	for _, stmt := range statements {
		if _, err = dbTx.Exec(ctx, stmt, hash[:]); err != nil {
			return errors.NewStorageError("packedsql: delete failed for %s", hash, err)
		}
	}

	if err = dbTx.Commit(ctx); err != nil {
		return errors.NewStorageError("packedsql: failed to commit delete transaction", err)
	}

	return nil
}
