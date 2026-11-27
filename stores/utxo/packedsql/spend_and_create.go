package packedsql

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/jackc/pgx/v5"
)

func (s *Store) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	options, err := utxo.ParseCreateOptions(opts...)
	if err != nil {
		return nil, nil, err
	}

	dbTx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, nil, errors.NewStorageError("packedsql: failed to begin SpendAndCreate transaction", err)
	}

	committed := false

	defer func() {
		if !committed {
			_ = dbTx.Rollback(ctx)
		}
	}()

	commit := func() error {
		if err := dbTx.Commit(ctx); err != nil {
			return errors.NewStorageError("packedsql: failed to commit SpendAndCreate transaction", err)
		}

		committed = true

		return nil
	}

	if _, err = dbTx.Exec(ctx, "SAVEPOINT sp1"); err != nil {
		return nil, nil, errors.NewStorageError("packedsql: failed to create savepoint", err)
	}

	var spends []*utxo.Spend

	if !options.CreateOnly {
		if options.IgnoreFlags.SkipUTXOHashCheck {
			spends, err = utxo.GetSpendsOutpointOnly(tx)
		} else {
			spends, err = utxo.GetSpends(tx)
		}

		if err != nil {
			return nil, nil, err
		}

		if len(spends) > 0 {
			if err = s.spendOnQuerier(ctx, dbTx, spends, blockHeight, options.IgnoreFlags); err != nil {
				if _, rbErr := dbTx.Exec(ctx, "ROLLBACK TO SAVEPOINT sp1"); rbErr != nil {
					return nil, spends, errors.NewStorageError("packedsql: savepoint rollback failed after spend error", rbErr, err)
				}

				if !options.SpendOnly && s.onlyMissingParents(spends) && s.recordExists(ctx, dbTx, recordHash(tx, options)) {
					for _, sp := range spends {
						sp.Err = nil
					}

					err = errors.NewTxExistsError("packedsql: transaction %s already exists, its spends were made by an earlier call", recordHash(tx, options))
				}

				if cErr := commit(); cErr != nil {
					return nil, spends, cErr
				}

				return nil, spends, err
			}
		}

		if options.SpendOnly {
			if err = commit(); err != nil {
				return nil, spends, err
			}

			return nil, spends, nil
		}
	}

	rows, md, err := s.buildTxRows(tx, blockHeight, options)
	if err != nil {
		return nil, nil, err
	}

	if err = s.insertTxRowsOn(ctx, dbTx, rows); err != nil {
		if errors.Is(err, errors.ErrTxExists) {
			if cErr := commit(); cErr != nil {
				return nil, spends, cErr
			}

			return nil, spends, err
		}

		if _, rbErr := dbTx.Exec(ctx, "ROLLBACK TO SAVEPOINT sp1"); rbErr != nil {
			return nil, spends, errors.NewStorageError("packedsql: savepoint rollback failed after create error", rbErr, err)
		}

		if cErr := commit(); cErr != nil {
			return nil, nil, cErr
		}

		return nil, nil, err
	}

	if err = commit(); err != nil {
		return nil, spends, err
	}

	if options.CreateOnly {
		return md, nil, nil
	}

	return md, spends, nil
}

// SpendAndCreateMulti writes each dependency level of the list concurrently through the atomic
// SpendAndCreate, as the shared DefaultSpendAndCreateMulti does for every store without a
// faster implementation.
func (s *Store) SpendAndCreateMulti(ctx context.Context, txs []*bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) ([]utxo.SpendAndCreateMultiResult, error) {
	return utxo.DefaultSpendAndCreateMulti(ctx, s, utxo.SpendAndCreateMultiWidthsFor(s.settings), txs, blockHeight, opts...)
}

// onlyMissingParents reports whether every failed spend failed because its parent has no
// record, and at least one did.
func (s *Store) onlyMissingParents(spends []*utxo.Spend) bool {
	failed := false

	for _, sp := range spends {
		if sp.Err == nil {
			continue
		}

		if !errors.Is(sp.Err, errors.ErrTxNotFound) {
			return false
		}

		failed = true
	}

	return failed
}

// recordExists reports whether the transaction already has a record. SpendAndCreate spends and
// creates in one database transaction, so a record written by it proves its spends were made,
// even when a parent has since been pruned.
func (s *Store) recordExists(ctx context.Context, q pgxQuerier, hash *chainhash.Hash) bool {
	var exists bool

	if err := q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM packed_txs WHERE hash = $1)`, hash[:]).Scan(&exists); err != nil {
		return false
	}

	return exists
}
