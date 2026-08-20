package packedsql

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/jackc/pgx/v5"
)

func (s *Store) Unspend(ctx context.Context, spends []*utxo.Spend, flagAsLocked ...bool) error {
	setLocked := len(flagAsLocked) > 0 && flagAsLocked[0]

	for _, sp := range spends {
		if err := s.unspendOne(ctx, sp, setLocked); err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) unspendOne(ctx context.Context, sp *utxo.Spend, setLocked bool) error {
	page := pageOfVout(sp.Vout, s.pageSize)
	slot := slotOfVout(sp.Vout, s.pageSize)
	spendFrom := int(slot)*slotSpendSize + 1
	expected := packSpendingData(sp.SpendingData)
	zeros := make([]byte, slotSpendSize)

	lockedExpr := ""
	if setLocked {
		lockedExpr = ", flags = flags | 8"
	}

	if page == 0 {
		_, err := s.pool.Exec(ctx,
			`UPDATE packed_txs SET
			   spends = overlay(spends PLACING $2::bytea FROM $3),
			   spent_count = spent_count - 1,
			   delete_at_height = NULL`+lockedExpr+`
			 WHERE hash = $1 AND substring(spends FROM $3 FOR 36) = $4::bytea`,
			sp.TxID[:], zeros, spendFrom, expected)
		if err != nil {
			return errors.NewStorageError("packedsql: unspend failed for %s:%d", sp.TxID, sp.Vout, err)
		}

		return nil
	}

	dbTx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return errors.NewStorageError("packedsql: failed to begin unspend transaction", err)
	}

	defer func() { _ = dbTx.Rollback(ctx) }()

	var wasComplete bool

	err = dbTx.QueryRow(ctx,
		`SELECT spent_count = spendable_count FROM packed_tx_pages WHERE hash = $1 AND page = $2 FOR UPDATE`,
		sp.TxID[:], page).Scan(&wasComplete)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}

		return errors.NewStorageError("packedsql: unspend page lookup failed for %s:%d", sp.TxID, sp.Vout, err)
	}

	ct, err := dbTx.Exec(ctx,
		`UPDATE packed_tx_pages SET
		   spends = overlay(spends PLACING $3::bytea FROM $4),
		   spent_count = spent_count - 1
		 WHERE hash = $1 AND page = $2 AND substring(spends FROM $4 FOR 36) = $5::bytea`,
		sp.TxID[:], page, zeros, spendFrom, expected)
	if err != nil {
		return errors.NewStorageError("packedsql: unspend page update failed for %s:%d", sp.TxID, sp.Vout, err)
	}

	if ct.RowsAffected() > 0 {
		pagesSpentExpr := ""
		if wasComplete {
			pagesSpentExpr = ", pages_spent = pages_spent - 1"
		}

		if _, err = dbTx.Exec(ctx,
			`UPDATE packed_txs SET delete_at_height = NULL`+pagesSpentExpr+lockedExpr+` WHERE hash = $1`,
			sp.TxID[:]); err != nil {
			return errors.NewStorageError("packedsql: unspend master update failed for %s:%d", sp.TxID, sp.Vout, err)
		}
	}

	return dbTx.Commit(ctx)
}
