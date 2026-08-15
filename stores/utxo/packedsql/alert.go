package packedsql

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/jackc/pgx/v5"
)

type overrideState struct {
	frozen         bool
	spendableIn    *int64
	reassignedHash []byte
	exists         bool
	spentBy        []byte
}

func (s *Store) readOverrideState(ctx context.Context, q pgxQuerier, sp *utxo.Spend) (*overrideState, error) {
	page := pageOfVout(sp.Vout, s.pageSize)
	slot := slotOfVout(sp.Vout, s.pageSize)
	spendFrom := int(slot)*slotSpendSize + 1

	st := &overrideState{}

	var (
		storedSpend []byte
		flags       int16
		err         error
	)

	if page == 0 {
		err = q.QueryRow(ctx,
			`SELECT substring(spends FROM $2 FOR 36), flags FROM packed_txs WHERE hash = $1`,
			sp.TxID[:], spendFrom).Scan(&storedSpend, &flags)
	} else {
		err = q.QueryRow(ctx,
			`SELECT substring(p.spends FROM $3 FOR 36), m.flags
			 FROM packed_tx_pages p JOIN packed_txs m ON m.hash = p.hash
			 WHERE p.hash = $1 AND p.page = $2`,
			sp.TxID[:], page, spendFrom).Scan(&storedSpend, &flags)
	}

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NewTxNotFoundError("packedsql: transaction %s not found", sp.TxID)
		}

		return nil, errors.NewStorageError("packedsql: failed to read utxo %s:%d", sp.TxID, sp.Vout, err)
	}

	if unpackSpendingData(storedSpend) != nil {
		st.spentBy = storedSpend
	}

	st.frozen = flags&flagFrozen != 0

	var oFrozen bool

	err = q.QueryRow(ctx,
		`SELECT frozen, spendable_in, reassigned_hash FROM utxo_overrides WHERE hash = $1 AND vout = $2`,
		sp.TxID[:], sp.Vout).Scan(&oFrozen, &st.spendableIn, &st.reassignedHash)

	switch {
	case err == nil:
		st.exists = true
		st.frozen = st.frozen || oFrozen
	case errors.Is(err, pgx.ErrNoRows):
	default:
		return nil, errors.NewStorageError("packedsql: failed to read override %s:%d", sp.TxID, sp.Vout, err)
	}

	return st, nil
}

func (s *Store) FreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	dbTx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return errors.NewStorageError("packedsql: failed to begin FreezeUTXOs transaction", err)
	}

	defer func() { _ = dbTx.Rollback(ctx) }()

	for _, sp := range spends {
		st, err := s.readOverrideState(ctx, dbTx, sp)
		if err != nil {
			return err
		}

		if st.spentBy != nil {
			spendingData := unpackSpendingData(st.spentBy)
			return errors.NewUtxoSpentError(*sp.TxID, sp.Vout, *sp.UTXOHash, spendingData)
		}

		if st.frozen {
			return errors.NewUtxoFrozenError("packedsql: transaction %s:%d already frozen", sp.TxID, sp.Vout)
		}
	}

	for _, sp := range spends {
		if _, err = dbTx.Exec(ctx,
			`INSERT INTO utxo_overrides (hash, vout, frozen) VALUES ($1, $2, TRUE)
			 ON CONFLICT (hash, vout) DO UPDATE SET frozen = TRUE`,
			sp.TxID[:], sp.Vout); err != nil {
			return errors.NewStorageError("packedsql: failed to freeze %s:%d", sp.TxID, sp.Vout, err)
		}

		if _, err = dbTx.Exec(ctx,
			`UPDATE packed_txs SET flags = flags | 16 WHERE hash = $1`, sp.TxID[:]); err != nil {
			return errors.NewStorageError("packedsql: failed to set overrides flag for %s", sp.TxID, err)
		}
	}

	if err = dbTx.Commit(ctx); err != nil {
		return errors.NewStorageError("packedsql: failed to commit FreezeUTXOs transaction", err)
	}

	return nil
}

func (s *Store) UnFreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	dbTx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return errors.NewStorageError("packedsql: failed to begin UnFreezeUTXOs transaction", err)
	}

	defer func() { _ = dbTx.Rollback(ctx) }()

	for _, sp := range spends {
		st, err := s.readOverrideState(ctx, dbTx, sp)
		if err != nil {
			return err
		}

		if !st.frozen {
			return errors.NewUtxoFrozenError("packedsql: transaction %s:%d is not frozen", sp.TxID, sp.Vout)
		}
	}

	for _, sp := range spends {
		if _, err = dbTx.Exec(ctx,
			`DELETE FROM utxo_overrides WHERE hash = $1 AND vout = $2 AND spendable_in IS NULL AND reassigned_hash IS NULL`,
			sp.TxID[:], sp.Vout); err != nil {
			return errors.NewStorageError("packedsql: failed to unfreeze %s:%d", sp.TxID, sp.Vout, err)
		}

		if _, err = dbTx.Exec(ctx,
			`UPDATE utxo_overrides SET frozen = FALSE WHERE hash = $1 AND vout = $2`,
			sp.TxID[:], sp.Vout); err != nil {
			return errors.NewStorageError("packedsql: failed to unfreeze override %s:%d", sp.TxID, sp.Vout, err)
		}

		if _, err = dbTx.Exec(ctx,
			`UPDATE packed_txs SET flags = flags & ~16 WHERE hash = $1
			 AND NOT EXISTS (SELECT 1 FROM utxo_overrides WHERE hash = $1)`,
			sp.TxID[:]); err != nil {
			return errors.NewStorageError("packedsql: failed to clear overrides flag for %s", sp.TxID, err)
		}
	}

	if err = dbTx.Commit(ctx); err != nil {
		return errors.NewStorageError("packedsql: failed to commit UnFreezeUTXOs transaction", err)
	}

	return nil
}

func (s *Store) ReAssignUTXO(ctx context.Context, oldUtxo *utxo.Spend, newUtxo *utxo.Spend, tSettings *settings.Settings) error {
	dbTx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return errors.NewStorageError("packedsql: failed to begin ReAssignUTXO transaction", err)
	}

	defer func() { _ = dbTx.Rollback(ctx) }()

	st, err := s.readOverrideState(ctx, dbTx, oldUtxo)
	if err != nil {
		return err
	}

	if !st.frozen {
		return errors.NewUtxoFrozenError("packedsql: transaction %s:%d is not frozen", oldUtxo.TxID, oldUtxo.Vout)
	}

	reassignBlocks := uint32(utxo.ReAssignedUtxoSpendableAfterBlocks)
	if tSettings != nil && tSettings.UtxoStore.ReAssignedUtxoSpendableAfterBlocks > 0 {
		reassignBlocks = tSettings.UtxoStore.ReAssignedUtxoSpendableAfterBlocks
	}

	spendableIn := int64(s.GetBlockHeight()) + int64(reassignBlocks)

	if _, err = dbTx.Exec(ctx,
		`INSERT INTO utxo_overrides (hash, vout, frozen, spendable_in, reassigned_hash) VALUES ($1, $2, FALSE, $3, $4)
		 ON CONFLICT (hash, vout) DO UPDATE SET frozen = FALSE, spendable_in = $3, reassigned_hash = $4`,
		oldUtxo.TxID[:], oldUtxo.Vout, spendableIn, newUtxo.UTXOHash[:]); err != nil {
		return errors.NewStorageError("packedsql: failed to reassign %s:%d", oldUtxo.TxID, oldUtxo.Vout, err)
	}

	if err = dbTx.Commit(ctx); err != nil {
		return errors.NewStorageError("packedsql: failed to commit ReAssignUTXO transaction", err)
	}

	return nil
}
