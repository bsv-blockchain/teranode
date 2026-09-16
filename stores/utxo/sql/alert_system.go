// Package sql provides a SQL-based implementation of the UTXO store interface.
// It supports both PostgreSQL and SQLite backends with automatic schema creation
// and migration.
//
// # Features
//
//   - Full UTXO lifecycle management (create, spend, unspend)
//   - Transaction metadata storage
//   - Input/output tracking
//   - Block height and median time tracking
//   - Optional UTXO expiration with automatic cleanup
//   - Prometheus metrics integration
//   - Support for the alert system (freeze/unfreeze/reassign UTXOs)
//
// # Usage
//
//	store, err := sql.New(ctx, logger, settings, &url.URL{
//	    Scheme: "postgres",
//	    Host:   "localhost:5432",
//	    User:   "user",
//	    Path:   "dbname",
//	    RawQuery: "expiration=1h",
//	})
//
// # Database Schema
//
// The store uses the following tables:
//   - transactions: Stores base transaction data
//   - inputs: Stores transaction inputs with previous output references
//   - outputs: Stores transaction outputs and UTXO state
//   - block_ids: Stores which blocks a transaction appears in
//
// # Metrics
//
// The following Prometheus metrics are exposed:
//   - teranode_sql_utxo_get: Number of UTXO retrieval operations
//   - teranode_sql_utxo_spend: Number of UTXO spend operations
//   - teranode_sql_utxo_reset: Number of UTXO reset operations
//   - teranode_sql_utxo_delete: Number of UTXO delete operations
//   - teranode_sql_utxo_errors: Number of errors by function and type
package sql

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
)

// FreezeUTXOs marks UTXOs as frozen, preventing them from being spent, and records each
// Spend's enforceAtHeight window (FreezeFrom/FreezeUntil) so the freeze is enforced at the
// same chain height on every node rather than from the moment the alert arrived here.
//
// Returns an error if any UTXO is already spent, or is already frozen with the same
// window. A repeat freeze that asks for a different window updates it: an authority can
// extend, shorten or shift the enforcement window of a freeze it already issued.
func (s *Store) FreezeUTXOs(ctx context.Context, spends []*utxostore.Spend, tSettings *settings.Settings) error {
	txHashIDMap := make(map[string]int)

	// check whether the UTXOs are already spent or frozen
	for _, spend := range spends {
		q := `
            SELECT t.id, o.frozen, o.spending_data, o.freezeFrom, o.freezeUntil
            FROM outputs AS o, transactions AS t
            WHERE t.hash = $1
              AND o.transaction_id = t.id AND o.idx = $2
        `

		var (
			id           int
			spendingData []byte
			frozen       bool
			freezeFrom   *uint32
			freezeUntil  *uint32
		)

		if err := s.db.QueryRowContext(ctx, q, spend.TxID[:], spend.Vout).Scan(&id, &frozen, &spendingData, &freezeFrom, &freezeUntil); err != nil {
			return err
		}

		if spendingData != nil {
			spendingData, err := spendpkg.NewSpendingDataFromBytes(spendingData)
			if err != nil {
				return errors.NewProcessingError("failed to create spending data from bytes", err)
			}

			return errors.NewUtxoSpentError(*spendingData.TxID, spend.Vout, *spend.UTXOHash, spendingData)
		}

		if frozen && nullableHeight(freezeFrom) == spend.FreezeFrom && nullableHeight(freezeUntil) == spend.FreezeUntil {
			return errors.NewUtxoFrozenError("transaction %s:%d already frozen", spend.TxID, spend.Vout)
		}

		txHashIDMap[spend.TxID.String()] = id
	}

	// if not, freeze the UTXO
	for _, spend := range spends {
		id := txHashIDMap[spend.TxID.String()]

		q := `UPDATE outputs SET frozen = true, freezeFrom = $3, freezeUntil = $4 WHERE transaction_id = $1 AND idx = $2 AND spending_data IS NULL`
		if _, err := s.db.ExecContext(ctx, q, id, spend.Vout, nullableHeightArg(spend.FreezeFrom), nullableHeightArg(spend.FreezeUntil)); err != nil {
			return err
		}
	}

	return nil
}

// nullableHeight normalises a NULL freeze bound to 0. NULL and 0 both mean "no bound" —
// "from genesis" for the lower bound, "no end" for the upper — so a freeze written before
// the window columns existed compares equal to an unqualified freeze.
func nullableHeight(h *uint32) uint32 {
	if h == nil {
		return 0
	}

	return *h
}

// nullableHeightArg is the inverse: a 0 bound is stored as NULL rather than 0, so an
// unqualified freeze leaves the columns as they would have been before this change.
func nullableHeightArg(h uint32) interface{} {
	if h == 0 {
		return nil
	}

	return h
}

// UnFreezeUTXOs removes the frozen status from UTXOs and clears their enforceAtHeight
// window, so an unfrozen output carries no residual height gate for a later re-freeze
// to inherit. Returns an error if any UTXO is not frozen.
func (s *Store) UnFreezeUTXOs(ctx context.Context, spends []*utxostore.Spend, tSettings *settings.Settings) error {
	txHashIDMap := make(map[string]int)

	// check whether the UTXOs are already spent or frozen
	for _, spend := range spends {
		q := `
            SELECT t.id, o.frozen
            FROM outputs AS o, transactions AS t
            WHERE t.hash = $1
              AND o.transaction_id = t.id AND o.idx = $2
        `

		var (
			id     int
			frozen bool
		)

		if err := s.db.QueryRowContext(ctx, q, spend.TxID[:], spend.Vout).Scan(&id, &frozen); err != nil {
			return err
		}

		if !frozen {
			return errors.NewUtxoFrozenError("transaction %s:%d is not frozen", spend.TxID, spend.Vout)
		}

		txHashIDMap[spend.TxID.String()] = id
	}

	for _, spend := range spends {
		id := txHashIDMap[spend.TxID.String()]

		q := `UPDATE outputs SET frozen = false, freezeFrom = NULL, freezeUntil = NULL WHERE transaction_id = $1 AND idx = $2 AND spending_data IS NULL AND frozen = true`
		if _, err := s.db.ExecContext(ctx, q, id, spend.Vout); err != nil {
			return err
		}
	}

	return nil
}

// ReAssignUTXO reassigns a frozen UTXO to a new transaction output.
// The UTXO must be frozen before it can be reassigned.
// The reassigned UTXO becomes spendable after ReAssignedUtxoSpendableAfterBlocks blocks.
func (s *Store) ReAssignUTXO(ctx context.Context, utxo *utxostore.Spend, newUtxo *utxostore.Spend, tSettings *settings.Settings) error {
	// check whether the UTXO is frozen
	q := `
            SELECT t.id, o.frozen
            FROM outputs AS o, transactions AS t
            WHERE t.hash = $1
              AND o.transaction_id = t.id AND o.idx = $2
        `

	var (
		id     int
		frozen bool
	)

	if err := s.db.QueryRowContext(ctx, q, utxo.TxID[:], utxo.Vout).Scan(&id, &frozen); err != nil {
		return err
	}

	if !frozen {
		return errors.NewUtxoFrozenError("transaction %s:%d is not frozen", utxo.TxID, utxo.Vout)
	}

	// Use configurable setting if provided, otherwise fall back to constant
	reassignBlocks := uint32(utxostore.ReAssignedUtxoSpendableAfterBlocks)
	if tSettings != nil && tSettings.UtxoStore.ReAssignedUtxoSpendableAfterBlocks > 0 {
		reassignBlocks = tSettings.UtxoStore.ReAssignedUtxoSpendableAfterBlocks
	}
	spendableIn := s.GetBlockHeight() + reassignBlocks

	// re-assign the UTXO to the new UTXO
	q = `
        UPDATE outputs
        SET utxo_hash = $1, frozen = false, freezeFrom = NULL, freezeUntil = NULL, spendableIn = $2
        WHERE transaction_id = $3
          AND idx = $4
          AND spending_data IS NULL
          AND frozen = true
    `
	if _, err := s.db.ExecContext(ctx, q, newUtxo.UTXOHash[:], spendableIn, id, utxo.Vout); err != nil {
		return err
	}

	return nil
}
