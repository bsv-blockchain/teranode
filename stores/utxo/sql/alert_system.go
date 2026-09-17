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
	"database/sql"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
)

// freezeRow is the freeze-relevant state of one output row.
type freezeRow struct {
	id            int
	spent         bool
	frozen        bool
	freezeFrom    *uint32
	freezeUntil   *uint32
	policyExpires *bool
}

// hasRecord reports whether the output carries a freeze at all: the policy marker or
// the consensus record. The record's presence is the freezeFrom column — always written
// when a freeze is recorded, whatever its value — because the policy marker alone is not
// durable across a block-validation spend the way the record must be.
func (r *freezeRow) hasRecord() bool {
	return r.frozen || r.freezeFrom != nil
}

// matches reports whether the stored record is exactly the one being asked for, in which
// case a repeat freeze is a no-op the caller is told about rather than a change.
func (r *freezeRow) matches(spend *utxostore.Spend) bool {
	return nullableHeight(r.freezeFrom) == spend.FreezeFrom &&
		nullableHeight(r.freezeUntil) == spend.FreezeUntil &&
		nullableBool(r.policyExpires) == spend.FreezePolicyExpires
}

// selectFreezeRow reads the freeze state of one output inside txn.
func (s *Store) selectFreezeRow(ctx context.Context, txn queryRower, spend *utxostore.Spend) (*freezeRow, error) {
	q := `
        SELECT t.id, o.spending_data IS NOT NULL, o.frozen, o.freezeFrom, o.freezeUntil, o.freezePolicyExpires
        FROM outputs AS o, transactions AS t
        WHERE t.hash = $1
          AND o.transaction_id = t.id AND o.idx = $2
    `

	r := &freezeRow{}

	if err := txn.QueryRowContext(ctx, q, spend.TxID[:], spend.Vout).Scan(&r.id, &r.spent, &r.frozen, &r.freezeFrom, &r.freezeUntil, &r.policyExpires); err != nil {
		return nil, err
	}

	return r, nil
}

// queryRower is the subset of *sql.Tx / *usql.DB the freeze helpers need.
type queryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// FreezeUTXOs records the alert system's freeze on each output: the policy marker
// (frozen = true, only while the output is unspent) and the consensus record — the
// enforceAtHeight window plus policyExpiresWithConsensus — which is recorded whether or
// not the output is spent, because it is a property of the outpoint rather than of this
// node's spent-state for it (issue #1422). See utxo.Store.FreezeUTXOs for the guarantees.
//
// Each output is handled in its own transaction and the write is a single UPDATE, so a
// spend landing between the read and the write cannot turn the call into a silent no-op:
// the record is written either way and only the policy marker follows the row's actual
// spent-state at write time.
//
// Returns an error if an output does not exist, or already carries exactly the record
// being asked for. A repeat freeze asking for a different record updates it.
func (s *Store) FreezeUTXOs(ctx context.Context, spends []*utxostore.Spend, tSettings *settings.Settings) error {
	for _, spend := range spends {
		if err := s.freezeUTXO(ctx, spend); err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) freezeUTXO(ctx context.Context, spend *utxostore.Spend) error {
	txn, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.NewStorageError("[FreezeUTXOs] failed to begin transaction", err)
	}

	defer func() {
		_ = txn.Rollback()
	}()

	r, err := s.selectFreezeRow(ctx, txn, spend)
	if err != nil {
		return err
	}

	if r.hasRecord() && r.matches(spend) {
		return errors.NewUtxoFrozenError("transaction %s:%d already frozen", spend.TxID, spend.Vout)
	}

	// The policy marker only has something to hold while the output is unspent; the
	// record is written regardless. Deciding that inside the UPDATE, rather than from the
	// SELECT above, is what makes the write race-free.
	q := `
        UPDATE outputs
        SET frozen = (CASE WHEN spending_data IS NULL THEN TRUE ELSE frozen END),
            freezeFrom = $3, freezeUntil = $4, freezePolicyExpires = $5
        WHERE transaction_id = $1 AND idx = $2
    `

	res, err := txn.ExecContext(ctx, q, r.id, spend.Vout, spend.FreezeFrom, nullableHeightArg(spend.FreezeUntil), nullableBoolArg(spend.FreezePolicyExpires))
	if err != nil {
		return err
	}

	if n, _ := res.RowsAffected(); n != 1 {
		return errors.NewStorageError("[FreezeUTXOs] freeze of %s:%d updated %d rows, want 1", spend.TxID, spend.Vout, n)
	}

	if err = txn.Commit(); err != nil {
		return errors.NewStorageError("[FreezeUTXOs] failed to commit", err)
	}

	if r.spent {
		s.logger.Infof("[FreezeUTXOs] freeze recorded on spent output %s:%d for heights [%d, %d)", spend.TxID, spend.Vout, spend.FreezeFrom, spend.FreezeUntil)
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

// nullableHeightArg is the inverse for the UPPER bound only: a 0 end is stored as NULL.
// The lower bound is always stored, 0 included, because its presence is the record.
func nullableHeightArg(h uint32) interface{} {
	if h == 0 {
		return nil
	}

	return h
}

func nullableBool(b *bool) bool {
	return b != nil && *b
}

func nullableBoolArg(b bool) interface{} {
	if !b {
		return nil
	}

	return true
}

// UnFreezeUTXOs removes the policy marker and the consensus record from each output. It
// succeeds on any output that carries either, spent or not, and is the only way a
// consensus record is ever deleted — alerts add or replace records, never remove them.
func (s *Store) UnFreezeUTXOs(ctx context.Context, spends []*utxostore.Spend, tSettings *settings.Settings) error {
	for _, spend := range spends {
		if err := s.unfreezeUTXO(ctx, spend); err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) unfreezeUTXO(ctx context.Context, spend *utxostore.Spend) error {
	txn, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.NewStorageError("[UnFreezeUTXOs] failed to begin transaction", err)
	}

	defer func() {
		_ = txn.Rollback()
	}()

	r, err := s.selectFreezeRow(ctx, txn, spend)
	if err != nil {
		return err
	}

	if !r.hasRecord() {
		return errors.NewUtxoFrozenError("transaction %s:%d is not frozen", spend.TxID, spend.Vout)
	}

	q := `
        UPDATE outputs
        SET frozen = FALSE, freezeFrom = NULL, freezeUntil = NULL, freezePolicyExpires = NULL
        WHERE transaction_id = $1 AND idx = $2
    `

	res, err := txn.ExecContext(ctx, q, r.id, spend.Vout)
	if err != nil {
		return err
	}

	if n, _ := res.RowsAffected(); n != 1 {
		return errors.NewStorageError("[UnFreezeUTXOs] unfreeze of %s:%d updated %d rows, want 1", spend.TxID, spend.Vout, n)
	}

	if err = txn.Commit(); err != nil {
		return errors.NewStorageError("[UnFreezeUTXOs] failed to commit", err)
	}

	return nil
}

// ReAssignUTXO reassigns a frozen UTXO to a new transaction output.
// The UTXO must be unspent and carry a freeze — the policy marker, the consensus record
// (a rolled-back below-window spend leaves only the record), or both.
// The reassigned UTXO becomes spendable after ReAssignedUtxoSpendableAfterBlocks blocks.
func (s *Store) ReAssignUTXO(ctx context.Context, utxo *utxostore.Spend, newUtxo *utxostore.Spend, tSettings *settings.Settings) error {
	txn, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.NewStorageError("[ReAssignUTXO] failed to begin transaction", err)
	}

	defer func() {
		_ = txn.Rollback()
	}()

	r, err := s.selectFreezeRow(ctx, txn, utxo)
	if err != nil {
		return err
	}

	if r.spent || !r.hasRecord() {
		return errors.NewUtxoFrozenError("transaction %s:%d is not frozen", utxo.TxID, utxo.Vout)
	}

	// Use configurable setting if provided, otherwise fall back to constant
	reassignBlocks := uint32(utxostore.ReAssignedUtxoSpendableAfterBlocks)
	if tSettings != nil && tSettings.UtxoStore.ReAssignedUtxoSpendableAfterBlocks > 0 {
		reassignBlocks = tSettings.UtxoStore.ReAssignedUtxoSpendableAfterBlocks
	}
	spendableIn := s.GetBlockHeight() + reassignBlocks

	// re-assign the UTXO to the new UTXO; the freeze record goes with the marker so a
	// later re-freeze does not inherit the old authority's heights
	q := `
        UPDATE outputs
        SET utxo_hash = $1, frozen = FALSE, freezeFrom = NULL, freezeUntil = NULL, freezePolicyExpires = NULL, spendableIn = $2
        WHERE transaction_id = $3
          AND idx = $4
          AND spending_data IS NULL
    `

	res, err := txn.ExecContext(ctx, q, newUtxo.UTXOHash[:], spendableIn, r.id, utxo.Vout)
	if err != nil {
		return err
	}

	if n, _ := res.RowsAffected(); n != 1 {
		return errors.NewStorageError("[ReAssignUTXO] reassignment of %s:%d updated %d rows, want 1", utxo.TxID, utxo.Vout, n)
	}

	if err = txn.Commit(); err != nil {
		return errors.NewStorageError("[ReAssignUTXO] failed to commit", err)
	}

	return nil
}
