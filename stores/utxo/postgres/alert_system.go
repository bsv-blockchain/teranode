package postgres

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/jackc/pgx/v5"
)

// FreezeUTXOs marks UTXOs as frozen, preventing them from being spent.
// Returns an error if any UTXO is already spent or frozen.
// Atomically guards the freeze on the txs array subscript; spend-state is
// confirmed via a LEFT JOIN against the spends table before the UPDATE.
func (s *Store) FreezeUTXOs(ctx context.Context, spends []*utxo.Spend, _ *settings.Settings) error {
	for _, spend := range spends {
		// prev_output_idx / the packed bitmap probes bind to INT4 — reject
		// out-of-range vouts at this entry point.
		if _, err := voutToInt32(spend.Vout); err != nil {
			return err
		}
		// Atomic freeze on the packed bitmap: set bit vout false→true only if
		// the slot exists, is not already frozen, and has no matching spends row.
		// Performing the guard inside the UPDATE's WHERE closes the check-then-write
		// race where two concurrent freezes both pass the check and both "succeed".
		//
		// out_frozens is NULL until the first freeze ("no output frozen" common
		// case); initialize it on demand to (out_count+7)/8 zero bytes. repeat()
		// returns text, so the zero bytea is built via decode(repeat('00',..),'hex').
		// The WHERE guard $2 < out_count keeps both get_bit and set_bit in range.
		//
		// Freezing is PER-OUTPUT (gold standard: aerospike freezes only the target
		// slot). We deliberately do NOT set the transaction-level `frozen` column
		// here: that column is the whole-tx freeze gate (set only at create via
		// WithFrozen) and the spend-validation CTE checks it as `tx_frozen`, so
		// setting it would block every other output of a multi-output tx. The
		// per-output out_frozens bit is the sole gate for an individually
		// frozen output.
		tag, err := s.pool.Exec(ctx, `
			UPDATE txs
			SET out_frozens = set_bit(
			        COALESCE(out_frozens, decode(repeat('00', (out_count + 7) / 8), 'hex')),
			        $2::int, 1)
			WHERE hash = $1
			  AND $2::int < out_count
			  AND NOT (out_frozens IS NOT NULL AND get_bit(out_frozens, $2::int) = 1)
			  AND NOT EXISTS (
				SELECT 1 FROM spends sp
				WHERE sp.prev_tx_hash = $1 AND sp.prev_output_idx = $2
			  )
		`, spend.TxID[:], spend.Vout)
		if err != nil {
			return errors.NewStorageError("[FreezeUTXOs] failed to freeze output %s:%d", spend.TxID, spend.Vout, err)
		}

		if tag.RowsAffected() == 0 {
			// Nothing was frozen — diagnose why so we return the correct typed error.
			if reason := s.freezeRejectReason(ctx, spend); reason != nil {
				return reason
			}
			// Output exists, unspent and unfrozen at diagnosis time but the guarded
			// UPDATE matched nothing — a concurrent freeze won the race. Treat as
			// already frozen.
			return errors.NewUtxoFrozenError("transaction %s:%d already frozen", spend.TxID, spend.Vout)
		}
	}

	return nil
}

// freezeRejectReason returns the typed error explaining why a guarded freeze
// UPDATE affected no rows (spent or not found), or nil if the output is present,
// unspent and unfrozen — meaning a concurrent freeze won the race.
// Packed form: reads the out_frozens bitmap on the txs row.
func (s *Store) freezeRejectReason(ctx context.Context, spend *utxo.Spend) error {
	var (
		outputFrozen bool
		spendingData []byte
	)
	err := s.pool.QueryRow(ctx, `
		SELECT
		    (t.out_frozens IS NOT NULL AND get_bit(t.out_frozens, $2::int) = 1),
		    sp.spending_data
		FROM txs t
		LEFT JOIN spends sp ON sp.prev_tx_hash = t.hash AND sp.prev_output_idx = $2
		WHERE t.hash = $1 AND $2::int < t.out_count
	`, spend.TxID[:], spend.Vout).Scan(&outputFrozen, &spendingData)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// No row means the tx does not exist, or the vout is out of range — either
			// way the UTXO does not exist. Report TxNotFound (not an opaque storage
			// error) so callers can distinguish "absent" from "storage failure".
			return errors.NewTxNotFoundError("[FreezeUTXOs] output not found %s:%d", spend.TxID, spend.Vout)
		}
		return errors.NewStorageError("[FreezeUTXOs] output lookup failed for %s:%d", spend.TxID, spend.Vout, err)
	}

	if spendingData != nil {
		sd, parseErr := spendpkg.NewSpendingDataFromBytes(spendingData)
		if parseErr != nil {
			return errors.NewProcessingError("failed to create spending data from bytes", parseErr)
		}
		// spend.UTXOHash is legitimately nil for callers that locate a UTXO by
		// (txid, vout) alone; guard the deref to match GetSpend (get.go) and avoid
		// an externally-reachable nil-pointer panic.
		var utxoHash chainhash.Hash
		if spend.UTXOHash != nil {
			utxoHash = *spend.UTXOHash
		}

		return errors.NewUtxoSpentError(*spend.TxID, spend.Vout, utxoHash, sd)
	}

	if outputFrozen {
		return errors.NewUtxoFrozenError("transaction %s:%d already frozen", spend.TxID, spend.Vout)
	}

	return nil
}

// UnFreezeUTXOs removes the frozen status from UTXOs.
// Returns an error if any UTXO is not frozen.
func (s *Store) UnFreezeUTXOs(ctx context.Context, spends []*utxo.Spend, _ *settings.Settings) error {
	for _, spend := range spends {
		// Vout binds to INT4 columns — reject out-of-range values up front.
		if _, err := voutToInt32(spend.Vout); err != nil {
			return err
		}
		// Clear the per-output bit in ONE guarded statement. The WHERE requires the
		// bit to be currently SET, making the UPDATE the sole source of truth: a
		// 0-row result means the output is not frozen or the tx no longer exists. We
		// surface that as a typed error rather than a silent no-op — matching
		// aerospike's unfreeze UDF (which returns TX_NOT_FOUND / UTXO_NOT_FROZEN) and
		// removing the prior TOCTOU between a separate frozen-check read and this
		// write.
		//
		// We DELIBERATELY do NOT touch the tx-level `frozen` column here, symmetric
		// with FreezeUTXOs (see its comment). `frozen` is the whole-tx freeze gate,
		// set only at create via WithFrozen, and is independent of the per-output
		// out_frozens bitmap. Recomputing it from the bitmap conflated the two:
		// unfreezing one output of a tx with another still-frozen output would flip
		// the whole-tx gate true and block every (never-frozen) output — and would
		// also silently drop a genuine create-time whole-tx freeze. Both aerospike
		// and the sql store keep the two concepts fully separate.
		tag, err := s.pool.Exec(ctx, `
			UPDATE txs
			SET out_frozens = set_bit(out_frozens, $2::int, 0)
			WHERE hash = $1 AND $2::int < out_count
			  AND out_frozens IS NOT NULL AND get_bit(out_frozens, $2::int) = 1
		`, spend.TxID[:], spend.Vout)
		if err != nil {
			return errors.NewStorageError("[UnFreezeUTXOs] failed to unfreeze output %s:%d", spend.TxID, spend.Vout, err)
		}
		if tag.RowsAffected() == 0 {
			return s.frozenWriteRejectReason(ctx, spend, "UnFreezeUTXOs")
		}
	}

	return nil
}

// ReAssignUTXO reassigns a frozen UTXO to a new transaction output.
// The UTXO must be frozen before it can be reassigned.
func (s *Store) ReAssignUTXO(ctx context.Context, utxoSpend *utxo.Spend, newUtxo *utxo.Spend, tSettings *settings.Settings) error {
	// Vout binds to INT4 columns — reject out-of-range values up front.
	if _, err := voutToInt32(utxoSpend.Vout); err != nil {
		return err
	}
	// utxoSpend.TxID and newUtxo.UTXOHash are dereferenced ([:]) in the UPDATE
	// below; guard them so a malformed caller yields InvalidArgument rather than an
	// externally-reachable nil-pointer panic (matching the guards in freezeReject
	// and GetSpend).
	if utxoSpend.TxID == nil {
		return errors.NewInvalidArgumentError("[ReAssignUTXO] utxoSpend.TxID must not be nil")
	}
	if newUtxo == nil || newUtxo.UTXOHash == nil {
		return errors.NewInvalidArgumentError("[ReAssignUTXO] newUtxo.UTXOHash must not be nil")
	}
	// The source UTXO must be frozen to be reassigned. That precondition is
	// enforced by the guarded UPDATE below (get_bit = 1 in the WHERE), so there is
	// no separate frozen-check read that could race with a concurrent unfreeze.

	// Use configurable setting if provided, otherwise fall back to constant.
	reassignBlocks := uint32(utxo.ReAssignedUtxoSpendableAfterBlocks)
	if tSettings != nil && tSettings.UtxoStore.ReAssignedUtxoSpendableAfterBlocks > 0 {
		reassignBlocks = tSettings.UtxoStore.ReAssignedUtxoSpendableAfterBlocks
	}
	spendableIn := s.GetBlockHeight() + reassignBlocks

	// Update the packed columns in ONE statement: splice the new utxo hash's
	// 16-byte prefix into the flat utxo_hashes (overlay at byte offset vout*16, 1-based),
	// clear the per-output frozen bit, and rebuild spendable_ins as a contiguous
	// 1-based INT[] of length out_count with this slot set (preserving any prior
	// reassignments) — so a concurrent Get/Spend never observes a torn state. The
	// rebuild is required because a bare `spendable_ins[vout+1] = v` on the NULL
	// create-time array yields a length-1 array at lower bound vout+1, which the
	// readers' `array_length(spendable_ins,1) >= vout+1` guard then misses for
	// vout >= 1 (dropping the maturity gate; see TestReAssignUTXOSpendableInAtVoutGE1).
	// Reassignment does NOT mutate out_spendables, so spendable_count is
	// intentionally left untouched. As in UnFreezeUTXOs, we DELIBERATELY do NOT
	// recompute the tx-level `frozen` column from the bitmap: it is the create-time
	// whole-tx gate and is independent of the per-output out_frozens (both aerospike
	// and the sql store keep them separate).
	si := int32(spendableIn)
	tag, err := s.pool.Exec(ctx, `
		UPDATE txs
		SET utxo_hashes = overlay(utxo_hashes placing $3::bytea from $2::int * 16 + 1 for 16),
		    out_frozens = set_bit(out_frozens, $2::int, 0),
		    spendable_ins = (
		        SELECT array_agg(
		            CASE WHEN g = $2::int THEN $4::int
		                 WHEN spendable_ins IS NOT NULL
		                      AND g + 1 BETWEEN array_lower(spendable_ins, 1) AND array_upper(spendable_ins, 1)
		                 THEN spendable_ins[g + 1]
		                 ELSE NULL::int END
		            ORDER BY g)
		        FROM generate_series(0, out_count - 1) AS g
		    )
		WHERE hash = $1 AND $2::int < out_count
		  AND out_frozens IS NOT NULL AND get_bit(out_frozens, $2::int) = 1
	`, utxoSpend.TxID[:], utxoSpend.Vout, newUtxo.UTXOHash[:16], si)
	if err != nil {
		return errors.NewStorageError("[ReAssignUTXO] failed to update packed columns for %s:%d", utxoSpend.TxID, utxoSpend.Vout, err)
	}
	if tag.RowsAffected() == 0 {
		return s.frozenWriteRejectReason(ctx, utxoSpend, "ReAssignUTXO")
	}

	return nil
}

// frozenWriteRejectReason explains why a guarded unfreeze/reassign UPDATE matched
// no rows: the transaction no longer exists, or the output is not frozen. This
// mirrors aerospike's unfreeze/reassign UDF, which returns TX_NOT_FOUND /
// UTXO_NOT_FROZEN rather than silently succeeding. Called only on the (rare)
// 0-row path, so the extra read never touches the success hot path.
func (s *Store) frozenWriteRejectReason(ctx context.Context, spend *utxo.Spend, op string) error {
	var frozen bool
	err := s.pool.QueryRow(ctx, `
		SELECT (t.out_frozens IS NOT NULL AND get_bit(t.out_frozens, $2::int) = 1)
		FROM txs t WHERE t.hash = $1 AND $2::int < t.out_count
	`, spend.TxID[:], spend.Vout).Scan(&frozen)
	if errors.Is(err, pgx.ErrNoRows) {
		return errors.NewTxNotFoundError("[%s] transaction not found %s:%d", op, spend.TxID, spend.Vout)
	}
	if err != nil {
		return errors.NewStorageError("[%s] reject-reason lookup failed for %s:%d", op, spend.TxID, spend.Vout, err)
	}
	// Row present but the bit is clear → not frozen (or a concurrent unfreeze won
	// the race). Either way the requested write did not apply.
	return errors.NewUtxoFrozenError("transaction %s:%d is not frozen", spend.TxID, spend.Vout)
}
