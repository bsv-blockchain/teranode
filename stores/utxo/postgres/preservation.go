package postgres

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// PreserveTransactions marks transactions to be preserved from deletion.
// Sets preserve_until to the given height and clears delete_at_height.
//
// PRUNE-ELIGIBILITY GATE: only transactions that already carry a delete_at_height
// stamp (eligible now), OR are already being preserved (preserve_until set, so a
// still-needed preservation can be renewed each cycle even though the first
// preservation cleared the DAH), are preserved. A tx with neither is not fully
// spent, so it is not at risk of pruning and there is nothing to protect —
// preserving it would be pointless work, and it is exactly the not-fully-spent
// input that the expiry path could otherwise turn into a bad deletion stamp.
//
// Postgres note: delete_at_height is set by the BACKGROUND sweep (dah_sweep.go),
// which can lag behind a tx becoming fully spent. So a freshly-fully-spent parent
// may have no DAH yet when a pruner cycle runs and be skipped here — that is benign:
// the sweep stamps it shortly after, and because the stamp is a FUTURE height
// (completion+1+retention) the pruner will not act for `retention` blocks, during
// which the next pruner cycle re-admits and re-preserves it. ProcessExpiredPreservations
// is the setter-side safety net for the case where a preserved (eligible) tx is later
// un-spent by a reorg.
func (s *Store) PreserveTransactions(ctx context.Context, txIDs []chainhash.Hash, preserveUntilHeight uint32) error {
	if len(txIDs) == 0 {
		return nil
	}

	totalAffected := int64(0)

	for i := 0; i < len(txIDs); i += maxINClauseSize {
		end := i + maxINClauseSize
		if end > len(txIDs) {
			end = len(txIDs)
		}

		chunk := make([][]byte, end-i)
		for j, txID := range txIDs[i:end] {
			id := txID
			chunk[j] = id[:]
		}

		inClause, args := buildINClauseLocal(chunk, 2)
		allArgs := append([]interface{}{int32(preserveUntilHeight)}, args...) // preserve_until is INT4

		// Only preserve prune-eligible txs (see the gate rationale above): those that
		// already carry a delete_at_height stamp, or are already preserved.
		//
		// C4: remove the hashes from pending_deletes in the SAME statement as the DAH
		// clear via a CTE. The DELETE is a harmless no-op for hashes not in the
		// side-table. SELECT count(*) FROM upd returns the txs-updated count so
		// totalAffected accounting is correct.
		var rowsAffected int64
		q := fmt.Sprintf(`WITH upd AS (
			UPDATE txs SET preserve_until = $1, delete_at_height = NULL
			WHERE hash IN %s
			  AND (delete_at_height IS NOT NULL OR preserve_until IS NOT NULL)
			RETURNING hash
		),
		_del AS (
			DELETE FROM pending_deletes WHERE hash IN (SELECT hash FROM upd)
		)
		SELECT count(*) FROM upd`, inClause)
		if err := s.pool.QueryRow(ctx, q, allArgs...).Scan(&rowsAffected); err != nil {
			return errors.NewStorageError("[PreserveTransactions] failed to preserve chunk", err)
		}

		totalAffected += rowsAffected
	}

	s.logger.Debugf("[PreserveTransactions] preserved %d out of %d transactions", totalAffected, len(txIDs))

	return nil
}

// ProcessExpiredPreservations handles transactions whose preservation period has
// expired. For each tx with preserve_until <= currentHeight it clears preserve_until,
// and sets delete_at_height ONLY when the tx is genuinely safe to drop.
//
// Upholding the invariant "delete_at_height set ⟹ safe to delete": preservation can
// be requested for any parent of an old unmined tx, including a parent that is not
// fully spent (e.g. one output spent by a now-resolved mempool child, another output
// still live). Stamping such a parent for deletion would let the pruner — which
// deletes purely on the stamp (see the DESIGN CONTRACT in
// pruner_provider.deleteTombstonedPartition) — remove a tx that still has live UTXOs.
//
// The eligibility CASE mirrors the sweep's conditions (the canonical DAH setter):
//   - conflicting txs get a DAH (they need not be mined); else
//   - on the longest chain (unmined_since IS NULL) AND mined (mined_info non-empty) AND fully
//     spent.
//
// Setter-C reconcile (proc v15): fully-spentness is decided from the MAINTAINED
// spent_bits bitmap (spendable_count > 0 AND bit_count(spent_bits) = spendable_count),
// NOT from a per-row count(*) re-aggregation over the spends table. This matches the
// sweep proc (dah_sweep_proc.go) and SetMinedMulti (mined.go), so all three DAH setters
// agree on "fully spent". The bitmap OR is idempotent, so bit_count can never overshoot
// spendable_count — no ground-truth recount is needed to trust the gate.
//
// LAGGING-BITMAP RULE: the bitmap is folded by the BACKGROUND sweep, which lags the
// tip by dahSafeTip's lag. A parent that just became fully spent may still show
// bit_count(spent_bits) < spendable_count when its preservation expires (the sweep
// has not yet folded its last spend). In that case this path deliberately does NOT
// stamp — it clears preserve_until and leaves delete_at_height NULL. That is SAFE: an
// unstamped tx is never pruned, and the sweep stamps it (completion+1+retention, a
// FUTURE height) once the fold catches up. Stamping "early" off a lagging bitmap would
// risk a wrong height; not stamping only defers reclaim by at most a sweep cycle. This
// upholds the invariant "delete_at_height set ⟹ genuinely fully spent" without ever
// leaving a not-yet-fully-spent tx stamped.
//
// An ineligible tx just has preserve_until cleared, delete_at_height left NULL; the
// sweep re-stamps it if/when it actually becomes eligible. This runs once per pruner
// cycle over only the small set of expiring preservations (gated by px_preserve_until),
// so it reads only two row-local columns (no spends join) off the hot delete path.
func (s *Store) ProcessExpiredPreservations(ctx context.Context, currentHeight uint32) error {
	// Retention 0 disables pruning: never stamp a DAH (mirrors the sweep's early
	// return); just clear the expired preservations.
	retention := s.settings.GetUtxoStoreBlockHeightRetention()
	if retention == 0 {
		if _, err := s.pool.Exec(ctx,
			`UPDATE txs SET preserve_until = NULL WHERE preserve_until IS NOT NULL AND preserve_until <= $1`,
			int32(currentHeight)); err != nil {
			return errors.NewStorageError("failed to process expired preservations", err)
		}

		return nil
	}

	// Widen to int64 before adding so the sum cannot wrap in uint32 arithmetic,
	// then narrow to int32 for the INT4 delete_at_height column. This is the
	// conflicting-branch DAH (conflicting txs have no meaningful spend-completion
	// height, so a currentHeight-relative future height is correct for them).
	deleteAtHeight := int32(int64(currentHeight) + int64(retention))
	retention32 := int32(retention) //nolint:gosec // retention is a small positive delta

	// Use a CTE so that:
	//   - rows that get a DAH stamp are upserted into pending_deletes (ins),
	//   - rows whose DAH is set to NULL are removed from pending_deletes (del),
	// all in the same statement. A SELECT count(*) FROM upd returns the number of
	// affected rows for logging.
	//
	// Setter-C reconcile (proc v15): the fully-spent branch reads the maintained bitmap
	// (spendable_count > 0 AND bit_count(spent_bits) = spendable_count) instead of the
	// old count(*) re-aggregation over spends. A tx whose bits have NOT all folded yet
	// (the background fold lags) fails this predicate → delete_at_height stays NULL and
	// the sweep stamps it later. The fully-spent branch stamps at the sweep-consistent
	// completion height GREATEST(last_spend_height, mined_at_height)+1+retention ($3), so
	// this safety-net path and the sweep never disagree on the DAH of the same tx.
	var rowsAffected int64
	err := s.pool.QueryRow(ctx, `
		WITH upd AS (
			UPDATE txs SET
				delete_at_height = CASE
					WHEN conflicting THEN $1
					WHEN unmined_since IS NULL
					     AND mined_info IS NOT NULL AND octet_length(mined_info) > 0
					     AND out_count > 0
					     AND spendable_count > 0
					     AND bit_count(spent_bits) = spendable_count
					THEN (GREATEST(COALESCE(last_spend_height, 0), COALESCE(mined_at_height, 0)) + 1 + $3)::int
					ELSE NULL
				END,
				preserve_until = NULL
			WHERE preserve_until IS NOT NULL AND preserve_until <= $2
			RETURNING hash, delete_at_height
		),
		ins AS (
			INSERT INTO pending_deletes (hash, delete_at_height)
			SELECT hash, delete_at_height FROM upd WHERE delete_at_height IS NOT NULL
			ON CONFLICT (hash) DO UPDATE SET delete_at_height = EXCLUDED.delete_at_height
		),
		del AS (
			DELETE FROM pending_deletes WHERE hash IN (SELECT hash FROM upd WHERE delete_at_height IS NULL)
		)
		SELECT count(*) FROM upd
	`, deleteAtHeight, int32(currentHeight), retention32).Scan(&rowsAffected)
	if err != nil {
		return errors.NewStorageError("failed to process expired preservations", err)
	}

	s.logger.Infof("[ProcessExpiredPreservations] processed %d expired preservations at height %d", rowsAffected, currentHeight)

	return nil
}
