package postgres

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5"
)

// dahReconcileCursorDDL is the per-partition rotating cursor for the bounded
// reconciliation backstop. One row per hash partition holds the hash of the last
// tx audited in that partition, so each reconcile pass resumes from there and
// rotates through the whole partition over many passes WITHOUT ever scanning all
// history in a single run. Created unconditionally in createSchemaInternal.
const dahReconcileCursorDDL = `
CREATE TABLE IF NOT EXISTS dah_reconcile_cursor (
    partition   INT   PRIMARY KEY,
    last_hash   BYTEA
);`

// dahReconcileAuditChain is the truth-recompute + correction CTE chain shared by
// the rotating-slice audit and the dirty-parents drain. The caller supplies a
// preceding CTE named `slice_txs` (the candidate parents, with spent_bits,
// out_count, out_spendables, gates, ...) and its own final SELECT; the chain is
// spliced between them. Sprintf verbs: %[1]s = partition leaf suffix,
// %[2]s = safe-tip parameter placeholder, %[3]s = retention parameter placeholder
// (the two call sites number their positional parameters differently).
//
// The chain recomputes each candidate's TRUE spent bitmap from the spends table
// (ground truth) and corrects the stored row where it disagrees:
//
//	byte_agg — per (parent, byte index) integer mask of the parent's SPENDABLE
//	           spends up to the safe tip: byte i/8 gets bit 1<<(i%8), the same
//	           LSB-first get_bit encoding as out_spendables/spent_bits. The
//	           spendability test mirrors the fold (CASE-guarded so get_bit is
//	           never evaluated out of range: prev_output_idx < out_count AND
//	           get_bit(out_spendables, idx) = 1).
//	truth    — per-parent BYTEA mask, width-stable at exactly (out_count+7)/8
//	           bytes: gap bytes are zero-filled SET-WISE (LATERAL series + LEFT
//	           JOIN — not a correlated subquery per parent), so a parent with NO
//	           qualifying spends gets the all-zero mask of that width. LEFT JOIN
//	           LATERAL keeps zero-output parents (empty series → gs.i IS NULL →
//	           FILTER drops it → COALESCE yields the empty '\x' mask, matching
//	           the zero-byte spent_bits create pre-sizes for out_count = 0).
//	           true_last_spend = max spent_at_height over the qualifying spends.
//	drift    — candidates whose stored row disagrees with truth. spent_bits from
//	           create is pre-sized to (out_count+7)/8 but rows written only by
//	           the fold can be SHORTER (the fold masks stop at the highest folded
//	           byte); missing trailing bytes mean zero, so the stored value is
//	           zero-padded to the truth width before the IS DISTINCT FROM —
//	           otherwise every short-but-equal row would count as drift forever.
//	           (repeat('00', n) with n clamped ≥ 0; a stored value LONGER than
//	           truth — impossible under the spent_bits ⊆ out_spendables
//	           invariant — still compares distinct and is rewritten to truth.)
//	upd      — SET spent_bits to the true mask (authoritative REPLACE, not an OR:
//	           the whole point is clearing wrongly-set bits) and last_spend_height
//	           to the true last spend, stamping / un-stamping delete_at_height
//	           per the CASE (see the function comments).
//	ins/del  — mirror the stamp changes into pending_deletes (the only pruner
//	           feed): freshly-completed stamps upserted, cleared stamps purged.
const dahReconcileAuditChain = `
		byte_agg AS (
			SELECT c.hash,
			       s.prev_output_idx / 8 AS byte_idx,
			       bit_or(1 << (s.prev_output_idx %% 8)) AS byte_val,
			       max(s.spent_at_height) AS max_h
			  FROM slice_txs c
			  JOIN spends_p%[1]s s
			    ON s.prev_tx_hash = c.hash
			   AND s.spent_at_height <= %[2]s
			 WHERE CASE WHEN s.prev_output_idx < c.out_count
			            THEN get_bit(c.out_spendables, s.prev_output_idx) = 1
			            ELSE false END
			 GROUP BY c.hash, s.prev_output_idx / 8
		),
		truth AS (
			SELECT c.hash,
			       COALESCE(decode(string_agg(lpad(to_hex(COALESCE(ba.byte_val, 0)), 2, '0'), '' ORDER BY gs.i)
			                    FILTER (WHERE gs.i IS NOT NULL), 'hex'), '\x'::bytea) AS true_bits,
			       max(ba.max_h) AS true_last_spend
			  FROM slice_txs c
			  LEFT JOIN LATERAL generate_series(0, (c.out_count + 7) / 8 - 1) gs(i) ON TRUE
			  LEFT JOIN byte_agg ba ON ba.hash = c.hash AND ba.byte_idx = gs.i
			 GROUP BY c.hash
		),
		drift AS (
			SELECT c.hash, c.spendable_count, c.mined_at_height, c.delete_at_height,
			       c.preserve_until, c.unmined_since, c.conflicting,
			       t.true_bits, t.true_last_spend
			  FROM slice_txs c
			  JOIN truth t ON t.hash = c.hash
			 WHERE (c.spent_bits || decode(repeat('00', GREATEST(octet_length(t.true_bits) - octet_length(c.spent_bits), 0)), 'hex'))
			       IS DISTINCT FROM t.true_bits
			    OR c.last_spend_height IS DISTINCT FROM t.true_last_spend
			    -- Stamped-but-not-fully-spent: a premature/stale delete_at_height (e.g. a
			    -- stamp taken from wrongly-full bits that a later Unspend heal corrected
			    -- down). The bits themselves may already be correct, so this is NOT caught
			    -- by the drift predicates above. Exclude the legitimate not-fully-spent
			    -- stamps: conflicting losers and preserved txs. Zero-spendable
			    -- (spendable_count = 0) is excluded by the explicit > 0 guard — never rely
			    -- on vacuous bitmap equality (out_spendables is NULLable).
			    OR (c.delete_at_height IS NOT NULL
			        AND bit_count(t.true_bits) < c.spendable_count
			        AND c.spendable_count > 0
			        AND NOT c.conflicting
			        AND c.preserve_until IS NULL
			        AND c.unmined_since IS NULL)
			    -- Fully-spent-but-not-stamped: a mined, fully-spent tx that LOST its
			    -- delete_at_height (e.g. a non-owning Unspend cleared the stamp of a
			    -- still-fully-spent parent — spend.go — leaving the bitmap complete, so
			    -- the drift predicates above do NOT fire). Without admitting it here the
			    -- upd re-stamp branch never runs and the tx is never re-stamped or pruned
			    -- (disk leak). Mirrors the exact conditions of that stamp branch.
			    OR (c.delete_at_height IS NULL
			        AND bit_count(t.true_bits) = c.spendable_count
			        AND c.spendable_count > 0
			        AND c.mined_at_height IS NOT NULL
			        AND NOT c.conflicting
			        AND c.preserve_until IS NULL
			        AND c.unmined_since IS NULL)
		),
		upd AS (
			UPDATE txs_p%[1]s t
			   SET spent_bits        = d.true_bits,
			       last_spend_height = d.true_last_spend,
			       delete_at_height  = CASE
			           WHEN bit_count(d.true_bits) = d.spendable_count
			                AND d.spendable_count > 0
			                AND d.mined_at_height IS NOT NULL
			                AND d.delete_at_height IS NULL
			                AND d.preserve_until IS NULL
			                AND d.unmined_since IS NULL
			           THEN (GREATEST(COALESCE(d.true_last_spend, 0), d.mined_at_height) + 1 + %[3]s)::int
			           -- Un-stamp a premature/stale stamp on a genuinely not-fully-spent,
			           -- non-conflicting, unpreserved, mined tx (defense-in-depth). Leaving
			           -- it would let the pruner delete a tx with a live UTXO.
			           WHEN d.delete_at_height IS NOT NULL
			                AND bit_count(d.true_bits) < d.spendable_count
			                AND d.spendable_count > 0
			                AND NOT d.conflicting
			                AND d.preserve_until IS NULL
			                AND d.unmined_since IS NULL
			           THEN NULL
			           ELSE d.delete_at_height
			       END
			  FROM drift d
			 WHERE t.hash = d.hash
			RETURNING t.hash, t.delete_at_height, t.spent_bits, t.spendable_count
		),
		ins AS (
			INSERT INTO pending_deletes_p%[1]s (hash, delete_at_height)
			SELECT hash, delete_at_height FROM upd
			 WHERE delete_at_height IS NOT NULL
			   AND bit_count(spent_bits) = spendable_count
			   AND spendable_count > 0
			ON CONFLICT (hash) DO UPDATE SET delete_at_height = EXCLUDED.delete_at_height
		),
		del AS (
			DELETE FROM pending_deletes_p%[1]s pd
			 USING upd u
			 WHERE pd.hash = u.hash AND u.delete_at_height IS NULL
		)`

// reconcileSpentBitsPartition runs one BOUNDED reconciliation pass over a single
// hash partition. It is the bitmap-recompute backstop for the MAINTAINED
// txs.spent_bits bitmap: the v15 fold is idempotent (duplicate ORs are no-ops),
// so the old counter-drift classes are structurally gone, but bits can still go
// wrong in ways the fold can never self-correct — the fold-vs-Unspend
// stale-snapshot race can re-set a just-cleared bit (primary heal: the
// dah_dirty_parents drain below; this audit is the defence-in-depth for a lost
// queue row), and any unforeseen corruption (torn write, operator surgery)
// persists forever in a maintained column. This pass restores the self-healing
// property, bounded so it never costs O(all history).
//
// Bounding: it selects at most `slice` txs from the partition, resuming from a
// per-partition rotating cursor (dah_reconcile_cursor.last_hash) ordered by hash,
// so successive passes rotate through the whole partition a slice at a time. When
// a pass returns fewer than `slice` rows it has reached the partition's end and
// the cursor wraps to the start. Per-tx recompute is O(that tx's spends), so the
// whole pass is O(slice + spends of those slice txs) — never the full chain.
//
// For each tx in the slice it recomputes the TRUE spent bitmap directly from the
// spends table using the SAME spendable predicate the fold uses
// (prev_output_idx < out_count AND get_bit(out_spendables, prev_output_idx)=1):
//
//   - true_bits       = byte-level mask of that tx's spendable-output spends,
//     width-stable at (out_count+7)/8 bytes (all-zero when none)
//   - true_last_spend = max(spent_at_height) over those spends
//
// Where the stored bitmap (or last_spend_height) differs it CORRECTS it, and —
// mirroring the fold's stamp — if the correction makes the bits complete
// (bit_count(true_bits) = spendable_count) on a mined, not-yet-stamped,
// unpreserved (unmined_since IS NULL) tx it also stamps delete_at_height =
// GREATEST(true_last_spend, mined_at_height)+1+retention and upserts it into
// pending_deletes (catching a previously-missed completion). It respects the
// same mined/preserve/unmined gates as the fold, so an unmined fully-spent tx is
// corrected but left unstamped (the mine path owns that stamp).
//
// It also UN-STAMPS in the reverse direction (defense-in-depth): a tx carrying a
// delete_at_height while it is NOT actually fully spent (bit_count(true_bits) <
// spendable_count) has that premature/stale stamp CLEARED and is removed from
// pending_deletes — otherwise the pruner (which deletes purely on the stamp)
// would delete a tx with a live UTXO. Legitimate not-fully-spent stamps are
// preserved: conflicting (double-spend loser) and preserved (preserve_until) txs
// are excluded, as is the zero-spendable case (spendable_count = 0).
//
// Because it REPLACES the bitmap with the recompute rather than OR-ing a delta,
// wrongly-set AND wrongly-clear bits are both fixed in one pass regardless of
// how they arose.
//
// safeTip bounds spends to spent_at_height <= safeTip, matching the fold's safe
// window (no spend still mid-commit is counted). retention is the same block-height
// retention the fold uses. Returns the number of rows whose stored values were
// corrected (drift found). The pass runs in one autocommit statement per
// partition; it is a background/maintenance path (never on the hot path).
//
// Serialization against the sweep: the audit statement opens with a `gate` CTE
// that takes pg_try_advisory_xact_lock(20240684 + partition) — the SAME key the
// sweep proc takes per band. Because the query is one autocommit statement, the
// lock is held for exactly its duration, so the reconciler and the sweep can
// never hold intersecting locks on the same partition's txs rows (the deadlock
// this closes). When the sweep already holds the partition the gate is closed,
// every data-modifying CTE downstream (which all derive from the now-empty
// `slice_txs`) becomes a no-op, and the call returns skipped=true with
// corrected=0 WITHOUT advancing or wrapping the rotating cursor: a naive skip
// that returned seen=0 would look identical to "partition exhausted" to the
// wrap logic below and would reset the rotation to the head on every contended
// tick, starving the tail of the partition forever. Skipping the cursor update
// entirely means the next tick simply retries the same slice.
func (s *Store) reconcileSpentBitsPartition(ctx context.Context, partition int, safeTip int64, retention int32, slice int) (int64, bool, error) {
	if slice <= 0 {
		slice = 1000
	}

	suffix := fmt.Sprintf("%02d", partition)

	// Resume from the per-partition cursor (NULL/absent → start of partition).
	// pgx.ErrNoRows is the expected "no cursor yet" case and leaves cursor nil; a
	// real DB error is logged (it silently discards rotation progress and restarts
	// the slice from the top otherwise, wasting a pass with no explanation).
	var cursor []byte
	if err := s.maint().QueryRow(ctx,
		`SELECT last_hash FROM dah_reconcile_cursor WHERE partition = $1`, partition).Scan(&cursor); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		s.logger.Warnf("[dahReconcile] partition %d cursor read failed; restarting slice from partition start: %v", partition, err)
		cursor = nil
	}

	// One statement:
	//  1. `slice_txs` picks up to `slice` candidate txs from THIS partition,
	//     hash > cursor (rotating), ordered by hash (stable, index-friendly on PK).
	//  2. the shared audit chain (dahReconcileAuditChain) recomputes true_bits +
	//     true_last_spend from that partition's spends for exactly those candidate
	//     hashes (O(their spends)) and corrects the drifted rows.
	//  3. the final SELECT returns (corrected_count, max_hash_seen) so Go can both
	//     report drift and advance/wrap the cursor.
	query := fmt.Sprintf(`
		WITH gate AS (
			SELECT pg_try_advisory_xact_lock(20240684 + $5::int) AS ok
		),
		slice_txs AS (
			SELECT hash, spendable_count, mined_at_height, delete_at_height,
			       spent_bits, last_spend_height, preserve_until, unmined_since,
			       out_count, out_spendables, conflicting
			  FROM txs_p%[1]s
			 WHERE (SELECT ok FROM gate)
			   AND ($1::bytea IS NULL OR hash > $1::bytea)
			 ORDER BY hash
			 LIMIT $2
		),`+dahReconcileAuditChain+`
		SELECT (SELECT ok FROM gate),
		       (SELECT count(*) FROM upd),
		       -- ORDER BY + LIMIT, not min()/max(): min/max over bytea only exist in
		       -- PostgreSQL 17+, and this store's floor is 14 (CI runs 16). Same
		       -- semantics incl. NULL on empty.
		       (SELECT hash FROM upd ORDER BY hash LIMIT 1),
		       (SELECT hash FROM slice_txs ORDER BY hash DESC LIMIT 1),
		       (SELECT count(*) FROM slice_txs)
	`, suffix, "$3", "$4")

	var locked bool
	var corrected int64
	var sampleCorrectedHash []byte
	var maxHash []byte
	var seen int64
	if err := s.maint().QueryRow(ctx, query, cursor, slice, safeTip, retention, partition).
		Scan(&locked, &corrected, &sampleCorrectedHash, &maxHash, &seen); err != nil {
		return 0, false, errors.NewStorageError("[dahReconcile] partition %d audit pass", partition, err)
	}

	// Sweep holds this partition right now: skip WITHOUT touching the rotation
	// cursor (a naive skip returns seen=0, which the wrap logic below would treat
	// as "partition exhausted" and reset the rotation to the head forever).
	if !locked {
		return 0, true, nil
	}

	// Advance the cursor. If fewer than `slice` rows were seen the partition is
	// exhausted → wrap to the start (NULL) so the next pass re-scans from the top.
	var next []byte
	if seen >= int64(slice) && maxHash != nil {
		next = maxHash
	} // else next stays nil → wrap

	if _, err := s.maint().Exec(ctx,
		`INSERT INTO dah_reconcile_cursor (partition, last_hash) VALUES ($1, $2)
		 ON CONFLICT (partition) DO UPDATE SET last_hash = EXCLUDED.last_hash`,
		partition, next,
	); err != nil {
		return corrected, false, errors.NewStorageError("[dahReconcile] partition %d advance cursor", partition, err)
	}

	if corrected > 0 {
		// Log the drift with a small sample (one actually-corrected hash) so a persistent
		// bitmap bug is visible in ops without dumping every row.
		sample := ""
		if sampleCorrectedHash != nil {
			sample = hex.EncodeToString(sampleCorrectedHash)
		}
		s.logger.Warnf("[dahReconcile] partition %d corrected %d drifted spent_bits row(s) (slice=%d, sample hash≈%s)",
			partition, corrected, slice, sample)
		prometheusDAHReconcileCorrected.Add(float64(corrected))
	}

	return corrected, false, nil
}

// drainDirtyParentsPartition drains up to `limit` rows of the dah_dirty_parents
// heal queue for one partition and recomputes those parents' spent_bits /
// last_spend_height from spends with a FRESH snapshot, correcting, stamping,
// un-stamping and purging pending_deletes exactly like the rotating audit (same
// shared SQL chain). Unspend enqueues a parent here in the same transaction as
// its spend-row delete + bit clear, so the fold-vs-Unspend stale-snapshot race
// (a fold statement whose band snapshot predates a concurrent Unspend commit
// re-sets a just-cleared bit) is healed within ~one reconcile tick — the
// rotating audit alone rotates far too slowly to be the safety story for
// wrongly-full bits.
//
// Ordering is the whole correctness argument, so it is spelled out:
//
// The drain is ONE transaction of TWO statements: (1) DELETE the queue rows
// RETURNING their hashes, then (2) recompute + correct those parents. Under
// READ COMMITTED each statement takes its OWN snapshot, so the recompute's
// snapshot is taken strictly AFTER the delete executed. That order closes the
// lost-re-dirty race:
//
//   - An Unspend that commits BEFORE statement (2)'s snapshot is simply SEEN by
//     the recompute — its cleared bits are ground truth here.
//   - An Unspend that commits after that snapshot re-enqueues the parent: its
//     INSERT ... ON CONFLICT DO NOTHING either finds no queue row (ours is
//     already deleted-and-committed → plain insert) or finds our
//     deleted-but-uncommitted row, blocks on our transaction in the speculative
//     -insert conflict check, and — once our delete COMMITS — no longer
//     conflicts with the dead tuple, so it INSERTS a fresh row. Either way the
//     parent is back in the queue and the next tick heals it with a snapshot
//     that does see the Unspend.
//
// Deleting LAST (after the recompute) would be wrong: an Unspend committing
// between the recompute's snapshot and the delete would DO-NOTHING against the
// still-live queue row, our delete would then eat it, and the re-dirty — which
// our stale recompute did not see — would be lost forever. A single-statement
// CTE (DELETE ... RETURNING feeding the recompute) has the same hole in
// miniature: all CTEs share ONE snapshot taken before the DELETE executes, so
// an Unspend committing in that gap is both invisible to the recompute and
// swallowed by the delete. Two statements, delete first, is the only shape
// where every Unspend is either seen or re-queued.
//
// Residual hazard (accepted, self-healing): our lock order is queue row →
// parent txs row, while a concurrent Unspend of a drained parent holds the txs
// row and then touches the queue — a 40P01 deadlock is possible. Postgres
// aborts one side; if it is us the whole transaction (including the queue
// delete) rolls back, so no queue row is lost and the next tick retries. Rare:
// it needs an Unspend of that exact parent mid-drain (reorg path).
//
// It takes the same per-partition advisory lock as the sweep and the rotating
// audit (pg_try_advisory_xact_lock, held to commit). When the sweep holds the
// partition it returns skipped=true having touched nothing — the queue rows
// stay put for the next tick.
func (s *Store) drainDirtyParentsPartition(ctx context.Context, partition int, safeTip int64, retention int32, limit int) (int64, int64, bool, error) {
	if limit <= 0 {
		limit = 1000
	}

	suffix := fmt.Sprintf("%02d", partition)

	tx, err := s.maint().Begin(ctx)
	if err != nil {
		return 0, 0, false, errors.NewStorageError("[dahReconcile] partition %d dirty drain begin", partition, err)
	}

	// Rollback is a no-op after a successful Commit; on any error path it also
	// restores the drained queue rows (nothing is lost on failure).
	defer func() { _ = tx.Rollback(ctx) }()

	var locked bool
	if err = tx.QueryRow(ctx,
		`SELECT pg_try_advisory_xact_lock(20240684 + $1::int)`, partition).Scan(&locked); err != nil {
		return 0, 0, false, errors.NewStorageError("[dahReconcile] partition %d dirty drain gate", partition, err)
	}

	if !locked {
		return 0, 0, true, nil
	}

	// Statement (1): delete the queue rows FIRST (see the ordering comment above).
	// Oldest first so a persistently re-dirtied parent cannot starve the rest.
	rows, err := tx.Query(ctx, `
		DELETE FROM dah_dirty_parents
		 WHERE hash IN (
		     SELECT hash FROM dah_dirty_parents
		      WHERE partition = $1
		      ORDER BY enqueued_at
		      LIMIT $2
		 )
		RETURNING hash`, partition, limit)
	if err != nil {
		return 0, 0, false, errors.NewStorageError("[dahReconcile] partition %d dirty drain dequeue", partition, err)
	}

	var hashes [][]byte

	for rows.Next() {
		var h []byte
		if err = rows.Scan(&h); err != nil {
			rows.Close()
			return 0, 0, false, errors.NewStorageError("[dahReconcile] partition %d dirty drain scan", partition, err)
		}

		hashes = append(hashes, h)
	}

	rows.Close()

	if err = rows.Err(); err != nil {
		return 0, 0, false, errors.NewStorageError("[dahReconcile] partition %d dirty drain dequeue", partition, err)
	}

	if len(hashes) == 0 {
		return 0, 0, false, nil // empty queue; defer rollback releases the gate
	}

	// Statement (2): recompute + correct with a snapshot taken AFTER the delete.
	// Same audit chain as the rotating slice; a drained hash whose txs row is
	// already pruned simply drops out of slice_txs.
	query := fmt.Sprintf(`
		WITH slice_txs AS (
			SELECT hash, spendable_count, mined_at_height, delete_at_height,
			       spent_bits, last_spend_height, preserve_until, unmined_since,
			       out_count, out_spendables, conflicting
			  FROM txs_p%[1]s
			 WHERE hash = ANY($1::bytea[])
		),`+dahReconcileAuditChain+`
		SELECT (SELECT count(*) FROM upd)
	`, suffix, "$2", "$3")

	var corrected int64
	if err = tx.QueryRow(ctx, query, hashes, safeTip, retention).Scan(&corrected); err != nil {
		return 0, 0, false, errors.NewStorageError("[dahReconcile] partition %d dirty drain recompute", partition, err)
	}

	if err = tx.Commit(ctx); err != nil {
		return 0, 0, false, errors.NewStorageError("[dahReconcile] partition %d dirty drain commit", partition, err)
	}

	drained := int64(len(hashes))
	prometheusDAHDirtyParentsDrained.Add(float64(drained))

	if corrected > 0 {
		prometheusDAHReconcileCorrected.Add(float64(corrected))
	}

	s.logger.Infof("[dahReconcile] partition %d drained %d dirty parent(s), corrected %d", partition, drained, corrected)

	return drained, corrected, false, nil
}

// reconcileAllPartitionsOnce runs one bounded reconciliation pass across every
// partition and returns the total rows corrected. Per partition it FIRST drains
// the dirty-parents heal queue (the fast path for the fold-vs-Unspend race —
// bounded by Unspend volume, ≈ zero outside reorgs) and then runs the rotating
// -cursor slice audit (the slow full-coverage backstop). It is the
// maintenance-loop entry point; per-partition errors are logged and skipped
// (best-effort backstop, never fatal).
func (s *Store) reconcileAllPartitionsOnce(ctx context.Context, safeTip int64, retention int32, slice int) int64 {
	var total int64

	for p := 0; p < numPartitions; p++ {
		if ctx.Err() != nil {
			return total
		}

		// Dirty-parents fast drain first: these are KNOWN-suspect parents (an
		// Unspend flagged them), so they must not wait for the rotation to come
		// around. A skip (sweep holds the partition) loses nothing — the queue
		// rows stay put for the next tick.
		_, dirtyCorrected, dirtySkipped, err := s.drainDirtyParentsPartition(ctx, p, safeTip, retention, slice)
		if err != nil {
			if ctx.Err() == nil {
				s.logger.Warnf("[dahReconcile] partition %d dirty drain error (retry next tick): %v", p, err)
			}
		} else if !dirtySkipped {
			total += dirtyCorrected
		}

		if ctx.Err() != nil {
			return total
		}

		n, skipped, err := s.reconcileSpentBitsPartition(ctx, p, safeTip, retention, slice)
		if err != nil {
			if ctx.Err() == nil {
				s.logger.Warnf("[dahReconcile] partition %d pass error (retry next tick): %v", p, err)
			}

			continue
		}

		if skipped {
			continue // sweep holds the partition; retry same slice next tick
		}

		total += n
	}

	return total
}

// runDAHReconcile is the background reconciliation loop. It is intentionally SLOW
// and OFF the hot path: one dirty-queue drain + one bounded slice per partition
// per tick, on a long idle interval, so it heals flagged parents within ~a tick
// and rotates through all history over time at negligible cost. Driven off the
// same pruner service lifecycle as the sweep cursor.
func (s *postgresPrunerService) runDAHReconcile(ctx context.Context) {
	cfg := s.store.settings.UtxoStore

	interval := time.Duration(cfg.PostgresDAHReconcileIntervalMillis) * time.Millisecond
	if interval <= 0 {
		interval = 60 * time.Second
	}

	slice := cfg.PostgresDAHReconcileSlice
	if slice <= 0 {
		slice = 1000
	}

	lag := int64(cfg.PostgresDAHSweepLag)
	if lag <= 0 {
		lag = 2
	}

	retention := int32(s.store.settings.GetUtxoStoreBlockHeightRetention()) //nolint:gosec // small positive height delta

	s.store.logger.Infof("[dahReconcile] backstop active (slice=%d interval=%s)", slice, interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			safeTip := s.store.dahSafeTip(lag)
			if safeTip <= 0 {
				continue
			}

			corrected := s.store.reconcileAllPartitionsOnce(ctx, safeTip, retention, slice)
			if corrected > 0 {
				s.store.logger.Warnf("[dahReconcile] pass corrected %d drifted spent_bits row(s)", corrected)
			}
		}
	}
}
