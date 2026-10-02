package postgres

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/jackc/pgx/v5/pgconn"
)

// dahSweepProcVersion is bumped whenever dahSweepProcDDL changes. bootstrap
// recreates the procedure whenever the version stored in dah_sweep_control
// differs (in EITHER direction, so a binary rollback reinstalls the older body).
//
// Bumped 9→10: row-targeted batching (LIMIT batch_rows on the stamped set,
// loop-until-short-pass, single watermark advance) replaces the v_step height-window loop.
//
// Bumped 10→11: fold-forward consumer. v10's fatal flaw was that every CALL
// re-aggregated each candidate parent's ENTIRE spend history (the spend_agg CTE
// joined spends_pNN over the whole (watermark, safe_tip] range) — O(lifetime
// spends), which grows unbounded with the chain and cannot keep up with IBD,
// freezing the watermark. v11 folds only NEW spends, once, forward-only: per
// bounded height band it aggregates ONLY that band's spends, increments each
// parent's spent_progress counter, and stamps delete_at_height when the counter
// reaches spendable_count. Cost per band is O(new spends in band), independent of
// chain size, so the watermark always advances.
//
// Bumped 11→12: merge the per-band FOLD and STAMP into a SINGLE UPDATE ...
// RETURNING. v11 wrote the hot txs table TWICE per band — once to fold
// (spent_progress += n, last_spend_height) and again to stamp delete_at_height —
// and scanned the band twice. For txs that complete on the folded spend this
// doubled txs dead-tuple churn on the hottest table (~15% measured throughput
// regression). v12 stamps INLINE from the NEW spent_progress in the same UPDATE,
// so each folded tx is written once and the band is scanned once. Semantics are
// identical: same rows stamped, same delete_at_height values, same pending_deletes
// upserts.
//
// Bumped 12→13: authorise the stamp from GROUND TRUTH, not the counter. The
// maintained spent_progress can drift UPWARD — a reorg rewinds the watermark and the
// forward-only fold re-counts still-present spends (also plain arithmetic/lost-update
// drift). Trusting the counter stamped, and the pruner then irreversibly cascade-
// deleted, a tx whose output was still an unspent UTXO (the IBD data-loss wedge,
// mainnet h63266 / testnet …5e5ea, 2026-07-02). v13 keeps the counter as a cheap gate
// (spent_progress + d.n >= spendable_count) but confirms full-spend with a bounded
// recount of the tx's spendable spends (up to the band ceiling) before stamping, so a
// drifted counter can no longer schedule a live UTXO for deletion. The pending_deletes
// feed now keys off delete_at_height IS NOT NULL alone (the old spent_progress =
// spendable_count filter would wrongly drop a row the recount stamped after an
// overshoot). The counter itself is healed by the reconcile backstop.
//
// Bumped 13→14: delete the in-proc lock_timeout; work is bounded by units, never
// clocks. Three production incidents (2026-07-01, 07-02, 07-04→06) traced to the
// same execution-model flaw: wall-clock limits on healthy work. At dense chain
// regions one band's fold legitimately runs for many minutes; the Go client's
// 120s CALL timeout cancelled every CALL inside its first band → rollback →
// retry the SAME band forever → 36h zero-progress treadmill at INFO level →
// 343GB of unpruned rows → disk full → postgres crash. Raising the timeout then
// exposed the next clock: lock_timeout='5s' aborts an entire band (minutes of
// disk-bound work) because ONE row was contended for five seconds — and the
// contended rows were the reconciler's, whose whole job is touching the same
// drifted parents the fold re-touches (deadlock class 40P01). v14's position:
//   - A band takes as long as it takes. Interruption safety comes from the
//     per-band atomic COMMIT (fold+stamp+watermark), not from deadlines — any
//     interruption costs at most one band.
//   - Row-lock waits are waited out: hot-path writers hold row locks for
//     milliseconds; anything longer is a wedge, and a wedge is an ALERTING
//     problem (the Go-side stagnation monitor), not a transaction-abort problem.
//   - The DAH writers themselves never contend: the reconciler takes this
//     procedure's per-partition advisory lock (gate CTE), so fold and audit are
//     mutually exclusive per partition by construction.
//
// The Go driver mirrors this: no per-CALL context timeout (deleted), per-
// partition loops with idle-interval backoff on errors, and clock-immunity GUCs
// (statement_timeout=0 etc.) pinned on the maintenance pool — a server-level
// statement_timeout armed at CALL start cannot be defused by SET LOCAL inside
// the procedure. The only remaining "timeout" is the stagnation monitor
// (dah_stagnation.go): watermark frozen + backlog > 0 → Warnf → Errorf + gauge.
// It cancels nothing; a human decides. See TUNING.md "Execution model (v14)".
//
// Bumped 14→15: idempotent spent-bitmap fold; the stamp-time ground-truth
// recount is DELETED. v13's recount was the O(lifetime-spends) cost class
// reintroduced at stamp time: ~K random COLD heap reads per completing parent
// (spends are append-ordered by time while prev_tx_hash is random — a parent
// spent over years has K rows on ~K distinct pages), re-fired on EVERY later
// band for counter-overshoot parents. On mainnet it could not keep pace with
// IBD (2026-07-07 disk-full incident: watermarks froze hours BEFORE the disk
// filled). The recount existed only because the arithmetic spent_progress
// counter drifts; v15 replaces the counter with a per-output spent bitmap
// (txs.spent_bits) whose fold is idempotent by construction — OR-ing the same
// spend's bit twice is a no-op, so every counter-drift class (reorg
// watermark-rewind re-fold, reconciler double-cover, Unspend of never-folded
// spends) is structurally harmless — and the stamp gate becomes a single-row
// check: bit_count(spent_bits) = spendable_count. No spends-history read
// exists anywhere in the stamp path. The one residual hazard (a fold statement
// whose band snapshot predates a concurrent Unspend commit can re-set a
// just-cleared bit) is healed within ~one reconcile tick via the
// dah_dirty_parents queue Unspend populates transactionally.
//
// v15 also re-quantises bands by WORK, not heights: the fold source is capped
// at band_rows spends (ORDER BY spent_at_height LIMIT band_rows) and the
// watermark advances to the last FULLY-covered height (max_h - 1), re-folding
// the boundary height next band — legal only because duplicate ORs are free.
// A single height holding >= band_rows spends alone is full-drained as one
// band (bounded overshoot of one block). This kills the dense-region
// multi-hour, temp-spilling folds (band_heights=5000 could mean millions of
// rows); work_mem for the fold is pinned per band from the work_mem_mb knob.
// The watermark advance is a CAS (WHERE last_swept_height = v_from): a
// concurrent reorg rewind mid-CALL rolls the band back and ends the CALL so
// the rewound range is always re-folded.
//
// Bumped 15→16: single-byte fast path in the fold. Profiling the pruned bench
// showed the CALL at 12.3% of DB time while ~100% of its parents' band spends
// land in ONE mask byte (any <=8-output tx, any single-spend fold). Those
// parents now take an in-place set_byte OR (upd_fast) and skip the gap-filled
// string_agg/LATERAL mask machinery entirely; multi-byte parents keep the
// general path (upd_slow). Semantics identical — OR is idempotent and
// position-exact — and a width-guard routes any short spent_bits row to
// "unchanged, no stamp" (reconciler heals) instead of a band-aborting error.
//
// Bumped 16→17: below-checkpoint early DAH. A new p_checkpoint BIGINT parameter
// (0 = off) lets the stamp skip the retention wait for a tx whose mined height AND
// every folded spend height are at-or-below a hardcoded consensus checkpoint,
// where a reorg is impossible by rule. Both stamp sites (upd_fast/upd_slow) gate
// the retention term on the SAME GREATEST(last_spend, folded max_h, mined) the
// stamp uses: delete_at_height = GREATEST(..) + 1 + CASE WHEN GREATEST(..) <=
// p_checkpoint THEN 0 ELSE retention END. Keying the CASE on the same GREATEST is
// the load-bearing safety invariant — a row qualifies for the immediate stamp only
// when nothing about it (mine or any spend) sits above the boundary. The idempotent
// bitmap fold is untouched. The sanctioned zero-spendable inline stamp in mined.go
// is deliberately NOT changed: it keeps full retention (it never runs the sweep's
// per-band boundary logic and its all-OP_RETURN txs are cheap to retain).
const dahSweepProcVersion = 17

// dahSweepControlDDL is the kill-switch / tunables / proc-version / per-CALL
// outcome table. Created unconditionally in createSchemaInternal (plain DDL);
// the procedure itself is bootstrapped separately and only in proc mode.
const dahSweepControlDDL = `
CREATE TABLE IF NOT EXISTS dah_sweep_control (
    id                   INT  PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    enabled              BOOL        NOT NULL DEFAULT TRUE,
    proc_version         INT         NOT NULL DEFAULT 0,
    batch_rows           INT         NOT NULL DEFAULT 5000,
    max_windows_per_call INT         NOT NULL DEFAULT 32,
    band_heights         INT         NOT NULL DEFAULT 5000,
    last_called_at       TIMESTAMPTZ,
    last_rows_stamped    BIGINT      NOT NULL DEFAULT 0,
    last_candidates_seen BIGINT      NOT NULL DEFAULT 0,
    last_watermark       BIGINT      NOT NULL DEFAULT 0,
    total_rows_stamped   BIGINT      NOT NULL DEFAULT 0,
    last_caught_up       BOOL        NOT NULL DEFAULT FALSE,
    last_backlog         BIGINT      NOT NULL DEFAULT 0,
    last_hit_budget      BOOL        NOT NULL DEFAULT FALSE,
    last_lock_contended  BOOL        NOT NULL DEFAULT FALSE,
    last_error           TEXT
);
-- band_heights was added in proc v11 (fold-forward). Idempotent ALTER so existing
-- DBs get the column without a manual migration.
ALTER TABLE dah_sweep_control ADD COLUMN IF NOT EXISTS band_heights INT NOT NULL DEFAULT 5000;
-- band_rows (v15): the band work quantum in SPEND ROWS — the fold source is
-- capped at this many spends per band regardless of chain density. band_heights
-- becomes the max-heights cap for sparse regions.
ALTER TABLE dah_sweep_control ADD COLUMN IF NOT EXISTS band_rows INT NOT NULL DEFAULT 200000;
-- work_mem_mb (v15): per-band SET LOCAL work_mem for the fold aggregation, so a
-- band sorts/hashes in memory instead of spilling to pgsql_tmp (the first
-- ENOSPC casualty in the 2026-07-01 and 07-07 incidents). Safe to raise: the
-- maintenance pool bounds how many CALLs run concurrently.
ALTER TABLE dah_sweep_control ADD COLUMN IF NOT EXISTS work_mem_mb INT NOT NULL DEFAULT 256;`

// dahSweepProcDDL returns the server-side self-committing DAH stamp procedure DDL.
// It reproduces the exact DAH semantics of the in-process Go sweep
// (sweepDAHRangePartition) but commits per fully-drained height window, needs no
// statement_timeout, and is driven by a thin adaptive ticker via CALL. See
// docs/superpowers/dah-stored-proc-proposal.md for the full design.
//
// The procedure always upserts each stamped row's (hash, delete_at_height) into the
// per-leaf pending_deletes_pNN table inside the same EXECUTE block, keeping the
// side-table — the only pruner path — in sync without extra round-trips.
func dahSweepProcDDL() string {
	return dahSweepProcDDLWithPendingDeletes
}

// dahSweepProcDDLWithPendingDeletes is the spent-bitmap fold DAH sweep procedure
// (installed at proc version dahSweepProcVersion).
//
// v11 replaced v10's full-range re-aggregation with a forward-only fold. v12
// merged fold + stamp into one UPDATE ... RETURNING. v13 added a stamp-time
// ground-truth recount (drift-prone counter). v14 dropped in-proc lock_timeout.
// v15 replaces the counter with the idempotent spent_bits bitmap and DELETES the
// recount — no spends-history read exists anywhere in this procedure. Per band:
//
//  1. BAND SOURCE (work-quantised): the band is the next band_rows spends above
//     the watermark (ORDER BY spent_at_height LIMIT band_rows), capped at
//     band_heights heights for sparse regions. O(new spends in band) with a
//     row-bounded constant, regardless of chain density.
//  2. FOLD + STAMP in ONE UPDATE: aggregate the band's SPENDABLE spends into a
//     per-parent byte mask (byte i/8 gets bit 1<<(i%8) — the same LSB-first
//     encoding as out_spendables/get_bit), OR it into txs.spent_bits with a
//     multi-column SET row-subquery (the OR is evaluated once), advance
//     last_spend_height, and stamp delete_at_height inline when
//     bit_count(new_bits) = spendable_count AND mined_at_height IS NOT NULL AND
//     delete_at_height IS NULL AND preserve_until IS NULL AND unmined_since IS
//     NULL. delete_at_height = GREATEST(new last_spend_height, mined_at_height)
//     + 1 + retention. Every freshly stamped row is upserted into
//     pending_deletes_pNN in the same statement. A parent stamped in a PRIOR
//     band keeps its stamp (delete_at_height IS NULL guard); its pending_deletes
//     re-upsert rewrites the unchanged value — harmless. Duplicate folds are
//     no-ops by construction (OR), so re-processing after a watermark rewind or
//     a boundary-height re-fold cannot drift anything.
//  3. ADVANCE the watermark with a CAS (WHERE last_swept_height = v_from): if
//     the band was row-truncated the advance target is max_h - 1 (the boundary
//     height re-folds next band); a single height holding >= band_rows spends
//     alone is re-run as one full height (bounded overshoot of one block). A
//     CAS miss means a reorg rewound the watermark mid-CALL: the band ROLLS
//     BACK and the CALL exits so the next CALL restarts from the rewound
//     height. Fold + stamp + advance are ONE transaction per band.
//
// The loop runs at most v_max_bands (dah_sweep_control.max_windows_per_call)
// bands per CALL; work_mem is pinned per band from work_mem_mb so the fold
// aggregation does not spill to pgsql_tmp at sane band sizes.
//
// Division of labour: a tx spent-before-mined has its bits folded here but is
// NOT stamped (mined gate) — the mine path (SetMinedMulti) stamps it at mine
// from the same single-row bitmap gate. The sweep only stamps the
// mined-then-spent completion. The dirty-parents queue (dah_dirty_parents,
// populated transactionally by Unspend) plus the rotating rotating-slice audit
// (dah_reconcile.go) heal the one residual stale-snapshot hazard and any
// unforeseen bit corruption; the reconciler recomputes spent_bits from spends
// over bounded slices and un-stamps wrong stamps.
const dahSweepProcDDLWithPendingDeletes = `CREATE OR REPLACE PROCEDURE dah_sweep_batch(
    p_partition  INT,
    p_safe_tip   BIGINT,
    p_retention  INT,
    p_checkpoint BIGINT DEFAULT 0
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_leaf_suffix    TEXT := lpad(p_partition::text, 2, '0');
    v_from           BIGINT;
    v_cap            BIGINT;
    v_to             BIGINT;
    v_band           INT;
    v_band_rows      INT;
    v_work_mem_mb    INT;
    v_max_bands      INT;
    v_enabled        BOOLEAN;
    v_n              BIGINT;
    v_rows           BIGINT;
    v_max_h          BIGINT;
    v_nolimit        INT := NULL;
    v_total_stamped  BIGINT := 0;
    v_bands_done     INT := 0;
    v_sql            TEXT;
BEGIN
    SELECT enabled, band_heights, band_rows, work_mem_mb, max_windows_per_call
      INTO v_enabled, v_band, v_band_rows, v_work_mem_mb, v_max_bands
      FROM dah_sweep_control WHERE id = 1;

    IF NOT v_enabled THEN
        RETURN;
    END IF;

    IF v_band IS NULL OR v_band <= 0 THEN
        v_band := 5000;
    END IF;

    IF v_band_rows IS NULL OR v_band_rows <= 0 THEN
        v_band_rows := 200000;
    END IF;

    IF v_max_bands IS NULL OR v_max_bands <= 0 THEN
        v_max_bands := 8;
    END IF;

    IF current_setting('server_version_num')::int < 140000 THEN
        RAISE EXCEPTION 'dah_sweep_batch requires PostgreSQL 14+ (bit_count on bytea)';
    END IF;

    SELECT last_swept_height INTO v_from
      FROM dah_part_watermark WHERE partition = p_partition;

    IF v_from IS NULL OR v_from >= p_safe_tip THEN
        COMMIT;
        RETURN;
    END IF;

    -- The fold+stamp statement, built once per CALL (the leaf suffix is fixed);
    -- both the row-capped band pass and the single-height full-drain pass
    -- EXECUTE it. See the in-loop comment for the CTE-by-CTE walkthrough.
    v_sql := format($q$
            WITH band AS (
                SELECT s.prev_tx_hash, s.prev_output_idx, s.spent_at_height
                  FROM spends_p%1$s s
                 WHERE s.spent_at_height > $1 AND s.spent_at_height <= $2
                 ORDER BY s.spent_at_height
                 LIMIT $4
            ),
            band_stats AS (
                SELECT count(*) AS n_rows, max(spent_at_height) AS max_h FROM band
            ),
            byte_agg AS (
                SELECT b.prev_tx_hash AS hash,
                       b.prev_output_idx / 8 AS byte_idx,
                       bit_or(1 << (b.prev_output_idx %% 8)) AS byte_val,
                       max(b.spent_at_height) AS max_h
                  FROM band b
                  JOIN txs_p%1$s t ON t.hash = b.prev_tx_hash
                 WHERE CASE WHEN b.prev_output_idx < t.out_count
                            THEN get_bit(t.out_spendables, b.prev_output_idx) = 1
                            ELSE false END
                 GROUP BY b.prev_tx_hash, b.prev_output_idx / 8
            ),
            fanout AS (
                SELECT hash, count(*) AS n_bytes, min(byte_idx) AS b_idx, max(max_h) AS max_h
                  FROM byte_agg GROUP BY hash
            ),
            -- FAST PATH (v16): a parent whose band spends all land in ONE mask byte
            -- (every <=8-output tx, and any single-spend fold — the overwhelming
            -- majority) needs no gap-filled mask at all: OR the one byte in place
            -- with set_byte. Semantically identical to the general path because OR
            -- is idempotent and position-exact. The width guard routes a (should-
            -- not-exist) short spent_bits row to "unchanged bits, no stamp", which
            -- the reconciler heals — never an error that aborts the band.
            upd_fast AS (
                UPDATE txs_p%1$s t
                   SET (spent_bits, last_spend_height, delete_at_height) = (
                       SELECT x.nb,
                              GREATEST(COALESCE(t.last_spend_height, 0), f.max_h)::int,
                              CASE
                                  WHEN bit_count(x.nb) = t.spendable_count
                                       AND t.spendable_count > 0
                                       AND t.mined_at_height IS NOT NULL
                                       AND t.delete_at_height IS NULL
                                       AND t.preserve_until IS NULL
                                       AND t.unmined_since IS NULL
                                  THEN (GREATEST(GREATEST(COALESCE(t.last_spend_height, 0), f.max_h),
                                                 t.mined_at_height) + 1
                                        + CASE WHEN GREATEST(GREATEST(COALESCE(t.last_spend_height, 0), f.max_h),
                                                             t.mined_at_height) <= $5
                                               THEN 0 ELSE $3 END)::int
                                  ELSE t.delete_at_height
                              END
                         FROM (SELECT CASE WHEN f.b_idx < octet_length(t.spent_bits)
                                           THEN set_byte(t.spent_bits, f.b_idx,
                                                         get_byte(t.spent_bits, f.b_idx) | ba.byte_val)
                                           ELSE t.spent_bits
                                      END AS nb) x
                   )
                  FROM fanout f
                  JOIN byte_agg ba ON ba.hash = f.hash AND ba.byte_idx = f.b_idx
                 WHERE t.hash = f.hash
                   AND f.n_bytes = 1
                RETURNING t.hash, t.delete_at_height
            ),
            mask_agg AS (
                SELECT hb.hash,
                       decode(string_agg(lpad(to_hex(COALESCE(ba.byte_val, 0)), 2, '0'), '' ORDER BY gs.i), 'hex') AS mask,
                       hb.max_h
                  FROM (SELECT ba2.hash, max(ba2.byte_idx) AS max_byte, max(ba2.max_h) AS max_h
                          FROM byte_agg ba2
                          JOIN fanout f2 ON f2.hash = ba2.hash AND f2.n_bytes > 1
                         GROUP BY ba2.hash) hb
                 CROSS JOIN LATERAL generate_series(0, hb.max_byte) gs(i)
                  LEFT JOIN byte_agg ba ON ba.hash = hb.hash AND ba.byte_idx = gs.i
                 GROUP BY hb.hash, hb.max_h
            ),
            upd_slow AS (
                UPDATE txs_p%1$s t
                   SET (spent_bits, last_spend_height, delete_at_height) = (
                       SELECT COALESCE(x.nb, t.spent_bits),
                              GREATEST(COALESCE(t.last_spend_height, 0), d.max_h)::int,
                              CASE
                                  WHEN bit_count(COALESCE(x.nb, t.spent_bits)) = t.spendable_count
                                       AND t.spendable_count > 0
                                       AND t.mined_at_height IS NOT NULL
                                       AND t.delete_at_height IS NULL
                                       AND t.preserve_until IS NULL
                                       AND t.unmined_since IS NULL
                                  THEN (GREATEST(GREATEST(COALESCE(t.last_spend_height, 0), d.max_h),
                                                 t.mined_at_height) + 1
                                        + CASE WHEN GREATEST(GREATEST(COALESCE(t.last_spend_height, 0), d.max_h),
                                                             t.mined_at_height) <= $5
                                               THEN 0 ELSE $3 END)::int
                                  ELSE t.delete_at_height
                              END
                         FROM (SELECT (SELECT decode(string_agg(lpad(to_hex(
                                          CASE WHEN gi.i < octet_length(t.spent_bits) THEN get_byte(t.spent_bits, gi.i) ELSE 0 END
                                        | CASE WHEN gi.i < octet_length(d.mask)       THEN get_byte(d.mask, gi.i)       ELSE 0 END
                                       ), 2, '0'), '' ORDER BY gi.i), 'hex')
                                  FROM generate_series(0, GREATEST(octet_length(t.spent_bits), octet_length(d.mask)) - 1) gi(i)
                               ) AS nb) x
                   )
                  FROM mask_agg d
                 WHERE t.hash = d.hash
                RETURNING t.hash, t.delete_at_height
            ),
            upd AS (
                SELECT hash, delete_at_height FROM upd_fast
                UNION ALL
                SELECT hash, delete_at_height FROM upd_slow
            ),
            ins AS (
                INSERT INTO pending_deletes_p%1$s (hash, delete_at_height)
                SELECT hash, delete_at_height FROM upd
                 WHERE delete_at_height IS NOT NULL
                ON CONFLICT (hash) DO UPDATE SET delete_at_height = EXCLUDED.delete_at_height
            )
            SELECT (SELECT count(*) FROM upd WHERE delete_at_height IS NOT NULL),
                   (SELECT n_rows FROM band_stats),
                   (SELECT max_h  FROM band_stats)
    $q$, v_leaf_suffix);

    -- Fold forward one row-bounded band at a time. Each iteration processes at
    -- most band_rows spends and commits atomically, so per-band work is a fixed
    -- quantum regardless of chain density and per-CALL work is capped at
    -- v_max_bands bands.
    WHILE v_from < p_safe_tip AND v_bands_done < v_max_bands LOOP
        SELECT enabled INTO v_enabled FROM dah_sweep_control WHERE id = 1;
        EXIT WHEN NOT v_enabled;

        IF NOT pg_try_advisory_xact_lock(20240684 + p_partition) THEN
            EXIT;
        END IF;

        -- Pin the fold's sort/hash memory for THIS band's transaction so the
        -- aggregation stays in memory instead of spilling to pgsql_tmp (the
        -- first ENOSPC casualty in the 2026-07-01/07-07 incidents). Safe: the
        -- maintenance pool bounds concurrent CALLs.
        IF v_work_mem_mb IS NOT NULL AND v_work_mem_mb > 0 THEN
            EXECUTE format('SET LOCAL work_mem = %L', v_work_mem_mb::text || 'MB');
        END IF;

        v_cap := LEAST(v_from + v_band, p_safe_tip);

        -- (1+2) FOLD + STAMP in ONE UPDATE, no history read anywhere:
        --   band      — the next band_rows spends above the watermark (LIMIT NULL
        --               = no cap, used by the single-height full-drain re-run).
        --   byte_agg  — per (parent, byte index) integer bit mask of the band's
        --               SPENDABLE spends: byte i/8 gets bit 1<<(i%%8), matching the
        --               LSB-first get_bit encoding of out_spendables. Spendability
        --               test mirrors v10-v14 (prev_output_idx < out_count AND
        --               get_bit(out_spendables, idx) = 1).
        --   mask_agg  — per-parent BYTEA mask, gap bytes zero-filled set-wise
        --               (LATERAL series + LEFT JOIN — NOT a correlated subquery,
        --               which would rescan byte_agg per parent).
        --   upd       — OR the mask into spent_bits (bounds-guarded get_byte over
        --               the wider of the two; widths normally equal — create
        --               pre-sizes spent_bits to (out_count+7)/8), advance
        --               last_spend_height, and stamp inline when
        --               bit_count(new_bits) = spendable_count. The multi-column
        --               SET row-subquery evaluates the OR exactly once. The
        --               delete_at_height IS NULL guard keeps a prior band's stamp.
        --               Duplicate folds (watermark rewind, boundary re-fold) re-OR
        --               the same bits — no drift, by construction.
        --   ins       — freshly stamped rows feed pending_deletes, same statement.
        -- Returns (stamped, band rows scanned, max height covered).
        EXECUTE v_sql
        USING v_from, v_cap, p_retention, v_band_rows, p_checkpoint
        INTO v_n, v_rows, v_max_h;

        -- Band ceiling: if the row cap truncated the band, only heights strictly
        -- below max_h are fully covered — advance to max_h - 1 and re-fold the
        -- boundary height next band (duplicate ORs are no-ops). If a SINGLE
        -- height alone holds >= band_rows spends (max_h - 1 = v_from), re-run
        -- exactly that height with no row cap: bounded overshoot of one block.
        IF v_rows < v_band_rows THEN
            v_to := v_cap;
        ELSIF v_max_h - 1 > v_from THEN
            v_to := v_max_h - 1;
        ELSE
            EXECUTE v_sql
            USING v_from, v_max_h, p_retention, v_nolimit, p_checkpoint
            INTO v_n, v_rows, v_max_h;

            v_to := v_max_h;
        END IF;

        -- (3) CAS-ADVANCE the watermark, then COMMIT: fold + stamp + advance are
        -- one transaction for this band. A CAS miss means a reorg rewound the
        -- watermark mid-CALL (RewindDAHWatermark): roll the band back and end
        -- the CALL so the next CALL restarts from the rewound height and
        -- re-folds the new chain's spends (idempotent, so the surviving range
        -- re-ORs harmlessly).
        UPDATE dah_part_watermark
           SET last_swept_height = v_to
         WHERE partition = p_partition AND last_swept_height = v_from;

        IF NOT FOUND THEN
            ROLLBACK;
            EXIT;
        END IF;

        v_total_stamped := v_total_stamped + v_n;
        COMMIT;

        v_from := v_to;
        v_bands_done := v_bands_done + 1;
    END LOOP;

    UPDATE dah_sweep_control
       SET last_called_at = now(),
           total_rows_stamped = total_rows_stamped + v_total_stamped,
           last_rows_stamped = v_total_stamped
     WHERE id = 1;
    COMMIT;
END;
$$;`

// bootstrapDAHSweepProc seeds the control-row knobs from settings and installs
// dah_sweep_batch() when the stored proc_version differs from dahSweepProcVersion.
// The procedure is the ONLY DAH sweep mechanism (there is no in-process fallback),
// so it is mandatory: a missing CREATE privilege (SQLSTATE 42501) or a postgres
// older than 11 (COMMIT-inside-PROCEDURE) is returned as an error and fails store
// startup, surfacing the deployment problem instead of silently not pruning.
func (s *Store) bootstrapDAHSweepProc(ctx context.Context) (err error) {
	// PG version guard: COMMIT inside a PROCEDURE requires PostgreSQL 11+;
	// the spent-bitmap stamp gate additionally requires bit_count(bytea) (PostgreSQL 14+).
	var verNum int
	if err = s.pool.QueryRow(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&verNum); err != nil {
		return errors.NewStorageError("[dahSweep] read server_version_num", err)
	}

	if verNum < 140000 {
		return errors.NewStorageError("[dahSweep] postgres server_version_num=%d < 140000; the spent-bitmap DAH sweep procedure requires PostgreSQL 14+ (bit_count on bytea)", verNum)
	}

	// Fresh-sync guard: v15 replaced the spent_progress counter with the
	// spent_bits bitmap and there is deliberately NO migration — populating
	// spent_bits below the sweep watermark would need the O(all-history) pass
	// the fold design exists to avoid. A pre-v15 database (spent_progress
	// column present) must be dropped and re-synced; proc_version alone cannot
	// detect this (CREATE OR REPLACE would happily install v15 over a counter
	// schema and silently never stamp anything below the watermark).
	var hasCounterColumn bool
	if err = s.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.columns
		  WHERE table_schema = current_schema() AND table_name = 'txs' AND column_name = 'spent_progress')`,
	).Scan(&hasCounterColumn); err != nil {
		return errors.NewStorageError("[dahSweep] detect pre-v15 schema", err)
	}

	if hasCounterColumn {
		return errors.NewStorageError("[dahSweep] pre-v15 schema detected (txs.spent_progress exists): the v15 spent-bitmap store has no migration path; drop the database and re-sync")
	}

	// Fresh-sync guard (Phase B row diet): the mined-block info moved from three
	// parallel INT[] columns (block_ids/block_heights/subtree_idxs) into one
	// mined_info BYTEA of fixed 12-byte records, utxo_hashes narrowed to a 16-byte
	// prefix per output, and version/size_in_bytes narrowed to INTEGER — all in one
	// fresh-schema revision with deliberately NO in-place migration (re-packing every
	// row's arrays and halving every packed hash is an O(all-history) rewrite this
	// store avoids). CREATE TABLE IF NOT EXISTS is a no-op on an existing table, so an
	// old-layout database still carries block_ids and would silently mis-read every
	// mined_info access. Detect the old layout by the block_ids column and refuse to
	// start (same precedent as the pre-v15 guard above): drop and re-sync.
	var hasBlockIDsColumn bool
	if err = s.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.columns
		  WHERE table_schema = current_schema() AND table_name = 'txs' AND column_name = 'block_ids')`,
	).Scan(&hasBlockIDsColumn); err != nil {
		return errors.NewStorageError("[dahSweep] detect pre-Phase-B schema", err)
	}

	if hasBlockIDsColumn {
		return errors.NewStorageError("[dahSweep] pre-Phase-B schema detected (txs.block_ids exists): the packed mined_info / 16-byte utxo_hashes layout has no migration path; drop the database and re-sync")
	}

	// Seed tunable knobs from settings into the control row so ops tuning flows
	// through settings and the proc reads a single source of truth.
	maxWin := s.settings.UtxoStore.PostgresDAHSweepMaxWindowsPerCall
	if maxWin <= 0 {
		maxWin = 8
	}

	// band_heights caps the per-band fold width in HEIGHTS for sparse regions
	// (proc v11); band_rows is the v15 work quantum in SPEND ROWS. Both seeded
	// from settings so ops tuning flows through one source of truth.
	bandHeights := s.settings.UtxoStore.PostgresDAHSweepBandHeights
	if bandHeights <= 0 {
		bandHeights = 5000
	}

	bandRows := s.settings.UtxoStore.PostgresDAHSweepBandRows
	if bandRows <= 0 {
		bandRows = 200000
	}

	if _, err = s.pool.Exec(ctx,
		`UPDATE dah_sweep_control SET max_windows_per_call = $1, band_heights = $2, band_rows = $3 WHERE id = 1`,
		maxWin, bandHeights, bandRows,
	); err != nil {
		if isInsufficientPrivilege(err) {
			return errors.NewStorageError("[dahSweep] app role lacks privilege to write dah_sweep_control (42501); grant it so the DAH sweep can run")
		}

		return errors.NewStorageError("[dahSweep] seed control knobs", err)
	}

	// Recreate the procedure only when the stored version differs (either direction).
	var storedVersion int
	if err = s.pool.QueryRow(ctx, `SELECT proc_version FROM dah_sweep_control WHERE id = 1`).Scan(&storedVersion); err != nil {
		return errors.NewStorageError("[dahSweep] read proc_version", err)
	}

	if storedVersion == dahSweepProcVersion {
		return nil
	}

	// Drop the legacy single-CALL (BIGINT, INT) signature from the v1 era. The v2
	// procedure has a different signature (INT, BIGINT, INT), so CREATE OR REPLACE
	// would otherwise leave the old overload behind, unused. Best-effort.
	_, _ = s.pool.Exec(ctx, `DROP PROCEDURE IF EXISTS dah_sweep_batch(BIGINT, INT)`)

	// Drop the v16 three-arg signature (INT, BIGINT, INT). v17 adds p_checkpoint
	// with a DEFAULT, so leaving the old overload behind would make a 3-arg CALL
	// ambiguous. All callers now pass 4 args; best-effort.
	_, _ = s.pool.Exec(ctx, `DROP PROCEDURE IF EXISTS dah_sweep_batch(INT, BIGINT, INT)`)

	if _, err = s.pool.Exec(ctx, dahSweepProcDDL()); err != nil {
		if isInsufficientPrivilege(err) {
			return errors.NewStorageError("[dahSweep] app role lacks CREATE privilege for the dah_sweep_batch procedure (42501); grant CREATE so the DAH sweep can run")
		}

		return errors.NewStorageError("[dahSweep] create procedure", err)
	}

	if _, err = s.pool.Exec(ctx, `UPDATE dah_sweep_control SET proc_version = $1 WHERE id = 1`, dahSweepProcVersion); err != nil {
		return errors.NewStorageError("[dahSweep] record proc_version", err)
	}

	s.logger.Infof("[dahSweep] installed dah_sweep_batch procedure version %d", dahSweepProcVersion)

	return nil
}

// isInsufficientPrivilege reports whether err is a postgres insufficient_privilege
// (SQLSTATE 42501). Checked on the raw driver error before any teranode wrapping.
func isInsufficientPrivilege(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == usql.PgErrInsufficientPriv
}

// runDAHCursorProc drives the server-side dah_sweep_batch() procedure with one
// independent, forever-running loop PER PARTITION (runPartitionSweepLoop): each
// loop CALLs its own partition with NO per-CALL deadline, refires immediately
// while that partition has backlog, and idles otherwise. A shared semaphore
// bounds how many CALLs run concurrently (PostgresDAHSweepConcurrency) so a
// cold/contended disk is not thrashed by all partitions firing at once. One
// wedged partition stalls only itself (and whoever is waiting on the shared
// semaphore); the stagnation monitor (Task 5) is the thing that notices and
// names a stalled partition — this driver never times out a CALL.
//
// The spent-before-mined orphan gap (previously covered by the O(table) keyspace
// backstop) is now closed by the mine-time stamp (S6) in SetMinedMulti, so no
// backstop timer is needed here.
//
// A startup smoke CALL verifies COMMIT-inside-CALL is not suppressed by pgx
// middleware (2D000); there is no Go fallback, so it just logs the root cause.
func (s *postgresPrunerService) runDAHCursorProc(ctx context.Context) {
	cfg := s.store.settings.UtxoStore

	idleInterval := time.Duration(cfg.PostgresDAHSweepIdleIntervalMillis) * time.Millisecond
	if idleInterval <= 0 {
		idleInterval = 5 * time.Second
	}

	lag := int64(cfg.PostgresDAHSweepLag)
	if lag <= 0 {
		lag = 2
	}

	retention := int32(s.store.settings.GetUtxoStoreBlockHeightRetention()) //nolint:gosec // small positive height delta

	// Smoke CALL (partition 0, negative safe_tip → immediate no-op): verify
	// COMMIT-inside-CALL works. A wrapping transaction from pgx middleware raises
	// 2D000. No Go fallback exists; log the root cause loudly.
	smokeCtx, smokeCancel := context.WithTimeout(ctx, 5*time.Second)
	_, smokeErr := s.store.maint().Exec(smokeCtx, `CALL dah_sweep_batch($1, $2, $3, $4)`, 0, int64(-1), int32(0), int64(0))
	smokeCancel()

	if ctx.Err() != nil {
		return
	}

	if smokeErr != nil {
		var pgErr *pgconn.PgError
		if errors.As(smokeErr, &pgErr) && pgErr.Code == "2D000" {
			s.store.logger.Errorf("[dahCursor] COMMIT-in-CALL blocked by middleware (2D000): the DAH sweep cannot commit; CALL must run in autocommit (no wrapping transaction/tracer)")
		} else {
			s.store.logger.Errorf("[dahCursor] proc smoke CALL failed: %v", smokeErr)
		}
	}

	sweepConcurrency := cfg.PostgresDAHSweepConcurrency
	if sweepConcurrency <= 0 {
		sweepConcurrency = 1
	}

	s.logger.Infof("[dahCursor] proc driver active (%d partitions, concurrency=%d, idle=%s, no per-CALL deadline)", numPartitions, sweepConcurrency, idleInterval)

	if s.store.maintPool == nil {
		s.store.logger.Warnf("[dahCursor] no maintenance pool configured (utxostore_postgresMaintenancePoolConns=0): sweep CALLs share the main pool and are NOT immune to server-level statement_timeout")
	}

	sem := make(chan struct{}, sweepConcurrency)

	var wg sync.WaitGroup

	for p := 0; p < numPartitions; p++ {
		wg.Add(1)

		go func(partition int) {
			defer wg.Done()
			s.runPartitionSweepLoop(ctx, partition, sem, lag, retention, idleInterval)
		}(p)
	}

	wg.Wait()
}

// runPartitionSweepLoop drives one partition's sweep forever: CALL (no deadline)
// → refire immediately while that partition has backlog, else idle poll. ANY
// error path sleeps the idle interval before retrying — never hot-spin: during a
// postgres outage this loop must produce 1 quiet retry per idle interval, not a
// millisecond-cadence log flood. One wedged partition stalls only itself (plus
// whoever waits on the shared concurrency semaphore — the stagnation monitor
// names every starved partition independently).
func (s *postgresPrunerService) runPartitionSweepLoop(ctx context.Context, partition int, sem chan struct{}, lag int64, retention int32, idleInterval time.Duration) {
	timer := time.NewTimer(0) // first pass immediately
	defer timer.Stop()

	// Last observed partition watermark. Refiring immediately on backlog alone
	// hot-spins whenever a CALL returns cleanly WITHOUT advancing the watermark —
	// the sweep is disabled (dah_sweep_control.enabled=false) or the band lock is
	// held by the reconciler. Only refire at zero delay when the watermark
	// actually moved; otherwise back off to the idle interval.
	prevWm := int64(-1)

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		safeTip := s.store.dahSafeTip(lag)
		if safeTip <= 0 {
			timer.Reset(idleInterval)
			continue
		}

		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return
		}

		err := s.store.sweepOnePartition(ctx, partition, safeTip, retention, s.store.earlyDAHSweepBoundary())
		<-sem

		if ctx.Err() != nil {
			return
		}

		if err != nil {
			s.store.logger.Warnf("[dahCursor] partition %d CALL error (SQLSTATE %s; retry in %s): %v", partition, pgSQLState(err), idleInterval, err)
			prometheusDAHSweepErrors.Inc()
			timer.Reset(idleInterval)

			continue
		}

		backlog, berr := s.store.dahPartitionBacklog(ctx, partition, safeTip)
		if berr != nil {
			if ctx.Err() == nil {
				s.store.logger.Warnf("[dahCursor] partition %d backlog probe error (retry in %s): %v", partition, idleInterval, berr)
			}

			timer.Reset(idleInterval)

			continue
		}

		// backlog is measured AFTER the CALL, so wm is this partition's true
		// watermark (safeTip movement cancels out). Refire immediately only when
		// there is remaining backlog AND the last CALL advanced the watermark
		// (partial drain of a large backlog). A non-zero backlog with an unmoved
		// watermark means the CALL did nothing (disabled / lock-contended) — back
		// off instead of spinning.
		wm := safeTip - backlog
		if backlog > 0 && wm > prevWm {
			timer.Reset(0)
		} else {
			timer.Reset(idleInterval)
		}

		prevWm = wm
	}
}

// sweepAllPartitionsOnce fires one dah_sweep_batch() CALL per partition IN
// PARALLEL and returns the rows stamped this pass (delta of the control row's
// cumulative counter). Each CALL drains up to max_windows_per_call bands of
// band_heights heights, committing per band; a cancelled CALL loses at most one
// uncommitted band and resumes from the per-partition watermark next pass — the
// watermark only advances when a band fully drains. CALLs have no deadline; work
// is bounded by unit size (bands per CALL), never by wall-clock. Stall
// visibility is the stagnation monitor's responsibility. Per-partition errors
// are logged, not fatal. Shared by the background cursor and Prune.
func (s *Store) sweepAllPartitionsOnce(ctx context.Context, safeTip int64, retention int32) int64 {
	var before int64
	baselineOK := true
	if err := s.maint().QueryRow(ctx, `SELECT total_rows_stamped FROM dah_sweep_control WHERE id = 1`).Scan(&before); err != nil {
		// Without a valid baseline the delta is meaningless (a successful "after"
		// read would report the entire cumulative counter as this pass's work). Log
		// and report 0 for this pass rather than an inflated figure.
		s.logger.Warnf("[dahSweep] could not read total_rows_stamped baseline; this pass's stamped count will report 0: %v", err)
		baselineOK = false
	}

	// Bound how many partitions sweep at once. Each CALL scans cold partition pages
	// from disk; firing all 8 at once thrashes a single contended/cold disk and is
	// SLOWER in aggregate than fewer sweepers (measured). A buffered channel acts as
	// a semaphore; concurrency=1 → fully sequential, concurrency>=numPartitions → the
	// original all-parallel behaviour.
	concurrency := s.settings.UtxoStore.PostgresDAHSweepConcurrency
	if concurrency <= 0 {
		concurrency = 1
	}

	if concurrency > numPartitions {
		concurrency = numPartitions
	}

	sem := make(chan struct{}, concurrency)

	// Read the early-DAH boundary once for this pass so every partition CALL uses
	// a single consistent value (0 when the feature is off).
	boundary := s.earlyDAHSweepBoundary()

	var wg sync.WaitGroup

	for p := 0; p < numPartitions; p++ {
		p := p

		wg.Add(1)

		go func() {
			defer wg.Done()

			sem <- struct{}{}
			defer func() { <-sem }()

			// Stop launching new CALLs once the parent context is done.
			if ctx.Err() != nil {
				return
			}

			if err := s.sweepOnePartition(ctx, p, safeTip, retention, boundary); err != nil {
				if ctx.Err() == nil {
					s.logger.Warnf("[dahSweep] partition %d CALL error (SQLSTATE %s; retry next pass): %v", p, pgSQLState(err), err)
					prometheusDAHSweepErrors.Inc()
				}
			}
		}()
	}

	wg.Wait()

	var after int64
	if err := s.maint().QueryRow(ctx, `SELECT total_rows_stamped FROM dah_sweep_control WHERE id = 1`).Scan(&after); err != nil {
		s.logger.Warnf("[dahSweep] could not read total_rows_stamped after sweep; stamped count reported as 0: %v", err)
		return 0
	}

	if baselineOK && after >= before {
		return after - before
	}

	return 0
}

// sweepOnePartition fires one dah_sweep_batch() CALL for a single partition with
// NO deadline: work is bounded by unit size (max_windows_per_call bands of
// band_heights heights), never by wall-clock. A 45-minute dense band runs for 45
// minutes and commits. The CALL runs on the store lifetime ctx only, so shutdown
// still cancels it cleanly; stall visibility is the stagnation monitor's job.
func (s *Store) sweepOnePartition(ctx context.Context, partition int, safeTip int64, retention int32, boundary int64) error {
	start := time.Now()
	_, err := s.maint().Exec(ctx, `CALL dah_sweep_batch($1, $2, $3, $4)`, partition, safeTip, retention, boundary)
	prometheusDAHSweepCallDuration.Observe(time.Since(start).Seconds())

	return err
}

// earlyDAHSweepBoundary returns the checkpoint boundary to pass to dah_sweep_batch:
// the published early-DAH boundary when the EarlyDAHBelowCheckpoint feature is
// enabled, else 0. A boundary of 0 makes the procedure stamp full retention for
// every row — identical to v16 behaviour.
func (s *Store) earlyDAHSweepBoundary() int64 {
	if s.settings.UtxoStore.EarlyDAHBelowCheckpoint {
		return int64(s.earlyDAHBoundary.Load())
	}

	return 0
}

// pgSQLState returns the postgres SQLSTATE code carried by err, or "" when err
// is nil or not a postgres error. Used to make error logs classifiable (40P01
// deadlock vs 55P03 lock timeout vs everything else) without changing behavior.
func pgSQLState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}

	return ""
}

// dahPartitionBacklog returns safeTip minus this partition's watermark (0 when
// caught up). Unlike the old aggregate dahWatermarkBacklog it returns errors
// instead of a silent 0, so a failed probe can never masquerade as "caught up".
func (s *Store) dahPartitionBacklog(ctx context.Context, partition int, safeTip int64) (int64, error) {
	var wm int64
	if err := s.maint().QueryRow(ctx,
		`SELECT last_swept_height FROM dah_part_watermark WHERE partition = $1`, partition).Scan(&wm); err != nil {
		return 0, errors.NewStorageError("[dahSweep] read watermark for partition %d", partition, err)
	}

	if safeTip > wm {
		return safeTip - wm, nil
	}

	return 0, nil
}
