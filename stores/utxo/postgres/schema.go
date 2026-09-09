package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/jackc/pgx/v5/pgxpool"
)

// createSchema creates the v7 turbo UTXO tables in the connected PostgreSQL
// database: 2 LOGGED hash-partitioned tables (txs, spends). The outputs
// table has been folded into txs as PACKED per-output columns (utxo_hashes
// flat bytea, out_spendables/out_frozens bitmaps, out_count/spendable_count
// scalars, coinbase_spending_height scalar, spendable_ins array), eliminating
// the per-tx outputs INSERT, the random-hash PK index, and the cascade-DELETE.
// raw_tx lives on the txs row (immutable BYTEA blob).
//
// Schema design tradeoffs (vs. the standard stores/utxo/sql store):
//
//   - No foreign keys. Child rows (spends) reference txs by BYTEA hash rather
//     than a surrogate id, and pruning explicitly cascades via
//     DELETE FROM spends → txs inside one txn (see pruner_provider).
//
//   - "Is this output spent?" lives in the spends table as a row, not as a
//     nullable spending_data column. Spends become pure INSERTs (no MVCC
//     bloat on txs output arrays).
//
//   - mined-block info (block_id/height/subtree_idx) is packed into ONE
//     mined_info bytea (fixed 12-byte records) and conflicting_children is an
//     array on txs; all per-output UTXO fields are PACKED columns on txs
//     (flat bytea + bitmaps + scalar counts). A single row lookup returns
//     everything needed for spend validation — zero extra table JOINs.
func (s *Store) createSchema(ctx context.Context) error {
	if err := createSchemaInternalFF(ctx, s.pool, s.maintPool, s.logger, s.settings.UtxoStore.PostgresTxsFillfactor); err != nil {
		return err
	}

	// The server-side DAH sweep procedure is the only sweep mechanism (there is no
	// in-process fallback), so bootstrapping it is mandatory: a failure (missing
	// CREATE privilege, or postgres < 11) fails store startup and surfaces the
	// deployment problem rather than silently never pruning.
	if err := s.bootstrapDAHSweepProc(ctx); err != nil {
		return err
	}

	return nil
}

// partitionSpec defines a partitioned table, the fillfactor for its children,
// and the autovacuum storage params for its children.
//
// autovacuum MUST be set on the leaf partitions: autovacuum operates on the
// leaf relations, and storage params set on a partitioned PARENT are never
// inherited by autovacuum on the leaves. (A parent-level ALTER TABLE txs SET
// (autovacuum_*) silently does nothing for vacuum scheduling — it only affects
// partitions created later via the parent's defaults, not the leaf's vacuum.)
type partitionSpec struct {
	name       string
	fillfactor int
	autovacuum string // SET-clause body applied per leaf partition
}

// numPartitions is the number of hash partitions per table. Burst benchmarks
// favoured a single partition (no fan-out overhead). Under SUSTAINED high-churn
// load that inverts: the txs `locked`-flag UPDATE produces ~1 dead tuple per tx,
// and a single autovacuum worker on one large partition cannot reclaim at the
// dead-tuple generation rate (measured: txs_dead grows unbounded to 24M+ at
// 60K TPS). Splitting into N partitions lets up to autovacuum_max_workers leaves
// be vacuumed concurrently, each a fraction of the size, so aggregate vacuum
// throughput scales with the churn. 8 keeps vacuum ahead with the default 3
// workers while bounding fan-out cost on reads.
const numPartitions = 8

// createSchemaWithPoolFlag executes all DDL statements using the provided pool.
// The usePendingDeletes parameter is retained for backward compatibility with
// existing test call sites but is now ignored: the pending_deletes side-table
// is the only pruner path, so it is always created and the px_delete_at_height
// BRIN index on txs is always backfilled into pending_deletes and dropped.
func createSchemaWithPoolFlag(ctx context.Context, pool *pgxpool.Pool, _ bool) error {
	// No maintenance pool in the test/compat path: the backfill (if it runs)
	// falls back to the main pool.
	return createSchemaInternal(ctx, pool, nil, ulogger.TestLogger{})
}

// createSchemaInternal executes all DDL statements using the provided pool. It
// always creates the pending_deletes side-table (8 leaves + per-leaf btree on
// delete_at_height) and unconditionally backfills+drops the legacy
// px_delete_at_height BRIN index on txs — pending_deletes is the only pruner path.
func createSchemaInternal(ctx context.Context, pool *pgxpool.Pool, maintPool *pgxpool.Pool, logger ulogger.Logger) error {
	return createSchemaInternalFF(ctx, pool, maintPool, logger, 0)
}

// createSchemaInternalFF is createSchemaInternal with an explicit txs
// fillfactor (utxostore_postgresTxsFillfactor). Applied only at CREATE TABLE —
// an existing deployment keeps the fillfactor its leaves were built with.
// Out-of-range values (including the 0 the compat wrappers pass) fall back to
// the HOT-safe default of 50; see the partitionSpec comment for the trade.
func createSchemaInternalFF(ctx context.Context, pool *pgxpool.Pool, maintPool *pgxpool.Pool, logger ulogger.Logger, txsFillfactor int) error {
	if txsFillfactor < 10 || txsFillfactor > 100 {
		txsFillfactor = 50
	}
	ddlStatements := []string{
		txsDDL,
		spendsDDL,
	}

	for _, ddl := range ddlStatements {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			return errors.NewStorageError("schema creation failed\nDDL: %s", ddl, err)
		}
	}

	// Create numPartitions hash partitions for each table with appropriate
	// fillfactor and per-leaf autovacuum tuning.
	//
	// txs is the only high-churn table: the validator hot path UPDATEs the
	// `locked` flag per tx (Create(WithLocked)+SetLocked), producing one dead
	// tuple per tx. At scale_factor=0.05 on a multi-million-row partition that
	// lets ~5% (≈1.7M) dead tuples accumulate before vacuum fires — observed as
	// a 0.3M–1.9M dead-tuple oscillation that breaks HOT-chain reuse under load.
	// 0.01 fires at ~1% (≈340K), and cost_delay=0 keeps the sweep from being
	// throttled mid-run. spends is insert-only (no UPDATE churn), so the
	// insert_scale_factor path (keeping the visibility map fresh for the
	// deferred-DAH sweep's index-only counts) is what matters there — left at the
	// gentler 0.05 so insert-triggered vacuums don't steal hot-path I/O.
	const commonAV = "autovacuum_vacuum_insert_scale_factor = 0.02, "
	tables := []partitionSpec{
		// txs is the only UPDATE-churned table: SetLocked(false), the SetMinedMulti
		// block_ids append, AND the DAH sweep's delete_at_height stamp each rewrite
		// the row — up to ~3 updates/tx. fillfactor=50 leaves half the page free so
		// those rewrites stay HOT (no new index entries, reclaimed cheaply by
		// HOT-prune on the next page touch) instead of accumulating index-bloating
		// dead tuples that only a full vacuum can reclaim. This bounds dead-tuple
		// density at the SOURCE, so a gentle autovacuum config (which the DAH sweep
		// needs — an aggressive one starves the sweep) is enough. cost_limit=8000
		// (4x default) + cost_delay=0 keep what vacuum is still needed un-throttled;
		// freeze_max_age is raised so no anti-wraparound vacuum stalls reclaim mid-run.
		{"txs", txsFillfactor, "autovacuum_vacuum_scale_factor = 0.01, " + commonAV +
			"autovacuum_vacuum_cost_limit = 8000, " +
			"autovacuum_freeze_max_age = 500000000, " +
			"autovacuum_vacuum_cost_delay = 0, " +
			"autovacuum_analyze_scale_factor = 0.005, " +
			"autovacuum_vacuum_insert_threshold = 1000"},
		{"spends", 100, "autovacuum_vacuum_scale_factor = 0.05, " + commonAV +
			"autovacuum_vacuum_cost_limit = 2000, " +
			"autovacuum_vacuum_cost_delay = 2, " +
			"autovacuum_analyze_scale_factor = 0.05"},
	}
	for _, spec := range tables {
		for i := 0; i < numPartitions; i++ {
			ddl := fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS %s_p%02d PARTITION OF %s FOR VALUES WITH (MODULUS %d, REMAINDER %d) WITH (fillfactor = %d)",
				spec.name, i, spec.name, numPartitions, i, spec.fillfactor,
			)
			if _, err := pool.Exec(ctx, ddl); err != nil {
				return errors.NewStorageError("partition creation failed for %s_p%02d", spec.name, i, err)
			}

			// Idempotent: also back-fills partitions created before these settings
			// existed. Set on the leaf (autovacuum ignores parent-level params).
			av := fmt.Sprintf("ALTER TABLE %s_p%02d SET (%s)", spec.name, i, spec.autovacuum)
			if _, err := pool.Exec(ctx, av); err != nil {
				return errors.NewStorageError("autovacuum tuning failed for %s_p%02d", spec.name, i, err)
			}
		}
	}

	// Height index on spends. The DAH sweep enumerates candidate parents purely from
	// the spends side by height window ("spends enumerate, txs decides"). The v15 band
	// CTE is
	//     SELECT prev_tx_hash, prev_output_idx, spent_at_height
	//       FROM spends_pNN WHERE spent_at_height > $wm AND spent_at_height <= $cap
	//       ORDER BY spent_at_height LIMIT band_rows
	// so the index MUST be range-scannable AND sort-ordered on spent_at_height: the
	// ORDER BY .. LIMIT is the per-band work-quantisation, and losing that order makes
	// the LIMIT sort the whole band and spill to pgsql_tmp — the ENOSPC watermark-freeze
	// incident class. The per-band cost stays proportional to the band's spend-rows, not
	// to accumulated chain size. spent_at_height is MONOTONIC (block height) so inserts
	// land on the right-most leaf (cheap, hot-page append); spends is INSERT-ONLY so a
	// btree does NOT break HOT (HOT is an UPDATE concern).
	//
	// WHY A PLAIN (spent_at_height) btree, not the former covering form — a measured
	// regime change, not a reversal:
	//
	//   The prior index was (spent_at_height, prev_tx_hash) INCLUDE (prev_output_idx),
	//   chosen so the band scan was INDEX-ONLY (all three columns index-resident, zero
	//   heap fetches — measured 1,881 buffers vs 201,858 for a non-covering scan, and a
	//   plain height-only btree once showed bimodal sweep throughput, V3 CV 38%). That
	//   was correct WHILE spends were effectively insert-only: the visibility map stays
	//   all-visible, so index-only actually fires. Early-DAH broke that precondition. It
	//   deletes spends aggressively below the checkpoint (measured 479M spend deletes of
	//   churn on live mainnet mid-IBD), so the VM is never all-visible and the covering
	//   index-only scan fetches the heap ANYWAY: measured on that live table, 60,074 of
	//   66,260 band rows (91%) were heap fetches. So the covering form paid the heap cost
	//   regardless AND cost ~60 GB (7.4 GB/leaf) — an index-only advantage that had
	//   already evaporated in this regime.
	//
	//   The plain (spent_at_height) btree is 114 MB/leaf (98.5% smaller — many spends
	//   share each height so it deduplicates heavily; INCLUDE forfeits deduplicate_items,
	//   the plain form regains it), returns the identical band rows in the same order (no
	//   spill), and measured EQUIVALENT sweep buffers/latency on that same churned table
	//   (both heap-bound; warm ~100 ms either way, plain slightly faster cold — a small
	//   index to navigate instead of a cold 7.4 GB one). It is also cheaper to insert
	//   (narrower row, dedup re-enabled; the covering form measured ~+18% spend-INSERT).
	//   The old V3 bimodal collapse was the covering index's fast index-only mode beating
	//   the height-only heap mode inconsistently; in the early-DAH regime both are
	//   heap-bound, so there is no fast mode to be bimodal against. The swap is gated on
	//   TestThroughput_QueueStorePruned showing no sweep-throughput collapse.
	//
	//   SEPARATE lever (not this change): the heap-bound sweep cost itself — the plain
	//   index must visit a heap the churn has scattered by spent_at_height (delete-then-
	//   reuse decorrelates it), and the same churn keeps the VM stale. Addressing either
	//   (aggressive spends autovacuum to keep the VM fresh, or re-correlating the heap)
	//   speeds the sweep independently of this index's size.
	//
	// LIVE-MIGRATION NOTE: on an existing large deployment build the new index with
	// CREATE INDEX CONCURRENTLY per leaf BEFORE deploying, so the CREATE below is a
	// no-op. The startup DROP of the old covering index is fast, but a blocking build of
	// the new one at startup is not.
	//
	// There is deliberately NO index on txs.mined_at_height (uncorrelated; a btree there
	// would break HOT on the hot mine UPDATE). The only txs that become fully-spent AT
	// mine time without a spend in a swept window (zero-spendable) are stamped inline in
	// SetMinedMulti; the rarer spent-while-unmined case is caught by the backstop.
	for i := 0; i < numPartitions; i++ {
		// Create the plain height btree FIRST (no-op if pre-built CONCURRENTLY for a
		// live swap), THEN drop the superseded forms — so a height index is always
		// present and the sweep never falls back to a seq scan mid-migration.
		idxStmts := []string{
			fmt.Sprintf(`CREATE INDEX IF NOT EXISTS spends_p%02d_height_btree ON spends_p%02d USING btree (spent_at_height)`, i, i),
			// Superseded covering INCLUDE form (see the regime-change note above).
			fmt.Sprintf(`DROP INDEX IF EXISTS spends_p%02d_h_hash_oidx_btree`, i),
			// Ancient two-column form.
			fmt.Sprintf(`DROP INDEX IF EXISTS spends_p%02d_h_hash_btree`, i),
		}
		for _, ddl := range idxStmts {
			if _, err := pool.Exec(ctx, ddl); err != nil {
				return errors.NewStorageError("height index creation failed", err)
			}
		}
	}

	// Drop legacy txs.mined_at_height indexes if a prior build created them. The sweep
	// no longer scans txs by mined_at_height (see above), so these are dead weight: the
	// BRIN was useless (uncorrelated) and the btree hurt the mine-path HOT ratio.
	for i := 0; i < numPartitions; i++ {
		for _, ddl := range []string{
			fmt.Sprintf(`DROP INDEX IF EXISTS txs_p%02d_mined_at_height_btree`, i),
			fmt.Sprintf(`DROP INDEX IF EXISTS txs_p%02d_mined_at_height_brin`, i),
			// Dead since the DAH backstop was removed (3e3f7f4e9); drop on existing DBs.
			fmt.Sprintf(`DROP INDEX IF EXISTS spends_p%02d_spent_at_height_brin`, i),
		} {
			if _, err := pool.Exec(ctx, ddl); err != nil {
				return errors.NewStorageError("legacy mined_at_height index drop failed", err)
			}
		}
	}

	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS dah_watermark (
			id INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
			last_swept_height BIGINT NOT NULL DEFAULT 0
		)`); err != nil {
		return errors.NewStorageError("dah_watermark creation failed", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO dah_watermark (id, last_swept_height) VALUES (1, 0) ON CONFLICT (id) DO NOTHING`); err != nil {
		return errors.NewStorageError("dah_watermark seed failed", err)
	}

	// Per-partition DAH sweep watermark: one row per hash partition so the 8
	// dah_sweep_batch() CALLs the cursor fans out in PARALLEL each track their own
	// progress independently. Seeded from the legacy single-row dah_watermark so an
	// existing deployment continues from where it had swept (no re-sweep from 0;
	// re-sweeping would be idempotent but wasteful).
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS dah_part_watermark (
			partition INT PRIMARY KEY CHECK (partition BETWEEN 0 AND %d),
			last_swept_height BIGINT NOT NULL DEFAULT 0
		)`, numPartitions-1)); err != nil {
		return errors.NewStorageError("dah_part_watermark creation failed", err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO dah_part_watermark (partition, last_swept_height)
		SELECT g, COALESCE((SELECT last_swept_height FROM dah_watermark WHERE id = 1), 0)
		FROM generate_series(0, %d) g
		ON CONFLICT (partition) DO NOTHING`, numPartitions-1)); err != nil {
		return errors.NewStorageError("dah_part_watermark seed failed", err)
	}

	// dah_sweep_control: kill switch, tunable knobs, proc version, and the
	// per-CALL outcome the proc-mode adaptive ticker reads (see dah_sweep_proc.go).
	// Plain DDL, always created; the procedure itself is bootstrapped separately
	// (bootstrapped separately in Store.createSchema).
	if _, err := pool.Exec(ctx, dahSweepControlDDL); err != nil {
		return errors.NewStorageError("dah_sweep_control creation failed", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO dah_sweep_control (id) VALUES (1) ON CONFLICT (id) DO NOTHING`); err != nil {
		return errors.NewStorageError("dah_sweep_control seed failed", err)
	}

	// dah_reconcile_cursor: per-partition rotating cursor for the bounded
	// spent_progress reconciliation backstop (Task 8). See dah_reconcile.go.
	if _, err := pool.Exec(ctx, dahReconcileCursorDDL); err != nil {
		return errors.NewStorageError("dah_reconcile_cursor creation failed", err)
	}

	// conflict_intents: write-ahead log for crash-safe ProcessConflicting /
	// ReverseProcessConflicting (see #861). One row per in-flight conflict-
	// resolution operation, recorded BEFORE its first state mutation and removed
	// once its terminal step commits; rows that survive a restart drive replay.
	// Intentionally NOT tied to txs — intents reference tx hashes, not row ids,
	// and must outlive any individual transaction record.
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS conflict_intents (
			intent_id     BYTEA PRIMARY KEY,
			kind          TEXT NOT NULL,
			block_height  BIGINT NOT NULL,
			block_hash    BYTEA NOT NULL,
			tx_hashes     BYTEA NOT NULL,
			started_at    BIGINT NOT NULL
		)`); err != nil {
		return errors.NewStorageError("conflict_intents creation failed", err)
	}

	// pending_deletes side-table: ALWAYS-ON (no flag). The pruner populates this
	// table with (hash, delete_at_height) rows and reads from it directly rather
	// than scanning txs via a BRIN index. Each hash partition gets its own leaf
	// and a btree index on delete_at_height for efficient height-range scans by
	// the pruner.
	//
	// This block is intentionally placed BEFORE the BRIN drop/backfill block
	// below so that pending_deletes exists when the backfill INSERT runs.
	if _, err := pool.Exec(ctx, pendingDeletesDDL); err != nil {
		return errors.NewStorageError("pending_deletes creation failed", err)
	}
	for i := 0; i < numPartitions; i++ {
		leafDDL := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS pending_deletes_p%02d PARTITION OF pending_deletes FOR VALUES WITH (MODULUS %d, REMAINDER %d)",
			i, numPartitions, i,
		)
		if _, err := pool.Exec(ctx, leafDDL); err != nil {
			return errors.NewStorageError("pending_deletes_p%02d creation failed", i, err)
		}
		idxDDL := fmt.Sprintf(
			"CREATE INDEX IF NOT EXISTS px_pd_dah_p%02d ON pending_deletes_p%02d USING btree (delete_at_height)",
			i, i,
		)
		if _, err := pool.Exec(ctx, idxDDL); err != nil {
			return errors.NewStorageError("pending_deletes_p%02d index creation failed", i, err)
		}

		// Idempotent, mirrors the txs/spends per-leaf pattern: autovacuum ignores
		// parent-level params, so set on each leaf. Gentle spends-style profile —
		// pd is insert-then-delete churn; an aggressive profile would steal I/O
		// from the sweep and pruner (see CLAUDE.md).
		av := fmt.Sprintf(
			"ALTER TABLE pending_deletes_p%02d SET ("+
				"autovacuum_vacuum_scale_factor = 0.05, "+
				"autovacuum_vacuum_insert_scale_factor = 0.02, "+
				"autovacuum_vacuum_cost_limit = 2000, "+
				"autovacuum_vacuum_cost_delay = 2, "+
				"autovacuum_analyze_scale_factor = 0.05)", i)
		if _, err := pool.Exec(ctx, av); err != nil {
			return errors.NewStorageError("autovacuum tuning failed for pending_deletes_p%02d", i, err)
		}
	}

	// pending_unmined side-table: ALWAYS-ON (no flag). Stores (hash, unmined_since)
	// rows for efficient old-unmined-tx queries by the iterator and pruner.
	// Each hash partition gets its own leaf and a btree index on unmined_since for
	// height-range scans. The backfill is guarded by a marker index so it runs only
	// once (on first startup with this schema version) rather than on every restart.
	if _, err := pool.Exec(ctx, pendingUnminedDDL); err != nil {
		return errors.NewStorageError("pending_unmined creation failed", err)
	}
	for i := 0; i < numPartitions; i++ {
		leafDDL := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS pending_unmined_p%02d PARTITION OF pending_unmined FOR VALUES WITH (MODULUS %d, REMAINDER %d)",
			i, numPartitions, i,
		)
		if _, err := pool.Exec(ctx, leafDDL); err != nil {
			return errors.NewStorageError("pending_unmined_p%02d creation failed", i, err)
		}
		idxDDL := fmt.Sprintf(
			"CREATE INDEX IF NOT EXISTS px_pu_since_p%02d ON pending_unmined_p%02d USING btree (unmined_since)",
			i, i,
		)
		if _, err := pool.Exec(ctx, idxDDL); err != nil {
			return errors.NewStorageError("pending_unmined_p%02d index creation failed", i, err)
		}
	}
	// Retire the legacy one-shot backfill marker index unconditionally, on every
	// startup (cheap no-op once already dropped) — see pendingUnminedBackfillDDL.
	if _, err := pool.Exec(ctx, pendingUnminedBackfillDropLegacyMarkerDDL); err != nil {
		return errors.NewStorageError("legacy pending_unmined backfill marker drop failed", err)
	}
	// Clean-shutdown-gated reconciliation backfill: skips the txs seq scan when
	// the previous shutdown was clean (see resolvePendingUnminedBackfill and
	// pendingUnminedBackfillDDL for details). The full-scan INSERT runs on the
	// maintenance pool (statement_timeout=0) when available so an operator's
	// server/role statement_timeout cannot abort it mid-startup.
	if err := resolvePendingUnminedBackfill(ctx, pool, maintPool, logger); err != nil {
		return err
	}

	// Partial indexes on txs for iterator/pruner queries. The base indexes are
	// always applied. The legacy px_delete_at_height BRIN on txs is no longer
	// used: pending_deletes is the only pruner path, so the BRIN is
	// unconditionally backfilled into pending_deletes (for any orphaned rows on
	// an existing DB) and dropped. The pending_deletes table is created above so
	// the backfill INSERT can run while the BRIN still exists (if present) for a
	// fast index-assisted scan.
	if _, err := pool.Exec(ctx, txsIndexesDDLBase); err != nil {
		return errors.NewStorageError("txs base index creation failed", err)
	}
	if _, err := pool.Exec(ctx, txsDAHBrinBackfillAndDropDDL); err != nil {
		return errors.NewStorageError("px_delete_at_height backfill+drop failed", err)
	}

	// Spent-bitmap fold columns (proc v15): add to v15 DBs idempotently. Both
	// columns are deliberately UNINDEXED — indexing either would disqualify HOT
	// updates on txs (measured: HOT ratio collapses from 83%+ to 33% when a
	// btree covers a column touched by the sweep UPDATE). The migration is a
	// no-op if the columns already exist; pre-v15 (spent_progress) databases are
	// rejected at bootstrapDAHSweepProc, not migrated.
	//
	// Skip-if-applied: `ADD COLUMN IF NOT EXISTS` is idempotent, but the ALTER
	// still takes an ACCESS EXCLUSIVE lock on txs even when it does nothing. On an
	// already-migrated DB that lock could deadlock against the DAH sweep's
	// per-partition locks at startup. An information_schema existence check first
	// skips the ALTER entirely once both columns are present.
	if _, err := applySpentBitmapMigration(ctx, pool); err != nil {
		return errors.NewStorageError("spent-bitmap column migration failed", err)
	}

	// Dirty-parents heal queue for the fold-vs-Unspend stale-snapshot race.
	if _, err := pool.Exec(ctx, dahDirtyParentsDDL); err != nil {
		return errors.NewStorageError("dah_dirty_parents creation failed", err)
	}

	// Playbook §4: LZ4 compression on raw_tx (faster than default pglz), and pin
	// utxo_hashes to EXTERNAL (never compressed; out-of-line only when the row is
	// wide — the spend path's O(1) sliced substr() detoast on wide parents relies
	// on it). Both are pre-checked against pg_attribute and skipped once applied,
	// for the same reason applySpentBitmapMigration is pre-checked above: the ALTER
	// takes an ACCESS EXCLUSIVE lock on txs even when the change is a no-op, and
	// that redundant lock can deadlock against the DAH sweep's per-partition locks
	// at startup.
	applyTxsColumnStorageTuning(ctx, pool, logger)

	// NOTE: autovacuum tuning lives on the leaf partitions (see partitionSpec
	// above), not here. A parent-level ALTER TABLE txs SET (autovacuum_*) is a
	// no-op for vacuum scheduling — autovacuum reads the leaf's reloptions — so
	// the previous parent-level block was removed to avoid a false sense of
	// safety. The effective per-partition values are the ones in the loop.

	return nil
}

// ---------------------------------------------------------------------------
// Table DDL — 2 LOGGED hash-partitioned tables (txs + spends)
// ---------------------------------------------------------------------------

// txs: consolidated transaction metadata + state + raw_tx + mined_info (packed
// 12-byte records) + conflicting_children (array) + PACKED per-output UTXO
// columns. LOGGED — UTXO set is durable state.
//
// Packed per-output encoding ("array packing", replaces the previous 5 parallel
// per-output array columns — a hot-path CPU fix: bulk create no longer pays
// per-row array_agg re-aggregation, and per-output access is O(1) byte
// arithmetic instead of per-element array CASE/array_length evaluation):
//
//	utxo_hashes              — flat concatenation, 16 bytes per output (the first
//	                           16 bytes / 128-bit prefix of each output's UTXO
//	                           hash); output i lives at byte offset i*16
//	                           (substr(.., i*16+1, 16), substr is 1-based). NULL
//	                           when no outputs. The column is a pure equality
//	                           guard on spend — never read back as data — so a
//	                           128-bit prefix is ample for a fail-closed check.
//	out_count                — number of outputs (slot_exists test: idx < out_count).
//	spendable_count          — count of spendable outputs; the deferred-DAH
//	                           "fully spent" comparand.
//	out_spendables           — bitmap, bit i = output i is spendable. Encoding
//	                           matches PostgreSQL get_bit(): bit n lives in byte
//	                           n/8 at position n%8 counting from the LEAST
//	                           significant bit (Go: buf[i/8] |= 1 << (i%8)).
//	                           NULL when no outputs.
//	out_frozens              — same bitmap encoding; NULL means "no output frozen"
//	                           (the common case — freeze materialises it on demand,
//	                           sized to (out_count+7)/8 bytes).
//	coinbase_spending_height — scalar coinbase maturity height (0 = non-coinbase).
//	                           The old per-output array was redundant: every output
//	                           of a coinbase tx gets the SAME maturity height.
//	spendable_ins            — per-output spendable_in height, kept as an INT[]
//	                           (set only by rare ReAssignUTXO; NULL common case;
//	                           NULL element = no restriction). 0-based output
//	                           index i → Postgres 1-based subscript [i+1].
//
// MIGRATION NOTE: this packed layout replaces the v7 array columns
// (utxo_hashes BYTEA[], out_spendables BOOLEAN[], out_frozens BOOLEAN[],
// coinbase_spending_heights BIGINT[], spendable_ins kept as-is). No live
// migration DDL is provided: bench databases are recreated from scratch and CI
// creates fresh schemas. A pre-existing database with the array layout must be
// dropped and re-synced.
//
// Column order is alignment-aware (Phase B row diet): 8-byte fixed columns first,
// then 4-byte fixed, then 1-byte bools, then variable-length (bytea/array). This
// minimises inter-column alignment padding on the heap tuple. All INSERT/SELECT
// statements reference columns BY NAME, so the physical order is free to change.
//
// version and size_in_bytes are INTEGER (int4), not BIGINT: version is a
// consensus uint32 stored via an int32 bit-cast (decoded uint32(int32(v))), and
// size_in_bytes is bounded by the ~1 GiB bytea payload cap — 4 fewer bytes each
// per row. lock_time and fee stay BIGINT (fee is satoshis, potentially large).
//
// mined_info replaces the three parallel INT[] columns (block_ids /
// block_heights / subtree_idxs) with one BYTEA of fixed 12-byte records
// (block_id int4 BE, height int4 BE, subtree_idx int4 BE): 13 B for the
// 1-element common case versus ~75 B for three single-element arrays. See
// packed_mined.go for the Go codec. The column is UNINDEXED (mine-path UPDATE
// stays HOT).
const txsDDL = `
CREATE TABLE IF NOT EXISTS txs (
    hash                      BYTEA PRIMARY KEY,
    lock_time                 BIGINT NOT NULL,
    fee                       BIGINT NOT NULL,
    inserted_at               TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    version                   INTEGER NOT NULL,
    size_in_bytes             INTEGER NOT NULL,
    unmined_since             INT,
    delete_at_height          INT,
    preserve_until            INT,
    mined_at_height           INT,
    out_count                 INT NOT NULL DEFAULT 0,
    spendable_count           INT NOT NULL DEFAULT 0,
    coinbase_spending_height  INT NOT NULL DEFAULT 0,
    last_spend_height         INT,
    coinbase                  BOOLEAN NOT NULL DEFAULT FALSE,
    locked                    BOOLEAN NOT NULL DEFAULT FALSE,
    conflicting               BOOLEAN NOT NULL DEFAULT FALSE,
    frozen                    BOOLEAN NOT NULL DEFAULT FALSE,
    raw_tx                    BYTEA,
    conflicting_children      BYTEA[],
    mined_info                BYTEA,
    utxo_hashes               BYTEA,
    out_spendables            BYTEA,
    out_frozens               BYTEA,
    spendable_ins             INT[],
    spent_bits                BYTEA NOT NULL DEFAULT '\x'::bytea
) PARTITION BY HASH (hash);`

// Indexes on txs.
//
// unmined_since is BRIN, not btree, and that choice is HOT-path-critical:
// SetMinedMulti modifies unmined_since (-> NULL) on every mined tx, and an
// UPDATE that modifies any column covered by a non-summarizing (btree) index
// disqualifies the row from a HOT update — forcing a full row copy plus new
// entries in EVERY index on the table, including the 32-byte random-hash PK.
// Measured on the sustained-prune bench: with a partial btree here the txs HOT
// ratio was 33.7%; BRIN is a summarizing AM (PG16+), so the mined update stays
// HOT (83%+ measured). Its consumers (unmined iterators, old-unmined queries)
// are background paths that tolerate bitmap-scan rechecks.
//
// delete_at_height no longer has an index on txs: the BRIN was retired by
// txsDAHBrinBackfillAndDropDDL. The pruner now reads the pending_deletes
// btree side-table exclusively; delete_at_height on txs is a stamp-only column
// used by the DAH sweep worker and read back only on restart recovery.
// txsIndexesDDLBase contains the txs partial indexes that are always created.
const txsIndexesDDLBase = `
CREATE INDEX IF NOT EXISTS px_unmined_since ON txs USING brin (unmined_since) WITH (pages_per_range = 32, autosummarize = on);
CREATE INDEX IF NOT EXISTS px_preserve_until ON txs (preserve_until) WHERE preserve_until IS NOT NULL;`

// txsDAHBrinBackfillAndDropDDL is the one-time migration that retires the legacy
// px_delete_at_height BRIN index on txs now that pending_deletes is the only
// pruner path. It runs unconditionally on every startup.
//
// The gated DO block runs only when the BRIN index px_delete_at_height still
// exists (i.e. the DB was previously running the old inline-delete path). In
// that case it backfills pending_deletes from any txs rows that already carry a
// non-NULL delete_at_height (orphaned rows the side-table pruner would otherwise
// never see), then drops the BRIN. The INSERT runs BEFORE the DROP so it can use
// the BRIN for a fast index-assisted scan rather than a full sequential scan.
//
// ON CONFLICT (hash) DO NOTHING makes the INSERT idempotent: rows already in
// pending_deletes (e.g. from a partial earlier run) are skipped cleanly.
//
// On all subsequent startups the BRIN is already gone so the IF EXISTS guard is
// false — neither the INSERT nor the DROP executes, avoiding a seq-scan.
const txsDAHBrinBackfillAndDropDDL = `
DO $$ BEGIN
  IF EXISTS(SELECT 1 FROM pg_class WHERE relname='px_delete_at_height') THEN
    INSERT INTO pending_deletes (hash, delete_at_height)
      SELECT hash, delete_at_height FROM txs WHERE delete_at_height IS NOT NULL
      ON CONFLICT (hash) DO NOTHING;
    DROP INDEX px_delete_at_height;
  END IF;
END $$;`

// txsSetterCMigrationDDL adds the spent-bitmap fold columns (proc v15) to txs
// tables on DB startup. ADD COLUMN IF NOT EXISTS so the statement is safe to
// re-run on every startup. It deliberately does NOT migrate a pre-v15
// (spent_progress counter) database: no backfill can populate spent_bits below
// the sweep watermark without the O(all-history) pass the fold design exists to
// avoid, so bootstrapDAHSweepProc refuses to arm when spent_progress is present
// and the DB must be dropped and re-synced (same precedent as the packed-layout
// migration note above).
//
// spent_bits BYTEA NOT NULL — one bit per output (LSB-first per byte, matching
//
//	the Go get_bit encoding buf[i/8] |= 1<<(i%8) used by out_spendables). Bit i
//	is set once the sweep has folded a spend of vout i. The create path
//	pre-sizes it to (out_count+7)/8 zero bytes so every later fold is a
//	same-size in-place overwrite; the '\x' DEFAULT only backstops rows from
//	paths that omit the column. The fold ORs band masks in (idempotent: reorg
//	watermark rewinds re-set the same bits, so the v13 counter-drift classes
//	are structurally gone); Unspend clears exactly the bits of the spend rows
//	it deletes. The DAH stamp fires when bit_count(spent_bits) =
//	spendable_count — a single-row check with no spends-history recount.
//	Invariant: spent_bits ⊆ out_spendables (the fold and the reconciler only
//	set bits that pass the out_spendables filter; Unspend only clears).
//	STORAGE MAIN keeps the (low-entropy, compressible) bitmap inline; only
//	>~16K-output outliers go out-of-line.
//
// last_spend_height INT (nullable) — running max spent_at_height across all
//
//	spendable outputs folded so far. NULL until the first spendable spend is
//	folded. Used by the sweep to stamp delete_at_height = last_spend_height.
//	Never lowered by Unspend (stale-high is safe: it feeds only
//	GREATEST(last_spend_height, mined_at_height), pushing deletion later).
//
// NEITHER column is indexed. An UPDATE touching any btree-indexed column on txs
// disqualifies the row from a HOT update (forcing a full row copy + index
// maintenance on the 32-byte random-hash PK), collapsing the HOT ratio from
// 83%+ to ~34% (measured). delete_at_height is the canonical unindexed precedent.
const txsSetterCMigrationDDL = `
ALTER TABLE txs ADD COLUMN IF NOT EXISTS spent_bits        BYTEA NOT NULL DEFAULT '\x'::bytea;
ALTER TABLE txs ADD COLUMN IF NOT EXISTS last_spend_height INT;
ALTER TABLE txs ALTER COLUMN spent_bits SET STORAGE MAIN;`

// applySpentBitmapMigration runs txsSetterCMigrationDDL only when the two fold
// columns are not already present, and reports whether it ran the ALTER.
//
// The ALTER is idempotent (ADD COLUMN IF NOT EXISTS) but still acquires an
// ACCESS EXCLUSIVE lock on txs on every startup even when it is a no-op. On an
// already-migrated production DB that redundant lock can deadlock against the
// DAH sweep's per-partition locks (observed SQLSTATE 40P01). A cheap
// information_schema probe first skips the ALTER once both columns exist, so a
// migrated DB never re-acquires the lock. Fresh/pre-v15 DBs still run the DDL.
func applySpentBitmapMigration(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	var existing int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM information_schema.columns
		WHERE table_name = 'txs'
		  AND column_name IN ('spent_bits', 'last_spend_height')`).Scan(&existing); err != nil {
		return false, err
	}

	// Both columns already present: nothing to do, and — importantly — do NOT take
	// the ACCESS EXCLUSIVE lock the ALTER would.
	if existing == 2 {
		return false, nil
	}

	if _, err := pool.Exec(ctx, txsSetterCMigrationDDL); err != nil {
		return false, err
	}

	return true, nil
}

// applyTxsColumnStorageTuning applies the two per-column storage tunings on txs
// (raw_tx → LZ4 compression, utxo_hashes → EXTERNAL storage). Each is pre-checked
// against pg_attribute so an already-tuned database never re-issues the ALTER: the
// ALTER takes an ACCESS EXCLUSIVE lock on txs even when the change is a no-op, and
// that redundant lock can deadlock at startup against the DAH sweep's per-partition
// locks (the same hazard applySpentBitmapMigration guards). The tunings are
// non-fatal (performance only), but failures are logged — a missing ALTER privilege
// or a Postgres build without liblz4 support would otherwise vanish silently.
func applyTxsColumnStorageTuning(ctx context.Context, pool *pgxpool.Pool, logger ulogger.Logger) {
	// raw_tx → LZ4 compression. attcompression is 'l' once set; the default
	// (unset) renders as an empty string and 'p' is pglz — either means "apply it".
	var compression string
	if err := pool.QueryRow(ctx, `
		SELECT attcompression::text FROM pg_attribute
		WHERE attrelid = 'txs'::regclass AND attname = 'raw_tx'`).Scan(&compression); err != nil {
		logger.Warnf("[schema] could not read raw_tx compression, skipping LZ4 tuning: %v", err)
	} else if compression != "l" {
		if _, err := pool.Exec(ctx, `ALTER TABLE txs ALTER COLUMN raw_tx SET COMPRESSION lz4`); err != nil {
			logger.Warnf("[schema] ALTER txs.raw_tx SET COMPRESSION lz4 failed (tuning skipped): %v", err)
		}
	}

	// utxo_hashes → EXTERNAL storage. attstorage is 'e' once set.
	var storage string
	if err := pool.QueryRow(ctx, `
		SELECT attstorage::text FROM pg_attribute
		WHERE attrelid = 'txs'::regclass AND attname = 'utxo_hashes'`).Scan(&storage); err != nil {
		logger.Warnf("[schema] could not read utxo_hashes storage, skipping EXTERNAL tuning: %v", err)
	} else if storage != "e" {
		if _, err := pool.Exec(ctx, `ALTER TABLE txs ALTER COLUMN utxo_hashes SET STORAGE EXTERNAL`); err != nil {
			logger.Warnf("[schema] ALTER txs.utxo_hashes SET STORAGE EXTERNAL failed (tuning skipped): %v", err)
		}
	}
}

// dahDirtyParentsDDL creates the dirty-parents heal queue. Unspend inserts the
// affected parent hashes here IN THE SAME TRANSACTION as the spend-row delete +
// bit clear; the reconciler drains this queue FIRST each tick with a fresh
// snapshot, recomputing spent_bits/last_spend_height from spends and un-stamping
// (+ purging pending_deletes) if a concurrent fold re-set a just-cleared bit
// from a stale statement snapshot. The queue is bounded by Unspend volume
// (≈ zero outside reorgs), so the heal arrives within ~one reconcile tick —
// unlike the rotating-slice audit, whose full rotation is weeks at mainnet
// scale and therefore can NOT be the safety story for wrongly-full bits.
const dahDirtyParentsDDL = `
CREATE TABLE IF NOT EXISTS dah_dirty_parents (
    hash        BYTEA PRIMARY KEY,
    partition   INT NOT NULL,
    enqueued_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);`

// pendingDeletesDDL creates the pending_deletes partitioned parent table. Each
// hash partition leaf is created separately in createSchemaWithPoolFlag.
// The table stores (hash, delete_at_height) rows for the pruner to consume;
// the stamp path populates it and the pruner clears it as rows are deleted.
const pendingDeletesDDL = `
CREATE TABLE IF NOT EXISTS pending_deletes (
    hash             BYTEA NOT NULL,
    delete_at_height INT   NOT NULL,
    PRIMARY KEY (hash)
) PARTITION BY HASH (hash);`

// pendingUnminedDDL creates the pending_unmined partitioned parent table. Each
// hash partition leaf is created separately in createSchemaWithPoolFlag.
// The table stores (hash, unmined_since) rows for the iterator and pruner to
// consume; the projection invariant maintains it in line with txs mutations.
// ALWAYS-ON: no feature flag guards this table — it is created unconditionally.
const pendingUnminedDDL = `
CREATE TABLE IF NOT EXISTS pending_unmined (
    hash           BYTEA NOT NULL,
    unmined_since  INT   NOT NULL,
    PRIMARY KEY (hash)
) PARTITION BY HASH (hash);`

// pendingUnminedBackfillDropLegacyMarkerDDL drops the retired one-shot backfill
// marker index. It runs unconditionally on every startup (a cheap no-op once
// the index is already gone) — unlike pendingUnminedBackfillDDL below, this
// statement carries no seq-scan cost, so it does not need the clean-shutdown
// gate.
const pendingUnminedBackfillDropLegacyMarkerDDL = `
DROP INDEX IF EXISTS px_pu_backfill_marker;`

// pendingUnminedBackfillDDL is the idempotent reconciliation backfill that
// repairs pending_unmined from txs. The create hot path no longer writes
// pending_unmined synchronously — rows are projected by the in-process
// write-behind projector (see pending_unmined_projector.go) — so an UNCLEAN
// stop (crash) can lose the projector's not-yet-flushed buffer. This
// INSERT..SELECT repairs any such gap by copying every non-conflicting unmined
// tx from txs (one seq scan). ON CONFLICT (hash) DO NOTHING makes re-runs free
// for rows already present.
//
// It is now gated by the store_clean_shutdown marker (see
// resolvePendingUnminedBackfill): a graceful Store.Stop()/Close() already
// performs a final drain of the write-behind buffer via
// stopPendingUnminedProjector(), which makes pending_unmined complete and
// correct on its own — so after a CLEAN shutdown this seq scan is redundant
// and is skipped. On a production mainnet database (hundreds of millions of
// txs rows) that seq scan costs minutes; skipping it on the common clean-
// restart path removes that cost entirely. It still runs whenever the marker
// says the previous shutdown was unclean, is missing, or fails to read — the
// fail-safe direction that preserves today's behaviour for crash recovery.
const pendingUnminedBackfillDDL = `
INSERT INTO pending_unmined (hash, unmined_since)
  SELECT hash, unmined_since FROM txs
  WHERE unmined_since IS NOT NULL AND conflicting = false
  ON CONFLICT (hash) DO NOTHING;`

// storeCleanShutdownDDL creates the single-row marker table that records
// whether the store's previous run shut down cleanly. Additive/idempotent:
// CREATE TABLE IF NOT EXISTS plus a seeding INSERT that is a no-op once the row
// exists, so it is safe to run unconditionally on every startup — including
// against a pre-existing database that predates this table, which naturally
// seeds clean=false and so falls into the fail-safe backfill branch on its
// first startup with this schema version.
//
// id=1 is the only row ever used; there is exactly one store per database.
const storeCleanShutdownDDL = `
CREATE TABLE IF NOT EXISTS store_clean_shutdown (
    id INT PRIMARY KEY,
    clean BOOLEAN NOT NULL,
    stamped_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO store_clean_shutdown (id, clean, stamped_at) VALUES (1, false, now()) ON CONFLICT (id) DO NOTHING;`

// resolvePendingUnminedBackfill gates the pending_unmined reconciliation
// backfill (pendingUnminedBackfillDDL) on the store_clean_shutdown marker.
//
// It first ensures the marker table exists (idempotent, safe every startup),
// then reads the id=1 row's clean value:
//   - clean == true:  the previous shutdown already drained the write-behind
//     projector (Store.Stop/Close → stopPendingUnminedProjector), so
//     pending_unmined is already complete. The backfill seq scan is skipped.
//   - clean == false, the row is missing, or the read errors for any reason:
//     the fail-safe direction — run the full backfill, exactly as before this
//     gate existed.
//
// In both branches the marker is then stamped clean=false: the store is now
// running, so any crash from this point forward must be treated as unclean on
// the next startup.
//
// The full-scan backfill INSERT runs on backfillPool when maintPool is non-nil,
// and on the main pool otherwise. The maintenance pool pins statement_timeout=0
// (see New in store.go), so a server/database/role statement_timeout that an
// operator armed cannot abort this one-time seq scan mid-startup. The maintenance
// pool is optional (PostgresMaintenancePoolConns defaults to 0), hence the
// fallback. The cheap marker table create/read/stamp always use the main pool.
func resolvePendingUnminedBackfill(ctx context.Context, pool *pgxpool.Pool, maintPool *pgxpool.Pool, logger ulogger.Logger) error {
	if _, err := pool.Exec(ctx, storeCleanShutdownDDL); err != nil {
		return errors.NewStorageError("store_clean_shutdown marker table creation failed", err)
	}

	var clean bool

	queryErr := pool.QueryRow(ctx, `SELECT clean FROM store_clean_shutdown WHERE id = 1`).Scan(&clean)

	if queryErr == nil && clean {
		logger.Infof("[pendingUnminedBackfill] skipped: previous shutdown was clean, pending_unmined is already reconciled")
	} else {
		if queryErr != nil {
			logger.Infof("[pendingUnminedBackfill] running full backfill: store_clean_shutdown marker missing or unreadable (%v)", queryErr)
		} else {
			logger.Infof("[pendingUnminedBackfill] running full backfill: previous shutdown was unclean")
		}

		// Run the seq-scan INSERT on the maintenance pool (statement_timeout=0)
		// when configured, falling back to the main pool otherwise.
		backfillPool := pool
		if maintPool != nil {
			backfillPool = maintPool
		}

		logger.Infof("[pendingUnminedBackfill] backfilling pending_unmined from txs (one-time, unclean restart)")

		start := time.Now()

		tag, err := backfillPool.Exec(ctx, pendingUnminedBackfillDDL)
		if err != nil {
			return errors.NewStorageError("pending_unmined backfill failed", err)
		}

		logger.Infof("[pendingUnminedBackfill] backfill complete: %d rows inserted in %s", tag.RowsAffected(), time.Since(start))
	}

	if _, err := pool.Exec(ctx, `UPDATE store_clean_shutdown SET clean = false, stamped_at = now() WHERE id = 1`); err != nil {
		return errors.NewStorageError("store_clean_shutdown marker update failed", err)
	}

	return nil
}

// spends: append-only spend records. LOGGED. A row here is the canonical
// "this output was spent by that tx" marker; Unspend deletes the row, and
// the pruner removes all rows for a parent_tx before removing the parent.
//
// prev_output_idx and spent_at_height are INT4, not BIGINT: a vout is a
// uint32 that can never reach 2^31 in a protocol-valid tx (guarded at the Go
// boundary — see voutToInt32), and block heights stay far below 2^31. The
// narrowing shrinks every heap row by 8 bytes and every UNIQUE-index entry by
// 4 — fewer bytes to copy/compare on each insert, probe, and reclaim delete
// in this hottest of tables. The same reasoning narrows the txs height
// columns above (unmined_since/delete_at_height/preserve_until/
// mined_at_height/coinbase_spending_height).
const spendsDDL = `
CREATE TABLE IF NOT EXISTS spends (
    prev_tx_hash    BYTEA NOT NULL,
    prev_output_idx INT   NOT NULL,
    spending_data   BYTEA NOT NULL,
    spent_at_height INT,
    UNIQUE (prev_tx_hash, prev_output_idx)
) PARTITION BY HASH (prev_tx_hash);`
