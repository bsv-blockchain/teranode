package postgres

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/util"
	"golang.org/x/sync/errgroup"
)

// Ensure Store implements the pruner.PrunerServiceProvider interface.
var _ pruner.PrunerServiceProvider = (*Store)(nil)

// GetPrunerService returns a pruner service scoped to this store instance,
// creating it lazily on first call. The service holds a reference to the store,
// so keeping it per-instance (rather than a package global) avoids leaking a
// closed store's pool into a later store with the same process lifetime.
func (s *Store) GetPrunerService() (pruner.Service, error) {
	s.prunerServiceMu.Lock()
	defer s.prunerServiceMu.Unlock()

	if s.prunerService != nil {
		return s.prunerService, nil
	}

	s.prunerService = &postgresPrunerService{
		store:  s,
		logger: s.logger,
	}

	return s.prunerService, nil
}

// postgresPrunerService implements pruner.Service for the postgres store.
type postgresPrunerService struct {
	store         *Store
	logger        interface{ Infof(string, ...interface{}) }
	observers     []pruner.Observer
	mu            sync.Mutex
	cursorStarted bool
	cursorCancel  context.CancelFunc
	cursorWg      sync.WaitGroup // tracks runDAHCursor + runDAHReconcile so stop() can wait them out
}

var _ pruner.Service = (*postgresPrunerService)(nil)

// Start starts the pruner service and launches the continuous DAH cursor (Worker 2).
// It is idempotent — subsequent calls are no-ops.
//
// The background cursor runs under a store-owned context derived from
// context.Background(), NOT the caller's ctx. The cursor is a long-lived worker
// tied to the store's lifetime; binding it to a request- or startup-scoped ctx
// would silently stop it (permanently, given the cursorStarted guard) when that
// ctx is cancelled. It is stopped explicitly via stop() from Store.Stop/Close.
func (s *postgresPrunerService) Start(_ context.Context) {
	s.mu.Lock()
	if s.cursorStarted {
		s.mu.Unlock()
		return
	}
	s.cursorStarted = true
	cursorCtx, cancel := context.WithCancel(context.Background())
	s.cursorCancel = cancel
	s.cursorWg.Add(2)
	s.mu.Unlock()
	s.logger.Infof("[PostgresPrunerService] starting DAH cursor (Worker 2) + reconciliation backstop")
	go s.runDAHCursor(cursorCtx)

	// The reconciliation backstop shares the cursor lifetime/cancel/WaitGroup: it is
	// the slow audit that restores the self-healing the spent-bitmap fold
	// otherwise lacks (Task 8). It runs off the hot path on a long interval.
	go func() {
		defer s.cursorWg.Done()
		s.runDAHReconcile(cursorCtx)
	}()
}

// stop cancels the background DAH cursor and WAITS for it to exit before
// returning, so a subsequent pool.Close() (from Store.Stop/Close) cannot race an
// in-flight sweep transaction into a use-after-close. Idempotent: a second call
// finds a nil cancel and waits on an already-zero WaitGroup.
func (s *postgresPrunerService) stop() {
	s.mu.Lock()
	cancel := s.cursorCancel
	s.cursorCancel = nil
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	s.cursorWg.Wait()
}

// AddObserver adds an observer to be notified when pruning completes.
func (s *postgresPrunerService) AddObserver(observer pruner.Observer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observers = append(s.observers, observer)
}

// Prune deletes rows with delete_at_height at or below the trigger height minus
// PruneDeleteMarginBlocks (the crash-replay margin — see below); it does not
// delete everything at or before the raw trigger height.
func (s *postgresPrunerService) Prune(ctx context.Context, blockHeight uint32, blockHashStr string) (int64, error) {
	if blockHeight == 0 {
		return 0, errors.NewProcessingError("cannot prune at block height 0")
	}

	// Crash-replay moat (2026-07-17 RAM-footprint design, Task 7): only rows
	// whose DAH is at least margin blocks below the trigger are deleted, so no
	// pipeline replaying work near the trigger watermark can reference a
	// deleted parent. Applies uniformly to retention-stamped and early-stamped
	// rows. blockHeight is adjusted ONCE, here, at the top of Prune — every
	// downstream height use (the EXISTS probe, deleteTombstoned, the batch
	// fetch) reads this same parameter, so the margin applies everywhere.
	//
	// A negative setting is a misconfiguration, not a "no margin" request: cast
	// straight to uint32 it wraps to a huge value, making blockHeight <= margin
	// true forever, so Prune would no-op on every call — silently disabling the
	// pruner (the exact failure class that has filled a disk in production
	// before). Clamp to 0 instead and say so once per call, loud enough to find.
	//
	// A huge POSITIVE setting is the mirror-image failure: it needs no integer
	// wrap to cause the exact same silent-no-op-forever outcome, since
	// blockHeight <= margin is simply true until the chain grows past the
	// misconfigured value (e.g. margin=100000 on a chain at height 50000 never
	// deletes anything). Clamp to maxPruneDeleteMarginBlocks and say so, same as
	// the negative case, so deletes resume once the chain clears a sane bound
	// instead of staying disabled for the life of the chain.
	marginSetting := s.store.settings.UtxoStore.PruneDeleteMarginBlocks
	switch {
	case marginSetting < 0:
		s.logger.Infof("[pruner] WARNING: PruneDeleteMarginBlocks is negative (%d); clamping to 0 -- a negative value would otherwise wrap to a huge margin and silently stop all deletes", marginSetting)
		marginSetting = 0
	case marginSetting > maxPruneDeleteMarginBlocks:
		s.logger.Infof("[pruner] WARNING: PruneDeleteMarginBlocks (%d) exceeds the maximum of %d; clamping -- an excessive margin would otherwise make blockHeight <= margin true for the life of the chain, silently stopping all deletes", marginSetting, maxPruneDeleteMarginBlocks)
		marginSetting = maxPruneDeleteMarginBlocks
	}
	margin := uint32(marginSetting) //nolint:gosec // clamped non-negative and bounded above
	if blockHeight <= margin {
		return 0, nil // nothing can be old enough yet
	}
	blockHeight -= margin

	startTime := time.Now()

	// Early-exit on empty backlog: the bench/production drivers retry Prune on a
	// tight loop when it returns 0, which without this probe costs a full 8-way
	// cascade-statement fan-out per empty call on the maintenance pool. One
	// btree-probe EXISTS answers it. pending_deletes is the ONLY pruner feed
	// (design contract below), so EXISTS=false genuinely means nothing to do.
	// Skipped while the background cursor is not running: the inline catch-up
	// sweep below may stamp new rows this call must then delete.
	s.mu.Lock()
	cursorOn := s.cursorStarted
	s.mu.Unlock()

	if cursorOn {
		var hasWork bool
		if err := s.store.pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM pending_deletes WHERE delete_at_height <= $1)`,
			int64(blockHeight),
		).Scan(&hasWork); err == nil && !hasWork {
			return 0, nil
		}
	}

	s.logger.Infof("[pruner][%s:%d] starting cleanup scan (delete_at_height <= %d)",
		blockHashStr, blockHeight, blockHeight)

	// Stamp a bounded catch-up slice before deleting — but ONLY when the background
	// Worker 2 cursor is NOT running. When the cursor IS running it is the
	// authoritative continuous stamper, so an inline catch-up sweep here is redundant
	// and, on the dedicated maintenance pool, just steals the connections the cascade
	// deletes need (measured: with both stamping AND this inline sweep on the maint
	// pool, deletes starve to 0 and the table never reclaims). When no cursor drives
	// (on-demand Prune callers) the inline sweep is required so stamping still makes
	// progress before the delete. The per-partition advisory lock keeps concurrent
	// callers safe either way.
	s.mu.Lock()
	cursorRunning := s.cursorStarted
	s.mu.Unlock()

	if !cursorRunning {
		lag := int64(s.store.settings.UtxoStore.PostgresDAHSweepLag)
		if lag <= 0 {
			lag = 2
		}

		retention := int32(s.store.settings.GetUtxoStoreBlockHeightRetention()) //nolint:gosec // small positive height delta
		s.store.sweepAllPartitionsOnce(ctx, s.store.dahSafeTip(lag), retention)
	}

	deletedCount, err := s.deleteTombstoned(ctx, blockHeight)
	if err != nil {
		return 0, err
	}

	elapsed := time.Since(startTime)
	tps := float64(deletedCount) / elapsed.Seconds()

	var tpsStr string
	if tps >= 1_000_000 {
		tpsStr = fmt.Sprintf("%.1fM records/sec", tps/1_000_000)
	} else if tps >= 1_000 {
		tpsStr = fmt.Sprintf("%.1fK records/sec", tps/1_000)
	} else {
		tpsStr = fmt.Sprintf("%.2f records/sec", tps)
	}

	s.logger.Infof("[pruner][%s:%d] completed cleanup in %v: deleted %s records (%s)",
		blockHashStr, blockHeight, elapsed, util.FormatComma(deletedCount), tpsStr)

	// Notify observers.
	s.mu.Lock()
	observers := make([]pruner.Observer, len(s.observers))
	copy(observers, s.observers)
	s.mu.Unlock()

	for _, obs := range observers {
		obs.OnPruneComplete(blockHeight, deletedCount)
	}

	return deletedCount, nil
}

// deleteTombstoned cascade-deletes every tombstoned tx (delete_at_height <=
// blockHeight) across all partitions IN PARALLEL — one goroutine per partition.
//
// The two tables hash-partition on the SAME tx hash with the SAME modulus, so
// txs_pNN and spends_pNN are ALIGNED: a tx in txs_pNN has all of its
// spend-markers in spends_pNN. That lets each goroutine scan and cascade-delete
// entirely within one partition pair, so no two goroutines ever touch the same
// heap/index pages — zero cross-worker lock/buffer contention. Crucially, the
// per-partition tombstone scan is 1/numPartitions the size and rides its OWN
// leaf index instead of fighting the concurrent-INSERT B-tree splits on the
// shared partitioned-parent index, which is what collapsed concurrent reclaim
// ~13x vs the isolated drain rate.
func (s *postgresPrunerService) deleteTombstoned(ctx context.Context, blockHeight uint32) (int64, error) {
	var totalDeleted atomic.Int64
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(pruneDeleteWorkers)
	for p := 0; p < numPartitions; p++ {
		p := p
		g.Go(func() error {
			n, err := s.deleteTombstonedPartition(gctx, p, blockHeight)
			totalDeleted.Add(n)
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return totalDeleted.Load(), err
	}

	return totalDeleted.Load(), nil
}

// pruneDeleteWorkers / pruneDeleteBatchSize tune the partition-parallel cascade
// delete. pruneDeleteWorkers caps concurrent partition goroutines (= numPartitions
// when they match, leaving ample headroom in the 100-conn pool for the write
// path); a 10000-hash batch keeps each set-based cascade one bounded leaf index
// scan while minimising round trips.
const pruneDeleteWorkers = 8

// maxPruneDeleteMarginBlocks upper-bounds PruneDeleteMarginBlocks. Without a
// cap, a misconfigured huge positive margin makes `blockHeight <= margin` true
// for the life of the chain, silently disabling every delete -- the same
// "pruner silently stops, disk fills" incident class as the negative-margin
// wrap, just approached from the other sign. 1000 is a generous multiple of
// the production default (32) while still guaranteeing deletes resume well
// before the chain grows enough for the misconfiguration to matter.
const maxPruneDeleteMarginBlocks = 1000

// pruneDeleteBatchSize bounds each cascade batch (one short transaction), which
// releases the maintenance pool connection between batches so stamping and other
// maintenance work interleave. Raised 10K -> 50K with the page-ordered cascade:
// a bigger batch means more co-doomed spends rows per heap page in the bitmap
// sweep, which is what makes each page visit pay (the clustering win grows with
// batch size). A var (not const) so tests can shrink it to exercise multi-batch
// draining without inserting hundreds of thousands of rows.
var pruneDeleteBatchSize = 50000

// deleteTombstonedPartition cascade-deletes the tombstoned txs of ONE partition.
// It scans the concrete leaf (txs_pNN) for tombstoned hashes, then cascade-deletes
// them from the ALIGNED spends_pNN → txs_pNN leaves in batched, single-statement
// CTEs. One round-trip per batch, and the statement is implicitly atomic: a
// mid-batch failure rolls back both table deletes together, so no orphans are
// left behind. Returns the number of txs rows removed. Leaf table names are
// derived from numPartitions (same package as the schema DDL), so the coupling
// stays in lock-step with partition creation.
func (s *postgresPrunerService) deleteTombstonedPartition(ctx context.Context, partIdx int, blockHeight uint32) (int64, error) {
	txsLeaf := fmt.Sprintf("txs_p%02d", partIdx)
	spendsLeaf := fmt.Sprintf("spends_p%02d", partIdx)

	// Delete tombstoned txs in bounded batches WITHOUT first loading every matching
	// hash into Go memory. A single statement selects up to pruneDeleteBatchSize
	// doomed hashes and cascade-deletes them from the aligned spends/txs leaves
	// (same MODULUS); we loop until a batch comes back short. Previously the initial
	// SELECT accumulated EVERY tombstoned hash into a [][]byte before any delete —
	// millions of rows per partition (×8 partitions) could OOM the node.
	//
	// ──────────────────────────────────────────────────────────────────────
	// DESIGN CONTRACT — read before "fixing" this delete.
	//
	// This is deliberately a DUMB, lightweight index range-delete: it removes every
	// tx whose delete_at_height has been reached and does NOT re-check whether the
	// tx is really mined / fully-spent / on-longest-chain / not-preserved. Reviewers
	// (and static analysis) repeatedly flag the missing re-check as a data-loss bug
	// and add a per-row eligibility gate here — that was tried and reverted. DO NOT
	// re-add it. Re-counting each candidate's spends on the hottest reclaim path
	// measurably cuts throughput, and the whole point of the deferred-DAH design is
	// to keep the fully-spent test OFF the hot path. (delete_at_height is also always
	// a FUTURE height = completion + 1 + retention, so a reached stamp means the tx
	// settled `retention` blocks ago.)
	//
	// Correctness lives with the DAH SETTERS. Their invariant: set delete_at_height
	// ONLY while a tx is genuinely safe to drop, and CLEAR it the instant it is not:
	//   • dah_sweep.go — stamps only mined+fully-spent+on-longest-chain+not-preserved;
	//     clears the stamp when a tx stops qualifying.
	//   • Unspend (spend.go) — a reorg revives an output → clears the stamp.
	//   • MarkTransactionsOnLongestChain(false) (conflicting.go) — a tx leaves the
	//     longest chain → clears the stamp.
	//   • PreserveTransactions / PreserveParentsOfOldUnminedTransactions — protect a
	//     parent of an unmined child by setting preserve_until (which clears the
	//     stamp), so it is not pruned while the child is unmined; PreserveTransactions
	//     only preserves already-eligible txs.
	//   • ProcessExpiredPreservations (preservation.go) — on expiry, re-stamps only
	//     when still eligible (conflicting, or mined+fully-spent+on-longest-chain),
	//     else clears preserve_until with the stamp left NULL.
	// If you change any code that writes delete_at_height, THAT is where eligibility
	// belongs — not here.
	//
	// Reorg vs prune — a known race we deliberately do NOT defend against here:
	// In theory the pruner could delete a tx at the same instant a reorg Unspend
	// revives it. In practice the two can never touch the same tx. A tx only becomes
	// prune-eligible `retention` blocks after it settled (delete_at_height =
	// completion + 1 + retention; see above), whereas a realistic reorg is 1–2 blocks
	// deep — so a prune-eligible tx is hundreds of blocks older than the deepest block
	// any reorg can reach. The prune-eligible set and the reorg-reachable set overlap
	// ONLY if retention is configured below the reorg depth, i.e. a catastrophic
	// misconfiguration. (Default retention is ~2 days of blocks.)
	//
	// We intentionally keep this simple rather than chase the window: a delete-time
	// re-check of delete_at_height is only a one-sided half-guard (it does nothing
	// when the DELETE wins the race and the later Unspend finds no row to revive), and
	// truly serialising prune against reorg is real complexity for a state the
	// retention margin already makes unreachable. The one invariant that keeps us safe:
	//
	//     retention MUST stay well above the deepest expected reorg.
	//
	// Hold that and this is a non-issue. Do NOT add per-row delete-time guards back.
	// ──────────────────────────────────────────────────────────────────────
	// Doomed candidates are enumerated from the pending_deletes side-table (the only
	// pruner path): a tiny, append-only table indexed by delete_at_height, so the
	// candidate scan stays proportional to the doomed set rather than to the live txs
	// table. The cascade deletes spends and the pending_deletes rows, then the txs rows.
	pdLeaf := fmt.Sprintf("pending_deletes_p%02d", partIdx)

	// PAGE-ORDERED CASCADE (two statements, one transaction per batch).
	//
	// The old single-statement form fed the doomed hashes to the spends delete
	// through a CTE join, which the planner can only execute as per-hash index
	// probes: the heap is visited in HASH order — random pages, one ~60B row
	// per cold 8KB fetch, K scattered fetches per K-output parent. But spends
	// of co-doomed parents CLUSTER physically (the heap is append-ordered by
	// spend time, and parents doomed together were fully spent together), so
	// the same rows can be reached with far fewer page visits IF the plan
	// sorts victim row-locations by physical page first — a Bitmap Index Scan
	// → Bitmap Heap Scan. Postgres can only build one bitmap over ALL victims
	// when the hashes arrive as a single = ANY(array) scan key, not a join;
	// hence phase 1 SELECTs the batch into an array and phase 2 deletes by it.
	// Measured ceiling for this change (del_spends removed outright): drain
	// 694,580 -> 1,504,282 rows/s (2.17x) at the MOST favourable baseline
	// (warm, 1-output chains); the gap widens with fan-out and cold pages.
	//
	// FOR UPDATE in phase 1 pins the batch's pending_deletes rows for the
	// transaction, so a concurrent Unspend revival (which removes the pd row)
	// serialises against this batch instead of racing the two phases — a
	// strictly SMALLER window than the old single statement (whose READ
	// COMMITTED re-check would delete a row revived mid-statement anyway; see
	// the retention-margin contract above, which remains the real guarantee).
	//
	// SET LOCAL enable_indexscan/enable_seqscan = off pins the DELETE's plan:
	// bitmap is the only access path left, so a planner cost blip can never
	// silently regress this back to random per-hash hops (or a full seqscan of
	// a multi-billion-row partition). Phase 1 keeps default settings — its
	// LIMIT must stop the index scan early, which a bitmap plan cannot do.
	doomedSQL := fmt.Sprintf(
		`SELECT hash FROM %s WHERE delete_at_height <= $1 LIMIT $2 FOR UPDATE`, pdLeaf)
	cascadeSQL := fmt.Sprintf(`
		WITH del_spends AS (DELETE FROM %[2]s WHERE prev_tx_hash = ANY($1::bytea[]) RETURNING 1),
		del_pd     AS (DELETE FROM %[3]s WHERE hash = ANY($1::bytea[]))
		DELETE FROM %[1]s WHERE hash = ANY($1::bytea[])`, txsLeaf, spendsLeaf, pdLeaf)

	// Loop until a batch comes back short (fewer than pruneDeleteBatchSize rows) — that
	// IS the drain signal: no more eligible tombstones remain in this partition. There is
	// deliberately NO per-call cap. An earlier version stopped after a fixed number of
	// batches per call, on the assumption that the caller loops and re-invokes so the
	// backlog still drains "in slices". The real caller (the pruner worker) only fires on
	// a new-block notification, so a bounded call left the rest of an eligible backlog
	// undrained until the NEXT block — and on a caught-up / sparse-block node that meant a
	// stamped backlog older than retention effectively never drained (observed live on
	// teratestnet: ~9.5M eligible rows sitting, oldest overdue ~20k blocks). The work must
	// be done regardless, so we do it now. Each batch is its own atomic statement that
	// releases the maintenance-pool connection, so a long drain still interleaves with
	// stamping and is interruptible at batch boundaries via ctx.
	var deleted int64
	for {
		select {
		case <-ctx.Done():
			return deleted, ctx.Err()
		default:
		}

		n, err := s.deleteTombstonedBatchWithRetry(ctx, doomedSQL, cascadeSQL, blockHeight, txsLeaf)
		if err != nil {
			return deleted, err
		}
		deleted += n
		// A short batch means the partition is drained.
		if n < int64(pruneDeleteBatchSize) {
			break
		}
	}

	return deleted, nil
}

// deleteTombstonedBatchWithRetry wraps deleteTombstonedBatch with a bounded,
// jittered retry on a Postgres deadlock (SQLSTATE 40P01). Concurrent cascade-delete
// workers across partitions (and a concurrent Unspend/reconcile touching the same
// spends/txs rows) can deadlock; Postgres aborts one side and rolls its transaction
// back entirely, so a retry that lands after the winner commits succeeds — the same
// remedy as the create-batch and SetMinedMulti retry sites (deadlock.go).
//
// Retrying the WHOLE batch (not a sub-piece of it) is the only sound option here,
// and it is safe: deleteTombstonedBatch is one pgx transaction (BEGIN..COMMIT) that
// is entirely rolled back on any error (the deferred Rollback fires; Commit is never
// reached), so a deadlocked attempt persists nothing — there is no partial state to
// reconcile. A retry re-runs phase 1's `SELECT ... FOR UPDATE` from scratch, which
// re-authorises the batch against CURRENT ground truth: the FOR UPDATE lock means a
// concurrent Unspend revival (which deletes the pending_deletes row before the
// pruner's SELECT can lock it) is either fully visible to the retry's fresh SELECT
// (the hash is simply absent, so it is not re-deleted) or was blocked behind the
// prior attempt's row lock and is resolved one way or the other before the retry's
// SELECT runs. Either way the retry authorises its delete against a fresh read, the
// same authority a first attempt would have had — never a stale, already-rolled-back
// candidate list.
func (s *postgresPrunerService) deleteTombstonedBatchWithRetry(ctx context.Context, doomedSQL, cascadeSQL string, blockHeight uint32, txsLeaf string) (int64, error) {
	n, err := retryOnPgDeadlock(ctx,
		func() (int64, error) {
			return s.deleteTombstonedBatch(ctx, doomedSQL, cascadeSQL, blockHeight)
		},
		func(attemptNum int, backoff time.Duration) {
			s.store.logger.Warnf("[pruner] postgres deadlock (40P01) on cascade delete batch %s, retry %d/%d after %s", txsLeaf, attemptNum, pgDeadlockMaxRetries, backoff)
		},
	)
	if err != nil {
		return 0, errors.NewStorageError("[pruner] cascade delete %s", txsLeaf, err)
	}

	return n, nil
}

// deleteTombstonedBatch runs one page-ordered cascade batch in its own short
// transaction: SELECT ... FOR UPDATE the doomed hashes, then one array-driven
// DELETE statement whose plan is pinned to bitmap scans (see the comment at
// the SQL construction). Returns the number of doomed hashes processed.
func (s *postgresPrunerService) deleteTombstonedBatch(ctx context.Context, doomedSQL, cascadeSQL string, blockHeight uint32) (int64, error) {
	pgxTx, err := s.store.maint().Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer pgxTx.Rollback(ctx) //nolint:errcheck

	// delete_at_height is INT4 — bind int32 so the doomed-scan predicate is
	// a native int4 comparison on the pending_deletes btree-indexed column.
	rows, err := pgxTx.Query(ctx, doomedSQL, int32(blockHeight), int64(pruneDeleteBatchSize)) //nolint:gosec // height < 2^31 validated at entry
	if err != nil {
		return 0, err
	}
	hashes := make([][]byte, 0, pruneDeleteBatchSize)
	for rows.Next() {
		var h []byte
		if scanErr := rows.Scan(&h); scanErr != nil {
			rows.Close()
			return 0, scanErr
		}
		hashes = append(hashes, h)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return 0, err
	}
	if len(hashes) == 0 {
		return 0, pgxTx.Commit(ctx)
	}

	if _, err = pgxTx.Exec(ctx, `SET LOCAL enable_indexscan = off`); err != nil {
		return 0, err
	}
	if _, err = pgxTx.Exec(ctx, `SET LOCAL enable_seqscan = off`); err != nil {
		return 0, err
	}
	if _, err = pgxTx.Exec(ctx, cascadeSQL, hashes); err != nil {
		return 0, err
	}
	if err = pgxTx.Commit(ctx); err != nil {
		return 0, err
	}
	return int64(len(hashes)), nil
}
