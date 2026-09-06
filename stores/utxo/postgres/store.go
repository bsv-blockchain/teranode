package postgres

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/batchermetrics"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/jackc/pgx/v5/pgxpool"
)

var _ utxo.Store = (*Store)(nil)

// Store implements the utxo.Store interface using direct writes to
// append-only PostgreSQL tables with optional batching for throughput.
type Store struct {
	logger   ulogger.Logger
	settings *settings.Settings
	storeURL *url.URL

	// pgxpool for all pgx-native operations.
	pool *pgxpool.Pool

	// Small dedicated pool for background reclaim (DAH sweep CALLs + pruner cascade
	// deletes), isolated from `pool` so the validator's batchers cannot starve
	// stamping/deleting under load. nil ⇒ reclaim falls back to `pool` (legacy
	// behaviour when PostgresMaintenancePoolConns <= 0).
	maintPool *pgxpool.Pool

	// utxo.BlockStateFields supplies the chain-tip height and median block time
	// as one atomic snapshot, and with it the Store interface's six block-state
	// methods. SetBlockHeight and SetBlockState are wrapped below to keep the
	// int32-cast guard at the single chokepoint.
	utxo.BlockStateFields

	// earlyDAHBoundary is the highest block height at or below which a fully-spent
	// tx may be stamped with an immediate delete-at-height (no retention wait). It
	// is published by the factory wiring (Task 6) once the main chain has confirmed
	// the pinned checkpoint hash; the DAH sweep reads it per CALL. 0 = off (the
	// EarlyDAHBelowCheckpoint setting also gates it — see earlyDAHSweepBoundary).
	earlyDAHBoundary atomic.Uint32

	// batchers — nil until Start() is called.
	createBatcher *batcher.Batcher[batchCreateItem]
	spendBatcher  *batcher.Batcher[batchSpendItem]
	getBatcher    *batcher.Batcher[batchGetItem]
	unlockBatcher *batcher.Batcher[batchUnlockItem]

	// achieved batch-size instrumentation (items/batches per batcher).
	batchStats batchSizeStats

	// pruner service — lazily created per store instance (not a package global).
	prunerService   pruner.Service
	prunerServiceMu sync.Mutex

	// pending_unmined write-behind projector (see pending_unmined_projector.go).
	// puOnce lazily starts the writer on first enqueue; channels are nil until then.
	puOnce sync.Once
	puMu   sync.Mutex
	puBuf  []puEntry
	puKick chan struct{}
	puStop chan struct{}
	puDone chan struct{}

	// puDropped latches true if the projector ever drops buffered entries at its
	// hard cap (a silent projection loss). Once set, markCleanShutdown records the
	// shutdown as UNCLEAN so the next startup runs the fail-safe backfill from txs
	// and re-projects any genuinely-unmined tx whose row was dropped. See
	// pending_unmined_projector.go enqueuePendingUnmined and markCleanShutdown.
	puDropped atomic.Bool
}

// batchSizeStats accumulates the real (post-trigger) batch sizes each batcher
// flushes, so a config's effective batch size can be measured rather than assumed.
type batchSizeStats struct {
	createItems, createBatches atomic.Int64
	spendItems, spendBatches   atomic.Int64
	getItems, getBatches       atomic.Int64
	unlockItems, unlockBatches atomic.Int64
}

// BatchSizeSnapshot returns the mean achieved batch size per batcher since the
// previous call and resets the counters. A config that flushes 500-item batches
// is size-cap-bound; a much smaller mean means the timeout/tick/drain trigger is
// firing before the size cap.
func (s *Store) BatchSizeSnapshot() map[string]float64 {
	avg := func(items, batches *atomic.Int64) float64 {
		b := batches.Swap(0)
		it := items.Swap(0)
		if b == 0 {
			return 0
		}
		return float64(it) / float64(b)
	}
	return map[string]float64{
		"create": avg(&s.batchStats.createItems, &s.batchStats.createBatches),
		"spend":  avg(&s.batchStats.spendItems, &s.batchStats.spendBatches),
		"get":    avg(&s.batchStats.getItems, &s.batchStats.getBatches),
		"unlock": avg(&s.batchStats.unlockItems, &s.batchStats.unlockBatches),
	}
}

// SetEarlyDAHBoundary publishes the highest checkpoint height once the main
// chain has confirmed the pinned checkpoint hash (see factory wiring). 0 = off.
func (s *Store) SetEarlyDAHBoundary(h uint32) { s.earlyDAHBoundary.Store(h) }

// New creates a new direct-write UTXO store.
// The storeURL scheme should be "postgres".
func New(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, storeURL *url.URL) (*Store, error) {
	pgxURL := *storeURL
	pgxURL.Scheme = "postgres"

	pgxConfig, err := pgxpool.ParseConfig(pgxURL.String())
	if err != nil {
		return nil, err
	}

	// Respect pool_max_conns from the connection URL when supplied; otherwise
	// default to 80. Stock PostgreSQL ships max_connections=100; a higher default
	// would claim the entire server budget and starve the maintenance pool, the
	// superuser-reserved slots, and other consumers. pgxpool opens lazily, so this
	// is only a ceiling. 80 stays >= the BatcherMaxConcurrent default (64) so the
	// safety check below does not reject a stock default config.
	if !pgxURL.Query().Has("pool_max_conns") {
		pgxConfig.MaxConns = 80
	}

	// Guard against a batcher/pool concurrency mismatch that can deadlock the store —
	// a single batcher exhausting the pool, every dispatch blocked on pgxpool.Acquire
	// while PostgreSQL sits idle (the betfair-pc mainnet outage). Fail fast with an
	// actionable message before opening any connection; warn on thin-but-workable headroom.
	if warn, cerr := checkBatcherPoolConfig(pgxConfig.MaxConns, tSettings.UtxoStore.BatcherMaxConcurrent, numConnectionBatchers); cerr != nil {
		return nil, cerr
	} else if warn != "" {
		logger.Warnf("%s", warn)
	}

	// Full durability — financial data requires synchronous_commit = on. Enforce it
	// on every connection rather than relying on the server default. Group commit
	// (commit_delay + commit_siblings) is the safe way to recover throughput, but
	// those are server-level GUCs and must be configured on the PostgreSQL instance.
	if pgxConfig.ConnConfig.RuntimeParams == nil {
		pgxConfig.ConnConfig.RuntimeParams = map[string]string{}
	}
	pgxConfig.ConnConfig.RuntimeParams["synchronous_commit"] = "on"

	pool, err := pgxpool.NewWithConfig(ctx, pgxConfig)
	if err != nil {
		return nil, err
	}

	// Dedicated maintenance pool for background reclaim (DAH sweep CALLs + pruner
	// cascade deletes), isolated from the validator write pool so reclaim is never
	// starved under load. Inherits the same DSN/TLS/synchronous_commit via Copy().
	// Default 0 ⇒ disabled (reclaim shares the main pool — legacy behaviour).
	var maintPool *pgxpool.Pool
	if mc := tSettings.UtxoStore.PostgresMaintenancePoolConns; mc > 0 {
		maintConfig := pgxConfig.Copy()
		maintConfig.MaxConns = int32(mc) //nolint:gosec // small positive bound
		// Keep numPartitions (8) warm so the fan-out CALLs/deletes never wait on a
		// lazy dial — but never exceed MaxConns: pgxpool rejects MinConns > MaxConns,
		// so a small mc (1..7) must clamp MinConns down rather than fail startup.
		minConns := numPartitions
		if mc < minConns {
			minConns = mc
		}
		maintConfig.MinConns = int32(minConns) //nolint:gosec // small positive bound

		// Clock immunity: background maintenance work (DAH sweep CALLs, pruner
		// deletes) is bounded by unit-of-work, never wall-clock. Pin the three
		// session GUCs that a server/database/role-level ops default could
		// otherwise use to kill a healthy multi-minute band mid-fold (a
		// statement_timeout armed at CALL start is NOT defusable by SET LOCAL
		// inside the procedure). Applies to the maintenance pool only — the
		// main pool keeps server defaults.
		if maintConfig.ConnConfig.RuntimeParams == nil {
			maintConfig.ConnConfig.RuntimeParams = map[string]string{}
		}
		maintConfig.ConnConfig.RuntimeParams["statement_timeout"] = "0"
		maintConfig.ConnConfig.RuntimeParams["lock_timeout"] = "0"
		maintConfig.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "0"
		// The DAH reconciler's lost-re-dirty correctness (dah_reconcile.go) relies
		// on each statement taking its OWN snapshot — i.e. READ COMMITTED — so a
		// recompute sees a spend that committed after its DELETE. Pin it here so a
		// server/database/role default of REPEATABLE READ (both statements sharing
		// one pre-DELETE snapshot) can't silently break the invariant.
		maintConfig.ConnConfig.RuntimeParams["default_transaction_isolation"] = "read committed"

		maintPool, err = pgxpool.NewWithConfig(ctx, maintConfig)
		if err != nil {
			pool.Close()
			return nil, err
		}
	}

	initPrometheusMetrics()

	s := &Store{
		logger:    logger,
		settings:  tSettings,
		storeURL:  storeURL,
		pool:      pool,
		maintPool: maintPool,
	}

	if err := s.createSchema(ctx); err != nil {
		pool.Close()
		if maintPool != nil {
			maintPool.Close()
		}
		return nil, err
	}

	return s, nil
}

// Start initializes the create and spend batchers for throughput optimization.
// Without calling Start(), Create() and Spend() work in direct (unbatched) mode.
func (s *Store) Start(ctx context.Context) {
	// Instrument batchers like the SQL store so batch size / duration / dispatch
	// reasons are exported via teranode_batcher_* metrics (label batcher="postgres_*").
	otelTracer := tracing.Tracer("utxo").OTelTracer()
	batcherOpts := func(name string) []batcher.Option {
		return []batcher.Option{
			batcher.WithName(name),
			batcher.WithLogger(s.logger),
			batcher.WithMetrics(batchermetrics.Provider()),
			batcher.WithTracer(otelTracer),
		}
	}
	// Per-batcher drain: the global BatcherDrainMode forces drain on every batcher;
	// the per-batcher *BatcherDrainMode settings (matching the aerospike store and the
	// dev-scale-1 config) enable it selectively by arrival pattern. maxConc caps the
	// number of in-flight batch callbacks (go-batcher SetMaxConcurrent).
	globalDrain := s.settings.BatcherDrainMode
	maxConc := s.settings.UtxoStore.BatcherMaxConcurrent

	// Create batcher — pipelines N creates via a single INSERT…SELECT FROM UNNEST
	// (see sendCreateBatchUNNEST; no COPY, no staging table).
	storeBatchSize := s.settings.UtxoStore.StoreBatcherSize
	if storeBatchSize <= 0 {
		storeBatchSize = 100
	}
	storeBatchDuration := time.Duration(s.settings.UtxoStore.StoreBatcherDurationMillis) * time.Millisecond
	if storeBatchDuration <= 0 {
		storeBatchDuration = 10 * time.Millisecond
	}
	s.createBatcher = batcher.NewWithPool(storeBatchSize, storeBatchDuration, s.sendCreateBatch, true, batcherOpts("postgres_create")...)
	configureBatcher(s.createBatcher, maxConc, globalDrain || s.settings.UtxoStore.StoreBatcherDrainMode, s.settings.UtxoStore.StoreBatcherTickerIntervalMillis)

	// Spend batcher — pipelines N validation CTEs via SendBatch (no transaction needed).
	// background=true is safe because pipelined CTEs don't hold row locks across batches.
	spendBatchSize := s.settings.UtxoStore.SpendBatcherSize
	if spendBatchSize <= 0 {
		spendBatchSize = 100
	}
	spendBatchDuration := time.Duration(s.settings.UtxoStore.SpendBatcherDurationMillis) * time.Millisecond
	if spendBatchDuration <= 0 {
		spendBatchDuration = 10 * time.Millisecond
	}
	s.spendBatcher = batcher.NewWithPool(spendBatchSize, spendBatchDuration, s.sendSpendBatch, true, batcherOpts("postgres_spend")...)
	configureBatcher(s.spendBatcher, maxConc, globalDrain || s.settings.UtxoStore.SpendBatcherDrainMode, s.settings.UtxoStore.SpendBatcherTickerIntervalMillis)

	// Get batcher — pipelines N SELECTs via SendBatch.
	getBatchSize := s.settings.UtxoStore.GetBatcherSize
	if getBatchSize <= 0 {
		getBatchSize = 500
	}
	getBatchDuration := storeBatchDuration
	s.getBatcher = batcher.NewWithPool(getBatchSize, getBatchDuration, s.sendGetBatch, true, batcherOpts("postgres_get")...)
	configureBatcher(s.getBatcher, maxConc, globalDrain || s.settings.UtxoStore.GetBatcherDrainMode, s.settings.UtxoStore.GetBatcherTickerIntervalMillis)

	// Unlock batcher — pipelines N UPDATEs via SendBatch.
	unlockBatchSize := s.settings.UtxoStore.LockedBatcherSize
	if unlockBatchSize <= 0 {
		unlockBatchSize = 500
	}
	unlockBatchDuration := storeBatchDuration
	s.unlockBatcher = batcher.NewWithPool(unlockBatchSize, unlockBatchDuration, s.sendUnlockBatch, true, batcherOpts("postgres_unlock")...)
	configureBatcher(s.unlockBatcher, maxConc, globalDrain || s.settings.UtxoStore.LockedBatcherDrainMode, s.settings.UtxoStore.LockedBatcherTickerIntervalMillis)

	// Launch the DAH sweep (Worker 2) here, from the store's own Start(), which the
	// factory (stores/utxo/factory/postgres.go) calls immediately AFTER New() — so
	// createSchema (including the spent-bitmap migration) has already completed.
	// Launching it here, rather than from the external pruner Server (which only
	// gates on the FSM leaving IDLE, NOT on schema/migrations), guarantees the
	// sweep never issues partition-locking CALLs while a startup ALTER still holds
	// ACCESS EXCLUSIVE on txs — the deadlock that took mainnet down.
	//
	// The sweep starting now (during IBD, FSM=CATCHINGBLOCKS) rather than after
	// FSM-from-IDLE is fine: it only needs the schema, which is ready, and stamping
	// DAH during IBD is correct. postgresPrunerService.Start is idempotent
	// (cursorStarted guard), so the external pruner Server's later
	// GetPrunerService().Start() call — on this same cached instance — is a no-op.
	s.startPrunerSweep(ctx)
}

// startPrunerSweep launches this store's DAH sweep cursor via its own pruner
// service. It resolves the SAME cached postgresPrunerService the external pruner
// Server obtains through GetPrunerService(), whose Start is idempotent — so the
// sweep is launched exactly once regardless of call order or a double external
// Start. A GetPrunerService() error is logged and startup continues (the pruner
// Server would surface the same failure); a nil service is a defensive no-op.
func (s *Store) startPrunerSweep(ctx context.Context) {
	svc, err := s.GetPrunerService()
	if err != nil {
		s.logger.Errorf("[postgres] could not start DAH sweep from store.Start: %v", err)
		return
	}
	if svc != nil {
		svc.Start(ctx)
	}
}

// configureBatcher applies the optional max-concurrency cap, drain mode, and tick
// interval to a batcher, in the order go-batcher requires: SetMaxConcurrent and
// SetDrainMode before the first Put, and SetTickInterval after SetDrainMode so the
// drain guard wins on conflict (tick is a no-op + warning when drain is enabled).
func configureBatcher[T any](b *batcher.Batcher[T], maxConcurrent int, drain bool, tickMs int) {
	if maxConcurrent > 0 {
		b.SetMaxConcurrent(maxConcurrent)
	}
	if drain {
		b.SetDrainMode(true)
	}
	if tickMs > 0 {
		b.SetTickInterval(time.Duration(tickMs) * time.Millisecond)
	}
}

// Stop closes batchers and database connections.
//
// It does NOT nil the batcher pointer fields. Those are written exactly once, in
// Start(), which happens-before any Create/Spend/Get/SetLocked call — so the
// unsynchronised nil-checks in those hot paths are race-free as long as nothing
// writes the pointers again. Nilling them here (concurrently with an in-flight
// operation reading them) was a data race; go-batcher Close is idempotent, so
// closing without nilling is safe and matches the sql store's Stop().
func (s *Store) Stop() {
	s.stopPrunerCursor()
	// createBatcher MUST close (and fully drain) before stopPendingUnminedProjector:
	// go-batcher's Close() blocks until every queued create has been handed to
	// sendCreateBatch (create.go), whose callback calls enqueuePendingUnmined. If
	// the projector's final drain ran first, any create batch dispatched by
	// Close() afterwards would append to puBuf with no consumer left to flush
	// it — silently lost, while the marker below would still claim "clean".
	if s.createBatcher != nil {
		s.createBatcher.Close()
	}
	s.stopPendingUnminedProjector()
	// stopPendingUnminedProjector's final drain (above) is what makes
	// pending_unmined complete and correct, so the clean-shutdown marker can
	// only be stamped true AFTER it returns. Stamp it here, while s.pool is
	// still open, so the next startup can skip the reconciliation backfill.
	s.markCleanShutdown(context.Background())
	if s.spendBatcher != nil {
		s.spendBatcher.Close()
	}
	if s.getBatcher != nil {
		s.getBatcher.Close()
	}
	if s.unlockBatcher != nil {
		s.unlockBatcher.Close()
	}
	// Close the maintenance pool before the main pool: the cursor is already
	// cancelled by stopPrunerCursor() above, so its in-flight CALL/delete drains first.
	if s.maintPool != nil {
		s.maintPool.Close()
	}
	if s.pool != nil {
		s.pool.Close()
	}
}

// maint returns the dedicated maintenance pool when configured, else the main
// pool (legacy behaviour). Background reclaim (DAH sweep CALLs, pruner cascade
// deletes) uses this so it is never starved by the validator batchers on `pool`.
func (s *Store) maint() *pgxpool.Pool {
	if s.maintPool != nil {
		return s.maintPool
	}

	return s.pool
}

// Close drains any in-flight batched writes and releases the connection pool,
// honouring the supplied context as a deadline. It mirrors the SQL store's
// contract (see utxo.Store): batchers are drained in dependency order with the
// state-mutating writers (spend, create) last so they have the best chance of
// committing before the deadline, and the pool is always closed once the drain
// goroutine finishes so connections are not leaked even if ctx expired first.
// createBatcher's drain must complete before stopPendingUnminedProjector's
// final drain and the clean-shutdown marker write — see markCleanShutdown's
// doc comment for the full precondition chain.
func (s *Store) Close(ctx context.Context) error {
	s.stopPrunerCursor()

	done := make(chan struct{})

	go func() {
		defer close(done)
		// Drain in dependency order: state-mutating writers last, EXCEPT
		// createBatcher — it must close (and fully drain) BEFORE
		// stopPendingUnminedProjector's final drain below. go-batcher's
		// Close() blocks until every queued create has been handed to
		// sendCreateBatch (create.go), whose callback calls
		// enqueuePendingUnmined; running the projector's final drain first
		// would let a create batch dispatched here land in puBuf with no
		// consumer left to flush it — silently lost, while the marker would
		// still claim "clean".
		if s.unlockBatcher != nil {
			s.unlockBatcher.Close()
		}
		if s.getBatcher != nil {
			s.getBatcher.Close()
		}
		if s.spendBatcher != nil {
			s.spendBatcher.Close()
		}
		if s.createBatcher != nil {
			s.createBatcher.Close()
		}
		// stopPendingUnminedProjector is Once-latched and idempotent (see
		// pending_unmined_projector.go), so calling it here is safe even if
		// Stop() already called it. Its final drain is what makes
		// pending_unmined complete and correct — now that createBatcher above
		// has fully drained — so it must run, and the clean-shutdown marker
		// must be stamped, before the pool closes below.
		s.stopPendingUnminedProjector()
		s.markCleanShutdown(ctx)
		// Always close the pool after the batchers drain, even if ctx has
		// already expired, so the connection pool is not leaked.
		if s.maintPool != nil {
			s.maintPool.Close()
		}
		if s.pool != nil {
			s.pool.Close()
		}
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// stopPrunerCursor cancels the background DAH cursor (Worker 2) if a pruner
// service was started, so it does not outlive the store's connection pool.
func (s *Store) stopPrunerCursor() {
	s.prunerServiceMu.Lock()
	ps := s.prunerService
	s.prunerServiceMu.Unlock()
	if pgps, ok := ps.(*postgresPrunerService); ok {
		pgps.stop()
	}
}

// markCleanShutdown stamps store_clean_shutdown.clean = true, signalling to the
// next startup that pending_unmined is complete, so the seq-scan reconciliation
// backfill can be skipped (see resolvePendingUnminedBackfill in schema.go).
// "Complete" is only true once this exact precondition chain has run, in
// order, on both the Stop() and Close() teardown paths:
//
//  1. s.createBatcher.Close() — go-batcher's Close() blocks until every
//     already-queued create has been handed to sendCreateBatch (create.go),
//     whose callback calls enqueuePendingUnmined. Closing any other batcher
//     first is fine; createBatcher specifically MUST close before step 2,
//     otherwise a create batch dispatched by its Close() would append to the
//     projector's puBuf with nothing left to flush it.
//  2. s.stopPendingUnminedProjector() — the writer's final drain flushes
//     whatever createBatcher just enqueued (plus anything already buffered)
//     into pending_unmined, making the table genuinely complete.
//  3. markCleanShutdown (this function) — only now can "clean" be stamped
//     true. Must run while s.pool is still open (before
//     pool.Close()/maintPool.Close()).
//
// Best-effort: a failed UPDATE (e.g. the pool is already draining, or ctx has
// a very short deadline) only costs one extra backfill on the next startup —
// never a correctness problem — so it is logged and swallowed rather than
// propagated, and it never blocks shutdown.
func (s *Store) markCleanShutdown(ctx context.Context) {
	if s.pool == nil {
		return
	}

	// If the projector ever dropped buffered entries at its hard cap, the final
	// drain cannot have re-projected them — pending_unmined may be missing a
	// genuinely-unmined tx. Stamp UNCLEAN so the next startup runs the fail-safe
	// backfill from txs and repairs the gap, rather than trusting an incomplete
	// side-table on the clean-restart fast path.
	if s.puDropped.Load() {
		if _, err := s.pool.Exec(ctx, `UPDATE store_clean_shutdown SET clean = false, stamped_at = now() WHERE id = 1`); err != nil {
			s.logger.Warnf("[markCleanShutdown] failed to stamp unclean-shutdown marker after projector drop: %v", err)
		} else {
			s.logger.Infof("[markCleanShutdown] projector dropped entries this run — recording shutdown as unclean to force next-startup pending_unmined backfill")
		}

		return
	}

	if _, err := s.pool.Exec(ctx, `UPDATE store_clean_shutdown SET clean = true, stamped_at = now() WHERE id = 1`); err != nil {
		s.logger.Warnf("[markCleanShutdown] failed to stamp clean-shutdown marker, next startup will run a full pending_unmined backfill: %v", err)
	}
}

// SupportsOutpointOnlySpend reports that this store honours outpoint-only
// spends: the spend SQL keys purely on (prev_tx_hash, prev_output_idx), so a
// SkipUTXOHashCheck spend built via utxo.GetSpendsOutpointOnly (no parent
// decoration, no UTXO hash) is applied exactly, not silently ignored.
func (s *Store) SupportsOutpointOnlySpend() bool { return true }

// PoolMaxConns returns the configured ceiling of the main pgxpool — the value
// set from pool_max_conns (or the default 80) in New. This is the connection
// budget that peak concurrent in-flight demand must not exceed; callers use it
// to fail closed when a concurrency setting would starve the pool (see the
// window-barrier-collapse guard in block validation). A pooled store never
// returns the 0 sentinel.
func (s *Store) PoolMaxConns() int { return int(s.pool.Config().MaxConns) }

// Health checks the database connection.
func (s *Store) Health(ctx context.Context, _ bool) (int, string, error) {
	var num int
	err := s.pool.QueryRow(ctx, "SELECT 1").Scan(&num)
	if err != nil {
		return 503, "Postgres UTXO Store", err
	}
	return 200, "Postgres UTXO Store", nil
}

func (s *Store) SetBlockHeight(blockHeight uint32) error {
	// Guard the tip at the single chokepoint so every downstream int32(height) cast
	// (e.g. unmined_since in unsetMinedMulti) is provably safe. See blockHeightToInt32.
	if _, err := blockHeightToInt32(blockHeight); err != nil {
		return err
	}
	return s.BlockStateFields.SetBlockHeight(blockHeight)
}

func (s *Store) SetBlockState(blockHeight, medianTime uint32) error {
	// Same int32-cast guard as SetBlockHeight — this is the other write path for
	// the tip height.
	if _, err := blockHeightToInt32(blockHeight); err != nil {
		return err
	}
	return s.BlockStateFields.SetBlockState(blockHeight, medianTime)
}
