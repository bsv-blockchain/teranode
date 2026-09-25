package utxo_test

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	pgstore "github.com/bsv-blockchain/teranode/stores/utxo/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ---------------------------------------------------------------------------
// Stable throughput harness
// ---------------------------------------------------------------------------
//
// TestThroughput_QueueStore (in throughput_test.go) measures each worker count
// exactly ONCE over a fixed op-count. At moderate concurrency that window is
// short and batcher-flush-timer-bound, so a single sample has 30–60% run-to-run
// variance — a single A/B comparison there is meaningless.
//
// This harness fixes the measurement methodology (not the store) so a number is
// trustworthy:
//
//   - Warmup phase, excluded from timing: opens the whole connection pool, primes
//     PostgreSQL plan/buffer caches, and brings the batchers to steady cadence
//     before the clock starts.
//   - Fixed-DURATION windows instead of fixed op-count: every cell runs for the
//     same wall-clock time, so the per-sample averaging (and thus the relative
//     variance) is comparable across worker counts. Longer windows also amortise
//     the goroutine-launch ramp.
//   - REPS per cell with median + coefficient-of-variation reporting, and an
//     explicit "UNSTABLE" flag when CV exceeds a threshold — so a noisy cell is
//     never silently read as signal.
//   - A FRESH store (clean DB) per worker count, so cells don't inherit each
//     other's table size / bloat / cache state (removes order coupling).
//
// Tier-2 follow-ups not done here: VACUUM/CHECKPOINT barriers and autovacuum
// pausing around the timed window, and raising the instance's max_wal_size so a
// checkpoint cannot land mid-run. Those need DB-admin privileges / instance
// tuning and are tracked separately.
//
// Tunable via env (all optional):
//   THROUGHPUT_REPS=5  THROUGHPUT_WARMUP_MS=2000  THROUGHPUT_MEASURE_MS=4000
//   THROUGHPUT_WORKERS=100,500,1000,5000,10000,15000

type stableCfg struct {
	reps       int
	warmup     time.Duration
	measure    time.Duration
	workers    []int
	unstableCV float64 // CV (%) above which a cell is flagged unreliable
	tier2      bool    // checkpoint/autovacuum control around the timed window
	verbose    bool    // per-rep timestamped TPS line (for sustained-run observation)
}

func defaultStableCfg() stableCfg {
	return stableCfg{
		reps:       envInt("THROUGHPUT_REPS", 5),
		warmup:     time.Duration(envInt("THROUGHPUT_WARMUP_MS", 2000)) * time.Millisecond,
		measure:    time.Duration(envInt("THROUGHPUT_MEASURE_MS", 4000)) * time.Millisecond,
		workers:    envWorkers("THROUGHPUT_WORKERS", []int{100, 500, 1000, 5000, 10000, 15000}),
		unstableCV: 10.0,
		// Default on; disabled only when explicitly set to "0". (Cannot use envInt
		// here — it rejects non-positive values, so "0" would fall through to the
		// default and silently leave Tier 2 enabled.)
		tier2:   os.Getenv("THROUGHPUT_TIER2") != "0",
		verbose: os.Getenv("THROUGHPUT_VERBOSE") != "",
	}
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func envWorkers(key string, def []int) []int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	out := make([]int, 0, len(def))
	for _, part := range strings.Split(v, ",") {
		if n, err := strconv.Atoi(strings.TrimSpace(part)); err == nil && n > 0 {
			out = append(out, n)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}

// ---------------------------------------------------------------------------
// Tier 2: checkpoint / autovacuum control
// ---------------------------------------------------------------------------
//
// The default-window sweep showed that at high concurrency the timed window is
// dominated by PostgreSQL CHECKPOINT and autovacuum I/O landing mid-measurement
// (reps that dodged a checkpoint hit ~110K TPS, reps that ate one fell to ~53K).
// pgAdmin moves that I/O out of the timed window, using a dedicated admin pool
// separate from the store's pool. Everything is best-effort and logged: table-
// level controls (VACUUM/ANALYZE, autovacuum_enabled) work as table owner;
// CHECKPOINT and the instance GUCs need superuser / pg_checkpoint and are simply
// skipped (with a one-time note) when the role lacks the privilege.
type pgAdmin struct {
	pool       *pgxpool.Pool
	super      bool
	logf       func(string, ...interface{})
	noted      map[string]bool
	tables     []string
	vacTargets []string // leaf partitions for autovacuum_enabled toggling
	tunedGUC   bool
}

func newPgAdmin(ctx context.Context, logf func(string, ...interface{}), tables []string) (*pgAdmin, error) {
	pool, err := pgxpool.New(ctx, throughputDSN)
	if err != nil {
		return nil, err
	}
	a := &pgAdmin{
		pool:   pool,
		logf:   logf,
		noted:  map[string]bool{},
		tables: tables, // ANALYZE / autovacuum targets — store-specific table names
	}
	_ = pool.QueryRow(ctx, `SELECT current_setting('is_superuser')::bool`).Scan(&a.super)
	return a, nil
}

func (a *pgAdmin) close() {
	if a != nil && a.pool != nil {
		a.pool.Close()
	}
}

// noteOnce logs the first failure of a given operation, then stays quiet so a
// per-rep best-effort op that the role can't perform doesn't spam the log.
func (a *pgAdmin) noteOnce(op string, err error) {
	if a.noted[op] {
		return
	}
	a.noted[op] = true
	a.logf("[tier2] %s skipped (best-effort): %v", op, err)
}

// tuneInstance widens the checkpoint budget so a single timed window is very
// unlikely to trigger a max_wal_size checkpoint. Requires superuser; restored by
// restoreInstance. Safe to leave applied (a larger WAL budget is not harmful).
func (a *pgAdmin) tuneInstance(ctx context.Context) {
	if !a.super {
		a.logf("[tier2] instance GUC tuning skipped (not superuser); relying on per-rep CHECKPOINT barrier")
		return
	}
	stmts := []string{
		`ALTER SYSTEM SET max_wal_size = '8GB'`,
		`ALTER SYSTEM SET checkpoint_timeout = '30min'`,
		`ALTER SYSTEM SET checkpoint_completion_target = 0.9`,
	}
	for _, s := range stmts {
		if _, err := a.pool.Exec(ctx, s); err != nil {
			a.noteOnce("ALTER SYSTEM", err)
			return
		}
	}
	if _, err := a.pool.Exec(ctx, `SELECT pg_reload_conf()`); err != nil {
		a.noteOnce("pg_reload_conf", err)
		return
	}
	a.tunedGUC = true
	a.logf("[tier2] instance tuned: max_wal_size=8GB checkpoint_timeout=30min completion_target=0.9")
}

func (a *pgAdmin) restoreInstance(ctx context.Context) {
	if !a.tunedGUC {
		return
	}
	for _, p := range []string{"max_wal_size", "checkpoint_timeout", "checkpoint_completion_target"} {
		_, _ = a.pool.Exec(ctx, "ALTER SYSTEM RESET "+p)
	}
	_, _ = a.pool.Exec(ctx, `SELECT pg_reload_conf()`)
}

// resolveVacTargets discovers the leaf tables autovacuum settings must target.
// txs/outputs/spends are partitioned parents, and storage parameters such as
// autovacuum_enabled can only be set on the leaf partitions (e.g. txs_p00), not
// on the partitioned parent. Resolved lazily, after the store has created the
// schema. Falls back to the parent name for any non-partitioned table.
func (a *pgAdmin) resolveVacTargets(ctx context.Context) {
	if a.vacTargets != nil {
		return
	}
	a.vacTargets = []string{}
	for _, parent := range a.tables {
		rows, err := a.pool.Query(ctx, `
			SELECT c.relname
			FROM pg_inherits i
			JOIN pg_class c ON c.oid = i.inhrelid
			JOIN pg_class p ON p.oid = i.inhparent
			WHERE p.relname = $1`, parent)
		if err != nil {
			a.noteOnce("resolve partitions", err)
			a.vacTargets = append(a.vacTargets, parent)
			continue
		}
		found := false
		for rows.Next() {
			var child string
			if scanErr := rows.Scan(&child); scanErr == nil {
				a.vacTargets = append(a.vacTargets, child)
				found = true
			}
		}
		rows.Close()
		if !found {
			a.vacTargets = append(a.vacTargets, parent) // not partitioned
		}
	}
}

// setAutovacuum toggles autovacuum on the bench tables' leaf partitions. Paused
// during a cell so autovacuum workers don't steal I/O mid-window; the explicit
// ANALYZE/CHECKPOINT barriers take over keeping stats and dirty-buffer state in
// hand.
func (a *pgAdmin) setAutovacuum(ctx context.Context, enabled bool) {
	a.resolveVacTargets(ctx)
	for _, tbl := range a.vacTargets {
		stmt := fmt.Sprintf("ALTER TABLE %s SET (autovacuum_enabled = %t)", tbl, enabled)
		if _, err := a.pool.Exec(ctx, stmt); err != nil {
			a.noteOnce("ALTER TABLE autovacuum_enabled", err)
			return
		}
	}
}

// analyze refreshes planner stats after the warmup writes (sampled, fast).
func (a *pgAdmin) analyze(ctx context.Context) {
	if _, err := a.pool.Exec(ctx, "ANALYZE "+strings.Join(a.tables, ", ")); err != nil {
		a.noteOnce("ANALYZE", err)
	}
}

// checkpoint flushes all dirty buffers and resets the WAL distance immediately
// before a timed window, so an in-window checkpoint becomes very unlikely.
func (a *pgAdmin) checkpoint(ctx context.Context) {
	if _, err := a.pool.Exec(ctx, "CHECKPOINT"); err != nil {
		a.noteOnce("CHECKPOINT", err)
	}
}

// newStableQueueStore builds a fresh postgres queue store on a clean DB and
// returns it with an explicit stop func, so the caller can close it (and free
// its connection pool) before opening the next cell's store — exactly one store
// is alive at a time. Mirrors newQueueStoreForBench but does not register a
// t.Cleanup, which would otherwise keep every cell's pool open until the end and
// exhaust max_connections.
func newStableQueueStore(t *testing.T) (benchStore, func()) {
	t.Helper()
	cleanDB(t) // t.Skipf's the whole test if postgres is unreachable
	ctx := context.Background()

	storeURL, _ := url.Parse(throughputDSN)
	storeURL.Scheme = "postgres"

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 60 * time.Second
	tSettings.UtxoStore.SpendBatcherDurationMillis = 5
	tSettings.UtxoStore.StoreBatcherDurationMillis = 5
	tSettings.UtxoStore.SpendBatcherSize = 500
	tSettings.UtxoStore.StoreBatcherSize = 500

	s, err := pgstore.New(ctx, ulogger.TestLogger{}, tSettings, storeURL)
	if err != nil {
		t.Fatalf("queue store: %v", err)
	}
	s.Start(ctx)
	return s, func() { s.Stop() }
}

// runStableValidator runs the validator hot path (Get+Spend+Create+Unlock per
// tx) for numWorkers concurrent chains, with one untimed warmup phase followed
// by cfg.reps timed phases of cfg.measure each. It returns the per-rep TPS.
func runStableValidator(t *testing.T, store benchStore, numWorkers, workerOffset int, cfg stableCfg, admin *pgAdmin) []float64 {
	t.Helper()
	ctx := context.Background()
	const blockHeight = uint32(200)
	_ = store.SetBlockHeight(blockHeight)

	// Pause autovacuum on the bench tables for the duration of this cell so an
	// autovacuum worker can't steal I/O inside a timed window; resume on exit.
	if admin != nil {
		admin.setAutovacuum(ctx, false)
		defer admin.setAutovacuum(ctx, true)
	}

	// Genesis tx per worker (untimed). Each worker extends its own chain from here.
	parents := make([]*bt.Tx, numWorkers)
	for i := 0; i < numWorkers; i++ {
		g := makeGenesisTx(i + workerOffset)
		if _, err := store.Create(ctx, g, blockHeight); err != nil {
			t.Fatalf("genesis create worker %d: %v", i, err)
		}
		parents[i] = g
	}

	// runPhase drives all workers for dur, advancing each worker's chain, and
	// returns the number of completed Get+Spend+Create+Unlock cycles. Each worker
	// hands its final parent back via parents[w] so the next phase continues the
	// chain rather than re-paying genesis cost.
	runPhase := func(dur time.Duration) int64 {
		var ops atomic.Int64
		var wg sync.WaitGroup
		wg.Add(numWorkers)
		deadline := time.Now().Add(dur)

		for w := 0; w < numWorkers; w++ {
			w := w
			go func() {
				defer wg.Done()
				parent := parents[w]
				// Always hand back the last fully-committed parent so a worker that
				// stops early (deadline or error) does not strand the next phase on
				// an already-spent output.
				defer func() { parents[w] = parent }()

				for time.Now().Before(deadline) {
					child := makeChildTx(parent)
					parentHash := parent.TxIDChainHash()

					if _, err := store.Get(ctx, parentHash, fields.BlockIDs, fields.BlockHeights); err != nil {
						return
					}
					if _, err := store.Spend(ctx, child, blockHeight); err != nil {
						return
					}
					if _, err := store.Create(ctx, child, blockHeight, utxo.WithLocked(true)); err != nil {
						return
					}
					if err := store.SetLocked(ctx, []chainhash.Hash{*child.TxIDChainHash()}, false); err != nil {
						return
					}

					parent = child
					ops.Add(1)
				}
			}()
		}
		wg.Wait()
		return ops.Load()
	}

	// Warmup: discarded. Brings pool, plan cache, and batchers to steady state.
	if cfg.warmup > 0 {
		_ = runPhase(cfg.warmup)
	}

	// Refresh planner stats once after the warmup writes (sampled, fast).
	if admin != nil {
		admin.analyze(ctx)
	}

	samples := make([]float64, 0, cfg.reps)
	for r := 0; r < cfg.reps; r++ {
		// Flush dirty buffers + reset WAL distance right before the timed window
		// so a checkpoint is very unlikely to fire during it.
		if admin != nil {
			admin.checkpoint(ctx)
		}
		start := time.Now()
		ops := runPhase(cfg.measure)
		elapsed := time.Since(start)
		if elapsed <= 0 {
			continue
		}
		tps := float64(ops) / elapsed.Seconds()
		samples = append(samples, tps)
		// Per-rep timestamped line: lets a sustained run be correlated against an
		// external pg-stats monitor (checkpoint/autovacuum/WAL) to see which reps a
		// background event landed in. Default-off so the normal sweep stays terse.
		if cfg.verbose {
			t.Logf("[rep] %s workers=%d rep=%d/%d tps=%.0f", time.Now().Format("15:04:05"), numWorkers, r+1, cfg.reps, tps)
		}
	}
	return samples
}

type stableStats struct {
	median, mean, cv, min, max float64
	n                          int
}

func summarize(samples []float64) stableStats {
	n := len(samples)
	if n == 0 {
		return stableStats{}
	}
	sorted := append([]float64(nil), samples...)
	sort.Float64s(sorted)

	var sum float64
	for _, v := range sorted {
		sum += v
	}
	mean := sum / float64(n)

	var med float64
	if n%2 == 1 {
		med = sorted[n/2]
	} else {
		med = (sorted[n/2-1] + sorted[n/2]) / 2
	}

	var variance float64
	for _, v := range sorted {
		d := v - mean
		variance += d * d
	}
	var sd float64
	if n > 1 {
		sd = math.Sqrt(variance / float64(n-1))
	}
	cv := 0.0
	if mean > 0 {
		cv = 100 * sd / mean
	}
	return stableStats{median: med, mean: mean, cv: cv, min: sorted[0], max: sorted[n-1], n: n}
}

// TestThroughput_QueueStoreStable is the consistency-focused counterpart to
// TestThroughput_QueueStore. It is skipped automatically when postgres is not
// reachable (via cleanDB). Run explicitly with, e.g.:
//
//	go test ./stores/utxo/ -run TestThroughput_QueueStoreStable -v -timeout 30m
//
// stableTarget describes a store to put through the stable harness.
type stableTarget struct {
	label       string   // for the report header
	adminTables []string // tables the Tier-2 layer ANALYZEs / pauses autovacuum on
	newStore    func(t *testing.T) (benchStore, func())
	reuseStore  bool // reuse one store across the whole sweep (for stores without a Close())
}

// queueTarget benches the new postgres queue store: fresh store per cell (it has
// a Stop()), Tier-2 controls target txs/outputs/spends.
func queueTarget() stableTarget {
	return stableTarget{
		label:       "Queue (postgres) Store",
		adminTables: []string{"txs", "outputs", "spends"},
		newStore:    newStableQueueStore,
		reuseStore:  false,
	}
}

// sqlTarget benches the legacy SQL store. It has no Close(), so a fresh store per
// cell would leak its 100-conn pool; reuse one store across the sweep (matching
// the original TestThroughput_SQLStore). Tier-2 targets the SQL store's tables.
func sqlTarget() stableTarget {
	return stableTarget{
		label:       "SQL Store (baseline)",
		adminTables: []string{"transactions", "outputs", "inputs"},
		newStore:    func(t *testing.T) (benchStore, func()) { return newSQLStoreForBench(t), func() {} },
		reuseStore:  true,
	}
}

func TestThroughput_QueueStoreStable(t *testing.T) {
	runStableSweep(t, queueTarget())
}

// TestThroughput_SQLStoreStable runs the legacy SQL UTXO store through the same
// warmup + reps + median/CV + Tier-2 (checkpoint/autovacuum) methodology, so its
// sustained throughput is a like-for-like baseline against the queue store.
// Skipped automatically when postgres is unreachable. Run with:
//
//	go test ./stores/utxo/ -run TestThroughput_SQLStoreStable -v -timeout 30m
func TestThroughput_SQLStoreStable(t *testing.T) {
	runStableSweep(t, sqlTarget())
}

func runStableSweep(t *testing.T, tgt stableTarget) {
	terminateOtherConnections(t)
	cfg := defaultStableCfg()

	t.Logf("[%s] stable harness: reps=%d warmup=%s measure=%s workers=%v tier2=%t (CV>%.0f%% flagged unstable)",
		tgt.label, cfg.reps, cfg.warmup, cfg.measure, cfg.workers, cfg.tier2, cfg.unstableCV)

	// Tier-2 checkpoint/autovacuum control (best-effort; gated by THROUGHPUT_TIER2).
	var admin *pgAdmin
	if cfg.tier2 {
		ctx := context.Background()
		a, err := newPgAdmin(ctx, t.Logf, tgt.adminTables)
		if err != nil {
			t.Logf("[tier2] disabled: cannot open admin pool: %v", err)
		} else {
			admin = a
			admin.tuneInstance(ctx)
			defer func() {
				admin.restoreInstance(ctx)
				admin.close()
			}()
		}
	}

	// For reuse-mode stores (no Close), build one store up front and keep it for
	// the whole sweep; otherwise build a fresh one per cell.
	var sharedStore benchStore
	var sharedStop func()
	if tgt.reuseStore {
		sharedStore, sharedStop = tgt.newStore(t)
		defer sharedStop()
	}

	type cell struct {
		workers int
		st      stableStats
	}
	results := make([]cell, 0, len(cfg.workers))

	offset := 0
	for _, w := range cfg.workers {
		store := sharedStore
		stop := func() {}
		if !tgt.reuseStore {
			store, stop = tgt.newStore(t)
		}

		samples := runStableValidator(t, store, w, offset, cfg, admin)
		stop()
		offset += w

		st := summarize(samples)
		results = append(results, cell{workers: w, st: st})
		t.Logf("[%s] workers=%-6d median=%9.0f mean=%9.0f CV=%5.1f%% range=[%.0f, %.0f] n=%d%s",
			tgt.label, w, st.median, st.mean, st.cv, st.min, st.max, st.n, unstableFlag(st.cv, cfg.unstableCV))
	}

	t.Logf("==== %s stable throughput — Get+Spend+Create+Unlock per tx (median TPS) ====", tgt.label)
	for _, c := range results {
		t.Logf("%6d workers | median %9.0f TPS | CV %5.1f%% | [%.0f, %.0f]%s",
			c.workers, c.st.median, c.st.cv, c.st.min, c.st.max, unstableFlag(c.st.cv, cfg.unstableCV))
	}
}

func unstableFlag(cv, threshold float64) string {
	if cv > threshold {
		return "  ⚠ UNSTABLE (do not compare)"
	}
	return ""
}
