package utxo_test

import (
	"context"
	"net/url"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	pgstore "github.com/bsv-blockchain/teranode/stores/utxo/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ---------------------------------------------------------------------------
// Phase-1 (PreserveParentsOfOldUnminedTransactions) measurement bench
// ---------------------------------------------------------------------------
//
// This harness measures the cost of the pruner's phase-1 path
// (PreserveParentsOfOldUnminedTransactions) which calls GetPrunableUnminedTxIterator
// to seq-scan unmined txs older than cutoff height. Establishes BASELINE cost
// BEFORE the pending_unmined side-table exists (a future task optimizes via that
// table).
//
// Workload: create a pool of GENUINELY-UNMINED txs (no MinedBlockInfos), then
// per height tick call the phase-1 path and measure the seq-scan cost.
//
// Run with, e.g.:
//
//	UNMINED_WORKERS=100 UNMINED_VERBOSE=1 \
//	  go test ./stores/utxo/ -run TestThroughput_UnminedPreserve -v -timeout 10m

const (
	// unminedHeightTickMS: how often the shared block height advances.
	unminedHeightTickMS = 500
)

// newUnminedBenchStore builds a fresh postgres queue store on a clean DB
// and returns the concrete *Store plus an explicit stop func.
func newUnminedBenchStore(t *testing.T) (*pgstore.Store, func()) {
	t.Helper()
	cleanDB(t) // Skipf's if postgres unreachable

	ctx := context.Background()
	storeURL, _ := url.Parse(throughputDSN)
	storeURL.Scheme = "postgres"

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 60 * time.Second
	tSettings.UtxoStore.StoreBatcherDurationMillis = 5
	tSettings.UtxoStore.StoreBatcherSize = 500
	// Set a short unmined retention so the phase-1 scan triggers quickly.
	// Default is 144; use 10 so txs created at startHeight=1 are eligible
	// once h - 10 > 1, i.e., h >= 12 (~6s of height ticks).
	tSettings.UtxoStore.UnminedTxRetention = 10
	// Short parent preservation for the bench (default 1440 is very long).
	tSettings.UtxoStore.ParentPreservationBlocks = 50
	// Large retention so nothing gets deleted during the bench,
	// keeping the unmined pool stable.
	tSettings.GlobalBlockHeightRetention = 100

	s, err := pgstore.New(ctx, ulogger.TestLogger{}, tSettings, storeURL)
	if err != nil {
		t.Fatalf("unmined bench store: %v", err)
	}
	s.Start(ctx)
	return s, func() { s.Stop() }
}

// buildUnminedGenesis creates numWorkers genesis txs starting at workerIDOffset
// and seeds the block height to startHeight. The returned parents slice and curH
// are shared across warmup and timed-measurement phases to avoid TX_EXISTS
// collisions from duplicate genesis creation.
func buildUnminedGenesis(t *testing.T, store *pgstore.Store, numWorkers, workerIDOffset int) ([]*bt.Tx, *atomic.Uint32) {
	t.Helper()
	ctx := context.Background()
	// Use a low startHeight so that within the warmup (2s at 500ms/tick = ~4 ticks),
	// the block height advances enough that the cutoff (h - UnminedTxRetention=10)
	// covers txs created at startHeight=1. At height ~12 the cutoff becomes 2 which
	// is > 1, so all genesis+child txs created at height 1 become eligible.
	const startHeight = uint32(1)

	var curH atomic.Uint32
	curH.Store(startHeight)
	_ = store.SetBlockHeight(startHeight)

	parents := make([]*bt.Tx, numWorkers)
	{
		conc := runtime.GOMAXPROCS(0) * 16
		if conc > numWorkers {
			conc = numWorkers
		}

		var gwg sync.WaitGroup
		var genErr atomic.Value
		sem := make(chan struct{}, conc)

		for i := 0; i < numWorkers; i++ {
			i := i
			sem <- struct{}{}
			gwg.Add(1)

			go func() {
				defer gwg.Done()
				defer func() { <-sem }()

				g := makeGenesisTx(i + workerIDOffset)
				if _, cErr := store.Create(ctx, g, startHeight); cErr != nil {
					genErr.Store(cErr)
					return
				}
				parents[i] = g
			}()
		}

		gwg.Wait()

		if e := genErr.Load(); e != nil {
			t.Fatalf("unmined pool genesis create: %v", e.(error))
		}
	}

	return parents, &curH
}

// runUnminedPreservationMeasurement drives the phase-1 pruner path repeatedly
// at advancing block heights. Returns per-rep elapsed time samples (in seconds).
//
// parents and curH are shared state so that successive calls (warmup then
// measured) continue from the same chain tips and block height — avoiding
// TX_EXISTS collisions from duplicate genesis creation.
func runUnminedPreservationMeasurement(
	t *testing.T,
	store *pgstore.Store,
	parents []*bt.Tx,
	curH *atomic.Uint32,
	reps int,
	verbose bool,
) []float64 {
	t.Helper()
	ctx := context.Background()

	// Build settings to pass to PreserveParentsOfOldUnminedTransactions.
	// store.settings is private so we construct equivalent values here.
	benchSettings := test.CreateBaseTestSettings(t)
	benchSettings.UtxoStore.UnminedTxRetention = 10
	benchSettings.UtxoStore.ParentPreservationBlocks = 50

	numWorkers := len(parents)

	// HEIGHT: advance the chain independently.
	driverCtx, cancel := context.WithCancel(ctx)
	var driverWG sync.WaitGroup
	driverWG.Add(1)
	go func() {
		defer driverWG.Done()
		tk := time.NewTicker(unminedHeightTickMS * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-driverCtx.Done():
				return
			case <-tk.C:
				_ = store.SetBlockHeight(curH.Add(1))
			}
		}
	}()

	// WORKER GOROUTINES: continuously create unmined child txs (no mining).
	// This keeps the unmined pool growing, driving up the seq-scan cost over time.
	driverWG.Add(1)
	go func() {
		defer driverWG.Done()
		var wg sync.WaitGroup
		wg.Add(numWorkers)

		for w := 0; w < numWorkers; w++ {
			w := w
			go func() {
				defer wg.Done()
				parent := parents[w]
				if parent == nil {
					return
				}
				for {
					select {
					case <-driverCtx.Done():
						// Persist the last parent so the next phase can continue the chain.
						parents[w] = parent
						return
					default:
						h := curH.Load()
						child := makeChildTx(parent)
						if _, err := store.Create(ctx, child, h); err != nil {
							parents[w] = parent
							return
						}
						parent = child
					}
				}
			}()
		}

		wg.Wait()
	}()

	// Let workers build up the unmined pool and let the block height advance
	// enough that the cutoff (h - UnminedTxRetention=10) reaches the txs
	// created at startHeight=1. We need h >= 12 which takes ~11 ticks at
	// 500ms/tick = 5.5s. Use 6s to give a comfortable margin.
	time.Sleep(6 * time.Second)

	// MEASUREMENT: call PreserveParentsOfOldUnminedTransactions repeatedly.
	// The phase-1 path calls GetPrunableUnminedTxIterator(cutoff), which does
	// a seq-scan filtered by unmined_since <= cutoff. This bench measures that
	// scan cost at each tick.
	samples := make([]float64, 0, reps)
	logger := ulogger.TestLogger{}

	for rep := 0; rep < reps; rep++ {
		if rep > 0 {
			time.Sleep(200 * time.Millisecond) // inter-rep breathing room
		}

		h := curH.Load()
		start := time.Now()

		// Call the store-agnostic phase-1 pruner function.
		// Internally calls store.GetPrunableUnminedTxIterator(cutoff) and iterates batches.
		processed, err := utxo.PreserveParentsOfOldUnminedTransactions(
			ctx, store, h, "bench", benchSettings, logger,
		)

		elapsed := time.Since(start).Seconds()
		samples = append(samples, elapsed)

		if verbose {
			var live int64
			if statPool, pErr := pgxpool.New(context.Background(), throughputDSN); pErr == nil {
				_ = statPool.QueryRow(context.Background(),
					`SELECT COALESCE(sum(n_live_tup),0) FROM pg_stat_user_tables WHERE relname LIKE 'txs%'`,
				).Scan(&live)
				statPool.Close()
			}

			errStr := ""
			if err != nil {
				errStr = " ERROR: " + err.Error()
			}
			t.Logf("[unmined-preserve] rep=%d/%d height=%d processed=%d elapsed=%.3fs pool_rows=%d%s",
				rep+1, reps, h, processed, elapsed, live, errStr)
		}
	}

	cancel()
	driverWG.Wait()
	return samples
}

func TestThroughput_UnminedPreserve(t *testing.T) {
	terminateOtherConnections(t)

	numWorkers := envInt("UNMINED_WORKERS", 100)
	reps := envInt("UNMINED_REPS", 5)
	verbose := envInt("UNMINED_VERBOSE", 0) != 0
	unstableCV := 25.0 // 25% acceptable for a scan-cost bench (I/O + pool-growth variance)

	store, stop := newUnminedBenchStore(t)
	defer stop()

	t.Logf("[Unmined PreserveParents] workers=%d height_tick=%dms reps=%d unmined_retention=10",
		numWorkers, unminedHeightTickMS, reps)

	// Build shared genesis state once. Use a large offset to avoid TX_EXISTS
	// collisions with other throughput harnesses (which use offsets 0..N).
	const workerIDOffset = 5_000_000
	parents, curH := buildUnminedGenesis(t, store, numWorkers, workerIDOffset)

	// Warmup (discarded): lets the height ticker and worker creation reach steady state.
	// Shares parents/curH so workers continue their chains into the timed phase.
	_ = runUnminedPreservationMeasurement(t, store, parents, curH, 1, false)

	// Timed measurements: GetPrunableUnminedTxIterator seq-scan cost.
	samples := runUnminedPreservationMeasurement(t, store, parents, curH, reps, verbose)

	st := summarize(samples)
	t.Logf("[Unmined PreserveParents] BASELINE median=%v mean=%v CV=%.1f%% range=[%v, %v] n=%d%s",
		time.Duration(int64(st.median*1e9)), time.Duration(int64(st.mean*1e9)), st.cv,
		time.Duration(int64(st.min*1e9)), time.Duration(int64(st.max*1e9)), st.n,
		unstableFlag(st.cv, unstableCV))
}
