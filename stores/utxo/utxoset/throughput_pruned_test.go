package utxoset

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// A like-for-like port of the append-only store's balanced throughput harness
// (PR 684, stores/utxo/throughput_pruned_test.go, TestThroughput_QueueStorePruned),
// so the two stores can be read off the same measurement.
//
// It is a port, not a rewrite. Everything that sets the number is copied: the same
// validator hot path per iteration (Get, batched BatchPreviousOutputsDecorate, Spend,
// Create-with-locked, SetLocked-false), the same transaction shape, the same worker
// chain-per-goroutine structure, the same background height ticker, the same 12 miner
// goroutines batching 4000 hashes into SetMinedMulti, the same continuously-running
// pruner service, the same bounded hand-off channel, the same warmup-then-reps timing,
// and the same reported unit: sustainable transactions per second, median over reps.
//
// Three things could NOT be copied, and each one is a caveat on the comparison rather
// than a detail:
//
//  1. There is no DAH sweep to run. In the append-only store the spend inserts a row and
//     a background sweep later stamps the parent for deletion; the harness measured the
//     rate at which creation and that reclaim pipeline balance. Here the spend IS the
//     delete, so the reclaim the other harness back-pressures against does not exist.
//
//  2. The table-size gate therefore never fires. It is kept, wired to the same 1.5M-row
//     cap on the table that holds UTXO state, because removing it would quietly change
//     the workload. The run logs how often it fired so a reader can check it did not.
//
//  3. This store's pruner reclaims retired spend-journal leaves and aged transaction-body
//     windows, on horizons of 1440 and 288 blocks. A run of this length never reaches
//     them, so the pruner spins and finds nothing. Growth in tx_ident and tx_body over
//     the run is logged for exactly that reason: it is the cost this measurement does
//     not charge.
//
// Run against a local native PostgreSQL:
//
//	UTXOSET_TEST_DSN='postgres://.../db' THROUGHPUT_WORKERS=1000,5000,10000 \
//	  go test ./stores/utxo/utxoset/ -run TestThroughput_UtxosetPruned -count=1 -v -timeout 30m

const (
	// tpRetention matches prunedRetention in the ported harness.
	tpRetention = 20
	// tpHeightTickMS matches prunedHeightTickMS.
	tpHeightTickMS = 250
	// tpMiners matches prunedMiners.
	tpMiners = 12
	// tpMineBatch matches prunedMineBatch.
	tpMineBatch = 4000
	// tpMineChanCap matches prunedMineChanCap.
	tpMineChanCap = 50000
	// tpTableCapRows matches prunedTableCapRows.
	tpTableCapRows = 1_500_000
	// tpStartHeight matches the ported harness's startHeight.
	tpStartHeight = int64(200)
)

// tpDecorateReq is one worker's request to extend a transaction's inputs from the store.
type tpDecorateReq struct {
	tx    *bt.Tx
	errCh chan error
}

func tpEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}

	return def
}

func tpEnvWorkers(key string, def []int) []int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}

	out := make([]int, 0, 4)

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

// tpDSN is the DSN the harness connects through, with the connection ceiling made
// explicit.
//
// The append-only store forces pool_max_conns to 80 inside its own constructor. This
// store takes whatever the URL carries, and pgx's default is one connection per CPU,
// which on this box is 16. Left alone the comparison would be 80 connections against 16
// and would be measuring pool width, not the store. So the same 80 is set here unless
// the caller has already chosen a value.
func tpDSN(t testing.TB) string {
	t.Helper()

	u, err := url.Parse(testDSN(t))
	require.NoError(t, err)

	q := u.Query()
	if !q.Has("pool_max_conns") {
		q.Set("pool_max_conns", "80")
		u.RawQuery = q.Encode()
	}

	return u.String()
}

// tpCleanDB drops this store's tables, the counterpart of the ported harness's cleanDB.
func tpCleanDB(t *testing.T, dsn string) {
	t.Helper()

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)

	defer pool.Close()

	require.NoError(t, pool.Ping(ctx))

	_, _ = pool.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
	                       WHERE datname = current_database() AND pid != pg_backend_pid()`)

	_, err = pool.Exec(ctx, `DROP TABLE IF EXISTS utxo CASCADE;
	                         DROP TABLE IF EXISTS spend_journal CASCADE;
	                         DROP TABLE IF EXISTS tx_ident CASCADE;
	                         DROP TABLE IF EXISTS tx_body CASCADE;
	                         DROP TABLE IF EXISTS applied_block CASCADE;
	                         DROP TABLE IF EXISTS applied_chunk CASCADE;`)
	require.NoError(t, err)
}

// tpP2PKHScript is the ported harness's p2pkhScript.
func tpP2PKHScript() *bscript.Script {
	s := bscript.Script([]byte{0x76, 0xa9, 0x14,
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
		0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14,
		0x88, 0xac})

	return &s
}

// tpTxOutputs is the ported harness's reprTxOutputs: 1 by default, raised to make the
// serialized transaction representative of a real mainnet parent so the decorate read
// pays a realistic parse cost.
var tpTxOutputs = tpEnvInt("THROUGHPUT_TX_OUTPUTS", 1)

// tpSeed gives every padding output distinct bytes, so the serialized body does not
// compress away and hide its own size.
var tpSeed atomic.Uint64

// tpPadOutputs is the ported harness's padReprOutputs.
func tpPadOutputs(tx *bt.Tx) {
	for i := 1; i < tpTxOutputs; i++ {
		const n = 30 // about a P2PKH-sized output payload

		b := make([]byte, 0, 3+n)
		b = append(b, bscript.OpFALSE, bscript.OpRETURN, byte(n))
		s := tpSeed.Add(1)

		for j := 0; j < n; j++ {
			b = append(b, byte(s>>(uint(j%8)*8))^byte(j*7+i)) //nolint:gosec // bench padding
		}

		tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: 0, LockingScript: bscript.NewFromBytes(b)})
	}
}

// tpGenesisTx is the ported harness's makeGenesisTx.
func tpGenesisTx(workerID int) *bt.Tx {
	tx := bt.NewTx()
	tx.Version = 1

	var h [32]byte

	h[0] = byte(workerID)
	h[1] = byte(workerID >> 8)
	h[2] = byte(workerID >> 16)
	h[3] = byte(workerID >> 24)
	h[4] = 0xFF

	prev, _ := chainhash.NewHash(h[:])
	_ = tx.From(prev.String(), 0, tpP2PKHScript().String(), 100_000_000)
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00})
	_ = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 50_000_000)
	tpPadOutputs(tx)

	return tx
}

// tpChildTx is the ported harness's makeChildTx.
func tpChildTx(parent *bt.Tx) *bt.Tx {
	tx := bt.NewTx()
	tx.Version = 1

	_ = tx.From(
		parent.TxIDChainHash().String(), 0,
		parent.Outputs[0].LockingScript.String(),
		parent.Outputs[0].Satoshis,
	)
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00})

	outVal := parent.Outputs[0].Satoshis / 2
	if outVal == 0 {
		outVal = 1
	}

	_ = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", outVal)
	tpPadOutputs(tx)

	return tx
}

// tpCfg is the ported harness's stableCfg, with the same defaults.
type tpCfg struct {
	reps       int
	warmup     time.Duration
	measure    time.Duration
	workers    []int
	unstableCV float64
	verbose    bool
}

func tpDefaultCfg() tpCfg {
	return tpCfg{
		reps:       tpEnvInt("THROUGHPUT_REPS", 5),
		warmup:     time.Duration(tpEnvInt("THROUGHPUT_WARMUP_MS", 2000)) * time.Millisecond,
		measure:    time.Duration(tpEnvInt("THROUGHPUT_MEASURE_MS", 4000)) * time.Millisecond,
		workers:    tpEnvWorkers("THROUGHPUT_WORKERS", []int{1000, 5000, 10000}),
		unstableCV: 10.0,
		verbose:    os.Getenv("THROUGHPUT_VERBOSE") != "",
	}
}

type tpStats struct {
	median, mean, cv, min, max float64
	n                          int
}

func tpSummarize(samples []float64) tpStats {
	if len(samples) == 0 {
		return tpStats{}
	}

	sorted := append([]float64(nil), samples...)
	sort.Float64s(sorted)

	st := tpStats{n: len(sorted), min: sorted[0], max: sorted[len(sorted)-1]}

	if len(sorted)%2 == 1 {
		st.median = sorted[len(sorted)/2]
	} else {
		st.median = (sorted[len(sorted)/2-1] + sorted[len(sorted)/2]) / 2
	}

	var sum float64
	for _, v := range sorted {
		sum += v
	}

	st.mean = sum / float64(len(sorted))

	var sq float64
	for _, v := range sorted {
		sq += (v - st.mean) * (v - st.mean)
	}

	if st.mean > 0 {
		st.cv = math.Sqrt(sq/float64(len(sorted))) / st.mean * 100
	}

	return st
}

func tpUnstableFlag(cv, limit float64) string {
	if cv > limit {
		return "  <-- UNSTABLE"
	}

	return ""
}

// tpNewStore builds a fresh store on a clean database, with the same batcher and
// retention settings the ported harness configures.
func tpNewStore(t *testing.T, dsn string) (*Store, func()) {
	t.Helper()

	tpCleanDB(t, dsn)

	ctx := context.Background()

	storeURL, err := url.Parse(dsn)
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 60 * time.Second
	tSettings.UtxoStore.SpendBatcherDurationMillis = 5
	tSettings.UtxoStore.StoreBatcherDurationMillis = 5
	tSettings.UtxoStore.SpendBatcherSize = 500
	tSettings.UtxoStore.StoreBatcherSize = 500
	tSettings.GlobalBlockHeightRetention = tpRetention
	tSettings.UtxoStore.BlockHeightRetentionAdjustment = 0

	s, err := New(ctx, ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)

	return s, func() { _ = s.Close(ctx) }
}

// tpTableStats reports live rows in the table holding UTXO state and in the two tables
// this store defers reclaim of, so a reader can see what the measured rate is not paying
// for.
func tpTableStats(ctx context.Context, pool *pgxpool.Pool) (utxoRows, identRows, bodyRows int64) {
	_ = pool.QueryRow(ctx, `SELECT COALESCE(sum(n_live_tup),0) FROM pg_stat_user_tables
	                        WHERE relname LIKE 'utxo_p%'`).Scan(&utxoRows)
	_ = pool.QueryRow(ctx, `SELECT COALESCE(sum(n_live_tup),0) FROM pg_stat_user_tables
	                        WHERE relname LIKE 'tx_ident_l%'`).Scan(&identRows)
	_ = pool.QueryRow(ctx, `SELECT COALESCE(sum(n_live_tup),0) FROM pg_stat_user_tables
	                        WHERE relname LIKE 'tx_body_%'`).Scan(&bodyRows)

	return utxoRows, identRows, bodyRows
}

// tpRunValidator is the ported runPrunedValidator, driving this store. It returns the
// per-rep transactions per second.
func tpRunValidator(t *testing.T, s *Store, numWorkers int, cfg tpCfg, statPool *pgxpool.Pool) []float64 {
	t.Helper()

	ctx := context.Background()

	var curH atomic.Int64

	curH.Store(tpStartHeight)
	require.NoError(t, s.SetBlockHeight(uint32(tpStartHeight))) //nolint:gosec // bounded

	svc, err := s.GetPrunerService()
	require.NoError(t, err)

	svc.Start(ctx)

	mineCh := make(chan chainhash.Hash, tpMineChanCap)
	driverCtx, cancel := context.WithCancel(ctx)

	var (
		driverWG                sync.WaitGroup
		totalMined, totalPruned atomic.Int64
	)

	// Decorate batcher: coalesce the concurrent per-worker decorate requests into
	// block-wide BatchPreviousOutputsDecorate calls, at the same 500/5ms as the ported
	// harness and as this store's own spend and create batchers.
	decorateBatcher := batcher.NewWithPool(500, 5*time.Millisecond, func(b []*tpDecorateReq) {
		dtxs := make([]*bt.Tx, len(b))
		for i, r := range b {
			dtxs[i] = r.tx
		}

		derr := s.BatchPreviousOutputsDecorate(driverCtx, dtxs)
		for _, r := range b {
			r.errCh <- derr
		}
	}, true)

	// Table-size gate, on the table holding UTXO state, at the ported cap.
	var (
		gateClosed  atomic.Bool
		gatedPauses atomic.Int64
	)

	driverWG.Add(1)

	go func() {
		defer driverWG.Done()

		tk := time.NewTicker(500 * time.Millisecond)
		defer tk.Stop()

		for {
			select {
			case <-driverCtx.Done():
				return
			case <-tk.C:
				var live int64
				if err := statPool.QueryRow(driverCtx, `SELECT COALESCE(sum(n_live_tup),0)
					FROM pg_stat_user_tables WHERE relname LIKE 'utxo_p%'`).Scan(&live); err == nil {
					gateClosed.Store(live > tpTableCapRows)
				}
			}
		}
	}()

	// Height: advance the chain independently of prune speed.
	driverWG.Add(1)

	go func() {
		defer driverWG.Done()

		tk := time.NewTicker(tpHeightTickMS * time.Millisecond)
		defer tk.Stop()

		for {
			select {
			case <-driverCtx.Done():
				return
			case <-tk.C:
				_ = s.SetBlockHeight(uint32(curH.Add(1))) //nolint:gosec // bounded
			}
		}
	}()

	// Miners: batch-mine freshly created transactions.
	for m := 0; m < tpMiners; m++ {
		driverWG.Add(1)

		go func() {
			defer driverWG.Done()

			buf := make([]*chainhash.Hash, 0, tpMineBatch)

			flush := func() {
				if len(buf) == 0 {
					return
				}

				if _, mErr := s.SetMinedMulti(driverCtx, buf, utxo.MinedBlockInfo{
					BlockID: 1, BlockHeight: uint32(curH.Load()), OnLongestChain: true, //nolint:gosec // bounded
				}); mErr == nil {
					totalMined.Add(int64(len(buf)))
				} else if driverCtx.Err() == nil {
					t.Logf("[prune] SetMinedMulti(%d): %v", len(buf), mErr)
				}

				buf = buf[:0]
			}

			tk := time.NewTicker(100 * time.Millisecond)
			defer tk.Stop()

			for {
				select {
				case <-driverCtx.Done():
					return
				case h := <-mineCh:
					hh := h
					buf = append(buf, &hh)

					if len(buf) >= tpMineBatch {
						flush()
					}
				case <-tk.C:
					flush()
				}
			}
		}()
	}

	// Pruner: run continuously, exactly as the ported harness does.
	driverWG.Add(1)

	go func() {
		defer driverWG.Done()

		for {
			if driverCtx.Err() != nil {
				return
			}

			d, pErr := svc.Prune(driverCtx, uint32(curH.Load()), "bench") //nolint:gosec // bounded
			if pErr == nil {
				totalPruned.Add(d)
			} else if driverCtx.Err() == nil {
				t.Logf("[prune] Prune: %v", pErr)
			}

			if d == 0 {
				select {
				case <-driverCtx.Done():
					return
				case <-time.After(5 * time.Millisecond):
				}
			}
		}
	}()

	// Genesis transaction per worker, untimed, fanned out so the create batcher fills.
	parents := make([]*bt.Tx, numWorkers)

	{
		conc := runtime.GOMAXPROCS(0) * 16
		if conc > numWorkers {
			conc = numWorkers
		}

		var (
			gwg    sync.WaitGroup
			genErr atomic.Value
		)

		sem := make(chan struct{}, conc)

		for i := 0; i < numWorkers; i++ {
			i := i
			sem <- struct{}{}

			gwg.Add(1)

			go func() {
				defer gwg.Done()
				defer func() { <-sem }()

				g := tpGenesisTx(i)
				if _, cErr := s.Create(ctx, g, uint32(tpStartHeight)); cErr != nil { //nolint:gosec // bounded
					genErr.Store(cErr)
					return
				}

				parents[i] = g
			}()
		}

		gwg.Wait()

		if e := genErr.Load(); e != nil {
			t.Fatalf("genesis create: %v", e.(error))
		}
	}

	runPhase := func(dur time.Duration) int64 {
		var (
			ops atomic.Int64
			wg  sync.WaitGroup
		)

		wg.Add(numWorkers)

		deadline := time.Now().Add(dur)

		for w := 0; w < numWorkers; w++ {
			w := w

			go func() {
				defer wg.Done()

				parent := parents[w]
				defer func() { parents[w] = parent }()

				for time.Now().Before(deadline) {
					for gateClosed.Load() {
						gatedPauses.Add(1)

						select {
						case <-driverCtx.Done():
							return
						case <-time.After(5 * time.Millisecond):
						}

						if !time.Now().Before(deadline) {
							return
						}
					}

					h := uint32(curH.Load()) //nolint:gosec // bounded
					child := tpChildTx(parent)
					parentHash := parent.TxIDChainHash()

					// Real transactions arrive un-extended: the input carries only the
					// outpoint. Null the pre-filled fields so the store actually does the
					// decorate read.
					child.Inputs[0].PreviousTxScript = nil
					child.Inputs[0].PreviousTxSatoshis = 0

					if _, err := s.Get(ctx, parentHash, fields.BlockIDs, fields.BlockHeights); err != nil {
						return
					}

					dreq := tpDecorateReq{tx: child, errCh: make(chan error, 1)}
					decorateBatcher.Put(&dreq)

					if err := <-dreq.errCh; err != nil {
						return
					}

					if _, err := spendOnly(ctx, s, child, h); err != nil {
						return
					}

					if _, err := s.Create(ctx, child, h, utxo.WithLocked(true)); err != nil {
						return
					}

					if err := s.SetLocked(ctx, []chainhash.Hash{*child.TxIDChainHash()}, false); err != nil {
						return
					}

					parent = child

					ops.Add(1)

					select {
					case mineCh <- *child.TxIDChainHash():
					case <-driverCtx.Done():
						return
					}
				}
			}()
		}

		wg.Wait()

		return ops.Load()
	}

	if cfg.warmup > 0 {
		_ = runPhase(cfg.warmup)
	}

	samples := make([]float64, 0, cfg.reps)

	for r := 0; r < cfg.reps; r++ {
		start := time.Now()
		ops := runPhase(cfg.measure)
		elapsed := time.Since(start)

		if elapsed <= 0 {
			continue
		}

		tps := float64(ops) / elapsed.Seconds()
		samples = append(samples, tps)

		if cfg.verbose {
			u, ident, body := tpTableStats(ctx, statPool)
			t.Logf("[rep] %s workers=%d rep=%d/%d tps=%.0f utxo_rows=%d tx_ident_rows=%d tx_body_rows=%d height=%d mined=%d mineCh=%d/%d gatedPauses=%d",
				time.Now().Format("15:04:05"), numWorkers, r+1, cfg.reps, tps, u, ident, body,
				curH.Load(), totalMined.Load(), len(mineCh), cap(mineCh), gatedPauses.Load())
		}
	}

	cancel()
	driverWG.Wait()

	u, ident, body := tpTableStats(ctx, statPool)
	t.Logf("[prune] totals: mined=%d prunerReported=%d finalHeight=%d utxo_rows=%d tx_ident_rows=%d tx_body_rows=%d gatedPauses=%d",
		totalMined.Load(), totalPruned.Load(), curH.Load(), u, ident, body, gatedPauses.Load())

	return samples
}

// TestThroughput_UtxosetPruned reports the sustainable balanced throughput of this store
// on the same workload, the same units and the same worker counts as the append-only
// store's TestThroughput_QueueStorePruned.
func TestThroughput_UtxosetPruned(t *testing.T) {
	if os.Getenv("UTXOSET_TEST_DSN") == "" {
		t.Skip("throughput harness needs a real PostgreSQL: set UTXOSET_TEST_DSN")
	}

	cfg := tpDefaultCfg()
	dsn := tpDSN(t)

	statPool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)

	defer statPool.Close()

	t.Logf("[utxoset pruned] retention=%d heightTick=%dms miners=%d reps=%d warmup=%s measure=%s outputsPerTx=%d workers=%v",
		tpRetention, tpHeightTickMS, tpMiners, cfg.reps, cfg.warmup, cfg.measure, tpTxOutputs, cfg.workers)

	results := make([]string, 0, len(cfg.workers))

	for _, w := range cfg.workers {
		s, stop := tpNewStore(t, dsn)
		samples := tpRunValidator(t, s, w, cfg, statPool)
		stop()

		st := tpSummarize(samples)
		line := fmt.Sprintf("[utxoset pruned] workers=%-6d median=%9.0f mean=%9.0f CV=%5.1f%% range=[%.0f, %.0f] n=%d%s",
			w, st.median, st.mean, st.cv, st.min, st.max, st.n, tpUnstableFlag(st.cv, cfg.unstableCV))
		t.Log(line)

		results = append(results, line)
	}

	for _, line := range results {
		t.Log(line)
	}
}
