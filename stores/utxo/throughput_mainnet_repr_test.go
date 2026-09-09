package utxo_test

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	pgstore "github.com/bsv-blockchain/teranode/stores/utxo/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestCollectReprSample_ReturnsShape(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, throughputDSN)
	if err != nil || pool.Ping(ctx) != nil {
		t.Skipf("no postgres")
	}
	defer pool.Close()
	require.NoError(t, resetReprStats(ctx, pool))
	s, err := collectReprSample(ctx, pool)
	require.NoError(t, err)
	require.GreaterOrEqual(t, s.bufHitPct, 0.0)
	require.LessOrEqual(t, s.bufHitPct, 100.0)
	require.GreaterOrEqual(t, s.liveRows, int64(0))
}

type reprSample struct {
	// atUnix is set by the caller (the Task 5 bench sets it to the block height); collectReprSample does not populate it.
	atUnix      int64
	liveRows    int64
	txsBytes    int64
	spendsBytes int64
	sliceMs     float64
	// doomedMs captures the single `WITH doomed … DELETE FROM spends … DELETE FROM txs` statement
	// (scan + inline cascade delete combined; scan-dominated when deletions are sparse).
	doomedMs float64
	// deleteMs captures a separate standalone delete path that this store does not emit as a hot
	// statement (the cascade delete is inline inside the doomed statement), so this is ~0 by construction.
	deleteMs  float64
	bufHitPct float64
}

func resetReprStats(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, `SELECT pg_stat_statements_reset()`); err != nil {
		return err
	}
	_, err := pool.Exec(ctx, `SELECT pg_stat_reset()`)
	return err
}

func collectReprSample(ctx context.Context, pool *pgxpool.Pool) (reprSample, error) {
	var s reprSample
	row := pool.QueryRow(ctx, `
		SELECT
		  COALESCE(SUM(pg_total_relation_size(c.oid)) FILTER (WHERE c.relname LIKE 'txs%'),0),
		  COALESCE(SUM(pg_total_relation_size(c.oid)) FILTER (WHERE c.relname LIKE 'spends%'),0),
		  COALESCE(SUM(c.reltuples::bigint) FILTER (WHERE c.relname LIKE 'txs_p%'),0)
		FROM pg_class c WHERE c.relkind IN ('r','p')`)
	if err := row.Scan(&s.txsBytes, &s.spendsBytes, &s.liveRows); err != nil {
		return s, err
	}
	// per-class exec time from pg_stat_statements.
	//
	// doomedMs: matches the single `WITH doomed … DELETE FROM spends … DELETE FROM txs`
	// statement that the pruner emits. pg_stat_statements stores it as one entry, so this
	// bucket captures the scan + inline cascade delete combined. When deletions are sparse
	// (liveRows ≈ created), the scan dominates and doomedMs ≈ pure scan cost.
	//
	// deleteMs: matches a separate standalone delete path (`DELETE FROM txs…`, `DELETE FROM
	// spends…`, `WITH del…`). This store does NOT emit such statements as hot entries — the
	// cascade delete is inline inside the doomed statement above — so deleteMs is ~0 by
	// construction (the filter matches nothing this store emits in the pruner hot path).
	classRow := pool.QueryRow(ctx, `
		SELECT
		  COALESCE(SUM(total_exec_time) FILTER (WHERE query LIKE 'WITH slice%'),0),
		  COALESCE(SUM(total_exec_time) FILTER (WHERE query LIKE '%doomed%'),0),
		  COALESCE(SUM(total_exec_time) FILTER (WHERE query LIKE 'DELETE FROM txs%'
		           OR query LIKE 'DELETE FROM spends%'
		           OR query LIKE 'WITH del%'),0)
		FROM pg_stat_statements`)
	if err := classRow.Scan(&s.sliceMs, &s.doomedMs, &s.deleteMs); err != nil {
		return s, err
	}
	hitRow := pool.QueryRow(ctx, `
		SELECT COALESCE(100.0*blks_hit/NULLIF(blks_hit+blks_read,0),0)
		FROM pg_stat_database WHERE datname=current_database()`)
	if err := hitRow.Scan(&s.bufHitPct); err != nil {
		return s, err
	}
	return s, nil
}

// TestThroughput_MainnetRepr drives the real postgres queue store + pruner with
// a workload shaped to mainnet statistics (out-count, spend-age, survivor rate)
// and no table-size gate. The live set grows until it reaches targetRows, at
// which point the test asserts that samples were collected and the set is non-empty.
//
// Environment knobs (all optional; tiny smoke-test defaults):
//
//	MREPR_TXS_PER_TICK=500        # transactions created per height tick
//	MREPR_HEIGHT_TICK=250         # ms between height advances
//	MREPR_GROWTH_TARGET_ROWS=50000 # stop when liveRows >= this
//	MREPR_SURVIVOR_PROB_PCT=5     # % outputs that are permanent survivors
//	MREPR_SAMPLE_TICKS=20         # sample every N ticks
func TestThroughput_MainnetRepr(t *testing.T) {
	cleanDB(t) // skips if no postgres
	ctx := context.Background()

	txsPerTick := envInt("MREPR_TXS_PER_TICK", 2000)
	heightTickMS := envInt("MREPR_HEIGHT_TICK", 250)
	targetRows := envInt("MREPR_GROWTH_TARGET_ROWS", 50000)
	survivorPct := envInt("MREPR_SURVIVOR_PROB_PCT", 5)
	sampleEveryTicks := envInt("MREPR_SAMPLE_TICKS", 20)
	workers := envInt("MREPR_WORKERS", 128)

	// --- store + pruner setup (matches newPrunedQueueStore pattern) ---
	storeURL, err := url.Parse(throughputDSN)
	require.NoError(t, err)
	storeURL.Scheme = "postgres"

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 60 * time.Second
	tSettings.UtxoStore.SpendBatcherDurationMillis = 5
	tSettings.UtxoStore.StoreBatcherDurationMillis = 5
	tSettings.UtxoStore.SpendBatcherSize = 500
	tSettings.UtxoStore.StoreBatcherSize = 500

	store, err := pgstore.New(ctx, ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)
	store.Start(ctx)
	t.Logf("[mrepr] pending_deletes always enabled (unconditional)")
	t.Cleanup(func() { store.Stop() })

	svc, err := store.GetPrunerService()
	require.NoError(t, err)
	driverCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	svc.Start(driverCtx)

	pool, err := pgxpool.New(ctx, throughputDSN)
	require.NoError(t, err)
	defer pool.Close()
	require.NoError(t, resetReprStats(ctx, pool))

	sched := newUTXOScheduler(99, float64(survivorPct)/100.0, 2)
	idToHash := make(map[uint64]*chainhash.Hash)

	var curH atomic.Int64
	curH.Store(1)
	var created atomic.Int64
	var samples []reprSample

	// --- main loop: NO gate. Advance height, create+spend, mine, prune, sample. ---
	runStart := time.Now()
	tick := time.NewTicker(time.Duration(heightTickMS) * time.Millisecond)
	defer tick.Stop()
	ticks := 0
	minedHashes := make([]*chainhash.Hash, 0, txsPerTick)
	for {
		h := uint32(curH.Load())
		_ = store.SetBlockHeight(h)

		// BUILD PHASE (single-threaded: scheduler + idToHash are not concurrency-safe)
		jobs := make([]*bt.Tx, txsPerTick)
		hasIn := make([]bool, txsPerTick)
		for i := 0; i < txsPerTick; i++ {
			id, oc, inputs := sched.createTx(int(h))
			tx := buildReprTx(id, inputs, oc, idToHash)
			idToHash[id] = tx.TxIDChainHash()
			jobs[i] = tx
			hasIn[i] = len(inputs) > 0
		}
		// DRIVE PHASE (concurrent: store.Create/Spend are goroutine-safe and batch-coalesce)
		hashes := make([]*chainhash.Hash, txsPerTick) // index-aligned; nil on failure -> no race (disjoint indices)
		var wg sync.WaitGroup
		sem := make(chan struct{}, workers)
		for i := range jobs {
			wg.Add(1)
			sem <- struct{}{}
			go func(i int) {
				defer wg.Done()
				defer func() { <-sem }()
				if hasIn[i] {
					_, _ = store.Spend(ctx, jobs[i], h)
				}
				if _, err := store.Create(ctx, jobs[i], h, utxo.WithLocked(true)); err == nil {
					hashes[i] = jobs[i].TxIDChainHash()
				}
			}(i)
		}
		wg.Wait()
		minedHashes = minedHashes[:0]
		for _, hsh := range hashes {
			if hsh != nil {
				minedHashes = append(minedHashes, hsh)
			}
		}
		if len(minedHashes) > 0 {
			_, _ = store.SetMinedMulti(ctx, minedHashes, utxo.MinedBlockInfo{BlockID: 1, BlockHeight: h, OnLongestChain: true})
			created.Add(int64(len(minedHashes)))
		}
		_, _ = svc.Prune(driverCtx, h, "bench")

		ticks++
		if ticks%sampleEveryTicks == 0 {
			s, sErr := collectReprSample(ctx, pool)
			require.NoError(t, sErr)
			s.atUnix = int64(h)
			samples = append(samples, s)
			t.Logf("[mrepr] h=%d liveRows=%d txsMB=%d sliceMs=%.0f cascadeMs=%.0f auxDelMs=%.0f hit%%=%.1f created=%d",
				h, s.liveRows, s.txsBytes/(1<<20), s.sliceMs, s.doomedMs, s.deleteMs, s.bufHitPct, created.Load())
			if s.liveRows >= int64(targetRows) {
				break
			}
		}
		curH.Add(1)
		<-tick.C
	}

	elapsed := time.Since(runStart)
	createRate := float64(created.Load()) / elapsed.Seconds()
	t.Logf("[mrepr] DONE workers=%d txsPerTick=%d created=%d elapsed=%s createRate=%.0f tx/s",
		workers, txsPerTick, created.Load(), elapsed.Round(time.Second), createRate)

	require.NotEmpty(t, samples, "collected at least one sample")
	last := samples[len(samples)-1]
	require.Greater(t, last.liveRows, int64(0), "live set grew")
}

// buildReprTx constructs a *bt.Tx with outCount 1000-sat outputs and a P2PKH
// locking script, spending the given parent outpoints. Uses LockTime to ensure
// a unique txid even when inputs is empty (source txs).
func buildReprTx(id uint64, inputs []outpoint, outCount int, idToHash map[uint64]*chainhash.Hash) *bt.Tx {
	tx := bt.NewTx()
	tx.Version = 1
	lockScript := p2pkhScript()
	for _, in := range inputs {
		ph := idToHash[in.tx]
		if ph == nil {
			continue
		}
		_ = tx.From(ph.String(), in.vout, lockScript.String(), 1000)
	}
	for v := 0; v < outCount; v++ {
		_ = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000)
	}
	// Make txid unique even when there are no inputs (source tx with no parent):
	tx.LockTime = uint32(id)
	return tx
}
