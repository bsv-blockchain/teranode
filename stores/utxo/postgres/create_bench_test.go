package postgres

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// benchDSN allows pointing the create-path benchmark at a throwaway postgres
// without touching the shared testDSN const. Falls back to testDSN.
func benchDSN() string {
	if d := os.Getenv("BENCH_PG_DSN"); d != "" {
		return d
	}
	return testDSN
}

// setupCreateBenchStore mirrors setupBenchStore but uses benchDSN() and the
// real Close() for cleanup.
func setupCreateBenchStore(b *testing.B) (*Store, context.Context) {
	b.Helper()
	ctx := context.Background()
	dsn := benchDSN()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		b.Skipf("Skipping: cannot connect to postgres (%s): %v", dsn, err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		b.Skipf("Skipping: cannot connect to postgres (%s): %v", dsn, err)
	}
	_, _ = pool.Exec(ctx, `
		DROP PROCEDURE IF EXISTS dah_sweep_batch(INT, BIGINT, INT) CASCADE;
		DROP TABLE IF EXISTS conflicting_children, block_ids, spends, outputs, inputs,
			tx_state, transactions, txs, dah_watermark, dah_part_watermark, dah_sweep_control CASCADE;`)
	pool.Close()

	storeURL, err := url.Parse(dsn)
	require.NoError(b, err)
	storeURL.Scheme = "postgres"

	bSettings := test.CreateBaseTestSettings(b)
	bSettings.UtxoStore.DBTimeout = 30 * time.Second

	store, err := New(ctx, ulogger.TestLogger{}, bSettings, storeURL)
	require.NoError(b, err)

	b.Cleanup(func() { store.Stop() })
	return store, ctx
}

// makeBenchCreateTx builds a unique tx (random output satoshis → unique txid)
// with 2 outputs. No inputs needed — Create only stores, it does not validate.
func makeBenchCreateTx() *bt.Tx {
	tx := bt.NewTx()
	//nolint:gosec
	_ = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", rand.Uint64()%1_000_000+10_000)
	//nolint:gosec
	_ = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", rand.Uint64()%1_000_000+10_000)
	return tx
}

// BenchmarkCreatePaths measures the single bulk create path (UNNEST) across the
// full range of configured StoreBatcherSize values (100 → 2048), with and
// without mined-block info. "Plain" is a validator-path create (unmined);
// "Mined" carries one MinedBlockInfo and exercises the per-row CASE/ARRAY
// columns. The two should be within noise — confirming that folding mined
// support into the UNNEST path (which let us delete the COPY+staging path)
// costs essentially nothing.
//
// Reported "ns/row" is the per-UTXO-tx cost; lower is better.
func BenchmarkCreatePaths(b *testing.B) {
	store, ctx := setupCreateBenchStore(b)

	for _, n := range []int{100, 500, 1024, 2048} {
		b.Run(fmt.Sprintf("Plain/%d", n), func(b *testing.B) {
			runCreateBench(b, store, ctx, n, false)
		})
		b.Run(fmt.Sprintf("Mined/%d", n), func(b *testing.B) {
			runCreateBench(b, store, ctx, n, true)
		})
	}
}

func runCreateBench(b *testing.B, store *Store, ctx context.Context, n int, mined bool) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Setup (untimed): clean slate + fresh unique batch.
		b.StopTimer()
		if _, err := store.pool.Exec(ctx, "TRUNCATE txs, outputs, spends CASCADE"); err != nil {
			b.Fatalf("truncate: %v", err)
		}
		batch := make([]*batchCreateItem, n)
		for j := 0; j < n; j++ {
			opts := &utxo.CreateOptions{}
			if mined {
				opts.MinedBlockInfos = []utxo.MinedBlockInfo{{
					BlockID: 1, BlockHeight: 1, OnLongestChain: true,
				}}
			}
			batch[j] = &batchCreateItem{
				tx:          makeBenchCreateTx(),
				blockHeight: 1,
				options:     opts,
				done:        make(chan batchCreateResult, 1),
			}
		}
		b.StartTimer()

		// Timed: the flush mechanism under test.
		if mined {
			store.sendCreateBatch(batch) // mined items → bulk UNNEST with block columns
		} else {
			store.sendCreateBatchUNNEST(ctx, batch)
		}
		for _, it := range batch {
			if res := <-it.done; res.Err != nil {
				b.Fatalf("create failed: %v", res.Err)
			}
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(n), "ns/row")
}
