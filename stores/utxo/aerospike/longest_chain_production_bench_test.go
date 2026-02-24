package aerospike_test

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// Package-level singleton for the shared Aerospike container.
// Go's benchmark framework re-invokes the parent benchmark function once per
// sub-benchmark, which would create a new container each time without this.
var (
	prodBenchOnce  sync.Once
	prodBenchStore utxo.Store
	prodBenchCtx   context.Context
)

func getSharedBenchStore(b *testing.B) (utxo.Store, context.Context) {
	prodBenchOnce.Do(func() {
		logger := ulogger.NewErrorTestLogger(b)
		tSettings := test.CreateBaseTestSettings(b)
		b.Logf("Starting Aerospike container (shared across all sub-benchmarks)...")
		start := time.Now()
		store, ctx, _ := initAerospikeBench(b, tSettings, logger)
		// Cleanup is handled by testcontainers' Ryuk sidecar when the process exits.
		// We intentionally don't defer deferFn() here because `b` may not be the
		// final benchmark instance — Go re-invokes the parent for each sub-benchmark.
		prodBenchStore = store
		prodBenchCtx = ctx
		b.Logf("Container ready in %v", time.Since(start))
	})
	return prodBenchStore, prodBenchCtx
}

// BenchmarkMarkTransactionsProductionScale benchmarks MarkTransactionsOnLongestChain
// with production-scale transaction counts using real Aerospike.
//
// IMPORTANT: Use -run=^$ to skip regular tests (there are 30+ tests in this package
// that each create their own Aerospike container).
//
// Run this benchmark on BOTH branches to compare:
//
// On main branch (old code):
//
//	git checkout main
//	go test -run=^$ -bench=BenchmarkMarkTransactionsProductionScale -benchmem -benchtime=1x -timeout=30m ./stores/utxo/aerospike/
//
// On feature branch (new code):
//
//	git checkout <your-branch>
//	go test -run=^$ -bench=BenchmarkMarkTransactionsProductionScale -benchmem -benchtime=1x -timeout=30m ./stores/utxo/aerospike/
//
// Compare the results to see the actual performance difference.
func BenchmarkMarkTransactionsProductionScale(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping production-scale benchmark in short mode")
	}

	store, ctx := getSharedBenchStore(b)

	testCases := []struct {
		name       string
		count      int
		seedOffset int
		note       string
	}{
		{"small", 1_000, 0, "Baseline test"},
		{"medium", 10_000, 10_000, "Typical small reorg"},
		{"large", 100_000, 100_000, "Large reorg"},
		{"xlarge", 1_000_000, 1_000_000, "Very large reorg - 1M transactions"},
	}

	for _, tc := range testCases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			b.Logf("========== %s: %d txs (%s) ==========", tc.name, tc.count, tc.note)

			// SETUP - Create test data (not timed)
			b.Logf("Creating %d test transactions...", tc.count)
			setupStart := time.Now()
			txHashes := createProductionLikeTransactions(b, ctx, store, tc.count, tc.seedOffset)
			b.Logf("Created in %v (%.0f tx/sec)",
				time.Since(setupStart), float64(tc.count)/time.Since(setupStart).Seconds())

			runtime.GC()
			startGoroutines := runtime.NumGoroutine()
			b.Logf("Goroutines before: %d", startGoroutines)

			// BENCHMARK - Only this is timed
			b.Logf("Benchmarking MarkTransactionsOnLongestChain...")
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				onLongestChain := (i % 2) == 0
				start := time.Now()
				err := store.MarkTransactionsOnLongestChain(ctx, txHashes, onLongestChain)
				require.NoError(b, err)
				b.Logf("  Iter %d: %v (%.0f tx/sec)", i+1, time.Since(start), float64(tc.count)/time.Since(start).Seconds())
			}

			b.StopTimer()

			runtime.GC()
			time.Sleep(50 * time.Millisecond)
			endGoroutines := runtime.NumGoroutine()

			throughput := float64(tc.count*b.N) / b.Elapsed().Seconds()
			goroutineDelta := endGoroutines - startGoroutines

			b.ReportMetric(float64(tc.count), "transactions")
			b.ReportMetric(float64(goroutineDelta), "goroutine_delta")
			b.ReportMetric(throughput, "tx/sec")

			b.Logf("RESULTS: Time=%v Throughput=%.0f tx/sec Goroutines=%d->%d (delta=%+d)",
				b.Elapsed(), throughput, startGoroutines, endGoroutines, goroutineDelta)
		})
	}
}

// createProductionLikeTransactions creates realistic transactions in Aerospike
// that match production characteristics. seedOffset ensures uniqueness across test cases.
// Creates transactions concurrently for better performance.
func createProductionLikeTransactions(b *testing.B, ctx context.Context, store utxo.Store, count int, seedOffset int) []chainhash.Hash {
	b.Helper()

	start := time.Now()

	txHashes := make([]chainhash.Hash, count)

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(100)

	for i := 0; i < count; i++ {
		i := i

		g.Go(func() error {
			tx := createRealisticTransaction(seedOffset + i)

			_, err := store.Create(gCtx, tx, uint32(100000+(seedOffset+i)%1000))
			if err != nil {
				return err
			}

			txHashes[i] = *tx.TxIDChainHash()
			return nil
		})
	}

	err := g.Wait()
	require.NoError(b, err)

	elapsed := time.Since(start)
	b.Logf("Created %d transactions in %v (%.0f tx/sec)",
		count, elapsed, float64(count)/elapsed.Seconds())

	return txHashes
}

// createRealisticTransaction creates a unique coinbase transaction for each seed.
func createRealisticTransaction(seed int) *bt.Tx {
	coinbaseTxHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17032dff0c2f71646c6e6b2f5e931c7f7b6199adf35e1300ffffffff01d15fa012000000001976a91417db35d440a673a218e70a5b9d07f895facf50d288ac00000000"

	tx, err := bt.NewTxFromString(coinbaseTxHex)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse coinbase template: %v", err))
	}

	if len(tx.Inputs) > 0 {
		uniqueData := fmt.Sprintf("benchmark-tx-%010d", seed)
		script := bscript.Script{}
		script = append(script, byte(len(uniqueData)))
		script = append(script, []byte(uniqueData)...)
		tx.Inputs[0].UnlockingScript = &script
	}

	if len(tx.Outputs) > 0 {
		tx.Outputs[0].Satoshis = uint64(5000000000 + seed)
	}

	return tx
}
