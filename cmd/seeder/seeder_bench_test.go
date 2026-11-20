package seeder

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxofactory "github.com/bsv-blockchain/teranode/stores/utxo/factory"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	testAerospike "github.com/bsv-blockchain/teranode/test/utils/aerospike"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// BenchmarkSeederUTXOImport measures UTXO-set import throughput into a real
// Aerospike container through the same reader + worker pool the seeder uses.
//
// Run with:
//
//	go test -bench=BenchmarkSeederUTXOImport -benchtime=1x -run '^$' -timeout=60m ./cmd/seeder/
//
// SEEDER_BENCH_TXS overrides the number of transactions per iteration, and
// SEEDER_BENCH_LARGE_EVERY how often a 200-output tx appears (0 disables them).
// SEEDER_BENCH_EXT_CONCURRENCY and SEEDER_BENCH_FSYNC tune the external-store
// path (utxostore_externalStoreConcurrency, file blob store fsyncMode).
func BenchmarkSeederUTXOImport(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	txCount := 200_000
	if v := os.Getenv("SEEDER_BENCH_TXS"); v != "" {
		n, err := strconv.Atoi(v)
		require.NoError(b, err)

		txCount = n
	}

	largeEvery := 500
	if v := os.Getenv("SEEDER_BENCH_LARGE_EVERY"); v != "" {
		n, err := strconv.Atoi(v)
		require.NoError(b, err)

		largeEvery = n
	}

	store, externalStoreConcurrency := newBenchAerospikeStore(b)

	cases := []struct {
		name          string
		workers       int
		utxoBatchSize int // 0 = one shared pass
	}{
		{"onepass/workers=500", 500, 0},
		{"onepass/workers=16384", 16384, 0},
		{"twopass/workers=16384", 16384, 128},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()

				// Unique txids per run so every import is a real create, not ErrTxExists.
				seed := fmt.Sprintf("%s-i%d-%d", tc.name, i, time.Now().UnixNano())
				path := writeCompleteSnapshotFile(b, benchWrappers(seed, txCount, largeEvery))

				b.StartTimer()

				start := time.Now()
				require.NoError(b, importUTXOSet(context.Background(), ulogger.TestLogger{}, store, path, importOptions{
					workerCount:            tc.workers,
					multiRecordWorkerCount: externalStoreConcurrency,
					channelSize:            1000,
					utxoBatchSize:          tc.utxoBatchSize,
				}))

				b.ReportMetric(float64(txCount)/time.Since(start).Seconds(), "tx/s")
			}
		})
	}
}

func newBenchAerospikeStore(b *testing.B) (utxo.Store, int) {
	b.Helper()

	aerospikeURL, teardown, err := testAerospike.InitAerospikeContainer()
	test.SkipIfContainerUnavailable(b, err)

	b.Cleanup(func() { _ = teardown() })

	storeURL, err := url.Parse(aerospikeURL)
	require.NoError(b, err)

	q := storeURL.Query()
	// The seeder default; SEEDER_BENCH_FSYNC=full measures the old behaviour.
	externalStoreURL := "file://" + b.TempDir() + "?fsyncMode=data"
	if v := os.Getenv("SEEDER_BENCH_FSYNC"); v != "" {
		externalStoreURL = "file://" + b.TempDir() + "?fsyncMode=" + v
	}

	q.Set("externalStore", externalStoreURL)
	storeURL.RawQuery = q.Encode()

	tSettings := test.CreateBaseTestSettings(b)
	tSettings.UtxoStore.UtxoStore = storeURL
	tSettings.UtxoStore.ExternalStoreConcurrency = 256 // the seeder default

	if v := os.Getenv("SEEDER_BENCH_EXT_CONCURRENCY"); v != "" {
		n, err := strconv.Atoi(v)
		require.NoError(b, err)

		tSettings.UtxoStore.ExternalStoreConcurrency = n
	}

	b.Logf("storeBatcherSize=%d storeBatcherDurationMillis=%d batcherMaxConcurrent=%d utxoBatchSize=%d",
		tSettings.UtxoStore.StoreBatcherSize, tSettings.UtxoStore.StoreBatcherDurationMillis,
		tSettings.UtxoStore.BatcherMaxConcurrent, tSettings.UtxoStore.UtxoBatchSize)

	store, err := utxofactory.NewStore(context.Background(), ulogger.TestLogger{}, tSettings, "seeder-bench", false)
	require.NoError(b, err)

	return store, tSettings.UtxoStore.ExternalStoreConcurrency
}

// benchWrappers builds a deterministic, mainnet-shaped mix: mostly 1-3 unspent
// P2PKH outputs, with one tx in largeEvery carrying 200 outputs so the
// multi-record external-store path is exercised too.
func benchWrappers(seed string, n int, largeEvery int) []*utxopersister.UTXOWrapper {
	script := make([]byte, 25)
	script[0], script[1], script[2], script[23], script[24] = 0x76, 0xa9, 0x14, 0x88, 0xac

	wrappers := make([]*utxopersister.UTXOWrapper, n)

	for i := 0; i < n; i++ {
		var idx [8]byte
		binary.LittleEndian.PutUint64(idx[:], uint64(i)) //nolint:gosec // non-negative loop index

		outputs := 1 + i%3
		if largeEvery > 0 && i%largeEvery == 0 {
			outputs = 200
		}

		utxos := make([]*utxopersister.UTXO, outputs)
		for j := range utxos {
			utxos[j] = &utxopersister.UTXO{Index: uint32(j), Value: 1000, Script: script} //nolint:gosec // small index
		}

		wrappers[i] = &utxopersister.UTXOWrapper{
			TxID:   chainhash.HashH(append([]byte(seed), idx[:]...)),
			Height: 100,
			UTXOs:  utxos,
		}
	}

	return wrappers
}

// noopCreateStore accepts every create without doing any work, isolating the
// seeder's own reader + worker overhead from the store.
type noopCreateStore struct {
	utxo.Store
}

func (noopCreateStore) SpendAndCreate(_ context.Context, _ *bt.Tx, _ uint32, _ ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	return nil, nil, nil
}

// BenchmarkSeederUTXOImportNoopStore measures the seeder-side ceiling (file
// reading, record decoding, tx building, worker dispatch) with a store that
// does nothing. No containers required.
func BenchmarkSeederUTXOImportNoopStore(b *testing.B) {
	const txCount = 500_000

	for _, workerCount := range []int{500, 16384} {
		b.Run(fmt.Sprintf("workers=%d", workerCount), func(b *testing.B) {
			path := writeCompleteSnapshotFile(b, benchWrappers("noop", txCount, 500))

			b.ReportAllocs()
			b.ResetTimer() // exclude building the snapshot from time and allocations

			for i := 0; i < b.N; i++ {
				start := time.Now()
				require.NoError(b, importUTXOSet(context.Background(), ulogger.TestLogger{}, noopCreateStore{}, path, importOptions{
					workerCount:            workerCount,
					multiRecordWorkerCount: 256,
					channelSize:            1000,
					utxoBatchSize:          128,
				}))

				b.ReportMetric(float64(txCount)/time.Since(start).Seconds(), "tx/s")
			}
		})
	}
}
