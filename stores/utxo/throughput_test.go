package utxo_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	aerostore "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	pgstore "github.com/bsv-blockchain/teranode/stores/utxo/postgres"
	sqlstore "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jackc/pgx/v5/pgxpool"
)

// throughputDSN is the postgres DSN used by all throughput tests.
// Override with THROUGHPUT_DSN to point at a different cluster (e.g. the disk-bound
// instance on :5439 used by the deferred-DAH lag bench).
var throughputDSN = func() string {
	if v := os.Getenv("THROUGHPUT_DSN"); v != "" {
		return v
	}
	return "postgres://teranode:teranode@localhost:5432/teranode_test"
}()

// benchStore is the utxo.Store contract plus the split Create/Spend methods the
// throughput harness drives directly. utxo.Store now exposes only the combined
// SpendAndCreate, but every concrete backend used here (sql, postgres,
// aerospike, and the bench-only sharded router) still implements the split
// methods, and the benches intentionally keep driving them so the measured hot
// path stays identical.
type benchStore interface {
	utxo.Store
	Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error)
	Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...utxo.IgnoreFlags) ([]*utxo.Spend, error)
}

func cleanDB(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, throughputDSN)
	if err != nil {
		t.Skipf("no postgres: %v", err)
	}
	defer pool.Close()
	// pgxpool.New is lazy and does not dial, so explicitly ping to detect an
	// unreachable server and skip (these are throughput benchmarks that require
	// a real Postgres, which the plain unit-test job does not provide).
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("no postgres: %v", err)
	}
	_, _ = pool.Exec(ctx, `SELECT cron.unschedule('materialize')`)
	_, _ = pool.Exec(ctx, `
		DROP PROCEDURE IF EXISTS materialize_loop() CASCADE;
		DROP PROCEDURE IF EXISTS dah_sweep_batch(BIGINT, INT) CASCADE;
		DROP PROCEDURE IF EXISTS dah_sweep_batch(INT, BIGINT, INT) CASCADE;
		DROP FUNCTION IF EXISTS process_batch(BIGINT) CASCADE;
		DROP FUNCTION IF EXISTS process_delete_at_height(BIGINT) CASCADE;
		DROP TABLE IF EXISTS batch_notifications, conflicting_children, block_ids, spends, outputs, inputs,
			tx_state, transactions, txs, txs_raw,
			dah_watermark, dah_part_watermark, dah_sweep_control,
			create_queue, input_queue, output_queue, spend_queue, mined_queue CASCADE;
	`)
}

func p2pkhScript() *bscript.Script {
	s := bscript.Script([]byte{0x76, 0xa9, 0x14,
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
		0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14,
		0x88, 0xac})
	return &s
}

// reprTxOutputs controls how many outputs each generated tx carries. The default
// (1) is the original tiny 1-in/1-out tx. Set THROUGHPUT_TX_OUTPUTS>1 to make
// raw_tx representative of real mainnet parent txs so the decorate path's
// de-TOAST + bt.NewTxFromBytes parse cost is actually exercised — the tiny
// default makes that cost ~free, which understates decorate (≈48% of live
// mainnet DB time) and any schema that changes how parent outputs are read.
var reprTxOutputs = envInt("THROUGHPUT_TX_OUTPUTS", 1)

// reprSeed gives every padding output unique bytes so raw_tx does not LZ4-compress
// away to nothing (real locking scripts carry distinct 20-byte hashes; identical
// padding would compress and hide the de-TOAST size).
var reprSeed atomic.Uint64

// padReprOutputs appends (reprTxOutputs-1) non-spendable OP_FALSE OP_RETURN data
// outputs. They are provably unspendable (ShouldStoreOutputAsUTXO=false), so
// spendable_count stays 1 and the fully-spent / prune model is unchanged: output
// 0 remains the only spendable chain output. They DO inflate out_count, the
// packed utxo_hashes, and raw_tx — the representative cost create and decorate
// pay on real txs.
func padReprOutputs(tx *bt.Tx) {
	for i := 1; i < reprTxOutputs; i++ {
		const n = 30 // ~ a P2PKH-sized output payload
		b := make([]byte, 0, 3+n)
		b = append(b, bscript.OpFALSE, bscript.OpRETURN, byte(n))
		s := reprSeed.Add(1)
		for j := 0; j < n; j++ {
			b = append(b, byte(s>>(uint(j%8)*8))^byte(j*7+i))
		}
		tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: 0, LockingScript: bscript.NewFromBytes(b)})
	}
}

// makeGenesisTx creates a unique genesis tx for a worker.
func makeGenesisTx(workerID int) *bt.Tx {
	tx := bt.NewTx()
	tx.Version = 1
	var h [32]byte
	h[0] = byte(workerID)
	h[1] = byte(workerID >> 8)
	h[2] = byte(workerID >> 16)
	h[3] = byte(workerID >> 24)
	h[4] = 0xFF
	prev, _ := chainhash.NewHash(h[:])
	_ = tx.From(prev.String(), 0, p2pkhScript().String(), 100_000_000)
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00})
	_ = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 50_000_000)
	padReprOutputs(tx)
	return tx
}

// makeChildTx creates a tx that spends output 0 of the parent.
func makeChildTx(parent *bt.Tx) *bt.Tx {
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
	padReprOutputs(tx)
	return tx
}

func runValidatorThroughputOffset(t *testing.T, store benchStore, opsPerWorker int, numWorkers int, workerOffset int) float64 {
	return doRunValidatorThroughput(t, store, opsPerWorker, numWorkers, workerOffset)
}

// runValidatorThroughput simulates the validator hot path per tx:
//  1. Get parent tx (verify it exists)
//  2. Spend parent outputs
//  3. Create child tx (with WithLocked)
//  4. SetLocked(false) to unlock
//
// Each worker builds a chain: genesis → tx1 → tx2 → ...
func runValidatorThroughput(t *testing.T, store benchStore, opsPerWorker int, numWorkers int) float64 {
	return doRunValidatorThroughput(t, store, opsPerWorker, numWorkers, 0)
}

func doRunValidatorThroughput(t *testing.T, store benchStore, opsPerWorker int, numWorkers int, workerOffset int) float64 {
	ctx := context.Background()
	blockHeight := uint32(200)
	_ = store.SetBlockHeight(blockHeight)

	// Pre-create genesis txs sequentially (these are the "coinbases")
	genesisTxs := make([]*bt.Tx, numWorkers)
	for i := 0; i < numWorkers; i++ {
		genesisTxs[i] = makeGenesisTx(i + workerOffset)
		_, err := store.Create(ctx, genesisTxs[i], blockHeight)
		if err != nil {
			t.Fatalf("genesis create worker %d: %v", i, err)
		}
	}

	// Collect all tx hashes for SetMinedMulti at the end
	allHashes := make([][]*chainhash.Hash, numWorkers)

	var totalOps atomic.Int64
	var errCount atomic.Int64
	var totalGetNs, totalSpendNs, totalCreateNs, totalUnlockNs atomic.Int64
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	start := time.Now()

	for w := 0; w < numWorkers; w++ {
		w := w
		go func() {
			defer wg.Done()
			parent := genesisTxs[w]
			var workerHashes []*chainhash.Hash
			// Include genesis tx
			h := genesisTxs[w].TxIDChainHash()
			workerHashes = append(workerHashes, h)

			for i := 0; i < opsPerWorker; i++ {
				child := makeChildTx(parent)
				parentHash := parent.TxIDChainHash()

				// 1. GET parent tx — verify it exists (same fields as validator)
				t0 := time.Now()
				_, err := store.Get(ctx, parentHash, fields.BlockIDs, fields.BlockHeights)
				totalGetNs.Add(time.Since(t0).Nanoseconds())
				if err != nil {
					if errCount.Add(1) <= 5 {
						t.Logf("worker %d iter %d (of %d): GET: %v (parent=%s)", w, i, opsPerWorker, err, parentHash)
					}
					return
				}

				// 2. SPEND parent outputs via child tx
				t1 := time.Now()
				spends, err := store.Spend(ctx, child, blockHeight)
				totalSpendNs.Add(time.Since(t1).Nanoseconds())
				if err != nil {
					if errCount.Add(1) <= 5 {
						spendDetail := ""
						for _, sp := range spends {
							if sp.Err != nil {
								spendDetail = fmt.Sprintf(" [%s:%d err=%v]", sp.TxID, sp.Vout, sp.Err)
								break
							}
						}
						t.Logf("worker %d iter %d: SPEND: %v%s", w, i, err, spendDetail)
					}
					return
				}

				// 3. CREATE child tx (locked)
				t2 := time.Now()
				_, err = store.Create(ctx, child, blockHeight, utxo.WithLocked(true))
				totalCreateNs.Add(time.Since(t2).Nanoseconds())
				if err != nil {
					if errCount.Add(1) <= 3 {
						t.Logf("worker %d iter %d: CREATE: %v", w, i, err)
					}
					return
				}

				// 4. UNLOCK child tx
				t3 := time.Now()
				err = store.SetLocked(ctx, []chainhash.Hash{*child.TxIDChainHash()}, false)
				totalUnlockNs.Add(time.Since(t3).Nanoseconds())
				if err != nil {
					if errCount.Add(1) <= 3 {
						t.Logf("worker %d iter %d: UNLOCK: %v", w, i, err)
					}
					return
				}

				childHash := child.TxIDChainHash()
				workerHashes = append(workerHashes, childHash)
				totalOps.Add(1)
				parent = child
			}
			allHashes[w] = workerHashes
		}()
	}

	wg.Wait()
	validatorElapsed := time.Since(start)
	ops := totalOps.Load()
	errs := errCount.Load()
	validatorTPS := float64(ops) / validatorElapsed.Seconds()

	// Per-op average latency.
	if ops > 0 {
		avgGet := time.Duration(totalGetNs.Load() / ops)
		avgSpend := time.Duration(totalSpendNs.Load() / ops)
		avgCreate := time.Duration(totalCreateNs.Load() / ops)
		avgUnlock := time.Duration(totalUnlockNs.Load() / ops)
		t.Logf("  Avg latency: Get=%v  Spend=%v  Create=%v  Unlock=%v  Total=%v",
			avgGet, avgSpend, avgCreate, avgUnlock, avgGet+avgSpend+avgCreate+avgUnlock)
	}

	if errs > 0 {
		t.Logf("  (%d workers errored out)", errs)
	}

	// --- SetMinedMulti: simulate a block being mined with all txs ---
	allTxHashes := make([]*chainhash.Hash, 0, len(allHashes)*opsPerWorker)
	for _, wh := range allHashes {
		allTxHashes = append(allTxHashes, wh...)
	}

	if len(allTxHashes) > 0 {
		minedInfo := utxo.MinedBlockInfo{
			BlockID:        1,
			BlockHeight:    blockHeight + 1,
			SubtreeIdx:     0,
			OnLongestChain: true,
		}
		minedStart := time.Now()
		_, err := store.SetMinedMulti(ctx, allTxHashes, minedInfo)
		minedElapsed := time.Since(minedStart)

		if err != nil {
			t.Logf("  SetMinedMulti failed: %v", err)
		} else {
			minedTPS := float64(len(allTxHashes)) / minedElapsed.Seconds()
			t.Logf("  SetMinedMulti: %d txs in %v = %.0f TPS", len(allTxHashes), minedElapsed, minedTPS)
		}
	}

	return validatorTPS
}

func newSQLStoreForBench(t *testing.T) benchStore {
	t.Helper()
	cleanDB(t)
	ctx := context.Background()
	storeURL, _ := url.Parse(throughputDSN)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 60 * time.Second
	tSettings.UtxoStore.SpendBatcherDurationMillis = 5
	tSettings.UtxoStore.StoreBatcherDurationMillis = 5
	tSettings.UtxoStore.SpendBatcherSize = 500
	tSettings.UtxoStore.StoreBatcherSize = 500
	tSettings.UtxoStore.GetBatcherSize = 500
	tSettings.UtxoStore.GetBatcherDurationMillis = 5
	tSettings.Postgres.MaxOpenConns = 100
	logger := ulogger.TestLogger{}
	s, err := sqlstore.New(ctx, logger, tSettings, storeURL)
	if err != nil {
		t.Fatalf("sql store: %v", err)
	}
	return s
}

func newQueueStoreForBench(t *testing.T) benchStore {
	t.Helper()
	cleanDB(t)
	ctx := context.Background()
	storeURL, _ := url.Parse(throughputDSN)
	storeURL.Scheme = "postgres"
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 60 * time.Second
	tSettings.UtxoStore.SpendBatcherDurationMillis = 5
	tSettings.UtxoStore.StoreBatcherDurationMillis = 5
	tSettings.UtxoStore.SpendBatcherSize = 500
	tSettings.UtxoStore.StoreBatcherSize = 500
	logger := ulogger.TestLogger{}
	s, err := pgstore.New(ctx, logger, tSettings, storeURL)
	if err != nil {
		t.Fatalf("queue store: %v", err)
	}
	s.Start(ctx)
	t.Cleanup(func() { s.Stop() })
	return s
}

func terminateOtherConnections(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, throughputDSN)
	if err != nil {
		return
	}
	defer pool.Close()
	_, _ = pool.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid != pg_backend_pid()`)
	time.Sleep(500 * time.Millisecond)
}

func TestThroughput_SQLStore(t *testing.T) {
	s := newSQLStoreForBench(t)
	workerOffset := 0
	for _, workers := range []int{1, 10, 100, 500, 1000, 5000, 10000, 15000} {
		opsPerWorker := 10
		if workers <= 10 {
			opsPerWorker = 100
		} else if workers <= 100 {
			opsPerWorker = 50
		} else if workers <= 1000 {
			opsPerWorker = 20
		}
		offset := workerOffset
		workerOffset += workers
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			opsPerSec := runValidatorThroughputOffset(t, s, opsPerWorker, workers, offset)
			t.Logf("SQL Store: %d workers × %d ops = %.0f TPS (Get+Spend+Create+Unlock per tx)",
				workers, opsPerWorker, opsPerSec)
		})
	}
}

func TestThroughput_QueueStore(t *testing.T) {
	terminateOtherConnections(t)
	s := newQueueStoreForBench(t)
	workerOffset := 0
	for _, workers := range []int{1, 10, 100, 500, 1000, 5000, 10000, 15000} {
		opsPerWorker := 10
		if workers <= 10 {
			opsPerWorker = 50
		} else if workers <= 1000 {
			opsPerWorker = 20
		}
		offset := workerOffset
		workerOffset += workers
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			opsPerSec := runValidatorThroughputOffset(t, s, opsPerWorker, workers, offset)
			t.Logf("Queue Store: %d workers × %d ops = %.0f TPS (Get+Spend+Create+Unlock per tx)",
				workers, opsPerWorker, opsPerSec)
		})
	}
}

func newAerospikeStoreForBench(t *testing.T) benchStore {
	t.Helper()
	ctx := context.Background()
	storeURL, _ := url.Parse("aerospike://localhost:3000/utxo-store?set=throughput_test&block_retention=1&externalStore=file://./data/externalStore")
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 60 * time.Second
	tSettings.UtxoStore.SpendBatcherDurationMillis = 5
	tSettings.UtxoStore.StoreBatcherDurationMillis = 5
	tSettings.UtxoStore.SpendBatcherSize = 500
	tSettings.UtxoStore.StoreBatcherSize = 500
	tSettings.UtxoStore.GetBatcherSize = 500
	tSettings.UtxoStore.GetBatcherDurationMillis = 5
	logger := ulogger.TestLogger{}
	s, err := aerostore.New(ctx, logger, tSettings, storeURL)
	if err != nil {
		t.Skipf("aerospike not available: %v", err)
	}
	s.SetExternalStore(memory.New())
	return s
}

func TestThroughput_AerospikeStore(t *testing.T) {
	s := newAerospikeStoreForBench(t)
	workerOffset := 0
	for _, workers := range []int{1, 10, 100, 500, 1000, 5000, 10000, 15000} {
		opsPerWorker := 10
		if workers <= 10 {
			opsPerWorker = 100
		} else if workers <= 100 {
			opsPerWorker = 50
		} else if workers <= 1000 {
			opsPerWorker = 20
		}
		offset := workerOffset
		workerOffset += workers
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			opsPerSec := runValidatorThroughputOffset(t, s, opsPerWorker, workers, offset)
			t.Logf("Aerospike Store: %d workers × %d ops = %.0f TPS (Get+Spend+Create+Unlock per tx)",
				workers, opsPerWorker, opsPerSec)
		})
	}
}

// TestThroughput_QueueExperiment is a fast experiment harness testing 1K and 10K workers.
func TestThroughput_QueueExperiment(t *testing.T) {
	terminateOtherConnections(t)
	s := newQueueStoreForBench(t)
	workerOffset := 0
	for _, workers := range []int{1000, 10000} {
		opsPerWorker := 20
		if workers > 5000 {
			opsPerWorker = 10
		}
		offset := workerOffset
		workerOffset += workers
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			opsPerSec := runValidatorThroughputOffset(t, s, opsPerWorker, workers, offset)
			t.Logf("EXPERIMENT: %d workers × %d ops = %.0f TPS",
				workers, opsPerWorker, opsPerSec)
		})
	}
}
