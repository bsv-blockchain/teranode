package blockassembly

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/factory"
	testAerospike "github.com/bsv-blockchain/teranode/test/utils/aerospike"
	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// BenchmarkLoadUnminedTransactions benchmarks the loadUnminedTransactions function
// with varying transaction counts and scan modes against a real Aerospike container.
//
// Run with:
//
//	go test -bench=BenchmarkLoadUnminedTransactions -benchmem -benchtime=3x -timeout=30m
//
// For CPU profiling to identify hotspots:
//
//	go test -bench=BenchmarkLoadUnminedTransactions/txCount=10000/fullScan=false -benchmem -benchtime=3x -cpuprofile=cpu.prof
//	go tool pprof -http=:8080 cpu.prof
//
// For memory profiling:
//
//	go test -bench=BenchmarkLoadUnminedTransactions/txCount=10000/fullScan=false -benchmem -benchtime=3x -memprofile=mem.prof
//	go tool pprof -http=:8080 mem.prof
func BenchmarkLoadUnminedTransactions(b *testing.B) {
	// Skip if running in short mode
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	// Test with different transaction counts
	txCounts := []int{1000, 10000, 50000}

	for _, txCount := range txCounts {
		for _, fullScan := range []bool{false, true} {
			b.Run(fmt.Sprintf("txCount=%d/fullScan=%t", txCount, fullScan), func(b *testing.B) {
				benchmarkLoadUnminedTransactions(b, txCount, fullScan)
			})
		}
	}
}

func benchmarkLoadUnminedTransactions(b *testing.B, txCount int, fullScan bool) {
	initPrometheusMetrics()

	// Initialize Aerospike container
	b.Logf("Initializing Aerospike container...")
	aerospikeURL, teardownAerospike, err := testAerospike.InitAerospikeContainer()
	require.NoError(b, err)

	b.Cleanup(func() {
		_ = teardownAerospike()
	})

	b.Logf("Aerospike container initialized at: %s", aerospikeURL)

	postgresURL, teardownPostgres, err := postgres.SetupTestPostgresContainer()
	require.NoError(b, err)

	b.Cleanup(func() {
		_ = teardownPostgres()
	})

	b.Logf("Postgres container initialized at: %s", postgresURL)

	// Parse Aerospike URL
	parsedURL, err := url.Parse(aerospikeURL)
	require.NoError(b, err)

	// Create test settings
	tSettings := test.CreateBaseTestSettings(b)
	tSettings.UtxoStore.UtxoStore = parsedURL
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 1024
	tSettings.BlockAssembly.OnRestartValidateParentChain = false // Disable for benchmark

	// Create UTXO store with Aerospike
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	b.Logf("Creating UTXO store...")
	utxoStore, err := factory.NewStore(ctx, logger, tSettings, "benchmark", false)
	require.NoError(b, err)

	// Set initial block height
	err = utxoStore.SetBlockHeight(1)
	require.NoError(b, err)

	// Create blockchain store and client
	blockchainStoreURL, err := url.Parse(postgresURL)
	require.NoError(b, err)

	blockchainStore, err := blockchainstore.NewStore(logger, blockchainStoreURL, tSettings)
	require.NoError(b, err)

	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockchainStore, nil, nil)
	require.NoError(b, err)

	// Create blob store
	blobStore := memory.New()

	// Create BlockAssembler
	stats := gocore.NewStat("benchmark")

	b.Logf("Creating BlockAssembler...")
	blockAssembler, err := NewBlockAssembler(ctx, logger, tSettings, stats, utxoStore, blobStore, blockchainClient, nil)
	require.NoError(b, err)
	require.NotNil(b, blockAssembler)

	// Get genesis block
	genesisBlock, err := blockchainStore.GetBlockByID(ctx, 0)
	require.NoError(b, err)

	blockAssembler.setBestBlockHeader(genesisBlock.Header, 0)
	blockAssembler.SetSkipWaitForPendingBlocks(true)

	// Start the subtree processor goroutine to handle Reset() calls
	blockAssembler.subtreeProcessor.Start(ctx)

	// Populate UTXO store with unmined transactions
	b.Logf("Populating UTXO store with %d unmined transactions...", txCount)
	start := time.Now()

	transactions := generateTestTransactions(b, txCount)

	// Create transactions in batches for better performance
	batchSize := 1000
	for i := 0; i < len(transactions); i += batchSize {
		end := i + batchSize
		if end > len(transactions) {
			end = len(transactions)
		}

		for j := i; j < end; j++ {
			tx := transactions[j]

			// Create with unmined status (no block info)
			_, err = utxoStore.Create(ctx, tx, 1)
			require.NoError(b, err)
		}

		if (i+batchSize)%10000 == 0 {
			b.Logf("Populated %d/%d transactions...", i+batchSize, txCount)
		}
	}

	populateDuration := time.Since(start)
	b.Logf("Population completed in %s (%.2f tx/sec)", populateDuration, float64(txCount)/populateDuration.Seconds())

	// If testing with index-based scan, wait for the index to be ready
	if !fullScan {
		b.Logf("Waiting for unmined_since index to be ready...")
		if indexWaiter, ok := utxoStore.(interface {
			WaitForIndexReady(ctx context.Context, indexName string) error
		}); ok {
			err := indexWaiter.WaitForIndexReady(ctx, "unminedSinceIndex")
			if err != nil {
				b.Logf("Warning: index wait failed: %v", err)
			} else {
				b.Logf("Index ready")
			}
		}
	}

	// Reset timer to exclude setup time
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		// Clear the subtree processor state before each iteration
		blockAssembler.subtreeProcessor.Reset(genesisBlock.Header, nil, nil, false, nil)

		// Benchmark the loadUnminedTransactions function
		err := blockAssembler.loadUnminedTransactions(ctx, fullScan)
		require.NoError(b, err)
	}

	b.StopTimer()

	// Report additional metrics
	txCount64 := int64(txCount)
	b.ReportMetric(float64(txCount64)/b.Elapsed().Seconds()*float64(b.N), "tx/sec")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(txCount64), "ns/tx")
}

// generateTestTransactions creates a slice of test transactions with realistic structure.
func generateTestTransactions(tb testing.TB, count int) []*bt.Tx {
	transactions := make([]*bt.Tx, count)

	// Standard P2PKH locking script (OP_DUP OP_HASH160 <20 bytes> OP_EQUALVERIFY OP_CHECKSIG)
	prevLockingScript := bscript.NewFromBytes([]byte{
		0x76, 0xa9, 0x14, // OP_DUP OP_HASH160 <20 bytes push>
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 20 bytes of pubkey hash
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x88, 0xac, // OP_EQUALVERIFY OP_CHECKSIG
	})

	for i := 0; i < count; i++ {
		tx := bt.NewTx()

		// Add random locktime for variation
		tx.LockTime = uint32(i % 1000)

		// Create an input with a random previous tx
		prevTxID := make([]byte, 32)
		_, err := rand.Read(prevTxID)
		require.NoError(tb, err)

		prevHash, err := chainhash.NewHash(prevTxID)
		require.NoError(tb, err)

		input := &bt.Input{
			PreviousTxOutIndex: uint32(i % 10),
			PreviousTxSatoshis: 1000,
			PreviousTxScript:   prevLockingScript, // Required for extended transaction
			UnlockingScript:    bscript.NewFromBytes([]byte{0x01, 0x02, 0x03}),
			SequenceNumber:     0xffffffff,
		}
		_ = input.PreviousTxIDAdd(prevHash)

		tx.Inputs = []*bt.Input{input}

		// Add an output
		output := &bt.Output{
			Satoshis:      900,
			LockingScript: prevLockingScript, // Use same script for output
		}
		tx.Outputs = []*bt.Output{output}

		transactions[i] = tx
	}

	return transactions
}

// BenchmarkLoadUnminedTransactions_MixedStates benchmarks with transactions in various states
// (some with block IDs, some locked) to simulate real-world conditions.
func BenchmarkLoadUnminedTransactions_MixedStates(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	initPrometheusMetrics()

	txCount := 10000

	// Initialize Aerospike container
	b.Logf("Initializing Aerospike container...")
	aerospikeURL, teardown, err := testAerospike.InitAerospikeContainer()
	require.NoError(b, err)

	b.Cleanup(func() {
		_ = teardown()
	})

	parsedURL, err := url.Parse(aerospikeURL)
	require.NoError(b, err)

	tSettings := test.CreateBaseTestSettings(b)
	tSettings.UtxoStore.UtxoStore = parsedURL
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 1024
	tSettings.BlockAssembly.OnRestartValidateParentChain = false

	ctx := context.Background()
	logger := ulogger.TestLogger{}

	utxoStore, err := factory.NewStore(ctx, logger, tSettings, "benchmark", false)
	require.NoError(b, err)

	err = utxoStore.SetBlockHeight(100)
	require.NoError(b, err)

	blockchainStoreURL, err := url.Parse("sqlitememory://")
	require.NoError(b, err)

	blockchainStore, err := blockchainstore.NewStore(logger, blockchainStoreURL, tSettings)
	require.NoError(b, err)

	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockchainStore, nil, nil)
	require.NoError(b, err)

	// Add some blocks to the chain
	genesisBlock, err := blockchainStore.GetBlockByID(ctx, 0)
	require.NoError(b, err)

	prevBlock := genesisBlock.Header
	for i := 1; i <= 10; i++ {
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevBlock.Hash(),
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix() + int64(i*60)),
			Bits:           model.NBit{},
			Nonce:          uint32(i),
		}

		coinbaseTx, _ := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")

		err = blockchainClient.AddBlock(ctx, &model.Block{
			Header:           header,
			CoinbaseTx:       coinbaseTx,
			TransactionCount: 1,
			Subtrees:         []*chainhash.Hash{},
		}, "")
		require.NoError(b, err)

		prevBlock = header
	}

	blobStore := memory.New()

	stats := gocore.NewStat("benchmark")

	blockAssembler, err := NewBlockAssembler(ctx, logger, tSettings, stats, utxoStore, blobStore, blockchainClient, nil)
	require.NoError(b, err)

	blockAssembler.setBestBlockHeader(prevBlock, 10)
	blockAssembler.SetSkipWaitForPendingBlocks(true)

	// Start the subtree processor goroutine to handle Reset() calls
	blockAssembler.subtreeProcessor.Start(ctx)

	// Populate with mixed transaction states
	b.Logf("Populating UTXO store with %d transactions in mixed states...", txCount)

	transactions := generateTestTransactions(b, txCount)

	for i, tx := range transactions {
		switch i % 4 {
		case 0, 1: // 50% unmined
			_, err = utxoStore.Create(ctx, tx, 100)
			require.NoError(b, err)

		case 2: // 25% already mined in main chain
			_, err = utxoStore.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(
				utxo.MinedBlockInfo{
					BlockID:     uint32(5 + (i % 5)),
					BlockHeight: uint32(5 + (i % 5)),
					SubtreeIdx:  1,
				},
			))
			require.NoError(b, err)

		case 3: // 25% locked
			meta, err := utxoStore.Create(ctx, tx, 100)
			require.NoError(b, err)

			err = utxoStore.SetLocked(ctx, []chainhash.Hash{*meta.Tx.TxIDChainHash()}, true)
			require.NoError(b, err)
		}

		if (i+1)%1000 == 0 {
			b.Logf("Populated %d/%d transactions...", i+1, txCount)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		blockAssembler.subtreeProcessor.Reset(genesisBlock.Header, nil, nil, false, nil)
		err := blockAssembler.loadUnminedTransactions(ctx, false)
		require.NoError(b, err)
	}

	b.StopTimer()

	txCount64 := int64(txCount)
	b.ReportMetric(float64(txCount64)/b.Elapsed().Seconds()*float64(b.N), "tx/sec")
}
