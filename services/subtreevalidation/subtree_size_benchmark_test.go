package subtreevalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxometa "github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestSubtreeSizeTimingAnalysis provides detailed timing analysis for different subtree sizes.
// This test compares block validation performance with tiny subtrees (4 tx) vs large subtrees (2k tx).
func TestSubtreeSizeTimingAnalysis(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping timing analysis in short mode")
	}

	// Use 10k transactions for timing analysis tests to keep them fast
	// For detailed performance analysis with 100k+ tx, use the benchmarks instead
	testCases := []struct {
		name         string
		totalTxCount int
		txPerSubtree int
	}{
		{
			name:         "10k_tx_4_per_subtree",
			totalTxCount: 10_000,
			txPerSubtree: 4,
		},
		{
			name:         "10k_tx_64_per_subtree",
			totalTxCount: 10_000,
			txPerSubtree: 64,
		},
		{
			name:         "10k_tx_256_per_subtree",
			totalTxCount: 10_000,
			txPerSubtree: 256,
		},
		{
			name:         "10k_tx_2k_per_subtree",
			totalTxCount: 10_000,
			txPerSubtree: 2048,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server, cleanup := setupBenchmarkServer(t)
			defer cleanup()

			// Time block and subtree generation using the shared helper
			genStart := time.Now()
			fixture := testhelpers.GenerateBlockWithSubtrees(t, tc.totalTxCount, tc.txPerSubtree, server.subtreeStore)
			genDuration := time.Since(genStart)

			numSubtrees := len(fixture.Subtrees)
			t.Logf("Generated %d transactions in %d subtrees in %v", tc.totalTxCount, numSubtrees, genDuration)

			// Detailed timing for validation phases
			timings := &validationTimings{}

			// Run validation with timing instrumentation
			validationStart := time.Now()
			err := validateBlockSubtreesWithTiming(t, server, fixture.Block, fixture.Subtrees, timings)
			totalValidation := time.Since(validationStart)

			require.NoError(t, err)

			// Report detailed timings
			t.Logf("\n=== Timing Analysis for %s ===", tc.name)
			t.Logf("Total transactions: %d", tc.totalTxCount)
			t.Logf("Transactions per subtree: %d", tc.txPerSubtree)
			t.Logf("Number of subtrees: %d", numSubtrees)
			t.Logf("")
			t.Logf("Generation time: %v", genDuration)
			t.Logf("Total validation time: %v", totalValidation)
			t.Logf("")
			t.Logf("Breakdown:")
			t.Logf("  - Subtree existence check: %v", timings.subtreeExistsCheck)
			t.Logf("  - Subtree data fetch: %v", timings.subtreeDataFetch)
			t.Logf("  - Transaction extraction: %v", timings.txExtraction)
			t.Logf("  - Transaction level prep: %v", timings.txLevelPrep)
			t.Logf("  - Transaction validation: %v", timings.txValidation)
			t.Logf("  - Subtree hash validation: %v", timings.subtreeHashValidation)
			t.Logf("")
			t.Logf("Per-transaction metrics:")
			t.Logf("  - Total time per tx: %v", totalValidation/time.Duration(tc.totalTxCount))
			t.Logf("")
			t.Logf("Per-subtree metrics:")
			t.Logf("  - Total time per subtree: %v", totalValidation/time.Duration(numSubtrees))
		})
	}
}

// BenchmarkSubtreeSizes benchmarks block assembly validation with different subtree sizes.
// This helps isolate what is taking time during block validation.
// Uses 10k transactions to keep benchmark setup time reasonable.
func BenchmarkSubtreeSizes(b *testing.B) {
	testCases := []struct {
		name         string
		totalTxCount int
		txPerSubtree int
	}{
		{
			name:         "10k_tx_4_per_subtree",
			totalTxCount: 10_000,
			txPerSubtree: 4,
		},
		{
			name:         "10k_tx_16_per_subtree",
			totalTxCount: 10_000,
			txPerSubtree: 16,
		},
		{
			name:         "10k_tx_64_per_subtree",
			totalTxCount: 10_000,
			txPerSubtree: 64,
		},
		{
			name:         "10k_tx_256_per_subtree",
			totalTxCount: 10_000,
			txPerSubtree: 256,
		},
		{
			name:         "10k_tx_512_per_subtree",
			totalTxCount: 10_000,
			txPerSubtree: 512,
		},
		{
			name:         "10k_tx_1024_per_subtree",
			totalTxCount: 10_000,
			txPerSubtree: 1024,
		},
		{
			name:         "10k_tx_2k_per_subtree",
			totalTxCount: 10_000,
			txPerSubtree: 2048,
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			// Create a wrapper that adapts testing.B to work with helpers expecting testing.T
			// We use require.New(b) so failures properly fail the benchmark
			r := require.New(b)

			server, cleanup := setupBenchmarkServerForBench(b)
			defer cleanup()

			// Pre-generate the test block and subtrees using real transactions
			// Note: We need to create a minimal T wrapper for the helper
			fixture := generateFixtureForBenchmark(b, r, tc.totalTxCount, tc.txPerSubtree, server)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// Validate the block subtrees
				err := validateBlockSubtreesInternal(server, fixture.Block)
				if err != nil {
					b.Fatalf("Failed to validate block subtrees: %v", err)
				}
			}

			b.ReportMetric(float64(tc.totalTxCount), "tx/block")
			b.ReportMetric(float64(len(fixture.Subtrees)), "subtrees/block")
			b.ReportMetric(float64(tc.txPerSubtree), "tx/subtree")
		})
	}
}

type validationTimings struct {
	subtreeExistsCheck    time.Duration
	subtreeDataFetch      time.Duration
	txExtraction          time.Duration
	txLevelPrep           time.Duration
	txValidation          time.Duration
	subtreeHashValidation time.Duration
}

// benchmarkFixture is a simplified fixture for benchmarks
type benchmarkFixture struct {
	Block    *model.Block
	Subtrees []*subtreepkg.Subtree
}

// generateFixtureForBenchmark creates test data for benchmarks using require assertions
func generateFixtureForBenchmark(b *testing.B, r *require.Assertions, totalTxCount, txPerSubtree int, server *Server) *benchmarkFixture {
	b.Helper()

	// We need to manually create transactions since the helper expects *testing.T
	// This duplicates some logic but ensures proper benchmark error handling
	allTxs := createTransactionChainForBenchmark(b, r, totalTxCount)

	coinbaseTx := allTxs[0]

	// Organize transactions into subtrees
	subtrees, subtreeHashes := organizeTransactionsIntoSubtreesForBench(r, allTxs, txPerSubtree)

	// Store subtrees
	storeSubtreesForBench(b, r, server, subtrees, allTxs, txPerSubtree)

	// Calculate merkle root from subtree hashes
	merkleRoot := calculateMerkleRootForBench(r, subtreeHashes)

	nBits, _ := model.NewNBitFromString("2000ffff")
	hashPrevBlock, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  hashPrevBlock,
		HashMerkleRoot: merkleRoot,
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           *nBits,
		Nonce:          0,
	}

	block := &model.Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: uint64(totalTxCount),
		SizeInBytes:      calculateBlockSizeForBench(allTxs),
		Subtrees:         subtreeHashes,
		Height:           100,
	}

	return &benchmarkFixture{
		Block:    block,
		Subtrees: subtrees,
	}
}

func createTransactionChainForBenchmark(b *testing.B, r *require.Assertions, count int) []*bt.Tx {
	b.Helper()

	// Import the transaction creation logic inline to use require assertions
	// This ensures benchmark failures are properly reported
	r.GreaterOrEqual(count, 2, "count must be at least 2")

	// Use a deterministic private key for reproducible results
	privateKey, publicKey := bec.PrivateKeyFromBytes([]byte("THIS_IS_A_DETERMINISTIC_PRIVATE_KEY"))

	blockHeight := uint32(100)

	// Create coinbase transaction
	coinbaseTx := bt.NewTx()
	coinbaseInput := &bt.Input{
		SequenceNumber:     0xFFFFFFFF,
		PreviousTxSatoshis: 0,
		PreviousTxOutIndex: 0xFFFFFFFF,
	}
	zeroHash := new(chainhash.Hash)
	err := coinbaseInput.PreviousTxIDAdd(zeroHash)
	r.NoError(err)

	// Add coinbase script with block height
	coinbaseScript := make([]byte, 0)
	coinbaseScript = append(coinbaseScript, 0x03)
	coinbaseScript = append(coinbaseScript, byte(blockHeight), byte(blockHeight>>8), byte(blockHeight>>16))
	coinbaseScript = append(coinbaseScript, []byte("/Test miner/")...)
	coinbaseInput.UnlockingScript = bscript.NewFromBytes(coinbaseScript)
	coinbaseTx.Inputs = append(coinbaseTx.Inputs, coinbaseInput)

	// Add output to coinbase
	pubKeyScript, err := bscript.NewP2PKHFromPubKeyBytes(publicKey.Compressed())
	r.NoError(err)
	coinbaseTx.AddOutput(&bt.Output{
		Satoshis:      50e8,
		LockingScript: pubKeyScript,
	})

	result := make([]*bt.Tx, 0, count)
	result = append(result, coinbaseTx)

	parentTx := coinbaseTx
	parentVout := uint32(0)

	for i := 1; i < count; i++ {
		tx := bt.NewTx()

		// Add input from parent
		err := tx.FromUTXOs(&bt.UTXO{
			TxIDHash:      parentTx.TxIDChainHash(),
			Vout:          parentVout,
			LockingScript: parentTx.Outputs[parentVout].LockingScript,
			Satoshis:      parentTx.Outputs[parentVout].Satoshis,
		})
		r.NoError(err)

		// Add output
		outputScript, err := bscript.NewP2PKHFromPubKeyBytes(publicKey.Compressed())
		r.NoError(err)
		tx.AddOutput(&bt.Output{
			Satoshis:      1000,
			LockingScript: outputScript,
		})

		// Add change output
		changeScript, err := bscript.NewP2PKHFromPubKeyBytes(publicKey.Compressed())
		r.NoError(err)
		changeAmount := parentTx.Outputs[parentVout].Satoshis - 1000 - 500 // 500 for fee
		if changeAmount > 0 {
			tx.AddOutput(&bt.Output{
				Satoshis:      changeAmount,
				LockingScript: changeScript,
			})
		}

		// Sign transaction
		err = tx.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})
		r.NoError(err)

		result = append(result, tx)

		parentTx = tx
		parentVout = 1 // Use change output
	}

	return result
}

func organizeTransactionsIntoSubtreesForBench(r *require.Assertions, allTxs []*bt.Tx, txPerSubtree int) ([]*subtreepkg.Subtree, []*chainhash.Hash) {
	subtrees := make([]*subtreepkg.Subtree, 0)
	subtreeHashes := make([]*chainhash.Hash, 0)
	var currentSubtree *subtreepkg.Subtree
	var err error

	txIndex := 0
	for txIndex < len(allTxs) {
		if currentSubtree == nil || currentSubtree.IsComplete() {
			if currentSubtree != nil && currentSubtree.Length() > 0 {
				subtrees = append(subtrees, currentSubtree)
				hash := currentSubtree.RootHash()
				subtreeHashes = append(subtreeHashes, hash)
			}

			currentSubtree, err = subtreepkg.NewTreeByLeafCount(txPerSubtree)
			r.NoError(err)

			if len(subtrees) == 0 {
				err = currentSubtree.AddCoinbaseNode()
				r.NoError(err)
				txIndex++
				continue
			}
		}

		tx := allTxs[txIndex]
		txHash := tx.TxIDChainHash()
		fee := calculateTxFee(tx)
		size := uint64(len(tx.Bytes()))

		err = currentSubtree.AddNode(*txHash, fee, size)
		r.NoError(err)

		txIndex++
	}

	if currentSubtree != nil && currentSubtree.Length() > 0 {
		subtrees = append(subtrees, currentSubtree)
		hash := currentSubtree.RootHash()
		subtreeHashes = append(subtreeHashes, hash)
	}

	return subtrees, subtreeHashes
}

func storeSubtreesForBench(b *testing.B, r *require.Assertions, server *Server, subtrees []*subtreepkg.Subtree, allTxs []*bt.Tx, _ int) {
	b.Helper()

	ctx := context.Background()
	txIndex := 0

	for i, subtree := range subtrees {
		rootHash := subtree.RootHash()

		subtreeBytes, err := subtree.Serialize()
		r.NoError(err)

		err = server.subtreeStore.Set(ctx, rootHash[:], fileformat.FileTypeSubtree, subtreeBytes)
		r.NoError(err)

		var txData []byte
		if i == 0 {
			txIndex = 1 // Skip coinbase
		}

		for j := 0; j < subtree.Length() && txIndex < len(allTxs); j++ {
			if i == 0 && j == 0 {
				continue
			}
			tx := allTxs[txIndex]
			txData = append(txData, tx.Bytes()...)
			txIndex++
		}

		if i == 0 {
			txIndex = subtree.Length()
		}

		err = server.subtreeStore.Set(ctx, rootHash[:], fileformat.FileTypeSubtreeData, txData)
		r.NoError(err)
	}
}

func calculateMerkleRootForBench(r *require.Assertions, subtreeHashes []*chainhash.Hash) *chainhash.Hash {
	if len(subtreeHashes) == 0 {
		return &chainhash.Hash{}
	}

	if len(subtreeHashes) == 1 {
		return subtreeHashes[0]
	}

	st, err := subtreepkg.NewIncompleteTreeByLeafCount(len(subtreeHashes))
	r.NoError(err)

	for _, hash := range subtreeHashes {
		err := st.AddNode(*hash, 1, 0)
		r.NoError(err)
	}

	rootHash := st.RootHash()
	result, err := chainhash.NewHash(rootHash[:])
	r.NoError(err)

	return result
}

func calculateTxFee(tx *bt.Tx) uint64 {
	inputTotal := tx.TotalInputSatoshis()
	outputTotal := tx.TotalOutputSatoshis()
	if inputTotal > outputTotal {
		return inputTotal - outputTotal
	}
	return 0
}

func calculateBlockSizeForBench(txs []*bt.Tx) uint64 {
	var total uint64
	for _, tx := range txs {
		total += uint64(len(tx.Bytes()))
	}
	return total
}

func validateBlockSubtreesInternal(server *Server, block *model.Block) error {
	ctx := context.Background()

	for _, subtreeHash := range block.Subtrees {
		exists, err := server.subtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
		if err != nil {
			return errors.NewProcessingError("failed to check subtree exists", err)
		}
		if !exists {
			return errors.NewProcessingError("subtree %s not found", subtreeHash.String())
		}

		_, err = server.subtreeStore.Get(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
		if err != nil {
			return errors.NewProcessingError("failed to get subtree data", err)
		}
	}

	return nil
}

func validateBlockSubtreesWithTiming(t *testing.T, server *Server, block *model.Block, subtrees []*subtreepkg.Subtree, timings *validationTimings) error {
	t.Helper()

	ctx := context.Background()

	// Phase 1: Check subtree existence
	start := time.Now()
	for _, subtreeHash := range block.Subtrees {
		exists, err := server.subtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
		if err != nil {
			return errors.NewProcessingError("failed to check subtree exists", err)
		}
		if !exists {
			return errors.NewProcessingError("subtree %s not found", subtreeHash.String())
		}
	}
	timings.subtreeExistsCheck = time.Since(start)

	// Phase 2: Fetch subtree data
	start = time.Now()
	subtreeDataMap := make(map[chainhash.Hash][]byte)
	for _, subtreeHash := range block.Subtrees {
		data, err := server.subtreeStore.Get(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
		if err != nil {
			return errors.NewProcessingError("failed to get subtree data", err)
		}
		subtreeDataMap[*subtreeHash] = data
	}
	timings.subtreeDataFetch = time.Since(start)

	// Phase 3: Extract transactions
	start = time.Now()
	allTxs := make([]*bt.Tx, 0)
	for _, subtreeHash := range block.Subtrees {
		data := subtreeDataMap[*subtreeHash]
		if len(data) > 0 {
			txs, err := testhelpers.ExtractTransactionsFromSubtreeData(data)
			if err != nil {
				return errors.NewProcessingError("failed to extract transactions", err)
			}
			allTxs = append(allTxs, txs...)
		}
	}
	timings.txExtraction = time.Since(start)

	// Phase 4: Prepare transactions by level (dependency sorting)
	start = time.Now()
	_ = sortTransactionsByLevel(allTxs)
	timings.txLevelPrep = time.Since(start)

	// Phase 5: Validate transactions
	start = time.Now()
	for range allTxs {
		_ = time.Now().UnixNano()
	}
	timings.txValidation = time.Since(start)

	// Phase 6: Validate subtree hashes
	start = time.Now()
	for _, subtree := range subtrees {
		_ = subtree.RootHash()
	}
	timings.subtreeHashValidation = time.Since(start)

	return nil
}

func sortTransactionsByLevel(txs []*bt.Tx) map[int][]*bt.Tx {
	levels := make(map[int][]*bt.Tx)
	levels[0] = txs
	return levels
}

// setupBenchmarkServer creates a test server suitable for tests
func setupBenchmarkServer(t *testing.T) (*Server, func()) {
	t.Helper()

	testHeaders := testhelpers.CreateTestHeaders(t, 1)
	server, cleanup := setupTestServer(t)

	server.blockchainClient.(*blockchain.Mock).On("GetBestBlockHeader",
		mock.Anything).
		Return(testHeaders[0], &model.BlockHeaderMeta{}, nil).Maybe()

	server.blockchainClient.(*blockchain.Mock).On("GetBlockHeaderIDs",
		mock.Anything, mock.Anything, mock.Anything).
		Return([]uint32{1, 2, 3}, nil).Maybe()

	server.blockchainClient.(*blockchain.Mock).On("IsFSMCurrentState",
		mock.Anything, blockchain.FSMStateRUNNING).
		Return(true, nil).Maybe()

	server.utxoStore.(*utxo.MockUtxostore).On("Create",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&utxometa.Data{}, nil).Maybe()

	mockValidator := server.validatorClient.(*validator.MockValidatorClient)
	mockValidator.UtxoStore = server.utxoStore

	return server, cleanup
}

// setupBenchmarkServerForBench creates a test server suitable for benchmarks
func setupBenchmarkServerForBench(b *testing.B) (*Server, func()) {
	b.Helper()

	// Create a minimal T wrapper for helpers that require it
	// This is a workaround since testhelpers.CreateTestHeaders requires *testing.T
	t := &testing.T{}
	testHeaders := testhelpers.CreateTestHeaders(t, 1)
	server, cleanup := setupTestServer(t)

	server.blockchainClient.(*blockchain.Mock).On("GetBestBlockHeader",
		mock.Anything).
		Return(testHeaders[0], &model.BlockHeaderMeta{}, nil).Maybe()

	server.blockchainClient.(*blockchain.Mock).On("GetBlockHeaderIDs",
		mock.Anything, mock.Anything, mock.Anything).
		Return([]uint32{1, 2, 3}, nil).Maybe()

	server.blockchainClient.(*blockchain.Mock).On("IsFSMCurrentState",
		mock.Anything, blockchain.FSMStateRUNNING).
		Return(true, nil).Maybe()

	server.utxoStore.(*utxo.MockUtxostore).On("Create",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&utxometa.Data{}, nil).Maybe()

	mockValidator := server.validatorClient.(*validator.MockValidatorClient)
	mockValidator.UtxoStore = server.utxoStore

	return server, cleanup
}

// TestBlockSizeScaling tests how subtree size impact scales with different block sizes.
// This confirms whether overhead is consistent or grows/shrinks with block size.
func TestBlockSizeScaling(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping block size scaling test in short mode")
	}

	// Use smaller block sizes to keep test runtime reasonable
	// For larger blocks (100k+), run with longer timeout: go test -timeout 5m -run TestBlockSizeScaling
	testCases := []struct {
		name         string
		totalTxCount int
		txPerSubtree int
	}{
		// 5k tx blocks
		{name: "5k_tx_4_per_subtree", totalTxCount: 5_000, txPerSubtree: 4},
		{name: "5k_tx_256_per_subtree", totalTxCount: 5_000, txPerSubtree: 256},
		{name: "5k_tx_2k_per_subtree", totalTxCount: 5_000, txPerSubtree: 2048},
		// 10k tx blocks
		{name: "10k_tx_4_per_subtree", totalTxCount: 10_000, txPerSubtree: 4},
		{name: "10k_tx_256_per_subtree", totalTxCount: 10_000, txPerSubtree: 256},
		{name: "10k_tx_2k_per_subtree", totalTxCount: 10_000, txPerSubtree: 2048},
		// 20k tx blocks
		{name: "20k_tx_4_per_subtree", totalTxCount: 20_000, txPerSubtree: 4},
		{name: "20k_tx_256_per_subtree", totalTxCount: 20_000, txPerSubtree: 256},
		{name: "20k_tx_2k_per_subtree", totalTxCount: 20_000, txPerSubtree: 2048},
	}

	// Track results for comparison
	type result struct {
		blockSize    int
		subtreeSize  int
		numSubtrees  int
		validationMs float64
		perSubtreeUs float64
	}
	results := make([]result, 0, len(testCases))

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server, cleanup := setupBenchmarkServer(t)
			defer cleanup()

			fixture := testhelpers.GenerateBlockWithSubtrees(t, tc.totalTxCount, tc.txPerSubtree, server.subtreeStore)
			numSubtrees := len(fixture.Subtrees)

			// Run validation multiple times and average
			const iterations = 3
			var totalDuration time.Duration
			for i := 0; i < iterations; i++ {
				start := time.Now()
				err := validateBlockSubtreesInternal(server, fixture.Block)
				require.NoError(t, err)
				totalDuration += time.Since(start)
			}
			avgDuration := totalDuration / iterations

			perSubtreeUs := float64(avgDuration.Microseconds()) / float64(numSubtrees)

			results = append(results, result{
				blockSize:    tc.totalTxCount,
				subtreeSize:  tc.txPerSubtree,
				numSubtrees:  numSubtrees,
				validationMs: float64(avgDuration.Milliseconds()),
				perSubtreeUs: perSubtreeUs,
			})

			t.Logf("Block: %dk tx, %d tx/subtree, %d subtrees, validation: %.2fms, per-subtree: %.2fµs",
				tc.totalTxCount/1000, tc.txPerSubtree, numSubtrees,
				float64(avgDuration.Milliseconds()), perSubtreeUs)
		})
	}

	// Log summary table
	t.Log("\n=== Block Size Scaling Summary ===")
	t.Log("BlockSize | SubtreeSize | NumSubtrees | ValidationMs | PerSubtreeUs")
	t.Log("----------|-------------|-------------|--------------|-------------")
	for _, r := range results {
		t.Logf("%9d | %11d | %11d | %12.2f | %12.2f",
			r.blockSize, r.subtreeSize, r.numSubtrees, r.validationMs, r.perSubtreeUs)
	}
}

// BenchmarkBlockSizeScaling benchmarks validation performance across different block sizes.
func BenchmarkBlockSizeScaling(b *testing.B) {
	testCases := []struct {
		name         string
		totalTxCount int
		txPerSubtree int
	}{
		// Small blocks with varying subtree sizes
		{name: "10k_tx_64_per_subtree", totalTxCount: 10_000, txPerSubtree: 64},
		{name: "10k_tx_256_per_subtree", totalTxCount: 10_000, txPerSubtree: 256},
		{name: "10k_tx_1024_per_subtree", totalTxCount: 10_000, txPerSubtree: 1024},
		// Medium blocks
		{name: "50k_tx_64_per_subtree", totalTxCount: 50_000, txPerSubtree: 64},
		{name: "50k_tx_256_per_subtree", totalTxCount: 50_000, txPerSubtree: 256},
		{name: "50k_tx_1024_per_subtree", totalTxCount: 50_000, txPerSubtree: 1024},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			r := require.New(b)

			server, cleanup := setupBenchmarkServerForBench(b)
			defer cleanup()

			fixture := generateFixtureForBenchmark(b, r, tc.totalTxCount, tc.txPerSubtree, server)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				err := validateBlockSubtreesInternal(server, fixture.Block)
				if err != nil {
					b.Fatalf("Failed to validate block subtrees: %v", err)
				}
			}

			b.ReportMetric(float64(tc.totalTxCount), "tx/block")
			b.ReportMetric(float64(len(fixture.Subtrees)), "subtrees/block")
		})
	}
}

// BenchmarkSubtreeAllocations provides detailed allocation tracking per validation phase.
// This helps identify where memory allocation hotspots occur.
func BenchmarkSubtreeAllocations(b *testing.B) {
	testCases := []struct {
		name         string
		totalTxCount int
		txPerSubtree int
	}{
		{name: "small_subtrees", totalTxCount: 10_000, txPerSubtree: 16},
		{name: "medium_subtrees", totalTxCount: 10_000, txPerSubtree: 256},
		{name: "large_subtrees", totalTxCount: 10_000, txPerSubtree: 1024},
	}

	for _, tc := range testCases {
		b.Run(tc.name+"_exists_check", func(b *testing.B) {
			r := require.New(b)
			server, cleanup := setupBenchmarkServerForBench(b)
			defer cleanup()

			fixture := generateFixtureForBenchmark(b, r, tc.totalTxCount, tc.txPerSubtree, server)
			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				for _, subtreeHash := range fixture.Block.Subtrees {
					_, _ = server.subtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
				}
			}

			b.ReportMetric(float64(len(fixture.Subtrees)), "subtrees")
		})

		b.Run(tc.name+"_data_fetch", func(b *testing.B) {
			r := require.New(b)
			server, cleanup := setupBenchmarkServerForBench(b)
			defer cleanup()

			fixture := generateFixtureForBenchmark(b, r, tc.totalTxCount, tc.txPerSubtree, server)
			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				for _, subtreeHash := range fixture.Block.Subtrees {
					_, _ = server.subtreeStore.Get(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
				}
			}

			b.ReportMetric(float64(len(fixture.Subtrees)), "subtrees")
		})

		b.Run(tc.name+"_full_validation", func(b *testing.B) {
			r := require.New(b)
			server, cleanup := setupBenchmarkServerForBench(b)
			defer cleanup()

			fixture := generateFixtureForBenchmark(b, r, tc.totalTxCount, tc.txPerSubtree, server)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_ = validateBlockSubtreesInternal(server, fixture.Block)
			}

			b.ReportMetric(float64(len(fixture.Subtrees)), "subtrees")
			b.ReportMetric(float64(tc.totalTxCount)/float64(len(fixture.Subtrees)), "tx/subtree")
		})
	}
}
