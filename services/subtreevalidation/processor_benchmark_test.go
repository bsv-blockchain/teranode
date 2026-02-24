package subtreevalidation

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// BenchmarkSubtreeProcessor benchmarks the subtree processor for different block configurations.
func BenchmarkSubtreeProcessor(b *testing.B) {
	testCases := []struct {
		name         string
		totalTxCount int
		txPerSubtree int
	}{
		// Small blocks
		{"100_tx_64_per_subtree", 100, 64},
		{"500_tx_64_per_subtree", 500, 64},
		{"500_tx_256_per_subtree", 500, 256},
		// Medium blocks
		{"1k_tx_64_per_subtree", 1_000, 64},
		{"1k_tx_256_per_subtree", 1_000, 256},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			runProcessorBenchmark(b, tc.totalTxCount, tc.txPerSubtree)
		})
	}
}

// TestSubtreeProcessorTiming provides detailed timing for subtree processing.
func TestSubtreeProcessorTiming(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping processor timing test in short mode")
	}

	testCases := []struct {
		name         string
		totalTxCount int
		txPerSubtree int
	}{
		{"100_tx_64_per_subtree", 100, 64},
		{"500_tx_256_per_subtree", 500, 256},
		{"1k_tx_256_per_subtree", 1_000, 256},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			duration := runTimedProcessorTest(t, tc.totalTxCount, tc.txPerSubtree)
			t.Logf("\n=== Processor Timing: %s ===", tc.name)
			t.Logf("Duration: %v", duration)
		})
	}
}

func runProcessorBenchmark(b *testing.B, totalTxCount, txPerSubtree int) {
	b.Helper()

	// Create a real testing.T for setup (required by test helpers)
	t := &testing.T{}

	// Pre-generate shared test data once using the helper
	// This stores FileTypeSubtree and FileTypeSubtreeData
	tempStore := blobmemory.New()
	fixture := testhelpers.GenerateBlockWithSubtrees(t, totalTxCount, txPerSubtree, tempStore)

	// Now store FileTypeSubtreeToCheck for legacy mode (using same data as FileTypeSubtree)
	ctx := context.Background()
	for _, subtreeHash := range fixture.Block.Subtrees {
		// Copy FileTypeSubtree to FileTypeSubtreeToCheck for legacy mode
		subtreeBytes, err := tempStore.Get(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
		if err != nil {
			b.Fatalf("Failed to get subtree: %v", err)
		}
		err = tempStore.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes)
		if err != nil {
			b.Fatalf("Failed to store subtreeToCheck: %v", err)
		}

		// Clear the FileTypeSubtree validation marker from template
		_ = tempStore.Del(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
	}

	blockBytes, err := fixture.Block.Bytes()
	if err != nil {
		b.Fatalf("Failed to serialize block: %v", err)
	}

	request := &subtreevalidation_api.CheckBlockSubtreesRequest{
		Block:   blockBytes,
		BaseUrl: "legacy", // Use legacy mode to load from store instead of HTTP
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Create fresh server for each iteration to avoid state issues
		// This is done outside the timer
		b.StopTimer()
		server, subtreeStore, cleanup := setupRealServerWithIterationID(t, i)

		// Copy subtree data to the fresh store
		copyStoreData(tempStore, subtreeStore, fixture)

		// Create coinbase UTXO
		_, err := server.utxoStore.Create(ctx, fixture.CoinbaseTx, 0)
		if err != nil {
			cleanup()
			b.Fatalf("Failed to create coinbase UTXO: %v", err)
		}
		b.StartTimer()

		_, err = server.CheckBlockSubtrees(context.Background(), request)

		b.StopTimer()
		cleanup()
		b.StartTimer()

		if err != nil {
			b.Fatalf("CheckBlockSubtrees failed: %v", err)
		}
	}

	b.ReportMetric(float64(totalTxCount), "tx/block")
	b.ReportMetric(float64(len(fixture.Block.Subtrees)), "subtrees/block")
}

func runTimedProcessorTest(t *testing.T, totalTxCount, txPerSubtree int) time.Duration {
	t.Helper()

	server, subtreeStore, cleanup := setupRealServer(t)
	defer cleanup()

	// Generate test data with real transactions
	fixture := testhelpers.GenerateBlockWithSubtrees(t, totalTxCount, txPerSubtree, subtreeStore)

	// Store FileTypeSubtreeToCheck for legacy mode to work
	ctx := context.Background()
	for i, subtree := range fixture.Subtrees {
		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)
		err = subtreeStore.Set(ctx, fixture.Block.Subtrees[i][:], fileformat.FileTypeSubtreeToCheck, subtreeBytes)
		require.NoError(t, err)
	}

	// Only create the coinbase UTXO - the rest will be created during validation
	_, err := server.utxoStore.Create(ctx, fixture.CoinbaseTx, 0)
	require.NoError(t, err)

	clearSubtreeValidationState(server, fixture.Block.Subtrees, fixture.Transactions)

	blockBytes, err := fixture.Block.Bytes()
	require.NoError(t, err)

	request := &subtreevalidation_api.CheckBlockSubtreesRequest{
		Block:   blockBytes,
		BaseUrl: "legacy",
	}

	start := time.Now()
	_, err = server.CheckBlockSubtrees(context.Background(), request)
	duration := time.Since(start)

	require.NoError(t, err)

	return duration
}

// setupRealServer creates a server with real stores for benchmarking
func setupRealServer(t *testing.T) (*Server, blob.Store, func()) {
	return setupRealServerWithIterationID(t, 0)
}

// blockchainMockWrapper wraps the blockchain.Mock to provide dynamic MTP calculation
type blockchainMockWrapper struct {
	*blockchain.Mock
}

// GetMedianTimePastForHeights overrides the mock to return dynamic values
func (w *blockchainMockWrapper) GetMedianTimePastForHeights(ctx context.Context, heights []uint32) ([]uint32, error) {
	// Return a timestamp for each requested height
	mtps := make([]uint32, len(heights))
	for i := range mtps {
		mtps[i] = 1000000 // Simple mock timestamp
	}
	return mtps, nil
}

// setupRealServerWithIterationID creates a server with a unique database per iteration
func setupRealServerWithIterationID(t *testing.T, iterationID int) (*Server, blob.Store, func()) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.SubtreeValidation.SpendBatcherSize = 100
	// Disable block assembly for benchmarks since we don't have a block assembly client
	tSettings.BlockAssembly.Disabled = true

	// Create real SQL UTXO store (in-memory SQLite) with unique name per iteration
	utxoStoreURL, err := url.Parse(fmt.Sprintf("sqlitememory:///benchmark_%d", iterationID))
	if err != nil {
		panic(err)
	}

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	if err != nil {
		panic(err)
	}

	txStore := blobmemory.New()
	subtreeStore := blobmemory.New()

	// Use a mock blockchain client for benchmark setup
	baseMock := &blockchain.Mock{}
	mockBlockchainClient := &blockchainMockWrapper{Mock: baseMock}

	// Create a real validator for proper transaction chain validation
	validatorClient, err := validator.New(ctx, logger, tSettings, utxoStore, nil, nil, nil, mockBlockchainClient)
	if err != nil {
		panic(err)
	}

	// Set up default mocks for the blockchain client (on the underlying Mock)
	testHeaders := testhelpers.CreateTestHeaders(t, 1)

	// The generated blocks use this genesis hash as their parent
	genesisHash, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")

	baseMock.On("GetBestBlockHeader", mock.Anything).
		Return(testHeaders[0], &model.BlockHeaderMeta{ID: 100}, nil).Maybe()

	baseMock.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).
		Return([]uint32{100, 99, 98}, nil).Maybe()

	baseMock.On("IsFSMCurrentState", mock.Anything, blockchain.FSMStateRUNNING).
		Return(true, nil).Maybe()

	runningState := blockchain.FSMStateRUNNING
	baseMock.On("GetFSMCurrentState", mock.Anything).
		Return(&runningState, nil).Maybe()

	baseMock.On("GetBlockExists", mock.Anything, mock.Anything).
		Return(true, nil).Maybe()

	// Mock GetBlockHeader for the genesis hash (used by generated blocks)
	baseMock.On("GetBlockHeader", mock.Anything, genesisHash).
		Return(testHeaders[0], &model.BlockHeaderMeta{ID: 99}, nil).Maybe()

	baseMock.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).
		Return(true, nil).Maybe()

	// Create server directly without subscription to avoid blockchain errors
	server := &Server{
		logger:           logger,
		settings:         tSettings,
		subtreeStore:     subtreeStore,
		txStore:          txStore,
		utxoStore:        utxoStore,
		validatorClient:  validatorClient,
		blockchainClient: mockBlockchainClient,
	}

	// Create a fresh quorum for this server instance
	tmpDir := t.TempDir()
	server.quorum, err = NewQuorum(logger, subtreeStore, tmpDir)
	if err != nil {
		panic(err)
	}

	// Initialize orphanage
	orphanage, err := NewOrphanage(time.Minute*10, 100, logger)
	if err != nil {
		panic(err)
	}
	server.orphanage = orphanage

	// Initialize server state to avoid nil pointer dereference
	currentBlockIDsMap := map[uint32]bool{100: true, 99: true, 98: true}
	server.currentBlockIDsMap.Store(&currentBlockIDsMap)

	bestBlockHeaderMeta := &model.BlockHeaderMeta{ID: 100, Height: 100}
	server.bestBlockHeaderMeta.Store(bestBlockHeaderMeta)

	return server, subtreeStore, func() {
		// Cleanup
	}
}

func clearSubtreeValidationState(server *Server, subtreeHashes []*chainhash.Hash, allTxs []*bt.Tx) {
	ctx := context.Background()

	for _, hash := range subtreeHashes {
		// Delete validated subtree marker so it will be reprocessed
		_ = server.subtreeStore.Del(ctx, hash[:], fileformat.FileTypeSubtree)
		// Keep SubtreeData and SubtreeToCheck so they can be loaded
	}

	// Delete all non-coinbase UTXOs created during validation so they'll be recreated
	for _, tx := range allTxs {
		if !tx.IsCoinbase() {
			_ = server.utxoStore.Delete(ctx, tx.TxIDChainHash())
		}
	}
}

// copyStoreData copies subtree data from source store to destination store
func copyStoreData(src, dst blob.Store, fixture *testhelpers.SubtreeTestFixture) {
	ctx := context.Background()

	for _, hash := range fixture.Block.Subtrees {
		// Copy FileTypeSubtreeToCheck
		data, err := src.Get(ctx, hash[:], fileformat.FileTypeSubtreeToCheck)
		if err == nil {
			_ = dst.Set(ctx, hash[:], fileformat.FileTypeSubtreeToCheck, data)
		}

		// Copy FileTypeSubtreeData
		data, err = src.Get(ctx, hash[:], fileformat.FileTypeSubtreeData)
		if err == nil {
			_ = dst.Set(ctx, hash[:], fileformat.FileTypeSubtreeData, data)
		}
	}
}

// BenchmarkStreamingProcessorPhases benchmarks individual phases of the streaming processor.
func BenchmarkStreamingProcessorPhases(b *testing.B) {
	testCases := []struct {
		name    string
		txCount int
	}{
		{"100_tx", 100},
		{"500_tx", 500},
		{"1k_tx", 1_000},
	}

	for _, tc := range testCases {
		b.Run("FilterValidated/"+tc.name, func(b *testing.B) {
			benchmarkFilterAlreadyValidated(b, tc.txCount)
		})

		b.Run("ClassifyProcess/"+tc.name, func(b *testing.B) {
			benchmarkClassifyAndProcess(b, tc.txCount)
		})
	}
}

func benchmarkFilterAlreadyValidated(b *testing.B, txCount int) {
	b.Helper()

	t := &testing.T{}
	server, _, cleanup := setupRealServer(t)
	defer cleanup()

	// Create test transactions using real transaction chain
	allTxs := transactions.CreateTestTransactionChainWithCount(t, uint32(txCount))

	blockHash := chainhash.Hash{}
	blockIds := make(map[uint32]bool)
	sp := newStreamingProcessor(server, blockHash, 100, blockIds)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = sp.filterAlreadyValidated(context.Background(), allTxs)
	}

	b.ReportMetric(float64(txCount), "tx/op")
}

func benchmarkClassifyAndProcess(b *testing.B, txCount int) {
	b.Helper()

	t := &testing.T{}
	server, _, cleanup := setupRealServer(t)
	defer cleanup()

	// Create test transactions using real transaction chain
	allTxs := transactions.CreateTestTransactionChainWithCount(t, uint32(txCount))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		blockHash := chainhash.Hash{}
		blockIds := make(map[uint32]bool)
		sp := newStreamingProcessor(server, blockHash, 100, blockIds)
		sp.validatorOptions = validator.ProcessOptions()

		// Classify and process via streaming API
		_ = sp.classifyAndProcessForTest(context.Background(), allTxs)
	}

	b.ReportMetric(float64(txCount), "tx/op")
}
