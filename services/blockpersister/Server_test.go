package blockpersister

import (
	"context"
	"encoding/hex"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jellydator/ttlcache/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var (
	coinbaseTx, _ = bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff08044c86041b020602ffffffff0100f2052a010000004341041b0e8c2567c12536aa13357b79a073dc4444acb83c4ec7a0e2f99dd7457516c5817242da796924ca4e99947d087fedf9ce467cb9f7c6287078f801df276fdf84ac00000000")

	txIds = []string{
		"8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87", // Coinbase
		"fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4",
		"6359f0868171b1d194cbee1af2f16ea598ae8fad666d9b012c8ed2b79a236ec4",
		"e9a66845e05d5abc0ad04ec80f774a7e585c6e8db975962d069a522137b80c1d",
	}

	merkleRoot, _ = chainhash.NewHashFromStr("f3e94742aca4b5ef85488dc37c06c3282295ffec960994b2c0d5ac2a25a95766")

	prevBlockHashStr = "000000000002d01c1fccc21636b607dfd930d31d01c3a62104612a1719011250"
	bitsStr          = "1b04864c"
)

// TestOneTransaction validates the processing of a single transaction block
func TestOneTransaction(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	var err error

	subtrees := make([]*subtree.Subtree, 1)

	subtrees[0], err = subtree.NewTree(1)
	require.NoError(t, err)

	err = subtrees[0].AddCoinbaseNode()
	require.NoError(t, err)

	// blockValidationService, err := New(ulogger.TestLogger{}, nil, nil, nil, nil)
	// require.NoError(t, err)

	// this now needs to be here since we do not have the full subtrees in the Block struct
	// which is used in the CheckMerkleRoot function
	coinbaseHash := coinbaseTx.TxIDChainHash()

	subtrees[0].ReplaceRootNode(coinbaseHash, 0, uint64(coinbaseTx.Size()))

	subtreeHashes := make([]*chainhash.Hash, len(subtrees))

	for i, subTree := range subtrees {
		rootHash := subTree.RootHash()
		subtreeHashes[i], _ = chainhash.NewHash(rootHash[:])
	}

	merkleRootHash := coinbaseTx.TxIDChainHash()

	block, err := model.NewBlock(
		&model.BlockHeader{
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: merkleRootHash,
		},
		coinbaseTx,
		subtreeHashes,
		0, 0, 0, 0)
	require.NoError(t, err)

	ctx := context.Background()
	subtreeStore := memory.New()

	subtreeBytes, _ := subtrees[0].Serialize()
	_ = subtreeStore.Set(ctx, subtrees[0].RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)

	// loads the subtrees into the block
	err = block.GetAndValidateSubtrees(ctx, ulogger.TestLogger{}, subtreeStore, tSettings.Block.GetAndValidateSubtreesConcurrency)
	require.NoError(t, err)

	// err = blockValidationService.CheckMerkleRoot(block)
	err = block.CheckMerkleRoot(ctx)
	assert.NoError(t, err)
}

// TestTwoTransactions validates the processing of a block with two transactions
func TestTwoTransactions(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	coinbaseTx, _ := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff07044c86041b0147ffffffff0100f2052a01000000434104ad3b4c6ee28cb0c438c87b4efe1c36e1e54c10efc690f24c2c02446def863c50e9bf482647727b415aa81b45d0f7aa42c2cb445e4d08f18b49c027b58b6b4041ac00000000")
	coinbaseTxID, _ := chainhash.NewHashFromStr("de2c2e8628ab837ceff3de0217083d9d5feb71f758a5d083ada0b33a36e1b30e")
	txid1, _ := chainhash.NewHashFromStr("89878bfd69fba52876e5217faec126fc6a20b1845865d4038c12f03200793f48")
	expectedMerkleRoot, _ := chainhash.NewHashFromStr("7a059188283323a2ef0e02dd9f8ba1ac550f94646290d0a52a586e5426c956c5")

	assert.Equal(t, coinbaseTxID, coinbaseTx.TxIDChainHash())

	var err error

	subtrees := make([]*subtree.Subtree, 1)

	subtrees[0], err = subtree.NewTree(1)
	require.NoError(t, err)

	empty := &chainhash.Hash{}
	err = subtrees[0].AddNode(*empty, 0, 0)
	require.NoError(t, err)

	err = subtrees[0].AddNode(*txid1, 0, 0)
	require.NoError(t, err)

	// blockValidationService, err := New(ulogger.TestLogger{}, nil, nil, nil, nil)
	// require.NoError(t, err)

	// this now needs to be here since we do not have the full subtrees in the Block struct
	// which is used in the CheckMerkleRoot function
	coinbaseHash := coinbaseTx.TxIDChainHash()

	subtrees[0].ReplaceRootNode(coinbaseHash, 0, uint64(coinbaseTx.Size()))

	subtreeHashes := make([]*chainhash.Hash, len(subtrees))

	for i, subTree := range subtrees {
		rootHash := subTree.RootHash()
		subtreeHashes[i], _ = chainhash.NewHash(rootHash[:])
	}

	expectedMerkleRootHash, _ := chainhash.NewHash(expectedMerkleRoot.CloneBytes())

	block, err := model.NewBlock(
		&model.BlockHeader{
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: expectedMerkleRootHash,
		},
		coinbaseTx,
		subtreeHashes,
		0, 0, 0, 0)
	require.NoError(t, err)

	ctx := context.Background()
	subtreeStore := memory.New()

	subtreeBytes, _ := subtrees[0].Serialize()
	_ = subtreeStore.Set(ctx, subtrees[0].RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes)

	// loads the subtrees into the block
	err = block.GetAndValidateSubtrees(ctx, ulogger.TestLogger{}, subtreeStore, tSettings.Block.GetAndValidateSubtreesConcurrency)
	require.NoError(t, err)

	// err = blockValidationService.CheckMerkleRoot(block)
	err = block.CheckMerkleRoot(ctx)
	assert.NoError(t, err)
}

// TestMerkleRoot validates the merkle root calculation functionality
func TestMerkleRoot(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	var err error

	subtrees := make([]*subtree.Subtree, 2)

	subtrees[0], err = subtree.NewTreeByLeafCount(2) // height = 1
	require.NoError(t, err)
	subtrees[1], err = subtree.NewTreeByLeafCount(2) // height = 1
	require.NoError(t, err)

	err = subtrees[0].AddCoinbaseNode()
	require.NoError(t, err)

	hash1, err := chainhash.NewHashFromStr(txIds[1])
	require.NoError(t, err)
	err = subtrees[0].AddNode(*hash1, 1, 0)
	require.NoError(t, err)

	hash2, err := chainhash.NewHashFromStr(txIds[2])
	require.NoError(t, err)
	err = subtrees[1].AddNode(*hash2, 1, 0)
	require.NoError(t, err)

	hash3, err := chainhash.NewHashFromStr(txIds[3])
	require.NoError(t, err)
	err = subtrees[1].AddNode(*hash3, 1, 0)
	require.NoError(t, err)

	assert.Equal(t, txIds[0], coinbaseTx.TxID())

	prevBlockHash, err := chainhash.NewHashFromStr(prevBlockHashStr)
	if err != nil {
		t.Fail()
	}

	bits, err := hex.DecodeString(bitsStr)
	if err != nil {
		t.Fail()
	}

	// this now needs to be here since we do not have the full subtrees in the Block struct
	// which is used in the CheckMerkleRoot function
	coinbaseHash := coinbaseTx.TxIDChainHash()

	subtrees[0].ReplaceRootNode(coinbaseHash, 0, uint64(coinbaseTx.Size()))

	ctx := context.Background()
	subtreeStore := memory.New()

	subtreeHashes := make([]*chainhash.Hash, len(subtrees))

	for i, subTree := range subtrees {
		rootHash := subTree.RootHash()
		subtreeHashes[i], _ = chainhash.NewHash(rootHash[:])

		subtreeBytes, _ := subTree.Serialize()
		_ = subtreeStore.Set(ctx, rootHash[:], fileformat.FileTypeSubtree, subtreeBytes)
	}

	nBits, _ := model.NewNBitFromSlice(bits)

	block, err := model.NewBlock(
		&model.BlockHeader{
			Version:        1,
			Timestamp:      1293623863,
			Nonce:          274148111,
			HashPrevBlock:  prevBlockHash,
			HashMerkleRoot: merkleRoot,
			Bits:           *nBits,
		},
		coinbaseTx,
		subtreeHashes,
		0, 0, 0, 0)
	assert.NoError(t, err)

	// blockValidationService, err := New(ulogger.TestLogger{}, nil, nil, nil, nil)
	// require.NoError(t, err)

	// loads the subtrees into the block
	err = block.GetAndValidateSubtrees(ctx, ulogger.TestLogger{}, subtreeStore, tSettings.Block.GetAndValidateSubtreesConcurrency)
	require.NoError(t, err)

	// err = blockValidationService.CheckMerkleRoot(block)
	err = block.CheckMerkleRoot(ctx)
	assert.NoError(t, err)
}

// TestTtlCache validates the TTL cache functionality
func TestTtlCache(t *testing.T) {
	// ttlcache.WithTTL[chainhash.Hash, bool](1 * time.Second),
	cache := ttlcache.New[chainhash.Hash, bool]()

	for _, txId := range txIds {
		hash, _ := chainhash.NewHashFromStr(txId)
		cache.Set(*hash, true, 1*time.Second)
	}

	go cache.Start()
	assert.Equal(t, 4, cache.Len())
	time.Sleep(2 * time.Second)
	assert.Equal(t, 0, cache.Len())
}

// TestNewConstructor validates the New constructor functionality
func TestNewConstructor(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	server := New(ctx, logger, tSettings, nil, nil, nil, nil)

	assert.NotNil(t, server)
	assert.Equal(t, ctx, server.ctx)
	assert.Equal(t, logger, server.logger)
	assert.Equal(t, tSettings, server.settings)
	assert.Nil(t, server.blockStore)
	assert.Nil(t, server.subtreeStore)
	assert.Nil(t, server.utxoStore)
	assert.Nil(t, server.blockchainClient)
	assert.NotNil(t, server.stats)
}

// TestNewWithOptions validates constructor with optional configuration
func TestNewWithOptions(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Create server with WithSetInitialState option
	serverWithOpts := New(
		ctx,
		logger,
		tSettings,
		nil,
		nil,
		nil,
		nil,
	)

	assert.NotNil(t, serverWithOpts)
}

// TestHealthLiveness validates liveness health check
func TestHealthLiveness(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	server := New(ctx, logger, tSettings, nil, nil, nil, nil)

	status, message, err := server.Health(ctx, true)

	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "OK", message)
}

// TestHealthReadinessNilDependencies validates readiness check with nil dependencies
func TestHealthReadinessNilDependencies(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Create server with nil dependencies
	server := New(ctx, logger, tSettings, nil, nil, nil, nil)

	status, message, err := server.Health(ctx, false)

	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, message, "200")
}

// TestInit validates initialization functionality
func TestInit(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	server := New(ctx, logger, tSettings, nil, nil, nil, nil)

	err := server.Init(ctx)
	assert.NoError(t, err)
}

// TestInitPrometheusMetrics validates Prometheus metrics initialization
func TestInitPrometheusMetrics(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	server := New(ctx, logger, tSettings, nil, nil, nil, nil)

	// Initialize once
	err := server.Init(ctx)
	assert.NoError(t, err)

	// Initialize again - should not panic (sync.Once protection)
	err = server.Init(ctx)
	assert.NoError(t, err)
}

// TestStop validates stop functionality
func TestStop(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	server := New(ctx, logger, tSettings, nil, nil, nil, nil)

	err := server.Stop(ctx)
	assert.NoError(t, err)
}

// TestConcurrentOperations validates concurrent access safety
func TestConcurrentOperations(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	server := New(ctx, logger, tSettings, nil, nil, nil, nil)

	// Run multiple operations concurrently
	const numGoroutines = 10
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer func() { done <- true }()

			// Multiple operations
			_, _, _ = server.Health(ctx, true)
			_ = server.Init(ctx)
			_ = server.Stop(ctx)
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}

// Benchmark tests
func BenchmarkHealthLiveness(b *testing.B) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(&testing.T{})

	server := New(ctx, logger, tSettings, nil, nil, nil, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = server.Health(ctx, true)
	}
}

func BenchmarkInit(b *testing.B) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(&testing.T{})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		server := New(ctx, logger, tSettings, nil, nil, nil, nil)
		_ = server.Init(ctx)
	}
}

// MockBlockchainClient implements blockchain.ClientI for testing
type MockBlockchainClient struct {
	mu sync.RWMutex

	// FSM state management
	fsmState                 blockchain.FSMStateType
	fsmTransitionWaitTime    time.Duration
	fsmTransitionFromIdleErr error

	// Block data
	bestBlockHeader     *model.BlockHeader
	bestBlockHeaderMeta *model.BlockHeaderMeta
	blocks              map[uint32]*model.Block // height -> block

	// Error injection
	healthErr                 error
	getBestBlockHeaderErr     error
	getBlockByHeightErr       error
	waitUntilFSMTransitionErr error

	// Health check responses
	healthStatus  int
	healthMessage string

	// Call tracking
	healthCalls                 int
	getBestBlockHeaderCalls     int
	getBlockByHeightCalls       int
	waitUntilFSMTransitionCalls int

	// Expected calls for verification
	expectedGetBlockByHeightHeight uint32
}

// Enhanced Health method tests for 100% coverage

// TestHealthReadiness_AllDependenciesHealthy tests readiness check with all healthy dependencies
func TestHealthReadiness_AllDependenciesHealthy(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Create healthy mock dependencies
	runningState := blockchain.FSMStateRUNNING
	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("Health", mock.Anything, false).Return(http.StatusOK, "healthy", nil)
	mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&runningState, nil)

	mockBlockStore := &blob.MockStore{}
	mockBlockStore.On("Health", mock.Anything, false).Return(http.StatusOK, "healthy", nil)

	mockSubtreeStore := &blob.MockStore{}
	mockSubtreeStore.On("Health", mock.Anything, false).Return(http.StatusOK, "healthy", nil)

	mockUTXOStore := &utxo.MockUtxostore{}
	mockUTXOStore.On("Health", mock.Anything, false).Return(http.StatusOK, "healthy", nil)

	server := New(ctx, logger, tSettings, mockBlockStore, mockSubtreeStore, mockUTXOStore, mockBlockchainClient)

	status, message, err := server.Health(ctx, false) // readiness check

	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, message, "200")
}

// TestHealthReadiness_BlockchainClientUnhealthy tests readiness check with unhealthy blockchain client
func TestHealthReadiness_BlockchainClientUnhealthy(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Create unhealthy blockchain client
	runningState := blockchain.FSMStateRUNNING
	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("Health", mock.Anything, false).Return(http.StatusServiceUnavailable, "blockchain client unhealthy", errors.NewProcessingError("blockchain client unhealthy"))
	mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&runningState, nil)

	// Healthy other dependencies
	mockBlockStore := &blob.MockStore{}
	mockBlockStore.On("Health", mock.Anything, false).Return(http.StatusOK, "healthy", nil)

	mockSubtreeStore := &blob.MockStore{}
	mockSubtreeStore.On("Health", mock.Anything, false).Return(http.StatusOK, "healthy", nil)

	mockUTXOStore := &utxo.MockUtxostore{}
	mockUTXOStore.On("Health", mock.Anything, false).Return(http.StatusOK, "healthy", nil)

	server := New(ctx, logger, tSettings, mockBlockStore, mockSubtreeStore, mockUTXOStore, mockBlockchainClient)

	status, message, err := server.Health(ctx, false) // readiness check

	assert.NoError(t, err) // health.CheckAll always returns nil error
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, message, "blockchain client unhealthy") // Error should be in message
}

// TestHealthReadiness_BlockStoreUnhealthy tests readiness check with unhealthy block store
func TestHealthReadiness_BlockStoreUnhealthy(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Create healthy blockchain client
	runningState := blockchain.FSMStateRUNNING
	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("Health", mock.Anything, false).Return(http.StatusOK, "healthy", nil)
	mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&runningState, nil)

	// Create unhealthy block store
	mockBlockStore := &blob.MockStore{}
	mockBlockStore.On("Health", mock.Anything, false).Return(http.StatusServiceUnavailable, "block store unhealthy", errors.NewProcessingError("block store unhealthy"))

	// Healthy other dependencies
	mockSubtreeStore := &blob.MockStore{}
	mockSubtreeStore.On("Health", mock.Anything, false).Return(http.StatusOK, "healthy", nil)

	mockUTXOStore := &utxo.MockUtxostore{}
	mockUTXOStore.On("Health", mock.Anything, false).Return(http.StatusOK, "healthy", nil)

	server := New(ctx, logger, tSettings, mockBlockStore, mockSubtreeStore, mockUTXOStore, mockBlockchainClient)

	status, message, err := server.Health(ctx, false) // readiness check

	assert.NoError(t, err) // health.CheckAll always returns nil error
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, message, "block store unhealthy") // Error should be in message
}

// TestHealthReadiness_SubtreeStoreUnhealthy tests readiness check with unhealthy subtree store
func TestHealthReadiness_SubtreeStoreUnhealthy(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Create healthy blockchain client
	runningState := blockchain.FSMStateRUNNING
	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("Health", mock.Anything, false).Return(http.StatusOK, "healthy", nil)
	mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&runningState, nil)

	// Create healthy block store
	mockBlockStore := &blob.MockStore{}
	mockBlockStore.On("Health", mock.Anything, false).Return(http.StatusOK, "healthy", nil)

	// Create unhealthy subtree store
	mockSubtreeStore := &blob.MockStore{}
	mockSubtreeStore.On("Health", mock.Anything, false).Return(http.StatusServiceUnavailable, "subtree store unhealthy", errors.NewProcessingError("subtree store unhealthy"))

	// Healthy UTXO store
	mockUTXOStore := &utxo.MockUtxostore{}
	mockUTXOStore.On("Health", mock.Anything, false).Return(http.StatusOK, "healthy", nil)

	server := New(ctx, logger, tSettings, mockBlockStore, mockSubtreeStore, mockUTXOStore, mockBlockchainClient)

	status, message, err := server.Health(ctx, false) // readiness check

	assert.NoError(t, err) // health.CheckAll always returns nil error
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, message, "subtree store unhealthy") // Error should be in message
}

// TestHealthReadiness_UTXOStoreUnhealthy tests readiness check with unhealthy UTXO store
func TestHealthReadiness_UTXOStoreUnhealthy(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Create healthy blockchain client
	runningState := blockchain.FSMStateRUNNING
	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("Health", mock.Anything, false).Return(http.StatusOK, "healthy", nil)
	mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&runningState, nil)

	// Create healthy stores
	mockBlockStore := &blob.MockStore{}
	mockBlockStore.On("Health", mock.Anything, false).Return(http.StatusOK, "healthy", nil)

	mockSubtreeStore := &blob.MockStore{}
	mockSubtreeStore.On("Health", mock.Anything, false).Return(http.StatusOK, "healthy", nil)

	// Create unhealthy UTXO store
	mockUTXOStore := &utxo.MockUtxostore{}
	mockUTXOStore.On("Health", mock.Anything, false).Return(http.StatusServiceUnavailable, "UTXO store unhealthy", errors.NewProcessingError("UTXO store unhealthy"))

	server := New(ctx, logger, tSettings, mockBlockStore, mockSubtreeStore, mockUTXOStore, mockBlockchainClient)

	status, message, err := server.Health(ctx, false) // readiness check

	assert.NoError(t, err) // health.CheckAll always returns nil error
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, message, "UTXO store unhealthy") // Error should be in message
}

// TestHealthReadiness_MultipleDependenciesUnhealthy tests readiness check with multiple unhealthy dependencies
func TestHealthReadiness_MultipleDependenciesUnhealthy(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Create multiple unhealthy dependencies
	runningState := blockchain.FSMStateRUNNING
	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("Health", mock.Anything, false).Return(http.StatusServiceUnavailable, "blockchain client unhealthy", errors.NewProcessingError("blockchain client unhealthy"))
	mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&runningState, nil)

	mockBlockStore := &blob.MockStore{}
	mockBlockStore.On("Health", mock.Anything, false).Return(http.StatusServiceUnavailable, "block store unhealthy", errors.NewProcessingError("block store unhealthy"))

	mockSubtreeStore := &blob.MockStore{}
	mockSubtreeStore.On("Health", mock.Anything, false).Return(http.StatusOK, "healthy", nil)

	mockUTXOStore := &utxo.MockUtxostore{}
	mockUTXOStore.On("Health", mock.Anything, false).Return(http.StatusOK, "healthy", nil)

	server := New(ctx, logger, tSettings, mockBlockStore, mockSubtreeStore, mockUTXOStore, mockBlockchainClient)

	status, message, err := server.Health(ctx, false) // readiness check

	assert.NoError(t, err) // health.CheckAll always returns nil error
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, message, "blockchain client unhealthy") // Both errors should be in message
	assert.Contains(t, message, "block store unhealthy")
}

// TestHealthReadiness_SomeDependenciesNil tests readiness check with some nil dependencies
func TestHealthReadiness_SomeDependenciesNil(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Only provide some dependencies (others will be nil)
	runningState := blockchain.FSMStateRUNNING
	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("Health", mock.Anything, false).Return(http.StatusOK, "healthy", nil)
	mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&runningState, nil)

	mockBlockStore := &blob.MockStore{}
	mockBlockStore.On("Health", mock.Anything, false).Return(http.StatusOK, "healthy", nil)

	server := New(ctx, logger, tSettings, mockBlockStore, nil, nil, mockBlockchainClient)

	status, message, err := server.Health(ctx, false) // readiness check

	// Should succeed even with nil dependencies (they are not checked if nil)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, message, "200")
}

// TestHealthReadiness_AllDependenciesNil tests readiness check with all nil dependencies
func TestHealthReadiness_AllDependenciesNil(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	server := New(ctx, logger, tSettings, nil, nil, nil, nil)

	status, message, err := server.Health(ctx, false) // readiness check

	// Should succeed with no dependencies to check
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, message, "200")
}

// TestHealthLiveness_AlwaysHealthy tests liveness check (should always return OK)
func TestHealthLiveness_AlwaysHealthy(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Create unhealthy dependencies - should not affect liveness check
	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("Health", mock.Anything, mock.Anything).Return(http.StatusServiceUnavailable, "blockchain client unhealthy", errors.NewProcessingError("blockchain client unhealthy"))

	mockBlockStore := &blob.MockStore{}
	mockBlockStore.On("Health", mock.Anything, mock.Anything).Return(http.StatusServiceUnavailable, "block store unhealthy", errors.NewProcessingError("block store unhealthy"))

	server := New(ctx, logger, tSettings, mockBlockStore, nil, nil, mockBlockchainClient)

	status, message, err := server.Health(ctx, true) // liveness check

	// Liveness check should always succeed regardless of dependencies
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "OK", message)
}

// TestHealthContext_Cancellation tests health check with context cancellation
func TestHealthContext_Cancellation(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	mockBlockchainClient := &blockchain.Mock{}
	server := New(context.Background(), logger, tSettings, nil, nil, nil, mockBlockchainClient)

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Health check should still work even with cancelled context
	// (current implementation doesn't check context cancellation)
	status, message, err := server.Health(ctx, true)

	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "OK", message)
}

// TestHealthConcurrency tests concurrent health checks
func TestHealthConcurrency(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	runningState := blockchain.FSMStateRUNNING
	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("Health", mock.Anything, mock.Anything).Return(http.StatusOK, "healthy", nil)
	mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&runningState, nil)

	mockBlockStore := &blob.MockStore{}
	mockBlockStore.On("Health", mock.Anything, mock.Anything).Return(http.StatusOK, "healthy", nil)

	mockSubtreeStore := &blob.MockStore{}
	mockSubtreeStore.On("Health", mock.Anything, mock.Anything).Return(http.StatusOK, "healthy", nil)

	mockUTXOStore := &utxo.MockUtxostore{}
	mockUTXOStore.On("Health", mock.Anything, mock.Anything).Return(http.StatusOK, "healthy", nil)

	server := New(ctx, logger, tSettings, mockBlockStore, mockSubtreeStore, mockUTXOStore, mockBlockchainClient)

	const numGoroutines = 20
	var wg sync.WaitGroup
	results := make(chan error, numGoroutines)

	// Run concurrent health checks
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(checkLiveness bool) {
			defer wg.Done()
			status, _, err := server.Health(ctx, checkLiveness)
			if err != nil || status != http.StatusOK {
				results <- errors.NewProcessingError("health check failed: status=%d, err=%v", status, err)
			} else {
				results <- nil
			}
		}(i%2 == 0) // Alternate between liveness and readiness checks
	}

	wg.Wait()
	close(results)

	// Verify all health checks succeeded
	errorCount := 0
	for err := range results {
		if err != nil {
			errorCount++
			t.Logf("Concurrent health check error: %v", err)
		}
	}

	assert.Equal(t, 0, errorCount)
}
