package blockpersister

import (
	"context"
	"encoding/hex"
	"io"
	"net/http"
	"net/url"
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
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/blob/storetypes"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
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
	getBlocksNotPersistedCalls  int

	// Expected calls for verification
	expectedGetBlockByHeightHeight uint32

	// Blocks to return from GetBlocksNotPersisted
	blocksNotPersisted []*model.Block
}

func NewMockBlockchainClient() *MockBlockchainClient {
	// Create default best block header
	hash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

	return &MockBlockchainClient{
		fsmState: blockchain.FSMStateIDLE,
		bestBlockHeader: &model.BlockHeader{
			HashMerkleRoot: hash,
		},
		bestBlockHeaderMeta: &model.BlockHeaderMeta{
			Height: 100,
		},
		blocks:        make(map[uint32]*model.Block),
		healthStatus:  http.StatusOK,
		healthMessage: "OK",
	}
}

// SetFSMState sets the current FSM state
func (m *MockBlockchainClient) SetFSMState(state blockchain.FSMStateType) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fsmState = state
}

// SetBestBlockHeight sets the height of the best block
func (m *MockBlockchainClient) SetBestBlockHeight(height uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bestBlockHeaderMeta.Height = height
}

// SetBlock adds a block at the specified height
func (m *MockBlockchainClient) SetBlock(height uint32, block *model.Block) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blocks[height] = block
}

// SetHealthError sets an error to return from Health calls
func (m *MockBlockchainClient) SetHealthError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.healthErr = err
}

// SetGetBestBlockHeaderError sets an error to return from GetBestBlockHeader calls
func (m *MockBlockchainClient) SetGetBestBlockHeaderError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getBestBlockHeaderErr = err
}

// SetGetBlockByHeightError sets an error to return from GetBlockByHeight calls
func (m *MockBlockchainClient) SetGetBlockByHeightError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getBlockByHeightErr = err
}

// SetFSMTransitionFromIdleError sets an error to return from WaitUntilFSMTransitionFromIdleState
func (m *MockBlockchainClient) SetFSMTransitionFromIdleError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fsmTransitionFromIdleErr = err
}

// SetFSMTransitionWaitTime sets how long FSM transition should wait before succeeding
func (m *MockBlockchainClient) SetFSMTransitionWaitTime(duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fsmTransitionWaitTime = duration
}

// GetCallCounts returns the number of times methods were called
func (m *MockBlockchainClient) GetCallCounts() (health, bestHeader, blockByHeight, fsmTransition, getBlocksNotPersisted int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.healthCalls, m.getBestBlockHeaderCalls, m.getBlockByHeightCalls, m.waitUntilFSMTransitionCalls, m.getBlocksNotPersistedCalls
}

// Health implements blockchain.ClientI
func (m *MockBlockchainClient) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.healthCalls++

	if m.healthErr != nil {
		return http.StatusServiceUnavailable, "error", m.healthErr
	}

	return m.healthStatus, m.healthMessage, nil
}

// GetBestBlockHeader implements blockchain.ClientI
func (m *MockBlockchainClient) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getBestBlockHeaderCalls++

	if m.getBestBlockHeaderErr != nil {
		return nil, nil, m.getBestBlockHeaderErr
	}

	return m.bestBlockHeader, m.bestBlockHeaderMeta, nil
}

// GetBlockByHeight implements blockchain.ClientI
func (m *MockBlockchainClient) GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getBlockByHeightCalls++
	m.expectedGetBlockByHeightHeight = height

	if m.getBlockByHeightErr != nil {
		return nil, m.getBlockByHeightErr
	}

	if block, exists := m.blocks[height]; exists {
		return block, nil
	}

	return nil, errors.NewError("block not found at height %d", height)
}

// WaitUntilFSMTransitionFromIdleState implements blockchain.ClientI
func (m *MockBlockchainClient) WaitUntilFSMTransitionFromIdleState(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.waitUntilFSMTransitionCalls++

	if m.fsmTransitionFromIdleErr != nil {
		return m.fsmTransitionFromIdleErr
	}

	// Simulate the wait time
	if m.fsmTransitionWaitTime > 0 {
		select {
		case <-time.After(m.fsmTransitionWaitTime):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Transition from IDLE to RUNNING after wait completes
	if m.fsmState == blockchain.FSMStateIDLE {
		m.fsmState = blockchain.FSMStateRUNNING
	}

	return nil
}

// Mock implementations for all other required methods (minimal implementations)
func (m *MockBlockchainClient) AddBlock(ctx context.Context, block *model.Block, peerID string, opts ...options.StoreBlockOption) error {
	return nil
}
func (m *MockBlockchainClient) GetNextBlockID(ctx context.Context) (uint64, error) { return 0, nil }
func (m *MockBlockchainClient) SendNotification(ctx context.Context, notification *blockchain_api.Notification) error {
	return nil
}
func (m *MockBlockchainClient) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Return a basic block with ID matching the hash
	// This is a simple implementation for testing purposes
	return &model.Block{
		ID:     100, // Default ID
		Height: 100,
		Header: &model.BlockHeader{
			HashPrevBlock: blockHash,
		},
	}, nil
}
func (m *MockBlockchainClient) GetBlocks(ctx context.Context, blockHash *chainhash.Hash, numberOfBlocks uint32) ([]*model.Block, error) {
	return nil, nil
}
func (m *MockBlockchainClient) GetBlockByID(ctx context.Context, id uint64) (*model.Block, error) {
	return nil, nil
}
func (m *MockBlockchainClient) GetBlockStats(ctx context.Context) (*model.BlockStats, error) {
	return nil, nil
}
func (m *MockBlockchainClient) GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error) {
	return nil, nil
}
func (m *MockBlockchainClient) GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error) {
	return nil, nil
}
func (m *MockBlockchainClient) GetLastNInvalidBlocks(ctx context.Context, n int64) ([]*model.BlockInfo, error) {
	return nil, nil
}
func (m *MockBlockchainClient) GetSuitableBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.SuitableBlock, error) {
	return nil, nil
}
func (m *MockBlockchainClient) GetHashOfAncestorBlock(ctx context.Context, hash *chainhash.Hash, depth int) (*chainhash.Hash, error) {
	return nil, nil
}
func (m *MockBlockchainClient) GetNextWorkRequired(ctx context.Context, hash *chainhash.Hash, currentBlockTime int64) (*model.NBit, error) {
	return nil, nil
}
func (m *MockBlockchainClient) GetBlockExists(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
	return false, nil
}
func (m *MockBlockchainClient) GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	return nil, nil, nil
}
func (m *MockBlockchainClient) GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return nil, nil, nil
}
func (m *MockBlockchainClient) GetBlockHeadersToCommonAncestor(ctx context.Context, hashTarget *chainhash.Hash, blockLocatorHashes []*chainhash.Hash, maxHeaders uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return nil, nil, nil
}
func (m *MockBlockchainClient) GetBlockHeadersFromCommonAncestor(ctx context.Context, hashTarget *chainhash.Hash, blockLocatorHashes []chainhash.Hash, maxHeaders uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return nil, nil, nil
}
func (m *MockBlockchainClient) GetLatestBlockHeaderFromBlockLocator(ctx context.Context, bestBlockHash *chainhash.Hash, blockLocator []chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	return nil, nil, nil
}
func (m *MockBlockchainClient) GetBlockHeadersFromOldest(ctx context.Context, chainTipHash, targetHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return nil, nil, nil
}
func (m *MockBlockchainClient) GetBlockHeadersFromTill(ctx context.Context, blockHashFrom *chainhash.Hash, blockHashTill *chainhash.Hash) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return nil, nil, nil
}
func (m *MockBlockchainClient) GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return nil, nil, nil
}
func (m *MockBlockchainClient) GetBlockHeadersByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return nil, nil, nil
}
func (m *MockBlockchainClient) GetBlocksByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.Block, error) {
	return nil, nil
}
func (m *MockBlockchainClient) FindBlocksContainingSubtree(ctx context.Context, subtreeHash *chainhash.Hash, maxBlocks uint32) ([]*model.Block, error) {
	return nil, nil
}
func (m *MockBlockchainClient) InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) ([]chainhash.Hash, error) {
	return nil, nil
}
func (m *MockBlockchainClient) RevalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	return nil
}
func (m *MockBlockchainClient) GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error) {
	return nil, nil
}
func (m *MockBlockchainClient) Subscribe(ctx context.Context, source string) (chan *blockchain_api.Notification, error) {
	return nil, nil
}
func (m *MockBlockchainClient) GetSubscribers(ctx context.Context) ([]string, error) {
	return nil, nil
}
func (m *MockBlockchainClient) GetState(ctx context.Context, key string) ([]byte, error) {
	return nil, nil
}
func (m *MockBlockchainClient) SetState(ctx context.Context, key string, data []byte) error {
	return nil
}
func (m *MockBlockchainClient) SetBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	return nil
}
func (m *MockBlockchainClient) ClearBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	return nil
}
func (m *MockBlockchainClient) GetBlockIsMined(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
	return false, nil
}
func (m *MockBlockchainClient) GetBlocksMinedNotSet(ctx context.Context) ([]*model.Block, error) {
	return nil, nil
}
func (m *MockBlockchainClient) SetBlockSubtreesSet(ctx context.Context, blockHash *chainhash.Hash) error {
	return nil
}
func (m *MockBlockchainClient) SetBlockProcessedAt(ctx context.Context, blockHash *chainhash.Hash, clear ...bool) error {
	return nil
}
func (m *MockBlockchainClient) GetBlocksSubtreesNotSet(ctx context.Context) ([]*model.Block, error) {
	return nil, nil
}
func (m *MockBlockchainClient) GetBestHeightAndTime(ctx context.Context) (uint32, uint32, error) {
	return 0, 0, nil
}
func (m *MockBlockchainClient) CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error) {
	// Default to true - blocks are on the current chain unless specifically testing reorg scenarios
	return true, nil
}
func (m *MockBlockchainClient) CheckBlockIsAncestorOfBlock(ctx context.Context, blockIDs []uint32, blockHash *chainhash.Hash) (bool, error) {
	// Default to false - blocks are not ancestors unless specifically testing reorg scenarios
	return false, nil
}
func (m *MockBlockchainClient) GetChainTips(ctx context.Context) ([]*model.ChainTip, error) {
	return nil, nil
}
func (m *MockBlockchainClient) GetFSMCurrentState(ctx context.Context) (*blockchain.FSMStateType, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return &m.fsmState, nil
}
func (m *MockBlockchainClient) IsFSMCurrentState(ctx context.Context, state blockchain.FSMStateType) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.fsmState == state, nil
}
func (m *MockBlockchainClient) WaitForFSMtoTransitionToGivenState(context.Context, blockchain.FSMStateType) error {
	return nil
}
func (m *MockBlockchainClient) GetFSMCurrentStateForE2ETestMode() blockchain.FSMStateType {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.fsmState
}
func (m *MockBlockchainClient) IsFullyReady(ctx context.Context) (bool, error) { return true, nil }
func (m *MockBlockchainClient) Run(ctx context.Context, source string) error   { return nil }
func (m *MockBlockchainClient) CatchUpBlocks(ctx context.Context) error        { return nil }
func (m *MockBlockchainClient) LegacySync(ctx context.Context) error           { return nil }
func (m *MockBlockchainClient) Idle(ctx context.Context) error                 { return nil }
func (m *MockBlockchainClient) SendFSMEvent(ctx context.Context, event blockchain.FSMEventType) error {
	return nil
}
func (m *MockBlockchainClient) GetBlockLocator(ctx context.Context, blockHeaderHash *chainhash.Hash, blockHeaderHeight uint32) ([]*chainhash.Hash, error) {
	return nil, nil
}
func (m *MockBlockchainClient) LocateBlockHeaders(ctx context.Context, locator []*chainhash.Hash, hashStop *chainhash.Hash, maxHashes uint32) ([]*model.BlockHeader, error) {
	return nil, nil
}
func (m *MockBlockchainClient) ReportPeerFailure(ctx context.Context, hash *chainhash.Hash, peerID string, failureType string, reason string) error {
	return nil
}
func (m *MockBlockchainClient) GetBlocksNotPersisted(ctx context.Context, limit int) ([]*model.Block, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getBlocksNotPersistedCalls++

	if len(m.blocksNotPersisted) == 0 {
		return []*model.Block{}, nil
	}

	// Return up to 'limit' blocks
	if limit > len(m.blocksNotPersisted) {
		limit = len(m.blocksNotPersisted)
	}

	result := make([]*model.Block, limit)
	copy(result, m.blocksNotPersisted[:limit])
	return result, nil
}
func (m *MockBlockchainClient) SetBlockPersistedAt(ctx context.Context, blockHash *chainhash.Hash) error {
	return nil
}

// Blob deletion methods (no-op implementations for interface compliance)
func (m *MockBlockchainClient) ScheduleBlobDeletion(ctx context.Context, blobKey []byte, fileType string, storeType storetypes.BlobStoreType, deleteAtHeight uint32) (int64, bool, error) {
	return 0, false, nil
}

func (m *MockBlockchainClient) CancelBlobDeletion(ctx context.Context, blobKey []byte, fileType string, storeType storetypes.BlobStoreType) (bool, error) {
	return false, nil
}

func (m *MockBlockchainClient) ListScheduledDeletions(ctx context.Context, minHeight, maxHeight uint32, storeType storetypes.BlobStoreType, filterByStore bool, limit, offset int) ([]*blockchain_api.ScheduledDeletion, int, error) {
	return nil, 0, nil
}

func (m *MockBlockchainClient) GetPendingBlobDeletions(ctx context.Context, height uint32, limit int) ([]*blockchain_api.ScheduledDeletion, error) {
	return nil, nil
}

func (m *MockBlockchainClient) RemoveBlobDeletion(ctx context.Context, deletionID int64) error {
	return nil
}

func (m *MockBlockchainClient) IncrementBlobDeletionRetry(ctx context.Context, deletionID int64, maxRetries int) (bool, int, error) {
	return false, 0, nil
}

func (m *MockBlockchainClient) CompleteBlobDeletions(ctx context.Context, completedIDs []int64, failedIDs []int64, maxRetries int) (int, int, error) {
	return 0, 0, nil
}

func (m *MockBlockchainClient) AcquireBlobDeletionBatch(ctx context.Context, height uint32, limit int, lockTimeoutSeconds int) (string, []*blockchain_api.ScheduledDeletion, error) {
	return "", nil, nil
}

func (m *MockBlockchainClient) CompleteBlobDeletionBatch(ctx context.Context, batchToken string, completedIDs []int64, failedIDs []int64, maxRetries int) error {
	return nil
}

// MockStore implements basic store interfaces for testing
type MockBlobStore struct {
	data      map[string][]byte
	healthErr error
}

func NewMockBlobStore() *MockBlobStore {
	return &MockBlobStore{
		data: make(map[string][]byte),
	}
}

func (m *MockBlobStore) SetHealthError(err error) {
	m.healthErr = err
}

func (m *MockBlobStore) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if m.healthErr != nil {
		return http.StatusServiceUnavailable, "error", m.healthErr
	}
	return http.StatusOK, "OK", nil
}

func (m *MockBlobStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, data []byte, opts ...bloboptions.FileOption) error {
	m.data[string(key)] = data
	return nil
}

func (m *MockBlobStore) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...bloboptions.FileOption) ([]byte, error) {
	if data, exists := m.data[string(key)]; exists {
		return data, nil
	}
	return nil, errors.NewError("key not found")
}

func (m *MockBlobStore) Delete(ctx context.Context, key []byte) error { return nil }
func (m *MockBlobStore) Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...bloboptions.FileOption) error {
	return nil
}
func (m *MockBlobStore) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...bloboptions.FileOption) (bool, error) {
	return false, nil
}
func (m *MockBlobStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...bloboptions.FileOption) (io.ReadCloser, error) {
	return nil, nil
}
func (m *MockBlobStore) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...bloboptions.FileOption) error {
	return nil
}
func (m *MockBlobStore) SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, dah uint32, opts ...bloboptions.FileOption) error {
	return nil
}

func (m *MockBlobStore) GetPartial(ctx context.Context, key []byte, fileType fileformat.FileType, offset, length int64, opts ...bloboptions.FileOption) ([]byte, error) {
	return nil, nil
}
func (m *MockBlobStore) GetRange(ctx context.Context, key []byte, fileType fileformat.FileType, offset, length int64, opts ...bloboptions.FileOption) (io.ReadCloser, error) {
	return nil, nil
}
func (m *MockBlobStore) GetSize(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...bloboptions.FileOption) (int64, error) {
	return 0, nil
}
func (m *MockBlobStore) Close(ctx context.Context) error     { return nil }
func (m *MockBlobStore) SetCurrentBlockHeight(height uint32) {}

// MockUTXOStore implements basic UTXO store interface for testing
type MockUTXOStore struct {
	healthErr error
}

func NewMockUTXOStore() *MockUTXOStore {
	return &MockUTXOStore{}
}

func (m *MockUTXOStore) SetHealthError(err error) {
	m.healthErr = err
}

func (m *MockUTXOStore) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if m.healthErr != nil {
		return http.StatusServiceUnavailable, "error", m.healthErr
	}
	return http.StatusOK, "OK", nil
}

// Required interface methods for utxo.Store
func (m *MockUTXOStore) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	return nil, nil
}
func (m *MockUTXOStore) Get(ctx context.Context, hash *chainhash.Hash, fields ...fields.FieldName) (*meta.Data, error) {
	return nil, nil
}
func (m *MockUTXOStore) Delete(ctx context.Context, hash *chainhash.Hash) error { return nil }
func (m *MockUTXOStore) GetSpend(ctx context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	return nil, nil
}
func (m *MockUTXOStore) GetMeta(ctx context.Context, hash *chainhash.Hash, txMeta *meta.Data) error {
	return nil
}
func (m *MockUTXOStore) Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	return nil, nil
}
func (m *MockUTXOStore) Unspend(ctx context.Context, spends []*utxo.Spend, flagAsLocked ...bool) error {
	return nil
}
func (m *MockUTXOStore) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	return nil, nil
}
func (m *MockUTXOStore) GetUnminedTxIterator(bool) (utxo.UnminedTxIterator, error) { return nil, nil }
func (m *MockUTXOStore) GetPrunableUnminedTxIterator(cutoffBlockHeight uint32) (utxo.UnminedTxIterator, error) {
	return nil, nil
}
func (m *MockUTXOStore) QueryOldUnminedTransactions(ctx context.Context, cutoffBlockHeight uint32) ([]chainhash.Hash, error) {
	return nil, nil
}
func (m *MockUTXOStore) PreserveTransactions(ctx context.Context, txIDs []chainhash.Hash, preserveUntilHeight uint32) error {
	return nil
}
func (m *MockUTXOStore) ProcessExpiredPreservations(ctx context.Context, currentHeight uint32) error {
	return nil
}
func (m *MockUTXOStore) BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*utxo.UnresolvedMetaData, fields ...fields.FieldName) error {
	return nil
}
func (m *MockUTXOStore) PreviousOutputsDecorate(ctx context.Context, tx *bt.Tx) error { return nil }
func (m *MockUTXOStore) FreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	return nil
}
func (m *MockUTXOStore) UnFreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	return nil
}
func (m *MockUTXOStore) ReAssignUTXO(ctx context.Context, utxoSpend *utxo.Spend, newUtxo *utxo.Spend, tSettings *settings.Settings) error {
	return nil
}
func (m *MockUTXOStore) GetCounterConflicting(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	return nil, nil
}
func (m *MockUTXOStore) GetConflictingChildren(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	return nil, nil
}
func (m *MockUTXOStore) SetConflicting(ctx context.Context, txHashes []chainhash.Hash, setValue bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	return nil, nil, nil
}
func (m *MockUTXOStore) SetLocked(ctx context.Context, txHashes []chainhash.Hash, setValue bool) error {
	return nil
}
func (m *MockUTXOStore) MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash, onLongestChain bool) error {
	return nil
}
func (m *MockUTXOStore) SetBlockHeight(height uint32) error     { return nil }
func (m *MockUTXOStore) GetBlockHeight() uint32                 { return 0 }
func (m *MockUTXOStore) SetMedianBlockTime(height uint32) error { return nil }
func (m *MockUTXOStore) GetMedianBlockTime() uint32             { return 0 }

func (m *MockUTXOStore) GetBlockState() utxo.BlockState {
	return utxo.BlockState{
		Height:     m.GetBlockHeight(),
		MedianTime: m.GetMedianBlockTime(),
	}
}

// Comprehensive tests for Start method

// TestStart_FSMTransitionError tests error handling when FSM transition fails
func TestStart_FSMTransitionError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// StateFile no longer used

	// Create mock blockchain client with FSM transition error
	mockClient := NewMockBlockchainClient()
	mockClient.SetFSMTransitionFromIdleError(errors.NewError("FSM transition failed"))

	server := New(ctx, logger, tSettings, nil, nil, nil, mockClient)

	// Channel to signal when ready
	readyCh := make(chan struct{})

	// Start server
	err := server.Start(ctx, readyCh)

	// Should return the FSM transition error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "FSM transition failed")

	// Ready channel should be closed even on error
	select {
	case <-readyCh:
		// Expected - channel should be closed
	default:
		t.Fatal("Ready channel should be closed on error")
	}
}

// TestStart_HTTPServerSetup tests HTTP server setup when configured
func TestStart_HTTPServerSetup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// StateFile no longer used

	// Set HTTP listen address to trigger HTTP server setup
	tSettings.BlockPersister.HTTPListenAddress = "127.0.0.1:0"
	// Use a simple memory store URL for HTTP server
	memoryURL, err := url.Parse("memory://")
	require.NoError(t, err)
	tSettings.Block.BlockStore = memoryURL

	// Create mock blockchain client
	mockClient := NewMockBlockchainClient()
	mockClient.SetFSMState(blockchain.FSMStateIDLE)

	server := New(ctx, logger, tSettings, nil, nil, nil, mockClient)

	// Channel to signal when ready
	readyCh := make(chan struct{})

	// Channel to receive the start error
	startErrCh := make(chan error, 1)

	// Start server in goroutine
	go func() {
		err := server.Start(ctx, readyCh)
		startErrCh <- err
	}()

	// Wait for ready signal or timeout
	select {
	case <-readyCh:
		// Server started successfully
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Timeout waiting for server to be ready")
	}

	// Cancel context to stop server
	cancel()

	// Wait for server to complete and get the error
	var startErr error
	select {
	case startErr = <-startErrCh:
		// Server completed
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Timeout waiting for server to complete")
	}

	// Should succeed even with HTTP server
	assert.NoError(t, startErr)
}

// TestStart_HTTPServerConfigurationError tests error handling when block store is not configured
func TestStart_HTTPServerConfigurationError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// StateFile no longer used

	// Set HTTP listen address but no block store URL
	tSettings.BlockPersister.HTTPListenAddress = "127.0.0.1:0"
	tSettings.Block.BlockStore = nil // This will cause configuration error

	// Create mock blockchain client
	mockClient := NewMockBlockchainClient()

	server := New(ctx, logger, tSettings, nil, nil, nil, mockClient)

	// Channel to signal when ready
	readyCh := make(chan struct{})

	// Start server
	err := server.Start(ctx, readyCh)

	// Should return configuration error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "blockstore setting error")

	// Ready channel should be closed even on error
	select {
	case <-readyCh:
		// Expected - channel should be closed
	default:
		t.Fatal("Ready channel should be closed on error")
	}
}

// TestStart_BlockProcessingLoop tests the main block processing loop
func TestStart_BlockProcessingLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Set short sleep time for testing
	tSettings.BlockPersister.PersistSleep = 10 * time.Millisecond
	// PersistAge no longer used

	// StateFile no longer used

	// Don't set HTTP listen address
	tSettings.BlockPersister.HTTPListenAddress = ""

	// Create mock blockchain client
	mockClient := NewMockBlockchainClient()
	mockClient.SetFSMState(blockchain.FSMStateIDLE)
	mockClient.SetBestBlockHeight(20) // Set tip high enough to process blocks

	// Create a test block to be processed
	testHash, _ := chainhash.NewHashFromStr(txIds[0])
	prevHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000000")
	testBlockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  prevHash,
		HashMerkleRoot: testHash,
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           model.NBit{},
		Nonce:          0,
	}
	testBlock, err := model.NewBlock(testBlockHeader, coinbaseTx, nil, 0, 0, 1, 0)
	require.NoError(t, err)
	mockClient.SetBlock(1, testBlock)

	// Create mock stores
	mockBlockStore := NewMockBlobStore()
	mockSubtreeStore := NewMockBlobStore()
	mockUTXOStore := NewMockUTXOStore()

	server := New(ctx, logger, tSettings, mockBlockStore, mockSubtreeStore, mockUTXOStore, mockClient)

	// Set initial state to height 0 (so next block is 1)
	// Don't set any initial state - height will be 0 by default

	// Channel to signal when ready
	readyCh := make(chan struct{})

	// Start server in goroutine
	go func() {
		_ = server.Start(ctx, readyCh)
	}()

	// Wait for ready signal
	select {
	case <-readyCh:
		// Server started successfully
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for server to be ready")
	}

	// Give the processing loop time to run a few iterations
	time.Sleep(50 * time.Millisecond)

	// Cancel context to stop server
	cancel()

	// Wait for server to stop
	time.Sleep(20 * time.Millisecond)

	// Verify that blockchain client methods were called
	_, _, _, fsmTransitionCalls, getBlocksNotPersistedCalls := mockClient.GetCallCounts()
	assert.Equal(t, 1, fsmTransitionCalls)
	assert.GreaterOrEqual(t, getBlocksNotPersistedCalls, 1) // Should have been called at least once
}

// TestStart_BlockProcessingNoBlocks tests processing loop when no blocks need processing
func TestStart_BlockProcessingNoBlocks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Set short sleep time for testing
	tSettings.BlockPersister.PersistSleep = 5 * time.Millisecond
	// PersistAge no longer used

	// StateFile no longer used

	// Don't set HTTP listen address
	tSettings.BlockPersister.HTTPListenAddress = ""

	// Create mock blockchain client
	mockClient := NewMockBlockchainClient()
	mockClient.SetBestBlockHeight(20) // Set tip but not high enough to process

	server := New(ctx, logger, tSettings, nil, nil, nil, mockClient)

	// Channel to signal when ready
	readyCh := make(chan struct{})

	// Start server in goroutine
	go func() {
		_ = server.Start(ctx, readyCh)
	}()

	// Wait for ready signal
	select {
	case <-readyCh:
		// Server started successfully
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for server to be ready")
	}

	// Give the processing loop time to run a few iterations
	time.Sleep(30 * time.Millisecond)

	// Cancel context to stop server
	cancel()

	// Wait for server to stop
	time.Sleep(10 * time.Millisecond)

	// Verify that blockchain client was called
	_, _, _, _, getBlocksNotPersistedCalls := mockClient.GetCallCounts()
	assert.GreaterOrEqual(t, getBlocksNotPersistedCalls, 1) // Should have been called
}

// TestStart_ContextCancellation tests proper context cancellation handling
func TestStart_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// StateFile no longer used

	// Don't set HTTP listen address
	tSettings.BlockPersister.HTTPListenAddress = ""

	// Create mock blockchain client that takes some time to transition
	mockClient := NewMockBlockchainClient()
	mockClient.SetFSMTransitionWaitTime(50 * time.Millisecond)

	server := New(ctx, logger, tSettings, nil, nil, nil, mockClient)

	// Channel to signal when ready
	readyCh := make(chan struct{})

	// Channel to receive the start error
	startErrCh := make(chan error, 1)

	// Start server in goroutine
	go func() {
		err := server.Start(ctx, readyCh)
		startErrCh <- err
	}()

	// Cancel context immediately
	cancel()

	// Wait for operation to complete
	select {
	case <-readyCh:
		// Ready channel should be closed
	case <-time.After(200 * time.Millisecond):
		// Timeout is acceptable for cancellation test
	}

	// Wait for server to complete and get the error
	var startErr error
	select {
	case startErr = <-startErrCh:
		// Server completed
	case <-time.After(200 * time.Millisecond):
		// Timeout is acceptable for cancellation test - server might not complete due to cancellation
	}

	// Error should be context cancellation or nil
	if startErr != nil {
		assert.Contains(t, startErr.Error(), "context canceled")
	}
}

// TestStart_ConcurrentReadyChannelClose tests that ready channel is closed exactly once
func TestStart_ConcurrentReadyChannelClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// StateFile no longer used

	// Don't set HTTP listen address
	tSettings.BlockPersister.HTTPListenAddress = ""

	// Create mock blockchain client
	mockClient := NewMockBlockchainClient()

	server := New(ctx, logger, tSettings, nil, nil, nil, mockClient)

	// Channel to signal when ready
	readyCh := make(chan struct{})

	// Start server in goroutine
	go func() {
		_ = server.Start(ctx, readyCh)
	}()

	// Wait for ready signal
	select {
	case <-readyCh:
		// Server started successfully
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for server to be ready")
	}

	// Try to read from channel again - should not block (channel should be closed)
	select {
	case <-readyCh:
		// Expected - closed channel allows immediate read
	case <-time.After(10 * time.Millisecond):
		t.Fatal("Ready channel should be closed and allow immediate read")
	}

	// Cancel to clean up
	cancel()
	time.Sleep(10 * time.Millisecond)
}

// TestStart_ProcessingLoopErrorHandling tests error handling in processing loop
func TestStart_ProcessingLoopErrorHandling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Set short sleep time for testing
	tSettings.BlockPersister.PersistSleep = 5 * time.Millisecond
	// PersistAge no longer used

	// StateFile no longer used

	// Don't set HTTP listen address
	tSettings.BlockPersister.HTTPListenAddress = ""

	// Create mock blockchain client that will return errors
	mockClient := NewMockBlockchainClient()
	mockClient.SetBestBlockHeight(100) // High enough to trigger processing
	mockClient.SetGetBestBlockHeaderError(errors.NewProcessingError("blockchain error"))

	server := New(ctx, logger, tSettings, nil, nil, nil, mockClient)

	// Channel to signal when ready
	readyCh := make(chan struct{})

	// Start server in goroutine
	go func() {
		_ = server.Start(ctx, readyCh)
	}()

	// Wait for ready signal
	select {
	case <-readyCh:
		// Server started successfully
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for server to be ready")
	}

	// Give the processing loop time to encounter errors and retry
	time.Sleep(30 * time.Millisecond)

	// Cancel context to stop server
	cancel()

	// Wait for server to stop
	time.Sleep(10 * time.Millisecond)

	// Verify that blockchain client was called multiple times (retries)
	_, _, _, _, getBlocksNotPersistedCalls := mockClient.GetCallCounts()
	assert.GreaterOrEqual(t, getBlocksNotPersistedCalls, 1)

	// The processing loop should handle errors and continue running until context is cancelled
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
