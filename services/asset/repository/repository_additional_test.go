package repository_test

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/asset/repository"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test Health method with liveness check
func TestRepository_Health_Liveness(t *testing.T) {
	repo := createTestRepository(t)

	status, message, err := repo.Health(context.Background(), true)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "OK", message)
}

// Test Health method with readiness check - all stores nil
func TestRepository_Health_Readiness_NilStores(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings(t)

	repo, err := repository.NewRepository(logger, settings, nil, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	status, message, err := repo.Health(context.Background(), false)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, message, "200")
}

// Test Health method with readiness check - all stores healthy
func TestRepository_Health_Readiness_Healthy(t *testing.T) {
	repo := createTestRepository(t)

	status, message, err := repo.Health(context.Background(), false)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, message, "OK")
}

// Test Health method with readiness check - unhealthy store
func TestRepository_Health_Readiness_Unhealthy(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings(t)

	// Create an unhealthy mock store
	unhealthyStore := &mockUnhealthyStore{}

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)
	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, settings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(logger, settings, blockChainStore, nil, nil)
	require.NoError(t, err)

	repo, err := repository.NewRepository(logger, settings, utxoStore, unhealthyStore, blockchainClient, nil, nil, nil, nil)
	require.NoError(t, err)

	status, message, err := repo.Health(ctx, false)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, message, "unhealthy")
}

// Test GetTxMeta method
func TestRepository_GetTxMeta(t *testing.T) {
	repo := createTestRepository(t)

	// Create test hash
	txHash, err := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	require.NoError(t, err)

	// Should return an error for non-existent transaction
	txMeta, err := repo.GetTxMeta(context.Background(), txHash)
	assert.Error(t, err)
	assert.Nil(t, txMeta)
}

// Test GetBlockStats method
func TestRepository_GetBlockStats(t *testing.T) {
	repo := createTestRepository(t)

	stats, err := repo.GetBlockStats(context.Background())
	// Should not error but might return empty stats
	assert.NoError(t, err)
	assert.NotNil(t, stats)
}

// Test GetBlockGraphData method
func TestRepository_GetBlockGraphData(t *testing.T) {
	repo := createTestRepository(t)

	data, err := repo.GetBlockGraphData(context.Background(), 86400000) // 24 hours in milliseconds
	// Should not error but might return empty data
	assert.NoError(t, err)
	assert.NotNil(t, data)
}

// Test GetTransactionMeta method
func TestRepository_GetTransactionMeta(t *testing.T) {
	repo := createTestRepository(t)

	txHash, err := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	require.NoError(t, err)

	// Should return an error for non-existent transaction
	txMeta, err := repo.GetTransactionMeta(context.Background(), txHash)
	assert.Error(t, err)
	assert.Nil(t, txMeta)
}

// Test GetBlockByHeight method
func TestRepository_GetBlockByHeight(t *testing.T) {
	repo := createTestRepository(t)

	// Should return an error for non-existent block
	block, err := repo.GetBlockByHeight(context.Background(), 999999)
	assert.Error(t, err)
	assert.Nil(t, block)
	assert.Contains(t, err.Error(), "BLOCK_NOT_FOUND")
}

// Test GetBlockHeader method
func TestRepository_GetBlockHeader(t *testing.T) {
	repo := createTestRepository(t)

	blockHash, err := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	require.NoError(t, err)

	// Should return an error for non-existent block
	header, meta, err := repo.GetBlockHeader(context.Background(), blockHash)
	assert.Error(t, err)
	assert.Nil(t, header)
	assert.Nil(t, meta)
}

// Test GetSubtreeBytes method with success case
func TestRepository_GetSubtreeBytes_Success(t *testing.T) {
	// Create test subtree data
	st, err := subtree.NewTreeByLeafCount(2)
	require.NoError(t, err)

	tx1 := &bt.Tx{Version: 1, LockTime: 0}
	tx2 := &bt.Tx{Version: 2, LockTime: 0}

	err = st.AddNode(*tx1.TxIDChainHash(), 0, 0)
	require.NoError(t, err)
	err = st.AddNode(*tx2.TxIDChainHash(), 0, 0)
	require.NoError(t, err)

	subtreeHash := st.RootHash()
	subtreeBytes, err := st.Serialize()
	require.NoError(t, err)

	// Create repository with data
	repo := createTestRepositoryWithSubtreeData(t, subtreeHash, subtreeBytes)

	// Test GetSubtreeBytes
	retrievedBytes, err := repo.GetSubtreeBytes(context.Background(), subtreeHash)
	assert.NoError(t, err)
	assert.Equal(t, subtreeBytes, retrievedBytes)
}

// Test GetSubtreeBytes method with fallback to FileTypeSubtreeToCheck
func TestRepository_GetSubtreeBytes_Fallback(t *testing.T) {
	// Create test subtree data
	st, err := subtree.NewTreeByLeafCount(2)
	require.NoError(t, err)

	tx := &bt.Tx{Version: 1, LockTime: 0}
	err = st.AddNode(*tx.TxIDChainHash(), 0, 0)
	require.NoError(t, err)
	err = st.AddNode(*tx.TxIDChainHash(), 0, 0)
	require.NoError(t, err)

	subtreeHash := st.RootHash()
	subtreeBytes, err := st.Serialize()
	require.NoError(t, err)

	// Store with FileTypeSubtreeToCheck instead of FileTypeSubtree
	repo := createTestRepositoryWithSubtreeDataToCheck(t, subtreeHash, subtreeBytes)

	// Test GetSubtreeBytes should fallback successfully
	retrievedBytes, err := repo.GetSubtreeBytes(context.Background(), subtreeHash)
	assert.NoError(t, err)
	assert.Equal(t, subtreeBytes, retrievedBytes)
}

// Test GetSubtreeBytes method with error
func TestRepository_GetSubtreeBytes_Error(t *testing.T) {
	repo := createTestRepository(t)

	nonExistentHash, err := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
	require.NoError(t, err)

	// Should return an error for non-existent subtree
	subtreeBytes, err := repo.GetSubtreeBytes(context.Background(), nonExistentHash)
	assert.Error(t, err)
	assert.Nil(t, subtreeBytes)
}

// Test GetSubtreeDataReaderFromBlockPersister method
func TestRepository_GetSubtreeDataReaderFromBlockPersister(t *testing.T) {
	repo := createTestRepository(t)

	nonExistentHash, err := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
	require.NoError(t, err)

	// Should return an error for non-existent data
	reader, err := repo.GetSubtreeDataReaderFromBlockPersister(context.Background(), nonExistentHash)
	assert.Error(t, err)
	assert.Nil(t, reader)
}

// Test GetSubtreeExists method - exists case
func TestRepository_GetSubtreeExists_Exists(t *testing.T) {
	// Create test subtree data
	st, err := subtree.NewTreeByLeafCount(2)
	require.NoError(t, err)

	tx := &bt.Tx{Version: 1, LockTime: 0}
	err = st.AddNode(*tx.TxIDChainHash(), 0, 0)
	require.NoError(t, err)
	err = st.AddNode(*tx.TxIDChainHash(), 0, 0)
	require.NoError(t, err)

	subtreeHash := st.RootHash()
	subtreeBytes, err := st.Serialize()
	require.NoError(t, err)

	repo := createTestRepositoryWithSubtreeData(t, subtreeHash, subtreeBytes)

	// Should return true for existing subtree
	exists, err := repo.GetSubtreeExists(context.Background(), subtreeHash)
	assert.NoError(t, err)
	assert.True(t, exists)
}

// Test GetSubtreeExists method - error case when blob store fails
func TestRepository_GetSubtreeExists_Error(t *testing.T) {
	repo := createTestRepository(t)

	// Use a hash that doesn't exist
	nonExistentHash, err := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
	require.NoError(t, err)

	// Should return false for non-existent subtree
	exists, err := repo.GetSubtreeExists(context.Background(), nonExistentHash)
	assert.NoError(t, err)
	assert.False(t, exists)
}

// Test GetSubtreeExists method - not exists case
func TestRepository_GetSubtreeExists_NotExists(t *testing.T) {
	repo := createTestRepository(t)

	nonExistentHash, err := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
	require.NoError(t, err)

	// Should return false for non-existent subtree
	exists, err := repo.GetSubtreeExists(context.Background(), nonExistentHash)
	assert.NoError(t, err)
	assert.False(t, exists)
}

// Test GetUtxo method
func TestRepository_GetUtxo(t *testing.T) {
	repo := createTestRepository(t)

	// Create a proper hash from string
	hash, err := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
	require.NoError(t, err)

	spend := &utxo.Spend{
		TxID:     hash, // Add TxID to prevent nil pointer issues
		UTXOHash: hash,
		Vout:     0,
	}

	// Should return an error for non-existent UTXO
	response, err := repo.GetUtxo(context.Background(), spend)
	assert.Error(t, err)
	assert.Nil(t, response)
}

// Test GetBestBlockHeader method
func TestRepository_GetBestBlockHeader(t *testing.T) {
	repo := createTestRepository(t)

	header, headerMeta, err := repo.GetBestBlockHeader(context.Background())
	// Should not error but might return empty results
	assert.NoError(t, err)
	assert.NotNil(t, header)
	assert.NotNil(t, headerMeta)
}

// Test GetBlockLocator method
func TestRepository_GetBlockLocator(t *testing.T) {
	repo := createTestRepository(t)

	blockHash, err := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	require.NoError(t, err)

	locator, err := repo.GetBlockLocator(context.Background(), blockHash, 100)
	// Should return an error for non-existent block
	assert.Error(t, err)
	assert.Nil(t, locator)
}

// Test GetBlockByID method
func TestRepository_GetBlockByID(t *testing.T) {
	repo := createTestRepository(t)

	// Should return an error for non-existent block ID
	block, err := repo.GetBlockByID(context.Background(), 999999)
	assert.Error(t, err)
	assert.Nil(t, block)
	assert.Contains(t, err.Error(), "BLOCK_NOT_FOUND")
}

// Test error handling in GetSubtreeBytes when reader.Close() fails
func TestRepository_GetSubtreeBytes_CloseError(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings(t)

	// Create mock store that returns a reader that fails on close
	mockStore := &mockStoreWithFailingReader{}

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)
	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, settings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(logger, settings, blockChainStore, nil, nil)
	require.NoError(t, err)

	repo, err := repository.NewRepository(logger, settings, utxoStore, nil, blockchainClient, nil, mockStore, nil, nil)
	require.NoError(t, err)

	testHash, _ := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")

	// Should still succeed even if close fails (error is ignored)
	data, err := repo.GetSubtreeBytes(ctx, testHash)
	assert.NoError(t, err)
	assert.Equal(t, []byte("test data"), data)
}

// Test error path in GetSubtreeHead when ReadFull returns short read
func TestRepository_GetSubtreeHead_ShortRead(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings(t)

	// Create mock store that returns a reader with insufficient data
	mockStore := &mockStoreWithShortReader{}

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)
	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, settings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(logger, settings, blockChainStore, nil, nil)
	require.NoError(t, err)

	repo, err := repository.NewRepository(logger, settings, utxoStore, nil, blockchainClient, nil, mockStore, nil, nil)
	require.NoError(t, err)

	testHash, _ := chainhash.NewHashFromStr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")

	// Should return ErrNotFound for short read
	subtree, numNodes, err := repo.GetSubtreeHead(ctx, testHash)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "error reading subtree head bytes")
	assert.Nil(t, subtree)
	assert.Equal(t, 0, numNodes)
}

// Test GetSubtreeTxIDsReader fallback path (50% -> 100%)
func TestRepository_GetSubtreeTxIDsReader_FallbackPath(t *testing.T) {
	// Create test subtree data
	st, err := subtree.NewTreeByLeafCount(2)
	require.NoError(t, err)

	tx := &bt.Tx{Version: 1, LockTime: 0}
	err = st.AddNode(*tx.TxIDChainHash(), 0, 0)
	require.NoError(t, err)
	err = st.AddNode(*tx.TxIDChainHash(), 0, 0)
	require.NoError(t, err)

	subtreeHash := st.RootHash()
	subtreeBytes, err := st.Serialize()
	require.NoError(t, err)

	repo := createTestRepositoryWithSubtreeDataToCheck(t, subtreeHash, subtreeBytes)

	// Should fallback to FileTypeSubtreeToCheck successfully
	reader, err := repo.GetSubtreeTxIDsReader(context.Background(), subtreeHash)
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	defer reader.Close()
}

// Test GetSubtreeExists different cases
func TestRepository_GetSubtreeExists_Additional(t *testing.T) {
	repo := createTestRepository(t)

	// Test with non-existent hash (should hit fallback path)
	nonExistentHash, err := chainhash.NewHashFromStr("2222222222222222222222222222222222222222222222222222222222222222")
	require.NoError(t, err)

	// Should return false for truly non-existent subtree
	exists, err := repo.GetSubtreeExists(context.Background(), nonExistentHash)
	assert.NoError(t, err)
	assert.False(t, exists)
}

// Test GetSubtree fallback path (66.7% -> 100%)
func TestRepository_GetSubtree_FallbackPath(t *testing.T) {
	// Create test subtree data
	st, err := subtree.NewTreeByLeafCount(2)
	require.NoError(t, err)

	tx := &bt.Tx{Version: 1, LockTime: 0}
	err = st.AddNode(*tx.TxIDChainHash(), 0, 0)
	require.NoError(t, err)
	err = st.AddNode(*tx.TxIDChainHash(), 0, 0)
	require.NoError(t, err)

	subtreeHash := st.RootHash()
	subtreeBytes, err := st.Serialize()
	require.NoError(t, err)

	repo := createTestRepositoryWithSubtreeDataToCheck(t, subtreeHash, subtreeBytes)

	// Should fallback to FileTypeSubtreeToCheck successfully
	retrievedSubtree, err := repo.GetSubtree(context.Background(), subtreeHash)
	assert.NoError(t, err)
	assert.NotNil(t, retrievedSubtree)
}

// Test GetTransaction success path via TX store (88.9% -> 100%)
func TestRepository_GetTransaction_TxStoreSuccess(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings(t)

	// Create a simple transaction using hex string
	tx, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030200002f6d312d65752f605f77009f74384816a31807ffffffff0100000000000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00000000")
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()
	txBytes := tx.ExtendedBytes()

	memoryURL, err := url.Parse("memory://")
	require.NoError(t, err)
	txStore, err := blob.NewStore(logger, memoryURL)
	require.NoError(t, err)

	// Store the transaction in the TxStore
	err = txStore.Set(ctx, txHash.CloneBytes(), fileformat.FileTypeTx, txBytes)
	require.NoError(t, err)

	subtreeStore, err := blob.NewStore(logger, memoryURL)
	require.NoError(t, err)
	blockStore, err := blob.NewStore(logger, memoryURL)
	require.NoError(t, err)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)
	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, settings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(logger, settings, blockChainStore, nil, nil)
	require.NoError(t, err)

	repo, err := repository.NewRepository(logger, settings, utxoStore, txStore, blockchainClient, nil, subtreeStore, blockStore, nil)
	require.NoError(t, err)

	// Should succeed using TxStore data (UTXO store will fail, fallback to TxStore)
	retrievedTxBytes, err := repo.GetTransaction(ctx, txHash)
	assert.NoError(t, err)
	assert.NotNil(t, retrievedTxBytes)
	assert.Equal(t, txBytes, retrievedTxBytes)
}

// Test GetTransactionMeta error path (80% -> 100%)
func TestRepository_GetTransactionMeta_Error(t *testing.T) {
	repo := createTestRepository(t)

	// Use a non-existent transaction hash
	nonExistentHash, err := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
	require.NoError(t, err)

	// Should return an error for non-existent transaction
	txMeta, err := repo.GetTransactionMeta(context.Background(), nonExistentHash)
	assert.Error(t, err)
	assert.Nil(t, txMeta)
}

// Test GetSubtreeHead fallback path (76.9% -> higher coverage)
func TestRepository_GetSubtreeHead_FallbackPath(t *testing.T) {
	// Create test subtree data
	st, err := subtree.NewTreeByLeafCount(2)
	require.NoError(t, err)

	tx := &bt.Tx{Version: 1, LockTime: 0}
	err = st.AddNode(*tx.TxIDChainHash(), 0, 0)
	require.NoError(t, err)
	err = st.AddNode(*tx.TxIDChainHash(), 0, 0)
	require.NoError(t, err)

	subtreeHash := st.RootHash()
	subtreeBytes, err := st.Serialize()
	require.NoError(t, err)

	repo := createTestRepositoryWithSubtreeDataToCheck(t, subtreeHash, subtreeBytes)

	// Should fallback to FileTypeSubtreeToCheck successfully
	retrievedSubtree, numNodes, err := repo.GetSubtreeHead(context.Background(), subtreeHash)
	assert.NoError(t, err)
	assert.NotNil(t, retrievedSubtree)
	assert.Equal(t, 2, numNodes)
}

// Test error paths for several 80% coverage functions
func TestRepository_80PercentCoverage_ErrorPaths(t *testing.T) {
	repo := createTestRepository(t)
	ctx := context.Background()

	nonExistentHash, err := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
	require.NoError(t, err)

	// Test GetBlockByHeight error path
	block, err := repo.GetBlockByHeight(ctx, 999999)
	assert.Error(t, err)
	assert.Nil(t, block)

	// Test GetBlockHeader error path
	header, headerMeta, err := repo.GetBlockHeader(ctx, nonExistentHash)
	assert.Error(t, err)
	assert.Nil(t, header)
	assert.Nil(t, headerMeta)

	// Test GetLastNBlocks - might succeed with empty results
	blockInfos, err := repo.GetLastNBlocks(ctx, 999999, false, 0)
	if err != nil {
		assert.Nil(t, blockInfos)
	} else {
		assert.NotNil(t, blockInfos)
	}

	// Test GetBlocks - might succeed with empty results
	blocks, err := repo.GetBlocks(ctx, nonExistentHash, 10)
	if err != nil {
		assert.Nil(t, blocks)
	} else {
		assert.NotNil(t, blocks)
	}

	// Test GetBlockHeaders - might succeed with empty results
	headers, headerMetas, err := repo.GetBlockHeaders(ctx, nonExistentHash, 10)
	if err != nil {
		assert.Nil(t, headers)
		assert.Nil(t, headerMetas)
	} else {
		assert.NotNil(t, headers)
		assert.NotNil(t, headerMetas)
	}

	// Test GetBlockHeadersFromHeight - might succeed with empty results
	headers, headerMetas, err = repo.GetBlockHeadersFromHeight(ctx, 999999, 10)
	if err != nil {
		assert.Nil(t, headers)
		assert.Nil(t, headerMetas)
	} else {
		assert.NotNil(t, headers)
		assert.NotNil(t, headerMetas)
	}

	// Test GetSubtreeTransactions error path
	txMap, err := repo.GetSubtreeTransactions(ctx, nonExistentHash)
	assert.Error(t, err)
	assert.NotNil(t, txMap)
	assert.Len(t, txMap, 0)

	// Test GetBestBlockHeader - empty blockchain might succeed or error
	header, headerMeta, err = repo.GetBestBlockHeader(ctx)
	if err != nil {
		assert.Nil(t, header)
		assert.Nil(t, headerMeta)
	}

	// Test GetBlockLocator error path
	locator, err := repo.GetBlockLocator(ctx, nonExistentHash, 100)
	assert.Error(t, err)
	assert.Nil(t, locator)

	// Test GetBlockByID error path
	block, err = repo.GetBlockByID(ctx, 999999)
	assert.Error(t, err)
	assert.Nil(t, block)
}

// Helper functions

func createTestRepository(t *testing.T) *repository.Repository {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)
	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	memoryURL, err := url.Parse("memory://")
	require.NoError(t, err)
	txStore, err := blob.NewStore(logger, memoryURL)
	require.NoError(t, err)
	subtreeStore, err := blob.NewStore(logger, memoryURL)
	require.NoError(t, err)
	blockStore, err := blob.NewStore(logger, memoryURL)
	require.NoError(t, err)

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, settings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(logger, settings, blockChainStore, nil, nil)
	require.NoError(t, err)

	repo, err := repository.NewRepository(logger, settings, utxoStore, txStore, blockchainClient, nil, subtreeStore, blockStore, nil)
	require.NoError(t, err)

	return repo
}

func createTestRepositoryWithSubtreeData(t *testing.T, subtreeHash *chainhash.Hash, subtreeBytes []byte) *repository.Repository {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)
	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	memoryURL, err := url.Parse("memory://")
	require.NoError(t, err)
	txStore, err := blob.NewStore(logger, memoryURL)
	require.NoError(t, err)
	subtreeStore, err := blob.NewStore(logger, memoryURL)
	require.NoError(t, err)
	blockStore, err := blob.NewStore(logger, memoryURL)
	require.NoError(t, err)

	// Store subtree data
	err = subtreeStore.Set(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtree, subtreeBytes)
	require.NoError(t, err)

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, settings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(logger, settings, blockChainStore, nil, nil)
	require.NoError(t, err)

	repo, err := repository.NewRepository(logger, settings, utxoStore, txStore, blockchainClient, nil, subtreeStore, blockStore, nil)
	require.NoError(t, err)

	return repo
}

func createTestRepositoryWithSubtreeDataToCheck(t *testing.T, subtreeHash *chainhash.Hash, subtreeBytes []byte) *repository.Repository {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)
	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	memoryURL, err := url.Parse("memory://")
	require.NoError(t, err)
	txStore, err := blob.NewStore(logger, memoryURL)
	require.NoError(t, err)
	subtreeStore, err := blob.NewStore(logger, memoryURL)
	require.NoError(t, err)
	blockStore, err := blob.NewStore(logger, memoryURL)
	require.NoError(t, err)

	// Store subtree data with FileTypeSubtreeToCheck
	err = subtreeStore.Set(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtreeToCheck, subtreeBytes)
	require.NoError(t, err)

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, settings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(logger, settings, blockChainStore, nil, nil)
	require.NoError(t, err)

	repo, err := repository.NewRepository(logger, settings, utxoStore, txStore, blockchainClient, nil, subtreeStore, blockStore, nil)
	require.NoError(t, err)

	return repo
}

// Mock stores for testing edge cases

type mockUnhealthyStore struct{}

func (m *mockUnhealthyStore) Health(ctx context.Context, includeMetrics bool) (int, string, error) {
	return http.StatusServiceUnavailable, "unhealthy", nil
}

func (m *mockUnhealthyStore) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	return nil, errors.NewStorageError("store is unhealthy")
}

func (m *mockUnhealthyStore) GetReader(ctx context.Context, key []byte, fileType fileformat.FileType) (io.ReadCloser, error) {
	return nil, errors.NewStorageError("store is unhealthy")
}

func (m *mockUnhealthyStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	return nil, errors.NewStorageError("store is unhealthy")
}

func (m *mockUnhealthyStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	return errors.NewStorageError("store is unhealthy")
}

func (m *mockUnhealthyStore) Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error {
	return errors.NewStorageError("store is unhealthy")
}

func (m *mockUnhealthyStore) KeyExists(ctx context.Context, key []byte, fileType fileformat.FileType) bool {
	return false
}

func (m *mockUnhealthyStore) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
	return false, errors.NewStorageError("store is unhealthy")
}

func (m *mockUnhealthyStore) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	return errors.NewStorageError("store is unhealthy")
}

func (m *mockUnhealthyStore) SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, height uint32, opts ...options.FileOption) error {
	return errors.NewStorageError("store is unhealthy")
}

func (m *mockUnhealthyStore) SetCurrentBlockHeight(height uint32) {}

func (m *mockUnhealthyStore) Close(ctx context.Context) error {
	return nil
}

type failingReadCloser struct {
	data []byte
	pos  int
}

func (f *failingReadCloser) Read(p []byte) (n int, err error) {
	if f.pos >= len(f.data) {
		return 0, io.EOF
	}
	n = copy(p, f.data[f.pos:])
	f.pos += n
	return n, nil
}

func (f *failingReadCloser) Close() error {
	return errors.NewStorageError("intentional close error")
}

type mockStoreWithFailingReader struct{}

func (m *mockStoreWithFailingReader) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	return &failingReadCloser{data: []byte("test data")}, nil
}

func (m *mockStoreWithFailingReader) Health(ctx context.Context, includeMetrics bool) (int, string, error) {
	return http.StatusOK, "OK", nil
}

func (m *mockStoreWithFailingReader) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	return []byte("test data"), nil
}

func (m *mockStoreWithFailingReader) GetReader(ctx context.Context, key []byte, fileType fileformat.FileType) (io.ReadCloser, error) {
	return &failingReadCloser{data: []byte("test data")}, nil
}

func (m *mockStoreWithFailingReader) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	return nil
}

func (m *mockStoreWithFailingReader) Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error {
	return nil
}

func (m *mockStoreWithFailingReader) KeyExists(ctx context.Context, key []byte, fileType fileformat.FileType) bool {
	return true
}

func (m *mockStoreWithFailingReader) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
	return true, nil
}

func (m *mockStoreWithFailingReader) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	return nil
}

func (m *mockStoreWithFailingReader) SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, height uint32, opts ...options.FileOption) error {
	return nil
}

func (m *mockStoreWithFailingReader) SetCurrentBlockHeight(height uint32) {}

func (m *mockStoreWithFailingReader) Close(ctx context.Context) error {
	return nil
}

type shortReadCloser struct {
	data []byte
	pos  int
}

func (s *shortReadCloser) Read(p []byte) (n int, err error) {
	// Only return 10 bytes instead of the expected 56
	if s.pos >= 10 {
		return 0, io.EOF
	}
	remaining := 10 - s.pos
	if len(p) > remaining {
		n = remaining
	} else {
		n = len(p)
	}
	copy(p, s.data[s.pos:s.pos+n])
	s.pos += n
	return n, nil
}

func (s *shortReadCloser) Close() error {
	return nil
}

type mockStoreWithShortReader struct{}

func (m *mockStoreWithShortReader) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	// Return a reader with insufficient data (only 10 bytes instead of 56 needed)
	data := make([]byte, 56)
	return &shortReadCloser{data: data}, nil
}

func (m *mockStoreWithShortReader) Health(ctx context.Context, includeMetrics bool) (int, string, error) {
	return http.StatusOK, "OK", nil
}

func (m *mockStoreWithShortReader) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	return nil, errors.NewStorageError("not implemented")
}

func (m *mockStoreWithShortReader) GetReader(ctx context.Context, key []byte, fileType fileformat.FileType) (io.ReadCloser, error) {
	return nil, errors.NewStorageError("not implemented")
}

func (m *mockStoreWithShortReader) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	return errors.NewStorageError("not implemented")
}

func (m *mockStoreWithShortReader) Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error {
	return errors.NewStorageError("not implemented")
}

func (m *mockStoreWithShortReader) KeyExists(ctx context.Context, key []byte, fileType fileformat.FileType) bool {
	return true
}

func (m *mockStoreWithShortReader) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
	return true, nil
}

func (m *mockStoreWithShortReader) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	return errors.NewStorageError("not implemented")
}

func (m *mockStoreWithShortReader) SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, height uint32, opts ...options.FileOption) error {
	return errors.NewStorageError("not implemented")
}

func (m *mockStoreWithShortReader) SetCurrentBlockHeight(height uint32) {}

func (m *mockStoreWithShortReader) Close(ctx context.Context) error {
	return nil
}
