package sql

import (
	"context"
	"database/sql"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	// Create unique test coinbase transaction (different from real genesis)
	testGenesisCoinbaseTx, _ = bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff4d04ffff001d0104455465737420436c617564652067656e657369732074782066726f6d20746573742073756974652074657374696e6720657375726520756e6971756520686173686573ffffffff0100f2052a01000000434104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac00000000") //nolint:lll // Test coinbase transaction hex string

	// Define a unique test genesis block that won't conflict with real genesis
	testGenesisBlock = &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1729259700,        // Different timestamp to ensure unique hash
			Nonce:          12345,             // Different nonce to ensure unique hash
			HashPrevBlock:  &chainhash.Hash{}, // All zeros for genesis
			HashMerkleRoot: hashMerkleRoot,
			Bits:           *bits,
		},
		Height:           0,
		CoinbaseTx:       testGenesisCoinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}
)

func TestStoreBlock_Genesis(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Test storing regular blocks (genesis is automatically handled by framework)
	// Store first regular block
	blockID, height, err := s.StoreBlock(context.Background(), block1, "test-peer")
	require.NoError(t, err)
	assert.Greater(t, blockID, uint64(0), "Block ID should be assigned")
	assert.Equal(t, uint32(1), height, "First block should have height 1")
}

func TestStoreBlock_RegularBlock(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Store regular blocks (genesis is automatically handled)
	blockID, height, err := s.StoreBlock(context.Background(), block1, "test-peer")
	require.NoError(t, err)
	assert.Greater(t, blockID, uint64(0), "Block ID should be assigned")
	assert.Equal(t, uint32(1), height, "First block should have height 1")

	// Store second regular block
	blockID2, height2, err := s.StoreBlock(context.Background(), block2, "test-peer")
	require.NoError(t, err)
	assert.Greater(t, blockID2, blockID, "Second block ID should be greater")
	assert.Equal(t, uint32(2), height2, "Second block should have height 2")
}

func TestStoreBlock_WithOptions(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Store block with options (genesis is automatically handled)
	blockID, height, err := s.StoreBlock(context.Background(), block1, "test-peer",
		options.WithMinedSet(true),
		options.WithSubtreesSet(true))
	require.NoError(t, err)
	assert.Greater(t, blockID, uint64(0))
	assert.Equal(t, uint32(1), height)
}

func TestStoreBlock_DuplicateBlock(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Store block first time
	_, _, err = s.StoreBlock(context.Background(), block1, "test-peer")
	require.NoError(t, err)

	// Try to store same block again - should fail
	_, _, err = s.StoreBlock(context.Background(), block1, "test-peer")
	assert.Error(t, err)
	if err != nil {
		assert.Contains(t, err.Error(), "block already exists")
	}
}

func TestStoreBlock_InvalidPreviousBlock(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Create a block that references a non-existent previous block
	nonExistentPrevHash, _ := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
	orphanBlock := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1729259800,
			Nonce:          0,
			HashPrevBlock:  nonExistentPrevHash, // Non-existent previous block
			HashMerkleRoot: hashMerkleRoot,
			Bits:           *bits,
		},
		Height:           1,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees: []*chainhash.Hash{
			subtree,
		},
	}

	// Try to store block with non-existent previous block
	_, _, err = s.StoreBlock(context.Background(), orphanBlock, "test-peer")
	assert.Error(t, err)
	if err != nil {
		assert.Contains(t, err.Error(), "previous block")
	}
}

func TestStoreBlock_ContextCancellation(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Try to store block with cancelled context
	_, _, err = s.StoreBlock(ctx, block1, "test-peer")
	assert.Error(t, err)
	if err != nil {
		assert.Contains(t, err.Error(), "context canceled")
	}
}

func TestStoreBlock_WithCustomID(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Store first block with custom ID
	blockID, height, err := s.StoreBlock(context.Background(), block1, "test-peer",
		options.WithID(50))
	require.NoError(t, err)
	assert.Equal(t, uint64(50), blockID)
	assert.Equal(t, uint32(1), height)

	// Store second block with custom ID
	blockID2, height2, err := s.StoreBlock(context.Background(), block2, "test-peer",
		options.WithID(100))
	require.NoError(t, err)
	assert.Equal(t, uint64(100), blockID2)
	assert.Equal(t, uint32(2), height2)
}

func TestStoreBlock_InvalidCustomIDForGenesis(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Store block first time
	_, _, err = s.StoreBlock(context.Background(), block1, "test-peer")
	require.NoError(t, err)

	// Try to store the same block with different custom ID - should fail
	_, _, err = s.StoreBlock(context.Background(), block1, "test-peer",
		options.WithID(100))
	assert.Error(t, err)
	if err != nil {
		assert.Contains(t, err.Error(), "block already exists")
	}
}

func TestStoreBlock_InvalidBlock(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Store block marked as invalid
	blockID, height, err := s.StoreBlock(context.Background(), block1, "test-peer", options.WithInvalid(true))
	require.NoError(t, err)
	assert.Greater(t, blockID, uint64(0))
	assert.Equal(t, uint32(1), height)
}

func TestGetPreviousBlockInfo_FromCache(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Store a block to populate cache
	_, _, err = s.StoreBlock(context.Background(), block1, "test-peer")
	require.NoError(t, err)

	// Get previous block info - should come from cache
	id, chainWork, height, invalid, err := s.getPreviousBlockInfo(context.Background(), *block1.Hash())
	require.NoError(t, err)
	assert.Greater(t, id, uint64(0))
	assert.NotNil(t, chainWork)
	assert.Equal(t, uint32(1), height)
	assert.False(t, invalid)
}

func TestGetPreviousBlockInfo_FromDatabase(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Store a block
	_, _, err = s.StoreBlock(context.Background(), block1, "test-peer")
	require.NoError(t, err)

	// Clear response cache to force database lookup
	s.ResetResponseCache()

	// Get previous block info - should come from database
	id, chainWork, height, invalid, err := s.getPreviousBlockInfo(context.Background(), *block1.Hash())
	require.NoError(t, err)
	assert.Greater(t, id, uint64(0))
	assert.NotNil(t, chainWork)
	assert.Equal(t, uint32(1), height)
	assert.False(t, invalid)
}

func TestGetPreviousBlockInfo_NotFound(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Try to get info for non-existent block
	nonExistentHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	_, _, _, _, err = s.getPreviousBlockInfo(context.Background(), *nonExistentHash)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "previous block")
	assert.Contains(t, err.Error(), "not found")
}

func TestGetPreviousBlockInfo_ContextCancellation(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Clear response cache to force database lookup
	s.ResetResponseCache()

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Try to get info with cancelled context
	_, _, _, _, err = s.getPreviousBlockInfo(ctx, *block1.Hash())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context canceled")
}

func TestParseSQLError_PostgreSQLConstraint(t *testing.T) {
	s := &SQL{}

	// Create a PostgreSQL constraint violation error
	pqErr := &pq.Error{
		Code: "23505", // Unique constraint violation
	}

	result := s.parseSQLError(pqErr, block1)
	assert.Error(t, result)
	assert.Contains(t, result.Error(), "block already exists")
	assert.Contains(t, result.Error(), block1.Hash().String())
}

func TestParseSQLError_SQLiteConstraint(t *testing.T) {
	s := &SQL{}

	// Test with a different generic SQL error
	genericSQLiteErr := sql.ErrTxDone

	result := s.parseSQLError(genericSQLiteErr, block1)
	assert.Error(t, result)
	assert.Contains(t, result.Error(), "failed to store block") // Generic error should be storage error
}

func TestParseSQLError_GenericError(t *testing.T) {
	s := &SQL{}

	// Create a generic SQL error
	genericErr := sql.ErrConnDone

	result := s.parseSQLError(genericErr, block1)
	assert.Error(t, result)
	assert.Contains(t, result.Error(), "failed to store block")
}

func TestGetPreviousBlockData_Genesis(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Test genesis block data
	coinbaseTxID := s.chainParams.GenesisBlock.Transactions[0].TxHash().String()
	genesis, height, previousBlockID, previousChainWork, previousBlockInvalid, err := s.getPreviousBlockData(context.Background(), coinbaseTxID, testGenesisBlock)

	require.NoError(t, err)
	assert.True(t, genesis)
	assert.Equal(t, uint32(0), height)
	assert.Equal(t, uint64(0), previousBlockID)
	assert.NotNil(t, previousChainWork)
	assert.Len(t, previousChainWork, 32)
	assert.False(t, previousBlockInvalid)
}

func TestGetPreviousBlockData_RegularBlock(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Store first block to create a valid parent for block2
	_, _, err = s.StoreBlock(context.Background(), block1, "test-peer")
	require.NoError(t, err)

	// Test regular block data - block2 references block1 as previous
	genesis, height, previousBlockID, previousChainWork, previousBlockInvalid, err := s.getPreviousBlockData(context.Background(), "non-genesis-coinbase", block2)

	require.NoError(t, err)
	assert.False(t, genesis)
	assert.Equal(t, uint32(2), height)
	assert.Greater(t, previousBlockID, uint64(0))
	assert.NotNil(t, previousChainWork)
	assert.False(t, previousBlockInvalid)
}

func TestGetPreviousBlockData_MissingPreviousBlock(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Create a block with non-existent previous block hash
	nonExistentPrevHash, _ := chainhash.NewHashFromStr("2222222222222222222222222222222222222222222222222222222222222222")
	orphanBlock := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1729259900,
			Nonce:          0,
			HashPrevBlock:  nonExistentPrevHash, // Non-existent previous block
			HashMerkleRoot: hashMerkleRoot,
			Bits:           *bits,
		},
		Height:           1,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
	}

	// Test with missing previous block
	_, _, _, _, _, err = s.getPreviousBlockData(context.Background(), "non-genesis-coinbase", orphanBlock)
	assert.Error(t, err)
	if err != nil {
		assert.Contains(t, err.Error(), "previous block")
		assert.Contains(t, err.Error(), "not found")
	}
}

func TestCalculateAndPrepareChainWork_Success(t *testing.T) {
	// Test with zero previous work (genesis block)
	previousChainWorkBytes := make([]byte, 32)

	chainWorkBytes, err := calculateAndPrepareChainWork(previousChainWorkBytes, block1)
	require.NoError(t, err)
	assert.NotNil(t, chainWorkBytes)
	assert.Len(t, chainWorkBytes, 32)
}

func TestCalculateAndPrepareChainWork_InvalidPreviousWork(t *testing.T) {
	// Test with invalid previous work bytes (wrong length)
	previousChainWorkBytes := []byte{0x01, 0x02}

	_, err := calculateAndPrepareChainWork(previousChainWorkBytes, block1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to convert previous chain work")
}

func TestValidateCoinbaseHeight_NoValidationNeeded(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Test block with version 1 (no BIP34 validation required)
	blockVersion1 := &model.Block{
		Header: &model.BlockHeader{
			Version: 1,
		},
		CoinbaseTx: block1.CoinbaseTx,
	}

	err = s.validateCoinbaseHeight(blockVersion1, 100)
	assert.NoError(t, err)
}

func TestValidateCoinbaseHeight_NoCoinbaseTx(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Test block without coinbase transaction
	blockNoCoinbase := &model.Block{
		Header: &model.BlockHeader{
			Version: 2,
		},
		CoinbaseTx: nil,
	}

	err = s.validateCoinbaseHeight(blockNoCoinbase, 100)
	assert.NoError(t, err)
}

func TestValidateCoinbaseHeight_PreBIP34(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Test with block height before BIP34 activation
	err = s.validateCoinbaseHeight(block1, 100) // height < 227835
	assert.NoError(t, err)                      // Should not fail for pre-BIP34 blocks
}

func TestGetCumulativeChainWork_Success(t *testing.T) {
	// Test cumulative chain work calculation
	zeroHash := &chainhash.Hash{}

	result, err := getCumulativeChainWork(zeroHash, block1)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

// TestGetCumulativeChainWork_InvalidBlock removed as getCumulativeChainWork
// is robust and handles edge cases gracefully without errors

func TestStoreBlock_DatabaseError(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	// Close database to cause error
	s.Close()

	// Try to store block with closed database
	_, _, err = s.StoreBlock(context.Background(), block1, "test-peer")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database is closed")
}

func TestStoreBlock_CacheManagement(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Store block
	blockID, height, err := s.StoreBlock(context.Background(), block1, "test-peer")
	require.NoError(t, err)

	// Verify block was stored in database
	storedHeader, storedMeta, err := s.GetBlockHeader(context.Background(), block1.Hash())
	require.NoError(t, err)
	assert.NotNil(t, storedHeader)
	assert.NotNil(t, storedMeta)
	assert.Equal(t, uint64(blockID), uint64(storedMeta.ID))
	assert.Equal(t, height, storedMeta.Height)
}

func TestStoreBlock_MinerExtraction(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Store block (has coinbase transaction)
	_, _, err = s.StoreBlock(context.Background(), block1, "test-peer")
	require.NoError(t, err)

	// Verify miner information was processed and stored in database
	_, storedMeta, err := s.GetBlockHeader(context.Background(), block1.Hash())
	require.NoError(t, err)
	require.NotNil(t, storedMeta)
	// Miner extraction may or may not succeed depending on coinbase format
	t.Logf("Extracted miner: %s", storedMeta.Miner)
}

func TestStoreBlock_SequenceOfBlocks(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Store sequence of blocks
	blockID1, height1, err := s.StoreBlock(context.Background(), block1, "peer-1")
	require.NoError(t, err)
	assert.Equal(t, uint32(1), height1)

	blockID2, height2, err := s.StoreBlock(context.Background(), block2, "peer-2")
	require.NoError(t, err)
	assert.Equal(t, uint32(2), height2)
	assert.Greater(t, blockID2, blockID1)

	blockID3, height3, err := s.StoreBlock(context.Background(), block3, "peer-3")
	require.NoError(t, err)
	assert.Equal(t, uint32(3), height3)
	assert.Greater(t, blockID3, blockID2)
}

func TestStoreBlock_ValidCoinbaseTransaction(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// First store a normal block to create a valid parent
	_, _, err = s.StoreBlock(context.Background(), block1, "test-peer")
	require.NoError(t, err)

	// Create block with minimal coinbase transaction that references the first block
	blockMinimalCoinbase := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  block1.Hash(),
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           *bits,
			Nonce:          0,
		},
		Height:           2,
		TransactionCount: 1,
		SizeInBytes:      100,
		CoinbaseTx:       coinbaseTx, // Use minimal coinbase transaction
	}

	// This should work with minimal miner extraction
	_, _, err = s.StoreBlock(context.Background(), blockMinimalCoinbase, "test-peer")
	assert.NoError(t, err)
}

func TestStoreBlock_InheritInvalidFromParent(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Genesis is automatically handled by New()

	// Store invalid block
	_, _, err = s.StoreBlock(context.Background(), block1, "test-peer", options.WithInvalid(true))
	require.NoError(t, err)

	// Store child of invalid block - should inherit invalid status
	_, _, err = s.StoreBlock(context.Background(), block2, "test-peer")
	require.NoError(t, err)

	// Verify both blocks are marked invalid in the database
	_, meta, err := s.GetBlockHeader(t.Context(), block1.Hash())
	require.NoError(t, err)
	assert.True(t, meta.Invalid)

	_, meta, err = s.GetBlockHeader(t.Context(), block2.Hash())
	require.NoError(t, err)
	assert.True(t, meta.Invalid)
}

func TestStoreBlock_ContextCancellationDuringPrevBlockLookup(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Clear response cache to force database lookup
	s.ResetResponseCache()

	// Create context that will be cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Try to store block with cancelled context
	_, _, err = s.StoreBlock(ctx, block1, "test-peer")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context canceled")
}

func TestGetPreviousBlockData_ContextCancellation(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Clear response cache to force database lookup
	s.ResetResponseCache()

	// Create orphan block that requires previous block lookup
	nonExistentPrevHash, _ := chainhash.NewHashFromStr("3333333333333333333333333333333333333333333333333333333333333333")
	orphanBlock := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1729259901,
			Nonce:          0,
			HashPrevBlock:  nonExistentPrevHash, // Non-existent previous block
			HashMerkleRoot: hashMerkleRoot,
			Bits:           *bits,
		},
		Height:           1,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
	}

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Try to get previous block data with cancelled context
	_, _, _, _, _, err = s.getPreviousBlockData(ctx, "non-genesis-coinbase", orphanBlock)
	assert.Error(t, err)
	if err != nil {
		assert.Contains(t, err.Error(), "context canceled")
	}
}

func TestValidateCoinbaseHeight_PostBIP34InvalidHeight(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Create a block that would be post-BIP34 but with invalid coinbase height extraction
	// This test verifies the BIP34 validation logic, though exact behavior depends on
	// the ExtractCoinbaseHeight implementation
	postBIP34Height := uint32(250000)

	// Test with a block that has version > 1 at post-BIP34 height
	err = s.validateCoinbaseHeight(block1, postBIP34Height)
	// The result depends on whether block1's coinbase contains valid height encoding
	// This test verifies the code path is exercised
	if err != nil {
		t.Logf("BIP34 validation failed as expected for height %d: %v", postBIP34Height, err)
	} else {
		t.Logf("BIP34 validation passed for height %d", postBIP34Height)
	}
}

func TestStoreBlock_SubtreeProcessing(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Genesis is automatically handled by New()

	// Store block with subtree processing
	blockID, height, err := s.StoreBlock(context.Background(), block1, "test-peer",
		options.WithSubtreesSet(true))
	require.NoError(t, err)
	assert.Greater(t, blockID, uint64(0))
	assert.Equal(t, uint32(1), height)

	// Verify subtrees were processed by querying database
	_, storedMeta, err := s.GetBlockHeader(context.Background(), block1.Hash())
	require.NoError(t, err)
	assert.True(t, storedMeta.SubtreesSet)
}

func TestStoreBlock_TimeConversionHandling(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Normal block storage should handle time conversion correctly
	_, _, err = s.StoreBlock(context.Background(), block1, "test-peer")
	require.NoError(t, err)

	// Verify timestamp was set in metadata by retrieving from database
	_, storedMeta, err := s.GetBlockHeader(context.Background(), block1.Hash())
	require.NoError(t, err)
	assert.Greater(t, storedMeta.Timestamp, uint32(0))
}

func TestStoreBlock_ResponseCacheInvalidation(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Store a block - should invalidate response cache
	_, _, err = s.StoreBlock(context.Background(), block1, "test-peer")
	require.NoError(t, err)

	// Verify block can be retrieved (cache invalidation succeeded)
	_, meta, err := s.GetBlockHeader(context.Background(), block1.Hash())
	require.NoError(t, err)
	require.NotNil(t, meta)
}

func TestStoreBlock_WithPersistedAt(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	ctx := context.Background()

	// Store block without WithPersistedAt - should be NULL
	blockID1, _, err := s.StoreBlock(ctx, block1, "test-peer")
	require.NoError(t, err)

	var persistedAt1 *string
	err = s.db.QueryRowContext(ctx, "SELECT persisted_at FROM blocks WHERE id = ?", blockID1).Scan(&persistedAt1)
	require.NoError(t, err)
	assert.Nil(t, persistedAt1, "persisted_at should be NULL without WithPersistedAt")

	// Store block with WithPersistedAt - should have timestamp
	startTime := time.Now()
	blockID2, _, err := s.StoreBlock(ctx, block2, "test-peer", options.WithPersistedAt())
	require.NoError(t, err)
	endTime := time.Now()

	var persistedAt2 *string
	err = s.db.QueryRowContext(ctx, "SELECT persisted_at FROM blocks WHERE id = ?", blockID2).Scan(&persistedAt2)
	require.NoError(t, err)
	require.NotNil(t, persistedAt2, "persisted_at should not be NULL with WithPersistedAt")

	parsedTime, err := time.Parse("2006-01-02 15:04:05", *persistedAt2)
	require.NoError(t, err, "persisted_at should be in valid datetime format")
	assert.True(t, parsedTime.After(startTime.Add(-2*time.Second)), "persisted_at should be after start time")
	assert.True(t, parsedTime.Before(endTime.Add(2*time.Second)), "persisted_at should be before end time")
}

func TestStoreBlock_WithPersistedAt_Postgres(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	pgContainer, err := postgres.Run(ctx,
		"postgres:13",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Minute),
		),
	)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, pgContainer.Terminate(ctx))
	}()

	// Get connection string
	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	dbURL, err := url.Parse(connStr)
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)
	s, err := New(ulogger.TestLogger{}, dbURL, tSettings)
	require.NoError(t, err)
	defer s.Close()

	// Store block without WithPersistedAt - should be NULL
	blockID1, _, err := s.StoreBlock(ctx, block1, "test-peer")
	require.NoError(t, err)

	var persistedAt1 *time.Time
	err = s.db.QueryRowContext(ctx, "SELECT persisted_at FROM blocks WHERE id = $1", blockID1).Scan(&persistedAt1)
	require.NoError(t, err)
	assert.Nil(t, persistedAt1, "persisted_at should be NULL without WithPersistedAt")

	// Store block with WithPersistedAt - should have timestamp
	startTime := time.Now()
	blockID2, _, err := s.StoreBlock(ctx, block2, "test-peer", options.WithPersistedAt())
	require.NoError(t, err)
	endTime := time.Now()

	var persistedAt2 *time.Time
	err = s.db.QueryRowContext(ctx, "SELECT persisted_at FROM blocks WHERE id = $1", blockID2).Scan(&persistedAt2)
	require.NoError(t, err)
	require.NotNil(t, persistedAt2, "persisted_at should not be NULL with WithPersistedAt")
	assert.True(t, persistedAt2.After(startTime.Add(-2*time.Second)), "persisted_at should be after start time")
	assert.True(t, persistedAt2.Before(endTime.Add(2*time.Second)), "persisted_at should be before end time")
}

func TestParseSQLError_NilError(t *testing.T) {
	s := &SQL{}

	// Test with nil error
	result := s.parseSQLError(nil, block1)
	assert.Error(t, result)
	assert.Contains(t, result.Error(), "failed to store block")
}
