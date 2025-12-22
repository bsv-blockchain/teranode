package utxopersister

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	terrors "github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestReadWriteHeight(t *testing.T) {
	ctx := context.Background()

	store := memory.New()

	// Create a new UTXO persister
	tSettings := test.CreateBaseTestSettings(t)
	s := New(ctx, ulogger.TestLogger{}, tSettings, store, nil)

	oldHeight, err := s.readLastHeight(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), oldHeight)

	// Write the height
	err = s.writeLastHeight(ctx, 100_000)
	require.NoError(t, err)

	height, err := s.readLastHeight(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(100_000), height)

	// Write the old height back
	err = s.writeLastHeight(ctx, oldHeight)
	require.NoError(t, err)
}

// Helper function to create a test hash
func createTestHash(s string) chainhash.Hash {
	hash := chainhash.DoubleHashH([]byte(s))
	return hash
}

// Helper function to create test block header
func createTestBlockHeader() *model.BlockHeader {
	prevHash := createTestHash("prev-block")
	merkleRoot := createTestHash("merkle-root")

	// Create NBit from bytes
	var nBit model.NBit
	copy(nBit[:], []byte{0xff, 0xff, 0x00, 0x1d}) // Little endian representation of 0x1d00ffff

	return &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &prevHash,
		HashMerkleRoot: &merkleRoot,
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           nBit,
		Nonce:          12345,
	}
}

// Helper function to create test block header meta
func createTestBlockHeaderMeta(height uint32) *model.BlockHeaderMeta {
	return &model.BlockHeaderMeta{
		Height:      height,
		ChainWork:   []byte("chainwork"),
		BlockTime:   uint32(time.Now().Unix()),
		Timestamp:   uint32(time.Now().Unix()),
		TxCount:     1,
		SizeInBytes: 1000,
		MinedSet:    true,
		SubtreesSet: true,
	}
}

// Test NewDirect constructor
func TestNewDirect(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()
	blockchainStore := &blockchain_store.MockStore{}

	server, err := NewDirect(ctx, logger, tSettings, blockStore, blockchainStore)

	require.NoError(t, err)
	assert.NotNil(t, server)
	assert.Equal(t, logger, server.logger)
	assert.Equal(t, tSettings, server.settings)
	assert.Equal(t, blockStore, server.blockStore)
	assert.Equal(t, blockchainStore, server.blockchainStore)
	assert.NotNil(t, server.stats)
	assert.NotNil(t, server.triggerCh)
}

// Test New constructor with nil block store
func TestNew_NilBlockStore(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	mockBlockchainClient := &blockchain.Mock{}

	server := New(ctx, logger, tSettings, nil, mockBlockchainClient)

	assert.NotNil(t, server)
	assert.Equal(t, logger, server.logger)
	assert.Equal(t, tSettings, server.settings)
	assert.Nil(t, server.blockStore)
	assert.Equal(t, mockBlockchainClient, server.blockchainClient)
}

// Test Health - liveness check
func TestHealth_LivenessCheck(t *testing.T) {
	ctx := context.Background()
	server := &Server{}

	status, message, err := server.Health(ctx, true)

	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "OK", message)
}

// Test Health - readiness check with no dependencies (simpler test)
func TestHealth_ReadinessCheck_NoDependencies(t *testing.T) {
	ctx := context.Background()

	server := &Server{
		// No dependencies set
	}

	status, message, err := server.Health(ctx, false)

	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, message, "200")
}

// Test Init - successful initialization
func TestInit_Success(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()
	server := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, nil)

	// Write a test height first
	err := server.writeLastHeight(ctx, 12345)
	require.NoError(t, err)

	err = server.Init(ctx)

	assert.NoError(t, err)
	assert.Equal(t, uint32(12345), server.lastHeight)
}

// Test Init - no existing height file
func TestInit_NoExistingHeight(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()
	server := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, nil)

	err := server.Init(ctx)

	assert.NoError(t, err)
	assert.Equal(t, uint32(0), server.lastHeight)
}

// Test Stop
func TestStop(t *testing.T) {
	ctx := context.Background()
	server := &Server{}

	err := server.Stop(ctx)

	assert.NoError(t, err)
}

// Test trigger - not running
func TestTrigger_NotRunning(t *testing.T) {
	ctx := context.Background()

	// Create server with minimal setup to avoid nil pointer errors
	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()
	mockBlockchainClient := &blockchain.Mock{}

	// Mock the blockchain client methods that will be called during processNextBlock
	mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
		createTestBlockHeader(),
		createTestBlockHeaderMeta(150),
		nil,
	)

	server := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, mockBlockchainClient)
	server.lastHeight = 100

	err := server.trigger(ctx, "test")

	assert.NoError(t, err)
	assert.False(t, server.running)

	mockBlockchainClient.AssertExpectations(t)
}

// Test trigger - already running
func TestTrigger_AlreadyRunning(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)
	server := &Server{
		running:  true,
		logger:   ulogger.TestLogger{},
		settings: tSettings,
		mu:       sync.Mutex{},
	}

	err := server.trigger(ctx, "test")

	assert.NoError(t, err)
	assert.True(t, server.running) // Should remain true
}

// Test processNextBlock - waiting for confirmations
func TestProcessNextBlock_WaitingForConfirmations(t *testing.T) {
	ctx := context.Background()

	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
		createTestBlockHeader(),
		createTestBlockHeaderMeta(150), // Best block is at height 150
		nil,
	)

	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()
	server := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, mockBlockchainClient)
	server.lastHeight = 100 // Current height 100, need 100 confirmations, so can process up to height 50

	duration, err := server.processNextBlock(ctx)

	assert.NoError(t, err)
	assert.Equal(t, 1*time.Minute, duration)

	mockBlockchainClient.AssertExpectations(t)
}

// Test processNextBlock - error getting best block header
func TestProcessNextBlock_GetBestBlockHeaderError(t *testing.T) {
	ctx := context.Background()

	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
		(*model.BlockHeader)(nil),
		(*model.BlockHeaderMeta)(nil),
		terrors.NewServiceError("blockchain error"),
	)

	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()
	server := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, mockBlockchainClient)

	duration, err := server.processNextBlock(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "blockchain error")
	assert.Equal(t, time.Duration(0), duration)

	mockBlockchainClient.AssertExpectations(t)
}

// Test readLastHeight - file parsing error
func TestReadLastHeight_ParseError(t *testing.T) {
	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()
	server := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, nil)

	// Write invalid height data
	err := blockStore.Set(ctx, nil, "dat", []byte("invalid-height"), options.WithFilename("lastProcessed"))
	require.NoError(t, err)

	height, err := server.readLastHeight(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse height")
	assert.Equal(t, uint32(0), height)
}

// Test verifyLastSet - error case (no UTXO set exists)
func TestVerifyLastSet_NoUTXOSetExists(t *testing.T) {
	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()
	server := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, nil)

	testHash := createTestHash("test-block")

	// This should fail because no UTXO set exists
	err := server.verifyLastSet(ctx, &testHash)

	assert.Error(t, err)
}

// Test trigger - processNextBlock returns not found error
func TestTrigger_ProcessNextBlockNotFound(t *testing.T) {
	ctx := context.Background()

	mockBlockchainClient := &blockchain.Mock{}
	// Mock to return not found error
	mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
		(*model.BlockHeader)(nil),
		(*model.BlockHeaderMeta)(nil),
		terrors.ErrNotFound,
	)

	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()
	server := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, mockBlockchainClient)

	err := server.trigger(ctx, "test")

	assert.NoError(t, err) // ErrNotFound is handled gracefully
	assert.False(t, server.running)

	mockBlockchainClient.AssertExpectations(t)
}

// Test trigger - processNextBlock returns other error
func TestTrigger_ProcessNextBlockError(t *testing.T) {
	ctx := context.Background()

	mockBlockchainClient := &blockchain.Mock{}
	// Mock to return generic error
	mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
		(*model.BlockHeader)(nil),
		(*model.BlockHeaderMeta)(nil),
		terrors.NewServiceError("service error"),
	)

	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()
	server := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, mockBlockchainClient)

	err := server.trigger(ctx, "test")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "service error")
	assert.False(t, server.running)

	mockBlockchainClient.AssertExpectations(t)
}

// Test processNextBlock - wrong number of headers
func TestProcessNextBlock_WrongNumberOfHeaders(t *testing.T) {
	ctx := context.Background()

	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(
		createTestBlockHeader(),
		createTestBlockHeaderMeta(200), // Best block height 200
		nil,
	)
	// Mock GetBlockHeadersByHeight to return empty slice (wrong number)
	mockBlockchainClient.On("GetBlockHeadersByHeight", mock.Anything, mock.Anything, mock.Anything).Return(
		[]*model.BlockHeader{}, // Empty slice instead of 1 header
		[]*model.BlockHeaderMeta{},
		nil,
	)

	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()
	server := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, mockBlockchainClient)
	server.lastHeight = 50

	duration, err := server.processNextBlock(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "1 headers should have been returned, got 0")
	assert.Equal(t, time.Duration(0), duration)

	mockBlockchainClient.AssertExpectations(t)
}

// Test writeLastHeight - success (already covered but explicit test)
func TestWriteLastHeight_Success(t *testing.T) {
	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings(t)
	blockStore := memory.New()
	server := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, nil)

	err := server.writeLastHeight(ctx, 54321)

	assert.NoError(t, err)

	// Verify it was written correctly
	height, err := server.readLastHeight(ctx)
	assert.NoError(t, err)
	assert.Equal(t, uint32(54321), height)
}

// Test trigger - already running (simplified)
func TestTrigger_AlreadyRunning_Simple(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)
	server := New(ctx, ulogger.TestLogger{}, tSettings, memory.New(), nil)
	server.running = true

	err := server.trigger(ctx, "test")

	assert.NoError(t, err)
	assert.True(t, server.running) // Should remain true
}
