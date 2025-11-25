package httpimpl

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/bump"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockRepositoryForMerkleProof is a mock implementation of repository.Interface for testing merkle proof
type MockRepositoryForMerkleProof struct {
	mock.Mock
}

func (m *MockRepositoryForMerkleProof) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	args := m.Called(ctx, checkLiveness)
	return args.Int(0), args.String(1), args.Error(2)
}

func (m *MockRepositoryForMerkleProof) GetTxMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error) {
	args := m.Called(ctx, hash)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*meta.Data), args.Error(1)
}

func (m *MockRepositoryForMerkleProof) GetTransaction(ctx context.Context, hash *chainhash.Hash) ([]byte, error) {
	args := m.Called(ctx, hash)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockRepositoryForMerkleProof) GetBlockStats(ctx context.Context) (*model.BlockStats, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.BlockStats), args.Error(1)
}

func (m *MockRepositoryForMerkleProof) GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error) {
	args := m.Called(ctx, periodMillis)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.BlockDataPoints), args.Error(1)
}

func (m *MockRepositoryForMerkleProof) GetTransactionMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error) {
	args := m.Called(ctx, hash)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*meta.Data), args.Error(1)
}

func (m *MockRepositoryForMerkleProof) GetBlockByHash(ctx context.Context, hash *chainhash.Hash) (*model.Block, error) {
	args := m.Called(ctx, hash)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.Block), args.Error(1)
}

func (m *MockRepositoryForMerkleProof) GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error) {
	args := m.Called(ctx, height)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.Block), args.Error(1)
}

func (m *MockRepositoryForMerkleProof) GetBlockByID(ctx context.Context, id uint64) (*model.Block, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.Block), args.Error(1)
}

func (m *MockRepositoryForMerkleProof) GetBlocksByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.Block, error) {
	args := m.Called(ctx, startHeight, endHeight)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*model.Block), args.Error(1)
}

func (m *MockRepositoryForMerkleProof) FindBlocksContainingSubtree(ctx context.Context, subtreeHash *chainhash.Hash) ([]uint32, []uint32, []int, error) {
	args := m.Called(ctx, subtreeHash)
	if args.Error(3) != nil {
		return nil, nil, nil, args.Error(3)
	}
	return args.Get(0).([]uint32), args.Get(1).([]uint32), args.Get(2).([]int), args.Error(3)
}

func (m *MockRepositoryForMerkleProof) GetBlockHeader(ctx context.Context, hash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	args := m.Called(ctx, hash)
	if args.Get(0) == nil {
		return nil, nil, args.Error(2)
	}
	return args.Get(0).(*model.BlockHeader), args.Get(1).(*model.BlockHeaderMeta), args.Error(2)
}

func (m *MockRepositoryForMerkleProof) GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error) {
	args := m.Called(ctx, n, includeOrphans, fromHeight)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*model.BlockInfo), args.Error(1)
}

func (m *MockRepositoryForMerkleProof) GetBlocks(ctx context.Context, hash *chainhash.Hash, n uint32) ([]*model.Block, error) {
	args := m.Called(ctx, hash, n)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*model.Block), args.Error(1)
}

func (m *MockRepositoryForMerkleProof) GetBlockHeaders(ctx context.Context, hash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	args := m.Called(ctx, hash, numberOfHeaders)
	if args.Get(0) == nil {
		return nil, nil, args.Error(2)
	}
	return args.Get(0).([]*model.BlockHeader), args.Get(1).([]*model.BlockHeaderMeta), args.Error(2)
}

// ... (Additional mock methods would continue here, but I'll skip them for brevity)

func TestGetMerkleProof(t *testing.T) {
	// Initialize Prometheus metrics
	initPrometheusMetrics()

	// Setup
	logger := ulogger.TestLogger{}
	tSettings := &settings.Settings{
		Asset: settings.AssetSettings{},
	}

	t.Run("successful merkle proof generation", func(t *testing.T) {
		// Create mock repository
		mockRepo := new(MockRepositoryForMerkleProof)
		h := &HTTP{
			logger:     logger,
			settings:   tSettings,
			repository: mockRepo,
		}

		// Create test data
		txHashStr := "abc1234567890123456789012345678901234567890123456789012345678901"
		txHash, err := chainhash.NewHashFromStr(txHashStr)
		require.NoError(t, err, "Failed to parse txHash")

		subtreeHashStr := "def4567890123456789012345678901234567890123456789012345678901234"
		subtreeHash, err := chainhash.NewHashFromStr(subtreeHashStr)
		require.NoError(t, err, "Failed to parse subtreeHash")

		merkleRootStr := "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321"
		merkleRoot, err := chainhash.NewHashFromStr(merkleRootStr)
		require.NoError(t, err, "Failed to parse merkleRoot")

		// Create mock transaction metadata
		txMeta := &meta.Data{
			Tx:           &bt.Tx{},
			BlockIDs:     []uint32{1},
			BlockHeights: []uint32{100},
			SubtreeIdxs:  []int{0},
		}

		// Create mock subtree with proper initialization
		mockSubtree, err := subtree.NewTreeByLeafCount(2)
		require.NoError(t, err, "Failed to create subtree")
		mockSubtree.Nodes = []subtree.Node{
			{Hash: *txHash},
			{Hash: chainhash.Hash{}},
		}

		// Create NBit for difficulty
		bits, _ := model.NewNBitFromString("1d00ffff")

		// Create mock block
		mockBlock := &model.Block{
			Header: &model.BlockHeader{
				HashPrevBlock:  &chainhash.Hash{},
				HashMerkleRoot: merkleRoot,
				Timestamp:      1234567890,
				Bits:           *bits,
				Nonce:          12345,
				Version:        1,
			},
			Subtrees: []*chainhash.Hash{subtreeHash},
			Height:   100,
		}

		// Create mock block header
		mockBlockHeader := &model.BlockHeader{
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: merkleRoot,
			Timestamp:      1234567890,
			Bits:           *bits,
			Nonce:          12345,
			Version:        1,
		}

		// Create mock block header metadata
		mockBlockHeaderMeta := &model.BlockHeaderMeta{
			Height: 100,
		}

		// Setup mock expectations
		mockRepo.On("GetTxMeta", mock.Anything, txHash).Return(txMeta, nil)
		mockRepo.On("GetBlockByID", mock.Anything, uint64(1)).Return(mockBlock, nil)
		mockRepo.On("GetSubtree", mock.Anything, subtreeHash).Return(mockSubtree, nil)
		mockRepo.On("GetBlockHeader", mock.Anything, mock.AnythingOfType("*chainhash.Hash")).Return(mockBlockHeader, mockBlockHeaderMeta, nil)

		// Create request
		e := echo.New()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/merkle_proof/"+txHashStr+"/json", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetParamNames("hash")
		c.SetParamValues(txHashStr)

		// Execute handler
		handler := h.GetMerkleProof(JSON)
		err = handler(c)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		// Parse BUMP response
		var response bump.Format
		assert.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		assert.Equal(t, uint32(100), response.BlockHeight)
		// BUMP format does not include txID, subtreeIndex, or txIndexInSubtree directly
		// These are encoded in the path structure

		mockRepo.AssertExpectations(t)
	})

	t.Run("invalid transaction hash length", func(t *testing.T) {
		mockRepo := new(MockRepositoryForMerkleProof)
		h := &HTTP{
			logger:     logger,
			settings:   tSettings,
			repository: mockRepo,
		}

		// Create request with invalid hash
		e := echo.New()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/merkle_proof/invalidhash/json", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetParamNames("hash")
		c.SetParamValues("invalidhash")

		// Execute handler
		handler := h.GetMerkleProof(JSON)
		err := handler(c)

		// Assert
		httpErr, ok := err.(*echo.HTTPError)
		require.True(t, ok)
		assert.Equal(t, http.StatusBadRequest, httpErr.Code)
		assert.Contains(t, httpErr.Message, "invalid hash length")
	})

	t.Run("invalid transaction hash format", func(t *testing.T) {
		mockRepo := new(MockRepositoryForMerkleProof)
		h := &HTTP{
			logger:     logger,
			settings:   tSettings,
			repository: mockRepo,
		}

		// Create request with invalid hash format (non-hex characters)
		invalidHash := "xyz1234567890123456789012345678901234567890123456789012345678901"
		e := echo.New()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/merkle_proof/"+invalidHash+"/json", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetParamNames("hash")
		c.SetParamValues(invalidHash)

		// Execute handler
		handler := h.GetMerkleProof(JSON)
		err := handler(c)

		// Assert
		httpErr, ok := err.(*echo.HTTPError)
		require.True(t, ok)
		assert.Equal(t, http.StatusBadRequest, httpErr.Code)
		assert.Contains(t, httpErr.Message, "invalid hash string")
	})

	t.Run("transaction not found", func(t *testing.T) {
		mockRepo := new(MockRepositoryForMerkleProof)
		h := &HTTP{
			logger:     logger,
			settings:   tSettings,
			repository: mockRepo,
		}

		// Create test data
		txHashStr := "abc1234567890123456789012345678901234567890123456789012345678901"
		txHash, _ := chainhash.NewHashFromStr(txHashStr)

		// Setup mock to return not found error
		mockRepo.On("GetTxMeta", mock.Anything, txHash).Return(nil, errors.ErrNotFound)

		// Also mock the subtree fallback attempt
		mockRepo.On("FindBlocksContainingSubtree", mock.Anything, txHash).Return(nil, nil, nil, errors.ErrNotFound)

		// Create request
		e := echo.New()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/merkle_proof/"+txHashStr+"/json", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetParamNames("hash")
		c.SetParamValues(txHashStr)

		// Execute handler
		handler := h.GetMerkleProof(JSON)
		err := handler(c)

		// Assert
		httpErr, ok := err.(*echo.HTTPError)
		require.True(t, ok)
		assert.Equal(t, http.StatusNotFound, httpErr.Code)
		assert.Contains(t, httpErr.Message, "hash not found as transaction or subtree")

		mockRepo.AssertExpectations(t)
	})

	t.Run("transaction not in any block", func(t *testing.T) {
		mockRepo := new(MockRepositoryForMerkleProof)
		h := &HTTP{
			logger:     logger,
			settings:   tSettings,
			repository: mockRepo,
		}

		// Create test data
		txHashStr := "abc1234567890123456789012345678901234567890123456789012345678901"
		txHash, _ := chainhash.NewHashFromStr(txHashStr)

		// Create mock transaction metadata with no blocks
		txMeta := &meta.Data{
			Tx:           &bt.Tx{},
			BlockIDs:     []uint32{}, // Empty - not in any block
			BlockHeights: []uint32{},
			SubtreeIdxs:  []int{},
		}

		// Setup mock to return transaction with no blocks
		mockRepo.On("GetTxMeta", mock.Anything, txHash).Return(txMeta, nil)

		// Mock the subtree fallback to return the same error
		mockRepo.On("FindBlocksContainingSubtree", mock.Anything, txHash).Return(nil, nil, nil, errors.NewProcessingError("transaction not in any block"))

		// Create request
		e := echo.New()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/merkle_proof/"+txHashStr+"/json", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetParamNames("hash")
		c.SetParamValues(txHashStr)

		// Execute handler
		handler := h.GetMerkleProof(JSON)
		err := handler(c)

		// Assert
		httpErr, ok := err.(*echo.HTTPError)
		require.True(t, ok)
		assert.Equal(t, http.StatusInternalServerError, httpErr.Code)
		assert.Contains(t, httpErr.Message, "transaction not in any block")

		mockRepo.AssertExpectations(t)
	})

	t.Run("binary stream mode", func(t *testing.T) {
		mockRepo := new(MockRepositoryForMerkleProof)
		h := &HTTP{
			logger:     logger,
			settings:   tSettings,
			repository: mockRepo,
		}

		// Create request
		txHashStr := "abc1234567890123456789012345678901234567890123456789012345678901"
		e := echo.New()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/merkle_proof/"+txHashStr, nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetParamNames("hash")
		c.SetParamValues(txHashStr)

		// Setup mock expectations - all merkle proof mocks
		txHash, _ := chainhash.NewHashFromStr(txHashStr)
		txMeta := &meta.Data{
			Tx:           &bt.Tx{},
			BlockIDs:     []uint32{1},
			BlockHeights: []uint32{100},
			SubtreeIdxs:  []int{0},
		}
		mockRepo.On("GetTxMeta", mock.Anything, txHash).Return(txMeta, nil)

		// We need all the other mocks too since the error happens at the end
		subtreeHashStr := "def4567890123456789012345678901234567890123456789012345678901234"
		subtreeHash, _ := chainhash.NewHashFromStr(subtreeHashStr)

		bits, _ := model.NewNBitFromString("1d00ffff")
		mockBlock := &model.Block{
			Header: &model.BlockHeader{
				HashPrevBlock:  &chainhash.Hash{},
				HashMerkleRoot: &chainhash.Hash{},
				Timestamp:      1234567890,
				Bits:           *bits,
				Nonce:          12345,
				Version:        1,
			},
			Subtrees: []*chainhash.Hash{subtreeHash},
			Height:   100,
		}

		mockSubtree, _ := subtree.NewTreeByLeafCount(2)
		mockSubtree.Nodes = []subtree.Node{
			{Hash: *txHash},
			{Hash: chainhash.Hash{}},
		}

		mockBlockHeader := &model.BlockHeader{
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      1234567890,
			Bits:           *bits,
			Nonce:          12345,
			Version:        1,
		}
		mockBlockHeaderMeta := &model.BlockHeaderMeta{Height: 100}

		mockRepo.On("GetBlockByID", mock.Anything, uint64(1)).Return(mockBlock, nil)
		mockRepo.On("GetSubtree", mock.Anything, subtreeHash).Return(mockSubtree, nil)
		mockRepo.On("GetBlockHeader", mock.Anything, mock.AnythingOfType("*chainhash.Hash")).Return(mockBlockHeader, mockBlockHeaderMeta, nil)

		// Execute handler with binary mode
		handler := h.GetMerkleProof(BINARY_STREAM)
		err := handler(c)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "application/octet-stream", rec.Header().Get("Content-Type"))
		assert.NotEmpty(t, rec.Body.Bytes())
	})
}

// TestMerkleProofAdapter tests the adapter that allows using merkleproof package with the repository
func TestMerkleProofAdapter(t *testing.T) {
	t.Run("adapter properly converts repository data", func(t *testing.T) {
		ctx := context.Background()
		txHash, _ := chainhash.NewHashFromStr("abc1234567890123456789012345678901234567890123456789012345678901")
		blockHash, _ := chainhash.NewHashFromStr("def1234567890123456789012345678901234567890123456789012345678901")
		subtreeHash, _ := chainhash.NewHashFromStr("fed1234567890123456789012345678901234567890123456789012345678901")

		// Create mock repository
		mockRepo := new(MockRepositoryForMerkleProof)

		// Setup mock expectations
		txMeta := &meta.Data{
			BlockHeights: []uint32{100, 101},
			SubtreeIdxs:  []int{0, 1},
			BlockIDs:     []uint32{100, 101},
		}

		block := &model.Block{
			Subtrees: []*chainhash.Hash{subtreeHash},
		}

		blockHeader := &model.BlockHeader{
			HashMerkleRoot: subtreeHash,
		}

		st := &subtree.Subtree{
			Nodes: []subtree.Node{
				{Hash: *txHash},
			},
		}

		// Setup mock calls
		mockRepo.On("GetTxMeta", ctx, txHash).Return(txMeta, nil)
		mockRepo.On("GetBlockByID", ctx, uint64(100)).Return(block, nil)
		mockRepo.On("GetBlockHeader", ctx, blockHash).Return(blockHeader, (*model.BlockHeaderMeta)(nil), nil)
		mockRepo.On("GetSubtree", ctx, subtreeHash).Return(st, nil)

		// Create adapter
		adapter := newMerkleProofAdapter(ctx, mockRepo)

		// Test GetTxMeta conversion
		txMetaSimple, err := adapter.GetTxMeta(txHash)
		assert.NoError(t, err)
		assert.NotNil(t, txMetaSimple)
		assert.Equal(t, []uint32{100, 101}, txMetaSimple.BlockIDs)
		assert.Equal(t, []uint32{100, 101}, txMetaSimple.BlockHeights)
		assert.Equal(t, []int{0, 1}, txMetaSimple.SubtreeIdxs)

		// Test GetBlockByID passthrough
		blockResult, err := adapter.GetBlockByID(100)
		assert.NoError(t, err)
		assert.NotNil(t, blockResult)
		assert.Equal(t, block, blockResult)

		// Test GetBlockHeader passthrough
		headerResult, err := adapter.GetBlockHeader(blockHash)
		assert.NoError(t, err)
		assert.NotNil(t, headerResult)
		assert.Equal(t, blockHeader, headerResult)

		// Test GetSubtree passthrough
		stResult, err := adapter.GetSubtree(subtreeHash)
		assert.NoError(t, err)
		assert.NotNil(t, stResult)
		assert.Equal(t, st, stResult)

		// Verify all expectations were met
		mockRepo.AssertExpectations(t)
	})
}

// Implement remaining mock methods to satisfy the interface
func (m *MockRepositoryForMerkleProof) GetBlockHeadersToCommonAncestor(ctx context.Context, hashTarget *chainhash.Hash, blockLocatorHashes []*chainhash.Hash, maxHeaders uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return nil, nil, nil
}

func (m *MockRepositoryForMerkleProof) GetBlockHeadersFromCommonAncestor(ctx context.Context, hashTarget *chainhash.Hash, blockLocatorHashes []chainhash.Hash, maxHeaders uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return nil, nil, nil
}

func (m *MockRepositoryForMerkleProof) GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return nil, nil, nil
}

func (m *MockRepositoryForMerkleProof) GetSubtreeBytes(ctx context.Context, hash *chainhash.Hash) ([]byte, error) {
	return nil, nil
}

func (m *MockRepositoryForMerkleProof) GetSubtreeTxIDsReader(ctx context.Context, hash *chainhash.Hash) (io.ReadCloser, error) {
	return nil, nil
}

func (m *MockRepositoryForMerkleProof) GetSubtreeDataReaderFromBlockPersister(ctx context.Context, hash *chainhash.Hash) (io.ReadCloser, error) {
	return nil, nil
}

func (m *MockRepositoryForMerkleProof) GetSubtreeDataReader(ctx context.Context, subtreeHash *chainhash.Hash) (io.ReadCloser, error) {
	return nil, nil
}

func (m *MockRepositoryForMerkleProof) GetSubtree(ctx context.Context, hash *chainhash.Hash) (*subtree.Subtree, error) {
	args := m.Called(ctx, hash)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*subtree.Subtree), args.Error(1)
}

func (m *MockRepositoryForMerkleProof) GetSubtreeData(ctx context.Context, hash *chainhash.Hash) (*subtree.Data, error) {
	return nil, nil
}

func (m *MockRepositoryForMerkleProof) GetSubtreeTransactions(ctx context.Context, hash *chainhash.Hash) (map[chainhash.Hash]*bt.Tx, error) {
	return nil, nil
}

func (m *MockRepositoryForMerkleProof) GetSubtreeExists(ctx context.Context, hash *chainhash.Hash) (bool, error) {
	return false, nil
}

func (m *MockRepositoryForMerkleProof) GetSubtreeHead(ctx context.Context, hash *chainhash.Hash) (*subtree.Subtree, int, error) {
	return nil, 0, nil
}

func (m *MockRepositoryForMerkleProof) GetUtxo(ctx context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	return nil, nil
}

func (m *MockRepositoryForMerkleProof) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	return nil, nil, nil
}

func (m *MockRepositoryForMerkleProof) GetLegacyBlockReader(ctx context.Context, hash *chainhash.Hash, wireBlock ...bool) (*io.PipeReader, error) {
	return nil, nil
}

func (m *MockRepositoryForMerkleProof) GetBlockLocator(ctx context.Context, blockHeaderHash *chainhash.Hash, height uint32) ([]*chainhash.Hash, error) {
	return nil, nil
}

func (m *MockRepositoryForMerkleProof) GetBlockchainClient() blockchain.ClientI {
	return nil
}

func (m *MockRepositoryForMerkleProof) GetBlockvalidationClient() blockvalidation.Interface {
	return nil
}

func (m *MockRepositoryForMerkleProof) GetP2PClient() p2p.ClientI {
	return nil
}
