package blockchain

import (
	"context"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/stores/blockchain/options"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockStore implements the Store interface for testing
type mockStore struct {
	blocks      map[chainhash.Hash]*model.Block
	headers     map[chainhash.Hash]*model.BlockHeader
	headerMetas map[chainhash.Hash]*model.BlockHeaderMeta
	bestBlock   *model.BlockHeader
	fsmState    string
}

func newMockStore() *mockStore {
	return &mockStore{
		blocks:      make(map[chainhash.Hash]*model.Block),
		headers:     make(map[chainhash.Hash]*model.BlockHeader),
		headerMetas: make(map[chainhash.Hash]*model.BlockHeaderMeta),
	}
}

func (m *mockStore) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return 200, "healthy", nil
}

func (m *mockStore) GetDB() interface{} {
	return nil
}

func (m *mockStore) GetDBEngine() util.SQLEngine {
	return "sqlite"
}

func (m *mockStore) GetHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, error) {
	if header, ok := m.headers[*blockHash]; ok {
		return header, nil
	}

	return nil, errors.NewProcessingError("block not found")
}

func (m *mockStore) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, uint32, error) {
	if block, ok := m.blocks[*blockHash]; ok {
		return block, block.Height, nil
	}

	return nil, 0, errors.NewProcessingError("block not found")
}

func (m *mockStore) GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error) {
	for _, block := range m.blocks {
		if block.Height == height {
			return block, nil
		}
	}

	return nil, errors.NewProcessingError("block not found")
}

func (m *mockStore) StoreBlock(ctx context.Context, block *model.Block, peerID string, opts ...options.StoreBlockOption) (uint64, uint32, error) {
	hash := block.Header.Hash()
	m.blocks[*hash] = block
	m.headers[*hash] = block.Header

	meta := &model.BlockHeaderMeta{
		TxCount: block.TransactionCount,
	}
	m.headerMetas[*hash] = meta

	return uint64(len(m.blocks)), block.Height, nil
}

func (m *mockStore) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	if m.bestBlock == nil {
		if len(m.headers) == 0 {
			return nil, nil, errors.NewProcessingError("no blocks in store")
		}
		// For mock, just return any header
		for _, header := range m.headers {
			meta := &model.BlockHeaderMeta{
				TxCount: 1,
			}

			return header, meta, nil
		}
	}

	hash := m.bestBlock.Hash()

	return m.bestBlock, m.headerMetas[*hash], nil
}

func (m *mockStore) GetFSMState(ctx context.Context) (string, error) {
	return m.fsmState, nil
}

func (m *mockStore) SetFSMState(ctx context.Context, state string) error {
	m.fsmState = state
	return nil
}

// TestStoreBlock tests the block storage functionality
func TestStoreBlock(t *testing.T) {
	store := newMockStore()
	ctx := context.Background()

	// Create a test block
	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()), // nolint:gosec
		Bits:           model.NBit{},
		Nonce:          12345,
	}

	block := &model.Block{
		Header:           header,
		TransactionCount: 1,
		Height:           1,
	}

	// Store the block
	id, height, err := store.StoreBlock(ctx, block, "test-peer")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), id)
	assert.Equal(t, uint32(1), height)

	// Retrieve the block
	hash := block.Hash()
	retrievedBlock, retrievedHeight, err := store.GetBlock(ctx, hash)
	require.NoError(t, err)
	assert.Equal(t, block.Height, retrievedHeight)
	assert.Equal(t, block.Header.Version, retrievedBlock.Header.Version)
	assert.Equal(t, block.Header.HashPrevBlock, retrievedBlock.Header.HashPrevBlock)
	assert.Equal(t, block.Header.HashMerkleRoot, retrievedBlock.Header.HashMerkleRoot)
	assert.Equal(t, block.Header.Timestamp, retrievedBlock.Header.Timestamp)
	assert.Equal(t, block.Header.Bits, retrievedBlock.Header.Bits)
	assert.Equal(t, block.Header.Nonce, retrievedBlock.Header.Nonce)

	// Retrieve the block by height
	retrievedByHeight, err := store.GetBlockByHeight(ctx, block.Height)
	require.NoError(t, err)
	assert.Equal(t, block.Height, retrievedByHeight.Height)
}

// TestGetHeader tests the header retrieval functionality
func TestGetHeader(t *testing.T) {
	store := newMockStore()
	ctx := context.Background()

	// Create and store a test block
	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()), // nolint:gosec
		Bits:           model.NBit{},
		Nonce:          12345,
	}

	block := &model.Block{
		Header:           header,
		TransactionCount: 1,
		Height:           1,
	}

	_, _, err := store.StoreBlock(ctx, block, "test-peer")
	require.NoError(t, err)

	// Retrieve the header
	hash := block.Hash()
	retrievedHeader, err := store.GetHeader(ctx, hash)
	require.NoError(t, err)
	assert.Equal(t, header.Version, retrievedHeader.Version)
	assert.Equal(t, header.HashPrevBlock, retrievedHeader.HashPrevBlock)
	assert.Equal(t, header.HashMerkleRoot, retrievedHeader.HashMerkleRoot)
	assert.Equal(t, header.Timestamp, retrievedHeader.Timestamp)
	assert.Equal(t, header.Bits, retrievedHeader.Bits)
	assert.Equal(t, header.Nonce, retrievedHeader.Nonce)

	// Test retrieving non-existent header
	_, err = store.GetHeader(ctx, &chainhash.Hash{0xff})
	assert.Error(t, err)
}

// TestFSMState tests the FSM state storage and retrieval
func TestFSMState(t *testing.T) {
	store := newMockStore()
	ctx := context.Background()

	// Test initial state
	state, err := store.GetFSMState(ctx)
	require.NoError(t, err)
	assert.Empty(t, state)

	// Set state
	err = store.SetFSMState(ctx, "RUNNING")
	require.NoError(t, err)

	// Get state
	state, err = store.GetFSMState(ctx)
	require.NoError(t, err)
	assert.Equal(t, "RUNNING", state)
}

// TestHealth tests the health check functionality
func TestHealth(t *testing.T) {
	store := newMockStore()
	ctx := context.Background()

	// Test health check
	status, msg, err := store.Health(ctx, true)
	require.NoError(t, err)
	assert.Equal(t, 200, status)
	assert.Equal(t, "healthy", msg)
}

// TestDBEngine tests the database engine getter
func TestDBEngine(t *testing.T) {
	store := newMockStore()
	engine := store.GetDBEngine()
	assert.Equal(t, util.SQLEngine("sqlite"), engine)
}
