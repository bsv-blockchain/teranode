// Package blockchain provides interfaces and implementations for blockchain data storage and retrieval.
//
// This file implements a mock version of the Store interface for testing purposes.
// The MockStore provides an in-memory implementation that simulates blockchain
// storage operations without requiring a database connection. It maintains simple
// maps for block lookups and chain state, making it suitable for unit testing
// components that depend on blockchain storage.
//
// The mock implementation is thread-safe and provides basic functionality for
// storing and retrieving blocks, checking existence, and managing chain state.
// Some methods are fully implemented while others use a placeholder that panics
// with "implement me" when called, allowing for incremental implementation as needed.
package blockchain

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/stores/blob/file"
	"github.com/bitcoin-sv/teranode/stores/blockchain/options"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/usql"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// MockStore provides an in-memory implementation of the Store interface for testing purposes.
// It maintains a simplified blockchain representation with maps for block lookups and state tracking.
// The implementation is thread-safe with a read-write mutex protecting all operations.
type MockStore struct {
	// Blocks maps block hashes to block objects for direct hash-based lookups
	Blocks map[chainhash.Hash]*model.Block
	// BlockExists maps block hashes to boolean existence flags for quick existence checks
	BlockExists map[chainhash.Hash]bool
	// BlockByHeight maps heights to block objects for height-based lookups
	BlockByHeight map[uint32]*model.Block
	// BestBlock represents the current best block in the chain (highest height)
	BestBlock *model.Block
	// state tracks the current state of the mock store (e.g., IDLE)
	state string
	// mu provides thread-safe access to all MockStore fields
	mu sync.RWMutex
}

// NewMockStore creates and initializes a new MockStore instance with empty maps and default state.
// This factory function is the recommended way to instantiate a MockStore for testing.
//
// Returns:
//   - *MockStore: A new, initialized MockStore instance with empty block maps and IDLE state
func NewMockStore() *MockStore {
	return &MockStore{
		Blocks:        map[chainhash.Hash]*model.Block{},
		BlockExists:   map[chainhash.Hash]bool{},
		BlockByHeight: map[uint32]*model.Block{},
		state:         "IDLE",
	}
}

// implementMe is a constant used as a placeholder for methods that are not yet implemented
// in the MockStore. Methods using this constant will panic with this message when called.
const implementMe = "implement me"

// Health checks the health status of the mock store.
// This implementation always returns a successful status since the mock store
// is an in-memory implementation with no external dependencies to check.
//
// Parameters:
//   - ctx: Context for the operation (unused in this implementation)
//   - checkLiveness: Boolean flag to determine if liveness should be checked (unused)
//
// Returns:
//   - int: HTTP status code (always http.StatusOK)
//   - string: Status message (always "OK")
//   - error: Any error encountered (always nil)
func (m *MockStore) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return http.StatusOK, "OK", nil
}

func (m *MockStore) GetDB() *usql.DB {
	return nil
}

func (m *MockStore) GetDBEngine() util.SQLEngine {
	panic(implementMe)
}

func (m *MockStore) GetHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, error) {
	panic(implementMe)
}

// GetBlock retrieves a complete block from the in-memory store by its hash.
// This implements the blockchain.Store.GetBlock interface method.
//
// The method uses a read lock to ensure thread safety while accessing the Blocks map.
// If the block is not found in the map, it returns a predefined BlockNotFound error.
//
// Parameters:
//   - ctx: Context for the operation (unused in this implementation)
//   - blockHash: The unique hash identifier of the block to retrieve
//
// Returns:
//   - *model.Block: The complete block data if found
//   - uint32: The height of the block in the blockchain
//   - error: ErrBlockNotFound if the block is not in the store, nil otherwise
func (m *MockStore) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, uint32, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	block, ok := m.Blocks[*blockHash]
	if !ok {
		return nil, 0, errors.ErrBlockNotFound
	}

	return block, block.Height, nil
}

func (m *MockStore) GetBlocks(ctx context.Context, blockHash *chainhash.Hash, numberOfBlocks uint32) ([]*model.Block, error) {
	panic(implementMe)
}

// GetBlockByHeight retrieves a block from the in-memory store by its height.
// This implements the blockchain.Store.GetBlockByHeight interface method.
//
// The method uses a read lock to ensure thread safety while accessing the BlockByHeight map.
// If no block exists at the specified height, it returns a predefined BlockNotFound error.
//
// Parameters:
//   - ctx: Context for the operation (unused in this implementation)
//   - height: The block height to retrieve
//
// Returns:
//   - *model.Block: The complete block data if found at the specified height
//   - error: ErrBlockNotFound if no block exists at the height, nil otherwise
func (m *MockStore) GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	block, ok := m.BlockByHeight[height]
	if !ok {
		return nil, errors.ErrBlockNotFound
	}

	return block, nil
}

func (m *MockStore) GetBlockInChainByHeightHash(ctx context.Context, height uint32, hash *chainhash.Hash) (*model.Block, bool, error) {
	block, err := m.GetBlockByHeight(ctx, height)
	if err != nil {
		return nil, false, err
	}

	return block, false, nil
}

func (m *MockStore) GetBlockStats(ctx context.Context) (*model.BlockStats, error) {
	panic(implementMe)
}

func (m *MockStore) GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error) {
	panic(implementMe)
}

func (m *MockStore) GetBlockByID(_ context.Context, id uint64) (*model.Block, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, block := range m.Blocks {
		if uint64(block.ID) == id { // golint:nolint
			return block, nil
		}
	}

	return nil, context.DeadlineExceeded
}

func (m *MockStore) GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error) {
	panic(implementMe)
}

func (m *MockStore) GetLastNInvalidBlocks(ctx context.Context, n int64) ([]*model.BlockInfo, error) {
	panic(implementMe)
}

func (m *MockStore) GetSuitableBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.SuitableBlock, error) {
	panic(implementMe)
}

func (m *MockStore) GetHashOfAncestorBlock(ctx context.Context, blockHash *chainhash.Hash, depth int) (*chainhash.Hash, error) {
	panic(implementMe)
}

func (m *MockStore) GetLatestBlockHeaderFromBlockLocator(ctx context.Context, bestBlockHash *chainhash.Hash, blockLocator []chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	panic(implementMe)
}

func (m *MockStore) GetBlockHeadersFromOldest(ctx context.Context, chainTipHash, targetHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	panic(implementMe)
}

// GetBlockExists checks if a block exists in the in-memory store.
// This implements the blockchain.Store.GetBlockExists interface method.
//
// The method uses a read lock to ensure thread safety while accessing the BlockExists map.
// It checks if the given hash exists in the BlockExists map and returns its value.
// If the hash is not found in the map, it returns false without an error.
//
// Parameters:
//   - ctx: Context for the operation (unused in this implementation, hence the underscore)
//   - blockHash: The unique hash identifier of the block to check
//
// Returns:
//   - bool: True if the block exists, false otherwise
//   - error: Always nil in this implementation
func (m *MockStore) GetBlockExists(_ context.Context, blockHash *chainhash.Hash) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	exists, ok := m.BlockExists[*blockHash]

	if !ok {
		return false, nil
	}

	return exists, nil
}

func (m *MockStore) GetBlockHeight(ctx context.Context, blockHash *chainhash.Hash) (uint32, error) {
	panic(implementMe)
}

// StoreBlock stores a new block in the in-memory maps.
// This implements the blockchain.Store.StoreBlock interface method.
//
// The method uses a write lock to ensure thread safety while updating multiple maps.
// It updates the Blocks, BlockByHeight, and BlockExists maps with the new block data.
// If the new block has a greater height than the current BestBlock or if BestBlock is nil,
// the method also updates the BestBlock reference.
//
// Parameters:
//   - ctx: Context for the operation (unused in this implementation)
//   - block: The block object to store
//   - peerID: ID of the peer that provided the block (unused in this implementation)
//   - opts: Optional store block options (unused in this implementation)
//
// Returns:
//   - uint64: Block ID (uses block height as ID in this implementation)
//   - uint32: Block height
//   - error: Always nil in this implementation
func (m *MockStore) StoreBlock(ctx context.Context, block *model.Block, peerID string, opts ...options.StoreBlockOption) (uint64, uint32, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.Blocks[*block.Hash()] = block
	m.BlockByHeight[block.Height] = block
	m.BlockExists[*block.Hash()] = true

	if m.BestBlock == nil || block.Height > m.BestBlock.Height {
		m.BestBlock = block
	}

	return uint64(block.Height), block.Height, nil
}

// GetBestBlockHeader retrieves the header of the block at the tip of the best chain.
// This implements the blockchain.Store.GetBestBlockHeader interface method.
//
// The method uses a read lock to ensure thread safety while accessing the BestBlock field.
// It returns the header from the current BestBlock along with a minimal BlockHeaderMeta
// containing just the block height.
//
// Parameters:
//   - ctx: Context for the operation (unused in this implementation)
//
// Returns:
//   - *model.BlockHeader: The header of the best block in the chain
//   - *model.BlockHeaderMeta: Minimal metadata including just the height
//   - error: Always nil in this implementation
//
// Note: This implementation does not check if BestBlock is nil, which could cause a panic.
// In a production implementation, this should be handled.
func (m *MockStore) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.BestBlock.Header, &model.BlockHeaderMeta{Height: m.BestBlock.Height}, nil
}

func (m *MockStore) GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	block, ok := m.Blocks[*blockHash]
	if !ok {
		return nil, nil, errors.NewBlockNotFoundError(blockHash.String())
	}

	return block.Header, &model.BlockHeaderMeta{Height: block.Height}, nil
}

func (m *MockStore) GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	headers := make([]*model.BlockHeader, 0, numberOfHeaders)
	metas := make([]*model.BlockHeaderMeta, 0, numberOfHeaders)

	currentHash := blockHash
	for i := uint64(0); i < numberOfHeaders; i++ {
		block, ok := m.Blocks[*currentHash]
		if !ok {
			break
		}

		headers = append(headers, block.Header)
		metas = append(metas, &model.BlockHeaderMeta{
			ID:        block.ID,
			Height:    block.Height,
			TxCount:   block.TransactionCount,
			BlockTime: block.Header.Timestamp,
		})

		currentHash = block.Header.HashPrevBlock
	}

	return headers, metas, nil
}

func (m *MockStore) GetBlockHeadersFromTill(ctx context.Context, blockHashFrom *chainhash.Hash, blockHashTill *chainhash.Hash) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return []*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil
}

func (m *MockStore) GetForkedBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	panic(implementMe)
}

func (m *MockStore) GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	panic(implementMe)
}

func (m *MockStore) GetBlockHeadersByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	panic(implementMe)
}

func (m *MockStore) InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	panic(implementMe)
}

func (m *MockStore) RevalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	panic(implementMe)
}

func (m *MockStore) GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error) {
	return []uint32{}, nil
}

func (m *MockStore) GetState(ctx context.Context, key string) ([]byte, error) {
	panic(implementMe)
}

func (m *MockStore) SetState(ctx context.Context, key string, data []byte) error {
	panic(implementMe)
}

func (m *MockStore) GetBlockIsMined(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
	panic("implement me")
}

func (m *MockStore) SetBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	panic(implementMe)
}

func (m *MockStore) SetBlockProcessedAt(ctx context.Context, blockHash *chainhash.Hash, clear ...bool) error {
	panic(implementMe)
}

func (m *MockStore) GetBlocksMinedNotSet(_ context.Context) ([]*model.Block, error) {
	return []*model.Block{}, nil
}

func (m *MockStore) SetBlockSubtreesSet(ctx context.Context, blockHash *chainhash.Hash) error {
	return nil
}

func (m *MockStore) GetBlocksSubtreesNotSet(ctx context.Context) ([]*model.Block, error) {
	return []*model.Block{}, nil
}

func (m *MockStore) GetBlocksByTime(ctx context.Context, fromTime, toTime time.Time) ([][]byte, error) {
	panic(implementMe)
}

func (m *MockStore) LocateBlockHeaders(ctx context.Context, locator []*chainhash.Hash, hashStop *chainhash.Hash, maxHashes uint32) ([]*model.BlockHeader, error) {
	panic(implementMe)
}

func (m *MockStore) ExportBlockDB(ctx context.Context, hash *chainhash.Hash) (*file.File, error) {
	panic(implementMe)
}

func (m *MockStore) CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error) {
	panic(implementMe)
}

func (m *MockStore) GetChainTips(ctx context.Context) ([]*model.ChainTip, error) {
	panic(implementMe)
}

func (m *MockStore) SetFSMState(ctx context.Context, fsmState string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.state = fsmState
	return nil
}

func (m *MockStore) GetFSMState(ctx context.Context) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.state, nil
}
