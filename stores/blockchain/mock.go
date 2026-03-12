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

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/usql"
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
	// BlockChainWork maps block hashes to their cumulative chain work (for difficulty calculations)
	BlockChainWork map[chainhash.Hash][]byte
	// BlockIsMined maps block hashes to their mined status for mined status checks
	BlockIsMined map[chainhash.Hash]bool
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
		Blocks:         map[chainhash.Hash]*model.Block{},
		BlockExists:    map[chainhash.Hash]bool{},
		BlockByHeight:  map[uint32]*model.Block{},
		BlockChainWork: map[chainhash.Hash][]byte{},
		BlockIsMined:   map[chainhash.Hash]bool{},
		state:          "IDLE",
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

// GetDB returns the underlying SQL database instance.
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

// GetBlockInChainByHeightHash retrieves a block at a specific height in a chain determined by the start hash.
func (m *MockStore) GetBlockInChainByHeightHash(ctx context.Context, height uint32, hash *chainhash.Hash) (*model.Block, bool, error) {
	if hash == nil {
		block, err := m.GetBlockByHeight(ctx, height)
		if err != nil {
			return nil, false, err
		}
		return block, false, nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	block, ok := m.Blocks[*hash]
	if !ok {
		return nil, false, errors.ErrBlockNotFound
	}

	// for tests that build chains where blocks are not linked by HashPrevBlock
	// (e.g. every block points to the zero hash). In that case, use the height index.
	if block.Header == nil || block.Header.HashPrevBlock == nil || block.Header.HashPrevBlock.IsEqual(&chainhash.Hash{}) {
		blockAtHeight, ok := m.BlockByHeight[height]
		if !ok {
			return nil, false, errors.ErrBlockNotFound
		}
		return blockAtHeight, false, nil
	}

	if block.Height < height {
		return nil, false, errors.ErrBlockNotFound
	}

	for block.Height > height {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}

		if block.Header == nil || block.Header.HashPrevBlock == nil {
			return nil, false, errors.ErrBlockNotFound
		}

		parent, parentOK := m.Blocks[*block.Header.HashPrevBlock]
		if !parentOK {
			return nil, false, errors.ErrBlockNotFound
		}

		block = parent
	}

	return block, false, nil
}

func (m *MockStore) GetBlockStats(ctx context.Context) (*model.BlockStats, error) {
	panic(implementMe)
}

func (m *MockStore) GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error) {
	panic(implementMe)
}

// GetBlockByID retrieves a block from the in-memory store by its unique ID.
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

// GetNextBlockID retrieves the next available block ID by finding the highest existing ID and incrementing it.
func (m *MockStore) GetNextBlockID(_ context.Context) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Find the highest existing block ID and return the next one
	maxID := uint64(0)
	for _, block := range m.Blocks {
		if uint64(block.ID) > maxID {
			maxID = uint64(block.ID)
		}
	}

	return maxID + 1, nil
}

func (m *MockStore) GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error) {
	panic(implementMe)
}

func (m *MockStore) GetLastNInvalidBlocks(ctx context.Context, n int64) ([]*model.BlockInfo, error) {
	panic(implementMe)
}

// GetSuitableBlock retrieves a suitable block for mining/difficulty calculation.
// This implements the blockchain.Store.GetSuitableBlock interface method.
//
// The method simulates the SQL implementation by:
// 1. Getting the block at the given hash
// 2. Getting its two ancestors (parent and grandparent)
// 3. Sorting them by timestamp
// 4. Returning the median block
//
// Parameters:
//   - ctx: Context for the operation
//   - blockHash: Hash of the block to start from
//
// Returns:
//   - *model.SuitableBlock: The median block from the set of 3 blocks
//   - error: Error if block not found or insufficient ancestors
func (m *MockStore) GetSuitableBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.SuitableBlock, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Get the block at the given hash
	block, exists := m.Blocks[*blockHash]
	if !exists {
		return nil, errors.NewBlockNotFoundError("block not found", blockHash)
	}

	// Collect 3 blocks: current, parent, grandparent
	candidates := make([]*model.SuitableBlock, 0, 3)

	// Add current block
	candidates = append(candidates, &model.SuitableBlock{
		Hash:      blockHash[:],
		Height:    block.Height,
		NBits:     block.Header.Bits.CloneBytes(),
		Time:      block.Header.Timestamp,
		ChainWork: m.BlockChainWork[*blockHash],
	})

	// Add parent if exists
	if block.Header.HashPrevBlock != nil && !block.Header.HashPrevBlock.IsEqual(&chainhash.Hash{}) {
		if parentBlock, exists := m.Blocks[*block.Header.HashPrevBlock]; exists {
			candidates = append(candidates, &model.SuitableBlock{
				Hash:      block.Header.HashPrevBlock[:],
				Height:    parentBlock.Height,
				NBits:     parentBlock.Header.Bits.CloneBytes(),
				Time:      parentBlock.Header.Timestamp,
				ChainWork: m.BlockChainWork[*block.Header.HashPrevBlock],
			})

			// Add grandparent if exists
			if parentBlock.Header.HashPrevBlock != nil && !parentBlock.Header.HashPrevBlock.IsEqual(&chainhash.Hash{}) {
				if grandparentBlock, exists := m.Blocks[*parentBlock.Header.HashPrevBlock]; exists {
					candidates = append(candidates, &model.SuitableBlock{
						Hash:      parentBlock.Header.HashPrevBlock[:],
						Height:    grandparentBlock.Height,
						NBits:     grandparentBlock.Header.Bits.CloneBytes(),
						Time:      grandparentBlock.Header.Timestamp,
						ChainWork: m.BlockChainWork[*parentBlock.Header.HashPrevBlock],
					})
				}
			}
		}
	}

	// If we don't have 3 blocks, use what we have
	if len(candidates) < 3 {
		// Pad with the same block if needed (for genesis or early blocks)
		for len(candidates) < 3 {
			candidates = append(candidates, candidates[len(candidates)-1])
		}
	}

	// Sort by timestamp using the same function as production
	util.SortForDifficultyAdjustment(candidates)

	// Return the median (middle) block
	return candidates[1], nil
}

// GetHashOfAncestorBlock retrieves the hash of an ancestor block at a specified depth.
// This implements the blockchain.Store.GetHashOfAncestorBlock interface method.
//
// The method walks back through the chain using parent block references,
// going back 'depth' blocks from the starting block.
//
// Parameters:
//   - ctx: Context for the operation
//   - blockHash: Hash of the starting block
//   - depth: Number of blocks to go back (144 for difficulty adjustment)
//
// Returns:
//   - *chainhash.Hash: Hash of the ancestor block
//   - error: Error if ancestor at specified depth doesn't exist
func (m *MockStore) GetHashOfAncestorBlock(ctx context.Context, blockHash *chainhash.Hash, depth int) (*chainhash.Hash, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	currentHash := blockHash
	for i := 0; i < depth; i++ {
		block, exists := m.Blocks[*currentHash]
		if !exists {
			return nil, errors.NewBlockNotFoundError("block not found while traversing ancestors", currentHash)
		}

		// Check if we've reached genesis
		if block.Header.HashPrevBlock == nil || block.Header.HashPrevBlock.IsEqual(&chainhash.Hash{}) {
			// Can't go back further
			return nil, errors.NewProcessingError("insufficient chain depth for ancestor at depth %d", depth)
		}

		currentHash = block.Header.HashPrevBlock
	}

	return currentHash, nil
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

	blockStoreOptions := options.ProcessStoreBlockOptions(opts...)

	m.Blocks[*block.Hash()] = block
	m.BlockByHeight[block.Height] = block
	m.BlockExists[*block.Hash()] = true

	if blockStoreOptions.MinedSet {
		// If the block is marked as mined, we do not update the best block
		// add this to the mock
	}

	if blockStoreOptions.SubtreesSet {
		// If the block is marked as having subtrees set, we do not update the best block
		// add this to the mock
	}

	if blockStoreOptions.Invalid {
		// If the block is marked as invalid, we do not update the best block
		// add this to the mock
	}

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
// Note: This implementation now checks if BestBlock is nil to prevent panics.
func (m *MockStore) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.BestBlock == nil {
		return nil, nil, errors.NewBlockNotFoundError("no best block set")
	}

	return m.BestBlock.Header, &model.BlockHeaderMeta{Height: m.BestBlock.Height}, nil
}

// GetBlockHeader retrieves a block header and its metadata by the block's hash.
func (m *MockStore) GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	block, ok := m.Blocks[*blockHash]
	if !ok {
		return nil, nil, errors.NewBlockNotFoundError(blockHash.String())
	}

	return block.Header, &model.BlockHeaderMeta{Height: block.Height}, nil
}

// GetBlockHeaders retrieves multiple block headers starting from a specific block hash.
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

// GetBlockHeadersFromTill retrieves block headers between two specified blocks.
func (m *MockStore) GetBlockHeadersFromTill(ctx context.Context, blockHashFrom *chainhash.Hash, blockHashTill *chainhash.Hash) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return []*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil
}

func (m *MockStore) GetForkedBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	panic(implementMe)
}

func (m *MockStore) GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	panic(implementMe)
}

// GetBlockHeadersByHeight retrieves block headers within a specified height range.
// This method returns headers and metadata for all blocks between startHeight and endHeight (inclusive).
//
// Parameters:
//   - ctx: Context for the operation (unused in this implementation)
//   - startHeight: The lower bound of the height range (inclusive)
//   - endHeight: The upper bound of the height range (inclusive)
//
// Returns:
//   - []*model.BlockHeader: Slice of block headers in ascending height order
//   - []*model.BlockHeaderMeta: Slice of metadata for the corresponding block headers
//   - error: Always nil in this implementation
func (m *MockStore) GetBlockHeadersByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Handle reverse range
	if startHeight > endHeight {
		return []*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil
	}

	headers := make([]*model.BlockHeader, 0, endHeight-startHeight+1)
	metas := make([]*model.BlockHeaderMeta, 0, endHeight-startHeight+1)

	// Iterate through the height range and collect blocks
	for height := startHeight; height <= endHeight; height++ {
		block, ok := m.BlockByHeight[height]
		if !ok {
			continue
		}

		headers = append(headers, block.Header)
		metas = append(metas, &model.BlockHeaderMeta{
			ID:        block.ID,
			Height:    block.Height,
			TxCount:   block.TransactionCount,
			BlockTime: block.Header.Timestamp,
		})
	}

	return headers, metas, nil
}

func (m *MockStore) GetBlocksByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.Block, error) {
	panic(implementMe)
}

func (m *MockStore) FindBlocksContainingSubtree(ctx context.Context, subtreeHash *chainhash.Hash, maxBlocks uint32) ([]*model.Block, error) {
	panic(implementMe)
}

func (m *MockStore) InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) ([]chainhash.Hash, error) {
	panic(implementMe)
}

func (m *MockStore) RevalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	panic(implementMe)
}

// GetBlockHeaderIDs retrieves block header IDs starting from a specific block hash.
func (m *MockStore) GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error) {
	return []uint32{}, nil
}

func (m *MockStore) GetState(ctx context.Context, key string) ([]byte, error) {
	panic(implementMe)
}

func (m *MockStore) SetState(ctx context.Context, key string, data []byte) error {
	panic(implementMe)
}

func (m *MockStore) GetBlockIsMined(_ context.Context, blockHash *chainhash.Hash) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if blockHash == nil {
		return false, nil
	}
	isMined, exists := m.BlockIsMined[*blockHash]
	if !exists {
		// Default to true for blocks that exist, false otherwise
		if m.BlockExists[*blockHash] {
			return true, nil
		}
		return false, nil
	}
	return isMined, nil
}

func (m *MockStore) SetBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	panic(implementMe)
}

func (m *MockStore) ClearBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	panic(implementMe)
}

func (m *MockStore) SetBlockProcessedAt(ctx context.Context, blockHash *chainhash.Hash, clear ...bool) error {
	panic(implementMe)
}

// GetBlocksMinedNotSet retrieves blocks that haven't been marked as mined.
func (m *MockStore) GetBlocksMinedNotSet(_ context.Context) ([]*model.Block, error) {
	return []*model.Block{}, nil
}

// SetBlockSubtreesSet marks a block's subtrees as processed.
func (m *MockStore) SetBlockSubtreesSet(ctx context.Context, blockHash *chainhash.Hash) error {
	return nil
}

// GetBlocksSubtreesNotSet retrieves blocks whose subtrees haven't been processed.
func (m *MockStore) GetBlocksSubtreesNotSet(ctx context.Context) ([]*model.Block, error) {
	return []*model.Block{}, nil
}

// SetBlockPersistedAt marks a block as persisted to blob storage.
func (m *MockStore) SetBlockPersistedAt(ctx context.Context, blockHash *chainhash.Hash) error {
	return nil
}

// GetBlocksNotPersisted retrieves blocks that haven't been persisted to blob storage yet.
func (m *MockStore) GetBlocksNotPersisted(ctx context.Context, limit int) ([]*model.Block, error) {
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
	return true, nil
}

func (m *MockStore) CheckBlockIsAncestorOfBlock(ctx context.Context, blockIDs []uint32, blockHash *chainhash.Hash) (bool, error) {
	return false, nil
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
