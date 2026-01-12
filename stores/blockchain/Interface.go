// Package blockchain provides interfaces and implementations for blockchain data storage and retrieval.
// It offers a comprehensive API for storing, retrieving, and managing blockchain data including blocks,
// headers, transactions, and chain state information.
//
// The package follows a layered architecture with a core interface (Store) that defines all blockchain
// operations, with implementations for different storage backends including SQL databases.
// It integrates with other components of the Teranode system while maintaining storage abstraction.
//
// Package Organization:
// - Interface.go: Defines the core Store interface that all implementations must satisfy
// - mock.go: Provides an in-memory implementation for testing purposes
// - sql/: Contains SQL-based implementations supporting PostgreSQL and SQLite
// - options/: Defines functional options for configuring blockchain operations
//
// Implementation Patterns:
// - Repository pattern: Abstracts storage operations behind a common interface
// - Functional options: Provides flexible configuration without breaking API compatibility
// - Caching strategies: Improves performance for frequently accessed data
// - Context-based operations: Supports cancellation and timeouts for all operations
//
// Key Features:
// - Block and header storage and retrieval by hash, height, or other criteria
// - Chain state tracking (best chain, orphans, invalid blocks)
// - Support for chain reorganization
// - Performance optimizations through multi-level caching
// - Transaction data management and indexing
// - Blockchain analysis and statistics
// - Health monitoring and diagnostics
package blockchain

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/usql"
)

// Store defines the interface for blockchain data storage and retrieval operations.
// It provides methods for managing blocks, headers, and blockchain state.
type Store interface {
	// Health checks the health status of the store.
	// Parameters:
	//   - ctx: Context for the operation
	//   - checkLiveness: Boolean flag to determine if liveness should be checked
	// Returns: status code, status message, and any error encountered
	Health(ctx context.Context, checkLiveness bool) (int, string, error)

	// GetDB returns the underlying SQL database instance.
	GetDB() *usql.DB

	// GetDBEngine returns the SQL engine type being used.
	GetDBEngine() util.SQLEngine

	// GetHeader retrieves a block header by its hash.
	// Parameters:
	//   - ctx: Context for the operation
	//   - blockHash: Hash of the block to retrieve
	// Returns: BlockHeader and any error encountered
	GetHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, error)

	// GetBlock retrieves a block by its hash.
	// Parameters:
	//   - ctx: Context for the operation
	//   - blockHash: Hash of the block to retrieve
	// Returns: Block, block height, and any error encountered
	GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, uint32, error)

	// GetBlocks retrieves multiple blocks starting from a specific hash.
	// Parameters:
	//   - ctx: Context for the operation
	//   - blockHash: Starting block hash
	//   - numberOfBlocks: Number of blocks to retrieve
	// Returns: Slice of blocks and any error encountered
	GetBlocks(ctx context.Context, blockHash *chainhash.Hash, numberOfBlocks uint32) ([]*model.Block, error)

	// GetBlockByHeight retrieves a block at a specific height.
	// Parameters:
	//   - ctx: Context for the operation
	//   - height: Block height
	// Returns: Block and any error encountered
	GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error)
	GetBlockByID(ctx context.Context, id uint64) (*model.Block, error)

	// GetNextBlockID retrieves the next available block ID.
	// Parameters:
	//   - ctx: Context for the operation
	// Returns: next block ID and any error encountered
	GetNextBlockID(ctx context.Context) (uint64, error)

	// GetBlockInChainByHeightHash retrieves a block at a specific height in a chain determined by the start hash. This is useful for getting the block at a given height in a chain that may have a different tip.
	// Parameters:
	//   - ctx: Context for the operation
	//   - height: Block height
	//   - startHash: Hash determining the chain
	// Returns: Block, boolean indicating success, and any error encountered
	GetBlockInChainByHeightHash(ctx context.Context, height uint32, startHash *chainhash.Hash) (*model.Block, bool, error)

	// GetBlockStats retrieves statistical information about blocks.
	// Parameters:
	//   - ctx: Context for the operation
	// Returns: BlockStats and any error encountered
	GetBlockStats(ctx context.Context) (*model.BlockStats, error)

	// GetBlockGraphData retrieves block graph data for a specific time period.
	// Parameters:
	//   - ctx: Context for the operation
	//   - periodMillis: Time period in milliseconds
	// Returns: BlockDataPoints and any error encountered
	GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error)

	// GetLastNBlocks retrieves the last N blocks from the chain.
	// Parameters:
	//   - ctx: Context for the operation
	//   - n: Number of blocks to retrieve
	//   - includeOrphans: Whether to include orphaned blocks
	//   - fromHeight: Starting height for retrieval
	// Returns: Slice of BlockInfo and any error encountered
	GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error)

	// GetLastNInvalidBlocks retrieves the last N blocks that were marked as invalid.
	// Parameters:
	//   - ctx: Context for the operation
	//   - n: Number of invalid blocks to retrieve
	// Returns: Slice of BlockInfo and any error encountered
	GetLastNInvalidBlocks(ctx context.Context, n int64) ([]*model.BlockInfo, error)

	// GetSuitableBlock retrieves a suitable block starting from the given hash.
	// Parameters:
	//   - ctx: Context for the operation
	//   - blockHash: Starting block hash
	// Returns: SuitableBlock and any error encountered
	GetSuitableBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.SuitableBlock, error)

	// GetHashOfAncestorBlock retrieves the hash of an ancestor block at a specific depth.
	// Parameters:
	//   - ctx: Context for the operation
	//   - blockHash: Starting block hash
	//   - depth: Depth of the ancestor
	// Returns: Hash of the ancestor block and any error encountered
	GetHashOfAncestorBlock(ctx context.Context, blockHash *chainhash.Hash, depth int) (*chainhash.Hash, error)

	// GetLatestBlockHeaderFromBlockLocator retrieves the latest block header from a block locator.
	// Parameters:
	//   - ctx: Context for the operation
	//   - bestBlockHash: The best block hash to start the search from, backwards through the blockchain
	//   - blockLocator: Slice of block hashes representing the locator
	// Returns: BlockHeader, BlockHeaderMeta, and any error encountered
	GetLatestBlockHeaderFromBlockLocator(ctx context.Context, bestBlockHash *chainhash.Hash, blockLocator []chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error)

	// GetBlockHeadersFromOldest retrieves block headers starting from the oldest block.
	// This method is used to retrieve headers in a chain from the oldest block to the newest.
	// Parameters:
	//   - ctx: Context for the operation
	//   - chainTipHash: Hash of the current chain tip, used to determine the valid chain
	//   - targetHash: Starting block hash
	//   - numberOfHeaders: Number of headers to retrieve
	// Returns: Slice of BlockHeaders, slice of BlockHeaderMetas, and any error
	GetBlockHeadersFromOldest(ctx context.Context, chainTipHash, targetHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)

	// GetBlockExists checks if a block exists in the store.
	// Parameters:
	//   - ctx: Context for the operation
	//   - blockHash: Hash of the block to check
	// Returns: Boolean indicating existence and any error encountered
	GetBlockExists(ctx context.Context, blockHash *chainhash.Hash) (bool, error)

	// GetBlockHeight retrieves the height of a block.
	// Parameters:
	//   - ctx: Context for the operation
	//   - blockHash: Hash of the block
	// Returns: Block height and any error encountered
	GetBlockHeight(ctx context.Context, blockHash *chainhash.Hash) (uint32, error)

	// StoreBlock stores a new block in the database.
	// Parameters:
	//   - ctx: Context for the operation
	//   - block: Block to store
	//   - peerID: ID of the peer that provided the block
	//   - opts: Optional store block options
	// Returns: Block ID, height, and any error encountered
	StoreBlock(ctx context.Context, block *model.Block, peerID string, opts ...options.StoreBlockOption) (ID uint64, height uint32, err error)

	// GetBestBlockHeader retrieves the header of the best block in the chain.
	// Parameters:
	//   - ctx: Context for the operation
	// Returns: BlockHeader, BlockHeaderMeta, and any error encountered
	GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error)

	// GetBlockHeader retrieves a block header by its hash.
	// Parameters:
	//   - ctx: Context for the operation
	//   - blockHash: Hash of the block
	// Returns: BlockHeader, BlockHeaderMeta, and any error encountered
	GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error)

	// GetBlockHeaders retrieves multiple block headers starting from a specific hash.
	// Parameters:
	//   - ctx: Context for the operation
	//   - blockHash: Starting block hash
	//   - numberOfHeaders: Number of headers to retrieve
	// Returns: Slice of BlockHeaders, slice of BlockHeaderMetas, and any error encountered
	GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)

	// GetBlockHeadersFromTill retrieves block headers between two blocks.
	// Parameters:
	//   - ctx: Context for the operation
	//   - blockHashFrom: Starting block hash
	//   - blockHashTill: Ending block hash
	// Returns: Slice of BlockHeaders, slice of BlockHeaderMetas, and any error encountered
	GetBlockHeadersFromTill(ctx context.Context, blockHashFrom *chainhash.Hash, blockHashTill *chainhash.Hash) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)

	// GetForkedBlockHeaders retrieves headers of forked blocks.
	// Parameters:
	//   - ctx: Context for the operation
	//   - blockHash: Starting block hash
	//   - numberOfHeaders: Number of headers to retrieve
	// Returns: Slice of BlockHeaders, slice of BlockHeaderMetas, and any error encountered
	GetForkedBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)

	// GetBlockHeadersFromHeight retrieves block headers starting from a specific height.
	// Parameters:
	//   - ctx: Context for the operation
	//   - height: Starting height
	//   - limit: Maximum number of headers to retrieve
	// Returns: Slice of BlockHeaders, slice of BlockHeaderMetas, and any error encountered
	GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)

	// GetBlockHeadersByHeight retrieves block headers between two heights.
	// Parameters:
	//   - ctx: Context for the operation
	//   - startHeight: Starting height
	//   - endHeight: Ending height
	// Returns: Slice of BlockHeaders, slice of BlockHeaderMetas, and any error encountered
	GetBlockHeadersByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)

	// GetBlocksByHeight retrieves full blocks between two heights.
	// Parameters:
	//   - ctx: Context for the operation
	//   - startHeight: Starting height (inclusive)
	//   - endHeight: Ending height (inclusive)
	// Returns: Slice of full Blocks and any error encountered
	GetBlocksByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.Block, error)

	// FindBlocksContainingSubtree finds all blocks that contain the specified subtree hash.
	// This method searches through the blockchain to find blocks whose subtree arrays contain
	// the given subtree hash. It's useful for merkle proof construction and subtree tracking.
	// Parameters:
	//   - ctx: Context for the operation
	//   - subtreeHash: Hash of the subtree to search for
	//   - maxBlocks: Maximum number of blocks to search (0 means no limit)
	// Returns: Slice of blocks containing the subtree and any error encountered
	FindBlocksContainingSubtree(ctx context.Context, subtreeHash *chainhash.Hash, maxBlocks uint32) ([]*model.Block, error)

	// InvalidateBlock marks a block as invalid.
	// Parameters:
	//   - ctx: Context for the operation
	//   - blockHash: Hash of the block to invalidate
	// Returns: Any error encountered
	InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) ([]chainhash.Hash, error)

	// RevalidateBlock marks a previously invalidated block as valid.
	// Parameters:
	//   - ctx: Context for the operation
	//   - blockHash: Hash of the block to revalidate
	// Returns: Any error encountered
	RevalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error

	// GetBlockHeaderIDs retrieves block header IDs starting from a specific hash.
	// Parameters:
	//   - ctx: Context for the operation
	//   - blockHash: Starting block hash
	//   - numberOfHeaders: Number of header IDs to retrieve
	// Returns: Slice of header IDs and any error encountered
	GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error)

	// GetState retrieves state data for a given key.
	// Parameters:
	//   - ctx: Context for the operation
	//   - key: State key to retrieve
	// Returns: State data and any error encountered
	GetState(ctx context.Context, key string) ([]byte, error)

	// SetState stores state data for a given key.
	// Parameters:
	//   - ctx: Context for the operation
	//   - key: State key to set
	//   - data: State data to store
	// Returns: Any error encountered
	SetState(ctx context.Context, key string, data []byte) error
	GetBlockIsMined(ctx context.Context, blockHash *chainhash.Hash) (bool, error)

	// SetBlockMinedSet marks a block as mined.
	// Parameters:
	//   - ctx: Context for the operation
	//   - blockHash: Hash of the block to mark
	// Returns: Any error encountered
	SetBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error

	// ClearBlockMinedSet resets the mined_set flag to false for a block,
	// triggering background job processing to update transaction states.
	// Used during fork handling to re-process transactions on orphaned chains.
	// Parameters:
	//   - ctx: Context for the operation
	//   - blockHash: Hash of the block to reset
	// Returns: Any error encountered
	ClearBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error

	// GetBlocksMinedNotSet retrieves blocks that haven't been marked as mined.
	// Parameters:
	//   - ctx: Context for the operation
	// Returns: Slice of unmined blocks and any error encountered
	GetBlocksMinedNotSet(ctx context.Context) ([]*model.Block, error)

	// SetBlockSubtreesSet marks a block's subtrees as processed.
	// Parameters:
	//   - ctx: Context for the operation
	//   - blockHash: Hash of the block to mark
	// Returns: Any error encountered
	SetBlockSubtreesSet(ctx context.Context, blockHash *chainhash.Hash) error

	// GetBlocksSubtreesNotSet retrieves blocks whose subtrees haven't been processed.
	// Parameters:
	//   - ctx: Context for the operation
	// Returns: Slice of blocks with unprocessed subtrees and any error encountered
	GetBlocksSubtreesNotSet(ctx context.Context) ([]*model.Block, error)

	// GetBlocksByTime retrieves blocks within a specified time range.
	// Parameters:
	//   - ctx: Context for the operation
	//   - fromTime: Start time
	//   - toTime: End time
	// Returns: Slice of block data and any error encountered
	GetBlocksByTime(ctx context.Context, fromTime, toTime time.Time) ([][]byte, error)

	// CheckBlockIsInCurrentChain checks if blocks are in the current chain.
	// Parameters:
	//   - ctx: Context for the operation
	//   - blockIDs: Slice of block IDs to check
	// Returns: Boolean indicating if blocks are in current chain and any error encountered
	CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error)

	// GetChainTips retrieves information about all known tips in the block tree.
	// This method finds all blocks that have no children (tips) and determines their
	// relationship to the main chain, including branch lengths and validation status.
	// Parameters:
	//   - ctx: Context for the operation
	// Returns: Slice of ChainTip structures and any error encountered
	GetChainTips(ctx context.Context) ([]*model.ChainTip, error)

	// GetFSMState retrieves the current FSM state.
	// Parameters:
	//   - ctx: Context for the operation
	// Returns: FSM state string and any error encountered
	GetFSMState(ctx context.Context) (string, error)

	// SetFSMState sets the FSM state.
	// Parameters:
	//   - ctx: Context for the operation
	//   - state: State to set
	// Returns: Any error encountered
	SetFSMState(ctx context.Context, state string) error

	// LocateBlockHeaders retrieves block headers using a locator and stop hash.
	// Legacy endpoint.
	// Parameters:
	//   - ctx: Context for the operation
	//   - locator: Slice of block hashes for location
	//   - hashStop: Hash to stop at
	//   - maxHashes: Maximum number of hashes to return
	// Returns: Slice of block headers and any error encountered
	LocateBlockHeaders(ctx context.Context, locator []*chainhash.Hash, hashStop *chainhash.Hash, maxHashes uint32) ([]*model.BlockHeader, error)

	// ExportBlockDB exports the block database starting from a specific hash.
	// Legacy endpoint.
	// Parameters:
	//   - ctx: Context for the operation
	//   - hash: Starting block hash
	// Returns: File containing the exported data and any error encountered
	ExportBlockDB(ctx context.Context, hash *chainhash.Hash) (*file.File, error)

	// SetBlockProcessedAt updates the processed_at timestamp for a block.
	// Parameters:
	//   - ctx: Context for the operation
	//   - blockHash: Hash of the block to update
	//   - clear: Boolean flag to determine if the timestamp should be cleared
	// Returns: Any error encountered
	SetBlockProcessedAt(ctx context.Context, blockHash *chainhash.Hash, clear ...bool) error

	// SetBlockPersistedAt updates the persisted_at timestamp for a block.
	// Parameters:
	//   - ctx: Context for the operation
	//   - blockHash: Hash of the block to update
	// Returns: Any error encountered
	SetBlockPersistedAt(ctx context.Context, blockHash *chainhash.Hash) error

	// GetBlocksNotPersisted retrieves blocks that haven't been persisted yet.
	// Parameters:
	//   - ctx: Context for the operation
	//   - limit: Maximum number of blocks to return
	// Returns: Slice of unpersisted blocks and any error encountered
	GetBlocksNotPersisted(ctx context.Context, limit int) ([]*model.Block, error)
}
