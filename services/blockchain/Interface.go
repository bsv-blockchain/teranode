// Package blockchain provides functionality for managing the Bitcoin blockchain within the Teranode system.
//
// The blockchain package is responsible for maintaining the blockchain data structure and state,
// processing new blocks, and providing access to blockchain data through well-defined interfaces.
// This file specifically defines the client interfaces that enable other services to interact
// with the blockchain service in a decoupled manner.
package blockchain

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
)

// ClientI defines the interface for blockchain client operations.
//
// This interface abstracts the communication with the blockchain service, allowing clients
// to perform operations like adding blocks, retrieving blockchain data, and monitoring the
// blockchain state without directly coupling to the underlying implementation details.
//
// The interface provides methods for:
// - Health checking and monitoring
// - Block addition and retrieval
// - Blockchain statistics and visualization data
// - Notification handling
// - Mining support operations
//
// Implementations of this interface handle the communication details (e.g., gRPC, direct calls)
// while presenting a consistent API to clients.
type ClientI interface {
	// Health checks the health status of the blockchain service.
	//
	// This method performs health checks for both liveness (whether the service is running)
	// and readiness (whether the service and its dependencies are ready to accept requests).
	// The behavior is determined by the checkLiveness parameter.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - checkLiveness: If true, checks only internal service state; if false, checks dependencies too
	//
	// Returns:
	// - HTTP status code (200 for healthy, 503 for unhealthy)
	// - Human-readable status message with health details
	// - Error if the health check encounters an unexpected failure
	Health(ctx context.Context, checkLiveness bool) (int, string, error)

	// AddBlock adds a new block to the blockchain.
	//
	// This method submits a new block to be added to the blockchain, converting the
	// internal model representation to the format required by the blockchain service.
	// It handles serialization of the block data, including header, transactions, and metadata.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - block: The block to be added, containing header, transactions, and metadata
	// - peerID: Identifier of the peer that provided this block (for tracking purposes)
	// - opts: Optional parameters to customize block storage behavior (e.g., mined status, invalid flag)
	//
	// Returns:
	// - Error if the block addition fails, nil on success
	AddBlock(ctx context.Context, block *model.Block, peerID string, opts ...options.StoreBlockOption) error

	// GetNextBlockID retrieves the next available block ID.
	//
	// This method fetches the next block ID that will be assigned to a new block when it is
	// added to the blockchain. This is useful for clients that need to know the upcoming
	// block ID for tracking or logging purposes.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	//
	// Returns:
	// - The next block ID as a uint64
	// - Error if the retrieval fails
	GetNextBlockID(ctx context.Context) (uint64, error)

	// SendNotification broadcasts a notification to subscribers.
	//
	// This method publishes a notification message to all active subscribers of the
	// blockchain service. Notifications can include events like new blocks, state changes,
	// or other significant blockchain events.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - notification: The notification message to broadcast
	//
	// Returns:
	// - Error if the notification dispatch fails, nil on success
	SendNotification(ctx context.Context, notification *blockchain_api.Notification) error

	// GetBlock retrieves a block by its hash.
	//
	// This method fetches a complete Bitcoin block identified by its hash. The returned
	// block includes the header, transactions, and all associated metadata.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - blockHash: The hash identifying the target block
	//
	// Returns:
	// - A complete model.Block if found
	// - Error if the block retrieval fails or if no block exists with that hash
	GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, error)

	// GetBlocks retrieves multiple blocks starting from a specific hash.
	//
	// This method fetches a sequence of Bitcoin blocks, starting from the block
	// identified by the provided hash. It allows retrieving a batch of consecutive
	// blocks in a single call, which is more efficient than making multiple GetBlock calls.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - blockHash: The hash of the starting block
	// - numberOfBlocks: Maximum number of blocks to retrieve
	//
	// Returns:
	// - An array of model.Block objects in chain order
	// - Error if the block retrieval fails
	GetBlocks(ctx context.Context, blockHash *chainhash.Hash, numberOfBlocks uint32) ([]*model.Block, error)

	// GetBlockByHeight retrieves a block at a specific height.
	//
	// This method fetches a Bitcoin block at the specified height in the blockchain
	// (distance from the genesis block). This is useful when you need to access a block
	// at a specific position in the chain rather than by its hash.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - height: The target block height (0 = genesis block)
	//
	// Returns:
	// - A complete model.Block if a block exists at the specified height
	// - Error if the block retrieval fails or if no block exists at that height
	GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error)

	// GetBlockByID retrieves a block by its ID.
	//
	// This method fetches a Bitcoin block using its internal database ID. The ID is a
	// unique identifier assigned by the blockchain service when the block is stored,
	// and is different from the block hash or height.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - id: The database ID of the target block
	//
	// Returns:
	// - A complete model.Block if found
	// - Error if the block retrieval fails or if no block exists with that ID
	GetBlockByID(ctx context.Context, id uint64) (*model.Block, error)

	// GetBlockStats retrieves statistical information about the blockchain.
	//
	// This method collects and returns aggregate statistics about the current state of the
	// blockchain, including information like current height, total blocks, difficulty,
	// and other relevant metrics. This provides a high-level overview of the blockchain
	// state without needing to retrieve individual blocks.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	//
	// Returns:
	// - BlockStats structure containing aggregate blockchain statistics
	// - Error if statistics retrieval fails
	GetBlockStats(ctx context.Context) (*model.BlockStats, error)

	// GetBlockGraphData retrieves data points for blockchain visualization.
	//
	// This method provides time-series data for visualizing blockchain metrics over time,
	// such as block rates, sizes, or transaction counts. The data is sampled according to
	// the specified period to provide an appropriate resolution for graphing.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - periodMillis: Time period in milliseconds between data points
	//
	// Returns:
	// - BlockDataPoints structure containing time-series data for visualization
	// - Error if data retrieval fails
	GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error)

	// GetLastNBlocks retrieves the most recent N blocks.
	//
	// This method fetches the most recent blocks in the blockchain, with options to include
	// orphaned blocks and start from a specific height. This is useful for displaying
	// recent blockchain activity or for analysis of recent blocks.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - n: Number of blocks to retrieve
	// - includeOrphans: Whether to include orphaned/invalid blocks in the results
	// - fromHeight: Starting height to retrieve blocks from (0 means from the tip)
	//
	// Returns:
	// - Array of BlockInfo structures with summarized block information
	// - Error if block retrieval fails
	GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error)

	// GetLastNInvalidBlocks retrieves the most recent N blocks that have been marked as invalid.
	//
	// This method fetches the most recent blocks that have been marked as invalid, which can
	// be useful for monitoring and debugging purposes. Invalid blocks are those that were
	// rejected due to consensus rule violations or other validation failures.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - n: Number of invalid blocks to retrieve
	//
	// Returns:
	// - Array of BlockInfo structures with summarized information about invalid blocks
	// - Error if block retrieval fails
	GetLastNInvalidBlocks(ctx context.Context, n int64) ([]*model.BlockInfo, error)

	// GetSuitableBlock finds a suitable block for mining purposes.
	//
	// This method identifies a block that is suitable as a base for mining operations,
	// typically finding the best block to build upon in the current blockchain state.
	// It considers factors like chain position, difficulty, and validity.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - blockHash: Hash of the block to evaluate or start search from (may be nil to use best block)
	//
	// Returns:
	// - SuitableBlock structure containing information needed for mining
	// - Error if suitable block identification fails
	GetSuitableBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.SuitableBlock, error)

	// GetHashOfAncestorBlock retrieves the hash of an ancestor block at a specific depth.
	//
	// This method traverses the blockchain backward from a specified block to find the hash
	// of an ancestor block at the given depth. This is useful for identifying blocks a specific
	// number of confirmations back from a reference point.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - hash: Hash of the starting block to traverse back from
	// - depth: Number of blocks to go back in the chain (1 = immediate parent)
	//
	// Returns:
	// - Hash of the ancestor block at the specified depth
	// - Error if ancestor lookup fails or if depth exceeds chain length
	GetHashOfAncestorBlock(ctx context.Context, hash *chainhash.Hash, depth int) (*chainhash.Hash, error)

	// GetNextWorkRequired calculates the required proof of work for the next block.
	//
	// This method computes the difficulty target (nBits) that should be used for the next
	// block in the chain based on the difficulty adjustment algorithm. It uses the hash
	// parameter to identify the current tip of the chain to base this calculation on.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - hash: Hash of the current tip block to base the calculation on
	//
	// Returns:
	// - NBit structure containing the difficulty target for the next block
	// - Error if the calculation fails
	GetNextWorkRequired(ctx context.Context, hash *chainhash.Hash, currentBlockTime int64) (*model.NBit, error)

	// GetBlockExists checks if a block exists in the blockchain.
	//
	// This method performs a lightweight existence check for a block with the specified hash,
	// without retrieving the full block data. This is useful for efficient validation of
	// whether a block is already known to the blockchain service.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - blockHash: Hash of the block to check for existence
	//
	// Returns:
	// - Boolean indicating whether the block exists (true) or not (false)
	// - Error if the check operation fails
	GetBlockExists(ctx context.Context, blockHash *chainhash.Hash) (bool, error)

	// GetBestBlockHeader retrieves the current best block header.
	//
	// This method returns the header of the current tip of the main chain, which represents
	// the most recent valid block with the highest cumulative work. This is a lightweight
	// alternative to retrieving the full best block.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	//
	// Returns:
	// - BlockHeader containing the header data of the best block
	// - BlockHeaderMeta containing additional metadata about the header
	// - Error if the retrieval fails
	GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error)

	// GetBlockHeader retrieves a specific block header.
	//
	// This method fetches just the header portion of a block identified by its hash, without
	// retrieving the full block data including transactions. This is a lightweight operation
	// useful for validation and chain navigation purposes.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - blockHash: Hash of the block whose header should be retrieved
	//
	// Returns:
	// - BlockHeader containing the header data
	// - BlockHeaderMeta containing additional metadata about the header
	// - Error if the header retrieval fails
	GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error)

	// GetBlockHeaders retrieves multiple block headers.
	//
	// This method fetches a sequence of block headers starting from the specified hash,
	// retrieving up to the requested number of headers. This is useful for efficiently
	// validating portions of the blockchain without transferring full block data.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - blockHash: Hash of the starting block
	// - numberOfHeaders: Maximum number of headers to retrieve
	//
	// Returns:
	// - Array of BlockHeader objects in chain order
	// - Array of corresponding BlockHeaderMeta objects with additional metadata
	// - Error if the header retrieval fails
	GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)

	// GetBlockHeadersToCommonAncestor retrieves headers from target hash back to a common ancestor.
	//
	// This method fetches block headers starting from the target hash and moving backward
	// until it finds a common ancestor with one of the provided locator hashes. This is
	// essential for blockchain synchronization between nodes that may have diverged.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - hashTarget: Hash of the starting block to trace back from
	// - blockLocatorHashes: Array of checkpoint hashes to find a common ancestor with
	//
	// Returns:
	// - Array of BlockHeader objects from target to common ancestor (inclusive)
	// - Array of corresponding BlockHeaderMeta objects with additional metadata
	// - Error if the header retrieval or ancestor finding fails
	GetBlockHeadersToCommonAncestor(ctx context.Context, hashTarget *chainhash.Hash, blockLocatorHashes []*chainhash.Hash, maxHeaders uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)

	// GetBlockHeadersFromCommonAncestor retrieves headers from a common ancestor to a target hash.
	//
	// This method fetches block headers starting from a common ancestor identified in the block locator
	// on the chain that contains the target hash, moving forward to the target hash.
	// This is useful for retrieving headers in a forward direction from a known point in the chain.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - hashTarget: Hash of the target block to retrieve headers up to
	// - blockLocatorHashes: Array of hashes representing the block locator to find the common ancestor
	// - maxHeaders: Maximum number of headers to retrieve
	//
	// Returns:
	// - Array of BlockHeader objects from common ancestor to target (inclusive)
	// - Array of corresponding BlockHeaderMeta objects with additional metadata
	// - Error if the header retrieval or ancestor finding fails
	GetBlockHeadersFromCommonAncestor(ctx context.Context, hashTarget *chainhash.Hash, blockLocatorHashes []chainhash.Hash, maxHeaders uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)

	// GetLatestBlockHeaderFromBlockLocator retrieves the latest block header from a block locator.
	//
	// This method finds the latest block header that can be reached from a given block locator,
	// starting from the best block hash. It traverses the blockchain using the provided locator
	// hashes to find the most recent header that is part of the main chain.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - bestBlockHash: Hash of the current best block to start the search from
	// - blockLocator: Array of hashes representing the block locator to traverse
	//
	// Returns:
	// - BlockHeader containing the latest header found
	// - BlockHeaderMeta containing additional metadata about the header
	// - Error if the header retrieval fails or if no valid header is found in the locator
	GetLatestBlockHeaderFromBlockLocator(ctx context.Context, bestBlockHash *chainhash.Hash, blockLocator []chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error)

	// GetBlockHeadersFromOldest retrieves block headers starting from the oldest block.
	//
	// This method fetches a sequence of block headers starting from the specified hash,
	// moving forward in the chain order. This is useful for retrieving headers in ascending
	// order, such as when processing blocks from the genesis block onward.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - chainTipHash: Hash of the current chain tip to ensure the blocks are on the same chain
	// - targetHash: Hash of the starting block (inclusive)
	// - numberOfHeaders: Maximum number of headers to retrieve
	//
	// Returns:
	// - Array of BlockHeader objects in ascending order
	// - Array of corresponding BlockHeaderMeta objects with additional metadata
	// - Error if the header retrieval fails or if the blocks aren't on the same chain
	GetBlockHeadersFromOldest(ctx context.Context, chainTipHash, targetHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)

	// GetBlockHeadersFromTill retrieves block headers between two blocks.
	//
	// This method fetches a sequence of block headers starting from one specified hash
	// and ending at another hash, inclusive of both endpoints. This allows efficient
	// retrieval of a specific section of the blockchain's headers.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - blockHashFrom: Hash of the starting block (inclusive)
	// - blockHashTill: Hash of the ending block (inclusive)
	//
	// Returns:
	// - Array of BlockHeader objects in chain order
	// - Array of corresponding BlockHeaderMeta objects with additional metadata
	// - Error if the header retrieval fails or if the blocks aren't on the same chain
	GetBlockHeadersFromTill(ctx context.Context, blockHashFrom *chainhash.Hash, blockHashTill *chainhash.Hash) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)

	// GetBlockHeadersFromHeight retrieves block headers from a specific height.
	//
	// This method fetches a sequence of block headers starting from the specified height
	// and continuing for the specified limit number of blocks. This allows efficient
	// retrieval of headers based on blockchain position rather than specific hashes.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - height: Starting block height to retrieve headers from
	// - limit: Maximum number of headers to retrieve
	//
	// Returns:
	// - Array of BlockHeader objects in chain order
	// - Array of corresponding BlockHeaderMeta objects with additional metadata
	// - Error if the header retrieval fails
	GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)

	// GetBlockHeadersByHeight retrieves block headers between two heights.
	//
	// This method fetches a sequence of block headers within a specified height range,
	// inclusive of both the start and end heights. This provides an efficient way to
	// retrieve a specific section of the blockchain based on positions rather than hashes.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - startHeight: Lower bound height to start retrieving headers from (inclusive)
	// - endHeight: Upper bound height to retrieve headers until (inclusive)
	//
	// Returns:
	// - Array of BlockHeader objects in ascending height order
	// - Array of corresponding BlockHeaderMeta objects with additional metadata
	// - Error if the header retrieval fails
	GetBlockHeadersByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)

	// GetBlocksByHeight retrieves full blocks between two heights.
	//
	// This method fetches a sequence of complete blocks within a specified height range,
	// inclusive of both the start and end heights. Unlike GetBlockHeadersByHeight which
	// only returns header data, this method provides full block information including
	// subtrees, transactions metadata, and coinbase data needed for operations like
	// subtree searching and detailed block analysis.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - startHeight: Lower bound height to start retrieving blocks from (inclusive)
	// - endHeight: Upper bound height to retrieve blocks until (inclusive)
	//
	// Returns:
	// - Array of complete Block objects in ascending height order
	// - Error if the block retrieval fails
	GetBlocksByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.Block, error)

	// FindBlocksContainingSubtree finds all blocks that contain the specified subtree hash.
	//
	// This method searches through the blockchain to find blocks whose subtree arrays contain
	// the given subtree hash. It's useful for merkle proof construction and subtree tracking.
	// The search is optimized using SQL queries to efficiently scan through blocks.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - subtreeHash: Hash of the subtree to search for
	// - maxBlocks: Maximum number of blocks to search (0 means no limit)
	//
	// Returns:
	// - Array of Block objects containing the specified subtree
	// - Error if the search fails
	FindBlocksContainingSubtree(ctx context.Context, subtreeHash *chainhash.Hash, maxBlocks uint32) ([]*model.Block, error)

	// InvalidateBlock marks a block as invalid.
	//
	// This method flags a block as invalid in the blockchain, which prevents it from being
	// considered part of the main chain. This is used when a block is determined to violate
	// consensus rules or other validation criteria. Invalidating a block also invalidates
	// all of its descendants.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - blockHash: Hash of the block to mark as invalid
	//
	// Returns:
	// - Error if the invalidation fails, nil on success
	InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) ([]chainhash.Hash, error)

	// RevalidateBlock restores a previously invalidated block.
	//
	// This method removes the invalid flag from a block that was previously marked as invalid,
	// allowing it to be reconsidered for inclusion in the main chain. This is useful for
	// recovery from false invalidations or after rule changes. Note that revalidating a block
	// does not automatically revalidate its descendants.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - blockHash: Hash of the block to restore to valid status
	//
	// Returns:
	// - Error if the revalidation fails, nil on success
	RevalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error

	// GetBlockHeaderIDs retrieves block header IDs.
	//
	// This method fetches the internal database IDs for a sequence of block headers starting
	// from the specified hash. These IDs can be used for efficient referencing of blocks
	// in subsequent operations without requiring the full hash.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - blockHash: Hash of the starting block
	// - numberOfHeaders: Maximum number of header IDs to retrieve
	//
	// Returns:
	// - Array of uint32 IDs corresponding to the requested blocks
	// - Error if the retrieval fails
	GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error)

	// Subscribe creates a subscription to blockchain notifications.
	//
	// This method establishes a subscription for real-time notifications about blockchain
	// events such as new blocks, state changes, and other significant events. The client
	// can process these notifications asynchronously through the returned channel.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - source: Identifier for the subscribing client (for logging and tracking)
	//
	// Returns:
	// - Channel of Notification objects that will receive blockchain events
	// - Error if the subscription creation fails
	Subscribe(ctx context.Context, source string) (chan *blockchain_api.Notification, error)

	// GetState retrieves state data by key.
	//
	// This method fetches arbitrary state data stored under the specified key in the
	// blockchain service's state database. This provides a generic key-value store
	// mechanism for maintaining application state alongside the blockchain.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - key: String identifier for the state data to retrieve
	//
	// Returns:
	// - Byte array containing the stored state data
	// - Error if the state retrieval fails or if the key doesn't exist
	GetState(ctx context.Context, key string) ([]byte, error)

	// SetState stores state data with a key.
	//
	// This method stores arbitrary data under the specified key in the blockchain service's
	// state database. This provides a generic key-value store mechanism for maintaining
	// application state alongside the blockchain. If the key already exists, its value
	// will be overwritten.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - key: String identifier under which to store the data
	// - data: Byte array containing the state data to store
	//
	// Returns:
	// - Error if the state storage fails, nil on success
	SetState(ctx context.Context, key string, data []byte) error

	// SetBlockMinedSet marks a block as mined.
	//
	// This method updates the blockchain database to indicate that a specific block has
	// been successfully mined. This status can affect how the block is treated in consensus
	// and can trigger related processes like block propagation and reward accounting.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - blockHash: Hash of the block to mark as mined
	//
	// Returns:
	// - Error if the operation fails, nil on success
	SetBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error

	// GetBlockIsMined retrieves whether a block has been marked as mined
	//
	// This method checks if a specific block has been flagged as successfully mined
	// in the blockchain database. This status information is useful for mining coordination
	// and validation processes.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - blockHash: Hash of the block to check mining status for
	//
	// Returns:
	// - Boolean indicating whether the block has been mined (true) or not (false)
	// - Error if the status check fails
	GetBlockIsMined(ctx context.Context, blockHash *chainhash.Hash) (bool, error)

	// GetBlocksMinedNotSet retrieves blocks not marked as mined.
	//
	// This method fetches information about blocks that are present in the blockchain
	// but have not yet been flagged as successfully mined. This is useful for identifying
	// blocks that may need mining status updates or for mining coordination.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	//
	// Returns:
	// - Array of Block structures containing blocks not marked as mined
	// - Error if the retrieval fails
	GetBlocksMinedNotSet(ctx context.Context) ([]*model.Block, error)

	// SetBlockSubtreesSet marks a block as having its subtrees set
	//
	// This method updates the blockchain database to indicate that the subtree hash structure
	// for a specific block has been properly set. Subtree hashes are important for efficient
	// validation and transaction lookup in the Bitcoin SV blockchain architecture.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - blockHash: Hash of the block to mark as having subtrees set
	//
	// Returns:
	// - Error if the operation fails, nil on success
	SetBlockSubtreesSet(ctx context.Context, blockHash *chainhash.Hash) error

	// SetBlockProcessedAt sets or clears the processed_at timestamp for a block.
	//
	// This method updates the timestamp indicating when a block was fully processed by the system.
	// This information is useful for tracking processing latency and debugging. The optional clear
	// parameter allows removing the timestamp instead of setting it.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - blockHash: Hash of the block to update the processed_at timestamp for
	// - clear: Optional boolean parameter - if true, clears the timestamp instead of setting it
	//
	// Returns:
	// - Error if the timestamp update fails, nil on success
	SetBlockProcessedAt(ctx context.Context, blockHash *chainhash.Hash, clear ...bool) error

	// SetBlockPersistedAt sets the persisted_at timestamp for a block.
	//
	// This method updates the timestamp indicating when a block was persisted to blob storage.
	// This information is used by the block persister to track which blocks have been persisted.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - blockHash: Hash of the block to update the persisted_at timestamp for
	//
	// Returns:
	// - Error if the timestamp update fails, nil on success
	SetBlockPersistedAt(ctx context.Context, blockHash *chainhash.Hash) error

	// GetBlocksNotPersisted retrieves blocks that haven't been persisted to blob storage yet.
	//
	// This method fetches blocks where persisted_at IS NULL and invalid = false, ordered by
	// height ascending. The limit parameter controls the maximum number of blocks returned
	// per call, which is useful for batching persistence operations.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - limit: Maximum number of blocks to retrieve
	//
	// Returns:
	// - Array of Block structures containing blocks not yet persisted
	// - Error if the retrieval fails
	GetBlocksNotPersisted(ctx context.Context, limit int) ([]*model.Block, error)

	// GetBlocksSubtreesNotSet retrieves blocks with unset subtrees.
	//
	// This method fetches information about blocks for which the subtree hash structure
	// has not yet been established. This is useful for identifying blocks that need
	// subtree processing as part of maintaining the blockchain's advanced data structures.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	//
	// Returns:
	// - Array of Block structures containing blocks with unset subtrees
	// - Error if the retrieval fails
	GetBlocksSubtreesNotSet(ctx context.Context) ([]*model.Block, error)

	// GetBestHeightAndTime retrieves the current best height and time.
	//
	// This method returns the current best block height and its timestamp in the blockchain.
	// This information is essential for determining the current state of the blockchain
	// and for synchronization processes.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	//
	// Returns:
	// - The height of the current best block as a uint32
	// - The timestamp of the current best block as a uint32 (Unix time)
	// - Error if the retrieval fails
	GetBestHeightAndTime(ctx context.Context) (uint32, uint32, error)

	// CheckBlockIsInCurrentChain checks if blocks are in the current chain.
	//
	// This method determines whether blocks with the specified IDs are part of the
	// current active blockchain (main chain) or if they belong to a side chain or are invalid.
	// This is important for validation and for determining the canonical state of the blockchain.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - blockIDs: Array of block IDs to check
	//
	// Returns:
	// - Boolean indicating whether all the blocks are in the main chain (true) or not (false)
	// - Error if the check operation fails
	CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error)

	// GetChainTips retrieves information about all known tips in the block tree.
	//
	// This method returns a list of all chain tips, including the main chain tip and any
	// orphaned branches. For each tip, it provides the height, hash, branch length from
	// the main chain, and validation status. This is essential for fork detection and
	// blockchain monitoring.
	//
	// The main chain tip will have branchlen=0 and status="active". Side chain tips
	// will have branchlen>0 indicating how many blocks back the fork occurred, with
	// status indicating their validation state ("valid-fork", "valid-headers",
	// "headers-only", or "invalid").
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	//
	// Returns:
	// - Array of ChainTip structures containing information about each tip
	// - Error if the retrieval fails
	GetChainTips(ctx context.Context) ([]*model.ChainTip, error)

	// FSM related endpoints
	//
	// GetFSMCurrentState retrieves the current state of the Finite State Machine.
	//
	// This method returns the current state of the blockchain's Finite State Machine (FSM),
	// which governs the operational state and transitions of the blockchain service.
	// The FSM represents different states such as initialization, normal operation,
	// synchronization, or recovery.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	//
	// Returns:
	// - Pointer to FSMStateType representing the current state of the blockchain FSM
	// - Error if the state retrieval fails
	GetFSMCurrentState(ctx context.Context) (*FSMStateType, error)
	// IsFSMCurrentState checks if the FSM is in a specific state.
	//
	// This method compares the current state of the blockchain FSM with the provided state
	// to determine if they match. This is useful for conditional logic based on the FSM state.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - state: The FSMStateType to check against the current state
	//
	// Returns:
	// - Boolean indicating whether the FSM is in the specified state (true) or not (false)
	// - Error if the state check fails
	IsFSMCurrentState(ctx context.Context, state FSMStateType) (bool, error)

	// WaitForFSMtoTransitionToGivenState blocks until the FSM transitions to the specified state.
	//
	// This method waits synchronously until the blockchain FSM reaches the specified state.
	// It is useful for coordinating operations that depend on specific blockchain states.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - state: The FSMStateType to wait for
	//
	// Returns:
	// - Error if the wait operation fails or times out
	WaitForFSMtoTransitionToGivenState(context.Context, FSMStateType) error

	// GetFSMCurrentStateForE2ETestMode retrieves the current FSM state for E2E testing.
	//
	// This method returns the current state of the blockchain FSM specifically for use in
	// end-to-end testing scenarios. It may bypass certain checks or validations that
	// would be performed in production environments.
	//
	// Returns:
	// - FSMStateType representing the current state of the blockchain FSM
	GetFSMCurrentStateForE2ETestMode() FSMStateType

	// WaitUntilFSMTransitionFromIdleState blocks until the FSM transitions from idle state.
	//
	// This method waits synchronously until the blockchain FSM transitions out of the idle state
	// to any other state. This is useful for operations that should only proceed after the
	// blockchain service has become active.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	//
	// Returns:
	// - Error if the wait operation fails or times out
	WaitUntilFSMTransitionFromIdleState(ctx context.Context) error

	// IsFullyReady checks if the blockchain service is fully operational.
	//
	// This method verifies that the blockchain service is ready for normal operations,
	// which includes both the FSM being in a non-IDLE state and the subscription
	// infrastructure being fully initialized. Services should use this method to
	// determine if they can safely proceed with blockchain operations and establish
	// reliable subscriptions.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	//
	// Returns:
	// - Boolean indicating whether the blockchain service is fully ready (true) or not (false)
	// - Error if the readiness check fails
	IsFullyReady(ctx context.Context) (bool, error)

	// Run initiates the normal operation of the blockchain service.
	//
	// This method starts the blockchain service in its standard operational mode,
	// processing blocks, validating transactions, and maintaining the blockchain state.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - source: Identifier for the source requesting the service to run
	//
	// Returns:
	// - Error if the service fails to start or encounters a critical issue
	Run(ctx context.Context, source string) error

	// CatchUpBlocks synchronizes the blockchain with peer nodes.
	//
	// This method initiates a process to catch up with the latest blocks from the network,
	// downloading and validating any blocks that are missing from the local blockchain.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	//
	// Returns:
	// - Error if the catch-up process fails
	CatchUpBlocks(ctx context.Context) error

	// ReportPeerFailure notifies the blockchain service about peer download failures.
	//
	// This method allows other services (like block validation, subtree validation) to report
	// when a peer fails to provide data correctly. The blockchain service broadcasts this to
	// subscribers (like P2P) who can take action such as switching to a different peer.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - hash: Hash of the block/subtree/tx being processed when the failure occurred
	// - peerID: Identifier of the failing peer
	// - failureType: Type of failure (e.g., "catchup", "subtree", "block")
	// - reason: Description of the failure
	//
	// Returns:
	// - Error if the notification fails
	ReportPeerFailure(ctx context.Context, hash *chainhash.Hash, peerID string, failureType string, reason string) error

	// LegacySync performs a legacy synchronization process.
	//
	// This method initiates a blockchain synchronization using the legacy synchronization
	// protocol, which may be needed for compatibility with older network nodes.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	//
	// Returns:
	// - Error if the legacy synchronization process fails
	LegacySync(ctx context.Context) error

	// Idle transitions the blockchain service to an idle state.
	//
	// This method puts the blockchain service into an idle state where it maintains
	// minimal activity and resource usage while still being responsive to commands.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	//
	// Returns:
	// - Error if the transition to idle state fails
	Idle(ctx context.Context) error
	// SendFSMEvent sends an event to the FSM to trigger a state transition.
	//
	// This method sends the specified event to the blockchain's Finite State Machine,
	// potentially causing a state transition according to the FSM's rules. This is used
	// to control the lifecycle and behavior of the blockchain service.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - event: The event to trigger, which should be a valid FSM event for the current state
	//
	// Returns:
	// - Error if the event triggering fails or if the event is invalid for the current state
	SendFSMEvent(ctx context.Context, event FSMEventType) error

	//
	// Legacy endpoints
	//

	// GetBlockLocator retrieves a block locator for blockchain synchronization.
	//
	// This method generates a block locator, which is a compact representation of the
	// blockchain's structure used for efficient synchronization between nodes. The locator
	// contains hashes spaced exponentially further apart moving from the tip backward,
	// allowing quick identification of common ancestors.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - blockHeaderHash: Hash of the starting block header for locator generation
	// - blockHeaderHeight: Height of the starting block header
	//
	// Returns:
	// - Array of hashes forming the block locator sequence
	// - Error if the locator generation fails
	GetBlockLocator(ctx context.Context, blockHeaderHash *chainhash.Hash, blockHeaderHeight uint32) ([]*chainhash.Hash, error)

	// LocateBlockHeaders locates block headers using a block locator.
	//
	// This method uses a block locator to efficiently find where two blockchain states
	// diverge, and then returns headers starting from that point. This is essential for
	// blockchain synchronization protocols that need to identify and retrieve only
	// the missing or divergent portions of the chain.
	//
	// Parameters:
	// - ctx: Context for the operation with timeout and cancellation support
	// - locator: Array of block hashes forming a locator sequence
	// - hashStop: Hash at which to stop returning headers, or nil to return maximum allowed
	// - maxHashes: Maximum number of headers to return
	//
	// Returns:
	// - Array of BlockHeader objects from divergence point to hashStop or chain tip
	// - Error if the header location or retrieval fails
	LocateBlockHeaders(ctx context.Context, locator []*chainhash.Hash, hashStop *chainhash.Hash, maxHashes uint32) ([]*model.BlockHeader, error)
}

const notImplemented = "not implemented"
