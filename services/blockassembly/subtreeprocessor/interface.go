// Package subtreeprocessor provides functionality for processing and managing transaction subtrees in Teranode.
//
// The subtreeprocessor is a critical component of the block assembly system that organizes
// transactions into efficient subtree structures for block creation. It handles:
//   - Transaction organization into subtrees based on dependencies and relationships
//   - Subtree completion detection and management
//   - Block reorganization handling with subtree state management
//   - Transaction queue management and processing
//   - Integration with UTXO store for transaction validation
//
// The subtree-based approach enables efficient parallel processing of transactions
// and optimizes the block assembly process for high-throughput Bitcoin operations.
package subtreeprocessor

import (
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/pkg/go-subtree"
	utxostore "github.com/bitcoin-sv/teranode/stores/utxo"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/libsv/go-bt/v2/chainhash"
)

// Interface defines the contract for subtree processor implementations.
// This interface abstracts the subtree processing operations, enabling different
// implementations while maintaining a consistent API for the block assembly system.
//
// The interface provides methods for:
//   - Adding and removing transactions from the processing queue
//   - Managing subtree state and completion detection
//   - Handling blockchain reorganizations and resets
//   - Retrieving completed subtrees for mining candidates
//   - Monitoring processor state and performance metrics
//
// Implementations of this interface are responsible for organizing transactions
// into efficient subtree structures and maintaining consistency during blockchain
// state changes.
type Interface interface {
	// Add adds a transaction node to the subtree processor for processing.
	// The transaction will be organized into appropriate subtrees based on
	// its dependencies and relationships with other transactions.
	//
	// Parameters:
	//   - node: The transaction node to add to processing
	//   - txInpoints: Transaction input points for dependency tracking
	Add(node subtree.SubtreeNode, txInpoints subtree.TxInpoints)

	// AddDirectly adds a transaction node directly to the processor without
	// using the queue. This is typically used for block assembly startup.
	// It allows immediate processing of transactions without waiting for
	// the queue to process them.
	//
	// Parameters:
	//   - node: The transaction node to add directly
	//   - txInpoints: Transaction input points for dependency tracking
	//
	// Returns:
	//   - error: Any error encountered during the addition
	//
	// Note: This method bypasses the normal queue processing and should be used
	AddDirectly(node subtree.SubtreeNode, txInpoints subtree.TxInpoints) error

	// GetCurrentRunningState returns the current operational state of the processor.
	// This provides visibility into whether the processor is running, stopped,
	// resetting, or in another operational state.
	//
	// Returns:
	//   - State: Current operational state of the processor
	GetCurrentRunningState() State

	// GetCurrentLength returns the current number of items in the processing queue.
	// This metric helps monitor the processor's workload and performance.
	//
	// Returns:
	//   - int: Number of items currently in the processing queue
	GetCurrentLength() int

	// CheckSubtreeProcessor performs a health check on the processor state.
	// This method validates that the processor is operating correctly and
	// identifies any issues that might affect processing.
	//
	// Returns:
	//   - error: Any error indicating processor health issues, nil if healthy
	CheckSubtreeProcessor() error

	// MoveForwardBlock processes a new block addition to the blockchain.
	// This updates the processor state to reflect the new blockchain tip
	// and handles any necessary subtree state changes.
	//
	// Parameters:
	//   - block: The new block being added to the blockchain
	//
	// Returns:
	//   - error: Any error encountered during block processing
	MoveForwardBlock(block *model.Block) error

	// Reorg handles blockchain reorganization by processing blocks that need
	// to be removed and added during the reorganization process.
	//
	// Parameters:
	//   - moveBackBlocks: Blocks to be removed from the chain
	//   - modeUpBlocks: Blocks to be added to the chain
	//
	// Returns:
	//   - error: Any error encountered during reorganization
	Reorg(moveBackBlocks []*model.Block, modeUpBlocks []*model.Block) error

	// Reset performs a complete reset of the processor state to a specific block.
	// This is used during major reorganizations or when recovering from errors.
	//
	// Parameters:
	//   - blockHeader: Target block header to reset to
	//   - moveBackBlocks: Blocks to be removed during reset
	//   - moveForwardBlocks: Blocks to be added during reset
	//   - isLegacySync: Whether this is part of legacy synchronization
	//
	// Returns:
	//   - ResetResponse: Response containing reset operation results
	Reset(blockHeader *model.BlockHeader, moveBackBlocks []*model.Block, moveForwardBlocks []*model.Block, isLegacySync bool) ResetResponse

	// Remove removes a specific transaction from the processor by its hash.
	// This is used when transactions become invalid or need to be excluded.
	//
	// Parameters:
	//   - hash: Hash of the transaction to remove
	//
	// Returns:
	//   - error: Any error encountered during transaction removal
	Remove(hash chainhash.Hash) error

	// GetCompletedSubtreesForMiningCandidate returns completed subtrees ready for mining.
	// These subtrees contain validated transactions that can be included in a block.
	//
	// Returns:
	//   - []*util.Subtree: Array of completed subtrees ready for mining
	GetCompletedSubtreesForMiningCandidate() []*subtree.Subtree

	// GetCurrentBlockHeader returns the current block header the processor is working with.
	// This represents the blockchain tip from the processor's perspective.
	//
	// Returns:
	//   - *model.BlockHeader: Current block header
	GetCurrentBlockHeader() *model.BlockHeader

	// SetCurrentBlockHeader updates the current block header in the processor.
	// This is used to synchronize the processor with blockchain state changes.
	//
	// Parameters:
	//   - blockHeader: New block header to set as current
	SetCurrentBlockHeader(blockHeader *model.BlockHeader)

	// GetCurrentSubtree returns the subtree currently being processed.
	// This provides visibility into the active processing state.
	//
	// Returns:
	//   - *util.Subtree: Currently active subtree, nil if none
	GetCurrentSubtree() *subtree.Subtree

	// GetCurrentTxMap returns the current transaction map with input points.
	// This provides access to the processor's transaction tracking state.
	//
	// Returns:
	//   - *util.SyncedMap[chainhash.Hash, meta.TxInpoints]: Current transaction map
	GetCurrentTxMap() *txmap.SyncedMap[chainhash.Hash, subtree.TxInpoints]

	// GetChainedSubtrees returns subtrees that are chained together.
	// These represent transaction dependencies and processing order.
	//
	// Returns:
	//   - []*util.Subtree: Array of chained subtrees
	GetChainedSubtrees() []*subtree.Subtree

	// GetUtxoStore returns the UTXO store used by the processor.
	// This provides access to the underlying UTXO validation system.
	//
	// Returns:
	//   - utxostore.Store: UTXO store instance
	GetUtxoStore() utxostore.Store

	// SetCurrentItemsPerFile configures the number of items per file for storage.
	// This affects how subtrees are organized and stored.
	//
	// Parameters:
	//   - v: Number of items per file to configure
	SetCurrentItemsPerFile(v int)

	// TxCount returns the total number of transactions processed.
	// This metric helps monitor processor throughput and performance.
	//
	// Returns:
	//   - uint64: Total transaction count
	TxCount() uint64

	// QueueLength returns the current length of the processing queue.
	// This indicates the processor's current workload.
	//
	// Returns:
	//   - int64: Current queue length
	QueueLength() int64

	// SubtreeCount returns the total number of subtrees managed by the processor.
	// This metric provides visibility into the processor's organizational state.
	//
	// Returns:
	//   - int: Total number of subtrees
	SubtreeCount() int

	// DeDuplicateTransactions triggers deduplication of transactions in the processor.
	// This removes duplicate transactions to maintain processing efficiency.
	DeDuplicateTransactions()
}
