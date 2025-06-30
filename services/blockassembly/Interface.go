// Package blockassembly provides functionality for assembling Bitcoin blocks in Teranode.
//
// This file defines the interfaces used by clients to interact with the blockassembly service.
// These interfaces abstract the implementation details of block assembly operations,
// providing a clean API for transaction submission, mining operations, and service status checks.
// Both production and mock implementations are provided to support various usage scenarios.
// The interfaces follow a consistent pattern aligned with other Teranode services.

package blockassembly

import (
	"context"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/pkg/go-subtree"
	"github.com/bitcoin-sv/teranode/services/blockassembly/blockassembly_api"
	"github.com/libsv/go-bt/v2/chainhash"
)

// ClientI defines the interface for block assembly client operations.
// This interface provides methods for external components to interact with the block assembly service,
// including transaction submission, mining operations, and service management functions.
// Implementations of this interface handle the communication details with the underlying service,
// whether through direct in-process calls or remote procedure calls.

type ClientI interface {
	// Health checks the health status of the block assembly service.
	//
	// Parameters:
	//   - ctx: Context for cancellation
	//   - checkLiveness: Whether to perform liveness check
	//
	// Returns:
	//   - int: HTTP status code indicating health state
	//   - string: Health status message
	//   - error: Any error encountered during health check
	Health(ctx context.Context, checkLiveness bool) (int, string, error)

	// Store stores a transaction in block assembly.
	//
	// Parameters:
	//   - ctx: Context for cancellation
	//   - hash: Transaction hash
	//   - fee: Transaction fee in satoshis
	//   - size: Transaction size in bytes
	//
	// Returns:
	//   - bool: True if storage was successful
	//   - error: Any error encountered during storage
	Store(ctx context.Context, hash *chainhash.Hash, fee, size uint64, txInpoints subtree.TxInpoints) (bool, error)

	// RemoveTx removes a transaction from block assembly.
	//
	// Parameters:
	//   - ctx: Context for cancellation
	//   - hash: Hash of transaction to remove
	//
	// Returns:
	//   - error: Any error encountered during removal
	RemoveTx(ctx context.Context, hash *chainhash.Hash) error

	// GetMiningCandidate retrieves a candidate block for mining.
	//
	// Parameters:
	//   - ctx: Context for cancellation
	//
	// Returns:
	//   - *model.MiningCandidate: Mining candidate block
	//   - error: Any error encountered during retrieval
	GetMiningCandidate(ctx context.Context, includeSubtreeHashes ...bool) (*model.MiningCandidate, error)

	// GetCurrentDifficulty retrieves the current mining difficulty.
	//
	// Parameters:
	//   - ctx: Context for cancellation
	//
	// Returns:
	//   - float64: Current difficulty value
	//   - error: Any error encountered during retrieval
	GetCurrentDifficulty(ctx context.Context) (float64, error)

	// SubmitMiningSolution submits a solution for a mined block.
	//
	// Parameters:
	//   - ctx: Context for cancellation
	//   - solution: Mining solution to submit
	//
	// Returns:
	//   - error: Any error encountered during submission
	SubmitMiningSolution(ctx context.Context, solution *model.MiningSolution) error

	// GenerateBlocks generates a specified number of blocks.
	//
	// Parameters:
	//   - ctx: Context for cancellation
	//   - req: Block generation request parameters
	//
	// Returns:
	//   - error: Any error encountered during generation
	GenerateBlocks(ctx context.Context, req *blockassembly_api.GenerateBlocksRequest) error

	// DeDuplicateBlockAssembly removes duplicate transactions from block assembly.
	//
	// Parameters:
	//   - ctx: Context for cancellation
	//
	// Returns:
	//   - error: Any error encountered during deduplication
	DeDuplicateBlockAssembly(ctx context.Context) error

	// ResetBlockAssembly resets the block assembly state.
	//
	// Parameters:
	//   - ctx: Context for cancellation
	//
	// Returns:
	//   - error: Any error encountered during reset
	ResetBlockAssembly(ctx context.Context) error

	// GetBlockAssemblyState retrieves the current state of block assembly.
	//
	// Parameters:
	//   - ctx: Context for cancellation
	//
	// Returns:
	//   - *blockassembly_api.StateMessage: Current state
	//   - error: Any error encountered during retrieval
	GetBlockAssemblyState(ctx context.Context) (*blockassembly_api.StateMessage, error)

	// GetBlockAssemblyBlockCandidate retrieves the block candidate for block assembly.
	//
	// Parameters:
	//   - ctx: Context for cancellation
	//
	// Returns:
	//   - *model.Block: Block candidate
	//   - error: Any error encountered during retrieval
	GetBlockAssemblyBlockCandidate(ctx context.Context) (*model.Block, error)
}

// Store defines the interface for block assembly storage operations.
// This interface abstracts the storage mechanism used for transactions in the block assembly process,
// providing methods for storing and removing transactions from the assembly queue.
// It isolates the storage implementation details from the client code and enables
// different storage backends to be used without changing the client code.

type Store interface {
	// Store stores a transaction in the storage system.
	//
	// Parameters:
	//   - ctx: Context for cancellation
	//   - hash: Transaction hash
	//   - fee: Transaction fee in satoshis
	//   - size: Transaction size in bytes
	//
	// Returns:
	//   - bool: True if storage was successful
	//   - error: Any error encountered during storage
	Store(ctx context.Context, hash *chainhash.Hash, fee, size uint64, txInpoints subtree.TxInpoints) (bool, error)

	// RemoveTx removes a transaction from storage.
	//
	// Parameters:
	//   - ctx: Context for cancellation
	//   - hash: Hash of transaction to remove
	//
	// Returns:
	//   - error: Any error encountered during removal
	RemoveTx(ctx context.Context, hash *chainhash.Hash) error
}
