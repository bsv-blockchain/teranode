// Package blockassembly provides functionality for assembling Bitcoin blocks in Teranode.
package blockassembly

import (
	"context"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockassembly/blockassembly_api"
	"github.com/libsv/go-bt/v2/chainhash"
)

// ClientI defines the interface for block assembly client operations.
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
	Store(ctx context.Context, hash *chainhash.Hash, fee, size uint64) (bool, error)

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
}

// Store defines the interface for block assembly storage operations.
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
	Store(ctx context.Context, hash *chainhash.Hash, fee, size uint64) (bool, error)

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

// Mock implements a mock version of the ClientI interface for testing.
type Mock struct {
	// State represents the mocked state
	State *blockassembly_api.StateMessage
}

// NewMock creates a new mock client.
//
// Returns:
//   - ClientI: Mock client implementation
//   - error: Any error encountered during creation
func NewMock() (ClientI, error) {
	c := &Mock{}

	return c, nil
}

func (m Mock) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return 0, "", nil
}

func (m Mock) Store(ctx context.Context, hash *chainhash.Hash, fee, size uint64) (bool, error) {
	// TODO implement me
	panic("implement me")
}

func (m Mock) RemoveTx(ctx context.Context, hash *chainhash.Hash) error {
	// TODO implement me
	panic("implement me")
}

func (m Mock) GetMiningCandidate(ctx context.Context, includeSubtreeHashes ...bool) (*model.MiningCandidate, error) {
	// TODO implement me
	panic("implement me")
}

func (m Mock) GetCurrentDifficulty(ctx context.Context) (float64, error) {
	// TODO implement me
	panic("implement me")
}

func (m Mock) SubmitMiningSolution(ctx context.Context, solution *model.MiningSolution) error {
	// TODO implement me
	panic("implement me")
}

func (m Mock) GenerateBlocks(ctx context.Context, req *blockassembly_api.GenerateBlocksRequest) error {
	// TODO implement me
	panic("implement me")
}

func (m Mock) DeDuplicateBlockAssembly(ctx context.Context) error {
	// TODO implement me
	panic("implement me")
}

func (m Mock) ResetBlockAssembly(ctx context.Context) error {
	// TODO implement me
	panic("implement me")
}

func (m Mock) GetBlockAssemblyState(ctx context.Context) (*blockassembly_api.StateMessage, error) {
	return m.State, nil
}
