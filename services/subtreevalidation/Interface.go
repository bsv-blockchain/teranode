// Package subtreevalidation implements the subtree validation service for the Teranode architecture.
//
// The subtree validation service is responsible for validating transaction subtrees, which are
// groups of interdependent transactions that are processed together to maintain blockchain integrity.
// This package ensures that all transactions within a subtree are valid according to Bitcoin SV
// consensus rules before they are committed to the blockchain.
//
// Key features and capabilities:
// - Efficient validation of transaction subtrees with interdependent transactions
// - Transaction metadata caching for improved performance
// - Parallel processing of transactions within subtrees
// - Missing transaction retrieval from network sources
// - Integration with blockchain and validation services
// - Health monitoring and diagnostics
//
// The package integrates with several other Teranode components:
// - Blockchain service: For chain state information and block height validation
// - Validator service: For transaction validation against consensus rules
// - UTXO store: For unspent transaction output state management
// - Blob stores: For persistent storage of subtrees and transactions
// - Kafka: For message-based communication with other services
//
// Architecture design:
// The service follows a client-server model with a gRPC API for external communication.
// It employs a layered approach with clear separation between API handling, business logic,
// and storage interactions. The service implements defensive programming techniques including
// retry mechanisms, graceful error handling, and proper resource management.
package subtreevalidation

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/mock"
)

// Interface defines the contract for subtree validation operations.
//
// This interface abstracts the subtree validation service, allowing for flexible
// implementations and easier testing through mocking. It provides the core operations
// needed by other services to validate transaction subtrees and ensure blockchain integrity.
//
// Implementations of this interface should handle transaction validation, subtree composition,
// and ensure that all Bitcoin consensus rules are followed before transactions are committed
// to the blockchain.
type Interface interface {
	// Health checks the health status of the subtree validation service.
	// If checkLiveness is true, it performs only liveness checks.
	// Returns HTTP status code, status message, and any error encountered.
	//
	// This method follows the standard Teranode health check pattern and is used by
	// monitoring systems to determine service availability and readiness.
	//
	// Parameters:
	//   - ctx: Context for cancellation and tracing
	//   - checkLiveness: If true, performs only basic liveness checks; if false, performs full readiness checks
	//
	// Returns:
	//   - int: HTTP status code (200 for healthy, other codes for specific issues)
	//   - string: Human-readable status message
	//   - error: Detailed error information if the service is unhealthy
	Health(ctx context.Context, checkLiveness bool) (int, string, error)

	// CheckSubtreeFromBlock validates a subtree with the given hash at a specific block height.
	// This is the primary method for validating transaction subtrees, ensuring that all
	// transactions in the subtree are valid according to consensus rules and can be included
	// in the blockchain.
	//
	// The method handles missing transaction retrieval, ancestor validation, and ensures that
	// all transactions in the subtree can be added to the blockchain at the specified height.
	//
	// Parameters:
	//   - ctx: Context for cancellation and tracing
	//   - hash: The hash of the subtree to validate
	//   - baseURL: URL to fetch missing transactions from if needed
	//   - blockHeight: The height of the block containing the subtree
	//   - blockHash: Optional hash of the block containing the subtree (can be nil)
	//   - previousBlockHash: Optional hash of the previous block (can be nil)
	//
	// Returns:
	//   - error: Any error encountered during validation, nil if successful
	CheckSubtreeFromBlock(ctx context.Context, hash chainhash.Hash, baseURL string, blockHeight uint32, blockHash, previousBlockHash *chainhash.Hash) error

	// CheckBlockSubtrees validates all subtrees in a block identified by its hash.
	// This method delegates the validation of all subtrees within a specific block to the subtree validation service.
	//
	// The method handles missing transaction retrieval, ancestor validation, and ensures that
	// all transactions in the subtree can be added to the blockchain at the specified height.
	//
	// Parameters:
	//   - ctx: Context for cancellation and tracing
	//   - blockHash: The hash of the block containing the subtrees to validate
	//   - blockHeight: The height of the block containing the subtrees
	//   - peerID: P2P peer identifier used for peer reputation tracking
	//   - baseURL: URL to fetch missing transactions from if needed
	//
	// Returns:
	//   - error: Any error encountered during validation, nil if successful
	CheckBlockSubtrees(ctx context.Context, block *model.Block, peerID, baseURL string) error
}

var _ Interface = &MockSubtreeValidation{}

// MockSubtreeValidation provides a mock implementation of the Interface for testing.
//
// This implementation uses the testify mock package to create a mockable version
// of the Interface. It allows test code to set expectations on method calls and
// control return values for predictable testing scenarios.
//
// MockSubtreeValidation is only intended for use in test code and should not be
// used in production environments.
type MockSubtreeValidation struct {
	mock.Mock
}

func (mv *MockSubtreeValidation) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	args := mv.Called(ctx, checkLiveness)
	return args.Int(0), args.String(1), args.Error(2)
}

func (mv *MockSubtreeValidation) CheckSubtreeFromBlock(ctx context.Context, hash chainhash.Hash, baseURL string, blockHeight uint32, blockHash, previousBlockHash *chainhash.Hash) error {
	args := mv.Called(ctx, hash, baseURL, blockHeight, blockHash, previousBlockHash)
	return args.Error(0)
}

func (mv *MockSubtreeValidation) CheckBlockSubtrees(ctx context.Context, block *model.Block, peerID, baseURL string) error {
	args := mv.Called(ctx, block, peerID, baseURL)
	return args.Error(0)
}
