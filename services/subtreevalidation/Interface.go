// Package subtreevalidation provides functionality for validating subtrees in a blockchain context.
// It handles the validation of transaction subtrees, manages transaction metadata caching,
// and interfaces with blockchain and validation services.
package subtreevalidation

import (
	"context"

	"github.com/libsv/go-bt/v2/chainhash"
)

// Interface defines the contract for subtree validation operations.
type Interface interface {
	// Health checks the health status of the subtree validation service.
	// If checkLiveness is true, it performs only liveness checks.
	// Returns HTTP status code, status message, and any error encountered.
	Health(ctx context.Context, checkLiveness bool) (int, string, error)

	// CheckSubtreeFromBlock validates a subtree with the given hash at a specific block height.
	// baseURL specifies the source for fetching missing transactions.
	// blockHash is optional and can be nil.
	CheckSubtreeFromBlock(ctx context.Context, hash chainhash.Hash, baseURL string, blockHeight uint32, blockHash *chainhash.Hash) error
}

var _ Interface = &MockSubtreeValidation{}

// MockSubtreeValidation provides a mock implementation of the Interface for testing.
type MockSubtreeValidation struct{}

func (mv *MockSubtreeValidation) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return 0, "MockValidator", nil
}

func (mv *MockSubtreeValidation) CheckSubtreeFromBlock(ctx context.Context, hash chainhash.Hash, baseURL string, blockHeight uint32, blockHash *chainhash.Hash) error {
	return nil
}
