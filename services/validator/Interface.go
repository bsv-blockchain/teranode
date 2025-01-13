/*
Package validator implements Bitcoin SV transaction validation functionality.

This file defines the core interfaces for the validator service, providing
the contract that all validator implementations must fulfill. It also includes
a mock implementation for testing purposes.
*/
package validator

import (
	"context"

	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/libsv/go-bt/v2"
)

// Interface defines the core validation functionality required for Bitcoin transaction validation.
// Any implementation of this interface must provide comprehensive transaction validation
// capabilities along with health monitoring and block height management.
type Interface interface {
	// Health performs health checks on the validator implementation
	// Parameters:
	//   - ctx: Context for the health check operation
	//   - checkLiveness: If true, only checks basic liveness
	// Returns:
	//   - int: HTTP status code indicating health status
	//   - string: Detailed health status message
	//   - error: Any errors encountered during health check
	Health(ctx context.Context, checkLiveness bool) (int, string, error)

	// Validate performs comprehensive validation of a transaction
	// Parameters:
	//   - ctx: Context for the validation operation
	//   - tx: The transaction to validate
	//   - blockHeight: Current block height for validation context
	//   - opts: Optional validation settings
	// Returns:
	//   - error: Any validation errors encountered
	Validate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...Option) (*meta.Data, error)

	// ValidateWithOptions performs comprehensive validation of a transaction with options passed in directly
	// Parameters:
	//   - ctx: Context for the validation operation
	//   - tx: The transaction to validate
	//   - blockHeight: Current block height for validation context
	//   - opts: validation options
	// Returns:
	//   - error: Any validation errors encountered
	ValidateWithOptions(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options) (*meta.Data, error)

	// GetBlockHeight returns the current block height
	// Returns:
	//   - uint32: Current block height
	GetBlockHeight() uint32

	// GetMedianBlockTime returns the median time of the last 11 blocks
	// Returns:
	//   - uint32: Median block time in Unix timestamp format
	GetMedianBlockTime() uint32

	// TriggerBatcher triggers the transaction batch processor
	// This method is used to manually trigger batch processing
	TriggerBatcher()
}

// Type assertion to ensure MockValidator implements Interface
var _ Interface = &MockValidator{}

// MockValidator provides a mock implementation of the validator Interface
// This implementation is primarily used for testing purposes and provides
// no-op implementations of all required methods.
type MockValidator struct{}

// Health implements the health check for the mock validator
// Always returns success without actually performing any checks
// Parameters:
//   - ctx: Context for the health check operation (unused in mock)
//   - checkLiveness: Boolean flag for liveness check (unused in mock)
//
// Returns:
//   - int: Always returns 0
//   - string: Always returns "Mock Validator"
//   - error: Always returns nil
func (mv *MockValidator) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return 0, "Mock Validator", nil
}

// Validate implements mock transaction validation
// Always returns success without performing any actual validation
// Parameters:
//   - ctx: Context for validation (unused in mock)
//   - tx: Transaction to validate (unused in mock)
//   - blockHeight: Block height for validation context (unused in mock)
//   - opts: Optional validation settings (unused in mock)
//
// Returns:
//   - error: Always returns nil
func (mv *MockValidator) Validate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...Option) (*meta.Data, error) {
	return nil, nil
}

// ValidateWithOptions implements mock transaction validation with options set directly
// Always returns success without performing any actual validation
// Parameters:
//   - ctx: Context for validation (unused in mock)
//   - tx: Transaction to validate (unused in mock)
//   - blockHeight: Block height for validation context (unused in mock)
//   - validationOptions: Validation options for the transaction (unused in mock)
//
// Returns:
//   - error: Always returns nil
func (mv *MockValidator) ValidateWithOptions(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options) (*meta.Data, error) {
	return nil, nil
}

// GetBlockHeight implements mock block height retrieval
// Always returns 0 without actually checking any block height
// Returns:
//   - uint32: Always returns 0
func (mv *MockValidator) GetBlockHeight() uint32 {
	return 0
}

// GetMedianBlockTime implements mock median block time retrieval
// Always returns 0 without calculating any actual median time
// Returns:
//   - uint32: Always returns 0
func (mv *MockValidator) GetMedianBlockTime() uint32 {
	return 0
}

// TriggerBatcher implements mock batch triggering
// No-op implementation that does nothing
func (mv *MockValidator) TriggerBatcher() {}
