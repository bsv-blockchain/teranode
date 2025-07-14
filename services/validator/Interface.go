/*
Package validator implements Bitcoin SV transaction validation functionality.

This package provides a comprehensive transaction validation framework that implements
the Bitcoin SV consensus and policy rules. It serves as a critical component in the
Teranode architecture, ensuring that only valid transactions are accepted into the
mempool and blocks.

This file defines the core interfaces for the validator service, providing the contract
that all validator implementations must fulfill. The Interface type establishes the
required methods for any validator implementation, ensuring consistent behavior across
different implementations or testing scenarios. The file also includes a mock implementation
for testing purposes that satisfies the interface contract without performing actual
validation.

The validator package interacts with multiple other components in the system:
- UTXO store for input validation and double-spend prevention
- Block assembly for transaction prioritization and mining
- Blockchain service for block height and chain state information
- Kafka for asynchronous event processing and communication

These interfaces enable loose coupling between components while enforcing the necessary
validation contract to maintain Bitcoin consensus rules.
*/
package validator

import (
	"context"

	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bsv-blockchain/go-bt/v2"
)

// Interface defines the core validation functionality required for Bitcoin transaction validation.
// Any implementation of this interface must provide comprehensive transaction validation
// capabilities along with health monitoring and block height management.
type Interface interface {
	// Health performs comprehensive health checks on the validator implementation to ensure
	// proper operation and readiness to process transactions. This method validates internal
	// state, connectivity to dependent services, and resource availability.
	//
	// Parameters:
	//   - ctx: Context for the health check operation, supports cancellation and timeouts
	//   - checkLiveness: If true, performs only basic liveness checks; if false, performs
	//     comprehensive readiness checks including database connectivity and service dependencies
	//
	// Returns:
	//   - int: HTTP status code indicating health status (200 for healthy, 503 for unhealthy)
	//   - string: Detailed health status message with diagnostic information
	//   - error: Any critical errors encountered during health check that prevent operation
	Health(ctx context.Context, checkLiveness bool) (int, string, error)

	// Validate performs comprehensive validation of a Bitcoin transaction according to consensus
	// rules and policy constraints. This method executes all validation steps including structure
	// validation, script execution, UTXO verification, and consensus rule enforcement.
	//
	// The validation process includes:
	// - Transaction structure and format validation
	// - Input/output consistency checks
	// - Script execution and signature verification
	// - UTXO existence and double-spend prevention
	// - Consensus rule compliance (block size, fees, etc.)
	//
	// Parameters:
	//   - ctx: Context for the validation operation, supports cancellation and timeouts
	//   - tx: The Bitcoin transaction to validate, must be properly formatted
	//   - blockHeight: Current block height for validation context and consensus rule application
	//   - opts: Optional validation settings that modify validation behavior (e.g., policy rules)
	//
	// Returns:
	//   - *meta.Data: Transaction metadata including validation results and processing information
	//   - error: Validation errors if transaction violates consensus rules or policy constraints
	Validate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...Option) (*meta.Data, error)

	// ValidateWithOptions performs comprehensive validation of a transaction with validation
	// options passed directly rather than using variadic parameters. This method provides
	// the same validation functionality as Validate but with explicit options configuration.
	//
	// Parameters:
	//   - ctx: Context for the validation operation, supports cancellation and timeouts
	//   - tx: The Bitcoin transaction to validate, must be properly formatted
	//   - blockHeight: Current block height for validation context and consensus rule application
	//   - validationOptions: Explicit validation options configuration including policy rules
	//
	// Returns:
	//   - *meta.Data: Transaction metadata including validation results and processing information
	//   - error: Validation errors if transaction violates consensus rules or policy constraints
	ValidateWithOptions(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options) (*meta.Data, error)

	// GetBlockHeight returns the current block height known to the validator service.
	// This height is used for validation context and consensus rule application, and should
	// reflect the latest confirmed block in the blockchain.
	//
	// Returns:
	//   - uint32: Current block height, zero if blockchain state is not available
	GetBlockHeight() uint32

	// GetMedianBlockTime returns the median timestamp of the last 11 blocks, which is used
	// for certain consensus rules including transaction locktime validation. This value
	// provides a more stable time reference than individual block timestamps.
	//
	// Returns:
	//   - uint32: Median block time in Unix timestamp format, zero if insufficient block history
	GetMedianBlockTime() uint32

	// TriggerBatcher manually triggers the transaction batch processor to process queued
	// transactions. This method is typically used for testing or administrative purposes
	// to force immediate processing of pending validation requests rather than waiting
	// for automatic batch processing intervals.
	//
	// This method does not return any values and executes asynchronously. The actual
	// batch processing results can be monitored through metrics and logging systems.
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
	return util.TxMetaDataFromTx(tx)
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
	return util.TxMetaDataFromTx(tx)
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
