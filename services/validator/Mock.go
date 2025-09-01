/*
Package validator implements Bitcoin SV transaction validation functionality.

This package provides comprehensive transaction validation for Bitcoin SV nodes,
including script verification, UTXO management, and policy enforcement. It supports
multiple script interpreters (GoBT, GoSDK, GoBDK) and implements the full Bitcoin
transaction validation ruleset.

Key features:
  - Transaction validation against Bitcoin consensus rules
  - UTXO spending and creation
  - Script verification using multiple interpreters
  - Policy enforcement
  - Block assembly integration
  - Kafka integration for transaction metadata

Usage:

	validator := NewTxValidator(logger, policy, params)
	err := validator.ValidateTransaction(tx, blockHeight, nil)
*/
package validator

import (
	"context"
	"sync"

	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/go-bt/v2"
)

// MockValidatorClient implements a test double for the validator client interface.
// This mock provides controllable behavior for testing validator integration scenarios,
// allowing tests to simulate various validation states, errors, and blockchain conditions.
//
// The mock supports:
// - Configurable block height and median time simulation
// - Error injection for testing failure scenarios
// - UTXO store integration for transaction validation testing
// - Thread-safe operation for concurrent test scenarios
type MockValidatorClient struct {
	// BlockHeight represents the current blockchain height for validation context
	BlockHeight uint32

	// MedianBlockTime represents the median time of recent blocks for timelock validation
	MedianBlockTime uint32

	// Errors contains a list of errors to return from validation operations
	Errors []error

	// ErrorsMu provides thread-safe access to the Errors slice
	ErrorsMu sync.Mutex

	// UtxoStore provides access to UTXO data for transaction validation
	UtxoStore utxo.Store
}

// Health implements the validator client health check interface for testing.
// Always returns successful status with "MockValidator" identifier.
func (m *MockValidatorClient) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return 0, "MockValidator", nil
}

// SetBlockHeight updates the mock's blockchain height for validation context.
// Allows tests to simulate different blockchain states and height-dependent validation rules.
func (m *MockValidatorClient) SetBlockHeight(blockHeight uint32) error {
	m.BlockHeight = blockHeight
	return nil
}

// GetBlockHeight returns the configured blockchain height.
// Used for height-dependent validation rules and timelock verification.
func (m *MockValidatorClient) GetBlockHeight() uint32 {
	return m.BlockHeight
}

// SetMedianBlockTime updates the mock's median block time for timelock validation.
// Allows tests to simulate different time-based validation scenarios.
func (m *MockValidatorClient) SetMedianBlockTime(medianTime uint32) error {
	m.MedianBlockTime = medianTime
	return nil
}

// GetMedianBlockTime returns the configured median block time.
// Used for timelock validation and time-based transaction rules.
func (m *MockValidatorClient) GetMedianBlockTime() uint32 {
	return m.MedianBlockTime
}

// Validate performs mock transaction validation with configurable error injection.
// Processes options and delegates to ValidateWithOptions.
func (m *MockValidatorClient) Validate(_ context.Context, tx *bt.Tx, blockHeight uint32, opts ...Option) (*meta.Data, error) {
	validationOptions := ProcessOptions(opts...)
	return m.ValidateWithOptions(context.Background(), tx, blockHeight, validationOptions)
}

// ValidateWithOptions performs mock transaction validation with error injection support.
// If errors are queued, returns the first error and removes it from the queue.
// Otherwise, creates UTXO entries using the configured UTXO store.
func (m *MockValidatorClient) ValidateWithOptions(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options) (txMetaData *meta.Data, err error) {
	m.ErrorsMu.Lock()
	defer m.ErrorsMu.Unlock()

	if len(m.Errors) > 0 {
		// return error and pop of stack
		err = m.Errors[0]
		m.Errors = m.Errors[1:]

		return nil, err
	}

	return m.UtxoStore.Create(context.Background(), tx, 0)
}

// TriggerBatcher implements the batcher trigger interface for testing.
// This is a no-op in the mock implementation as no actual batching occurs.
func (m *MockValidatorClient) TriggerBatcher() {}
