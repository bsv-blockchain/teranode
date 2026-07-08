/*
Package validator implements BSV Blockchain transaction validation functionality.

This package provides comprehensive transaction validation for BSV Blockchain nodes,
including BDK transaction validation, UTXO management, and policy enforcement.

Key features:
  - Transaction validation against Bitcoin consensus rules
  - UTXO spending and creation
  - BDK transaction validation
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

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util"
)

// Ensure MockValidator satisfies the validator Interface.
var _ Interface = &MockValidator{}

// MockValidator is the canonical test double for the validator Interface. It provides
// controllable behavior for testing validator integration scenarios, allowing tests to
// simulate various validation states, errors, and blockchain conditions.
//
// Validation behavior (in precedence order):
//   - ValidateFunc, if set, is invoked directly.
//   - Otherwise, if Errors is non-empty, the first queued error is returned and popped.
//   - Otherwise, if UtxoStore is set, the transaction is created in that store.
//   - Otherwise, transaction metadata is derived directly from the transaction.
//
// The mock is safe for concurrent use of the error queue.
type MockValidator struct {
	// BlockHeight represents the current blockchain height for validation context.
	BlockHeight uint32

	// MedianBlockTime represents the median time of recent blocks for timelock validation.
	MedianBlockTime uint32

	// Errors contains a list of errors to return from validation operations.
	Errors []error

	// ErrorsMu provides thread-safe access to the Errors slice.
	ErrorsMu sync.Mutex

	// UtxoStore, when set, is used to create UTXO entries during validation.
	UtxoStore utxo.Store

	// ValidateFunc, when set, overrides Validate/ValidateWithOptions behavior entirely.
	ValidateFunc func(ctx context.Context, tx *bt.Tx) (*meta.Data, error)

	// HealthFunc, when set, overrides Health behavior.
	HealthFunc func() (int, string, error)
}

// Health returns the configured health status, or a successful default.
func (m *MockValidator) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if m.HealthFunc != nil {
		return m.HealthFunc()
	}

	return 0, "MockValidator", nil
}

// SetBlockHeight updates the mock's blockchain height for validation context.
func (m *MockValidator) SetBlockHeight(blockHeight uint32) error {
	m.BlockHeight = blockHeight
	return nil
}

// GetBlockHeight returns the configured blockchain height.
func (m *MockValidator) GetBlockHeight() uint32 {
	return m.BlockHeight
}

// SetMedianBlockTime updates the mock's median block time for timelock validation.
func (m *MockValidator) SetMedianBlockTime(medianTime uint32) error {
	m.MedianBlockTime = medianTime
	return nil
}

// GetMedianBlockTime returns the configured median block time.
func (m *MockValidator) GetMedianBlockTime() uint32 {
	return m.MedianBlockTime
}

// Validate performs mock transaction validation, delegating to ValidateWithOptions.
func (m *MockValidator) Validate(_ context.Context, tx *bt.Tx, blockHeight uint32, opts ...Option) (*meta.Data, error) {
	return m.ValidateWithOptions(context.Background(), tx, blockHeight, ProcessOptions(opts...))
}

// ValidateWithOptions performs mock transaction validation. See MockValidator for the
// precedence of ValidateFunc, the error queue, the UTXO store, and the tx-derived default.
func (m *MockValidator) ValidateWithOptions(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options) (*meta.Data, error) {
	if m.ValidateFunc != nil {
		return m.ValidateFunc(ctx, tx)
	}

	m.ErrorsMu.Lock()
	defer m.ErrorsMu.Unlock()

	if len(m.Errors) > 0 {
		// return error and pop off the stack
		err := m.Errors[0]
		m.Errors = m.Errors[1:]

		return nil, err
	}

	if m.UtxoStore != nil {
		return m.UtxoStore.Create(context.Background(), tx, 0)
	}

	return util.TxMetaDataFromTx(tx)
}

// TriggerBatcher is a no-op in the mock implementation as no actual batching occurs.
func (m *MockValidator) TriggerBatcher() {}

// EnsureMTPLoaded is a no-op in the mock implementation as no actual MTP loading occurs.
func (m *MockValidator) EnsureMTPLoaded(_ context.Context, _ uint32) error {
	return nil
}
