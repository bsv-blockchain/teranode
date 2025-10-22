package rpc

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/mock"
)

// MockPropagationClient implements a mock propagation client for testing
type MockPropagationClient struct {
	mock.Mock
	ProcessTransactionCallCount int32
	ProcessTransactionResults   map[string]error // Map TxID to error result
	ProcessingDelay             time.Duration    // Delay to simulate processing time
	mu                          sync.RWMutex     // Protect concurrent access
}

func NewMockPropagationClient() *MockPropagationClient {
	return &MockPropagationClient{
		ProcessTransactionResults: make(map[string]error),
	}
}

// ProcessTransaction mocks the propagation client's ProcessTransaction method
func (m *MockPropagationClient) ProcessTransaction(ctx context.Context, tx *bt.Tx) error {
	m.mu.Lock()
	m.ProcessTransactionCallCount++
	m.mu.Unlock()

	// Check if we should simulate processing delay
	if m.ProcessingDelay > 0 {
		select {
		case <-time.After(m.ProcessingDelay):
			// Normal delay completion
		case <-ctx.Done():
			// Context was cancelled during delay
			return ctx.Err()
		}
	}

	args := m.Called(ctx, tx)

	// Check for pre-configured results based on TxID
	txID := tx.TxID()
	m.mu.RLock()
	if result, exists := m.ProcessTransactionResults[txID]; exists {
		m.mu.RUnlock()
		return result
	}
	m.mu.RUnlock()

	return args.Error(0)
}

// TriggerBatcher mocks the TriggerBatcher method
func (m *MockPropagationClient) TriggerBatcher() {
	m.Called()
}

// SetProcessTransactionResult sets a specific result for a given transaction ID
func (m *MockPropagationClient) SetProcessTransactionResult(txID string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ProcessTransactionResults[txID] = err
}

// GetProcessTransactionCallCount returns the number of times ProcessTransaction was called
func (m *MockPropagationClient) GetProcessTransactionCallCount() int32 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ProcessTransactionCallCount
}

// SetProcessingDelay sets the delay to simulate processing time
func (m *MockPropagationClient) SetProcessingDelay(delay time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ProcessingDelay = delay
}

// CreateTestTransaction creates a test Bitcoin transaction
func CreateTestTransaction() *bt.Tx {
	// Just create a new empty transaction - this is sufficient for most tests
	return bt.NewTx()
}

// CreateTestTransactionWithTxID creates a test transaction with a specific pattern for predictable TxID
func CreateTestTransactionWithTxID(seed byte) *bt.Tx {
	// Create a simple transaction with some variation based on seed
	tx := bt.NewTx()
	// For testing purposes, just use different version numbers based on seed
	tx.Version = uint32(seed)
	return tx
}

func CreateTestSettings(addresses []string) *settings.Settings {
	return &settings.Settings{
		Propagation: settings.PropagationSettings{
			GRPCAddresses: addresses,
		},
	}
}

// ErrorSimulator provides utility functions for simulating various error conditions
type ErrorSimulator struct{}

// CreateTxInvalidError creates a transaction invalid error for testing
func (e *ErrorSimulator) CreateTxInvalidError() error {
	return errors.NewTxInvalidError("test transaction invalid")
}

// CreateNetworkError creates a network error for testing
func (e *ErrorSimulator) CreateNetworkError() error {
	return errors.NewNetworkError("test network error")
}

// CreateProcessingError creates a processing error for testing
func (e *ErrorSimulator) CreateProcessingError() error {
	return errors.NewProcessingError("test processing error")
}

// CreateTimeoutError creates a timeout error for testing
func (e *ErrorSimulator) CreateTimeoutError() error {
	return errors.NewNetworkError("test timeout error")
}
