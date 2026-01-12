package utxo

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
)

// Simple test that covers the early return path to boost coverage
func TestPreserveParentsOfOldUnminedTransactions_EarlyReturn(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	settings := test.CreateBaseTestSettings(t)
	settings.UtxoStore.UnminedTxRetention = 10

	// Create a mock store (we won't use it because of early return)
	mockStore := new(MockUtxostore)

	// Test early return when block height is less than retention
	count, err := PreserveParentsOfOldUnminedTransactions(ctx, mockStore, 5, settings, logger)

	assert.NoError(t, err)
	assert.Equal(t, 0, count)
	// Should not call any store methods due to early return
	mockStore.AssertNotCalled(t, "GetUnminedTxIterator")
}

// Test the cutoff calculation logic
func TestCleanupCutoffCalculation(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	settings := test.CreateBaseTestSettings(t)
	settings.UtxoStore.UnminedTxRetention = 5

	mockStore := new(MockUtxostore)
	// Mock GetUnminedTxIterator to return empty iterator
	// Block height 15 - retention 5 = cutoff 10
	mockIter := &MockUnminedTxIterator{} // Returns nil, nil by default (empty)
	mockStore.On("GetUnminedTxIterator").Return(mockIter, nil)

	count, err := PreserveParentsOfOldUnminedTransactions(ctx, mockStore, 15, settings, logger)

	assert.NoError(t, err)
	assert.Equal(t, 0, count)
	mockStore.AssertExpectations(t)
}

// Test that covers the error path for storage errors
func TestPreserveParentsOfOldUnminedTransactions_StorageError(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	settings := test.CreateBaseTestSettings(t)
	settings.UtxoStore.UnminedTxRetention = 5

	mockStore := new(MockUtxostore)
	// Mock a storage error when getting iterator
	mockStore.On("GetUnminedTxIterator").
		Return((*MockUnminedTxIterator)(nil), errors.NewStorageError("storage error"))

	count, err := PreserveParentsOfOldUnminedTransactions(ctx, mockStore, 10, settings, logger)

	assert.Error(t, err)
	assert.Equal(t, 0, count)
	assert.Contains(t, err.Error(), "failed to get unmined tx iterator")
	mockStore.AssertExpectations(t)
}
