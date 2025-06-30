package blockassembly

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestStartUnminedTransactionCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("starts and stops cleanup ticker", func(t *testing.T) {
		mockStore := new(utxo.MockUtxostore)
		logger := ulogger.TestLogger{}
		settings := test.CreateBaseTestSettings()

		ba := &BlockAssembler{
			utxoStore:       mockStore,
			logger:          logger,
			settings:        settings,
			bestBlockHeight: atomic.Uint32{},
		}

		// Set block height to trigger cleanup
		ba.bestBlockHeight.Store(100)

		// Expect at least one cleanup call
		mockStore.On("QueryOldUnminedTransactions", mock.Anything, mock.Anything).
			Return([]chainhash.Hash{}, nil).
			Maybe() // May or may not be called depending on timing

		// Start cleanup
		ba.startUnminedTransactionCleanup(ctx)

		// Give it a moment to potentially run
		time.Sleep(50 * time.Millisecond)

		// Cancel context to stop
		cancel()

		// Give it time to stop
		time.Sleep(50 * time.Millisecond)
	})

	t.Run("does not cleanup when block height is 0", func(t *testing.T) {
		ctx := context.Background()
		mockStore := new(utxo.MockUtxostore)
		logger := ulogger.TestLogger{}
		settings := test.CreateBaseTestSettings()

		ba := &BlockAssembler{
			utxoStore:       mockStore,
			logger:          logger,
			settings:        settings,
			bestBlockHeight: atomic.Uint32{},
		}

		// Block height is 0
		ba.bestBlockHeight.Store(0)

		// Should not call cleanup
		mockStore.AssertNotCalled(t, "QueryOldUnminedTransactions")

		// Create a short-lived context
		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		ba.startUnminedTransactionCleanup(ctx)

		// Wait for context to expire
		<-ctx.Done()
		time.Sleep(50 * time.Millisecond)

		// Verify no cleanup was called
		mockStore.AssertNotCalled(t, "QueryOldUnminedTransactions")
	})
}

// TestCleanupDuringStartup tests that cleanup runs before loading unmined transactions
func TestCleanupDuringStartup(t *testing.T) {
	t.Run("cleanup runs before loading unmined transactions", func(t *testing.T) {
		ctx := context.Background()
		mockStore := new(utxo.MockUtxostore)
		logger := ulogger.TestLogger{}
		settings := test.CreateBaseTestSettings()
		settings.UtxoStore.UnminedTxRetention = 5

		// Setup expectations in order
		var cleanupCalled, iteratorCalled bool

		// Cleanup should be called first
		mockStore.On("QueryOldUnminedTransactions", mock.Anything, uint32(95)). // 100 - 5
											Return([]chainhash.Hash{}, nil).
											Run(func(args mock.Arguments) {
				cleanupCalled = true
				assert.False(t, iteratorCalled, "Cleanup should be called before iterator")
			}).
			Once()

		// Then iterator should be called
		mockIterator := new(MockUnminedTxIterator)
		mockStore.On("GetUnminedTxIterator").
			Return(mockIterator, nil).
			Run(func(args mock.Arguments) {
				iteratorCalled = true
				assert.True(t, cleanupCalled, "Iterator should be called after cleanup")
			}).
			Once()

		mockIterator.On("Next", mock.Anything).
			Return(nil, nil). // No unmined transactions
			Once()

		// Create BlockAssembler with mocked dependencies
		ba := &BlockAssembler{
			utxoStore:        mockStore,
			logger:           logger,
			settings:         settings,
			bestBlockHeight:  atomic.Uint32{},
			subtreeProcessor: &subtreeprocessor.MockSubtreeProcessor{}, // Add a mock subtree processor
		}

		// Set block height
		ba.bestBlockHeight.Store(100)

		// Call loadUnminedTransactions which includes cleanup
		err := ba.loadUnminedTransactions(ctx)

		require.NoError(t, err)
		assert.True(t, cleanupCalled)
		assert.True(t, iteratorCalled)
		mockStore.AssertExpectations(t)
		mockIterator.AssertExpectations(t)
	})
}

// Mock types for testing

type MockUnminedTxIterator struct {
	mock.Mock
}

func (m *MockUnminedTxIterator) Next(ctx context.Context) (*utxo.UnminedTransaction, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*utxo.UnminedTransaction), args.Error(1)
}

func (m *MockUnminedTxIterator) Err() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockUnminedTxIterator) Close() error {
	args := m.Called()
	return args.Error(0)
}
