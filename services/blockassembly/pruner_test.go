package blockassembly

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestCleanupDuringStartup tests that cleanup runs before loading unmined transactions
func TestCleanupDuringStartup(t *testing.T) {
	initPrometheusMetrics()

	t.Run("cleanup runs before loading unmined transactions", func(t *testing.T) {
		ctx := context.Background()
		mockStore := new(utxo.MockUtxostore)
		logger := ulogger.TestLogger{}
		settings := test.CreateBaseTestSettings(t)
		settings.UtxoStore.UnminedTxRetention = 5

		// Setup expectations in order
		var iteratorCalled bool

		// Then iterator should be called
		mockIterator := new(MockUnminedTxIterator)
		mockStore.On("GetUnminedTxIterator", mock.Anything).
			Return(mockIterator, nil).
			Run(func(args mock.Arguments) {
				iteratorCalled = true
			}).
			Once()

		mockIterator.On("Next", mock.Anything).
			Return(nil, nil). // No unmined transactions
			Once()

		blockchainClient := &blockchain.Mock{}
		blockchainClient.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{0}, nil)
		blockchainClient.On("GetBlock", mock.Anything, mock.Anything).Return([]uint32{0}, nil)

		subtreeProcessor := &subtreeprocessor.MockSubtreeProcessor{}
		subtreeProcessor.On("GetCurrentBlockHeader").Return(blockHeader1, nil)

		// Create BlockAssembler with mocked dependencies
		ba := &BlockAssembler{
			utxoStore:        mockStore,
			logger:           logger,
			settings:         settings,
			bestBlock:        atomic.Pointer[BestBlockInfo]{},
			subtreeProcessor: subtreeProcessor,
			blockchainClient: blockchainClient,
		}

		// Set block height
		ba.setBestBlockHeader(nil, 100)

		// Call loadUnminedTransactions which includes cleanup
		err := ba.loadUnminedTransactions(ctx, false)

		require.NoError(t, err)
		assert.True(t, iteratorCalled)
		mockStore.AssertExpectations(t)
		mockIterator.AssertExpectations(t)
	})
}

// Mock types for testing

type MockUnminedTxIterator struct {
	mock.Mock
}

func (m *MockUnminedTxIterator) Next(ctx context.Context) ([]*utxo.UnminedTransaction, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]*utxo.UnminedTransaction), args.Error(1)
}

func (m *MockUnminedTxIterator) Err() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockUnminedTxIterator) Close() error {
	args := m.Called()
	return args.Error(0)
}

// TestLoadUnminedTransactionsExcludesConflicting tests that loadUnminedTransactions excludes conflicting transactions
func TestLoadUnminedTransactionsExcludesConflicting(t *testing.T) {
	initPrometheusMetrics()

	t.Run("conflicting transactions are excluded during loading", func(t *testing.T) {
		ctx := context.Background()
		mockStore := new(utxo.MockUtxostore)
		logger := ulogger.TestLogger{}
		settings := test.CreateBaseTestSettings(t)
		settings.UtxoStore.UnminedTxRetention = 5

		// Create mock transactions - only normal transaction should be returned by iterator
		normalTxHash := chainhash.DoubleHashH([]byte("normal-tx"))

		normalTxs := []*utxo.UnminedTransaction{{
			Node: &subtree.Node{
				Hash:        normalTxHash,
				Fee:         1000,
				SizeInBytes: 250,
			},
			TxInpoints: &subtree.TxInpoints{},
			CreatedAt:  1000,
		}}

		// Setup iterator expectations - iterator should only return non-conflicting transactions
		mockIterator := new(MockUnminedTxIterator)
		mockStore.On("GetUnminedTxIterator", mock.Anything).
			Return(mockIterator, nil).
			Once()

		// Iterator should only return normal transactions (conflicting ones are filtered out by the iterator)
		mockIterator.On("Next", mock.Anything).
			Return(normalTxs, nil).
			Once()

		// Second call returns nil (end of iteration)
		mockIterator.On("Next", mock.Anything).
			Return(nil, nil).
			Once()

		// Setup mock subtree processor
		mockSubtreeProcessor := &subtreeprocessor.MockSubtreeProcessor{}

		// Should only be called once for the normal transaction batch
		mockSubtreeProcessor.On("AddNodesDirectly", mock.MatchedBy(func(txs []*utxo.UnminedTransaction) bool {
			return len(txs) == 1 && txs[0].Hash.String() == normalTxs[0].Hash.String()
		}), true).Return(nil).Once()
		// GetCurrentBlockHeader may be called multiple times during loading
		mockSubtreeProcessor.On("GetCurrentBlockHeader").Return(blockHeader1, nil).Maybe()

		blockchainClient := &blockchain.Mock{}
		blockchainClient.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return([]uint32{0}, nil)

		// Create BlockAssembler with mocked dependencies
		ba := &BlockAssembler{
			utxoStore:        mockStore,
			logger:           logger,
			settings:         settings,
			subtreeProcessor: mockSubtreeProcessor,
			blockchainClient: blockchainClient,
		}

		// Set block height
		ba.setBestBlockHeader(nil, 100)

		// Call loadUnminedTransactions
		err := ba.loadUnminedTransactions(ctx, false)

		require.NoError(t, err)
		mockStore.AssertExpectations(t)
		mockIterator.AssertExpectations(t)
		mockSubtreeProcessor.AssertExpectations(t)
	})
}
