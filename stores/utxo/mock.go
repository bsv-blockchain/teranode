// Package utxo provides mock implementations for testing UTXO store functionality.
// This file contains mock objects that implement the Store interface using testify/mock,
// enabling comprehensive unit testing of components that depend on UTXO storage.
package utxo

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/mock"
)

// MockUtxostore provides a mock implementation of the Store interface for testing purposes.
//
// This mock allows tests to verify interactions with UTXO storage operations without
// requiring an actual database backend. It uses testify/mock to record method calls
// and return predefined responses, enabling comprehensive testing of business logic
// that depends on UTXO operations.
//
// Usage:
//
//	mockStore := &MockUtxostore{}
//	mockStore.On("Get", mock.Anything, mock.Anything).Return(expectedData, nil)
//	// Use mockStore in place of real Store implementation
//	mockStore.AssertExpectations(t)
type MockUtxostore struct {
	mock.Mock
}

// Health mocks the health check functionality of the UTXO store.
// Returns the configured mock response for health status checks.
func (m *MockUtxostore) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	args := m.Called(ctx, checkLiveness)
	return args.Int(0), args.String(1), args.Error(2)
}

// Create mocks the creation of transaction metadata in the UTXO store.
// Returns the configured mock response for transaction creation operations.
func (m *MockUtxostore) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...CreateOption) (*meta.Data, error) {
	args := m.Called(ctx, tx, blockHeight, opts)
	return args.Get(0).(*meta.Data), args.Error(1)
}

// Get mocks the retrieval of transaction metadata from the UTXO store.
// Returns the configured mock response for transaction lookup operations.
func (m *MockUtxostore) Get(ctx context.Context, hash *chainhash.Hash, fields ...fields.FieldName) (*meta.Data, error) {
	args := m.Called(ctx, hash, fields)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*meta.Data), args.Error(1)
}

// Delete mocks the deletion of transaction metadata from the UTXO store.
// Returns the configured mock response for transaction deletion operations.
func (m *MockUtxostore) Delete(ctx context.Context, hash *chainhash.Hash) error {
	args := m.Called(ctx, hash)
	return args.Error(0)
}

// GetSpend mocks the retrieval of spend information from the UTXO store.
// Returns the configured mock response for spend lookup operations.
func (m *MockUtxostore) GetSpend(ctx context.Context, spend *Spend) (*SpendResponse, error) {
	args := m.Called(ctx, spend)
	return args.Get(0).(*SpendResponse), args.Error(1)
}

// GetMeta mocks the retrieval of complete transaction metadata from the UTXO store.
// Returns the configured mock response for full metadata lookup operations.
func (m *MockUtxostore) GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	args := m.Called(ctx, hash, data)
	if result := args.Get(0); result != nil {
		*data = *result.(*meta.Data)
	}
	return args.Error(0)
}

// Spend mocks the spending of transaction outputs in the UTXO store.
// Returns the configured mock response for transaction spending operations.
func (m *MockUtxostore) Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...IgnoreFlags) ([]*Spend, error) {
	args := m.Called(ctx, tx, blockHeight, ignoreFlags)
	return args.Get(0).([]*Spend), args.Error(1)
}

// Unspend mocks the reversal of transaction output spending in the UTXO store.
// Returns the configured mock response for transaction unspending operations.
func (m *MockUtxostore) Unspend(ctx context.Context, spends []*Spend, flagAsLocked ...bool) error {
	args := m.Called(ctx, spends, flagAsLocked)
	return args.Error(0)
}

// SetMinedMulti mocks the batch setting of mined status for multiple transactions.
// Returns the configured mock response for batch mined status operations.
func (m *MockUtxostore) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	args := m.Called(ctx, hashes, minedBlockInfo)

	return args.Get(0).(map[chainhash.Hash][]uint32), args.Error(1)
}

// GetUnminedTxIterator mocks the creation of an iterator for unmined transactions.
// Returns the configured mock response for unmined transaction iteration.
func (m *MockUtxostore) GetUnminedTxIterator(bool) (UnminedTxIterator, error) {
	args := m.Called()
	return args.Get(0).(UnminedTxIterator), args.Error(1)
}

// BatchDecorate mocks the batch decoration of unresolved metadata with field data.
// Returns the configured mock response for batch metadata decoration operations.
func (m *MockUtxostore) BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*UnresolvedMetaData, fields ...fields.FieldName) error {
	args := m.Called(ctx, unresolvedMetaDataSlice, fields)
	return args.Error(0)
}

// PreviousOutputsDecorate mocks the decoration of transaction inputs with previous output data.
// Returns the configured mock response for previous output decoration operations.
func (m *MockUtxostore) PreviousOutputsDecorate(ctx context.Context, tx *bt.Tx) error {
	args := m.Called(ctx, tx)
	return args.Error(0)
}

// FreezeUTXOs mocks the freezing of UTXOs to prevent their spending.
// Returns the configured mock response for UTXO freezing operations.
func (m *MockUtxostore) FreezeUTXOs(ctx context.Context, spends []*Spend, tSettings *settings.Settings) error {
	args := m.Called(ctx, spends, tSettings)
	return args.Error(0)
}

// UnFreezeUTXOs mocks the unfreezing of previously frozen UTXOs.
// Returns the configured mock response for UTXO unfreezing operations.
func (m *MockUtxostore) UnFreezeUTXOs(ctx context.Context, spends []*Spend, tSettings *settings.Settings) error {
	args := m.Called(ctx, spends, tSettings)
	return args.Error(0)
}

// ReAssignUTXO mocks the reassignment of a UTXO to a new transaction output.
// Returns the configured mock response for UTXO reassignment operations.
func (m *MockUtxostore) ReAssignUTXO(ctx context.Context, utxo *Spend, newUtxo *Spend, tSettings *settings.Settings) error {
	args := m.Called(ctx, utxo, newUtxo, tSettings)
	return args.Error(0)
}

// GetCounterConflicting mocks the retrieval of transactions that conflict with the given transaction.
// Returns the configured mock response for transaction conflict detection operations.
func (m *MockUtxostore) GetCounterConflicting(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	args := m.Called(ctx, txHash)
	return args.Get(0).([]chainhash.Hash), args.Error(1)
}

// GetConflictingChildren mocks the retrieval of child transactions that conflict with the given transaction.
// Returns the configured mock response for conflicting child transaction detection operations.
func (m *MockUtxostore) GetConflictingChildren(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	args := m.Called(ctx, txHash)
	return args.Get(0).([]chainhash.Hash), args.Error(1)
}

// SetConflicting mocks the setting of conflicting status for multiple transactions.
// Returns the configured mock response for transaction conflict status operations.
func (m *MockUtxostore) SetConflicting(ctx context.Context, txHashes []chainhash.Hash, value bool) ([]*Spend, []chainhash.Hash, error) {
	args := m.Called(ctx, txHashes, value)
	return args.Get(0).([]*Spend), args.Get(1).([]chainhash.Hash), args.Error(2)
}

// SetLocked mocks the setting of locked status for multiple transactions.
// Returns the configured mock response for transaction locking operations.
func (m *MockUtxostore) SetLocked(ctx context.Context, txHashes []chainhash.Hash, value bool) error {
	args := m.Called(ctx, txHashes, value)
	return args.Error(0)
}

// MarkTransactionsOnLongestChain mocks the marking of transactions as being on the longest chain.
// Returns the configured mock response for longest chain marking operations.
func (m *MockUtxostore) MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash, onLongestChain bool) error {
	args := m.Called(ctx, txHashes, onLongestChain)
	return args.Error(0)
}

// SetBlockHeight mocks the setting of the current block height in the UTXO store.
// Returns the configured mock response for block height update operations.
func (m *MockUtxostore) SetBlockHeight(height uint32) error {
	args := m.Called(height)
	return args.Error(0)
}

// GetBlockHeight mocks the retrieval of the current block height from the UTXO store.
// Returns the configured mock response for block height queries.
func (m *MockUtxostore) GetBlockHeight() uint32 {
	args := m.Called()
	return args.Get(0).(uint32)
}

// SetMedianBlockTime mocks the setting of median block time for the given height.
// Returns the configured mock response for median block time update operations.
func (m *MockUtxostore) SetMedianBlockTime(height uint32) error {
	args := m.Called(height)
	return args.Error(0)
}

// GetMedianBlockTime mocks the retrieval of the median block time from the UTXO store.
// Returns the configured mock response for median block time queries.
func (m *MockUtxostore) GetMedianBlockTime() uint32 {
	args := m.Called()
	return args.Get(0).(uint32)
}

// GetBlockState mocks the retrieval of an atomic snapshot of both block height and median block time.
// Returns the configured mock response for block state queries.
func (m *MockUtxostore) GetBlockState() BlockState {
	args := m.Called()
	return args.Get(0).(BlockState)
}

// QueryOldUnminedTransactions mocks the querying of old unmined transactions before a cutoff height.
// Returns the configured mock response for old unmined transaction queries.
func (m *MockUtxostore) QueryOldUnminedTransactions(ctx context.Context, cutoffBlockHeight uint32) ([]chainhash.Hash, error) {
	args := m.Called(ctx, cutoffBlockHeight)
	return args.Get(0).([]chainhash.Hash), args.Error(1)
}

// PreserveTransactions mocks the preservation of transactions until a specified block height.
// Returns the configured mock response for transaction preservation operations.
func (m *MockUtxostore) PreserveTransactions(ctx context.Context, txIDs []chainhash.Hash, preserveUntilHeight uint32) error {
	args := m.Called(ctx, txIDs, preserveUntilHeight)
	return args.Error(0)
}

// ProcessExpiredPreservations mocks the processing of expired transaction preservations.
// Returns the configured mock response for expired preservation cleanup operations.
func (m *MockUtxostore) ProcessExpiredPreservations(ctx context.Context, currentHeight uint32) error {
	args := m.Called(ctx, currentHeight)
	return args.Error(0)
}

// MockUnminedTxIterator is a simple mock implementation of utxo.UnminedTxIterator for testing
type MockUnminedTxIterator struct {
	mock.Mock
}

func (m *MockUnminedTxIterator) Next(ctx context.Context) ([]*UnminedTransaction, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*UnminedTransaction), args.Error(1)
}

func (m *MockUnminedTxIterator) Err() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockUnminedTxIterator) Close() error {
	args := m.Called()
	return args.Error(0)
}
