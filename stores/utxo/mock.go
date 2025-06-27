package utxo

import (
	"context"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/utxo/fields"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/mock"
)

// MockUtxostore provides a mock implementation of the Store interface for testing purposes.
type MockUtxostore struct {
	mock.Mock
}

func (m *MockUtxostore) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	args := m.Called(ctx, checkLiveness)
	return args.Int(0), args.String(1), args.Error(2)
}

func (m *MockUtxostore) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...CreateOption) (*meta.Data, error) {
	args := m.Called(ctx, tx, blockHeight, opts)
	return args.Get(0).(*meta.Data), args.Error(1)
}

func (m *MockUtxostore) Get(ctx context.Context, hash *chainhash.Hash, fields ...fields.FieldName) (*meta.Data, error) {
	args := m.Called(ctx, hash, fields)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*meta.Data), args.Error(1)
}

func (m *MockUtxostore) Delete(ctx context.Context, hash *chainhash.Hash) error {
	args := m.Called(ctx, hash)
	return args.Error(0)
}

func (m *MockUtxostore) GetSpend(ctx context.Context, spend *Spend) (*SpendResponse, error) {
	args := m.Called(ctx, spend)
	return args.Get(0).(*SpendResponse), args.Error(1)
}

func (m *MockUtxostore) GetMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error) {
	args := m.Called(ctx, hash)
	return args.Get(0).(*meta.Data), args.Error(1)
}

func (m *MockUtxostore) Spend(ctx context.Context, tx *bt.Tx, ignoreFlags ...IgnoreFlags) ([]*Spend, error) {
	args := m.Called(ctx, tx, ignoreFlags)
	return args.Get(0).([]*Spend), args.Error(1)
}

func (m *MockUtxostore) Unspend(ctx context.Context, spends []*Spend, flagAsUnspendable ...bool) error {
	args := m.Called(ctx, spends, flagAsUnspendable)
	return args.Error(0)
}

func (m *MockUtxostore) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo MinedBlockInfo) error {
	args := m.Called(ctx, hashes, minedBlockInfo)
	return args.Error(0)
}

func (m *MockUtxostore) GetUnminedTxIterator() (UnminedTxIterator, error) {
	args := m.Called()
	return args.Get(0).(UnminedTxIterator), args.Error(1)
}

func (m *MockUtxostore) BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*UnresolvedMetaData, fields ...fields.FieldName) error {
	args := m.Called(ctx, unresolvedMetaDataSlice, fields)
	return args.Error(0)
}

func (m *MockUtxostore) PreviousOutputsDecorate(ctx context.Context, tx *bt.Tx) error {
	args := m.Called(ctx, tx)
	return args.Error(0)
}

func (m *MockUtxostore) FreezeUTXOs(ctx context.Context, spends []*Spend, tSettings *settings.Settings) error {
	args := m.Called(ctx, spends, tSettings)
	return args.Error(0)
}

func (m *MockUtxostore) UnFreezeUTXOs(ctx context.Context, spends []*Spend, tSettings *settings.Settings) error {
	args := m.Called(ctx, spends, tSettings)
	return args.Error(0)
}

func (m *MockUtxostore) ReAssignUTXO(ctx context.Context, utxo *Spend, newUtxo *Spend, tSettings *settings.Settings) error {
	args := m.Called(ctx, utxo, newUtxo, tSettings)
	return args.Error(0)
}

func (m *MockUtxostore) GetCounterConflicting(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	args := m.Called(ctx, txHash)
	return args.Get(0).([]chainhash.Hash), args.Error(1)
}

func (m *MockUtxostore) GetConflictingChildren(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	args := m.Called(ctx, txHash)
	return args.Get(0).([]chainhash.Hash), args.Error(1)
}

func (m *MockUtxostore) SetConflicting(ctx context.Context, txHashes []chainhash.Hash, value bool) ([]*Spend, []chainhash.Hash, error) {
	args := m.Called(ctx, txHashes, value)
	return args.Get(0).([]*Spend), args.Get(1).([]chainhash.Hash), args.Error(2)
}

func (m *MockUtxostore) SetUnspendable(ctx context.Context, txHashes []chainhash.Hash, value bool) error {
	args := m.Called(ctx, txHashes, value)
	return args.Error(0)
}

func (m *MockUtxostore) SetBlockHeight(height uint32) error {
	args := m.Called(height)
	return args.Error(0)
}

func (m *MockUtxostore) GetBlockHeight() uint32 {
	args := m.Called()
	return args.Get(0).(uint32)
}

func (m *MockUtxostore) SetMedianBlockTime(height uint32) error {
	args := m.Called(height)
	return args.Error(0)
}

func (m *MockUtxostore) GetMedianBlockTime() uint32 {
	args := m.Called()
	return args.Get(0).(uint32)
}

func (m *MockUtxostore) QueryOldUnminedTransactions(ctx context.Context, cutoffBlockHeight uint32) ([]chainhash.Hash, error) {
	args := m.Called(ctx, cutoffBlockHeight)
	return args.Get(0).([]chainhash.Hash), args.Error(1)
}

func (m *MockUtxostore) PreserveTransactions(ctx context.Context, txIDs []chainhash.Hash, preserveUntilHeight uint32) error {
	args := m.Called(ctx, txIDs, preserveUntilHeight)
	return args.Error(0)
}

func (m *MockUtxostore) ProcessExpiredPreservations(ctx context.Context, currentHeight uint32) error {
	args := m.Called(ctx, currentHeight)
	return args.Error(0)
}
