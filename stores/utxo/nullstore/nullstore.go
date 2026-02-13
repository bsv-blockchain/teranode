// Package nullstore provides a null implementation of the UTXO store interface.
//
// The NullStore is a no-op implementation that discards all operations and returns
// empty results. It is primarily used for testing scenarios where UTXO storage
// functionality needs to be disabled or mocked out without affecting the rest
// of the system.
//
// All store operations succeed immediately without performing any actual storage,
// making it useful for performance testing or when UTXO tracking is not required.
package nullstore

import (
	"context"
	"net/http"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util"
)

// NullStore implements the utxo.Store interface with no-op operations.
// All methods succeed immediately without performing any actual storage operations.
// This is useful for testing scenarios or when UTXO tracking needs to be disabled.
type NullStore struct {
	blockHeight     uint32
	medianBlockTime uint32
}

// BatchDecorate implements utxo.Store.
func (m *NullStore) BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*utxo.UnresolvedMetaData, fields ...fields.FieldName) error {
	return nil
}

// NewNullStore creates a new NullStore instance.
// Returns a null UTXO store that implements all interface methods as no-ops.
func NewNullStore() (*NullStore, error) {
	return &NullStore{}, nil
}

func (m *NullStore) SetBlockHeight(height uint32) error {
	m.blockHeight = height
	return nil
}

func (m *NullStore) GetBlockHeight() uint32 {
	return m.blockHeight
}

func (m *NullStore) SetMedianBlockTime(medianTime uint32) error {
	m.medianBlockTime = medianTime
	return nil
}

func (m *NullStore) GetMedianBlockTime() uint32 {
	return m.medianBlockTime
}

func (m *NullStore) GetBlockState() utxo.BlockState {
	return utxo.BlockState{
		Height:     m.blockHeight,
		MedianTime: m.medianBlockTime,
	}
}

func (m *NullStore) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return http.StatusOK, "NullStore Store available", nil
}

func (m *NullStore) Get(ctx context.Context, hash *chainhash.Hash, fields ...fields.FieldName) (*meta.Data, error) {
	return &meta.Data{}, nil
}

func (m *NullStore) GetSpend(ctx context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	return nil, nil
}

func (m *NullStore) GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	result, err := m.Get(ctx, hash)
	if err != nil {
		return err
	}

	if result != nil {
		*data = *result
	}

	return nil
}

func (m *NullStore) MetaBatchDecorate(_ context.Context, _ []*utxo.UnresolvedMetaData, _ ...string) error {
	return nil
}

func (m *NullStore) PreviousOutputsDecorate(_ context.Context, tx *bt.Tx) error {
	for _, input := range tx.Inputs {
		if input == nil {
			continue
		}

		input.PreviousTxScript = bscript.NewFromBytes([]byte("test"))
		input.PreviousTxSatoshis = 100_000_000_000
	}

	return nil
}

func (m *NullStore) Create(_ context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	options := &utxo.CreateOptions{}
	for _, opt := range opts {
		opt(options)
	}

	txMetaData, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return nil, err
	}

	if options.IsCoinbase != nil {
		txMetaData.IsCoinbase = *options.IsCoinbase
	}

	if options.Conflicting {
		txMetaData.Conflicting = true
	}

	if options.Locked {
		txMetaData.Locked = true
	}

	return txMetaData, nil
}

func (m *NullStore) Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	if blockHeight == 0 {
		return nil, errors.NewProcessingError("blockHeight must be greater than zero")
	}

	return nil, nil
}

func (m *NullStore) Unspend(ctx context.Context, spends []*utxo.Spend, flagAsLocked ...bool) error {
	return nil
}

func (m *NullStore) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	return nil, nil
}

func (m *NullStore) GetUnminedTxIterator(bool) (utxo.UnminedTxIterator, error) {
	return nil, nil
}

func (m *NullStore) GetPrunableUnminedTxIterator(cutoffBlockHeight uint32) (utxo.UnminedTxIterator, error) {
	return nil, nil
}

func (m *NullStore) Delete(ctx context.Context, hash *chainhash.Hash) error {
	return nil
}

func (m *NullStore) FreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	return nil
}

func (m *NullStore) UnFreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	return nil
}

func (m *NullStore) ReAssignUTXO(ctx context.Context, utxo *utxo.Spend, newUtxo *utxo.Spend, tSettings *settings.Settings) error {
	return nil
}

func (m *NullStore) GetCounterConflicting(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	return nil, nil
}

func (m *NullStore) GetConflictingChildren(_ context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	return nil, nil
}

func (m *NullStore) SetConflicting(ctx context.Context, txHashes []chainhash.Hash, setValue bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	return nil, nil, nil
}

func (m *NullStore) SetLocked(ctx context.Context, txHashes []chainhash.Hash, setValue bool) error {
	return nil
}

func (m *NullStore) MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash, onLongestChain bool) error {
	return nil
}

func (m *NullStore) QueryOldUnminedTransactions(ctx context.Context, cutoffBlockHeight uint32) ([]chainhash.Hash, error) {
	return []chainhash.Hash{}, nil
}

func (m *NullStore) PreserveTransactions(ctx context.Context, txIDs []chainhash.Hash, preserveUntilHeight uint32) error {
	return nil
}

func (m *NullStore) ProcessExpiredPreservations(ctx context.Context, currentHeight uint32) error {
	return nil
}
