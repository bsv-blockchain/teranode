package nullstore

import (
	"context"
	"net/http"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	utxostore "github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

type NullStore struct {
	blockHeight     uint32
	medianBlockTime uint32
}

// BatchDecorate implements utxo.Store.
func (m *NullStore) BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*utxostore.UnresolvedMetaData, fields ...string) error {
	panic("unimplemented")
}

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

func (m *NullStore) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return http.StatusOK, "NullStore Store available", nil
}

func (m *NullStore) Get(_ context.Context, hash *chainhash.Hash, fields ...[]string) (*meta.Data, error) {
	return &meta.Data{}, nil
}

func (m *NullStore) GetSpend(ctx context.Context, spend *utxostore.Spend) (*utxostore.SpendResponse, error) {
	return nil, nil
}

func (m *NullStore) GetMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error) {
	return m.Get(ctx, hash)
}

func (m *NullStore) MetaBatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*utxostore.UnresolvedMetaData, fields ...string) error {
	return nil
}

func (m *NullStore) PreviousOutputsDecorate(ctx context.Context, outpoints []*meta.PreviousOutput) error {
	return nil
}

func (m *NullStore) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	return &meta.Data{}, nil
}

func (m *NullStore) Spend(ctx context.Context, tx *bt.Tx) ([]*utxo.Spend, error) {
	return nil, nil
}

func (m *NullStore) UnSpend(ctx context.Context, spends []*utxostore.Spend) error {
	return nil
}

func (m *NullStore) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error {
	return nil
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
