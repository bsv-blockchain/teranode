package nullstore

import (
	"context"

	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/libsv/go-bt/v2/chainhash"

	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/libsv/go-bt/v2"
)

type NullStore struct {
}

// BatchDecorate implements utxo.Store.
func (m *NullStore) BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*utxostore.UnresolvedMetaData, fields ...string) error {
	panic("unimplemented")
}

func NewNullStore() (*NullStore, error) {
	return &NullStore{}, nil
}

func (m *NullStore) SetBlockHeight(height uint32) error {
	return nil
}

func (m *NullStore) GetBlockHeight() (uint32, error) {
	return 0, nil
}

func (m *NullStore) Health(ctx context.Context) (int, string, error) {
	return 0, "NullStore Store", nil
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

func (m *NullStore) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, blockIDs ...uint32) (*meta.Data, error) {
	return &meta.Data{}, nil
}

func (m *NullStore) Spend(_ context.Context, spend []*utxostore.Spend, blockHeight uint32) error {
	return nil
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
