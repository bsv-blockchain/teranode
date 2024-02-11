package nullstore

import (
	"context"
	"github.com/libsv/go-bt/v2/chainhash"

	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/libsv/go-bt/v2"
)

type NullStore struct {
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

func (m *NullStore) Get(_ context.Context, spend *utxostore.Spend) (*utxostore.Response, error) {
	return &utxostore.Response{
		Status: int(utxostore.Status_OK),
	}, nil
}

func (m *NullStore) Store(_ context.Context, tx *bt.Tx, lockTime ...uint32) error {
	return nil
}

func (m *NullStore) StoreFromHashes(_ context.Context, txID chainhash.Hash, hashes []chainhash.Hash, lockTime uint32) error {
	return nil
}

func (m *NullStore) Spend(_ context.Context, spend []*utxostore.Spend) error {
	return nil
}

func (m *NullStore) UnSpend(ctx context.Context, spends []*utxostore.Spend) error {
	return nil
}

func (m *NullStore) Delete(_ context.Context, tx *bt.Tx) error {
	return nil
}

func (m *NullStore) DeleteSpends(deleteSpends bool) {
}
