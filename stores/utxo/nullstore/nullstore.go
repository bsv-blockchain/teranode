package nullstore

import (
	"context"

	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/libsv/go-bt/v2/chainhash"
)

type NullStore struct {
}

func NewNullStore() (*NullStore, error) {
	return &NullStore{}, nil
}

func (m *NullStore) SetBlockHeight(height uint32) error {
	return nil
}

func (m *NullStore) Health(ctx context.Context) (int, string, error) {
	return 0, "NullStore Store", nil
}

func (m *NullStore) Get(_ context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_OK),
	}, nil
}

func (m *NullStore) Store(_ context.Context, hash *chainhash.Hash, nLockTime uint32) (*utxostore.UTXOResponse, error) {
	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_OK),
	}, nil
}

func (m *NullStore) BatchStore(ctx context.Context, hashes []*chainhash.Hash, nLockTime uint32) (*utxostore.BatchResponse, error) {
	return &utxostore.BatchResponse{
		Status: 0,
	}, nil
}

func (m *NullStore) Spend(_ context.Context, hash *chainhash.Hash, txID *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_OK),
	}, nil
}

func (m *NullStore) Reset(ctx context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_OK),
	}, nil
}

func (m *NullStore) Delete(_ context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_OK),
	}, nil
}

func (m *NullStore) DeleteSpends(deleteSpends bool) {
}
