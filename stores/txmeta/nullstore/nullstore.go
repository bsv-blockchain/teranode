package nullstore

import (
	"context"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

type NullStore struct {
}

func New(_ ulogger.Logger) *NullStore {
	return &NullStore{}
}

func (m *NullStore) GetMeta(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	return m.Get(ctx, hash)
}

func (m *NullStore) Get(_ context.Context, _ *chainhash.Hash) (*txmeta.Data, error) {
	status := txmeta.Data{}
	return &status, nil
}

func (m *NullStore) GetMulti(ctx context.Context, hashes []*chainhash.Hash, fields ...string) (map[chainhash.Hash]*txmeta.Data, error) {
	results := make(map[chainhash.Hash]*txmeta.Data, len(hashes))

	// TODO make this into a batch call
	for _, hash := range hashes {
		data, err := m.Get(ctx, hash)
		if err != nil {
			return nil, err
		}
		results[*hash] = data
	}

	return results, nil
}

func (m *NullStore) Create(_ context.Context, tx *bt.Tx) (*txmeta.Data, error) {
	txMeta, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return txMeta, err
	}
	return txMeta, nil
}

func (m *NullStore) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) (err error) {
	return nil
}

func (m *NullStore) SetMined(_ context.Context, hash *chainhash.Hash, blockID uint32) error {
	return nil
}

func (m *NullStore) Delete(_ context.Context, hash *chainhash.Hash) error {
	return nil
}
