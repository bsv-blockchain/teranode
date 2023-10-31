package nullstore

import (
	"context"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

type NullStore struct {
}

func New() *NullStore {
	return &NullStore{}
}

func (m *NullStore) Get(_ context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	status := txmeta.Data{
		// Fee:            fee,
		// SizeInBytes:    sizeInBytes,
		// FirstSeen:      time.Now(),
		// ParentTxHashes: parentTxHashes,
		// UtxoHashes:     utxoHashes,
		// LockTime:       nLockTime,
	}
	return &status, nil
}

func (m *NullStore) Create(_ context.Context, tx *bt.Tx) (*txmeta.Data, error) {
	txMeta, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return txMeta, err
	}
	return txMeta, nil
}

func (m *NullStore) SetMined(_ context.Context, hash *chainhash.Hash, blockHash *chainhash.Hash) error {
	return nil
}

func (m *NullStore) Delete(_ context.Context, hash *chainhash.Hash) error {
	return nil
}
