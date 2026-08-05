package nullstore

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/assert"
)

func TestNullStoreImplementsInterface(t *testing.T) {
	var _ utxo.Store = (*NullStore)(nil)
}

func TestNullStoreCreate(t *testing.T) {
	store := &NullStore{}
	tx := &bt.Tx{}
	_ = tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      &chainhash.Hash{},
		Vout:          0,
		LockingScript: &bscript.Script{},
		Satoshis:      100,
	})
	ctx := context.Background()
	data, err := store.Create(ctx, tx, 0)

	assert.NoError(t, err)
	assert.NotNil(t, data)
}

func TestNullStoreGet(t *testing.T) {
	store := &NullStore{}
	ctx := context.Background()
	hash := &chainhash.Hash{}
	data, err := store.Get(ctx, hash)

	assert.NoError(t, err)
	assert.NotNil(t, data)
}

func TestNullStoreSetLocked(t *testing.T) {
	store := &NullStore{}
	ctx := context.Background()
	err := store.SetLocked(ctx, nil, true)

	assert.NoError(t, err)
}

func TestNullStoreCreateFirstNoOps(t *testing.T) {
	store := &NullStore{}
	ctx := context.Background()
	tx := &bt.Tx{}

	assert.False(t, store.SupportsCreateFirst())
	assert.NoError(t, store.FinalizeTransaction(ctx, tx))

	hashes, err := store.QueryStaleCreatingTxs(ctx, 100, 0)
	assert.NoError(t, err)
	assert.Nil(t, hashes)
}
