package nullstore

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
)

func TestNullStoreImplementsInterface(t *testing.T) {
	var _ utxo.Store = (*NullStore)(nil)
}

func TestNullStoreCreate(t *testing.T) {
	store := &NullStore{}
	tx := &bt.Tx{}
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

func TestNullStoreSetUnspendable(t *testing.T) {
	store := &NullStore{}
	ctx := context.Background()
	err := store.SetUnspendable(ctx, nil, true)

	assert.NoError(t, err)
}
