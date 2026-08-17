package nullstore

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
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

// TestNullStoreBlockState runs the shared block-state contract cases against the
// null store. It is the only store the shared suite did not cover, which is how
// its block state came to behave differently from every other implementation.
func TestNullStoreBlockState(t *testing.T) {
	t.Run("set block height zero", func(t *testing.T) {
		tests.SetBlockHeightZero(t, &NullStore{})
	})

	t.Run("set block state contract", func(t *testing.T) {
		tests.SetBlockStateContract(t, &NullStore{})
	})

	t.Run("set block state snapshot under concurrency", func(t *testing.T) {
		tests.SetBlockStateSnapshotUnderConcurrency(t, &NullStore{})
	})
}
