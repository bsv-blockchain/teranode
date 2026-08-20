package packedsql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

func TestSpendAndCreateHappyPath(t *testing.T) {
	store := newTestStore(t)
	parent := newExtendedTx(t, 2, 300_000)

	_, err := store.Create(context.Background(), parent, 100)
	require.NoError(t, err)

	child := newSpendingTx(t, parent, 0, 1)

	md, spends, err := store.SpendAndCreate(context.Background(), child, 101)
	require.NoError(t, err)
	require.NotNil(t, md)
	require.Len(t, spends, 2)

	for _, sp := range spends {
		require.NoError(t, sp.Err)
		require.NotNil(t, sp.SpendingData)
	}

	var n int
	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM packed_txs WHERE hash = $1`, child.TxIDChainHash()[:]).Scan(&n))
	require.Equal(t, 1, n)

	require.NotEqual(t, make([]byte, slotSpendSize), slotSpend(t, store, parent, 0))
}

func TestSpendAndCreateAtomicOnSpendFailure(t *testing.T) {
	store := newTestStore(t)

	existing := newExtendedTx(t, 1, 310_000)
	_, err := store.Create(context.Background(), existing, 100)
	require.NoError(t, err)

	missing := newExtendedTx(t, 1, 311_000)

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash:      existing.TxIDChainHash(),
		Vout:          0,
		LockingScript: existing.Outputs[0].LockingScript,
		Satoshis:      existing.Outputs[0].Satoshis,
	}))
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash:      missing.TxIDChainHash(),
		Vout:          0,
		LockingScript: missing.Outputs[0].LockingScript,
		Satoshis:      missing.Outputs[0].Satoshis,
	}))

	for i := range child.Inputs {
		child.Inputs[i].UnlockingScript = bscript.NewFromBytes([]byte{0x51})
	}

	require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 999))

	_, spends, err := store.SpendAndCreate(context.Background(), child, 101)
	require.Error(t, err)
	require.NotNil(t, spends)

	require.Equal(t, make([]byte, slotSpendSize), slotSpend(t, store, existing, 0))

	var n int
	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM packed_txs WHERE hash = $1`, child.TxIDChainHash()[:]).Scan(&n))
	require.Equal(t, 0, n)
}

func TestSpendAndCreateTxExistsLeavesSpends(t *testing.T) {
	store := newTestStore(t)
	parent := newExtendedTx(t, 1, 320_000)

	_, err := store.Create(context.Background(), parent, 100)
	require.NoError(t, err)

	child := newSpendingTx(t, parent, 0)

	_, err = store.Create(context.Background(), child, 100)
	require.NoError(t, err)

	_, spends, err := store.SpendAndCreate(context.Background(), child, 101)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxExists))
	require.NotNil(t, spends)

	require.NotEqual(t, make([]byte, slotSpendSize), slotSpend(t, store, parent, 0))
}

func TestSpendAndCreateCreateOnly(t *testing.T) {
	store := newTestStore(t)
	tx := newExtendedTx(t, 1, 330_000)

	md, spends, err := store.SpendAndCreate(context.Background(), tx, 100, utxo.WithCreateOnly())
	require.NoError(t, err)
	require.NotNil(t, md)
	require.Nil(t, spends)
}

func TestSpendAndCreateSpendOnly(t *testing.T) {
	store := newTestStore(t)
	parent := newExtendedTx(t, 1, 340_000)

	_, err := store.Create(context.Background(), parent, 100)
	require.NoError(t, err)

	child := newSpendingTx(t, parent, 0)

	md, spends, err := store.SpendAndCreate(context.Background(), child, 101, utxo.WithSpendOnly())
	require.NoError(t, err)
	require.Nil(t, md)
	require.Len(t, spends, 1)

	var n int
	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM packed_txs WHERE hash = $1`, child.TxIDChainHash()[:]).Scan(&n))
	require.Equal(t, 0, n)
}

func TestSpendAndCreateDoubleSpendReportsPerInputErrors(t *testing.T) {
	store := newTestStore(t)
	parent := newExtendedTx(t, 1, 350_000)

	_, err := store.Create(context.Background(), parent, 100)
	require.NoError(t, err)

	childA := newSpendingTx(t, parent, 0)

	_, _, err = store.SpendAndCreate(context.Background(), childA, 101)
	require.NoError(t, err)

	childB := bt.NewTx()
	require.NoError(t, childB.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          0,
		LockingScript: parent.Outputs[0].LockingScript,
		Satoshis:      parent.Outputs[0].Satoshis,
	}))
	childB.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x52})
	require.NoError(t, childB.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 998))

	_, spends, err := store.SpendAndCreate(context.Background(), childB, 101)
	require.Error(t, err)
	require.Len(t, spends, 1)
	require.True(t, errors.Is(spends[0].Err, errors.ErrSpent))

	var n int
	require.NoError(t, store.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM packed_txs WHERE hash = $1`, childB.TxIDChainHash()[:]).Scan(&n))
	require.Equal(t, 0, n)
}
