package packedsql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

func TestBatchDecorate(t *testing.T) {
	store := newTestStore(t)

	tx1 := newExtendedTx(t, 1, 120_000)
	tx2 := newExtendedTx(t, 2, 121_000)
	unknown := newExtendedTx(t, 1, 122_000)

	_, err := store.Create(context.Background(), tx1, 100)
	require.NoError(t, err)
	_, err = store.Create(context.Background(), tx2, 100)
	require.NoError(t, err)

	items := []*utxo.UnresolvedMetaData{
		{Hash: *tx1.TxIDChainHash(), Idx: 0},
		{Hash: *unknown.TxIDChainHash(), Idx: 1},
		{Hash: *tx2.TxIDChainHash(), Idx: 2, Fields: []fields.FieldName{fields.Fee}},
	}

	require.NoError(t, store.BatchDecorate(context.Background(), items, fields.Fee, fields.SizeInBytes, fields.Tx))

	require.NoError(t, items[0].Err)
	require.NotNil(t, items[0].Data)
	require.Equal(t, tx1.Size(), int(items[0].Data.SizeInBytes))

	require.Error(t, items[1].Err)
	require.True(t, errors.Is(items[1].Err, errors.ErrTxNotFound))

	require.NoError(t, items[2].Err)
	require.NotNil(t, items[2].Data)
	require.Nil(t, items[2].Data.Tx)
	require.NotZero(t, items[2].Data.Fee)
}

func TestPreviousOutputsDecorate(t *testing.T) {
	store := newTestStore(t)

	parent := newExtendedTx(t, 2, 130_000)
	_, err := store.Create(context.Background(), parent, 100)
	require.NoError(t, err)

	child := bt.NewTx()
	child.Inputs = append(child.Inputs, &bt.Input{PreviousTxOutIndex: 1})
	require.NoError(t, child.Inputs[0].PreviousTxIDAdd(parent.TxIDChainHash()))

	require.NoError(t, store.PreviousOutputsDecorate(context.Background(), child))

	require.NotNil(t, child.Inputs[0].PreviousTxScript)
	require.Equal(t, *parent.Outputs[1].LockingScript, *child.Inputs[0].PreviousTxScript)
	require.Equal(t, parent.Outputs[1].Satoshis, child.Inputs[0].PreviousTxSatoshis)
}

func TestBatchPreviousOutputsDecorateSkipsDecorated(t *testing.T) {
	store := newTestStore(t)

	parent := newExtendedTx(t, 1, 140_000)
	_, err := store.Create(context.Background(), parent, 100)
	require.NoError(t, err)

	existing := bscript.NewFromBytes([]byte{0x51})

	decorated := bt.NewTx()
	decorated.Inputs = append(decorated.Inputs, &bt.Input{PreviousTxOutIndex: 0, PreviousTxScript: existing})
	require.NoError(t, decorated.Inputs[0].PreviousTxIDAdd(parent.TxIDChainHash()))

	undecorated := bt.NewTx()
	undecorated.Inputs = append(undecorated.Inputs, &bt.Input{PreviousTxOutIndex: 0})
	require.NoError(t, undecorated.Inputs[0].PreviousTxIDAdd(parent.TxIDChainHash()))

	require.NoError(t, store.BatchPreviousOutputsDecorate(context.Background(), []*bt.Tx{decorated, undecorated}))

	require.Same(t, existing, decorated.Inputs[0].PreviousTxScript)
	require.NotNil(t, undecorated.Inputs[0].PreviousTxScript)
	require.Equal(t, *parent.Outputs[0].LockingScript, *undecorated.Inputs[0].PreviousTxScript)
}

func TestOutputsColumnPartialRead(t *testing.T) {
	store := newTestStore(t)

	bigScript := make([]byte, 1_100_000)
	for i := range bigScript {
		bigScript[i] = byte(i % 251)
	}

	parent := newExtendedTx(t, 1, 150_000)
	parent.Outputs = append(parent.Outputs, &bt.Output{
		Satoshis:      1,
		LockingScript: bscript.NewFromBytes(bigScript),
	})

	_, err := store.Create(context.Background(), parent, 100)
	require.NoError(t, err)

	var attstorage string
	err = store.pool.QueryRow(context.Background(),
		`SELECT attstorage::text FROM pg_attribute
		 WHERE attrelid = (SELECT inhrelid FROM pg_inherits WHERE inhparent = 'packed_txs'::regclass LIMIT 1)
		 AND attname = 'outputs'`).Scan(&attstorage)
	require.NoError(t, err)
	require.Equal(t, "e", attstorage)

	var colSize int
	err = store.pool.QueryRow(context.Background(),
		`SELECT pg_column_size(outputs) FROM packed_txs WHERE hash = $1`,
		parent.TxIDChainHash()[:]).Scan(&colSize)
	require.NoError(t, err)
	require.Greater(t, colSize, 1_000_000)

	child := bt.NewTx()
	child.Inputs = append(child.Inputs, &bt.Input{PreviousTxOutIndex: 0})
	require.NoError(t, child.Inputs[0].PreviousTxIDAdd(parent.TxIDChainHash()))

	require.NoError(t, store.PreviousOutputsDecorate(context.Background(), child))
	require.Equal(t, *parent.Outputs[0].LockingScript, *child.Inputs[0].PreviousTxScript)
}
