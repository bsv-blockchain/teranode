package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// TestSendGetBatchDecoratesEachFieldSetSeparately pins that a Get served by the
// batcher answers with the field set it asked for, whoever shares its window.
//
// sendGetBatch used to decorate the whole batch with the union of every item's
// fields. A fields.TxInpoints Get that shared a window with a fields.Tx Get then
// read all six inputs columns and came back carrying a Data.Tx, and the
// fields.Tx Get came back carrying TxInpoints. settings.conf sets
// utxostore_getBatcherSize to 4096, so this is the path a plain Get takes.
//
// sendGetBatch is called directly so the batch composition is fixed rather than
// left to the batcher's timing.
func TestSendGetBatchDecoratesEachFieldSetSeparately(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, tx := setup(ctx, t)

	_, err := store.Create(ctx, tx, 12345)
	require.NoError(t, err)

	hash := *tx.TxIDChainHash()

	newItem := func(bins ...fields.FieldName) *batchGetItem {
		return &batchGetItem{hash: hash, fields: bins, done: make(chan batchGetItemData, 1)}
	}

	inpointsOnly := newItem(fields.TxInpoints)
	txOnly := newItem(fields.Tx)
	// The same field set as inpointsOnly written differently, so it has to land
	// in the same group.
	inpointsAgain := newItem(fields.TxInpoints, fields.TxInpoints)

	store.sendGetBatch([]*batchGetItem{inpointsOnly, txOnly, inpointsAgain})

	inpointsResult := <-inpointsOnly.done
	txResult := <-txOnly.done
	inpointsAgainResult := <-inpointsAgain.done

	for _, r := range []batchGetItemData{inpointsResult, txResult, inpointsAgainResult} {
		require.NoError(t, r.Err)
		require.NotNil(t, r.Data)
	}

	require.Nil(t, inpointsResult.Data.Tx, "a batch-mate's fields.Tx must not attach a Tx to a fields.TxInpoints Get")
	require.Len(t, inpointsResult.Data.TxInpoints.ParentTxHashes, 1)
	require.Equal(t, *tx.Inputs[0].PreviousTxIDChainHash(), inpointsResult.Data.TxInpoints.ParentTxHashes[0])

	require.Nil(t, inpointsAgainResult.Data.Tx)
	require.Equal(t, inpointsResult.Data.TxInpoints, inpointsAgainResult.Data.TxInpoints)

	require.NotNil(t, txResult.Data.Tx)
	require.Equal(t, tx.ExtendedBytes(), txResult.Data.Tx.ExtendedBytes())
	require.Empty(t, txResult.Data.TxInpoints.ParentTxHashes, "a batch-mate's fields.TxInpoints must not widen a fields.Tx Get")

	require.NotSame(t, inpointsResult.Data, txResult.Data)
}

// TestFieldSetKeyIgnoresOrderAndRepetition pins the grouping key: two lists that
// name the same fields share a group, and different sets do not.
func TestFieldSetKeyIgnoresOrderAndRepetition(t *testing.T) {
	require.Equal(t,
		fieldSetKey([]fields.FieldName{fields.Tx, fields.BlockIDs}),
		fieldSetKey([]fields.FieldName{fields.BlockIDs, fields.Tx, fields.BlockIDs}))

	require.NotEqual(t,
		fieldSetKey([]fields.FieldName{fields.Tx}),
		fieldSetKey([]fields.FieldName{fields.TxInpoints}))

	require.NotEqual(t,
		fieldSetKey([]fields.FieldName{fields.Tx}),
		fieldSetKey([]fields.FieldName{fields.Tx, fields.TxInpoints}))
}
