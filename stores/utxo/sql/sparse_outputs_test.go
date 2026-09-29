package sql

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// sparseOutputStore builds a store for the sparse-output tests. It deliberately
// does not reuse setup(), which returns one fixed fully-populated transaction.
func sparseOutputStore(ctx context.Context, t *testing.T, name string) *Store {
	t.Helper()

	initPrometheusMetrics()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 30 * time.Second
	tSettings.BatcherDrainMode = true // batcher fires immediately in tests
	tSettings.Pruner.UTXODefensiveEnabled = false

	storeURL, err := url.Parse("sqlitememory:///" + name)
	require.NoError(t, err)

	store, err := New(ctx, ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)

	return store
}

func sparseOutputScript(t *testing.T) *bscript.Script {
	t.Helper()

	script, err := bscript.NewFromHexString("76a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac")
	require.NoError(t, err)

	return script
}

// sparseOutputTx builds a transaction whose Outputs slice has nil holes, the
// shape cmd/seeder produces via utxopersister.PadUTXOsWithNil: outputs survive at
// their original vout, and the vouts already spent at snapshot time are nil.
// Outputs land at vout 0 and vout 5, so indices 1 to 4 never reach the outputs
// table while createOutputs preserves 0 and 5 as their original indices.
func sparseOutputTx(t *testing.T) (*bt.Tx, *chainhash.Hash) {
	t.Helper()

	script := sparseOutputScript(t)

	tx := bt.NewTx()
	tx.Outputs = make([]*bt.Output, 6)
	tx.Outputs[0] = &bt.Output{Satoshis: 1000, LockingScript: script}
	tx.Outputs[5] = &bt.Output{Satoshis: 2000, LockingScript: script}

	// A nil-holed transaction cannot reproduce its own txid, which is exactly why
	// the seeder passes the real one through WithTXID.
	txID := chainhash.HashH([]byte("sparse-output-parent"))

	return tx, &txID
}

// TestGetUtxosWithSparseOutputsIsIndexedByVout is the regression test for the
// fields.Utxos read on a transaction with gaps in outputs.idx. SpendingDatas is
// indexed by vout but was sized by the row count from the gap-free outputs read,
// so the first output past a gap indexed out of range and panicked.
func TestGetUtxosWithSparseOutputsIsIndexedByVout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := sparseOutputStore(ctx, t, "sparse_outputs_read")
	defer func() {
		require.NoError(t, store.Close(ctx))
	}()

	tx, txID := sparseOutputTx(t)

	_, _, err := store.SpendAndCreate(ctx, tx, 100, utxo.WithCreateOnly(), utxo.WithTXID(txID))
	require.NoError(t, err)

	// The read that used to panic.
	data, err := store.Get(ctx, txID, fields.Utxos)
	require.NoError(t, err)
	require.NotNil(t, data)

	// Sized by the highest stored vout, not by the two rows that came back.
	require.Len(t, data.SpendingDatas, 6)

	// Nothing is spent yet, and the gap positions stay nil rather than shifting
	// the vout-5 entry down into slot 1.
	for vout, spendingData := range data.SpendingDatas {
		require.Nil(t, spendingData, "vout %d should be unspent", vout)
	}
}

// TestGetUtxosWithSparseOutputsReportsSpenderAtItsVout proves the fix keeps the
// spender at its real vout. Sizing by the row count did not merely panic: at a
// smaller gap it silently attributed the spend to the wrong output index.
func TestGetUtxosWithSparseOutputsReportsSpenderAtItsVout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := sparseOutputStore(ctx, t, "sparse_outputs_spend")
	defer func() {
		require.NoError(t, store.Close(ctx))
	}()

	tx, txID := sparseOutputTx(t)

	_, _, err := store.SpendAndCreate(ctx, tx, 100, utxo.WithCreateOnly(), utxo.WithTXID(txID))
	require.NoError(t, err)

	// Spend vout 5, the output on the far side of the gap. The input carries the
	// parent's satoshis and script so the store can derive the same utxo hash it
	// stored at create.
	script := sparseOutputScript(t)

	input := &bt.Input{
		PreviousTxOutIndex: 5,
		PreviousTxSatoshis: tx.Outputs[5].Satoshis,
		PreviousTxScript:   tx.Outputs[5].LockingScript,
		UnlockingScript:    script,
		SequenceNumber:     0xffffffff,
	}
	require.NoError(t, input.PreviousTxIDAdd(txID))

	spendingTx := bt.NewTx()
	spendingTx.Inputs = append(spendingTx.Inputs, input)
	spendingTx.Outputs = append(spendingTx.Outputs, &bt.Output{Satoshis: 1500, LockingScript: script})

	_, _, err = store.SpendAndCreate(ctx, spendingTx, 101, utxo.WithSpendOnly())
	require.NoError(t, err)

	data, err := store.Get(ctx, txID, fields.Utxos)
	require.NoError(t, err)
	require.Len(t, data.SpendingDatas, 6)

	// The spend is recorded at vout 5, not at the row-count position 1.
	require.Nil(t, data.SpendingDatas[0], "vout 0 is still unspent")
	require.Nil(t, data.SpendingDatas[1], "vout 1 does not exist and must not carry the vout-5 spend")
	require.NotNil(t, data.SpendingDatas[5], "vout 5 should carry the spend")
	require.True(t, data.SpendingDatas[5].TxID.IsEqual(spendingTx.TxIDChainHash()))
}

// TestGetTxOutputsAreIndexedByVout is the regression test for the outputs read.
// createOutputs preserves each surviving output's original index, so appending
// row by row collapsed the holes and shifted every later output down a slot:
// Outputs[1] held the vout-5 output. That was a silent wrong answer, not a
// panic, and it reached every consumer that indexes Outputs by vout.
func TestGetTxOutputsAreIndexedByVout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := sparseOutputStore(ctx, t, "sparse_outputs_vout")
	defer func() {
		require.NoError(t, store.Close(ctx))
	}()

	tx, txID := sparseOutputTx(t)

	_, _, err := store.SpendAndCreate(ctx, tx, 100, utxo.WithCreateOnly(), utxo.WithTXID(txID))
	require.NoError(t, err)

	data, err := store.Get(ctx, txID, fields.Tx)
	require.NoError(t, err)
	require.NotNil(t, data)
	require.NotNil(t, data.Tx)

	// Six slots, not the two rows that came back.
	require.Len(t, data.Tx.Outputs, 6)

	require.NotNil(t, data.Tx.Outputs[0], "vout 0 exists")
	require.Equal(t, uint64(1000), data.Tx.Outputs[0].Satoshis)

	require.NotNil(t, data.Tx.Outputs[5], "vout 5 exists and must not have shifted to slot 1")
	require.Equal(t, uint64(2000), data.Tx.Outputs[5].Satoshis)

	// The gap positions are holes, which is the state TxIsSerializable already
	// tests for and the state every vout-indexing consumer already nil-checks.
	for _, vout := range []int{1, 2, 3, 4} {
		require.Nil(t, data.Tx.Outputs[vout], "vout %d does not exist", vout)
	}

	// A transaction carrying a hole cannot reproduce its own bytes, and the
	// serializing boundaries gate on exactly this.
	require.False(t, data.TxIsSerializable())
}

// TestBatchDecorateTxOutputsAreIndexedByVout covers the same fix on the batch
// read path, which has its own outputs query.
func TestBatchDecorateTxOutputsAreIndexedByVout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := sparseOutputStore(ctx, t, "sparse_outputs_vout_batch")
	defer func() {
		require.NoError(t, store.Close(ctx))
	}()

	tx, txID := sparseOutputTx(t)

	_, _, err := store.SpendAndCreate(ctx, tx, 100, utxo.WithCreateOnly(), utxo.WithTXID(txID))
	require.NoError(t, err)

	items := []*utxo.UnresolvedMetaData{{Hash: *txID, Idx: 0}}

	require.NoError(t, store.BatchDecorate(ctx, items, fields.Tx))
	require.NoError(t, items[0].Err)
	require.NotNil(t, items[0].Data)
	require.NotNil(t, items[0].Data.Tx)

	require.Len(t, items[0].Data.Tx.Outputs, 6)
	require.NotNil(t, items[0].Data.Tx.Outputs[5])
	require.Equal(t, uint64(2000), items[0].Data.Tx.Outputs[5].Satoshis)
	require.Nil(t, items[0].Data.Tx.Outputs[1])
}
