package aerospike_test

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestParentOutputsForValidation(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	_, store, _, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	t.Run("contract", func(t *testing.T) {
		tests.ParentOutputsForValidation(t, store)
	})

	t.Run("reads outputs, never inputs", func(t *testing.T) {
		tests.ParentOutputsReadsOutputsNotInputs(t, store)
	})
}

// External parents are reconstructed from the blob store; the answer must match
// the transaction's own outputs, and an index past the end is NoSuchIndex.
func TestParentOutputsForValidationExternalParent(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	_, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	parent := bt.NewTx()
	require.NoError(t, parent.FromUTXOs(&bt.UTXO{
		TxIDHash:      tests.Tx.TxIDChainHash(),
		Vout:          3,
		LockingScript: tests.Tx.Inputs[0].PreviousTxScript,
		Satoshis:      tests.Tx.Inputs[0].PreviousTxSatoshis,
	}))
	parent.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00})

	// More outputs than utxostore_utxoBatchSize forces the external path.
	for i := 0; i < tSettings.UtxoStore.UtxoBatchSize+20; i++ {
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", uint64(1000+i)))
	}

	_, _, err := store.SpendAndCreate(ctx, parent, 100, utxo.WithCreateOnly())
	require.NoError(t, err)

	last := uint32(len(parent.Outputs) - 1)
	answers, err := store.ParentOutputsForValidation(ctx, []utxo.Outpoint{
		{TxID: *parent.TxIDChainHash(), Vout: last},
		{TxID: *parent.TxIDChainHash(), Vout: last + 1},
	})
	require.NoError(t, err)
	require.Len(t, answers, 2)
	tests.RequireAnswered(t, answers)

	require.NoError(t, answers[0].Err)
	require.Equal(t, utxo.ParentOutputNotMined, answers[0].Status)
	require.Equal(t, parent.Outputs[last].Satoshis, answers[0].Satoshis)
	require.Equal(t, []byte(*parent.Outputs[last].LockingScript), []byte(*answers[0].LockingScript))

	require.Equal(t, utxo.ParentOutputNoSuchIndex, answers[1].Status)
}

// A cancelled context fails the whole call rather than answering any slot.
func TestParentOutputsForValidationCancelled(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	_, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	cctx, cancel := context.WithCancel(ctx)
	cancel()

	answers, err := store.ParentOutputsForValidation(cctx, []utxo.Outpoint{{TxID: *tests.TXHash, Vout: 0}})
	require.Error(t, err)
	require.Nil(t, answers)
}

// A truncated inline outputs bin, a parent record whose stored output list is
// shorter than the parent really has, answers NoSuchIndex for the missing
// index, the verdict the validator turns into TxInvalid. This is the one place
// a local corruption reads as a verdict on the child: the inline bins are not
// re-hashed, so a short list cannot be told from a parent that never had the
// output. The legacy decorate path has carried the same hazard (get.go, the
// scope note in getExternalTransaction); this pins it rather than hides it. The
// outputs the bin still holds answer as before.
func TestParentOutputsForValidationTruncatedInlineOutputsBin(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	parent := createTransactionWithOutputs(3)
	_, err := store.Create(ctx, parent, 0)
	require.NoError(t, err)

	hash := parent.TxIDChainHash()

	// Before: every output answers.
	answers, err := store.ParentOutputsForValidation(ctx, []utxo.Outpoint{{TxID: *hash, Vout: 0}, {TxID: *hash, Vout: 2}})
	require.NoError(t, err)
	require.Equal(t, utxo.ParentOutputNotMined, answers[0].Status)
	require.Equal(t, utxo.ParentOutputNotMined, answers[1].Status)
	require.Equal(t, parent.Outputs[2].Satoshis, answers[1].Satoshis)

	// Truncate the stored list to its first output, as a corrupt record would.
	key, aErr := aerospike.NewKey(store.GetNamespace(), store.GetName(), hash[:])
	require.Nil(t, aErr)

	first := parent.Outputs[0].Bytes()
	require.Nil(t, client.PutBins(nil, key, aerospike.NewBin(fields.Outputs.String(), aerospike.NewListValue([]interface{}{first}))))

	answers, err = store.ParentOutputsForValidation(ctx, []utxo.Outpoint{{TxID: *hash, Vout: 0}, {TxID: *hash, Vout: 2}})
	require.NoError(t, err)
	require.Len(t, answers, 2)

	require.NoError(t, answers[0].Err)
	require.Equal(t, utxo.ParentOutputNotMined, answers[0].Status, "the output the bin still holds answers")
	require.Equal(t, parent.Outputs[0].Satoshis, answers[0].Satoshis)

	require.NoError(t, answers[1].Err, "a short list is read as a parent without that output, not as a fault")
	require.Equal(t, utxo.ParentOutputNoSuchIndex, answers[1].Status)
}
