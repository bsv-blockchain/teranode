package aerospike_test

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/stores/utxo"
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
