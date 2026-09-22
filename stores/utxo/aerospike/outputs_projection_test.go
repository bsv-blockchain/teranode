package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestGetOutputsProjection pins the narrow previous-output projection the
// validator uses to re-extend transactions (GHSA-v76m-6vc7-g7c7).
//
// For an inline parent, fields.Outputs skips the inputs bin. Externally stored
// parents have no outputs bin, so this projection still fetches the full body.
//
// Fails before the fix: the Get switch had no fields.Outputs case, so the bin
// was fetched as a dependency of fields.Tx but never decoded on its own, and
// Data.Tx came back nil.
func TestGetOutputsProjection(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	logger := ulogger.NewErrorTestLogger(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	defer deferFn()

	cleanDB(t, client)

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	resp, err := store.Get(ctx, tx.TxIDChainHash(), fields.Outputs)
	require.NoError(t, err)
	require.NotNil(t, resp.Tx, "fields.Outputs must decode into Data.Tx, as fields.Inputs does")
	require.Len(t, resp.Tx.Outputs, len(tx.Outputs))

	for i, want := range tx.Outputs {
		require.NotNil(t, resp.Tx.Outputs[i], "output %d must be decoded", i)
		require.Equal(t, want.Satoshis, resp.Tx.Outputs[i].Satoshis, "output %d satoshis", i)
		require.Equal(t, []byte(*want.LockingScript), []byte(*resp.Tx.Outputs[i].LockingScript), "output %d script", i)
	}

	// The point of the projection, and true only for an inline parent: the
	// inputs bin is not fetched. An externally-stored parent has no outputs bin
	// at all, so it takes the full-body branch instead — see the external case
	// below, which does carry inputs.
	require.Empty(t, resp.Tx.Inputs, "fields.Outputs must not pull the inputs bin for an inline parent")
}

// TestGetOutputsProjection_ExternalParent covers the other branch of the same
// case, and the fields.External dependency the projection depends on.
//
// create.go writes the inputs and outputs bins only when a transaction is stored
// inline, so for an externally-stored parent fields.Outputs must fall back to the
// full body — which is why fields.Outputs is in the needsFullExternalTx gate and
// why requesting it pulls in fields.External. If that dependency were ever
// dropped, external parents would decode zero outputs and every honest spend of
// a large parent would be rejected for having no output at that index, which is
// the failure this test exists to catch.
func TestGetOutputsProjection_ExternalParent(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	// Small batch size so a modest fixture spills past one record, which is what
	// makes create.go store it externally.
	tSettings.UtxoStore.UtxoBatchSize = 2

	logger := ulogger.NewErrorTestLogger(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	defer deferFn()

	cleanDB(t, client)

	// More outputs than UtxoBatchSize => multiple records => external.
	bigTx := createTransactionWithOutputs(tSettings.UtxoStore.UtxoBatchSize + 1)

	_, err := store.Create(ctx, bigTx, 0)
	require.NoError(t, err)

	resp, err := store.Get(ctx, bigTx.TxIDChainHash(), fields.Outputs)
	require.NoError(t, err)
	require.NotNil(t, resp.Tx, "an external parent must still resolve its outputs")
	require.Len(t, resp.Tx.Outputs, len(bigTx.Outputs),
		"every output must be present, or extension fails for large parents")

	for i, want := range bigTx.Outputs {
		require.NotNil(t, resp.Tx.Outputs[i], "output %d must be decoded", i)
		require.Equal(t, want.Satoshis, resp.Tx.Outputs[i].Satoshis, "output %d satoshis", i)
	}
}

// Both requested field orders must retain both projections of an inline parent.
func TestGetOutputsProjection_WithInputs(t *testing.T) {
	settings := test.CreateBaseTestSettings(t)
	client, store, ctx, cleanup := initAerospike(t, settings, ulogger.NewErrorTestLogger(t))
	defer cleanup()
	cleanDB(t, client)
	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)
	for _, order := range [][]fields.FieldName{{fields.Inputs, fields.Outputs}, {fields.Outputs, fields.Inputs}} {
		t.Run(order[0].String()+"_first", func(t *testing.T) {
			items := []*utxo.UnresolvedMetaData{{Hash: *tx.TxIDChainHash(), Fields: order}}
			require.NoError(t, store.BatchDecorate(ctx, items))
			require.NoError(t, items[0].Err)
			got := items[0].Data.Tx
			require.NotNil(t, got)
			require.Len(t, got.Inputs, len(tx.Inputs))
			require.Len(t, got.Outputs, len(tx.Outputs))
			for i, want := range tx.Inputs {
				require.Equal(t, want.PreviousTxIDChainHash(), got.Inputs[i].PreviousTxIDChainHash())
				require.Equal(t, want.PreviousTxOutIndex, got.Inputs[i].PreviousTxOutIndex)
				require.Equal(t, want.PreviousTxSatoshis, got.Inputs[i].PreviousTxSatoshis)
				require.Equal(t, *want.UnlockingScript, *got.Inputs[i].UnlockingScript)
				require.Equal(t, *want.PreviousTxScript, *got.Inputs[i].PreviousTxScript)
			}
			for i, want := range tx.Outputs {
				require.Equal(t, want.Satoshis, got.Outputs[i].Satoshis)
				require.Equal(t, *want.LockingScript, *got.Outputs[i].LockingScript)
			}
		})
	}
}
