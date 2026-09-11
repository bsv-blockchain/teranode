package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// TestGetOutputsProjection pins the narrow previous-output projection the
// validator uses to re-extend transactions (GHSA-v76m-6vc7-g7c7).
//
// Re-extending every transaction means reading a parent output for every input,
// so the read must not drag the parent's inputs along. The outputs query already
// ran for fields.Outputs, but the result was only attached to Data.Tx when
// fields.Tx was requested — so asking for the narrow projection returned a nil Tx
// and the extend path could not see the parent's outputs at all.
func TestGetOutputsProjection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, tx := setup(ctx, t)

	_, err := utxoStore.Create(ctx, tx, 12345)
	require.NoError(t, err)

	txMeta, err := utxoStore.Get(ctx, tx.TxIDChainHash(), fields.Outputs)
	require.NoError(t, err)
	require.NotNil(t, txMeta.Tx, "fields.Outputs must attach the decoded outputs to Data.Tx")
	require.Len(t, txMeta.Tx.Outputs, len(tx.Outputs))

	for i, want := range tx.Outputs {
		require.NotNil(t, txMeta.Tx.Outputs[i], "output %d must be decoded", i)
		require.Equal(t, want.Satoshis, txMeta.Tx.Outputs[i].Satoshis, "output %d satoshis", i)
		require.Equal(t, []byte(*want.LockingScript), []byte(*txMeta.Tx.Outputs[i].LockingScript), "output %d script", i)
	}

	// The point of the projection: inputs are not fetched.
	require.Empty(t, txMeta.Tx.Inputs, "fields.Outputs must not pull the inputs")
}

// TestGetInputsProjectionLeavesTxNil pins a projection that must NOT attach a
// transaction to Data.Tx.
//
// fields.Inputs has always returned a nil Data.Tx from this store, and callers
// distinguish "no transaction" by that nil. needInputs is true for it, so
// keying tx construction on needInputs/needOutputs while adding the
// fields.Outputs projection would silently flip it to non-nil. This is the only
// executable guard on that invariant.
func TestGetInputsProjectionLeavesTxNil(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, tx := setup(ctx, t)

	_, err := utxoStore.Create(ctx, tx, 12345)
	require.NoError(t, err)

	txMeta, err := utxoStore.Get(ctx, tx.TxIDChainHash(), fields.Inputs)
	require.NoError(t, err)
	require.Nil(t, txMeta.Tx, "fields.Inputs must not attach a transaction to Data.Tx")
}
