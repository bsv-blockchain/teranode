package sql

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// A tx can spend one parent that is already deleted and one that survives. Clearing its parent
// references must unspend the surviving parent: the missing parent must not undo that unspend.
func TestClearParentRefsUnspendsSurvivingParentWhenAnotherParentIsGone(t *testing.T) {
	ctx := t.Context()

	store, surviving := setup(ctx, t)

	gone := bt.NewTx()
	require.NoError(t, gone.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 10000))
	require.NoError(t, gone.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 9000))
	gone.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})

	child := bt.NewTx()
	require.NoError(t, child.From(surviving.TxIDChainHash().String(), 0, surviving.Outputs[0].LockingScript.String(), surviving.Outputs[0].Satoshis))
	require.NoError(t, child.From(gone.TxIDChainHash().String(), 0, gone.Outputs[0].LockingScript.String(), gone.Outputs[0].Satoshis))
	require.NoError(t, child.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

	for _, in := range child.Inputs {
		in.UnlockingScript = bscript.NewFromBytes([]byte{})
	}

	_, err := store.Create(ctx, surviving, 0)
	require.NoError(t, err)

	_, err = store.Create(ctx, gone, 0)
	require.NoError(t, err)

	_, err = store.Create(ctx, child, 0)
	require.NoError(t, err)

	spends, err := store.Spend(ctx, child, store.GetBlockHeight()+1)
	require.NoError(t, err)
	require.Len(t, spends, len(child.Inputs))

	survivingSpend := &utxo.Spend{TxID: surviving.TxIDChainHash(), Vout: 0, UTXOHash: spends[0].UTXOHash}

	before, err := store.GetSpend(ctx, survivingSpend)
	require.NoError(t, err)
	require.NotNil(t, before.SpendingData, "precondition: the surviving parent output is spent by the child")

	require.NoError(t, store.Delete(ctx, gone.TxIDChainHash()))

	require.NoError(t, utxo.ClearParentRefs(ctx, store, child.TxIDChainHash()))

	after, err := store.GetSpend(ctx, survivingSpend)
	require.NoError(t, err)
	require.Nil(t, after.SpendingData, "the surviving parent must not name the child as its spender")
}
