package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// TestGetSpendNamesTheSpendingInput.
//
// GetSpend answers "who took this outpoint" with the spending transaction AND the input of it
// that did the taking, and the asset API publishes both. The input index used to be a
// hardcoded zero, because the journal kept the spender's txid but not which of its inputs
// consumed the coin, so every coin taken by input 1 or later was published with a reference
// to the wrong input.
//
// The spender here takes two coins, the second through input 1, and the parents are ordered
// so that the stored inpoints (parent-major, deduplicated) would give a different answer from
// the transaction's own input order: input 0 spends parent B, input 1 spends parent A, input 2
// spends B again.
func TestGetSpendNamesTheSpendingInput(t *testing.T) {
	s, ctx := newTestStore(t)

	a := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, a, 100)
	require.NoError(t, err)

	b := mkTx(t, 2, 6_000)
	_, err = s.Create(ctx, b, 100)
	require.NoError(t, err)

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(
		&bt.UTXO{TxIDHash: b.TxIDChainHash(), Vout: 0, LockingScript: b.Outputs[0].LockingScript, Satoshis: b.Outputs[0].Satoshis},
		&bt.UTXO{TxIDHash: a.TxIDChainHash(), Vout: 1, LockingScript: a.Outputs[1].LockingScript, Satoshis: a.Outputs[1].Satoshis},
		&bt.UTXO{TxIDHash: b.TxIDChainHash(), Vout: 1, LockingScript: b.Outputs[1].LockingScript, Satoshis: b.Outputs[1].Satoshis},
	))
	child.AddOutput(&bt.Output{Satoshis: 10_000, LockingScript: a.Outputs[0].LockingScript})

	_, err = s.Create(ctx, child, 101)
	require.NoError(t, err)

	spends, err := spendOnly(ctx, s, child, 101)
	require.NoError(t, err)

	for _, sp := range spends {
		require.NoError(t, sp.Err)
	}

	want := []struct {
		parent *bt.Tx
		vout   uint32
		vin    int
	}{
		{b, 0, 0},
		{a, 1, 1},
		{b, 1, 2},
	}

	for _, w := range want {
		resp, err := s.GetSpend(ctx, &utxo.Spend{TxID: w.parent.TxIDChainHash(), Vout: w.vout})
		require.NoError(t, err)
		require.Equal(t, int(utxo.Status_SPENT), resp.Status)
		require.NotNil(t, resp.SpendingData)
		require.Equal(t, child.TxIDChainHash().String(), resp.SpendingData.TxID.String())
		require.Equal(t, w.vin, resp.SpendingData.Vin,
			"%s:%d was consumed by input %d of the spender", w.parent.TxIDChainHash(), w.vout, w.vin)
	}

	// The per-output spending data on a metadata read comes from the same journal row, so it
	// names the same input.
	got, err := s.Get(ctx, a.TxIDChainHash(), fields.Utxos)
	require.NoError(t, err)
	require.Len(t, got.SpendingDatas, 2)
	require.Nil(t, got.SpendingDatas[0], "a:0 is unspent")
	require.NotNil(t, got.SpendingDatas[1])
	require.Equal(t, 1, got.SpendingDatas[1].Vin, "a:1 was consumed by input 1")
}
