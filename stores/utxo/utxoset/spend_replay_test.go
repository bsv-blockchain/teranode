package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/stretchr/testify/require"
)

// TestSpendReplayedByTheSameTransactionSucceeds is what keeps a half-applied block from
// wedging the node forever.
//
// Delete-on-spend destroys the coin row, and absence IS the rejection. So a block interrupted
// part-way through application leaves coins already gone, and re-offering that block asks the
// store to spend them again. Reporting that as a double spend is wrong and it is fatal: the
// block can never be applied, the tip never advances, and no restart helps. It happened on
// mainnet at height 97389.
//
// The same transaction spending the same coin is the SAME spend, not a competing one. The
// journal already records who took each coin, so the store can tell the two apart.
func TestSpendReplayedByTheSameTransactionSucceeds(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          0,
		LockingScript: parent.Outputs[0].LockingScript,
		Satoshis:      parent.Outputs[0].Satoshis,
	}))

	spends, err := spendOnly(ctx, s, child, 200)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err, "the first spend takes the coin")

	// The same transaction, offered again, exactly as a re-applied block offers it.
	replay := bt.NewTx()
	require.NoError(t, replay.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          0,
		LockingScript: parent.Outputs[0].LockingScript,
		Satoshis:      parent.Outputs[0].Satoshis,
	}))

	require.Equal(t, child.TxIDChainHash().String(), replay.TxIDChainHash().String(),
		"the replay must be the same transaction, or this test proves nothing")

	spends2, err := spendOnly(ctx, s, replay, 200)
	require.NoError(t, err)
	require.NoError(t, spends2[0].Err,
		"re-spending a coin THIS transaction already took is a replay, not a double spend")
	require.Nil(t, spends2[0].ConflictingTxID,
		"and it must not name itself as a competing spender")

	// The spend is also the decorate fetch, so a replay still has to hand back what script
	// validation needs. The journal row holds both.
	require.Equal(t, parent.Outputs[0].Satoshis, replay.Inputs[0].PreviousTxSatoshis,
		"a replayed spend must still return the satoshis")
	require.NotNil(t, replay.Inputs[0].PreviousTxScript,
		"and the locking script")
}

// TestSpendByADifferentTransactionIsStillRejected pins the other half. Making replay work
// must not make an actual double spend succeed.
func TestSpendByADifferentTransactionIsStillRejected(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	first := bt.NewTx()
	require.NoError(t, first.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          0,
		LockingScript: parent.Outputs[0].LockingScript,
		Satoshis:      parent.Outputs[0].Satoshis,
	}))

	spends, err := spendOnly(ctx, s, first, 200)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)

	// A DIFFERENT transaction reaching for the same coin: it adds an output, so its id differs.
	rival := bt.NewTx()
	require.NoError(t, rival.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          0,
		LockingScript: parent.Outputs[0].LockingScript,
		Satoshis:      parent.Outputs[0].Satoshis,
	}))
	rival.AddOutput(&bt.Output{Satoshis: 1, LockingScript: parent.Outputs[0].LockingScript})

	require.NotEqual(t, first.TxIDChainHash().String(), rival.TxIDChainHash().String())

	spends2, err := spendOnly(ctx, s, rival, 200)
	require.Error(t, err, "a rejected spend is now a returned error, and rolled back")
	require.Error(t, spends2[0].Err, "a different transaction must still be rejected")
	require.NotNil(t, spends2[0].ConflictingTxID, "and told who took the coin")
	require.Equal(t, first.TxIDChainHash().String(), spends2[0].ConflictingTxID.String())
}
