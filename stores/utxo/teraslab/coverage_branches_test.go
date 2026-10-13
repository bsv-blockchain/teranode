//go:build teraslab

package teraslab_test

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// TestSmallBranchCoverage exercises the cheap early-return / skip guards that
// the larger happy-path tests step over.
func TestSmallBranchCoverage(t *testing.T) {
	store, ctx, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()

	tx := newTestTx(t)
	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	t.Run("SetMinedMulti with empty hashes is a no-op", func(t *testing.T) {
		res, err := store.SetMinedMulti(ctx, nil, utxo.MinedBlockInfo{})
		require.NoError(t, err)
		require.Nil(t, res)
	})

	t.Run("Unspend with empty list is a no-op", func(t *testing.T) {
		require.NoError(t, store.Unspend(ctx, nil, false))
	})

	t.Run("Spend honors ignore flags on output 1", func(t *testing.T) {
		spendTx := makeSpendTx(t, tx, 1)
		spends, err := store.Spend(ctx, spendTx, store.GetBlockHeight()+1,
			utxo.IgnoreFlags{IgnoreConflicting: true, IgnoreLocked: true})
		require.NoError(t, err)
		require.Len(t, spends, 1)
	})

	t.Run("Unspend skips nil-TxID entries and unspends the rest", func(t *testing.T) {
		// Spend output 0, then unspend a list mixing a nil-TxID entry (skipped
		// via continue) with the real spend.
		spendTx := makeSpendTx(t, tx, 0)
		_, err := store.Spend(ctx, spendTx, store.GetBlockHeight()+1)
		require.NoError(t, err)

		utxoHash0, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
		require.NoError(t, err)

		spends := []*utxo.Spend{
			{TxID: nil}, // skipped
			{
				TxID:         tx.TxIDChainHash(),
				Vout:         0,
				UTXOHash:     utxoHash0,
				SpendingData: spend.NewSpendingData(spendTx.TxIDChainHash(), 0),
			},
		}
		require.NoError(t, store.Unspend(ctx, spends, false))
	})
}

// TestIteratorNextContextError covers the GetRecordBatch error arm inside each
// iterator's Next: with an already-cancelled context the page fetch fails and
// the error is recorded and returned.
func TestIteratorNextContextError(t *testing.T) {
	store, ctx, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()

	require.NoError(t, store.SetBlockHeight(100))
	tx := newTestTx(t)
	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)

	cancelledCtx := func() context.Context {
		c, cancel := context.WithCancel(context.Background())
		cancel()
		return c
	}

	t.Run("unmined iterator Next errors on cancelled ctx", func(t *testing.T) {
		iter, err := store.GetUnminedTxIterator()
		require.NoError(t, err)
		defer iter.Close()

		_, err = iter.Next(cancelledCtx())
		require.Error(t, err)
		require.Error(t, iter.Err())
	})

	t.Run("consistency scan Next errors on cancelled ctx", func(t *testing.T) {
		iter, err := store.ScanInconsistentUnminedTxs()
		require.NoError(t, err)
		defer iter.Close()

		_, err = iter.Next(cancelledCtx())
		require.Error(t, err)
		require.Error(t, iter.Err())
	})
}

// TestPreviousOutputsDecorateMixedInputs covers the "already decorated" skip
// branch: an input that already carries PreviousTxScript is left untouched while
// its undecorated sibling (same parent) is filled in.
func TestPreviousOutputsDecorateMixedInputs(t *testing.T) {
	store, ctx, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()

	parent := newTestTx(t)
	_, err := store.Create(ctx, parent, 0)
	require.NoError(t, err)

	// Two inputs to the same parent: input 0 pre-decorated, input 1 not.
	preScript := parent.Outputs[0].LockingScript
	spendTx := &bt.Tx{Version: 1, Inputs: []*bt.Input{
		{PreviousTxOutIndex: 0, PreviousTxScript: preScript, PreviousTxSatoshis: parent.Outputs[0].Satoshis},
		{PreviousTxOutIndex: 1},
	}}
	require.NoError(t, spendTx.Inputs[0].PreviousTxIDAdd(parent.TxIDChainHash()))
	require.NoError(t, spendTx.Inputs[1].PreviousTxIDAdd(parent.TxIDChainHash()))

	require.NoError(t, store.PreviousOutputsDecorate(ctx, spendTx))

	// Pre-decorated input untouched; the other now filled from parent output 1.
	require.Equal(t, preScript, spendTx.Inputs[0].PreviousTxScript)
	require.NotNil(t, spendTx.Inputs[1].PreviousTxScript)
	require.Equal(t, parent.Outputs[1].Satoshis, spendTx.Inputs[1].PreviousTxSatoshis)
}
