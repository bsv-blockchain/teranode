package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// A list whose parent already exists falls back to the per-transaction default after step 1
// has committed every outside spend. A child that the fallback then rejects must not keep the
// outside spends step 1 made for it: its independent input is unspent afterwards and another
// transaction can take it.
func TestSpendAndCreateMultiFallbackRestoresTheOutsideSpendsOfARejectedChild(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	const height = 1000

	w := tests.BuildMultiWorkload(t, 0xb0, 1, 2)
	w.StoreRoots(t, s, height-1)
	rootA, rootB := w.Roots[0], w.Roots[1]

	// The parent exists before the list runs, which sends the list to the fallback, and a
	// thief has taken the output the child spends, which the fallback finds and step 1 cannot.
	parent := payFrom(t, 1, []*bt.Tx{rootA}, []uint32{0})
	_, _, err := s.SpendAndCreate(ctx, parent, height-1)
	require.NoError(t, err)

	thief := payFrom(t, 2, []*bt.Tx{parent}, []uint32{0})
	_, _, err = s.SpendAndCreate(ctx, thief, height-1)
	require.NoError(t, err)

	child := payFrom(t, 3, []*bt.Tx{parent, rootB}, []uint32{0, 0}) // rootB:0 is the independent input

	results, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{parent, child}, height, utxo.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Equal(t, utxo.MultiTxExisted, results[0].Status)
	require.Equal(t, utxo.MultiTxFailed, results[1].Status, "%v", results[1].Err)
	require.ErrorIs(t, results[1].Err, errors.ErrSpent)

	_, err = s.Get(ctx, child.TxIDChainHash())
	require.ErrorIs(t, err, errors.ErrTxNotFound, "the rejected child has no record")

	utxoHash, err := util.UTXOHashFromOutput(rootB.TxIDChainHash(), rootB.Outputs[0], 0)
	require.NoError(t, err)

	resp, err := s.GetSpend(ctx, &utxo.Spend{TxID: rootB.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash})
	require.NoError(t, err)
	require.Nil(t, resp.SpendingData, "rootB:0 is still marked spent by the rejected child")

	other := payFrom(t, 4, []*bt.Tx{rootB}, []uint32{0})
	_, _, err = s.SpendAndCreate(ctx, other, height)
	require.NoError(t, err, "rootB:0 is not spendable by another transaction")
}

// A parent created in an earlier step-2 chunk has its outputs netted: its child's spend of them is
// a spend-journal row committed with the parent, and no UTXO row is ever written. When a later
// chunk then meets a parent that already exists, the rest of the list goes to the per-transaction
// default, and a child the default rejects must not keep the netted spend that earlier chunk
// committed for it: the parent's output is unspent afterwards and another transaction can take it.
//
// Default chunk bounds. Level 0 holds 257 transactions, one more than multiCreateChunkTxs, so it
// is split: the first chunk holds the netted parent and 255 fillers and commits, the second holds
// only the parent that already exists and stops the netted write.
func TestSpendAndCreateMultiFallbackRestoresTheNettedSpendsOfARejectedChild(t *testing.T) {
	t.Run("netted", func(t *testing.T) {
		s, ctx := newUncheckpointedStore(t)
		checkRejectedChildLeavesNoSpend(t, s, ctx, func(txs []*bt.Tx, height uint32) ([]utxo.SpendAndCreateMultiResult, error) {
			return s.SpendAndCreateMulti(ctx, txs, height, utxo.WithIgnoreLocked(true))
		})
	})

	// The control: the same list through the per-transaction default alone, where each child's
	// spends and create share one database transaction.
	t.Run("per-transaction", func(t *testing.T) {
		s, ctx := newUncheckpointedStore(t)
		checkRejectedChildLeavesNoSpend(t, s, ctx, func(txs []*bt.Tx, height uint32) ([]utxo.SpendAndCreateMultiResult, error) {
			return utxo.DefaultSpendAndCreateMulti(ctx, s, utxo.SpendAndCreateMultiConcurrency(s.settings), txs, height, utxo.WithIgnoreLocked(true))
		})
	})
}

func checkRejectedChildLeavesNoSpend(t *testing.T, s *Store, ctx context.Context,
	run func(txs []*bt.Tx, height uint32) ([]utxo.SpendAndCreateMultiResult, error)) {
	t.Helper()

	const (
		height  = 1000
		fillers = 255
	)

	require.Equal(t, 256, multiCreateChunkTxs, "the test relies on the default create chunk bound")

	w := tests.BuildMultiWorkload(t, 0xb1, 1, fillers+3)
	w.StoreRoots(t, s, height-1)

	rootP, rootQ, rootB := w.Roots[0], w.Roots[fillers+1], w.Roots[fillers+2]

	parent := payFrom(t, 1, []*bt.Tx{rootP}, []uint32{0})

	// The second parent exists before the list runs, which stops the netted write at its chunk,
	// and a thief has taken the output the child spends, which the default then finds.
	existing := payFrom(t, 2, []*bt.Tx{rootQ}, []uint32{0})
	_, _, err := s.SpendAndCreate(ctx, existing, height-1)
	require.NoError(t, err)

	thief := payFrom(t, 3, []*bt.Tx{existing}, []uint32{0})
	_, _, err = s.SpendAndCreate(ctx, thief, height-1)
	require.NoError(t, err)

	child := payFrom(t, 4, []*bt.Tx{parent, existing, rootB}, []uint32{0, 0, 0})

	txs := append([]*bt.Tx{parent}, w.Txs[1:fillers+1]...)
	txs = append(txs, existing, child)

	results, err := run(txs, height)
	require.NoError(t, err)
	require.Equal(t, utxo.MultiTxCreated, results[0].Status, "%v", results[0].Err)
	require.Equal(t, utxo.MultiTxExisted, results[fillers+1].Status, "%v", results[fillers+1].Err)
	require.Equal(t, utxo.MultiTxFailed, results[fillers+2].Status, "%v", results[fillers+2].Err)
	require.ErrorIs(t, results[fillers+2].Err, errors.ErrSpent)

	_, err = s.Get(ctx, child.TxIDChainHash())
	require.ErrorIs(t, err, errors.ErrTxNotFound, "the rejected child has no record")

	for _, out := range []struct {
		name string
		tx   *bt.Tx
	}{{"parent:0", parent}, {"rootB:0", rootB}} {
		utxoHash, err := util.UTXOHashFromOutput(out.tx.TxIDChainHash(), out.tx.Outputs[0], 0)
		require.NoError(t, err)

		resp, err := s.GetSpend(ctx, &utxo.Spend{TxID: out.tx.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash})
		require.NoError(t, err)
		require.Nil(t, resp.SpendingData, "%s is still marked spent by the rejected child", out.name)
	}

	other := payFrom(t, 5, []*bt.Tx{parent, rootB}, []uint32{0, 0})
	_, _, err = s.SpendAndCreate(ctx, other, height)
	require.NoError(t, err, "parent:0 and rootB:0 are not spendable by another transaction")
}
