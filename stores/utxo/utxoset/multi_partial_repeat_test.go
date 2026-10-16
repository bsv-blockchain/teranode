package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

// Quick validation repeats only the transactions of a list that were not Created or Existed,
// with the list parents of each (the SpendAndCreateMulti contract in Interface.go). The netted
// write above the checkpoint reports failures per transaction, so the shorter repeat must finish
// the write. Here a parent goes through and its child fails on an outside coin that is missing.
// The parent's output that the child spends must not be lost to netting: once the coin turns
// up, a repeat writes the child, and both its spends name it. Quick validation repeats the child
// with its parent; the child alone must work too.
func TestSpendAndCreateMultiRepeatOfAFailedChild(t *testing.T) {
	for _, withParent := range []bool{true, false} {
		name := "child alone"
		if withParent {
			name = "child with its parent"
		}

		t.Run(name, func(t *testing.T) {
			s, ctx := newUncheckpointedStore(t)

			const height = 1100

			w := tests.BuildMultiWorkload(t, 0xb1, 1, 2) // two roots to spend from
			rootA, rootB := w.Roots[0], w.Roots[1]

			_, _, err := s.SpendAndCreate(ctx, rootA, height-1, utxo.WithCreateOnly())
			require.NoError(t, err)

			parent := payFrom(t, 1, []*bt.Tx{rootA}, []uint32{0})
			child := payFrom(t, 2, []*bt.Tx{parent, rootB}, []uint32{0, 0}) // rootB is not stored yet

			results, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{parent, child}, height, utxo.WithIgnoreLocked(true))
			require.NoError(t, err, "a per-transaction failure is reported in the results, not for the whole call")
			require.Equal(t, utxo.MultiTxCreated, results[0].Status, "%v", results[0].Err)
			require.Equal(t, utxo.MultiTxFailed, results[1].Status)
			require.Equal(t, len(parent.Outputs), utxoRowsOf(t, s, parent), "the failed child's spend of the parent is not netted away")

			_, _, err = s.SpendAndCreate(ctx, rootB, height-1, utxo.WithCreateOnly())
			require.NoError(t, err)

			repeat := []*bt.Tx{child}
			if withParent {
				repeat = []*bt.Tx{parent, child}
			}

			results, err = s.SpendAndCreateMulti(ctx, repeat, height, utxo.WithIgnoreLocked(true))
			require.NoError(t, err)

			if withParent {
				require.Equal(t, utxo.MultiTxExisted, results[0].Status, "the parent of the repeat: %v", results[0].Err)
			}

			last := results[len(results)-1]
			require.Equal(t, utxo.MultiTxCreated, last.Status, "the child of the repeat: %v", last.Err)

			_, err = s.Get(ctx, child.TxIDChainHash())
			require.NoError(t, err)

			for i, spent := range []struct {
				tx   *bt.Tx
				vout uint32
			}{{parent, 0}, {rootB, 0}} {
				thief := payFrom(t, uint32(10+i), []*bt.Tx{spent.tx}, []uint32{spent.vout}) //nolint:gosec // test data
				_, spends, err := s.SpendAndCreate(ctx, thief, height+1)
				require.ErrorIs(t, err, errors.ErrSpent, "input %d", i)
				require.Len(t, spends, 1)
				require.NotNil(t, spends[0].ConflictingTxID)
				require.Equal(t, *child.TxIDChainHash(), *spends[0].ConflictingTxID, "input %d is spent by the child", i)
			}
		})
	}
}
