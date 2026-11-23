package utxo

import (
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// fanInList builds n one-output parents and a child that spends output 0 of
// each, returning the list (parents first) and its txids.
func fanInList(tb testing.TB, n int) ([]*bt.Tx, []chainhash.Hash) {
	tb.Helper()

	txs := make([]*bt.Tx, 0, n+1)
	child := bt.NewTx()

	for i := 0; i < n; i++ {
		parent := bt.NewTx()
		parent.LockTime = uint32(i) //nolint:gosec // test data

		in := &bt.Input{PreviousTxOutIndex: 0, UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
		require.NoError(tb, in.PreviousTxIDAdd(&chainhash.Hash{0xee, byte(i), byte(i >> 8), byte(i >> 16)}))
		parent.Inputs = append(parent.Inputs, in)
		parent.AddOutput(&bt.Output{Satoshis: 1000, LockingScript: bscript.NewFromBytes([]byte{0x51})})
		txs = append(txs, parent)

		spend := &bt.Input{PreviousTxOutIndex: 0, UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
		require.NoError(tb, spend.PreviousTxIDAdd(parent.TxIDChainHash()))
		child.Inputs = append(child.Inputs, spend)
	}

	child.AddOutput(&bt.Output{Satoshis: 1000, LockingScript: bscript.NewFromBytes([]byte{0x51})})
	txs = append(txs, child)

	txids := make([]chainhash.Hash, len(txs))
	for i, tx := range txs {
		txids[i] = *tx.TxIDChainHash()
	}

	return txs, txids
}

// A child spending several outputs of one parent records that parent once,
// and its distinct parents appear in the order its inputs name them.
func TestCheckSpendAndCreateMultiList_ParentsDeduplicatedInOrder(t *testing.T) {
	a := multiTx(t, 1, 3, outsideOutpoint(1))
	b := multiTx(t, 2, 2, outsideOutpoint(2))
	c := multiTx(t, 3, 1, outsideOutpoint(3))
	child := multiTx(t, 4, 1, outOf(b, 0), outOf(a, 0), outOf(b, 1), outOf(a, 2), outOf(c, 0), outOf(a, 1))
	grandchild := multiTx(t, 5, 1, outOf(child, 0))

	txs := []*bt.Tx{a, b, c, child, grandchild}
	txids := make([]chainhash.Hash, len(txs))

	for i, tx := range txs {
		txids[i] = *tx.TxIDChainHash()
	}

	parents, err := checkSpendAndCreateMultiList(txs, txids)
	require.NoError(t, err)
	require.Equal(t, [][]int{nil, nil, nil, {1, 0, 2}, {3}}, parents)
}

func BenchmarkCheckSpendAndCreateMultiList_FanIn(b *testing.B) {
	for _, n := range []int{80_000, 160_000} {
		b.Run(fmt.Sprintf("parents=%d", n), func(b *testing.B) {
			txs, txids := fanInList(b, n)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if _, err := checkSpendAndCreateMultiList(txs, txids); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
