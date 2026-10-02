package subtreevalidation

import (
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// parentsBatch builds a batchState over txs with every transaction missing,
// as processTransactionsBatched does before resolveFromMemory.
func parentsBatch(txs []*bt.Tx) *batchState {
	b := &batchState{
		txs:      txs,
		hashes:   make([]chainhash.Hash, len(txs)),
		position: make(map[chainhash.Hash]int, len(txs)),
		heights:  make([][]uint32, len(txs)),
		parents:  make([][]int, len(txs)),
		fallback: make([]bool, len(txs)),
	}

	for i, tx := range txs {
		b.hashes[i] = *tx.TxIDChainHash()
		b.position[b.hashes[i]] = i
		b.missing = append(b.missing, i)
	}

	return b
}

// spendOf builds a transaction spending the given outputs of the given
// parents, with one OP_TRUE output.
func spendOf(tb testing.TB, lockTime uint32, parents []*bt.Tx, vouts []uint32) *bt.Tx {
	tb.Helper()

	tx := bt.NewTx()
	tx.LockTime = lockTime

	for n, p := range parents {
		in := &bt.Input{PreviousTxOutIndex: vouts[n], UnlockingScript: bscript.NewFromBytes([]byte{}), SequenceNumber: 0xffffffff}
		require.NoError(tb, in.PreviousTxIDAdd(p.TxIDChainHash()))
		tx.Inputs = append(tx.Inputs, in)
	}

	tx.AddOutput(&bt.Output{Satoshis: 1000, LockingScript: opTrue})

	return tx
}

// outsideTx builds a transaction with nOutputs OP_TRUE outputs spending an
// outpoint outside the batch.
func outsideTx(tb testing.TB, seed uint32, nOutputs int) *bt.Tx {
	tb.Helper()

	tx := bt.NewTx()
	tx.LockTime = seed

	in := &bt.Input{PreviousTxOutIndex: 0, UnlockingScript: bscript.NewFromBytes([]byte{0x00}), SequenceNumber: 0xffffffff}
	require.NoError(tb, in.PreviousTxIDAdd(&chainhash.Hash{0xaa, byte(seed), byte(seed >> 8), byte(seed >> 16)}))
	tx.Inputs = append(tx.Inputs, in)

	for i := 0; i < nOutputs; i++ {
		tx.AddOutput(&bt.Output{Satoshis: 1000, LockingScript: opTrue})
	}

	return tx
}

// A transaction spending several outputs of one same-batch parent records that
// parent once, and its distinct parents appear in the order its inputs name
// them.
func TestResolveFromMemory_ParentsDeduplicatedInOrder(t *testing.T) {
	a := outsideTx(t, 1, 3)
	b := outsideTx(t, 2, 2)
	c := outsideTx(t, 3, 1)
	child := spendOf(t, 4, []*bt.Tx{b, a, b, a, c, a}, []uint32{0, 0, 1, 2, 0, 1})
	grandchild := spendOf(t, 5, []*bt.Tx{child}, []uint32{0})

	batch := parentsBatch([]*bt.Tx{a, b, c, child, grandchild})

	_, _, err := resolveFromMemory(batch, batchedTestHeight)
	require.NoError(t, err)
	require.Equal(t, [][]int{nil, nil, nil, {1, 0, 2}, {3}}, batch.parents)
}

func BenchmarkResolveFromMemory_FanIn(b *testing.B) {
	for _, n := range []int{80_000, 160_000} {
		b.Run(fmt.Sprintf("parents=%d", n), func(b *testing.B) {
			txs := make([]*bt.Tx, 0, n+1)
			vouts := make([]uint32, n)

			for i := 0; i < n; i++ {
				txs = append(txs, outsideTx(b, uint32(i), 1)) //nolint:gosec // test data
			}

			txs = append(txs, spendOf(b, 0xffffffff, txs, vouts))
			batch := parentsBatch(txs)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				for j := range batch.parents {
					batch.parents[j] = nil
				}

				if _, _, err := resolveFromMemory(batch, batchedTestHeight); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// A block whose transaction spends several outputs of the same in-block parents,
// in an interleaved order, is checked and written whole on the batch path: every
// record is created and every one of those outputs is spent by the right input.
func TestCheckBlockSubtreesBatched_RepeatedInBlockParents(t *testing.T) {
	f := newBatchedFixture(t, blockchain.FSMStateCATCHINGBLOCKS)

	roots := []*bt.Tx{storedRoot(t, f, 1, opTrue), storedRoot(t, f, 2, opTrue), storedRoot(t, f, 3, opTrue)}
	a := opTrueTx(t, 11, []*bt.Tx{roots[0]}, []uint32{0})
	b := opTrueTx(t, 12, []*bt.Tx{roots[1]}, []uint32{0})
	c := opTrueTx(t, 13, []*bt.Tx{roots[2]}, []uint32{0})
	child := opTrueTx(t, 14, []*bt.Tx{b, a, b, c, a}, []uint32{0, 0, 1, 0, 1})
	grandchild := opTrueTx(t, 15, []*bt.Tx{child}, []uint32{0})
	txs := []*bt.Tx{a, b, c, child, grandchild}

	require.NoError(t, checkBlock(t, f, storeBlock(t, f, txs, false)))
	require.Positive(t, f.store.multiCalls.Load(), "catch-up above the checkpoint must take the batch path")

	requireCreatedUnmined(t, f, txs)

	for _, s := range []struct {
		parent *bt.Tx
		vout   uint32
		child  *bt.Tx
		vin    int
	}{{b, 0, child, 0}, {a, 0, child, 1}, {b, 1, child, 2}, {c, 0, child, 3}, {a, 1, child, 4}, {child, 0, grandchild, 0}} {
		utxoHash, err := util.UTXOHashFromOutput(s.parent.TxIDChainHash(), s.parent.Outputs[s.vout], s.vout)
		require.NoError(t, err)

		resp, err := f.store.GetSpend(context.Background(), &utxo.Spend{TxID: s.parent.TxIDChainHash(), Vout: s.vout, UTXOHash: utxoHash})
		require.NoError(t, err)
		require.NotNil(t, resp.SpendingData, "%s:%d is unspent", s.parent.TxIDChainHash(), s.vout)
		require.Equal(t, *s.child.TxIDChainHash(), *resp.SpendingData.TxID)
		require.Equal(t, s.vin, resp.SpendingData.Vin)
	}
}

// When a parent spent several times by one child falls back to the
// per-transaction path, the child falls back with it, however many of its inputs
// name that parent, and ends up exactly as the per-transaction path leaves it.
// Lists of two put the child in a list after its parent's, so it is the batch
// path's own parent bookkeeping, not the store's, that has to send it back.
func TestProcessTransactionsBatched_RepeatedParentFallsBackWithChild(t *testing.T) {
	f := newBatchedFixture(t, blockchain.FSMStateCATCHINGBLOCKS)
	f.server.settings.SubtreeValidation.SpendAndCreateMultiMaxTxs = 2

	checker, ok := f.server.batchChecker(blockchain.FSMStateCATCHINGBLOCKS, batchedTestHeight)
	require.True(t, ok)

	roots := []*bt.Tx{storedRoot(t, f, 1, opTrue), storedRoot(t, f, 2, opTrue)}

	// An unmined transaction spends root 0's output 0 first, so loser's spend
	// fails on the batch path and it goes through the per-transaction path.
	thief := opTrueTx(t, 99, []*bt.Tx{roots[0]}, []uint32{0})
	_, err := f.validator.Validate(context.Background(), thief, batchedTestHeight)
	require.NoError(t, err)

	loser := opTrueTx(t, 1, []*bt.Tx{roots[0]}, []uint32{0})
	winner := opTrueTx(t, 2, []*bt.Tx{roots[1]}, []uint32{0})
	child := opTrueTx(t, 3, []*bt.Tx{loser, winner, loser}, []uint32{0, 0, 1})

	err = f.server.processTransactionsBatched(context.Background(), checker, []*bt.Tx{loser, winner, child}, chainhash.Hash{}, batchedTestHeight, 0, 0, map[uint32]bool{})
	require.NoError(t, err)
	require.Equal(t, int64(1), f.store.multiCalls.Load(), "the child's list is never sent: the child falls back before it")

	requireCreatedUnmined(t, f, []*bt.Tx{winner})

	for _, tx := range []*bt.Tx{loser, child} {
		md, err := f.store.Get(context.Background(), tx.TxIDChainHash())
		require.NoError(t, err)
		require.True(t, md.Conflicting, "%s went through the per-transaction path, which creates it conflicting", tx.TxIDChainHash())
	}
}
