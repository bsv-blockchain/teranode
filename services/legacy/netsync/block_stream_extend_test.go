package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// noopEmit, used below, already exists in block_stream_dedup_test.go in this
// same package.

// streamBuilderWithParentAndChild returns a fresh builder, a funding
// transaction, and a transaction that spends that funding transaction's only
// output. Neither has been fed to the builder yet: it is the caller's choice
// of what to add, and in what order, that decides whether the child gets
// extended.
//
// child's input is built through FromUTXOs with its locking script and
// satoshis left unset, rather than through Tx.From (as streamTx in
// block_stream_builder_test.go does): From always produces a non-nil
// LockingScript, even from an empty hex string, so a transaction built that
// way already reads as "extended" before extendFromBlock ever runs. Leaving
// the input's PreviousTxScript nil here is what makes the two tests that use
// this fixture able to tell in-block extension apart from an already-filled
// input.
func streamBuilderWithParentAndChild(t *testing.T) (b *blockStreamBuilder, parent *bt.Tx, child *bt.Tx) {
	t.Helper()

	const txCount = 8

	b, err := newBlockStreamBuilder(txCount, 8, coinbaseTx(t), noopEmit, newDedupMap(txCount))
	require.NoError(t, err)

	parent = simpleFundingTx(t, 1000)

	child = bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash: parent.TxIDChainHash(),
		Vout:     0,
	}))
	require.NoError(t, child.PayToAddress("1BitcoinEaterAddressDontSendf59kuE", 900))

	return b, parent, child
}

// streamBuilderForBounds returns a builder whose remembered-outputs window is
// set to limit, small enough to exercise eviction within a short loop. The
// window cannot be handed in through newBlockStreamBuilder's own parameters —
// it is not part of that constructor's contract — so the test reaches past it
// and sets the unexported field directly, which is available to it because
// this file is in the same package.
func streamBuilderForBounds(t *testing.T, limit int) *blockStreamBuilder {
	t.Helper()

	const txCount = 64
	const maxItems = 8

	b, err := newBlockStreamBuilder(txCount, maxItems, coinbaseTx(t), noopEmit, newDedupMap(txCount))
	require.NoError(t, err)

	b.recentLimit = limit

	return b
}

// simpleFundingTx builds a transaction with no real inputs and a single
// output of satoshis, standing in for an earlier transaction in the block
// whose output a later one spends. It follows the same shape as coinbaseTx
// in block_stream_builder_test.go; varying satoshis across calls is what
// gives repeated calls distinct transaction ids.
func simpleFundingTx(t *testing.T, satoshis uint64) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.PayToAddress("1BitcoinEaterAddressDontSendf59kuE", satoshis))

	return tx
}

// TestStreamBuilder_ExtendsAParentInTheSameBlock is the cheap half of extension.
// The parent's outputs went past this code moments ago, so filling the child's
// input in costs a map lookup and no store call at all.
func TestStreamBuilder_ExtendsAParentInTheSameBlock(t *testing.T) {
	b, parent, child := streamBuilderWithParentAndChild(t)

	require.NoError(t, b.AddTx(parent, parent.TxIDChainHash()))
	require.NoError(t, b.AddTx(child, child.TxIDChainHash()))

	require.True(t, child.IsExtended(),
		"a child whose parent is in the same block must be extended, because the parent just went past")
	require.Equal(t, parent.Outputs[0].Satoshis, child.Inputs[0].PreviousTxSatoshis)
	require.Equal(t, parent.Outputs[0].LockingScript.String(), child.Inputs[0].PreviousTxScript.String())
}

// TestStreamBuilder_LeavesAnOutOfBlockParentAlone is the other half, and it is
// what keeps this cheap. A parent in an earlier block would need a store lookup
// per input on the socket goroutine, which is the cost this whole path exists to
// avoid. The reader re-extends those on demand, which it already does.
func TestStreamBuilder_LeavesAnOutOfBlockParentAlone(t *testing.T) {
	b, _, orphanChild := streamBuilderWithParentAndChild(t)

	require.NoError(t, b.AddTx(orphanChild, orphanChild.TxIDChainHash()))

	require.False(t, orphanChild.IsExtended(),
		"a parent from an earlier block is not looked up; the reader re-extends it")
}

// TestStreamBuilder_StampsARealFeeWhenEveryInputIsExtended pins what extension
// buys. The fee is not required to be right here: subtree validation rewrites it
// from transaction metadata before any consensus check reads it. Stamping it when
// it is free simply saves that work.
func TestStreamBuilder_StampsARealFeeWhenEveryInputIsExtended(t *testing.T) {
	b, parent, child := streamBuilderWithParentAndChild(t)

	require.NoError(t, b.AddTx(parent, parent.TxIDChainHash()))
	require.NoError(t, b.AddTx(child, child.TxIDChainHash()))

	fee := b.current.Nodes[b.current.Length()-1].Fee
	require.Positive(t, fee,
		"a fully extended transaction has a computable fee, so stamping zero throws away something already in hand")
}

// TestStreamBuilder_DoesNotExtendFromItself is the ordering claim: a
// transaction must not extend from its own outputs. Nothing in the builder
// checks that a caller-supplied hash actually matches the transaction it
// names, so a transaction whose input's previous-transaction id equals the
// hash passed for that same AddTx call is constructible, not theoretical —
// this builds exactly that and pins that it comes out unextended, because its
// own outputs are not remembered until after it has already been processed.
func TestStreamBuilder_DoesNotExtendFromItself(t *testing.T) {
	const txCount = 8

	b, err := newBlockStreamBuilder(txCount, 8, coinbaseTx(t), noopEmit, newDedupMap(txCount))
	require.NoError(t, err)

	var selfHash chainhash.Hash
	selfHash[0] = 0xAB

	tx := bt.NewTx()
	require.NoError(t, tx.FromUTXOs(&bt.UTXO{TxIDHash: &selfHash, Vout: 0, Satoshis: 1000}))
	require.NoError(t, tx.PayToAddress("1BitcoinEaterAddressDontSendf59kuE", 900))

	require.NoError(t, b.AddTx(tx, &selfHash))

	require.False(t, tx.IsExtended(),
		"a transaction must not extend from its own outputs; they are not remembered until after it is processed")
}

// TestStreamBuilder_BoundsWhatItRemembers is the memory guard. Remembering every
// output of every transaction is the thing streaming exists to avoid: at target
// scale that is the whole block's output data back in the heap.
func TestStreamBuilder_BoundsWhatItRemembers(t *testing.T) {
	b := streamBuilderForBounds(t, 4)

	for i := 0; i < 50; i++ {
		tx := simpleFundingTx(t, uint64(1000+i))
		require.NoError(t, b.AddTx(tx, tx.TxIDChainHash()))
	}

	require.LessOrEqual(t, b.rememberedOutputs(), 4,
		"the map must be bounded; an unbounded one puts the whole block's outputs back in the heap")
}
