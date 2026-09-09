package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// drain pulls every batch from an iterator and returns the flattened result.
func drain(t *testing.T, it utxo.UnminedTxIterator, ctx context.Context) []*utxo.UnminedTransaction {
	t.Helper()

	var all []*utxo.UnminedTransaction

	for {
		batch, err := it.Next(ctx)
		require.NoError(t, err)

		if len(batch) == 0 {
			break
		}

		all = append(all, batch...)
	}

	require.NoError(t, it.Err())
	require.NoError(t, it.Close())

	return all
}

// hashesOf reduces a batch to the set of transaction ids it named.
func hashesOf(txs []*utxo.UnminedTransaction) map[string]*utxo.UnminedTransaction {
	m := make(map[string]*utxo.UnminedTransaction, len(txs))
	for _, tx := range txs {
		m[tx.Node.Hash.String()] = tx
	}

	return m
}

// TestUnminedIteratorReturnsEveryTransactionWaitingToBeMined is the query block assembly
// rebuilds its whole mempool from, at startup and after every reorg, with no height bound.
//
// A transaction missing from this answer never gets mined. On a delete-on-spend store that
// is unrecoverable rather than merely slow: its inputs' coin rows were deleted when it was
// first accepted, and an absent coin row reads as already spent, so nobody can ever spend
// them again.
func TestUnminedIteratorReturnsEveryTransactionWaitingToBeMined(t *testing.T) {
	s, ctx := newTestStore(t)

	waiting := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, waiting, 700_000)
	require.NoError(t, err)

	mined := mkTx(t, 1, 2_000)
	_, err = s.Create(ctx, mined, 700_000, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 700_000, OnLongestChain: true}))
	require.NoError(t, err)

	it, err := s.GetUnminedTxIterator()
	require.NoError(t, err)

	got := hashesOf(drain(t, it, ctx))

	require.Contains(t, got, waiting.TxID(), "a transaction nobody has mined must be offered to block assembly")
	require.NotContains(t, got, mined.TxID(), "one already on the longest chain must not be")
}

// TestUnminedIteratorReturnsAForkMinedTransaction is the case the whole predicate rests on,
// and the reason "has no block membership" is the wrong test.
//
// A transaction mined only into a block that lost carries block membership AND is still
// waiting to be mined. Testing for empty membership would drop it. Measured on a 20 million
// row table, that weaker test returned 25,000 of 25,499 waiting transactions: the 499 it
// missed were exactly these.
// The fork membership is now reached by a stamp rather than by a create carrying block
// information. That create takes the block path, which writes no identity row, and this
// iterator walks the identity table. At the tip the stamp is the only shape there is:
// everything but the coinbase arrives from the mempool first, and a block that later loses
// stamped it on the way past.
func TestUnminedIteratorReturnsAForkMinedTransaction(t *testing.T) {
	s, ctx := newTestStore(t)

	forked := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, forked, 700_000)
	require.NoError(t, err)

	// Stamped by a block nobody claims is on the longest chain, so the marker is left alone.
	_, err = s.SetMinedMulti(ctx, hashes(forked),
		utxo.MinedBlockInfo{BlockID: 77, BlockHeight: 700_000})
	require.NoError(t, err)

	// Block assembly determines that block is not on the main chain. That is how this state is
	// reached: by someone establishing the fact, not by the store guessing at create time when
	// nobody could know it yet.
	require.NoError(t, s.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*forked.TxIDChainHash()}, false))

	it, err := s.GetUnminedTxIterator()
	require.NoError(t, err)

	got := hashesOf(drain(t, it, ctx))

	require.Contains(t, got, forked.TxID(),
		"membership names a block, but no MAIN-CHAIN block, so this transaction is still waiting")
	require.NotEmpty(t, got[forked.TxID()].BlockIDs, "and the block it was in is still reported")
}

// TestUnminedIteratorSkipsConflictingTransactions. Block assembly's own query excludes them,
// because a transaction that lost a double-spend race must not be offered for mining.
func TestUnminedIteratorSkipsConflictingTransactions(t *testing.T) {
	s, ctx := newTestStore(t)

	ok := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, ok, 700_000)
	require.NoError(t, err)

	loser := mkTx(t, 1, 2_000)
	_, err = s.Create(ctx, loser, 700_000, utxo.WithConflicting(true))
	require.NoError(t, err)

	it, err := s.GetUnminedTxIterator()
	require.NoError(t, err)

	got := hashesOf(drain(t, it, ctx))

	require.Contains(t, got, ok.TxID())
	require.NotContains(t, got, loser.TxID(), "a conflict loser must not be offered for mining")
}

// TestUnminedIteratorCarriesWhatBlockAssemblyNeedsToRebuildACandidate.
//
// Block assembly reassembles a mining candidate from the fee, the size and the inputs, not
// from the serialized transaction. That is what lets the body age out of its window while
// the transaction stays mineable.
func TestUnminedIteratorCarriesWhatBlockAssemblyNeedsToRebuildACandidate(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash: parent.TxIDChainHash(), Vout: 0,
		LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis,
	}))
	child.AddOutput(&bt.Output{Satoshis: 4_000, LockingScript: parent.Outputs[0].LockingScript})

	_, err = s.Create(ctx, child, 200)
	require.NoError(t, err)

	it, err := s.GetUnminedTxIterator()
	require.NoError(t, err)

	got := hashesOf(drain(t, it, ctx))
	require.Contains(t, got, child.TxID())

	c := got[child.TxID()]
	require.Equal(t, uint64(child.Size()), c.Node.SizeInBytes)
	require.Equal(t, 200, c.UnminedSince, "the height it has been waiting from")
	require.NotNil(t, c.TxInpoints, "the inputs are how a candidate is rebuilt without the body")
	require.Equal(t, 1, len(c.TxInpoints.ParentTxHashes), "one input, one parent")
	require.Equal(t, *parent.TxIDChainHash(), c.TxInpoints.ParentTxHashes[0])
}

// TestPrunableUnminedIteratorBoundsByAge. The preservation pass asks a narrower question:
// which waiting transactions have been waiting longer than the retention window, so their
// parents need their lifetime extended.
func TestPrunableUnminedIteratorBoundsByAge(t *testing.T) {
	s, ctx := newTestStore(t)

	old := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, old, 700_000)
	require.NoError(t, err)

	recent := mkTx(t, 1, 2_000)
	_, err = s.Create(ctx, recent, 700_900)
	require.NoError(t, err)

	// Cutoff sits between the two.
	it, err := s.GetPrunableUnminedTxIterator(700_500)
	require.NoError(t, err)

	got := hashesOf(drain(t, it, ctx))

	require.Contains(t, got, old.TxID(), "waiting since before the cutoff")
	require.NotContains(t, got, recent.TxID(), "still inside the window")
}

// TestUnminedIteratorIsEmptyWhenNothingIsWaiting, which must be an empty answer rather than
// an error, because it is the ordinary state of a caught-up node.
func TestUnminedIteratorIsEmptyWhenNothingIsWaiting(t *testing.T) {
	s, ctx := newTestStore(t)

	it, err := s.GetUnminedTxIterator()
	require.NoError(t, err)

	require.Empty(t, drain(t, it, ctx))
}

// TestUnminedIteratorEndsWithNil is the terminator the callers actually test for, and getting
// it wrong does not fail, it spins.
//
// Every consumer of this iterator breaks its loop on a NIL batch, not on an empty one. Handing
// back an empty but non-nil slice therefore never ends the loop. On the mainnet box that spun
// inside one call for 67 minutes, burning 15% of the machine's CPU on allocating and zeroing the
// same slice, and it held up the pruner's second phase, which is the reclaim, so nothing was
// ever reclaimed.
//
// The other two iterators in this package and the sql store's version all return nil here. This
// one did not.
func TestUnminedIteratorEndsWithNil(t *testing.T) {
	s, ctx := newTestStore(t)

	// Nothing waiting to be mined, which is the ordinary state and the one that spun.
	it, err := s.GetUnminedTxIterator()
	require.NoError(t, err)

	defer func() { require.NoError(t, it.Close()) }()

	batch, err := it.Next(ctx)
	require.NoError(t, err)
	require.Nil(t, batch, "an exhausted iterator must return nil, because every caller breaks on nil")
}

// TestUnminedIteratorEndsWithNilAfterAFullDrain, so the terminator arrives after real rows too
// rather than only on an empty store.
func TestUnminedIteratorEndsWithNilAfterAFullDrain(t *testing.T) {
	s, ctx := newTestStore(t)

	for i := 0; i < 3; i++ {
		tx := mkTx(t, 1, uint64(1_000+i))
		_, err := s.Create(ctx, tx, 700_000)
		require.NoError(t, err)
	}

	it, err := s.GetUnminedTxIterator()
	require.NoError(t, err)

	defer func() { require.NoError(t, it.Close()) }()

	first, err := it.Next(ctx)
	require.NoError(t, err)
	require.Len(t, first, 3)

	second, err := it.Next(ctx)
	require.NoError(t, err)
	require.Nil(t, second, "the call after the last row must terminate the caller's loop")
}
