package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// newDedupMap returns a fresh in-memory duplicate-transaction map, sized for
// txCount entries. These tests are about partitioning and streaming, not about
// dedup (see block_stream_dedup_test.go for that), but the constructor now
// requires a real map rather than accepting nil.
func newDedupMap(txCount int) txmap.TxMap {
	return txmap.NewSplitSwissMapUint64(uint32(txCount)) //nolint:gosec // test tx count is small and non-negative
}

// streamTx builds a distinct, well-formed transaction for index i, spending
// output 0 of a synthetic per-index parent id that is never itself added to
// any builder in this file. bt.NewOutput does not exist in go-bt v2.7.1, so
// the input is built through FromUTXOs instead of a literal *bt.Input.
//
// FromUTXOs is used rather than Tx.From deliberately: From parses its
// prevTxLockingScript argument with bscript.NewFromHexString even when given
// "", which returns a non-nil, empty *Script — so a transaction built with
// From already reads as extended (IsExtended only checks for a nil script)
// before block_stream_builder.go's extendFromBlock ever runs, which would
// mask the very thing that function is testing. Leaving LockingScript unset on
// the UTXO here keeps PreviousTxScript genuinely nil, i.e. not yet extended,
// which is what a transaction spending an out-of-block parent actually looks
// like on the wire. Because the parent id is synthetic and distinct from any
// real transaction hash, extendFromBlock's lookup always misses for these
// transactions regardless of what has already streamed past, so every test in
// this file that predates fee stamping keeps seeing a fee of zero.
func streamTx(t *testing.T, i int) (*bt.Tx, *chainhash.Hash) {
	t.Helper()

	tx := bt.NewTx()
	tx.Version = 1

	var parent chainhash.Hash
	parent[0] = byte(i)
	parent[1] = byte(i >> 8)
	parent[2] = byte(i >> 16)

	require.NoError(t, tx.FromUTXOs(&bt.UTXO{TxIDHash: &parent, Vout: 0, Satoshis: 1000}))
	require.NoError(t, tx.PayToAddress("1BitcoinEaterAddressDontSendf59kuE", 900))

	return tx, tx.TxIDChainHash()
}

// coinbaseTx builds a transaction with no real inputs, standing in for the
// block's coinbase. Only its id and size are used.
func coinbaseTx(t *testing.T) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.PayToAddress("1BitcoinEaterAddressDontSendf59kuE", 5000000000))

	return tx
}

// TestBlockStreamBuilder_EmitsEachSubtreeAsItFills is the memory claim. The
// builder must hand a subtree off the moment it is full and stop referencing it,
// because holding them all is exactly what this replaces.
func TestBlockStreamBuilder_EmitsEachSubtreeAsItFills(t *testing.T) {
	const maxItems = 8
	const txCount = 20

	emitted := make([]int, 0, 3)
	lengths := make([]int, 0, 3)

	emit := func(index int, st *subtreepkg.Subtree, data *subtreepkg.Data, meta *subtreepkg.Meta) error {
		require.NotNil(t, st)
		require.NotNil(t, data)
		require.NotNil(t, meta)

		emitted = append(emitted, index)
		lengths = append(lengths, st.Length())

		return nil
	}

	b, err := newBlockStreamBuilder(txCount, maxItems, coinbaseTx(t), emit, newDedupMap(txCount))
	require.NoError(t, err)

	// The coinbase occupies slot zero and is added by the builder, so the stream
	// supplies txCount-1 further transactions.
	for i := 1; i < txCount; i++ {
		tx, hash := streamTx(t, i)
		require.NoError(t, b.AddTx(tx, hash))
	}

	require.Equal(t, []int{0, 1}, emitted,
		"the first two subtrees must be emitted during the stream, not held until the end")

	root, subtreeHashes, err := b.Finish()
	require.NoError(t, err)
	require.NotNil(t, root)

	require.Equal(t, []int{0, 1, 2}, emitted, "the final subtree is emitted by Finish")
	require.Equal(t, []int{8, 8, 4}, lengths, "only the final subtree may be short")
	require.Len(t, subtreeHashes, 3)
}

// TestBlockStreamBuilder_RootMatchesTheAllAtOnceComputation is the correctness
// claim: streaming must not change the answer.
func TestBlockStreamBuilder_RootMatchesTheAllAtOnceComputation(t *testing.T) {
	const maxItems = 8
	const txCount = 20

	kept := make([]*subtreepkg.Subtree, 0, 3)

	emit := func(index int, st *subtreepkg.Subtree, data *subtreepkg.Data, meta *subtreepkg.Meta) error {
		kept = append(kept, st)

		return nil
	}

	cb := coinbaseTx(t)

	b, err := newBlockStreamBuilder(txCount, maxItems, cb, emit, newDedupMap(txCount))
	require.NoError(t, err)

	for i := 1; i < txCount; i++ {
		tx, hash := streamTx(t, i)
		require.NoError(t, b.AddTx(tx, hash))
	}

	got, _, err := b.Finish()
	require.NoError(t, err)

	want, err := referenceRootFromSubtrees(kept, cb.TxIDChainHash(), uint64(cb.Size()))
	require.NoError(t, err)

	require.Equal(t, want.String(), got.String(),
		"streaming the subtrees must produce the same root as building them all first")
}

// TestBlockStreamBuilder_RefusesMoreTransactionsThanDeclared pins the bound. The
// transaction count comes off the wire before the transactions, so a peer sending
// more than it declared is misbehaving and must not be able to grow our state.
func TestBlockStreamBuilder_RefusesMoreTransactionsThanDeclared(t *testing.T) {
	b, err := newBlockStreamBuilder(4, 8, coinbaseTx(t), func(int, *subtreepkg.Subtree, *subtreepkg.Data, *subtreepkg.Meta) error {
		return nil
	}, newDedupMap(4))
	require.NoError(t, err)

	for i := 1; i < 4; i++ {
		tx, hash := streamTx(t, i)
		require.NoError(t, b.AddTx(tx, hash))
	}

	tx, hash := streamTx(t, 99)

	err = b.AddTx(tx, hash)
	require.Error(t, err)
	require.Contains(t, err.Error(), "more transactions than the 4 it declared")
}

// TestBlockStreamBuilder_RefusesACoinbaseOnlyBlock pins the divergence from
// prepareSubtrees (handle_block.go), which returns zero subtrees for a
// coinbase-only block instead of calling this builder at all. Left unguarded,
// a leaf count of 1 would make this builder emit one subtree whose root is the
// go-subtree CoinbasePlaceholder constant — the same root for every
// coinbase-only block in the chain, so each would overwrite the last one's
// files under the same three keys. This must fail before that can happen.
func TestBlockStreamBuilder_RefusesACoinbaseOnlyBlock(t *testing.T) {
	_, err := newBlockStreamBuilder(1, 8, coinbaseTx(t), func(int, *subtreepkg.Subtree, *subtreepkg.Data, *subtreepkg.Meta) error {
		return nil
	}, newDedupMap(1))
	require.Error(t, err)
	require.Contains(t, err.Error(), "coinbase-only block, got tx count 1")
}

// TestBlockStreamBuilder_RefusesFewerTransactionsThanDeclared pins the other end.
// A truncated stream must fail rather than produce a root over a short block.
func TestBlockStreamBuilder_RefusesFewerTransactionsThanDeclared(t *testing.T) {
	b, err := newBlockStreamBuilder(8, 8, coinbaseTx(t), func(int, *subtreepkg.Subtree, *subtreepkg.Data, *subtreepkg.Meta) error {
		return nil
	}, newDedupMap(8))
	require.NoError(t, err)

	for i := 1; i < 5; i++ {
		tx, hash := streamTx(t, i)
		require.NoError(t, b.AddTx(tx, hash))
	}

	_, _, err = b.Finish()
	require.Error(t, err)
	require.Contains(t, err.Error(), "5 of the 8 transactions it declared")
}

// TestBlockStreamBuilder_StopsOnAnEmitFailure pins that a failed write aborts the
// block. There is no second source for a subtree file, so continuing past a write
// failure would produce a block whose files are incomplete.
func TestBlockStreamBuilder_StopsOnAnEmitFailure(t *testing.T) {
	b, err := newBlockStreamBuilder(20, 8, coinbaseTx(t), func(int, *subtreepkg.Subtree, *subtreepkg.Data, *subtreepkg.Meta) error {
		return errors.NewStorageError("disk on fire")
	}, newDedupMap(20))
	require.NoError(t, err)

	var lastErr error

	for i := 1; i < 20 && lastErr == nil; i++ {
		tx, hash := streamTx(t, i)
		lastErr = b.AddTx(tx, hash)
	}

	require.Error(t, lastErr)
	require.Contains(t, lastErr.Error(), "disk on fire")
}

// TestBlockStreamBuilder_DropsEachSubtreeAfterEmitting is the memory claim stated
// as a test rather than a comment. Holding the subtrees would produce identical
// output, so nothing else in this file would notice.
//
// The mid-stream checks (after the first of three subtrees) only pin the shape
// of streaming, not the memory claim: startSubtree unconditionally installs a
// fresh current/currentData/currentMeta for the next subtree regardless of
// whether the old one was dropped or held elsewhere, so those fields look
// identical either way at that point. The only place a "hold instead of drop"
// mutation is externally observable is after the FINAL subtree, where no next
// subtree is started: that is where this test's discriminating assertions live.
func TestBlockStreamBuilder_DropsEachSubtreeAfterEmitting(t *testing.T) {
	b, err := newBlockStreamBuilder(20, 8, coinbaseTx(t), func(int, *subtreepkg.Subtree, *subtreepkg.Data, *subtreepkg.Meta) error {
		return nil
	}, newDedupMap(20))
	require.NoError(t, err)

	// The coinbase already occupies slot zero of the first subtree (capacity 8),
	// so it takes 7 more real transactions, not 8, to fill and emit it.
	for i := 1; i < 8; i++ {
		tx, hash := streamTx(t, i)
		require.NoError(t, b.AddTx(tx, hash))
	}

	require.Equal(t, 1, b.emitted, "the first subtree should have been emitted")
	require.NotNil(t, b.current, "and a fresh one opened")
	require.Equal(t, 0, b.current.Length(), "which is empty")
	require.Len(t, b.subtreeHashes, 1, "only the hash of the emitted subtree is kept")

	for i := 8; i < 20; i++ {
		tx, hash := streamTx(t, i)
		require.NoError(t, b.AddTx(tx, hash))
	}

	_, _, err = b.Finish()
	require.NoError(t, err)

	require.Nil(t, b.current, "the final subtree must be dropped once Finish emits it, not just the middle ones")
	require.Nil(t, b.currentData, "and its transaction data with it")
	require.Nil(t, b.currentMeta, "and its inpoint metadata with it")
}
