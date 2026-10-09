package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestMarkOnLongestChainClearsTheMempoolMarker is the repair block assembly runs at startup.
//
// It finds transactions that have main-chain containment while still marked as waiting to be
// mined, and this call is how it fixes them. Until it existed the node could not start
// at all once any such transaction was in the store.
func TestMarkOnLongestChainClearsTheMempoolMarker(t *testing.T) {
	s, ctx := newTestStore(t)

	txs := mkStoredTxs(t, s, 700_000, 1_000, 4)

	// Stored but not named, so a statement matching on the partition key alone would be caught.
	bystander := mkStoredTxs(t, s, 700_000, 8_000, 1)[0]

	hs := make([]chainhash.Hash, 0, len(txs))
	for _, h := range txHashes(txs) {
		hs = append(hs, *h)
	}

	require.NoError(t, s.MarkTransactionsOnLongestChain(ctx, hs, true))

	for _, tx := range txs {
		h := tx.TxIDChainHash()
		require.Nil(t, readIdent(t, s, ctx, h[:]).offChainSince,
			"a transaction on the longest chain is no longer waiting to be mined")
	}

	bh := bystander.TxIDChainHash()
	require.NotNil(t, readIdent(t, s, ctx, bh[:]).offChainSince,
		"a transaction the call did not name must keep its marker")
}

// TestMarkOffLongestChainGivesAFreshClock is the reverse, and it pins the same rule the
// un-mine path follows: the clock comes from the CURRENT tip, not from the height the
// transaction was created at.
func TestMarkOffLongestChainGivesAFreshClock(t *testing.T) {
	s, ctx := newTestStore(t)

	txs := mkStoredTxs(t, s, 100, 1_000, 3)

	hs := make([]chainhash.Hash, 0, len(txs))
	for _, h := range txHashes(txs) {
		hs = append(hs, *h)
	}

	require.NoError(t, s.MarkTransactionsOnLongestChain(ctx, hs, true))
	require.NoError(t, s.SetBlockHeight(5_000))
	require.NoError(t, s.MarkTransactionsOnLongestChain(ctx, hs, false))

	for _, tx := range txs {
		h := tx.TxIDChainHash()

		r := readIdent(t, s, ctx, h[:])
		require.NotNil(t, r.offChainSince, "off the longest chain means waiting again")
		require.Equal(t, int32(5_000), *r.offChainSince,
			"with a clock from the current tip, not from created_height")
	}
}

// TestMarkOnLongestChainReportsATransactionItDoesNotHold. The sql store returns an error
// rather than a silent no-op, because the caller is repairing an inconsistency and a
// silently skipped row leaves the inconsistency in place.
func TestMarkOnLongestChainReportsATransactionItDoesNotHold(t *testing.T) {
	s, ctx := newTestStore(t)

	known := mkStoredTxs(t, s, 700_000, 1_000, 1)[0]
	missing := mkTx(t, 1, 9_999)

	err := s.MarkTransactionsOnLongestChain(ctx,
		[]chainhash.Hash{*known.TxIDChainHash(), *missing.TxIDChainHash()}, true)

	require.Error(t, err, "a hash the store does not hold must fail loudly")
	require.Contains(t, err.Error(), missing.TxIDChainHash().String())

	// The transactions it DOES hold are still repaired, so a partial input does not
	// strand the rest.
	kh := known.TxIDChainHash()
	require.Nil(t, readIdent(t, s, ctx, kh[:]).offChainSince)
}

// TestMarkOnLongestChainOnAnEmptyListIsANoOp, which is how block assembly calls it on the
// ordinary path where nothing needs repairing.
func TestMarkOnLongestChainOnAnEmptyListIsANoOp(t *testing.T) {
	s, ctx := newTestStore(t)

	require.NoError(t, s.MarkTransactionsOnLongestChain(ctx, nil, true))
	require.NoError(t, errors.Join())
}

// TestMarkOffLongestChainSetsTheMarkerAndLeavesContainmentAlone: the mark call with false
// carries no block, so it cannot say which containment row to doubt, and it does not try. It
// writes the marker and nothing else; the rows recording which blocks contain the transaction
// stay, because a chain switch cannot make "block 42 contains this transaction" false.
func TestMarkOffLongestChainSetsTheMarkerAndLeavesContainmentAlone(t *testing.T) {
	s, ctx := newTestStore(t)
	require.NoError(t, s.SetBlockHeight(700_150))

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_099)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true})
	require.NoError(t, err)

	require.NoError(t, s.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, false))

	require.True(t, identExists(t, s, ctx, tx))
	require.Equal(t, 1, minedRows(t, s, ctx, tx), "containment is not a mark-off's business")

	got, err := s.Get(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, []uint32{42}, got.BlockIDs, "the block that contains it is still reported")
	require.Equal(t, uint32(700_150), got.UnminedSince)

	h, b := utxoFacts(t, s, ctx, tx)
	require.Equal(t, int32(0), h, "and no UTXO is touched")
	require.Equal(t, int32(0), b)
}

// TestMarkOnLongestChainOnlyClearsTheMarker: the mark call with true writes one nullable
// integer, whether the transaction is in one block or two. Nothing moves between tables any
// more, so there is no single-block special case.
func TestMarkOnLongestChainOnlyClearsTheMarker(t *testing.T) {
	s, ctx := newTestStore(t)

	one := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, one, 700_099)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(one), utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100})
	require.NoError(t, err)

	require.NoError(t, s.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*one.TxIDChainHash()}, true))
	require.True(t, identExists(t, s, ctx, one), "the identity row stays until the stamp")
	require.Nil(t, markerOf(t, s, ctx, one))
	require.Equal(t, 1, minedRows(t, s, ctx, one))

	two := mkTx(t, 1, 6_000)
	_, err = s.Create(ctx, two, 700_099)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(two), utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100})
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(two), utxo.MinedBlockInfo{BlockID: 44, BlockHeight: 700_101})
	require.NoError(t, err)

	require.NoError(t, s.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*two.TxIDChainHash()}, true))
	require.True(t, identExists(t, s, ctx, two))
	require.Nil(t, markerOf(t, s, ctx, two))
	require.Equal(t, 2, minedRows(t, s, ctx, two), "both rows stay; the call does not choose between them")
}

// TestMarkOnLongestChainCountsAContainedTransactionWithNoIdentityRowAsReached. A block-path
// transaction has containment and no identity row, and it is already in the state mark-on asks
// for, so the call must count it as reached rather than report it missing. Without this block
// assembly's startup repair could not be re-run after an interrupted one.
func TestMarkOnLongestChainCountsAContainedTransactionWithNoIdentityRowAsReached(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)
	require.False(t, identExists(t, s, ctx, tx))

	require.NoError(t, s.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, true))
	require.NoError(t, s.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, true),
		"and again, because the repair is re-run after an interrupted start")
}

// TestMarkOffLongestChainReportsAndCountsAContainedTransactionWithNoIdentityRow. A transaction
// with containment and no identity row was created through the block path at or below the
// checkpoint, is a coinbase, was seeded or has been stamped, and no reorg the node admits should
// mark any of them off. Mark-off has no row to write the marker on, so it reports the hash as
// not found, as it always has, and every non-coinbase one moves the guard counter, which the
// soak requires to stay at zero.
func TestMarkOffLongestChainReportsAndCountsAContainedTransactionWithNoIdentityRow(t *testing.T) {
	s, ctx := newTestStore(t)
	require.NoError(t, s.SetBlockHeight(700_150))

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	// A real coinbase: the store reads the coinbase bit off the transaction itself.
	coinbase := mkCoinbase(t, 6_000)
	_, err = s.Create(ctx, coinbase, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	before := testutil.ToFloat64(noIdentityReached.WithLabelValues("mark_off"))

	err = s.MarkTransactionsOnLongestChain(ctx,
		[]chainhash.Hash{*tx.TxIDChainHash(), *coinbase.TxIDChainHash()}, false)
	require.True(t, errors.Is(err, errors.ErrTxNotFound), "no identity row to mark: %v", err)

	require.Equal(t, before+1, testutil.ToFloat64(noIdentityReached.WithLabelValues("mark_off")),
		"the ordinary transaction is counted, the coinbase is not")
	require.Equal(t, 1, minedRows(t, s, ctx, tx), "and containment is untouched either way")
}
