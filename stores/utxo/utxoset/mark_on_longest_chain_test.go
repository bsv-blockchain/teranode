package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestMarkOnLongestChainClearsTheMempoolMarker is the repair block assembly runs at startup.
//
// It finds transactions that carry main-chain block membership while still marked as waiting
// to be mined, and this call is how it fixes them. Until it existed the node could not start
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

// TestMarkOffLongestChainIsAnUnMine: the mark call with false carries no block, so every
// membership row of the transaction comes back as a fork triple and the marker is set.
func TestMarkOffLongestChainIsAnUnMine(t *testing.T) {
	s, ctx := newTestStore(t)
	require.NoError(t, s.SetBlockHeight(700_150))

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_099)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true})
	require.NoError(t, err)

	require.NoError(t, s.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, false))

	require.True(t, identExists(t, s, ctx, tx))
	require.Equal(t, 0, minedRows(t, s, ctx, tx))

	got, err := s.Get(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, []uint32{42}, got.BlockIDs, "the block is remembered as a fork triple")
	require.Equal(t, uint32(700_150), got.UnminedSince)
}

// TestMarkOnLongestChainMovesASingleBlockRow: the mark call with true on a row naming one
// block moves it into membership; on a row naming two it only clears the marker.
func TestMarkOnLongestChainMovesASingleBlockRow(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_099)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100})
	require.NoError(t, err)

	require.NoError(t, s.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, true))
	require.False(t, identExists(t, s, ctx, tx))
	require.Equal(t, 1, minedRows(t, s, ctx, tx))

	// A row naming two blocks stays: the call carries no block id, so it cannot say which of
	// them is main.
	two := mkTx(t, 1, 6_000)
	_, err = s.Create(ctx, two, 700_099)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(two), utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100})
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(two), utxo.MinedBlockInfo{BlockID: 44, BlockHeight: 700_101})
	require.NoError(t, err)

	require.NoError(t, s.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*two.TxIDChainHash()}, true))
	require.True(t, identExists(t, s, ctx, two))
	require.Equal(t, 0, minedRows(t, s, ctx, two))
	require.Nil(t, readIdent(t, s, ctx, two.TxIDChainHash()[:]).offChainSince)
}

// TestMarkOnLongestChainSkipsADroppedWindow. A single-block identity row can name a height
// whose membership window has already been dropped -- roughly 300 blocks of fork residue is
// exactly what block assembly's startup reload hands this call -- and the window cannot be
// recreated, because the floor exists to stop a retired window claiming its transactions
// afresh.
//
// The repair must still happen for every hash. Refusing the whole call because one stale fork
// triple cannot be settled would leave the node unable to start, which is the failure this
// method was written to fix in the first place. So a row naming a dropped window has its
// marker cleared and stays in the mempool table.
func TestMarkOnLongestChainSkipsADroppedWindow(t *testing.T) {
	s, ctx := newTestStore(t)

	// A block-path create is what puts window 0 on the table, so that dropping it moves the
	// floor past the height the fork triple below names.
	filler := mkTx(t, 1, 1_111)
	_, err := s.Create(ctx, filler, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 9, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	tx := mkTx(t, 1, 5_000)
	_, err = s.Create(ctx, tx, 100)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 100})
	require.NoError(t, err)

	dropped, err := s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)
	require.Equal(t, 1, dropped, "window 0 has to be gone for this test to mean anything")

	require.NoError(t, s.MarkTransactionsOnLongestChain(ctx,
		[]chainhash.Hash{*tx.TxIDChainHash()}, true))

	require.True(t, identExists(t, s, ctx, tx), "it stays in the mempool table")
	require.Equal(t, 0, minedRows(t, s, ctx, tx))
	require.Nil(t, readIdent(t, s, ctx, tx.TxIDChainHash()[:]).offChainSince,
		"the marker is cleared even though the row could not be settled")
}
