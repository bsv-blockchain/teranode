package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

// TestGetServesAFullySpentOldParentFromTheJournal is the case block validation asks about on
// most blocks above the highest checkpoint, and the one no other step can answer.
//
// The parent was mined longer ago than the membership retention, so its window is gone. Its
// last output has been spent, so there is no coin. Nothing preserved it, because preservation
// names parents of children that have been UNMINED for 144 blocks, and this child is mined in
// the next block. Identity, membership, preservation and coin all miss. The journal row from
// the spend is the only record left, and it carries the block facts copied off the coin the
// delete destroyed.
func TestGetServesAFullySpentOldParentFromTheJournal(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	// The membership window retires while the coin is still live, so the coin -- and only the
	// coin -- carries the block facts by the time the spend happens.
	_, err = s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)
	require.Equal(t, 0, minedRows(t, s, ctx, parent))

	spendOneOutput(t, s, ctx, parent, 0, 2_000)
	require.Equal(t, 0, coinCount(t, s, ctx, parent))

	got, err := s.Get(ctx, parent.TxIDChainHash(), fields.BlockIDs, fields.BlockHeights)
	require.NoError(t, err)
	require.Equal(t, []uint32{7}, got.BlockIDs)
	require.Equal(t, []uint32{100}, got.BlockHeights)
	require.Equal(t, []int{0}, got.SubtreeIdxs)
}

// TestUnspendRestoresBlockFactsFromTheJournalCopy: with the membership window gone, tx_mined
// has nothing to re-resolve the restored coin's block from. The journal's copy is the
// fallback, and it is preferred to the unconfirmed sentinel.
func TestUnspendRestoresBlockFactsFromTheJournalCopy(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	_, err = s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)

	child := spendOneOutput(t, s, ctx, parent, 0, 2_000)
	require.Equal(t, 0, coinCount(t, s, ctx, parent))

	require.NoError(t, s.Unspend(ctx, []*utxo.Spend{{
		TxID:         parent.TxIDChainHash(),
		Vout:         0,
		SpendingData: spend.NewSpendingData(child.TxIDChainHash(), 0),
	}}))

	require.Equal(t, 1, coinCount(t, s, ctx, parent))

	h, b := coinFactsOf(t, s, ctx, hashBytes(parent))
	require.Equal(t, int32(100), h, "restored from the journal copy, not the sentinel")
	require.Equal(t, int32(7), b)
}

// TestTheJournalStepIsNotConsultedForAMempoolParent: the identity row answers at step 1, and
// the journal copy for a mempool spend is the unconfirmed sentinel anyway, which the journal
// step filters out. A mempool parent whose coin is spent must not start claiming a block.
func TestTheJournalStepIsNotConsultedForAMempoolParent(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	spendOneOutput(t, s, ctx, parent, 0, 100)
	require.Equal(t, 0, coinCount(t, s, ctx, parent))

	got, err := s.Get(ctx, parent.TxIDChainHash(), fields.BlockIDs, fields.BlockHeights)
	require.NoError(t, err)
	require.True(t, identExists(t, s, ctx, parent), "the identity row is what answered")
	require.Empty(t, got.BlockIDs, "a mempool transaction claims no block")
	require.Empty(t, got.BlockHeights)
}

// TestGetStillReportsNotFoundOnceTheJournalLeafIsGoneToo is the other half of
// TestGetServesAFullySpentOldParentFromTheJournal: the journal step buys the parent exactly
// the journal's retention and not a block more. Past both retentions there is genuinely
// nothing left, and the store says so.
func TestGetStillReportsNotFoundOnceTheJournalLeafIsGoneToo(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	spendOneOutput(t, s, ctx, parent, 0, 100)

	_, err = s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)

	_, err = s.dropSpendJournalPartitionsBelow(ctx, 2_000)
	require.NoError(t, err)

	_, err = s.Get(ctx, parent.TxIDChainHash())
	require.True(t, errors.Is(err, errors.ErrTxNotFound))
}
