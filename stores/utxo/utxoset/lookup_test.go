package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// TestGetServesAMinedTransactionFromTheMembershipTable: no identity row exists, so the
// block ids, heights and subtree index come from tx_mined, and the body from its window.
func TestGetServesAMinedTransactionFromTheMembershipTable(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, SubtreeIdx: 3, OnLongestChain: true}))
	require.NoError(t, err)

	got, err := s.Get(ctx, tx.TxIDChainHash(), fields.BlockIDs, fields.BlockHeights)
	require.NoError(t, err)
	require.Equal(t, []uint32{42}, got.BlockIDs)
	require.Equal(t, []uint32{700_100}, got.BlockHeights)
	require.Equal(t, []int{3}, got.SubtreeIdxs)
	require.NotNil(t, got.Tx, "the body is inside its window")
	require.Equal(t, uint32(0), got.UnminedSince)
}

// TestGetServesAnOldParentFromItsCoinOnceTheWindowIsGone: the membership window was dropped,
// the transaction still has a live coin, and the coin's block facts are the answer. Fee,
// size, inputs and subtree index are zero, which is what a pruned SV Node can say too.
func TestGetServesAnOldParentFromItsCoinOnceTheWindowIsGone(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	_, err = s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)

	got, err := s.Get(ctx, tx.TxIDChainHash(), fields.BlockIDs, fields.BlockHeights)
	require.NoError(t, err)
	require.Equal(t, []uint32{7}, got.BlockIDs)
	require.Equal(t, []uint32{100}, got.BlockHeights)
	require.Equal(t, []int{0}, got.SubtreeIdxs)
	require.Equal(t, uint64(0), got.Fee)
	require.Nil(t, got.TxInpoints.ParentTxHashes)
}

// TestGetNeverAnswersBlockIdsFromTheCoinWhileAMembershipRowExists pins the read order. A
// coin holds one block id; a transaction stamped into two blocks must report both while
// the window lives, which only the membership table can do.
func TestGetNeverAnswersBlockIdsFromTheCoinWhileAMembershipRowExists(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	// A second block at the same height stamps it.
	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 43, BlockHeight: 700_100})
	require.NoError(t, err)

	got, err := s.Get(ctx, tx.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err)
	require.Equal(t, []uint32{42, 43}, got.BlockIDs, "both blocks, in insertion order")
}

// TestGetServesAFullySpentTransactionPastItsWindowWhileItsJournalLeafLives is the case block
// validation asks about on most blocks above the highest checkpoint.
//
// This test used to assert ErrTxNotFound for exactly this state, and that was wrong by design:
// membership retires 1440 blocks after the parent was MINED and the journal 1440 blocks after
// the coin was SPENT, so the two are counted from different clocks and a parent can lose its
// window while its journal row still stands. During that window the store CAN answer, and it
// must: the alternative is a BlockIncompleteError the caller retries forever. Both the base
// branch and aerospike keep a fully-spent parent answerable for a window after the spend.
//
// TestGetStillReportsNotFoundOnceTheJournalLeafIsGoneToo in spent_parent_test.go is the other
// half: past both retentions the transaction really is gone, which is aerospike's behaviour
// after its delete-at-height and what the shared suite's pruning test requires.
func TestGetServesAFullySpentTransactionPastItsWindowWhileItsJournalLeafLives(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	spendOneOutput(t, s, ctx, parent, 0, 100)

	_, err = s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)

	got, err := s.Get(ctx, parent.TxIDChainHash(), fields.BlockIDs, fields.BlockHeights)
	require.NoError(t, err)
	require.Equal(t, []uint32{7}, got.BlockIDs)
	require.Equal(t, []uint32{100}, got.BlockHeights)

	// And once the journal leaf goes too, there is genuinely nothing left.
	_, err = s.dropSpendJournalPartitionsBelow(ctx, 2_000)
	require.NoError(t, err)

	_, err = s.Get(ctx, parent.TxIDChainHash())
	require.True(t, errors.Is(err, errors.ErrTxNotFound))
}

// TestBatchDecorateFollowsTheSameOrder: one mempool row, one membership row, one coin-only
// parent and one unknown, in a single call.
func TestBatchDecorateFollowsTheSameOrder(t *testing.T) {
	s, ctx := newTestStore(t)

	mempool := mkTx(t, 1, 5_001)
	_, err := s.Create(ctx, mempool, 700_200)
	require.NoError(t, err)

	mined := mkTx(t, 1, 5_002)
	_, err = s.Create(ctx, mined, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	old := mkTx(t, 1, 5_003)
	_, err = s.Create(ctx, old, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)
	_, err = s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)

	unknown := mkTx(t, 1, 5_004)

	items := []*utxo.UnresolvedMetaData{
		{Hash: *mempool.TxIDChainHash()}, {Hash: *mined.TxIDChainHash()},
		{Hash: *old.TxIDChainHash()}, {Hash: *unknown.TxIDChainHash()},
	}
	require.NoError(t, s.BatchDecorate(ctx, items, fields.BlockIDs))

	require.NoError(t, items[0].Err)
	require.Empty(t, items[0].Data.BlockIDs)
	require.NoError(t, items[1].Err)
	require.Equal(t, []uint32{42}, items[1].Data.BlockIDs)
	require.NoError(t, items[2].Err)
	require.Equal(t, []uint32{7}, items[2].Data.BlockIDs)
	require.True(t, errors.Is(items[3].Err, errors.ErrTxNotFound))
}

// TestBlockIdRecoveryReadsTheMembershipRow pins what quick validation and legacy sync do on
// a retry: Get the first non-coinbase transaction with fields.BlockIDs and reuse BlockIDs[0].
// That transaction has no identity row in this design; the answer comes from tx_mined.
// services/blockvalidation/quick_validate.go:429-444, services/legacy/netsync/handle_block.go:1236-1262.
func TestBlockIdRecoveryReadsTheMembershipRow(t *testing.T) {
	s, ctx := newTestStore(t)

	first := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, first, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	got, err := s.Get(ctx, first.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err)
	require.Len(t, got.BlockIDs, 1)
	require.Equal(t, uint32(42), got.BlockIDs[0])
}

// TestBatchDecorateResolvesEveryLeafOfAMultiLeafBatch pins what the leaf-scalar identity probe
// changed: the statement now runs once per leaf group instead of once for the batch, so a
// grouping that dropped a leaf, or a group whose txids were sent under the wrong leaf, would
// answer for some transactions and report the rest as missing.
//
// The batch is deliberately large enough to cover every leaf. LeafFor is the first byte of the
// txid masked to NumLeaves, so a handful of random transactions is not enough to guarantee it;
// the loop keeps creating until every leaf has at least one and then checks the count.
func TestBatchDecorateResolvesEveryLeafOfAMultiLeafBatch(t *testing.T) {
	s, ctx := newTestStore(t)

	seen := map[int16]int{}
	items := make([]*utxo.UnresolvedMetaData, 0, 64)

	for sats := uint64(5_000); len(seen) < NumLeaves && sats < 5_200; sats++ {
		tx := mkTx(t, 1, sats)
		_, err := s.Create(ctx, tx, 700_100)
		require.NoError(t, err)

		seen[LeafFor(hashBytes(tx))]++

		items = append(items, &utxo.UnresolvedMetaData{Hash: *tx.TxIDChainHash()})
	}

	require.Len(t, seen, NumLeaves, "the batch has to span every leaf for this to prove anything")

	require.NoError(t, s.BatchDecorate(ctx, items, fields.BlockIDs))

	for _, it := range items {
		require.NoError(t, it.Err, "%s", it.Hash.String())
		require.NotNil(t, it.Data)
	}
}

// TestSetMinedMultiAnswersForEveryLeafOfAMultiLeafBatch is the same proof on the stamp path,
// where provePresentSQL is what has to name every transaction the caller asked about. The
// interface says every hash appears in the answer or the call fails, and a leaf grouping that
// lost a group would fail the whole batch rather than answer wrongly -- which is the right
// direction, and still worth pinning.
func TestSetMinedMultiAnswersForEveryLeafOfAMultiLeafBatch(t *testing.T) {
	s, ctx := newTestStore(t)

	seen := map[int16]int{}
	hashList := make([]*chainhash.Hash, 0, 64)

	for sats := uint64(6_000); len(seen) < NumLeaves && sats < 6_200; sats++ {
		tx := mkTx(t, 1, sats)
		_, err := s.Create(ctx, tx, 700_100)
		require.NoError(t, err)

		seen[LeafFor(hashBytes(tx))]++

		hashList = append(hashList, tx.TxIDChainHash())
	}

	require.Len(t, seen, NumLeaves)

	// A fork stamp, so the rows stay in the identity table and provePresentSQL is what answers.
	got, err := s.SetMinedMulti(ctx, hashList, utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100})
	require.NoError(t, err)
	require.Len(t, got, len(hashList))

	for _, h := range hashList {
		require.Equal(t, []uint32{42}, got[*h], "%s", h.String())
	}
}
