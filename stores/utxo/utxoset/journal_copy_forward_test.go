package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// journalRowsFor counts the journal rows for one output of parent, across every partition.
func journalRowsFor(t *testing.T, s *Store, ctx context.Context, parent *bt.Tx, vout uint32) int {
	t.Helper()

	h := parent.TxIDChainHash()
	k := Pack(h[:], vout)

	var n int
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM spend_journal WHERE ukey = $1 AND txid = $2`, k, h[:]).Scan(&n))

	return n
}

// TestJournalDropCarriesForwardSpendsOfUnminedTransactions is the settled design's pruner step
// 4, never built until now: before a journal partition is dropped, the rows whose spending
// transaction is still waiting to be mined are copied into a live partition.
//
// Those rows are the only record of the coins such a transaction took. If it later loses them
// to a double spend in a block, Unspend rebuilds the coins from exactly these rows, and it fails
// outright without them. The journal was kept for 1440 blocks to put that day off; with the
// rows carried forward it can be as short as reorgs need, because a waiting transaction's
// spends now live as long as it waits.
//
// A mined spender's row is NOT carried: its spend is settled and the partition's drop is the
// end of it.
func TestJournalDropCarriesForwardSpendsOfUnminedTransactions(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 1_000, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1_000, OnLongestChain: true}))
	require.NoError(t, err)

	waiting := spendOneOutput(t, s, ctx, parent, 0, 1_010)
	spendOneOutputInBlock(t, s, ctx, parent, 1, 1_010, 2)

	require.Equal(t, 1, journalRowsFor(t, s, ctx, parent, 0))
	require.Equal(t, 1, journalRowsFor(t, s, ctx, parent, 1))

	// Both spends sit in the partition covering 864-1151. Retire everything below 1440.
	require.NoError(t, s.SetBlockHeight(1_728))

	dropped, err := s.dropSpendJournalPartitionsBelow(ctx, 1_440)
	require.NoError(t, err)
	require.Positive(t, dropped)

	require.Equal(t, 1, journalRowsFor(t, s, ctx, parent, 0), "the waiting transaction's spend is carried forward, once")
	require.Equal(t, 0, journalRowsFor(t, s, ctx, parent, 1), "the mined transaction's spend goes with its partition")

	// The point of it: the waiting transaction can still give its coin back.
	spends, err := utxo.GetSpends(waiting)
	require.NoError(t, err)
	require.NoError(t, s.Unspend(ctx, spends))
	require.Equal(t, 1, utxoCount(t, s, ctx, parent), "the carried row rebuilds the coin")
}

// TestJournalKeepsTheSpendsOfARecentlyMinedTransaction is the review's reproduction: spend at
// 1151, mine at 1439, prune at 1440. The spend sits in the partition covering 864-1151, which a
// pruner session at 1440 drops (1440 less 288 retention is 1152). The spender is no longer
// waiting, so the waiting set's copy skips it, but its block is one deep, and a shallow reorg
// that takes the block back and then gives the spender's coin back needs that row. Undo history
// has to be kept by the height the spender was mined at, not the height it spent at.
func TestJournalKeepsTheSpendsOfARecentlyMinedTransaction(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 1_000, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1_000, OnLongestChain: true}))
	require.NoError(t, err)

	spender := spendOneOutput(t, s, ctx, parent, 0, 1_151)

	mined := utxo.MinedBlockInfo{BlockID: 2, BlockHeight: 1_439, OnLongestChain: true}
	_, err = s.SetMinedMulti(ctx, hashes(spender), mined)
	require.NoError(t, err)

	require.NoError(t, s.SetBlockHeight(1_440))

	svc, err := s.GetPrunerService()
	require.NoError(t, err)

	_, err = svc.Prune(ctx, 1_440, "deadbeef")
	require.NoError(t, err)

	require.Equal(t, 1, journalRowsFor(t, s, ctx, parent, 0), "the spend of a transaction mined one block ago was dropped")

	// The shallow reorg: block 2 is taken back, and the spender then gives its coin back.
	mined.UnsetMined = true
	_, err = s.SetMinedMulti(ctx, hashes(spender), mined)
	require.NoError(t, err)

	spends, err := utxo.GetSpends(spender)
	require.NoError(t, err)
	require.NoError(t, s.Unspend(ctx, spends))
	require.Equal(t, 2, utxoCount(t, s, ctx, parent), "the reorg could not restore the parent's output")
}

// TestJournalCopyForwardIsIdempotent pins the crash window between the carry and the drop. The
// carry commits on its own, so a restart there carries the same partition again on the next
// pass, and that must not leave two rows: every journal reader answers once per row.
func TestJournalCopyForwardIsIdempotent(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 1_000, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1_000, OnLongestChain: true}))
	require.NoError(t, err)

	spendOneOutput(t, s, ctx, parent, 0, 1_010)

	require.NoError(t, s.SetBlockHeight(1_728))

	leaf := uint32(1_010 / SpendJournalPartitionBlocks)

	for i := 0; i < 2; i++ {
		_, err = s.copyForwardUnminedSpends(ctx, leaf, 1_440)
		require.NoError(t, err)
	}

	require.Equal(t, 1, journalRowsFor(t, s, ctx, parent, 0), "exactly one carried row, the original moved")
}

// reinstateOriginal puts back, in the retiring partition at spentHeight, the original of every
// carried journal row for parent's output vout. That is the state a build before the carry
// became a move left between its copy and its drop, and a database upgraded inside that window
// still holds it.
func reinstateOriginal(t *testing.T, s *Store, ctx context.Context, parent *bt.Tx, vout, spentHeight uint32) {
	t.Helper()

	h := parent.TxIDChainHash()
	k := Pack(h[:], vout)

	_, err := s.pool.Exec(ctx, `
INSERT INTO spend_journal (spent_height, spending_vin, satoshis, created_height, spendable_from, flags,
                           mined_height, block_id, ukey, txid, spending_txid, script, hash_override)
SELECT $3::int, spending_vin, satoshis, created_height, spendable_from, flags,
       mined_height, block_id, ukey, txid, spending_txid, script, hash_override
  FROM spend_journal WHERE ukey = $1 AND txid = $2`, k, h[:], int32(spentHeight)) //nolint:gosec // test height
	require.NoError(t, err)
}

// TestJournalCopyForwardHealsADuplicateLeftByACopy covers a journal that a build before the move
// left with both rows for one spend, the original in the retiring partition and the carried copy
// in a live one. The next pass over the leaf deletes the original and inserts nothing, so the
// one row left is the carried one, at the height it was carried to.
func TestJournalCopyForwardHealsADuplicateLeftByACopy(t *testing.T) {
	s, ctx := newTestStore(t)
	parent, waiting := carriedWindow(t, s, ctx)

	reinstateOriginal(t, s, ctx, parent, 0, 1_010)
	require.Equal(t, 2, journalRowsFor(t, s, ctx, parent, 0), "the pre-move state: original and copy")

	leaf := uint32(1_010 / SpendJournalPartitionBlocks)
	copied, err := s.copyForwardUnminedSpends(ctx, leaf, 1_440)
	require.NoError(t, err)
	require.Zero(t, copied, "the carried row already exists, so nothing new is inserted")

	h := parent.TxIDChainHash()

	var spentHeight int32
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT spent_height FROM spend_journal WHERE ukey = $1 AND txid = $2`,
		Pack(h[:], 0), h[:]).Scan(&spentHeight))
	require.Equal(t, int32(1_728), spentHeight, "the carried row is the one kept")

	spends, err := s.SpendsMadeBy(ctx, *waiting.TxIDChainHash())
	require.NoError(t, err)
	require.NoError(t, s.Unspend(ctx, spends, false))
	requireRestoredOnce(t, s, ctx, parent)
}

// TestUnspendBetweenCopyForwardAndDropRestoresOneUTXO pins Unspend's own defence against two
// journal rows for one spend. The carry is a move now and no longer leaves two, but a database
// upgraded from a build whose carry was a copy can still hold the original and its copy, both
// attached and both matching the restore's ownership predicates, until the next pruner pass
// heals it. An Unspend landing there consumes both, and both reach the restore.
//
// The restore's NOT EXISTS guard cannot tell them apart: every CTE in the statement reads the
// same pre-statement snapshot, in which the UTXO is absent for both rows. So it must collapse
// them itself. The end state is exactly one live UTXO for the outpoint, no journal row left for
// a later replay to find, and a clean return.
func TestUnspendBetweenCopyForwardAndDropRestoresOneUTXO(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 1_000, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1_000, OnLongestChain: true}))
	require.NoError(t, err)

	waiting := spendOneOutput(t, s, ctx, parent, 0, 1_010)

	require.NoError(t, s.SetBlockHeight(1_728))

	// The pruner's first step for the retiring leaf, and nothing after it, with the original put
	// back where a copy would have left it.
	leaf := uint32(1_010 / SpendJournalPartitionBlocks)
	copied, err := s.copyForwardUnminedSpends(ctx, leaf, 1_440)
	require.NoError(t, err)
	require.Equal(t, int64(1), copied)
	reinstateOriginal(t, s, ctx, parent, 0, 1_010)
	require.Equal(t, 2, journalRowsFor(t, s, ctx, parent, 0), "the original and its carried copy")

	spends, err := utxo.GetSpends(waiting)
	require.NoError(t, err)
	require.NoError(t, s.Unspend(ctx, spends), "one spend, one restore")

	h := parent.TxIDChainHash()
	k := Pack(h[:], 0)

	var live int
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM utxo WHERE leaf = $1 AND ukey = $2 AND txid = $3`,
		LeafFor(h[:]), k, h[:]).Scan(&live))
	require.Equal(t, 1, live, "exactly one live UTXO for the outpoint, never two")
	require.Equal(t, 0, journalRowsFor(t, s, ctx, parent, 0), "both journal rows consumed")
	require.Equal(t, 2, utxoCount(t, s, ctx, parent), "output 0 restored once, output 1 untouched")

	// Restored as the copy described it: still mined in block 1.
	var minedHeight, blockID int32
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT mined_height, block_id FROM utxo WHERE leaf = $1 AND ukey = $2 AND txid = $3`,
		LeafFor(h[:]), k, h[:]).Scan(&minedHeight, &blockID))
	require.Equal(t, int32(1_000), minedHeight)
	require.Equal(t, int32(1), blockID)
}

// carriedWindow sets up the interval between the copy-forward and the drop: a waiting
// transaction's spend at 1,010, the store at 1,728, and the pruner's carry for the retiring leaf
// run on its own, so the leaf is still attached. It returns the parent and the waiting spender.
func carriedWindow(t *testing.T, s *Store, ctx context.Context) (*bt.Tx, *bt.Tx) {
	t.Helper()

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 1_000, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1_000, OnLongestChain: true}))
	require.NoError(t, err)

	waiting := spendOneOutput(t, s, ctx, parent, 0, 1_010)

	require.NoError(t, s.SetBlockHeight(1_728))

	leaf := uint32(1_010 / SpendJournalPartitionBlocks)
	copied, err := s.copyForwardUnminedSpends(ctx, leaf, 1_440)
	require.NoError(t, err)
	require.Equal(t, int64(1), copied)

	return parent, waiting
}

// requireRestoredOnce asserts the end state of a reversal: one live UTXO at parent's output 0,
// output 1 untouched, and no journal row left for output 0 that a later read or reversal could
// act on.
func requireRestoredOnce(t *testing.T, s *Store, ctx context.Context, parent *bt.Tx) {
	t.Helper()

	h := parent.TxIDChainHash()
	k := Pack(h[:], 0)

	var live int
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM utxo WHERE leaf = $1 AND ukey = $2 AND txid = $3`,
		LeafFor(h[:]), k, h[:]).Scan(&live))
	require.Equal(t, 1, live, "exactly one live UTXO for the outpoint")
	require.Equal(t, 2, utxoCount(t, s, ctx, parent), "output 0 restored once, output 1 untouched")
	require.Equal(t, 0, journalRowsFor(t, s, ctx, parent, 0), "no journal row left behind")
}

// TestConflictReversalBetweenCopyForwardAndDrop is the review's reproduction (P2, duplicate
// journal copies break conflict reversal). Conflict reversal does not hand Unspend the spend it
// was given; it asks the store which coins the transaction took, and both readers it asks,
// SpendsMadeBy (ReverseProcessConflicting step 3) and SetConflicting (ProcessConflicting steps 1
// and 2), answer one record per journal row. While the carry and the drop were two steps, a
// carried spend had two rows, so each reader named the one input twice. Unspend restored it once
// and then failed its accounting check, "restored 1 ... of 2 requested", which aborts the
// reorganisation after the restore has committed.
//
// The carry now moves the row, so the readers never see two. The test asserts that directly,
// one row between the carry and the drop, and then that both reversals return cleanly.
func TestConflictReversalBetweenCopyForwardAndDrop(t *testing.T) {
	t.Run("SpendsMadeBy then Unspend", func(t *testing.T) {
		s, ctx := newTestStore(t)
		parent, waiting := carriedWindow(t, s, ctx)

		require.Equal(t, 1, journalRowsFor(t, s, ctx, parent, 0), "the carry left two rows for one spend")

		spends, err := s.SpendsMadeBy(ctx, *waiting.TxIDChainHash())
		require.NoError(t, err)
		require.Len(t, spends, 1, "one input, one record")

		require.NoError(t, s.Unspend(ctx, spends, false))
		requireRestoredOnce(t, s, ctx, parent)
	})

	t.Run("SetConflicting then Unspend", func(t *testing.T) {
		s, ctx := newTestStore(t)
		parent, waiting := carriedWindow(t, s, ctx)

		require.Equal(t, 1, journalRowsFor(t, s, ctx, parent, 0), "the carry left two rows for one spend")

		affected, _, err := s.SetConflicting(ctx, []chainhash.Hash{*waiting.TxIDChainHash()}, true)
		require.NoError(t, err)
		require.Len(t, affected, 1, "one input, one record")

		require.NoError(t, s.Unspend(ctx, affected, true))
		requireRestoredOnce(t, s, ctx, parent)
	})
}

// TestJournalCopyForwardReadsItsSpendersOncePerPass is icellan's review point: a pass that
// retires several journal partitions read every waiting and recently mined transaction again
// for each one. On a backlog that is many reads of the same set. The pass now reads the set once
// and carries from each partition with it. Both waiting spends, one in each retiring partition,
// must still be carried.
func TestJournalCopyForwardReadsItsSpendersOncePerPass(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 1_000, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1_000, OnLongestChain: true}))
	require.NoError(t, err)

	spendOneOutput(t, s, ctx, parent, 0, 1_010) // partition 3, heights 864-1151
	spendOneOutput(t, s, ctx, parent, 1, 1_200) // partition 4, heights 1152-1439

	require.NoError(t, s.SetBlockHeight(1_440))

	reads := 0
	copyForwardCollected = func() { reads++ }

	t.Cleanup(func() { copyForwardCollected = nil })

	_, err = s.dropSpendJournalPartitionsBelow(ctx, 1_440)
	require.NoError(t, err)

	require.Equal(t, 1, reads, "the pass read the spenders again for each retiring partition")
	require.Equal(t, 1, journalRowsFor(t, s, ctx, parent, 0), "the spend in partition 3 was carried")
	require.Equal(t, 1, journalRowsFor(t, s, ctx, parent, 1), "the spend in partition 4 was carried")
}

// copyForwardUnminedSpends is collectCopyForward then copyForwardFrom, for a test that wants one
// pass. It carries forward, from the journal partition for leaf, every spend
// whose spending transaction is still waiting to be mined, and returns how many rows it copied.
// keepFrom is the lowest height the pass keeps; copies are stamped at the store's height, or
// keepFrom if the store's height is below it, so they never land in a partition the same pass
// drops.
//
// This is the settled design's pruner step 4 (2026-08-26-utxoset-settled-design.md, "Copy
// forward any journal row whose spending transaction is still unmined"). A waiting
// transaction's journal rows are the only record of the coins it took, and Unspend rebuilds
// them from those rows when the transaction loses a double spend to a block. Carried forward,
// they live as long as the transaction waits, which is what lets journal retention be as short
// as reorgs need rather than 1440 blocks.
//
// Recognising a resubmitted transaction does not need these rows: the validator answers that
// from the transaction's own txid (Validator.knownTxResubmission).
//
// The spends of a transaction mined at or above keepFrom are carried too (see
// recentlyMinedInpointsSQL). Retention is kept by the height the spender was mined at, not the
// height it spent at: a spender that waited across the partition's top and was then mined has
// its block inside the journal's retention, and a shallow reorg that takes the block back and
// then gives its coins back needs these rows exactly as a waiting spender's loss does. The copy
// lands at the store's height, so it is kept for another retention's worth of blocks and then
// goes, once its block is below keepFrom. Below the highest checkpoint no block can be un-mined,
// so nothing mined there is carried.
func (s *Store) copyForwardUnminedSpends(ctx context.Context, leaf, keepFrom uint32) (int64, error) {
	c, err := s.collectCopyForward(ctx, keepFrom)
	if err != nil {
		return 0, err
	}

	return s.copyForwardFrom(ctx, leaf, keepFrom, c)
}
