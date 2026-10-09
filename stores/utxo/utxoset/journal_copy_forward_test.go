package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
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

// TestJournalCopyForwardIsIdempotent pins the crash window between the copy and the drop. The
// copy commits on its own, so a restart there copies the same partition again on the next
// pass, and that must not leave two rows: Unspend consumes one row per restore.
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

	require.Equal(t, 2, journalRowsFor(t, s, ctx, parent, 0), "the original and exactly one carried copy")
}

// TestUnspendBetweenCopyForwardAndDropRestoresOneUTXO pins the window between the copy-forward
// and the drop of the retiring partition. The copy commits on its own and the partition is
// detached after it, so for that interval a waiting transaction's spend has two journal rows,
// the original and its carried copy, both attached and both matching the restore's ownership
// predicates. An Unspend landing there consumes both, and both reach the restore.
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

	// The pruner's first step for the retiring leaf, and nothing after it: the copy has
	// committed and the original partition is still attached.
	leaf := uint32(1_010 / SpendJournalPartitionBlocks)
	copied, err := s.copyForwardUnminedSpends(ctx, leaf, 1_440)
	require.NoError(t, err)
	require.Equal(t, int64(1), copied)
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
