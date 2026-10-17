package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// repeatAcrossPartitionThenReorg finishes the review's reproduction once a crashed multi write
// has left child's spends journaled at 1151, the top of the partition covering 864-1151, with
// child itself not written. The validator then accepts child at 1400, in the next partition, a
// block mines it at 1439, and a pruner session at 1440 drops the partition covering 864-1151
// (1440 less 288 retention is 1152). The block is one deep, so a shallow reorg can take it back,
// and child must then be able to give its coins back.
func repeatAcrossPartitionThenReorg(t *testing.T, s *Store, ctx context.Context, child, parent *bt.Tx,
	wantUTXOs int) {
	t.Helper()

	_, _, err := s.SpendAndCreate(ctx, child, 1_400, utxo.WithIgnoreLocked(true))
	require.NoError(t, err, "the earlier spend is accepted as the same spend")

	mined := utxo.MinedBlockInfo{BlockID: 2, BlockHeight: 1_439, OnLongestChain: true}
	_, err = s.SetMinedMulti(ctx, hashes(child), mined)
	require.NoError(t, err)

	require.NoError(t, s.SetBlockHeight(1_440))

	svc, err := s.GetPrunerService()
	require.NoError(t, err)

	_, err = svc.Prune(ctx, 1_440, "deadbeef")
	require.NoError(t, err)

	require.Equal(t, 1, journalRowsFor(t, s, ctx, parent, 0),
		"the spend repeated in a later partition was dropped with the earlier one")

	// The shallow reorg: block 2 is taken back, and child then gives its coins back.
	mined.UnsetMined = true
	_, err = s.SetMinedMulti(ctx, hashes(child), mined)
	require.NoError(t, err)

	spends, err := utxo.GetSpends(child)
	require.NoError(t, err)
	require.NoError(t, s.Unspend(ctx, spends))
	require.Equal(t, wantUTXOs, utxoCount(t, s, ctx, parent), "the reorg could not restore the parent's output")
}

// TestJournalMovesASpendRepeatedInALaterPartition is the review's reproduction of a spend and
// its create split across a partition boundary. SpendAndCreateMulti commits its spends before
// its creates, so a crash between the two leaves the spends journaled at the block's height with
// the transaction not written. If the block is never written again, the same transaction can
// arrive later and be accepted, its spends recognised as its own. The copy-forward keeps the
// spends of a recently mined transaction only when its created_height is below the drop line,
// because it assumes a spend is never journaled below its transaction's created_height. The
// repeat breaks that, unless the repeat moves the journal row to its own height.
func TestJournalMovesASpendRepeatedInALaterPartition(t *testing.T) {
	crash := errMultiCrash

	t.Run("a spend of an output outside the list", func(t *testing.T) {
		s, ctx := newUncheckpointedStore(t)

		parent := mkTx(t, 2, 5_000)
		_, err := s.Create(ctx, parent, 1_000, utxo.WithMinedBlockInfo(
			utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1_000, OnLongestChain: true}))
		require.NoError(t, err)

		child := spendOutput(t, parent, 0, 1)

		multiFault = func(at string) error {
			if at == "spent" {
				return crash
			}

			return nil
		}

		_, err = s.SpendAndCreateMulti(ctx, []*bt.Tx{child}, 1_151, utxo.WithIgnoreLocked(true))
		multiFault = nil
		require.ErrorIs(t, err, crash)

		require.Equal(t, 1, journalRowsFor(t, s, ctx, parent, 0), "the crash keeps the spend")
		require.Equal(t, 1, utxoCount(t, s, ctx, parent))

		repeatAcrossPartitionThenReorg(t, s, ctx, child, parent, 2)
	})

	t.Run("a netted spend of a parent in the list", func(t *testing.T) {
		s, ctx := newUncheckpointedStore(t)

		old := multiCreateChunkTxs
		multiCreateChunkTxs = 1

		t.Cleanup(func() { multiCreateChunkTxs = old })

		root := mkTx(t, 1, 50_000)
		_, err := s.Create(ctx, root, 1_000, utxo.WithMinedBlockInfo(
			utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1_000, OnLongestChain: true}))
		require.NoError(t, err)

		parent := spendOutput(t, root, 0, 2)
		child := spendOutput(t, parent, 0, 1)

		// Group 0 writes parent, with the netted journal row of child's spend of its output 0.
		// The crash comes before group 1 writes child.
		multiFault = func(at string) error {
			if at == "created group 0" {
				return crash
			}

			return nil
		}

		_, err = s.SpendAndCreateMulti(ctx, []*bt.Tx{parent, child}, 1_151, utxo.WithIgnoreLocked(true))
		multiFault = nil
		require.ErrorIs(t, err, crash)

		require.Equal(t, 1, journalRowsFor(t, s, ctx, parent, 0), "the netted spend is journaled")
		require.Equal(t, 1, utxoCount(t, s, ctx, parent), "and its output never became a UTXO")

		repeatAcrossPartitionThenReorg(t, s, ctx, child, parent, 2)
	})
}

// TestJournalLeavesASpendRepeatedInTheSamePartition pins the ordinary repeat: a crash retry of
// the same block, at the same height. Its journal row is already where it belongs, so the repeat
// writes nothing to the journal.
func TestJournalLeavesASpendRepeatedInTheSamePartition(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 1_000, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1_000, OnLongestChain: true}))
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)
	crash := errMultiCrash

	multiFault = func(at string) error {
		if at == "spent" {
			return crash
		}

		return nil
	}

	_, err = s.SpendAndCreateMulti(ctx, []*bt.Tx{child}, 1_100, utxo.WithIgnoreLocked(true))
	multiFault = nil
	require.ErrorIs(t, err, crash)

	h := parent.TxIDChainHash()

	var before uint32
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT xmin::text::bigint FROM spend_journal WHERE ukey = $1 AND txid = $2`,
		Pack(h[:], 0), h[:]).Scan(&before))

	_, _, err = s.SpendAndCreate(ctx, child, 1_151, utxo.WithIgnoreLocked(true))
	require.NoError(t, err)

	var after uint32
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT xmin::text::bigint FROM spend_journal WHERE ukey = $1 AND txid = $2`,
		Pack(h[:], 0), h[:]).Scan(&after))

	require.Equal(t, before, after, "a repeat in the same partition rewrote its journal row")
}
