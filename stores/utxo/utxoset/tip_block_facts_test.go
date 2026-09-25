package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// These tests pin cases where a UTXO, or the undo copy of a spent UTXO, ends up carrying block
// facts that are wrong once its containment window has retired. Each one needs a reorg or a
// transaction seen before its block, so none of them can happen during below-checkpoint sync,
// and all of them can at the tip. They reproduce known defects rather than guard working
// behaviour.
//
// The rule every test checks: once the window holding a transaction's containment rows is gone,
// whatever still answers for that transaction must name the block that is on the longest chain.
//
// All four pass as plain assertions now. Until the stamp existed three of them sat behind an
// expected-failure guard that failed the test the moment its defect was fixed, so a fix could
// not merge while its proof was still switched off; the last guard went with the read path's
// second tier and the preservation source rule.
//
// The design that fixes them is
// docs/superpowers/specs/2026-09-21-utxoset-block-facts-spec-rebuilt.md. Reproduction 4 was
// closed by the containment build, which made the un-mine a point delete. Reproductions 1 and 2
// are closed by the chain-aware stamp, and reproduction 3 by its drop rule.

// TestRetiringWindowStampsTheBlockThatWonTheReorg is reproduction 1: the transaction is mined
// in M, a competing block F also includes it, and F's chain then wins.
//
// This is the call sequence block assembly makes. F arrives as a fork, so recording it adds a
// containment row. When F's branch becomes the longest chain, Reset keeps the transaction out
// of the mark-off because it is in a move-forward block, and stampMoveForwardBlockAsMined
// records F again with OnLongestChain set. M stays a valid block on a side chain, so nothing
// un-mines it.
//
// The retirement stamp that used to take the earliest row, and so stamped M onto the UTXO, is
// gone. The stamp is told which block won by the chain answer the pruner service builds, and
// it deletes M's row as a loser before it writes F onto the UTXO.
func TestRetiringWindowStampsTheBlockThatWonTheReorg(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 99)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 8, BlockHeight: 100})
	require.NoError(t, err)

	// F's branch wins, and the move-forward stamp names F on the longest chain.
	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 8, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	tip := stampThrough(t, s, ctx, 0, map[uint32]uint32{100: 8})

	h, b := utxoFacts(t, s, ctx, tx)
	require.Equal(t, int32(100), h)
	require.Equal(t, int32(8), b, "the block that won the reorg, not the first block that recorded the transaction")
	require.Equal(t, 1, minedRows(t, s, ctx, tx), "block 7's containment row is gone")
	require.False(t, identExists(t, s, ctx, tx), "the identity row is gone")

	require.Equal(t, 1, dropStamped(t, s, ctx, tip))

	h, b = utxoFacts(t, s, ctx, tx)
	require.Equal(t, int32(100), h)
	require.Equal(t, int32(8), b, "still, once the window is gone")

	got, err := s.Get(ctx, tx.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err)
	require.Equal(t, []uint32{8}, got.BlockIDs)
}

// TestSideChainCreateIsCorrectedWhenTheMainChainBlockStampsIt is reproduction 2: a fork block F
// is applied first and creates the transaction on the block path, then the main chain block M
// includes it too.
//
// The store applies the checkpoint test itself, so on a network with no checkpoints (this
// store's) a block-carrying create takes the identity route: an identity row, a containment
// row for F and UTXOs at (0,0). When M later records the same transaction it adds its own
// containment row and clears the marker. The stamp, told that M won, writes (100, 7) and
// deletes F's row. Below the checkpoint this scenario cannot arise: a fork block cannot be
// header-proven.
func TestSideChainCreateIsCorrectedWhenTheMainChainBlockStampsIt(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 8, BlockHeight: 100}))
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	tip := stampThrough(t, s, ctx, 0, map[uint32]uint32{100: 7})

	h, b := utxoFacts(t, s, ctx, tx)
	require.Equal(t, int32(100), h)
	require.Equal(t, int32(7), b, "the main chain block, not the side-chain block that created the UTXO")
	require.Equal(t, 1, minedRows(t, s, ctx, tx), "block 8's containment row is gone")
	require.False(t, identExists(t, s, ctx, tx))

	require.Equal(t, 1, dropStamped(t, s, ctx, tip))

	got, err := s.Get(ctx, tx.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err)
	require.Equal(t, []uint32{7}, got.BlockIDs)
}

// TestParentSpentWhileUnconfirmedIsStillAnswerableAfterItsWindowRetires is reproduction 3: a
// transaction seen before its block is mined, and a child spends its only UTXO before the
// stamp reaches the window.
//
// The spend copies the UTXO into the journal with the UTXO's pair, and a UTXO created before its
// block still carries (0,0) at that point, because recording mined does not touch UTXOs. The
// stamp finds no live UTXO, so it stamps nothing, and nothing ever stamps journal rows. The
// containment window is then the parent's only home, and the drop rule keeps it attached past
// every undo copy of its UTXOs: the window drops no earlier than 1,728 blocks after its
// stamped_at, and never while an undo partition covering a height below stamped_at is
// attached.
//
// Three states. While the undo copy lives, the parent answers: its window is below the lookup
// floor by then, so the answer comes through the second tier, triggered by the (0,0) undo copy.
// Once the undo copy's partition has dropped, nothing triggers a read of the window even though
// it is still attached, and not found is the correct answer, because nothing can spend or
// unspend this parent any more. Once the window is gone too, the same.
func TestParentSpentWhileUnconfirmedIsStillAnswerableAfterItsWindowRetires(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 99)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(parent), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	spendOneOutput(t, s, ctx, parent, 0, 101)

	tip := stampThrough(t, s, ctx, 0, map[uint32]uint32{100: 7})
	require.Equal(t, uint32(575), tip)
	require.False(t, identExists(t, s, ctx, parent), "the stamp deletes the identity row even with no UTXO to stamp")

	// The undo copy's partition drops at tip 1,728; the window cannot drop before 2,591.
	require.NoError(t, s.SetBlockHeight(1727))

	require.Equal(t, int32(864), s.lookupFloor(), "window 0 is out of the first tier")

	tier2Before := testutil.ToFloat64(lookupTier2Keys.WithLabelValues("undo_zero"))

	got, err := s.Get(ctx, parent.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err, "a mined parent whose last UTXO was spent must still be found while its undo copy lives")
	require.Equal(t, []uint32{7}, got.BlockIDs)
	require.Equal(t, tier2Before+1, testutil.ToFloat64(lookupTier2Keys.WithLabelValues("undo_zero")), "through the second tier")

	// The undo copy's partition drops at 1,728. The window is still attached, and nothing
	// triggers a read of it.
	require.NoError(t, s.SetBlockHeight(1728))
	_, err = s.dropSpendJournalPartitionsBelow(ctx, 1728-s.journalRetention)
	require.NoError(t, err)
	require.True(t, windowAttached(t, s, ctx, 0))

	_, err = s.Get(ctx, parent.TxIDChainHash(), fields.BlockIDs)
	require.True(t, errors.Is(err, errors.ErrTxNotFound), "no trigger is left, and nothing can ask")

	require.Equal(t, 1, dropStamped(t, s, ctx, tip), "the window drops once every undo copy of its UTXOs is gone")

	_, err = s.Get(ctx, parent.TxIDChainHash(), fields.BlockIDs)
	require.True(t, errors.Is(err, errors.ErrTxNotFound), "nothing names block 7 any more, and nothing can ask")
}

// TestUnspendOfAnUnminedParentRestoresAnUnconfirmedUTXO: a parent seen before its block is
// mined in M, a child spends one of its UTXOs, M is un-mined so the parent is unmined again,
// and then the child's spend is undone.
//
// This is the fourth reproduction, and the containment change closes it, so its guard is a
// plain assertion. It runs on a store with no checkpoints, because an un-mine at or below the
// checkpoint is refused. The parent is created unmined rather than through the block path,
// because a block-path create writes no identity row and the un-mine, now a point delete,
// produces none, so the identity assertion below would have nothing to find. With an unmined create the
// identity row is there because it is kept alive until the stamp. The un-mine deletes the one
// containment row and sets the marker; the undo copy holds (0,0) because recording mined never
// touched the UTXO; and the restore, finding no containment row, falls back to that copy and
// puts (0,0) back. The old code's move-back deleted every row and its restore could resurrect a
// stale pair from the copy, which is the hazard the block-born variant still carries and which
// rests on the open decision about invalidation at or below the checkpoint.
func TestUnspendOfAnUnminedParentRestoresAnUnconfirmedUTXO(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 99)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(parent), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          0,
		LockingScript: parent.Outputs[0].LockingScript,
		Satoshis:      parent.Outputs[0].Satoshis,
	}))
	child.AddOutput(&bt.Output{Satoshis: parent.Outputs[0].Satoshis - 1_000, LockingScript: parent.Outputs[0].LockingScript})

	_, err = s.Create(ctx, child, 101)
	require.NoError(t, err)

	spends, err := spendOnly(ctx, s, child, 101)
	require.NoError(t, err)

	require.NoError(t, s.SetBlockHeight(200))

	_, err = s.SetMinedMulti(ctx, hashes(parent), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, UnsetMined: true})
	require.NoError(t, err)
	require.True(t, identExists(t, s, ctx, parent), "the identity row was there all along and the un-mine marks it")
	require.NotNil(t, markerOf(t, s, ctx, parent), "the parent is unmined again")
	require.Equal(t, 0, minedRows(t, s, ctx, parent), "and the one containment row is gone")

	require.NoError(t, s.Unspend(ctx, spends, false))

	h, b := utxoFacts(t, s, ctx, parent)
	require.Equal(t, int32(0), h, "a restored UTXO of an unmined transaction is unconfirmed")
	require.Equal(t, int32(0), b)
}
