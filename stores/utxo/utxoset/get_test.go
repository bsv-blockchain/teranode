package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// TestGetReturnsTheWholeTransaction is the read path everything else is built on. A
// field-less Get is the default shape the conformance suite uses, and it expects the raw
// transaction back, which is why the body table had to exist before this could work.
func TestGetReturnsTheWholeTransaction(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 2, 4_242)
	_, err := s.Create(ctx, tx, 700_000)
	require.NoError(t, err)

	got, err := s.Get(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
	require.NotNil(t, got)

	require.NotNil(t, got.Tx, "a field-less Get must carry the transaction: callers dereference it without a nil check")
	require.Equal(t, tx.TxID(), got.Tx.TxID())
	require.Equal(t, uint64(tx.Size()), got.SizeInBytes)
	require.Equal(t, uint32(tx.LockTime), got.LockTime)
	require.False(t, got.IsCoinbase)
}

// TestGetReportsAMissingTransactionAsNotFound. The distinction matters: the validator turns
// a not-found parent into TxMissingParent and rejects the child, which is recoverable,
// whereas a zero-valued answer would be silently wrong.
func TestGetReportsAMissingTransactionAsNotFound(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 1_000)

	_, err := s.Get(ctx, tx.TxIDChainHash())
	require.True(t, errors.Is(err, errors.ErrTxNotFound), "want ErrTxNotFound, got %v", err)
}

// TestGetUnpacksBlockMembershipInInsertionOrder pins the ordering the shared conformance
// suite asserts: subtree indexes come back in the order they were written, never sorted.
//
// The two blocks are now reached by two stamps rather than by one create carrying two
// MinedBlockInfos. A create that carries block information takes the block path, which claims
// a single membership row for the block it was given, so two blocks can only ever be two
// separate acts. At the tip that is what happens anyway: the transaction arrives from the
// mempool and each block that contains it stamps it in turn.
func TestGetUnpacksBlockMembershipInInsertionOrder(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, tx, 700_000)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx),
		utxo.MinedBlockInfo{BlockID: 9, BlockHeight: 700_002, SubtreeIdx: 7, OnLongestChain: true})
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx),
		utxo.MinedBlockInfo{BlockID: 4, BlockHeight: 700_001, SubtreeIdx: 2, OnLongestChain: true})
	require.NoError(t, err)

	got, err := s.Get(ctx, tx.TxIDChainHash())
	require.NoError(t, err)

	require.Equal(t, []uint32{9, 4}, got.BlockIDs)
	require.Equal(t, []uint32{700_002, 700_001}, got.BlockHeights)
	require.Equal(t, []int{7, 2}, got.SubtreeIdxs, "insertion order, not sorted")
}

// TestGetReportsWhetherTheTransactionIsWaitingToBeMined. Block assembly reads this to decide
// whether a transaction belongs in the mempool, and a wrong answer here loses funds: on a
// delete-on-spend store the parents' coin rows are already gone, and an absent coin row
// reads as spent, so a transaction dropped from the mempool can never be mined and its
// inputs can never be spent again.
func TestGetReportsWhetherTheTransactionIsWaitingToBeMined(t *testing.T) {
	s, ctx := newTestStore(t)

	waiting := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, waiting, 700_000)
	require.NoError(t, err)

	got, err := s.Get(ctx, waiting.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, uint32(700_000), got.UnminedSince, "a mempool arrival is waiting to be mined")

	mined := mkTx(t, 1, 2_000)
	_, err = s.Create(ctx, mined, 700_000, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 700_000, SubtreeIdx: 0, OnLongestChain: true}))
	require.NoError(t, err)

	got, err = s.Get(ctx, mined.TxIDChainHash())
	require.NoError(t, err)
	require.Zero(t, got.UnminedSince, "confirmed on the longest chain means not waiting")
}

// TestGetSurvivesABodyThatHasAgedOut is the case that panics on this branch today, in the
// asset repository, because a body-less row is dereferenced with no nil check.
//
// The body window is dropped after 288 blocks while the identity row lives as long as any
// output is unspent, so a body-less row is the ordinary steady state for an old transaction,
// not an error. Get must return what it has.
func TestGetSurvivesABodyThatHasAgedOut(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, tx, 700_000)
	require.NoError(t, err)

	h := tx.TxIDChainHash()
	_, err = s.pool.Exec(ctx, `DELETE FROM tx_body WHERE txid = $1`, h[:])
	require.NoError(t, err)

	got, err := s.Get(ctx, h)
	require.NoError(t, err, "an aged-out body is the steady state for an old transaction, not a failure")
	require.Nil(t, got.Tx, "and the caller must be able to see that it is absent")
	require.Equal(t, uint64(tx.Size()), got.SizeInBytes, "everything the identity row holds still answers")
}

// TestCreatingAConflictingTransactionRecordsItOnItsParents.
//
// When a transaction loses a double-spend race it is stored as conflicting rather than
// discarded, because resolving the conflict later needs to find it. Finding it means asking
// the PARENT whose coin was contested, so the parent has to carry the list.
//
// Without this, conflict resolution has no way to walk from a contested coin to the
// transactions competing for it.
func TestCreatingAConflictingTransactionRecordsItOnItsParents(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	loser := bt.NewTx()
	require.NoError(t, loser.FromUTXOs(&bt.UTXO{
		TxIDHash: parent.TxIDChainHash(), Vout: 0,
		LockingScript: parent.Outputs[0].LockingScript, Satoshis: parent.Outputs[0].Satoshis,
	}))
	loser.AddOutput(&bt.Output{Satoshis: 4_000, LockingScript: parent.Outputs[0].LockingScript})

	_, err = s.Create(ctx, loser, 100, utxo.WithConflicting(true))
	require.NoError(t, err)

	got, err := s.Get(ctx, loser.TxIDChainHash(), fields.ConflictingChildren)
	require.NoError(t, err)
	require.True(t, got.Conflicting, "the loser is flagged")
	require.Empty(t, got.ConflictingChildren, "and has no conflicting children of its own")

	gotParent, err := s.Get(ctx, parent.TxIDChainHash(), fields.ConflictingChildren)
	require.NoError(t, err)
	require.False(t, gotParent.Conflicting, "the parent did nothing wrong")
	require.Len(t, gotParent.ConflictingChildren, 1,
		"but it must name the transaction contesting its coin, or conflict resolution cannot find it")
	require.Equal(t, loser.TxID(), gotParent.ConflictingChildren[0].String())
}
