package utxoset

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// holdFenceExclusive takes the stamp's exclusive fence lock on a transaction of its own and
// returns the function that releases it. Ordinary postgres: what the stamp's first step does.
func holdFenceExclusive(t *testing.T, s *Store, ctx context.Context) func() {
	t.Helper()

	dbTx, err := s.pool.Begin(ctx)
	require.NoError(t, err)

	_, err = dbTx.Exec(ctx, `SELECT pg_advisory_xact_lock($1, $2)`, int32(fenceLockKey1), int32(fenceLockKey2))
	require.NoError(t, err)

	return func() { _ = dbTx.Rollback(ctx) }
}

// finishes reports whether fn returns within d.
func finishes(fn func(), d time.Duration) bool {
	done := make(chan struct{})

	go func() {
		fn()
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// TestFenceLockBlocksTheContainmentWritersAndNotSpends is the design's ST-25. While the stamp's
// first step holds the fence, a record-mined call, an un-mine and an Unspend all wait, and
// proceed once it is released. A spend does not wait. The block-path create is the fourth
// writer and has its own test below, because on a store with no checkpoints a block-carrying
// create takes the identity route and writes no containment.
func TestFenceLockBlocksTheContainmentWritersAndNotSpends(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	parent := mkTx(t, 3, 5_000)
	_, err := s.Create(ctx, parent, 99)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(parent), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	spender := spendOneOutput(t, s, ctx, parent, 0, 101)
	undo := []*utxo.Spend{{TxID: parent.TxIDChainHash(), Vout: 0, SpendingData: spend.NewSpendingData(spender.TxIDChainHash(), 0)}}

	// The second spender is created BEFORE the fence is taken. Creates go through the store's
	// serialised create batcher, and the block-path create below will sit in that batcher
	// blocked on the fence; an unmined create queued behind it would wait for the fence too,
	// through the batcher rather than through the lock, and that is not what this test is
	// measuring. The spend itself takes no batcher and no fence.
	second := mkSpender(t, parent, 1)
	_, err = s.Create(ctx, second, 102)
	require.NoError(t, err)

	release := holdFenceExclusive(t, s, ctx)
	defer release()

	const wait = 400 * time.Millisecond

	writers := map[string]func(){
		"record mined": func() {
			_, _ = s.SetMinedMulti(ctx, hashes(parent), utxo.MinedBlockInfo{BlockID: 8, BlockHeight: 100})
		},
		"un-mine": func() {
			_, _ = s.SetMinedMulti(ctx, hashes(parent), utxo.MinedBlockInfo{BlockID: 8, BlockHeight: 100, UnsetMined: true})
		},
		"unspend": func() {
			_ = s.Unspend(ctx, undo, false)
		},
	}

	for name, w := range writers {
		require.False(t, finishes(w, wait), "%s must wait for the fence", name)
	}

	require.True(t, finishes(func() {
		_, err := spendOnly(ctx, s, second, 102)
		require.NoError(t, err)
	}, 5*time.Second), "a spend never takes the fence")

	release()

	// Everything that was waiting now goes through. The un-mine and the record-mined call
	// raced each other, so only that the unspend landed is asserted.
	require.Eventually(t, func() bool {
		return utxoCount(t, s, ctx, parent) == 2
	}, 10*time.Second, 50*time.Millisecond, "the unspend restored output 0 once the fence was released")
}

// TestFenceLockBlocksTheBlockPathCreate is the fourth writer of ST-25, on a store with
// checkpoints so that a create carrying a block below the checkpoint writes containment.
func TestFenceLockBlocksTheBlockPathCreate(t *testing.T) {
	s, ctx := newTestStore(t)

	release := holdFenceExclusive(t, s, ctx)
	defer release()

	newTx := mkTx(t, 1, 6_000)

	create := func() {
		_, _ = s.Create(ctx, newTx, 200, utxo.WithMinedBlockInfo(
			utxo.MinedBlockInfo{BlockID: 20, BlockHeight: 200, OnLongestChain: true}))
	}

	require.False(t, finishes(create, 400*time.Millisecond), "a block-path create waits for the fence")

	release()

	require.Eventually(t, func() bool {
		return utxoCount(t, s, ctx, newTx) == 1
	}, 10*time.Second, 50*time.Millisecond, "and lands once it is released")
}

// TestUnspendRepairsAZeroPairFromTheOneRowBelowTheFence is the design's ST-10: a UTXO spent
// before its stamp and restored after it comes back carrying the winner's pair, read from the
// one containment row the stamp left below the fence, with no chain access.
func TestUnspendRepairsAZeroPairFromTheOneRowBelowTheFence(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 99)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(parent), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(parent), utxo.MinedBlockInfo{BlockID: 8, BlockHeight: 100})
	require.NoError(t, err)

	child := spendOneOutput(t, s, ctx, parent, 0, 101)

	stampThrough(t, s, ctx, 0, map[uint32]uint32{100: 7})
	require.Equal(t, [][2]int32{{100, 7}}, pairsOf(t, s, ctx, parent), "the surviving output is stamped")
	require.False(t, identExists(t, s, ctx, parent))

	before := testutil.ToFloat64(unspendRepaired)

	require.NoError(t, s.Unspend(ctx, []*utxo.Spend{{
		TxID: parent.TxIDChainHash(), Vout: 0, SpendingData: spend.NewSpendingData(child.TxIDChainHash(), 0),
	}}))

	require.Equal(t, [][2]int32{{100, 7}, {100, 7}}, pairsOf(t, s, ctx, parent), "the restored UTXO carries the winner, never the loser")
	require.Equal(t, before+1, testutil.ToFloat64(unspendRepaired))
}

// TestUnspendRefusesARestoreItCannotRepair is the design's ST-11: no containment row below the
// fence and no identity row, or more than one row, roll the whole call back with a storage
// error and a counter, and the undo copy is left where it was.
func TestUnspendRefusesARestoreItCannotRepair(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 99)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(parent), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	child := spendOneOutput(t, s, ctx, parent, 0, 101)
	undo := []*utxo.Spend{{TxID: parent.TxIDChainHash(), Vout: 0, SpendingData: spend.NewSpendingData(child.TxIDChainHash(), 0)}}

	stampThrough(t, s, ctx, 0, map[uint32]uint32{100: 7})

	// A second row below the fence, planted by raw SQL: the state a loser that survived the
	// stamp's delete would leave.
	plantMined(t, s, ctx, hashBytes(parent), 9, 100, 0)

	ambiguousBefore := testutil.ToFloat64(unspendRepairAmbiguous)

	err = s.Unspend(ctx, undo, false)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrStorageError), "a storage error, not an accounting one")
	require.Equal(t, ambiguousBefore+1, testutil.ToFloat64(unspendRepairAmbiguous))
	require.Equal(t, 0, utxoCount(t, s, ctx, parent), "rolled back: nothing restored")

	// Now no row at all, and no identity row.
	_, err = s.pool.Exec(ctx, `DELETE FROM tx_mined WHERE txid = $1`, hashBytes(parent))
	require.NoError(t, err)

	noSourceBefore := testutil.ToFloat64(unspendRepairNoSource)

	err = s.Unspend(ctx, undo, false)
	require.Error(t, err)
	require.Equal(t, noSourceBefore+1, testutil.ToFloat64(unspendRepairNoSource))
	require.Equal(t, 0, utxoCount(t, s, ctx, parent))

	var undoRows int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM spend_journal WHERE txid = $1`, hashBytes(parent)).Scan(&undoRows))
	require.Equal(t, 1, undoRows, "the undo copy was not consumed")
}

// TestSecondTierAnswersAnUnstampedParentBelowTheLookupFloor is the stamp-backlog case: the
// stamp is three windows late, a transaction seen before its block sits below the lookup floor
// with its identity row and no containment in the first tier, and the second tier answers.
func TestSecondTierAnswersAnUnstampedParentBelowTheLookupFloor(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 99)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(parent), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	// Lookup floor: (1440 - 576) aligned down = 864. Window 0 is out of the first tier.
	require.NoError(t, s.SetBlockHeight(1440))
	require.Equal(t, int32(864), s.lookupFloor())

	before := testutil.ToFloat64(lookupTier2Keys.WithLabelValues("ident_marker_null"))
	answered := testutil.ToFloat64(lookupTier2Answered)

	got, err := s.Get(ctx, parent.TxIDChainHash(), fields.BlockIDs, fields.BlockHeights)
	require.NoError(t, err)
	require.Equal(t, []uint32{7}, got.BlockIDs)
	require.Equal(t, []uint32{100}, got.BlockHeights)
	require.Equal(t, uint32(0), got.UnminedSince, "and the identity row's payload")

	require.Equal(t, before+1, testutil.ToFloat64(lookupTier2Keys.WithLabelValues("ident_marker_null")))
	require.Equal(t, answered+1, testutil.ToFloat64(lookupTier2Answered))

	// An unmined transaction below the floor is answered by its identity row alone and never
	// sent to the second tier.
	unmined := mkTx(t, 1, 6_000)
	_, err = s.Create(ctx, unmined, 99)
	require.NoError(t, err)

	got, err = s.Get(ctx, unmined.TxIDChainHash(), fields.BlockIDs)
	require.NoError(t, err)
	require.Empty(t, got.BlockIDs)
	require.NotZero(t, got.UnminedSince)
	require.Equal(t, before+1, testutil.ToFloat64(lookupTier2Keys.WithLabelValues("ident_marker_null")), "not a second-tier key")
}

// TestPreservationSkipsAParentInAnUncompletedWindow is the design's CT-4, and its complement:
// preservation copies nothing while the parent's only containment row is in a window the stamp
// has not completed, and copies the surviving row once it has.
func TestPreservationSkipsAParentInAnUncompletedWindow(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 99)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(parent), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100})
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(parent), utxo.MinedBlockInfo{BlockID: 8, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	waitingBefore := testutil.ToFloat64(preserveWaiting)

	require.NoError(t, s.PreserveTransactions(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, 5_000))

	_, ok := preservedPairOf(t, s, ctx, parent)
	require.False(t, ok, "skipped: a row in an uncompleted window can still be a loser")
	require.Equal(t, waitingBefore+1, testutil.ToFloat64(preserveWaiting))

	stampThrough(t, s, ctx, 0, map[uint32]uint32{100: 8})

	require.NoError(t, s.PreserveTransactions(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, 5_000))

	pair, ok := preservedPairOf(t, s, ctx, parent)
	require.True(t, ok)
	require.Equal(t, [2]int32{100, 8}, pair, "the surviving row is the winner")
}

// TestFencedRecordMinedAndUnMine pins the refusal table for record mined and the un-mine below
// the fence: an off-chain record inserts nothing and returns success with the submitted id; an
// on-chain record whose rows all exist is a quiet replay; an on-chain record with a row absent
// is the boundary error; an un-mine of an existing row is the boundary error; an un-mine of an
// absent row is allowed and counted.
func TestFencedRecordMinedAndUnMine(t *testing.T) {
	s, ctx := newUncheckpointedStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 99)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	stampThrough(t, s, ctx, 0, map[uint32]uint32{100: 7})

	floors, err := s.Floors(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(288), floors.StampFence)

	// A valid deep fork block that cannot win: recorded off the longest chain, nothing written,
	// success, the submitted id reported so the caller's coverage check passes.
	offBefore := testutil.ToFloat64(fenceNoops.WithLabelValues("off_chain_insert"))

	ids, err := s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 9, BlockHeight: 100})
	require.NoError(t, err)
	require.Contains(t, ids[*tx.TxIDChainHash()], uint32(9))
	require.Equal(t, 1, minedRows(t, s, ctx, tx), "no row was inserted")
	require.Equal(t, offBefore+1, testutil.ToFloat64(fenceNoops.WithLabelValues("off_chain_insert")))

	// A replay of the winner: quiet success.
	ids, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)
	require.Equal(t, []uint32{7}, ids[*tx.TxIDChainHash()])

	// A main-chain block below the fence with an absent row: the boundary error.
	refusedBefore := testutil.ToFloat64(boundaryRefusals.WithLabelValues("record_mined"))

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 9, BlockHeight: 100, OnLongestChain: true})
	require.Error(t, err)
	require.Contains(t, err.Error(), "[utxoset][boundary]")
	require.Equal(t, refusedBefore+1, testutil.ToFloat64(boundaryRefusals.WithLabelValues("record_mined")))
	require.Equal(t, 1, minedRows(t, s, ctx, tx))

	// An un-mine of the winner: the boundary error, nothing deleted.
	unmineRefused := testutil.ToFloat64(boundaryRefusals.WithLabelValues("un_mine"))

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, UnsetMined: true})
	require.Error(t, err)
	require.Equal(t, unmineRefused+1, testutil.ToFloat64(boundaryRefusals.WithLabelValues("un_mine")))
	require.Equal(t, 1, minedRows(t, s, ctx, tx))

	// An un-mine of an invalid deep fork block whose row was a loser and is gone: allowed.
	absentBefore := testutil.ToFloat64(fenceNoops.WithLabelValues("unmine_absent"))

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 9, BlockHeight: 100, UnsetMined: true})
	require.NoError(t, err)
	require.Equal(t, absentBefore+1, testutil.ToFloat64(fenceNoops.WithLabelValues("unmine_absent")))
	require.Equal(t, 1, minedRows(t, s, ctx, tx))
}

// TestFencedBlockPathCreate pins the create's two rows of the refusal table: a re-offered block
// below the fence falls through to the claim and gets ErrTxExists, and a block with a
// transaction that has no row there is the boundary error.
func TestFencedBlockPathCreate(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	stampThrough(t, s, ctx, 0, map[uint32]uint32{100: 7})

	_, err = s.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}))
	require.True(t, errors.Is(err, errors.ErrTxExists), "a re-offered block is refused by the claim as it always was")

	refusedBefore := testutil.ToFloat64(boundaryRefusals.WithLabelValues("create"))

	fresh := mkTx(t, 1, 6_000)
	_, err = s.Create(ctx, fresh, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}))
	require.Error(t, err)
	require.Contains(t, err.Error(), "[utxoset][boundary]")
	require.Equal(t, refusedBefore+1, testutil.ToFloat64(boundaryRefusals.WithLabelValues("create")))
	require.Equal(t, 0, utxoCount(t, s, ctx, fresh), "nothing written")
}

// mkSpender builds a transaction that spends one output of parent, without storing it.
func mkSpender(t *testing.T, parent *bt.Tx, vout uint32) *bt.Tx {
	t.Helper()

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          vout,
		LockingScript: parent.Outputs[vout].LockingScript,
		Satoshis:      parent.Outputs[vout].Satoshis,
	}))
	child.AddOutput(&bt.Output{Satoshis: parent.Outputs[vout].Satoshis - 1_000, LockingScript: parent.Outputs[vout].LockingScript})

	return child
}

// TestBlockPathCreateAheadOfTheTipIsCounted is decision 1: the window drop rule assumes no block
// is applied more than 287 heights ahead of the store's height, and the block-path create counts
// every block that breaks the assumption rather than enforcing it.
func TestBlockPathCreateAheadOfTheTipIsCounted(t *testing.T) {
	s, ctx := newTestStore(t)
	require.NoError(t, s.SetBlockHeight(100))

	before := testutil.ToFloat64(createAheadOfTip)

	inside := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, inside, 387, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 3, BlockHeight: 387, OnLongestChain: true}))
	require.NoError(t, err)
	require.Equal(t, before, testutil.ToFloat64(createAheadOfTip), "287 ahead is inside the premise")

	beyond := mkTx(t, 1, 6_000)
	_, err = s.Create(ctx, beyond, 388, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 4, BlockHeight: 388, OnLongestChain: true}))
	require.NoError(t, err)
	require.Equal(t, before+1, testutil.ToFloat64(createAheadOfTip), "288 ahead is counted, and the create still lands")
	require.Equal(t, 1, utxoCount(t, s, ctx, beyond))
}
