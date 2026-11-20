package utxoset

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestUnspendRestoresAUTXOWithItsParentsBlockFactsFromMembership: the journal carries no
// block facts (they are mutable, and the journal payload is not); the restore reads them
// from tx_mined, where the parent's row is present for as long as any of its spends can be
// undone.
func TestUnspendRestoresAUTXOWithItsParentsBlockFactsFromMembership(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	child := spendOneOutput(t, s, ctx, parent, 0, 700_150)

	spends, err := utxo.GetSpends(child)
	require.NoError(t, err)
	require.NoError(t, s.Unspend(ctx, spends))

	h, b := utxoFacts(t, s, ctx, parent)
	require.Equal(t, int32(700_100), h)
	require.Equal(t, int32(42), b)
}

// TestUnspendOfAnAlreadyRestoredUTXOIsANoOp pins the fix for the idempotent-replay gap: a
// second Unspend on an outpoint the first call already restored must succeed without
// creating a duplicate UTXO, even when the replayed request names a different spender than
// whatever actually did the restoring. This is exactly the shape BlockAssembler's
// conflict-intent WAL replay can produce -- a crash between a successful Unspend and its
// intent's completion record means replay calls Unspend again, and it may not remember (or
// may misremember) which spending transaction the original call used. Ownership only gates
// consuming the journal row; once the UTXO is live, the UTXO being unspent is the fact that
// matters, not who put it there.
func TestUnspendOfAnAlreadyRestoredUTXOIsANoOp(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	child := spendOneOutput(t, s, ctx, parent, 0, 700_150)

	realSpends, err := utxo.GetSpends(child)
	require.NoError(t, err)
	require.NoError(t, s.Unspend(ctx, realSpends))
	require.Equal(t, 1, utxoCount(t, s, ctx, parent), "the UTXO must be restored exactly once")

	// A replayed Unspend naming a spender that never actually spent this outpoint.
	fakeSpender := chainhash.HashH([]byte("not-the-real-spender"))
	fakeSpends := []*utxo.Spend{{
		TxID:         parent.TxIDChainHash(),
		Vout:         0,
		SpendingData: spend.NewSpendingData(&fakeSpender, 0),
	}}

	require.NoError(t, s.Unspend(ctx, fakeSpends),
		"re-unspending an already-restored UTXO must be a no-op even under a different claimed spender")
	require.Equal(t, 1, utxoCount(t, s, ctx, parent), "a replayed restore must not create a second UTXO")
}

// utxoFlagsOf reads the flag byte off one live UTXO, located exactly.
func utxoFlagsOf(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx, vout uint32) int16 {
	t.Helper()

	h := tx.TxIDChainHash()

	var flags int16
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT flags FROM utxo WHERE leaf = $1 AND ukey = $2 AND txid = $3`,
		LeafFor(h[:]), Pack(h[:], vout), h[:]).Scan(&flags))

	return flags
}

// TestUnspendLocksAUTXOItFoundAlreadyLive is the hold half of the restore, and it was missing
// for exactly the UTXOs conflict resolution needs it for.
//
// Unspend(spends, true) means "put these UTXOs back AND hold them", and the hold is what stops
// anyone else spending a contested parent while the resolution decides which child gets it. The
// flag was ORed only into rows the restore INSERTED, so a parent whose UTXO was already live --
// which is what a crash between the unspend and the lock leaves, and exactly the case
// SetConflicting now reports -- came back from step 2 unheld, stayed spendable for the whole of
// steps 2 to 5, and then had step 5's SetLocked(false) applied to it anyway. If it had been
// locked for some unrelated reason, that step dropped the unrelated lock too.
//
// The sql reference locks the transaction row unconditionally, which is why it has never had
// this gap.
func TestUnspendLocksAUTXOItFoundAlreadyLive(t *testing.T) {
	s, ctx := newTestStore(t)

	// A block-path parent: its UTXO is live and nothing has ever spent it, so there is no
	// journal row for the restore to consume.
	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	// The child that contests it, marked conflicting. SetConflicting names the parent because
	// its UTXO is live, which is the record ProcessConflicting hands to Unspend at step 2.
	child := spendOutput(t, parent, 0, 1)
	_, err = s.Create(ctx, child, 700_101)
	require.NoError(t, err)

	affected, _, err := s.SetConflicting(ctx, []chainhash.Hash{*child.TxIDChainHash()}, true)
	require.NoError(t, err)
	require.Len(t, affected, 1, "the live parent must be named, or there is nothing to hold")

	require.NoError(t, s.Unspend(ctx, affected, true))

	require.Equal(t, 1, utxoCount(t, s, ctx, parent), "nothing was restored, and nothing duplicated")
	require.NotZero(t, utxoFlagsOf(t, s, ctx, parent, 0)&FlagLocked,
		"a UTXO the restore found already live must still be held")

	// And the driver's step 5 releases it, which is the whole point of naming it.
	require.NoError(t, s.SetLocked(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, false))
	require.Zero(t, utxoFlagsOf(t, s, ctx, parent, 0)&FlagLocked)
}

// TestUnspendWithoutTheHoldLeavesALiveUTXOAlone is the other half: Unspend(spends) with no hold
// asked for must not invent one. A reorg restore has no business locking anything.
func TestUnspendWithoutTheHoldLeavesALiveUTXOAlone(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)
	_, err = s.Create(ctx, child, 700_101)
	require.NoError(t, err)

	affected, _, err := s.SetConflicting(ctx, []chainhash.Hash{*child.TxIDChainHash()}, true)
	require.NoError(t, err)
	require.Len(t, affected, 1)

	require.NoError(t, s.Unspend(ctx, affected, false))
	require.Zero(t, utxoFlagsOf(t, s, ctx, parent, 0)&FlagLocked)
}

// TestUnspendGatesTheJournalsBlockFactsOnASingleSharedCondition pins the fact that
// mined_height and block_id are read off the journal's own copy as ONE pair, gated on the
// same condition, rather than as two independently-gated columns that happen to usually
// agree.
//
// mined_height falling back through NULLIF(t.mined_height, 0) and block_id falling back
// through CASE WHEN t.mined_height > 0 are two different mechanisms for what must be the
// same fact: nothing in the schema stops mined_height from going negative (it is a plain
// INTEGER, no CHECK), and NULLIF only tests "not exactly zero" while the CASE tests
// "greater than zero" -- they diverge on a negative value. A journal row that got there
// some other way than the paired write the comments assume -- mined_height negative,
// block_id still carrying whatever it last held -- must restore the UTXO as fully
// unconfirmed, not as a height with no matching block.
func TestUnspendGatesTheJournalsBlockFactsOnASingleSharedCondition(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 700_100)
	require.NoError(t, err)

	ph := parent.TxIDChainHash()
	spender := chainhash.HashH([]byte("spender"))

	// The UTXO is spent by hand, outside the normal spend path, so the journal row below is
	// the only fact the restore has to work from -- there is no tx_mined row to prefer it
	// over either.
	_, err = s.pool.Exec(ctx, `DELETE FROM utxo WHERE leaf = $1 AND ukey = $2 AND txid = $3`,
		LeafFor(ph[:]), Pack(ph[:], 0), ph[:])
	require.NoError(t, err)

	require.NoError(t, s.ensureSpendJournalPartition(ctx, 700_150))

	_, err = s.pool.Exec(ctx, `
        INSERT INTO spend_journal (spent_height, satoshis, created_height, spendable_from,
                                   flags, mined_height, block_id, ukey, txid, spending_txid,
                                   script)
        VALUES (700150, $1, 700100, 0, 0, -7, 55, $2, $3, $4, '\x00')`,
		parent.Outputs[0].Satoshis, Pack(ph[:], 0), ph[:], spender[:])
	require.NoError(t, err)

	require.NoError(t, s.Unspend(ctx, []*utxo.Spend{{
		TxID:         ph,
		Vout:         0,
		SpendingData: spend.NewSpendingData(&spender, 0),
	}}))

	h, b := utxoFacts(t, s, ctx, parent)
	require.Zero(t, h, "a negative journal height must never surface as this UTXO's mined height")
	require.Zero(t, b, "and its paired block id must be zeroed with it, not restored on its own")
}

// TestUnspendGivesTheRestoredUTXOItsTransactionsLockAsItIsNow is a coin spent through
// IgnoreLocked while its transaction was locked, as block validation spends a parent mined in
// the same block, and restored after the transaction was unlocked. The journal recorded the
// lock, and copying it back left a coin no plain spend could take, with nothing left to clear
// it. In aerospike the lock is the transaction's, so the restored coin is spendable.
func TestUnspendGivesTheRestoredUTXOItsTransactionsLockAsItIsNow(t *testing.T) {
	s, ctx := newTestStore(t)
	require.NoError(t, s.SetBlockHeight(100))

	parent := mkTx(t, 2, 10_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithLocked(true))
	require.NoError(t, err)

	loser := spendParentTx(t, parent, nil, 1_000, 0)
	_, err = s.Create(ctx, loser, 101)
	require.NoError(t, err)

	spends, err := spendOnly(ctx, s, loser, 101, utxo.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Len(t, spends, 1)

	require.NoError(t, s.SetLocked(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, false))

	require.NoError(t, s.Unspend(ctx, spends))
	require.Nil(t, spenderOf(t, s, ctx, parent, 0))

	other := spendParentTx(t, parent, nil, 2_000, 0)
	_, err = s.Create(ctx, other, 102)
	require.NoError(t, err)

	_, err = spendOnly(ctx, s, other, 102)
	require.NoError(t, err, "the restored UTXO of an unlocked transaction must be spendable")
}

// TestUnspendKeepsTheLockOfATransactionStillLocked is the other half: a coin restored while
// its transaction is still locked comes back locked, so a plain spend is still refused.
func TestUnspendKeepsTheLockOfATransactionStillLocked(t *testing.T) {
	s, ctx := newTestStore(t)
	require.NoError(t, s.SetBlockHeight(100))

	parent := mkTx(t, 2, 10_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithLocked(true))
	require.NoError(t, err)

	loser := spendParentTx(t, parent, nil, 1_000, 0)
	spends, err := spendOnly(ctx, s, loser, 101, utxo.WithIgnoreLocked(true))
	require.NoError(t, err)

	require.NoError(t, s.Unspend(ctx, spends))

	spends, err = spendOnly(ctx, s, spendParentTx(t, parent, nil, 2_000, 0), 102)
	require.Error(t, err)
	require.True(t, errors.Is(spends[0].Err, errors.ErrTxLocked), "got %v", spends[0].Err)
}

// lockedStampedParentSpent is the fixture for the stamped-lock tests: a parent mined in block 7
// at height 100, stamped so its identity row is gone and only its containment row in tx_mined
// records it, then locked through SetLocked, which reaches that containment row, and spent
// through IgnoreLocked by an unmined loser. It returns the parent, the loser's spends and the
// store's height after the spend.
func lockedStampedParentSpent(t *testing.T, s *Store, ctx context.Context) (*bt.Tx, []*utxo.Spend, uint32) {
	t.Helper()

	parent := mkTx(t, 2, 10_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	tip := stampThrough(t, s, ctx, 0, map[uint32]uint32{100: 7})

	require.False(t, identExists(t, s, ctx, parent), "fixture: the stamp deleted the identity row")
	require.Equal(t, 1, minedRows(t, s, ctx, parent), "fixture: the containment row is the only record")
	require.Zero(t, s.lookupFloor(), "fixture: the containment row at 100 is above the lookup floor")

	require.NoError(t, s.SetLocked(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, true))

	var minedFlags int16
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT flags FROM tx_mined WHERE txid = $1`,
		hashBytes(parent)).Scan(&minedFlags))
	require.NotZero(t, minedFlags&FlagLocked, "fixture: SetLocked locked the containment row")

	spendAt := tip + 1
	require.NoError(t, s.SetBlockHeight(spendAt))

	spends, err := spendOnly(ctx, s, spendParentTx(t, parent, nil, 1_000, 0), spendAt, utxo.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Len(t, spends, 1)

	return parent, spends, spendAt
}

// TestUnspendGivesAStampedTransactionsRestoredUTXOItsLockAsItIsNow is
// TestUnspendGivesTheRestoredUTXOItsTransactionsLockAsItIsNow for a mined transaction the stamp
// has moved out of tx_ident: the lock the restore reads is the one on its containment row, and
// that row was unlocked after the spend, so the restored coin is spendable.
func TestUnspendGivesAStampedTransactionsRestoredUTXOItsLockAsItIsNow(t *testing.T) {
	s, ctx := newTestStore(t)

	parent, spends, spendAt := lockedStampedParentSpent(t, s, ctx)

	require.NoError(t, s.SetLocked(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, false))

	require.NoError(t, s.Unspend(ctx, spends))
	require.Nil(t, spenderOf(t, s, ctx, parent, 0))

	_, err := spendOnly(ctx, s, spendParentTx(t, parent, nil, 2_000, 0), spendAt+1)
	require.NoError(t, err, "the restored UTXO of an unlocked stamped transaction must be spendable")
}

// TestUnspendKeepsTheLockOfAStampedTransactionStillLocked is the other half: the containment
// row is still locked when the coin is restored, so the coin comes back locked and a plain spend
// is refused. The lock is on tx_mined alone here, so a restore that read only tx_ident would
// hand back a spendable coin.
func TestUnspendKeepsTheLockOfAStampedTransactionStillLocked(t *testing.T) {
	s, ctx := newTestStore(t)

	parent, spends, spendAt := lockedStampedParentSpent(t, s, ctx)

	require.NoError(t, s.Unspend(ctx, spends))
	require.Nil(t, spenderOf(t, s, ctx, parent, 0))

	spends, err := spendOnly(ctx, s, spendParentTx(t, parent, nil, 2_000, 0), spendAt+1)
	require.Error(t, err)
	require.True(t, errors.Is(spends[0].Err, errors.ErrTxLocked), "got %v", spends[0].Err)
}

// TestUnspendIgnoresALockOnAContainmentRowBelowTheLookupFloor: the restore reads a containment
// row's lock only at or above the lookup floor, the same bound SetLocked writes and the reads
// look within. Here the containment row at height 100 is still locked but the tip has moved so
// the floor is 288, so the restore does not see that lock and the coin comes back spendable.
func TestUnspendIgnoresALockOnAContainmentRowBelowTheLookupFloor(t *testing.T) {
	s, ctx := newTestStore(t)

	parent, spends, _ := lockedStampedParentSpent(t, s, ctx)

	// 864 is the lowest height whose floor is one partition: 864 - (stamp depth 288 + 288) = 288.
	const tipAboveFloor = 864

	require.NoError(t, s.SetBlockHeight(tipAboveFloor))
	require.Equal(t, int32(TxMinedPartitionBlocks), s.lookupFloor(), "fixture: the floor is above the row at 100")
	require.Equal(t, 1, minedRows(t, s, ctx, parent), "fixture: the locked containment row is still there")

	require.NoError(t, s.Unspend(ctx, spends))
	require.Nil(t, spenderOf(t, s, ctx, parent, 0))

	_, err := spendOnly(ctx, s, spendParentTx(t, parent, nil, 2_000, 0), tipAboveFloor+1)
	require.NoError(t, err, "a lock below the lookup floor must not be copied onto the restored UTXO")
}

// setConflictingTo is SetConflicting's statements, without the note of children it writes first.
func setConflictingTo(s *Store, tx *bt.Tx, value bool) lockChange {
	return func(ctx context.Context, dbTx pgx.Tx) error {
		named := []chainhash.Hash{*tx.TxIDChainHash()}

		inpoints, err := s.readConflictingInputs(ctx, named)
		if err != nil {
			return err
		}

		_, _, err = s.runConflictingPlan(ctx, dbTx, s.planConflicting(named, inpoints), value)

		return err
	}
}

// TestUnspendReadsTheConflictingMarkBelowTheLookupFloor: the conflicting bit is read from
// containment rows down to the dropped floor, not only from the lookup floor up as the lock bit
// is. The parent's only row is at height 100. It was marked while still above the lookup floor,
// after the loser had spent its coin, so the loser's journal row carries no mark. Once the floor
// passes 100, a restore that read only from the lookup floor up would find no row, keep the
// journal's bit and hand back a coin that is spendable while its transaction is a loser.
func TestUnspendReadsTheConflictingMarkBelowTheLookupFloor(t *testing.T) {
	s, ctx := newTestStore(t)

	require.NoError(t, s.SetBlockHeight(100))

	parent := mkTx(t, 2, 10_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	spends, err := spendOnly(ctx, s, spendParentTx(t, parent, nil, 1_000, 0), 100)
	require.NoError(t, err)
	require.Len(t, spends, 1)

	_, err = s.SetMinedMulti(ctx, []*chainhash.Hash{parent.TxIDChainHash()},
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	_, _, err = s.SetConflicting(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, true)
	require.NoError(t, err)

	stampThrough(t, s, ctx, 0, map[uint32]uint32{100: 7})
	require.False(t, identExists(t, s, ctx, parent), "fixture: the stamp deleted the identity row")
	require.Equal(t, 1, minedRows(t, s, ctx, parent), "fixture: the containment row is the only record")

	require.NoError(t, s.SetBlockHeight(864))
	require.Equal(t, int32(TxMinedPartitionBlocks), s.lookupFloor(), "fixture: the lookup floor is above the row at 100")

	require.NoError(t, s.Unspend(ctx, spends))
	require.Nil(t, spenderOf(t, s, ctx, parent, 0))
	require.NotZero(t, utxoFlagsOf(t, s, ctx, parent, 0)&FlagConflicting,
		"the restored coin must carry the mark on a containment row below the lookup floor")
}

// TestUnspendRacingAConflictingClearBelowTheLookupFloorWaitsForIt: the restore reads the
// conflicting bit from containment rows down to the dropped floor, below the lookup floor the lock
// bit stops at, so its share lock reaches that far too. Here the parent has its identity row and a
// containment row at height 100, under a lookup floor of 288, both marked conflicting. A
// SetConflicting clearing the mark has updated both rows and not committed. The restore waits on
// the identity row either way, but a containment row it read without a lock would still show the
// mark from before the wait, and the coin would come back conflicting after its transaction was
// cleared.
func TestUnspendRacingAConflictingClearBelowTheLookupFloorWaitsForIt(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 10_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, []*chainhash.Hash{parent.TxIDChainHash()},
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	require.NoError(t, s.SetBlockHeight(864))
	require.Equal(t, int32(TxMinedPartitionBlocks), s.lookupFloor(), "fixture: the lookup floor is above the row at 100")
	require.True(t, identExists(t, s, ctx, parent), "fixture: the identity row is still there")
	require.Equal(t, 1, minedRows(t, s, ctx, parent), "fixture: the containment row at 100 is still there")

	_, _, err = s.SetConflicting(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, true)
	require.NoError(t, err)

	spends, err := spendOnly(ctx, s, spendParentTx(t, parent, nil, 1_000, 0), 865, utxo.WithIgnoreConflicting(true))
	require.NoError(t, err)
	require.Len(t, spends, 1)

	changeWhileRestoreRuns(t, s, ctx, setConflictingTo(s, parent, false), spends)

	require.Nil(t, spenderOf(t, s, ctx, parent, 0))
	require.Zero(t, utxoFlagsOf(t, s, ctx, parent, 0)&FlagConflicting,
		"the restored coin must not keep a conflicting mark its transaction lost while the restore waited")
}

// TestUnspendCutByItsDeadlineLeavesTheUndoRecord: nothing in the store bounds the restore's wait
// for the rows it share-locks, so the caller's deadline does. A restore cut while it waits must
// return an error and roll back whole, the coin still spent and its undo record still there, so a
// later Unspend can restore it. The validator's shed unwind depends on that.
func TestUnspendCutByItsDeadlineLeavesTheUndoRecord(t *testing.T) {
	s, ctx := newTestStore(t)

	parent, spends := mempoolParentSpent(t, s, ctx, false)
	h := parent.TxIDChainHash()

	holder, err := s.pool.Begin(ctx)
	require.NoError(t, err)

	defer func() { _ = holder.Rollback(context.Background()) }()

	_, err = holder.Exec(ctx, `SELECT 1 FROM tx_ident WHERE txid = $1 FOR UPDATE`, h[:])
	require.NoError(t, err)

	cut, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()

	require.Error(t, s.Unspend(cut, spends), "the deadline cuts the wait")
	require.NotNil(t, spenderOf(t, s, ctx, parent, 0), "the coin is still spent")
	require.Equal(t, 1, countWhere(t, s, ctx, `SELECT count(*) FROM spend_journal WHERE txid = $1`, h[:]),
		"the undo record is still there")

	require.NoError(t, holder.Rollback(ctx))

	require.NoError(t, s.Unspend(ctx, spends))
	require.Nil(t, spenderOf(t, s, ctx, parent, 0), "a later Unspend restores the coin")
}

// mempoolParentSpent is the fixture for the restore-against-lock-change race on a transaction
// whose lock lives on its identity row: a parent created locked or not, its output 0 spent
// through IgnoreLocked by an unmined loser, as block validation spends a locked parent. It
// returns the parent and the loser's spends.
func mempoolParentSpent(t *testing.T, s *Store, ctx context.Context, locked bool) (*bt.Tx, []*utxo.Spend) {
	t.Helper()

	require.NoError(t, s.SetBlockHeight(100))

	parent := mkTx(t, 2, 10_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithLocked(locked))
	require.NoError(t, err)

	spends, err := spendOnly(ctx, s, spendParentTx(t, parent, nil, 1_000, 0), 101, utxo.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Len(t, spends, 1)

	return parent, spends
}

// lockChange is one way the store writes a transaction's lock, run inside the caller's
// transaction.
type lockChange func(ctx context.Context, dbTx pgx.Tx) error

// setLockedTo is SetLocked's statements.
func setLockedTo(s *Store, parent *bt.Tx, value bool) lockChange {
	return func(ctx context.Context, dbTx pgx.Tx) error {
		return s.setLockedIn(ctx, dbTx, []chainhash.Hash{*parent.TxIDChainHash()}, value)
	}
}

// mineRelease is the lock release a mine makes, both halves of it: releaseLocksOnMine on the
// transaction's own rows, then releaseUTXOLocks on its coins, as recordMined runs them.
func mineRelease(s *Store, parent *bt.Tx) lockChange {
	return func(ctx context.Context, dbTx pgx.Tx) error {
		floor := s.lookupFloor()

		released, err := releaseLocksOnMine(ctx, dbTx, [][]byte{hashBytes(parent)}, floor)
		if err != nil {
			return err
		}

		return releaseUTXOLocks(ctx, dbTx, released, floor)
	}
}

// changeWhileRestoreRuns is the race with the lock change first. The change has run every
// statement and is not yet committed when the restore starts, so its coin update cannot have
// seen the coin the restore is about to insert. The restore must wait for the change's commit
// and read the lock it leaves, or the coin keeps the old lock and nothing changes it after that.
func changeWhileRestoreRuns(t *testing.T, s *Store, ctx context.Context, change lockChange, spends []*utxo.Spend) {
	t.Helper()

	dbTx, err := s.pool.Begin(ctx)
	require.NoError(t, err)

	defer func() { _ = dbTx.Rollback(ctx) }()

	require.NoError(t, change(ctx, dbTx))

	done := make(chan error, 1)

	go func() { done <- s.Unspend(ctx, spends) }()

	waitForLockWait(t, s, ctx, "spend_journal")
	require.NoError(t, dbTx.Commit(ctx))
	require.NoError(t, <-done)
}

// restoreWhileChangeRuns is the race with the restore first. The restore has run and is not yet
// committed when the lock change starts, so the change cannot see the coin yet. The change must
// wait for the restore's commit and then set the lock on the coin it finds.
func restoreWhileChangeRuns(t *testing.T, s *Store, ctx context.Context, change lockChange, spends []*utxo.Spend) {
	t.Helper()

	req, err := newUnspendRequest(spends, false)
	require.NoError(t, err)

	restore, err := s.pool.Begin(ctx)
	require.NoError(t, err)

	defer func() { _ = restore.Rollback(ctx) }()

	res, err := s.restoreIn(ctx, restore, req)
	require.NoError(t, err)
	require.Equal(t, 1, res.restored)

	done := make(chan error, 1)

	go func() {
		dbTx, err := s.pool.Begin(ctx)
		if err != nil {
			done <- err
			return
		}

		if err := change(ctx, dbTx); err != nil {
			_ = dbTx.Rollback(ctx)
			done <- err

			return
		}

		done <- dbTx.Commit(ctx)
	}()

	waitForLockWait(t, s, ctx, "UPDATE tx_")
	require.NoError(t, restore.Commit(ctx))
	require.NoError(t, <-done)
}

// requireRestoredCoinLock checks the restored coin carries the lock the change left on its
// transaction.
func requireRestoredCoinLock(t *testing.T, s *Store, ctx context.Context, parent *bt.Tx, locked bool) {
	t.Helper()

	require.Nil(t, spenderOf(t, s, ctx, parent, 0))
	require.Equal(t, locked, utxoFlagsOf(t, s, ctx, parent, 0)&FlagLocked != 0,
		"the restored coin must carry its transaction's lock as the change left it")
}

// TestUnspendRacingAnUnlockOfTheIdentityRowWaitsForIt: a mempool transaction, unlocked while
// its coin is being restored, unlock first.
func TestUnspendRacingAnUnlockOfTheIdentityRowWaitsForIt(t *testing.T) {
	s, ctx := newTestStore(t)

	parent, spends := mempoolParentSpent(t, s, ctx, true)

	changeWhileRestoreRuns(t, s, ctx, setLockedTo(s, parent, false), spends)
	requireRestoredCoinLock(t, s, ctx, parent, false)
}

// TestUnlockOfTheIdentityRowRacingAnUnspendWaitsForIt: the same race, restore first.
func TestUnlockOfTheIdentityRowRacingAnUnspendWaitsForIt(t *testing.T) {
	s, ctx := newTestStore(t)

	parent, spends := mempoolParentSpent(t, s, ctx, true)

	restoreWhileChangeRuns(t, s, ctx, setLockedTo(s, parent, false), spends)
	requireRestoredCoinLock(t, s, ctx, parent, false)
}

// TestUnspendRacingALockOfTheIdentityRowWaitsForIt: the other direction, an unlocked mempool
// transaction locked while its coin is being restored, lock first.
func TestUnspendRacingALockOfTheIdentityRowWaitsForIt(t *testing.T) {
	s, ctx := newTestStore(t)

	parent, spends := mempoolParentSpent(t, s, ctx, false)

	changeWhileRestoreRuns(t, s, ctx, setLockedTo(s, parent, true), spends)
	requireRestoredCoinLock(t, s, ctx, parent, true)
}

// TestLockOfTheIdentityRowRacingAnUnspendWaitsForIt: the lock direction, restore first.
func TestLockOfTheIdentityRowRacingAnUnspendWaitsForIt(t *testing.T) {
	s, ctx := newTestStore(t)

	parent, spends := mempoolParentSpent(t, s, ctx, false)

	restoreWhileChangeRuns(t, s, ctx, setLockedTo(s, parent, true), spends)
	requireRestoredCoinLock(t, s, ctx, parent, true)
}

// TestUnspendRacingAMineReleaseWaitsForIt: a mine releases a locked transaction's lock while its
// coin is being restored, mine first. The mine writes the lock the way SetLocked does, the rows
// in one statement and the coins in a later one, so it has the same race.
func TestUnspendRacingAMineReleaseWaitsForIt(t *testing.T) {
	s, ctx := newTestStore(t)

	parent, spends := mempoolParentSpent(t, s, ctx, true)

	changeWhileRestoreRuns(t, s, ctx, mineRelease(s, parent), spends)
	requireRestoredCoinLock(t, s, ctx, parent, false)
}

// TestMineReleaseRacingAnUnspendWaitsForIt: the mine's release, restore first.
func TestMineReleaseRacingAnUnspendWaitsForIt(t *testing.T) {
	s, ctx := newTestStore(t)

	parent, spends := mempoolParentSpent(t, s, ctx, true)

	restoreWhileChangeRuns(t, s, ctx, mineRelease(s, parent), spends)
	requireRestoredCoinLock(t, s, ctx, parent, false)
}

// TestUnspendRacingAnUnlockOfAContainmentRowWaitsForIt: a stamped transaction, whose lock lives
// only on its containment row in tx_mined, unlocked while its coin is being restored, unlock
// first.
func TestUnspendRacingAnUnlockOfAContainmentRowWaitsForIt(t *testing.T) {
	s, ctx := newTestStore(t)

	parent, spends, _ := lockedStampedParentSpent(t, s, ctx)

	changeWhileRestoreRuns(t, s, ctx, setLockedTo(s, parent, false), spends)
	requireRestoredCoinLock(t, s, ctx, parent, false)
}

// TestUnlockOfAContainmentRowRacingAnUnspendWaitsForIt: the stamped case, restore first. The
// unlock's containment-row update waits on the restore's share lock. When that update shared
// a statement with the coin update, the coin update still read the snapshot taken before the
// wait, missed the coin, and left it locked.
func TestUnlockOfAContainmentRowRacingAnUnspendWaitsForIt(t *testing.T) {
	s, ctx := newTestStore(t)

	parent, spends, _ := lockedStampedParentSpent(t, s, ctx)

	restoreWhileChangeRuns(t, s, ctx, setLockedTo(s, parent, false), spends)
	requireRestoredCoinLock(t, s, ctx, parent, false)
}

// crossedLockChange holds first's identity row in an open transaction, starts an Unspend of
// both parents' spends, waits for it to block on first, then takes second's identity row too.
// When the restore had already share-locked second before it reached first, that last update
// waits on the restore while the restore waits on it, and PostgreSQL stops the restore, which
// started waiting first, as the deadlock victim. The Unspend must run again and succeed.
//
// It reports false when the holder was the victim instead. That happens when more than the
// restore's deadlock_timeout passes between the restore blocking and the holder blocking: the
// restore's one deadlock check has already run and found no cycle, so the holder's check is the
// one that finds it. The holder's change is then rolled back, so the caller redoes the iteration
// on a fresh store rather than asserting on a lock that was never changed.
func crossedLockChange(t *testing.T, s *Store, ctx context.Context, first, second *bt.Tx, spends []*utxo.Spend) bool {
	t.Helper()

	holder, err := s.pool.Begin(ctx)
	require.NoError(t, err)

	defer func() { _ = holder.Rollback(context.Background()) }()

	// The restore must be the deadlock victim, and it is without this: PostgreSQL runs the check
	// in the backend whose deadlock_timeout runs out first, and the restore started waiting first.
	// Only a superuser may raise the holder's timeout, which widens that margin from milliseconds.
	var superuser string
	require.NoError(t, holder.QueryRow(ctx, `SELECT current_setting('is_superuser')`).Scan(&superuser))

	if superuser == "on" {
		_, err = holder.Exec(ctx, `SET LOCAL deadlock_timeout = '30s'`)
		require.NoError(t, err)
	} else {
		t.Log("not a superuser, so the holder keeps the default deadlock_timeout")
	}

	require.NoError(t, setLockedTo(s, first, false)(ctx, holder))

	done := make(chan error, 1)

	go func() { done <- s.Unspend(ctx, spends) }()

	waitForLockWait(t, s, ctx, "spend_journal")

	err = setLockedTo(s, second, false)(ctx, holder)
	if isDeadlock(err) {
		require.NoError(t, holder.Rollback(ctx))
		require.NoError(t, <-done)

		return false
	}

	require.NoError(t, err)

	require.NoError(t, holder.Commit(ctx))
	require.NoError(t, <-done, "a deadlock is retried, not returned")

	return true
}

// TestUnspendRetriesADeadlockWithALockChange: the restore share-locks each parent's identity
// row in turn, and a SetLocked naming both parents can hold one and then ask for the other.
// Whichever row the restore reaches first, one of the two orders crosses it, so across both
// orders at least one attempt is stopped as a deadlock and run again, and both coins come
// back with the lock the change left.
func TestUnspendRetriesADeadlockWithALockChange(t *testing.T) {
	before := testutil.ToFloat64(deadlockRetried.WithLabelValues("unspend"))

	for _, holdFirst := range []int{0, 1} {
		for attempt := 1; ; attempt++ {
			if crossedLockChangeOnce(t, holdFirst) {
				break
			}

			require.Less(t, attempt, 3, "the holder, not the restore, was the deadlock victim on every attempt")
		}
	}

	require.GreaterOrEqual(t, testutil.ToFloat64(deadlockRetried.WithLabelValues("unspend"))-before, 1.0,
		"one of the two orders must cross the restore's share locks and be retried")
}

// crossedLockChangeOnce is one order of TestUnspendRetriesADeadlockWithALockChange on a fresh
// store. It reports false when crossedLockChange had to give the iteration up.
func crossedLockChangeOnce(t *testing.T, holdFirst int) bool {
	t.Helper()

	s, ctx := newTestStore(t)

	require.NoError(t, s.SetBlockHeight(100))

	parents := make([]*bt.Tx, 2)

	var spends []*utxo.Spend

	for i := range parents {
		parents[i] = mkTx(t, 2, uint64(10_000+i))
		_, err := s.Create(ctx, parents[i], 100, utxo.WithLocked(true))
		require.NoError(t, err)

		spent, err := spendOnly(ctx, s, spendParentTx(t, parents[i], nil, 1_000, 0), 101, utxo.WithIgnoreLocked(true))
		require.NoError(t, err)
		require.Len(t, spent, 1)

		spends = append(spends, spent...)
	}

	if !crossedLockChange(t, s, ctx, parents[holdFirst], parents[1-holdFirst], spends) {
		return false
	}

	for _, parent := range parents {
		requireRestoredCoinLock(t, s, ctx, parent, false)
	}

	return true
}
