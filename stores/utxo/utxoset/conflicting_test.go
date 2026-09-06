package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// spendOutput builds a transaction that spends one output of parent and adds n outputs of its
// own, so that varying n gives distinct transactions.
func spendOutput(t *testing.T, parent *bt.Tx, vout uint32, nOut int) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          vout,
		LockingScript: parent.Outputs[vout].LockingScript,
		Satoshis:      parent.Outputs[vout].Satoshis,
	}))

	for i := 0; i < nOut; i++ {
		tx.AddOutput(&bt.Output{
			Satoshis:      uint64(1_000 + i),
			LockingScript: parent.Outputs[vout].LockingScript,
		})
	}

	return tx
}

// TestSetConflictingStopsTheCoinsBeingSpent is the whole point of the flag.
//
// It has to reach BOTH rows. The identity row is what a metadata read shows; the coin row is
// what the spend path reads, and the spend path never looks at the identity row. Setting only
// one leaves a transaction reporting itself conflicting while its coins stay spendable.
func TestSetConflictingStopsTheCoinsBeingSpent(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	ph := parent.TxIDChainHash()

	_, _, err = s.SetConflicting(ctx, []chainhash.Hash{*ph}, true)
	require.NoError(t, err)

	meta, err := s.Get(ctx, ph)
	require.NoError(t, err)
	require.True(t, meta.Conflicting, "the identity row must report it")

	child := spendOutput(t, parent, 0, 1)

	spends, err := spendOnly(ctx, s, child, 200)
	require.Error(t, err, "a rejected spend is now a returned error, and rolled back")
	require.True(t, errors.Is(spends[0].Err, errors.ErrTxConflicting),
		"the coin row must refuse the spend, got %v", spends[0].Err)
}

// TestClearingConflictingRestoresSpendability. Conflict resolution promotes a winner by
// clearing the flag and then spending through it, so a one-way flag would strand the winner.
func TestClearingConflictingRestoresSpendability(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	ph := parent.TxIDChainHash()

	_, _, err = s.SetConflicting(ctx, []chainhash.Hash{*ph}, true)
	require.NoError(t, err)

	_, _, err = s.SetConflicting(ctx, []chainhash.Hash{*ph}, false)
	require.NoError(t, err)

	meta, err := s.Get(ctx, ph)
	require.NoError(t, err)
	require.False(t, meta.Conflicting, "the flag must clear on the identity row")

	child := spendOutput(t, parent, 0, 1)

	spends, err := spendOnly(ctx, s, child, 200)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err, "and on the coin row, or the winner can never be spent")
}

// TestSetConflictingNotesTheContestOnItsParents.
//
// A transaction that loses a double-spend race is kept rather than discarded, because
// resolving the race later has to find it. Finding it means asking the PARENT whose coin was
// contested, so the parent carries the list. Without it there is no route from a contested
// coin to the transactions competing for it.
func TestSetConflictingNotesTheContestOnItsParents(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)
	_, err = s.Create(ctx, child, 101)
	require.NoError(t, err)

	ch := child.TxIDChainHash()

	_, _, err = s.SetConflicting(ctx, []chainhash.Hash{*ch}, true)
	require.NoError(t, err)

	pmeta, err := s.Get(ctx, parent.TxIDChainHash(), fields.ConflictingChildren)
	require.NoError(t, err)
	require.Len(t, pmeta.ConflictingChildren, 1)
	require.Equal(t, ch.String(), pmeta.ConflictingChildren[0].String(),
		"the parent must name the transaction contesting its coin")

	// Offering the same transaction again must not grow the list.
	_, _, err = s.SetConflicting(ctx, []chainhash.Hash{*ch}, true)
	require.NoError(t, err)

	pmeta, err = s.Get(ctx, parent.TxIDChainHash(), fields.ConflictingChildren)
	require.NoError(t, err)
	require.Len(t, pmeta.ConflictingChildren, 1, "a repeat must not be noted twice")
}

// TestSetConflictingReturnsOnlySpendsItsOwnUnspendCanRestore is the correction the review
// caught, and it is the one that decides whether conflict resolution can run at all.
//
// The first return value feeds straight into this store's Unspend, which fails unless EVERY
// record it is given comes back. So a record for an input that was never actually spent, or
// whose undo record has aged out, is not a harmless extra: it makes the whole restore fail.
// Only inputs with a live undo record naming this transaction may be reported.
func TestSetConflictingReturnsOnlySpendsItsOwnUnspendCanRestore(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)
	_, err = s.Create(ctx, child, 101)
	require.NoError(t, err)

	// Only now is the coin actually taken, so only now is there anything to restore.
	spends, err := spendOnly(ctx, s, child, 101)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)

	affected, _, err := s.SetConflicting(ctx, []chainhash.Hash{*child.TxIDChainHash()}, true)
	require.NoError(t, err)
	require.Len(t, affected, 1, "exactly the one input it actually spent")

	require.NotNil(t, affected[0].SpendingData, "Unspend refuses a record with no spender")
	require.Equal(t, child.TxIDChainHash().String(), affected[0].SpendingData.TxID.String())

	// The real proof: hand it straight back to Unspend, which is what conflict resolution
	// does with it.
	require.NoError(t, s.Unspend(ctx, affected, false),
		"every record returned must be one this store can restore")
}

// TestSetConflictingStillNamesAParentWhoseCoinWasNeverTaken. The same rule from the other
// side, and it used to say the opposite: a transaction whose inputs were never taken reported
// NO spends at all, on the argument that a record which is not a spend makes the restore fail.
//
// It does not. Unspend counts an already-live coin as work someone else has done and returns
// success, so the record costs a probe and nothing else. Omitting it is what actually breaks,
// because the shared conflict-resolution driver takes the parents named here as the set to
// unlock when it finishes: a parent locked by a crashed run, whose coin was restored before the
// crash, would be left locked forever with nothing to say so. That is the state
// ConflictWALCrashRecovery's forward_after_step2_unspend_lock case reconstructs.
func TestSetConflictingStillNamesAParentWhoseCoinWasNeverTaken(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)
	_, err = s.Create(ctx, child, 101)
	require.NoError(t, err)

	affected, _, err := s.SetConflicting(ctx, []chainhash.Hash{*child.TxIDChainHash()}, true)
	require.NoError(t, err)
	require.Len(t, affected, 1, "the parent whose coin is still live must be named")
	require.Equal(t, parent.TxIDChainHash().String(), affected[0].TxID.String())
	require.Equal(t, uint32(0), affected[0].Vout)
	require.Nil(t, affected[0].UTXOHash,
		"no journal row means no captured amount or script, so no coin identity is invented")

	// The proof that the extra record is harmless: it goes straight back to Unspend, which is
	// what conflict resolution does with it.
	require.NoError(t, s.Unspend(ctx, affected, false),
		"an already-live coin is a no-op success, not a missing-coin failure")
}

// TestSetConflictingReportsNothingBeyondJournalRetention is the half of the old rule that
// stands. An input with no journal row AND no live coin cannot be restored by this store: the
// undo payload has aged out, or a different transaction re-spent the coin since. Reporting it
// would make Unspend fail the whole restore rather than complete it, so it is omitted.
func TestSetConflictingReportsNothingBeyondJournalRetention(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)
	_, err = s.Create(ctx, child, 101)
	require.NoError(t, err)

	spends, err := spendOnly(ctx, s, child, 101)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)

	// The coin is gone, destroyed by the spend, and now the undo record goes too.
	_, err = s.pool.Exec(ctx, `DELETE FROM spend_journal`)
	require.NoError(t, err)

	affected, _, err := s.SetConflicting(ctx, []chainhash.Hash{*child.TxIDChainHash()}, true)
	require.NoError(t, err)
	require.Empty(t, affected, "a coin this store cannot restore must not be offered as one")
}

// TestSetConflictingCascadesDownTheChain exercises the second return value, which drives the
// walk that marks a loser's descendants.
//
// The coin row is destroyed the moment it is spent, so the undo journal is the only place this
// store can answer "who took this coin". This is the test that fails if the key range bound or
// the full-id recheck is wrong.
func TestSetConflictingCascadesDownTheChain(t *testing.T) {
	s, ctx := newTestStore(t)

	root := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, root, 100)
	require.NoError(t, err)

	child := spendOutput(t, root, 0, 1)
	_, err = s.Create(ctx, child, 101)
	require.NoError(t, err)

	spends, err := spendOnly(ctx, s, child, 101)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)

	grandchild := spendOutput(t, child, 0, 1)
	_, err = s.Create(ctx, grandchild, 102)
	require.NoError(t, err)

	spends, err = spendOnly(ctx, s, grandchild, 102)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)

	// One level at a time: marking the root must name the child as the next level.
	_, next, err := s.SetConflicting(ctx, []chainhash.Hash{*root.TxIDChainHash()}, true)
	require.NoError(t, err)
	require.Len(t, next, 1)
	require.Equal(t, child.TxIDChainHash().String(), next[0].String())

	// And the whole walk, through the shared recursive helper every store is held to.
	_, marked, err := utxo.MarkConflictingRecursively(ctx, s, []chainhash.Hash{*root.TxIDChainHash()})
	require.NoError(t, err)

	got := make(map[string]bool, len(marked))
	for _, h := range marked {
		got[h.String()] = true
	}

	require.True(t, got[root.TxIDChainHash().String()], "the root")
	require.True(t, got[child.TxIDChainHash().String()], "its child")
	require.True(t, got[grandchild.TxIDChainHash().String()], "and its grandchild")
}

// TestSetConflictingReportsATransactionItDoesNotHold, matching both reference stores: an
// absent hash is an error, not a silent skip.
func TestSetConflictingReportsATransactionItDoesNotHold(t *testing.T) {
	s, ctx := newTestStore(t)

	known := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, known, 100)
	require.NoError(t, err)

	missing := mkTx(t, 1, 9_999)

	_, _, err = s.SetConflicting(ctx,
		[]chainhash.Hash{*known.TxIDChainHash(), *missing.TxIDChainHash()}, true)

	require.Error(t, err, "a hash the store does not hold must fail loudly")
	require.Contains(t, err.Error(), missing.TxIDChainHash().String())
}

// TestSetConflictingOnAnEmptyListIsANoOp, which is how the cascade terminates.
func TestSetConflictingOnAnEmptyListIsANoOp(t *testing.T) {
	s, ctx := newTestStore(t)

	affected, next, err := s.SetConflicting(ctx, nil, true)
	require.NoError(t, err)
	require.Empty(t, affected)
	require.Empty(t, next)
}

// TestNotingTheSameContestTwiceRecordsItOnce.
//
// This replaces a defect test for the packed column the bookkeeping used to live in. That
// column was a concatenation of 32-byte ids, so membership had to be tested on a 32-byte
// boundary: a plain substring search matches bytes STRADDLING two neighbouring entries, reads
// that as already-present, and silently skips a real append. One row per child cannot be
// matched straddling its neighbours, so that whole class of defect is gone rather than
// defended against.
//
// What is left to pin is the dedupe, which now has two halves. Inside one window the unique
// index and ON CONFLICT DO NOTHING make a repeat write nothing; across windows the same pair
// is two legal rows, because a unique index on a partitioned table must include the partition
// key, so the READER has to say DISTINCT. Both are exercised: the store's height is moved past
// a window boundary between the two notes.
func TestNotingTheSameContestTwiceRecordsItOnce(t *testing.T) {
	s, ctx := newTestStore(t)
	require.NoError(t, s.SetBlockHeight(100))

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	loser := spendOutput(t, parent, 0, 1)
	_, err = s.Create(ctx, loser, 100, utxo.WithConflicting(true))
	require.NoError(t, err)

	// Same window: the insert's ON CONFLICT is what absorbs this.
	_, _, err = s.SetConflicting(ctx, []chainhash.Hash{*loser.TxIDChainHash()}, true)
	require.NoError(t, err)

	// A different window: two rows now exist, and only the reader's DISTINCT hides them.
	require.NoError(t, s.SetBlockHeight(100+SpendJournalPartitionBlocks))

	_, _, err = s.SetConflicting(ctx, []chainhash.Hash{*loser.TxIDChainHash()}, true)
	require.NoError(t, err)

	got, err := s.Get(ctx, parent.TxIDChainHash(), fields.ConflictingChildren)
	require.NoError(t, err)
	require.Len(t, got.ConflictingChildren, 1, "a repeat must not be named twice")
	require.Equal(t, loser.TxID(), got.ConflictingChildren[0].String())
}

// TestConflictingChildrenSurviveTheParentLeavingTheIdentityTable.
//
// A contested parent is very often a MINED transaction, and a mined transaction has no
// identity row: the longest-chain stamp moved it into the membership table. Bookkeeping kept
// on the identity row therefore has nowhere to land, and the note becomes a zero-row update.
// The route from a contested coin to the transactions competing for it is the only route
// conflict resolution has, so losing it loses the conflict.
func TestConflictingChildrenSurviveTheParentLeavingTheIdentityTable(t *testing.T) {
	s, ctx := newTestStore(t)
	require.NoError(t, s.SetBlockHeight(700_101))

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 700_099)
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(parent),
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true})
	require.NoError(t, err)
	require.False(t, identExists(t, s, ctx, parent),
		"the stamp must have moved the parent out of the identity table, or this test proves nothing")

	loser := spendOneOutput(t, s, ctx, parent, 0, 700_101)

	_, _, err = s.SetConflicting(ctx, []chainhash.Hash{*loser.TxIDChainHash()}, true)
	require.NoError(t, err)

	got, err := s.Get(ctx, parent.TxIDChainHash(), fields.ConflictingChildren)
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{*loser.TxIDChainHash()}, got.ConflictingChildren,
		"a mined parent must still name the transaction contesting its coin")
}

// TestConflictingChildrenSurviveAParentSettledByTheIdlessStamp is the same requirement on the
// other move.
//
// A stamp for a block not yet known to be on the longest chain leaves the row in the identity
// table carrying one triple; MarkTransactionsOnLongestChain then settles it and moves it out.
// That move used to refuse a row carrying conflicting children, so the bookkeeping pinned the
// transaction in the mempool table forever -- and the transaction stayed pinned whether or not
// anything ever read the list.
func TestConflictingChildrenSurviveAParentSettledByTheIdlessStamp(t *testing.T) {
	s, ctx := newTestStore(t)
	require.NoError(t, s.SetBlockHeight(700_101))

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 700_099)
	require.NoError(t, err)

	// No OnLongestChain: the row stays in the identity table with one triple.
	_, err = s.SetMinedMulti(ctx, hashes(parent),
		utxo.MinedBlockInfo{BlockID: 43, BlockHeight: 700_100})
	require.NoError(t, err)
	require.True(t, identExists(t, s, ctx, parent))

	loser := spendOneOutput(t, s, ctx, parent, 0, 700_101)

	_, _, err = s.SetConflicting(ctx, []chainhash.Hash{*loser.TxIDChainHash()}, true)
	require.NoError(t, err)

	require.NoError(t, s.MarkTransactionsOnLongestChain(ctx,
		[]chainhash.Hash{*parent.TxIDChainHash()}, true))
	require.False(t, identExists(t, s, ctx, parent),
		"a contested parent must settle like any other, not be pinned by its own bookkeeping")
	require.Equal(t, 1, minedRows(t, s, ctx, parent))

	got, err := s.Get(ctx, parent.TxIDChainHash(), fields.ConflictingChildren)
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{*loser.TxIDChainHash()}, got.ConflictingChildren)
}

// TestConflictingChildrenAnswerForAParentKnownOnlyFromItsCoin is the third read step.
//
// A transaction whose membership window has been dropped is known to this store only through
// one of its own live coins -- which is exactly what a pruned SV Node can say about a parent
// whose block it no longer holds. The contest is keyed on the txid rather than on whichever
// row answered, so it must attach to that answer too. Folding it into the identity read, or
// into the membership read, would have lost it here.
func TestConflictingChildrenAnswerForAParentKnownOnlyFromItsCoin(t *testing.T) {
	s, ctx := newTestStore(t)
	require.NoError(t, s.SetBlockHeight(700_101))

	// The block path, so the parent never has an identity row at all. TWO outputs, because the
	// coin the read answers from has to survive the spend below.
	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	dropped, err := s.dropTxMinedWindowsBelow(ctx, 2_000)
	require.NoError(t, err)
	require.Equal(t, 1, dropped, "the membership window has to be gone for this test to mean anything")

	loser := spendOneOutput(t, s, ctx, parent, 0, 700_101)

	_, _, err = s.SetConflicting(ctx, []chainhash.Hash{*loser.TxIDChainHash()}, true)
	require.NoError(t, err)

	// Neither of the first two steps can answer now, so the coin is the only source left.
	require.False(t, identExists(t, s, ctx, parent))
	require.Equal(t, 0, minedRows(t, s, ctx, parent))

	got, err := s.Get(ctx, parent.TxIDChainHash(), fields.ConflictingChildren)
	require.NoError(t, err)
	require.Equal(t, []uint32{7}, got.BlockIDs, "the block came off the coin row")
	require.Nil(t, got.TxInpoints.ParentTxHashes, "and the coin answer is thin, as it is for a pruned parent")
	require.Equal(t, []chainhash.Hash{*loser.TxIDChainHash()}, got.ConflictingChildren)
}

// TestSetConflictingReadsInputsFromAMinedTransaction.
//
// A transaction that lost a double-spend race is very often mined: it arrives in a block on the
// fork being abandoned, and conflict resolution then has to mark it, read what it spent, and
// hand those spends back so they can be undone. A longest-chain stamp moves it out of tx_ident
// and into tx_mined, so an inputs read that looks only at the identity table reports the
// transaction as not held at all -- and SetConflicting fails rather than resolving the race.
func TestSetConflictingReadsInputsFromAMinedTransaction(t *testing.T) {
	s, ctx := newTestStore(t)
	require.NoError(t, s.SetBlockHeight(700_100))

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 700_000)
	require.NoError(t, err)

	child := spendOneOutput(t, s, ctx, parent, 0, 700_100)

	// The move: stamped into a longest-chain block, so its identity row is gone and its
	// inpoints live on the membership row.
	_, err = s.SetMinedMulti(ctx, hashes(child),
		utxo.MinedBlockInfo{BlockID: 44, BlockHeight: 700_100})
	require.NoError(t, err)
	require.NoError(t, s.MarkTransactionsOnLongestChain(ctx,
		[]chainhash.Hash{*child.TxIDChainHash()}, true))
	require.False(t, identExists(t, s, ctx, child))
	require.Equal(t, 1, minedRows(t, s, ctx, child))

	affected, _, err := s.SetConflicting(ctx, []chainhash.Hash{*child.TxIDChainHash()}, true)
	require.NoError(t, err, "a mined transaction must be markable")

	require.Len(t, affected, 1, "and its spend must come back so conflict resolution can undo it")
	require.Equal(t, parent.TxIDChainHash().String(), affected[0].TxID.String())
	require.Equal(t, uint32(0), affected[0].Vout)

	got, err := s.Get(ctx, child.TxIDChainHash(), fields.Conflicting)
	require.NoError(t, err)
	require.True(t, got.Conflicting, "and the membership row must report the flag")

	pmeta, err := s.Get(ctx, parent.TxIDChainHash(), fields.ConflictingChildren)
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{*child.TxIDChainHash()}, pmeta.ConflictingChildren,
		"the contest is noted on the parent whichever table the child lives in")
}

// TestSpendsMadeByReachesAMinedTransaction. The undo half of the same gap: reversing a
// conflict resolution asks the demoted transaction what it took, and the demoted transaction
// came from a block.
func TestSpendsMadeByReachesAMinedTransaction(t *testing.T) {
	s, ctx := newTestStore(t)
	require.NoError(t, s.SetBlockHeight(700_100))

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 700_000)
	require.NoError(t, err)

	child := spendOneOutput(t, s, ctx, parent, 0, 700_100)

	_, err = s.SetMinedMulti(ctx, hashes(child),
		utxo.MinedBlockInfo{BlockID: 44, BlockHeight: 700_100})
	require.NoError(t, err)
	require.NoError(t, s.MarkTransactionsOnLongestChain(ctx,
		[]chainhash.Hash{*child.TxIDChainHash()}, true))

	made, err := s.SpendsMadeBy(ctx, *child.TxIDChainHash())
	require.NoError(t, err)
	require.Len(t, made, 1)
	require.Equal(t, parent.TxIDChainHash().String(), made[0].TxID.String())
	require.Equal(t, uint32(0), made[0].Vout)
}

// TestSetConflictingRefusesABlockPathMinedRow.
//
// A membership row written by the block path carries NULL inpoints: it records that a
// transaction is in a block, not what the transaction spends. Only a coinbase takes that path
// at the tip, and below the checkpoint nothing conflicts, so such a row can never be a conflict
// participant. Reading it as "spends nothing" would be worse than refusing it -- an empty
// input set makes the cascade report no counter-spender at all, which is the answer that lets
// a double spend through -- so it is a not-found here.
func TestSetConflictingRefusesABlockPathMinedRow(t *testing.T) {
	s, ctx := newTestStore(t)
	require.NoError(t, s.SetBlockHeight(700_100))

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_000, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 9, BlockHeight: 700_000, OnLongestChain: true}))
	require.NoError(t, err)
	require.False(t, identExists(t, s, ctx, tx))
	require.Equal(t, 1, minedRows(t, s, ctx, tx))

	_, _, err = s.SetConflicting(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, true)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound), "got %v", err)
}
