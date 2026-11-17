package utxoset

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// countWhere is a scalar count, for asserting what a delete did and did not reach.
func countWhere(t *testing.T, s *Store, ctx context.Context, sql string, args ...any) int {
	t.Helper()

	var n int
	require.NoError(t, s.pool.QueryRow(ctx, sql, args...).Scan(&n))

	return n
}

// TestDeleteRemovesEveryTraceOfATransaction. Deleting has to reach all four tables at once.
// An identity row removed while its UTXOs survive is a live output nothing can ever reclaim,
// because reclaim finds UTXOs through their identity row.
func TestDeleteRemovesEveryTraceOfATransaction(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 3, 5_000)
	_, err := s.Create(ctx, tx, 100)
	require.NoError(t, err)

	h := tx.TxIDChainHash()

	require.Equal(t, 3, countWhere(t, s, ctx, `SELECT count(*) FROM utxo WHERE txid = $1`, h[:]))
	require.Equal(t, 1, countWhere(t, s, ctx, `SELECT count(*) FROM tx_body WHERE txid = $1`, h[:]))

	require.NoError(t, s.Delete(ctx, h))

	require.Equal(t, 0, countWhere(t, s, ctx, `SELECT count(*) FROM utxo WHERE txid = $1`, h[:]),
		"every UTXO must go")
	require.Equal(t, 0, countWhere(t, s, ctx, `SELECT count(*) FROM tx_ident WHERE txid = $1`, h[:]),
		"the identity row must go")
	require.Equal(t, 0, countWhere(t, s, ctx, `SELECT count(*) FROM tx_body WHERE txid = $1`, h[:]),
		"the serialized bytes must go")

	_, gerr := s.Get(ctx, h)
	require.True(t, errors.Is(gerr, errors.ErrTxNotFound), "and the store must no longer hold it, got %v", gerr)
}

// TestDeleteOfATransactionTheStoreDoesNotHoldSucceeds. Absence is SUCCESS, which is what both
// reference stores do and what every caller assumes. Block assembly's reorg tolerates only a
// not-found error, so returning anything else here aborts a reorg.
func TestDeleteOfATransactionTheStoreDoesNotHoldSucceeds(t *testing.T) {
	s, ctx := newTestStore(t)

	absent := mkTx(t, 1, 9_999)
	require.NoError(t, s.Delete(ctx, absent.TxIDChainHash()),
		"a transaction the store never held deletes nothing, and that is not an error")
}

// TestDeleteThatCannotStartLeavesTheTransaction. A Delete whose context is already done cannot
// open its database transaction. It must say so as a storage error, not as success or not-found,
// and remove nothing.
func TestDeleteThatCannotStartLeavesTheTransaction(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 2, 5_001)
	_, err := s.Create(ctx, tx, 100)
	require.NoError(t, err)

	h := tx.TxIDChainHash()

	done, cancel := context.WithCancel(ctx)
	cancel()

	derr := s.Delete(done, h)
	require.True(t, errors.Is(derr, errors.ErrStorageError), "a Delete that never started is a storage error, got %v", derr)

	require.Equal(t, 2, countWhere(t, s, ctx, `SELECT count(*) FROM utxo WHERE txid = $1`, h[:]),
		"and every UTXO is still there")
	require.Equal(t, 1, countWhere(t, s, ctx, `SELECT count(*) FROM tx_ident WHERE txid = $1`, h[:]),
		"and so is the identity row")
}

// TestDeleteIsIdempotent, for the same reason: a reorg retried after a crash must not fail on
// the work it already did.
func TestDeleteIsIdempotent(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, tx, 100)
	require.NoError(t, err)

	h := tx.TxIDChainHash()

	require.NoError(t, s.Delete(ctx, h))
	require.NoError(t, s.Delete(ctx, h), "deleting twice must not fail")
}

// TestDeleteRemovesTheUndoRecordsItOwns. The journal rows where the deleted transaction is the
// PARENT are the undo payloads for its already-spent outputs. Left behind, a later unspend
// would restore a UTXO whose identity row no longer exists: spendable, invisible to reclaim,
// and permanent.
func TestDeleteRemovesTheUndoRecordsItOwns(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	ph := parent.TxIDChainHash()

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash:      ph,
		Vout:          0,
		LockingScript: parent.Outputs[0].LockingScript,
		Satoshis:      parent.Outputs[0].Satoshis,
	}))

	spends, err := spendOnly(ctx, s, child, 200)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)

	require.Equal(t, 1, countWhere(t, s, ctx, `SELECT count(*) FROM spend_journal WHERE txid = $1`, ph[:]),
		"the spend must have written an undo record against the parent")

	require.NoError(t, s.Delete(ctx, ph))

	require.Equal(t, 0, countWhere(t, s, ctx, `SELECT count(*) FROM spend_journal WHERE txid = $1`, ph[:]),
		"the undo records for the deleted transaction's own outputs must go with it")
}

// TestDeleteLeavesTheUndoRecordsWhereItWasTheSpender. Those rows authorise restoring OTHER
// transactions' UTXOs, and the offline rewind tool unspends BEFORE it deletes. Destroying them
// here would turn an ordering mistake into unrecoverable UTXO loss.
func TestDeleteLeavesTheUndoRecordsWhereItWasTheSpender(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	ph := parent.TxIDChainHash()

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash:      ph,
		Vout:          0,
		LockingScript: parent.Outputs[0].LockingScript,
		Satoshis:      parent.Outputs[0].Satoshis,
	}))
	child.AddOutput(&bt.Output{Satoshis: 4_000, LockingScript: parent.Outputs[0].LockingScript})

	_, err = s.Create(ctx, child, 200)
	require.NoError(t, err)

	spends, err := spendOnly(ctx, s, child, 200)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)

	ch := child.TxIDChainHash()

	require.Equal(t, 1, countWhere(t, s, ctx,
		`SELECT count(*) FROM spend_journal WHERE spending_txid = $1`, ch[:]))

	require.NoError(t, s.Delete(ctx, ch))

	require.Equal(t, 1, countWhere(t, s, ctx,
		`SELECT count(*) FROM spend_journal WHERE spending_txid = $1`, ch[:]),
		"the undo records this transaction created for OTHER transactions must survive")
}

// TestDeleteDoesNotReachATransactionSharingItsKeyPrefix is the case the full 32-byte recheck
// exists for.
//
// UTXOs are found by a range over the packed key, whose first 12 bytes are the transaction id
// prefix, because that is the only index on the table. The prefix is 96 bits and NON-UNIQUE by
// design, so it can locate a row but never authorise deleting one. The colliding row is planted
// directly, since no amount of test data will produce a 12-byte collision by chance.
func TestDeleteDoesNotReachATransactionSharingItsKeyPrefix(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 100)
	require.NoError(t, err)

	h := tx.TxIDChainHash()

	// Same first 12 bytes, different tail, so it packs into the same key range and the same
	// partition but is a different transaction.
	var twin chainhash.Hash

	copy(twin[:], h[:])
	twin[31] ^= 0xff

	require.Equal(t, h[:12], twin[:12], "the twin must share the prefix, or this test proves nothing")
	require.NotEqual(t, h[:], twin[:])

	ukey := Pack(twin[:], 0)
	_, err = s.pool.Exec(ctx, `
        INSERT INTO utxo (satoshis, created_height, spendable_from, leaf, flags, ukey, txid, script)
        VALUES (1, 100, 0, $1, 0, $2, $3, '\x00')`, LeafFor(twin[:]), ukey, twin[:])
	require.NoError(t, err)

	require.NoError(t, s.Delete(ctx, h))

	require.Equal(t, 0, countWhere(t, s, ctx, `SELECT count(*) FROM utxo WHERE txid = $1`, h[:]))
	require.Equal(t, 1, countWhere(t, s, ctx, `SELECT count(*) FROM utxo WHERE txid = $1`, twin[:]),
		"a different transaction sharing the key prefix must survive")
}

// TestDeleteLeavesOtherTransactionsAlone, the ordinary version of the same rule.
func TestDeleteLeavesOtherTransactionsAlone(t *testing.T) {
	s, ctx := newTestStore(t)

	doomed := mkTx(t, 2, 5_000)
	keeper := mkTx(t, 3, 6_000)

	_, err := s.Create(ctx, doomed, 100)
	require.NoError(t, err)
	_, err = s.Create(ctx, keeper, 100)
	require.NoError(t, err)

	require.NoError(t, s.Delete(ctx, doomed.TxIDChainHash()))

	kh := keeper.TxIDChainHash()
	require.Equal(t, 3, countWhere(t, s, ctx, `SELECT count(*) FROM utxo WHERE txid = $1`, kh[:]))
	require.Equal(t, 1, countWhere(t, s, ctx, `SELECT count(*) FROM tx_ident WHERE txid = $1`, kh[:]))
	require.Equal(t, 1, countWhere(t, s, ctx, `SELECT count(*) FROM tx_body WHERE txid = $1`, kh[:]))
}

// TestDeleteRemovesMembershipRows: the rewind tool deletes a transaction outright; its
// membership rows go with it so a later lookup does not resurrect a deleted transaction.
//
// It also asserts the body, because a block-path transaction has no tx_ident row to gate a
// body join through -- createMinedPlanSQL claims on tx_mined instead -- so a body join that
// only ever looked at what ident deleted left a mined-only transaction's tx_body row behind
// until its partition aged out on its own, rather than going with everything else Delete removes.
func TestDeleteRemovesMembershipRows(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	require.NoError(t, s.Delete(ctx, tx.TxIDChainHash()))
	require.Equal(t, 0, minedRows(t, s, ctx, tx))
	require.Equal(t, 0, utxoCount(t, s, ctx, tx))
	h := tx.TxIDChainHash()
	require.Equal(t, 0, countWhere(t, s, ctx, `SELECT count(*) FROM tx_body WHERE txid = $1`, h[:]),
		"a mined-only transaction's body must not survive Delete")
}

// TestDeleteRetriesADeadlockOnItsJournalRow: Delete takes the transaction's identity and
// membership rows first and removes the undo records for its spent outputs after, so a writer that
// holds one of those undo records and then asks for the identity row crosses it. Unspend of the same transaction is that writer: it consumes the undo
// record first and share-locks the identity row after. The holder plays it, with its own
// deadlock check pushed out where a superuser can do that, so Delete, which started waiting
// first, is normally the one PostgreSQL stops. Delete must run again and succeed rather than hand
// a reorg a storage error it does not tolerate.
func TestDeleteRetriesADeadlockOnItsJournalRow(t *testing.T) {
	before := testutil.ToFloat64(deadlockRetried.WithLabelValues("delete"))

	for attempt := 1; ; attempt++ {
		if deleteCrossesAHolder(t) {
			break
		}

		require.Less(t, attempt, 3, "the holder, not Delete, was the deadlock victim on every attempt")
	}

	require.Greater(t, testutil.ToFloat64(deadlockRetried.WithLabelValues("delete")), before,
		"Delete was stopped as a deadlock and ran again")
}

// deleteCrossesAHolder is one attempt of TestDeleteRetriesADeadlockOnItsJournalRow on a fresh
// store. It reports false when the holder was the victim instead: PostgreSQL checks a lock wait for
// a deadlock once, deadlock_timeout after it starts, so when more than that passes between Delete
// blocking and the holder blocking, Delete's check finds no cycle and the holder's finds it.
func deleteCrossesAHolder(t *testing.T) bool {
	t.Helper()

	s, ctx := newTestStore(t)

	parent, spends := mempoolParentSpent(t, s, ctx, false)
	h := parent.TxIDChainHash()
	ukey := Pack(h[:], spends[0].Vout)

	holder, err := s.pool.Begin(ctx)
	require.NoError(t, err)

	defer func() { _ = holder.Rollback(context.Background()) }()

	var superuser string
	require.NoError(t, holder.QueryRow(ctx, `SELECT current_setting('is_superuser')`).Scan(&superuser))

	if superuser == "on" {
		_, err = holder.Exec(ctx, `SET LOCAL deadlock_timeout = '30s'`)
		require.NoError(t, err)
	} else {
		t.Log("not a superuser, so the holder keeps the default deadlock_timeout")
	}

	_, err = holder.Exec(ctx, `SELECT 1 FROM spend_journal WHERE ukey = $1 AND txid = $2 FOR UPDATE`, ukey, h[:])
	require.NoError(t, err)

	done := make(chan error, 1)

	go func() { done <- s.Delete(ctx, h) }()

	waitForLockWait(t, s, ctx, "WITH k AS")

	_, err = holder.Exec(ctx, `SELECT 1 FROM tx_ident WHERE txid = $1 FOR SHARE`, h[:])
	if isDeadlock(err) {
		require.NoError(t, holder.Rollback(ctx))
		require.NoError(t, <-done)

		return false
	}

	require.NoError(t, err, "the holder waits out Delete's deadlock check and gets the row")
	require.NoError(t, holder.Rollback(ctx))

	require.NoError(t, <-done, "a deadlock is retried, not returned")
	require.Equal(t, 0, countWhere(t, s, ctx, `SELECT count(*) FROM tx_ident WHERE txid = $1`, h[:]))
	require.Equal(t, 0, countWhere(t, s, ctx, `SELECT count(*) FROM spend_journal WHERE txid = $1`, h[:]))

	return true
}

// TestDeleteWaitsForARestoreOfItsCoin: a restore of the transaction's coin that has run and not
// committed holds a share lock on the transaction's identity row. Delete must wait for it and then
// remove the coin it restored. When Delete was one statement, its snapshot was taken before the
// coin committed: it waited on the identity row, deleted that row once the restore committed, and
// left the coin behind, a live output whose transaction no longer exists and that nothing can
// reclaim.
func TestDeleteWaitsForARestoreOfItsCoin(t *testing.T) {
	s, ctx := newTestStore(t)

	parent, spends := mempoolParentSpent(t, s, ctx, false)
	h := parent.TxIDChainHash()

	req, err := newUnspendRequest(spends, false)
	require.NoError(t, err)

	restore, err := s.pool.Begin(ctx)
	require.NoError(t, err)

	defer func() { _ = restore.Rollback(context.Background()) }()

	res, err := s.restoreIn(ctx, restore, req)
	require.NoError(t, err)
	require.Equal(t, 1, res.restored)

	done := make(chan error, 1)

	go func() { done <- s.Delete(ctx, h) }()

	waitForLockWait(t, s, ctx, "tx_ident")
	require.NoError(t, restore.Commit(ctx))
	require.NoError(t, <-done)

	require.Equal(t, 0, countWhere(t, s, ctx, `SELECT count(*) FROM utxo WHERE txid = $1`, h[:]),
		"the coin the restore put back must not outlive its transaction")
	require.Equal(t, 0, countWhere(t, s, ctx, `SELECT count(*) FROM tx_ident WHERE txid = $1`, h[:]))
}

// TestDeleteWaitsForARestoreOfAMinedCoin is TestDeleteWaitsForARestoreOfItsCoin for a block-path
// transaction, which has containment rows in tx_mined and no identity row. The restore
// share-locks the containment rows, so the delete has to take those to wait for it.
func TestDeleteWaitsForARestoreOfAMinedCoin(t *testing.T) {
	s, ctx := newTestStore(t)

	require.NoError(t, s.SetBlockHeight(100))

	parent := mkTx(t, 2, 10_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}))
	require.NoError(t, err)

	h := parent.TxIDChainHash()
	require.Equal(t, 0, countWhere(t, s, ctx, `SELECT count(*) FROM tx_ident WHERE txid = $1`, h[:]), "fixture: no identity row")
	require.Equal(t, 1, countWhere(t, s, ctx, `SELECT count(*) FROM tx_mined WHERE txid = $1`, h[:]), "fixture: one containment row")

	spends, err := spendOnly(ctx, s, spendParentTx(t, parent, nil, 1_000, 0), 101)
	require.NoError(t, err)
	require.Len(t, spends, 1)

	req, err := newUnspendRequest(spends, false)
	require.NoError(t, err)

	restore, err := s.pool.Begin(ctx)
	require.NoError(t, err)

	defer func() { _ = restore.Rollback(context.Background()) }()

	res, err := s.restoreIn(ctx, restore, req)
	require.NoError(t, err)
	require.Equal(t, 1, res.restored)

	done := make(chan error, 1)

	go func() { done <- s.Delete(ctx, h) }()

	// Delete is the only backend that can wait here. "FROM" matches either of its statements:
	// PostgreSQL reports only the first 1024 bytes of a query, and deleteTxSQL names tx_mined
	// past that.
	waitForLockWait(t, s, ctx, "FROM")
	require.NoError(t, restore.Commit(ctx))
	require.NoError(t, <-done)

	require.Equal(t, 0, countWhere(t, s, ctx, `SELECT count(*) FROM utxo WHERE txid = $1`, h[:]),
		"the coin the restore put back must not outlive its transaction")
	require.Equal(t, 0, countWhere(t, s, ctx, `SELECT count(*) FROM tx_mined WHERE txid = $1`, h[:]))
}

// TestDeleteCutWhileWaitingOnARowDeletesNothing: a Delete cut while its lock statement waits on a
// row another transaction holds returns a storage error before it has deleted anything, so the
// transaction's identity row, coins and undo records are all still there. The cut is aimed at the
// lock statement, so this test needs that statement; the restore races above are what show the
// order matters.
func TestDeleteCutWhileWaitingOnARowDeletesNothing(t *testing.T) {
	s, ctx := newTestStore(t)

	parent, _ := mempoolParentSpent(t, s, ctx, false)
	h := parent.TxIDChainHash()

	holder, err := s.pool.Begin(ctx)
	require.NoError(t, err)

	defer func() { _ = holder.Rollback(context.Background()) }()

	_, err = holder.Exec(ctx, `SELECT 1 FROM tx_ident WHERE txid = $1 FOR UPDATE`, h[:])
	require.NoError(t, err)

	cut, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan error, 1)

	go func() { done <- s.Delete(cut, h) }()

	waitForLockWait(t, s, ctx, "FOR UPDATE OF i")
	cancel()

	var derr error

	select {
	case derr = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("Delete did not return after its context was cancelled")
	}

	require.True(t, errors.Is(derr, errors.ErrStorageError), "a Delete cut while it waits is a storage error, got %v", derr)
	require.ErrorContains(t, derr, "[utxoset][Delete] lock", "the cut landed while Delete waited for the rows")

	require.NoError(t, holder.Rollback(ctx))

	require.Equal(t, 1, countWhere(t, s, ctx, `SELECT count(*) FROM tx_ident WHERE txid = $1`, h[:]))
	require.Equal(t, 1, countWhere(t, s, ctx, `SELECT count(*) FROM utxo WHERE txid = $1`, h[:]),
		"the unspent output is still there")
	require.Equal(t, 1, countWhere(t, s, ctx, `SELECT count(*) FROM spend_journal WHERE txid = $1`, h[:]),
		"the undo record of the spent output is still there")
}
