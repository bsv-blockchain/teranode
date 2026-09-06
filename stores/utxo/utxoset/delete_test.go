package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
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
// An identity row removed while its coins survive is a live output nothing can ever reclaim,
// because reclaim finds coins through their identity row.
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
		"every coin must go")
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
// would restore a coin whose identity row no longer exists: spendable, invisible to reclaim,
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
// transactions' coins, and the offline rewind tool unspends BEFORE it deletes. Destroying them
// here would turn an ordering mistake into unrecoverable coin loss.
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
// Coins are found by a range over the packed key, whose first 12 bytes are the transaction id
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
// until its window aged out on its own, rather than going with everything else Delete removes.
func TestDeleteRemovesMembershipRows(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	require.NoError(t, s.Delete(ctx, tx.TxIDChainHash()))
	require.Equal(t, 0, minedRows(t, s, ctx, tx))
	require.Equal(t, 0, coinCount(t, s, ctx, tx))
	h := tx.TxIDChainHash()
	require.Equal(t, 0, countWhere(t, s, ctx, `SELECT count(*) FROM tx_body WHERE txid = $1`, h[:]),
		"a mined-only transaction's body must not survive Delete")
}
