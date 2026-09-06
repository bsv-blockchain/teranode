package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

// TestUnspendRestoresACoinWithItsParentsBlockFactsFromMembership: the journal carries no
// block facts (they are mutable, and the journal payload is not); the restore reads them
// from tx_mined, where the parent's row is present for as long as any of its spends can be
// undone.
func TestUnspendRestoresACoinWithItsParentsBlockFactsFromMembership(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	child := spendOneOutput(t, s, ctx, parent, 0, 700_150)

	spends, err := utxo.GetSpends(child)
	require.NoError(t, err)
	require.NoError(t, s.Unspend(ctx, spends))

	h, b := coinFacts(t, s, ctx, parent)
	require.Equal(t, int32(700_100), h)
	require.Equal(t, int32(42), b)
}

// TestUnspendOfAnAlreadyRestoredCoinIsANoOp pins the fix for the idempotent-replay gap: a
// second Unspend on an outpoint the first call already restored must succeed without
// creating a duplicate coin, even when the replayed request names a different spender than
// whatever actually did the restoring. This is exactly the shape BlockAssembler's
// conflict-intent WAL replay can produce -- a crash between a successful Unspend and its
// intent's completion record means replay calls Unspend again, and it may not remember (or
// may misremember) which spending transaction the original call used. Ownership only gates
// consuming the journal row; once the coin is live, the coin being unspent is the fact that
// matters, not who put it there.
func TestUnspendOfAnAlreadyRestoredCoinIsANoOp(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	child := spendOneOutput(t, s, ctx, parent, 0, 700_150)

	realSpends, err := utxo.GetSpends(child)
	require.NoError(t, err)
	require.NoError(t, s.Unspend(ctx, realSpends))
	require.Equal(t, 1, coinCount(t, s, ctx, parent), "the coin must be restored exactly once")

	// A replayed Unspend naming a spender that never actually spent this outpoint.
	fakeSpender := chainhash.HashH([]byte("not-the-real-spender"))
	fakeSpends := []*utxo.Spend{{
		TxID:         parent.TxIDChainHash(),
		Vout:         0,
		SpendingData: spend.NewSpendingData(&fakeSpender, 0),
	}}

	require.NoError(t, s.Unspend(ctx, fakeSpends),
		"re-unspending an already-restored coin must be a no-op even under a different claimed spender")
	require.Equal(t, 1, coinCount(t, s, ctx, parent), "a replayed restore must not create a second coin")
}

// coinFlagsOf reads the flag byte off one live coin, located exactly.
func coinFlagsOf(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx, vout uint32) int16 {
	t.Helper()

	h := tx.TxIDChainHash()

	var flags int16
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT flags FROM utxo WHERE leaf = $1 AND ukey = $2 AND txid = $3`,
		LeafFor(h[:]), Pack(h[:], vout), h[:]).Scan(&flags))

	return flags
}

// TestUnspendLocksACoinItFoundAlreadyLive is the hold half of the restore, and it was missing
// for exactly the coins conflict resolution needs it for.
//
// Unspend(spends, true) means "put these coins back AND hold them", and the hold is what stops
// anyone else spending a contested parent while the resolution decides which child gets it. The
// flag was ORed only into rows the restore INSERTED, so a parent whose coin was already live --
// which is what a crash between the unspend and the lock leaves, and exactly the case
// SetConflicting now reports -- came back from step 2 unheld, stayed spendable for the whole of
// steps 2 to 5, and then had step 5's SetLocked(false) applied to it anyway. If it had been
// locked for some unrelated reason, that step dropped the unrelated lock too.
//
// The sql reference locks the transaction row unconditionally, which is why it has never had
// this gap.
func TestUnspendLocksACoinItFoundAlreadyLive(t *testing.T) {
	s, ctx := newTestStore(t)

	// A block-path parent: its coin is live and nothing has ever spent it, so there is no
	// journal row for the restore to consume.
	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 700_100, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 700_100, OnLongestChain: true}))
	require.NoError(t, err)

	// The child that contests it, marked conflicting. SetConflicting names the parent because
	// its coin is live, which is the record ProcessConflicting hands to Unspend at step 2.
	child := spendOutput(t, parent, 0, 1)
	_, err = s.Create(ctx, child, 700_101)
	require.NoError(t, err)

	affected, _, err := s.SetConflicting(ctx, []chainhash.Hash{*child.TxIDChainHash()}, true)
	require.NoError(t, err)
	require.Len(t, affected, 1, "the live parent must be named, or there is nothing to hold")

	require.NoError(t, s.Unspend(ctx, affected, true))

	require.Equal(t, 1, coinCount(t, s, ctx, parent), "nothing was restored, and nothing duplicated")
	require.NotZero(t, coinFlagsOf(t, s, ctx, parent, 0)&FlagLocked,
		"a coin the restore found already live must still be held")

	// And the driver's step 5 releases it, which is the whole point of naming it.
	require.NoError(t, s.SetLocked(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, false))
	require.Zero(t, coinFlagsOf(t, s, ctx, parent, 0)&FlagLocked)
}

// TestUnspendWithoutTheHoldLeavesALiveCoinAlone is the other half: Unspend(spends) with no hold
// asked for must not invent one. A reorg restore has no business locking anything.
func TestUnspendWithoutTheHoldLeavesALiveCoinAlone(t *testing.T) {
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
	require.Zero(t, coinFlagsOf(t, s, ctx, parent, 0)&FlagLocked)
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
// block_id still carrying whatever it last held -- must restore the coin as fully
// unconfirmed, not as a height with no matching block.
func TestUnspendGatesTheJournalsBlockFactsOnASingleSharedCondition(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 700_100)
	require.NoError(t, err)

	ph := parent.TxIDChainHash()
	spender := chainhash.HashH([]byte("spender"))

	// The coin is spent by hand, outside the normal spend path, so the journal row below is
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

	h, b := coinFacts(t, s, ctx, parent)
	require.Zero(t, h, "a negative journal height must never surface as this coin's mined height")
	require.Zero(t, b, "and its paired block id must be zeroed with it, not restored on its own")
}
