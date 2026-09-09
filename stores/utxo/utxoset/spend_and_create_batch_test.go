package utxoset

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// These tests pin the one promise the batched write path makes: every caller gets the answer
// the single-transaction path would have given it. The deterministic cases drive the batch
// callback directly, as create_batch_test.go does, so the assertions are about what one flush
// does rather than about when the batcher decides to flush. The concurrency cases go through
// the public method with a real batcher.

// batchItem is one SpendAndCreate call queued for a directly driven flush.
func batchItem(tx *bt.Tx, height uint32, opts ...utxo.CreateOption) *spendAndCreateItem {
	options := &utxo.CreateOptions{}
	for _, opt := range opts {
		opt(options)
	}

	return newSpendAndCreateItem(tx, height, options, opts)
}

// flush drives one batch through the callback and collects each item's answer.
func flush(t *testing.T, s *Store, items ...*spendAndCreateItem) []spendAndCreateResult {
	t.Helper()

	s.sendSpendAndCreateBatch(items)

	out := make([]spendAndCreateResult, 0, len(items))

	for _, it := range items {
		select {
		case r := <-it.done:
			out = append(out, r)
		case <-time.After(10 * time.Second):
			t.Fatal("an item was never answered")
		}
	}

	return out
}

// coinCount is how many coin rows the store holds for tx.
func coinCount(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx) int {
	t.Helper()

	h := tx.TxIDChainHash()

	var n int

	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM utxo WHERE leaf = $1 AND txid = $2`, LeafFor(h[:]), h[:]).Scan(&n))

	return n
}

// identityCount is how many identity rows the store holds for tx: one if it is stored, else zero.
func identityCount(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx) int {
	t.Helper()

	h := tx.TxIDChainHash()

	var n int

	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM tx_ident WHERE leaf = $1 AND txid = $2`, LeafFor(h[:]), h[:]).Scan(&n))

	return n
}

// parents creates n independent parents, each with one 5,000 satoshi output, and returns them.
//
// They are written straight through the single create path rather than through a batcher, so
// the setup neither waits on a flush window nor depends on the code under test.
func parents(t *testing.T, s *Store, ctx context.Context, n int, height uint32) []*bt.Tx {
	t.Helper()

	require.NoError(t, s.ensureTxBodyPartition(ctx, height))

	out := make([]*bt.Tx, n)

	for i := range out {
		out[i] = distinctParent(t, uint32(nextParent.Add(1))) //nolint:gosec // test sequence
		require.NoError(t, createDirect(s, ctx, out[i], height))
	}

	return out
}

// nextParent makes every parent in this file unique. mkTx is deterministic, so two calls give
// the same txid, and the second would be refused as already held.
var nextParent atomic.Uint32

// distinctParent is mkTx with a distinct previous output on its input, so its txid is its own.
func distinctParent(t *testing.T, seq uint32) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000001", seq,
		"76a914000000000000000000000000000000000000000088ac", 100000))

	script, err := bscript.NewFromHexString("76a914000000000000000000000000000000000000000088ac")
	require.NoError(t, err)
	tx.AddOutput(&bt.Output{Satoshis: 5_000, LockingScript: script})

	return tx
}

// TestSpendAndCreateBatchAppliesEveryModeInOneFlush covers a batch mixing the three shapes
// production sends: the validator's spend-and-create, the block create pass, and the block spend
// pass. Each item must get the result shape its mode promises.
func TestSpendAndCreateBatchAppliesEveryModeInOneFlush(t *testing.T) {
	s, ctx := newTestStore(t)

	ps := parents(t, s, ctx, 4, 100)

	full := spendOutput(t, ps[0], 0, 2)
	createOnly := spendOutput(t, ps[1], 0, 1)
	spendOnly := spendOutput(t, ps[2], 0, 1)
	full2 := spendOutput(t, ps[3], 0, 1)

	res := flush(t, s,
		batchItem(full, 101),
		batchItem(createOnly, 101, utxo.WithCreateOnly()),
		batchItem(spendOnly, 101, utxo.WithSpendOnly()),
		batchItem(full2, 101),
	)

	require.NoError(t, res[0].err)
	require.NotNil(t, res[0].data)
	require.Len(t, res[0].spends, 1)
	require.NoError(t, res[0].spends[0].Err)
	require.Equal(t, 2, coinCount(t, s, ctx, full))
	require.Equal(t, 0, coinCount(t, s, ctx, ps[0]), "the parent coin was consumed")

	require.NoError(t, res[1].err)
	require.NotNil(t, res[1].data)
	require.Nil(t, res[1].spends, "create-only returns no spend records")
	require.Equal(t, 1, coinCount(t, s, ctx, createOnly))
	require.Equal(t, 1, coinCount(t, s, ctx, ps[1]), "create-only leaves the parent coin alone")

	require.NoError(t, res[2].err)
	require.Nil(t, res[2].data, "spend-only returns no record")
	require.Len(t, res[2].spends, 1)
	require.Equal(t, 0, coinCount(t, s, ctx, ps[2]))
	require.Equal(t, 0, identityCount(t, s, ctx, spendOnly), "spend-only stores nothing of its own")

	require.NoError(t, res[3].err)
	require.Equal(t, 1, coinCount(t, s, ctx, full2))

	// The spend is still the decorate fetch.
	require.Equal(t, uint64(5_000), full.Inputs[0].PreviousTxSatoshis)
	require.Equal(t, uint64(5_000), spendOnly.Inputs[0].PreviousTxSatoshis)
}

// TestSpendAndCreateBatchRejectsAFalseClaimWithoutTouchingItsSiblings is the security property.
// A forged claim about a coin's value must be rejected, must consume nothing, must not be stored,
// and must not disturb the honest transactions that shared its batch.
func TestSpendAndCreateBatchRejectsAFalseClaimWithoutTouchingItsSiblings(t *testing.T) {
	s, ctx := newTestStore(t)

	ps := parents(t, s, ctx, 3, 100)

	honestA := spendOutput(t, ps[0], 0, 1)
	forged := spendOutput(t, ps[1], 0, 1)
	forged.Inputs[0].PreviousTxSatoshis = 5_000_000_000
	honestB := spendOutput(t, ps[2], 0, 1)

	res := flush(t, s, batchItem(honestA, 101), batchItem(forged, 101), batchItem(honestB, 101))

	require.NoError(t, res[0].err)
	require.NoError(t, res[2].err)

	require.ErrorIs(t, res[1].err, errors.ErrUtxoError)
	require.Len(t, res[1].spends, 1)
	require.ErrorIs(t, res[1].spends[0].Err, errors.ErrUtxoHashMismatch)
	require.Nil(t, res[1].data)

	require.Equal(t, 0, identityCount(t, s, ctx, forged), "a rejected transaction is not stored")
	require.Equal(t, 0, coinCount(t, s, ctx, forged))
	require.Equal(t, 1, coinCount(t, s, ctx, ps[1]), "the forged claim consumed nothing")
	require.Equal(t, 0, coinCount(t, s, ctx, ps[0]))
	require.Equal(t, 0, coinCount(t, s, ctx, ps[2]))

	// And the coin is still there for its rightful spender.
	honest := spendOutput(t, ps[1], 0, 1)
	_, spends, err := s.SpendAndCreate(ctx, honest, 101)
	require.NoError(t, err)
	require.NoError(t, spends[0].Err)
}

// TestSpendAndCreateBatchArbitratesADoubleSpendBetweenSiblings: two transactions in ONE flush
// want the same coin. Exactly one may have it, and the loser must be told who won, because
// conflict detection reads that name.
func TestSpendAndCreateBatchArbitratesADoubleSpendBetweenSiblings(t *testing.T) {
	s, ctx := newTestStore(t)

	p := parents(t, s, ctx, 1, 100)[0]

	x := spendOutput(t, p, 0, 1)
	y := spendOutput(t, p, 0, 2)

	res := flush(t, s, batchItem(x, 101), batchItem(y, 101))

	var winner, loser int

	switch {
	case res[0].err == nil && res[1].err != nil:
		winner, loser = 0, 1
	case res[1].err == nil && res[0].err != nil:
		winner, loser = 1, 0
	default:
		t.Fatalf("exactly one must win: %v / %v", res[0].err, res[1].err)
	}

	txs := []*bt.Tx{x, y}

	require.ErrorIs(t, res[loser].err, errors.ErrUtxoError)
	require.Len(t, res[loser].spends, 1)
	require.ErrorIs(t, res[loser].spends[0].Err, errors.ErrSpent)
	require.NotNil(t, res[loser].spends[0].ConflictingTxID)
	require.Equal(t, *txs[winner].TxIDChainHash(), *res[loser].spends[0].ConflictingTxID)

	require.Equal(t, 1, identityCount(t, s, ctx, txs[winner]))
	require.Equal(t, 0, identityCount(t, s, ctx, txs[loser]))
	require.Equal(t, 0, coinCount(t, s, ctx, p))
}

// TestSpendAndCreateBatchLoserOfAnInvalidSiblingWins is the case the deferred re-judgement
// exists for. W contests L's coin but W is itself invalid on another input. Whichever of them
// the statement hands the contested coin to, the outcome must be the one the single path gives:
// W rejected, L accepted. If L is judged only against W's uncommitted delete, L is wrongly
// rejected as a double spend of a transaction that never existed.
func TestSpendAndCreateBatchLoserOfAnInvalidSiblingWins(t *testing.T) {
	s, ctx := newTestStore(t)

	// Both ways round, since which item the DELETE serves first is not specified. Fresh
	// transactions each time: a rejected W carries the store's true values afterwards (the
	// decorate overwrite), so re-offering the same object would no longer be a lie.
	for _, wFirst := range []bool{true, false} {
		ps := parents(t, s, ctx, 2, 100)

		// W spends both parents and lies about the second.
		w := spendOutput(t, ps[0], 0, 1)
		require.NoError(t, w.FromUTXOs(&bt.UTXO{
			TxIDHash: ps[1].TxIDChainHash(), Vout: 0,
			LockingScript: ps[1].Outputs[0].LockingScript, Satoshis: 5_000_000_000,
		}))

		l := spendOutput(t, ps[0], 0, 1)

		order := []*spendAndCreateItem{batchItem(w, 101), batchItem(l, 101)}
		if !wFirst {
			order[0], order[1] = order[1], order[0]
		}

		res := flush(t, s, order...)

		byTx := map[*bt.Tx]spendAndCreateResult{order[0].tx: res[0], order[1].tx: res[1]}

		require.ErrorIs(t, byTx[w].err, errors.ErrUtxoError, "wFirst=%v", wFirst)
		require.Equal(t, 0, identityCount(t, s, ctx, w))
		require.Equal(t, 1, coinCount(t, s, ctx, ps[1]), "the lied-about coin is untouched")

		require.NoError(t, byTx[l].err, "the honest contender must win (wFirst=%v)", wFirst)
		require.Equal(t, 1, identityCount(t, s, ctx, l))
		require.Equal(t, 0, coinCount(t, s, ctx, ps[0]))
	}
}

// TestSpendAndCreateBatchHoldsARepeatedTransactionOnce: the same transaction offered twice in one
// flush. Sequential arrival would store it once and answer the second with ErrTxExists and its
// spends left in place; one flush must say the same.
func TestSpendAndCreateBatchHoldsARepeatedTransactionOnce(t *testing.T) {
	s, ctx := newTestStore(t)

	p := parents(t, s, ctx, 1, 100)[0]
	c := spendOutput(t, p, 0, 2)

	res := flush(t, s, batchItem(c, 101), batchItem(c, 101))

	var stored, repeat int

	switch {
	case res[0].err == nil:
		stored, repeat = 0, 1
	case res[1].err == nil:
		stored, repeat = 1, 0
	default:
		t.Fatalf("one offer must be stored: %v / %v", res[0].err, res[1].err)
	}

	require.NotNil(t, res[stored].data)
	require.ErrorIs(t, res[repeat].err, errors.ErrTxExists)
	require.Nil(t, res[repeat].data)
	require.Len(t, res[repeat].spends, 1)
	require.NoError(t, res[repeat].spends[0].Err, "a repeat is a replay of our own spend, not a double spend")

	require.Equal(t, 1, identityCount(t, s, ctx, c))
	require.Equal(t, 2, coinCount(t, s, ctx, c))
	require.Equal(t, 0, coinCount(t, s, ctx, p))
	var bodies int
	h := c.TxIDChainHash()
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM tx_body WHERE txid = $1`, h[:]).Scan(&bodies))
	require.Equal(t, 1, bodies, "one body row, not two")
}

// TestSpendAndCreateBatchReplaysAnAppliedBatch is crash recovery through the batched path: a
// block interrupted after its batch committed is re-offered whole. Every item must answer
// ErrTxExists with its inputs decorated, and the store must be unchanged.
func TestSpendAndCreateBatchReplaysAnAppliedBatch(t *testing.T) {
	s, ctx := newTestStore(t)

	ps := parents(t, s, ctx, 3, 100)

	first := make([]*spendAndCreateItem, 0, 3)
	for _, p := range ps {
		first = append(first, batchItem(spendOutput(t, p, 0, 1), 101))
	}

	for _, r := range flush(t, s, first...) {
		require.NoError(t, r.err)
	}

	bodies := countRows(t, s, ctx, "tx_body")
	coins := countRows(t, s, ctx, "utxo")

	again := make([]*spendAndCreateItem, 0, 3)
	for _, p := range ps {
		again = append(again, batchItem(spendOutput(t, p, 0, 1), 101))
	}

	for i, r := range flush(t, s, again...) {
		require.ErrorIs(t, r.err, errors.ErrTxExists)
		require.Nil(t, r.data)
		require.Len(t, r.spends, 1)
		require.NoError(t, r.spends[0].Err)
		require.Equal(t, uint64(5_000), again[i].tx.Inputs[0].PreviousTxSatoshis, "the replay still decorates")
	}

	require.Equal(t, bodies, countRows(t, s, ctx, "tx_body"))
	require.Equal(t, coins, countRows(t, s, ctx, "utxo"))
}

// TestSpendAndCreateBatchAnswersANilTransactionAlone: a nil transaction is a caller bug, and the
// single path answers it with a processing error. In a batch it must get that same answer without
// costing its neighbours anything.
func TestSpendAndCreateBatchAnswersANilTransactionAlone(t *testing.T) {
	s, ctx := newTestStore(t)

	p := parents(t, s, ctx, 1, 100)[0]
	c := spendOutput(t, p, 0, 1)

	res := flush(t, s, batchItem(nil, 101), batchItem(c, 101))

	require.ErrorIs(t, res[0].err, errors.ErrProcessing)
	require.Nil(t, res[0].data)
	require.Nil(t, res[0].spends)

	require.NoError(t, res[1].err)
	require.Equal(t, 1, identityCount(t, s, ctx, c))
}

// TestSpendAndCreateBatchSpansPartitions: items in one flush landing in different journal leaves
// and body windows. Both partitions must exist before the transaction opens, or the insert fails
// with "no partition of relation found for row".
func TestSpendAndCreateBatchSpansPartitions(t *testing.T) {
	s, ctx := newTestStore(t)

	ps := parents(t, s, ctx, 2, 10)

	lo := SpendJournalPartitionBlocks - 1
	hi := SpendJournalPartitionBlocks * 3

	res := flush(t, s,
		batchItem(spendOutput(t, ps[0], 0, 1), uint32(lo)),
		batchItem(spendOutput(t, ps[1], 0, 1), uint32(hi)),
	)

	require.NoError(t, res[0].err)
	require.NoError(t, res[1].err)

	var n int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM spend_journal WHERE spent_height IN ($1, $2)`, lo, hi).Scan(&n))
	require.Equal(t, 2, n)
}

// TestSpendAndCreateBatchFallsBackToSingleTransactions: when the batch's own statement fails,
// every item is redone alone and gets the answer it would have had without the batcher.
//
// The failure is induced with a row trigger on the identity table that rejects a second insert
// within one transaction. The batch statement carries three identities and cannot pass it; each
// single transaction carries one and can. So the only way all three end up stored is through the
// fallback.
func TestSpendAndCreateBatchFallsBackToSingleTransactions(t *testing.T) {
	s, ctx := newTestStore(t)

	ps := parents(t, s, ctx, 3, 100)

	_, err := s.pool.Exec(ctx, `
CREATE OR REPLACE FUNCTION batchtest_one_identity_per_tx() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF current_setting('batchtest.seen', true) = '1' THEN
    RAISE EXCEPTION 'batchtest: a second identity row in one transaction';
  END IF;
  PERFORM set_config('batchtest.seen', '1', true);
  RETURN NEW;
END $$;
CREATE TRIGGER batchtest_one_identity_per_tx BEFORE INSERT ON tx_ident
  FOR EACH ROW EXECUTE FUNCTION batchtest_one_identity_per_tx();`)
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = s.pool.Exec(ctx, `DROP TRIGGER IF EXISTS batchtest_one_identity_per_tx ON tx_ident;
		                         DROP FUNCTION IF EXISTS batchtest_one_identity_per_tx()`)
	})

	items := make([]*spendAndCreateItem, 0, 3)
	for _, p := range ps {
		items = append(items, batchItem(spendOutput(t, p, 0, 1), 101))
	}

	for i, r := range flush(t, s, items...) {
		require.NoError(t, r.err)
		require.NotNil(t, r.data)
		require.Len(t, r.spends, 1)
		require.Equal(t, 1, identityCount(t, s, ctx, items[i].tx))
		require.Equal(t, 0, coinCount(t, s, ctx, ps[i]))
	}
}

// TestSpendAndCreateBatchFallbackKeepsARejectedClaimRejected is the security property under
// failure. A forged claim is rejected in the first spend round, and the statement has by then
// overwritten the forged input with the coin's true values. If the batch then fails for an
// unrelated reason and every item is redone alone, the forgery must NOT be among them: redone,
// it would pass the comparison it just failed, and a transaction the validator checked against
// forged values would be stored.
//
// The failure is induced as in TestSpendAndCreateBatchFallsBackToSingleTransactions: a trigger
// that rejects a second identity row in one transaction, which the honest survivors' create
// statement trips.
func TestSpendAndCreateBatchFallbackKeepsARejectedClaimRejected(t *testing.T) {
	s, ctx := newTestStore(t)

	ps := parents(t, s, ctx, 3, 100)

	_, err := s.pool.Exec(ctx, `
CREATE OR REPLACE FUNCTION batchtest_one_identity_per_tx() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF current_setting('batchtest.seen', true) = '1' THEN
    RAISE EXCEPTION 'batchtest: a second identity row in one transaction';
  END IF;
  PERFORM set_config('batchtest.seen', '1', true);
  RETURN NEW;
END $$;
CREATE TRIGGER batchtest_one_identity_per_tx BEFORE INSERT ON tx_ident
  FOR EACH ROW EXECUTE FUNCTION batchtest_one_identity_per_tx();`)
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = s.pool.Exec(ctx, `DROP TRIGGER IF EXISTS batchtest_one_identity_per_tx ON tx_ident;
		                         DROP FUNCTION IF EXISTS batchtest_one_identity_per_tx()`)
	})

	honestA := spendOutput(t, ps[0], 0, 1)
	forged := spendOutput(t, ps[1], 0, 1)
	forged.Inputs[0].PreviousTxSatoshis = 5_000_000_000
	honestB := spendOutput(t, ps[2], 0, 1)

	res := flush(t, s, batchItem(honestA, 101), batchItem(forged, 101), batchItem(honestB, 101))

	// The honest pair got there through the fallback.
	require.NoError(t, res[0].err)
	require.NoError(t, res[2].err)
	require.Equal(t, 1, identityCount(t, s, ctx, honestA))
	require.Equal(t, 1, identityCount(t, s, ctx, honestB))

	// The forgery did not.
	require.ErrorIs(t, res[1].err, errors.ErrUtxoError)
	require.ErrorIs(t, res[1].spends[0].Err, errors.ErrUtxoHashMismatch)
	require.Equal(t, 0, identityCount(t, s, ctx, forged), "a forged claim must never be stored, however the batch ends")
	require.Equal(t, 1, coinCount(t, s, ctx, ps[1]), "the coin it lied about is untouched")
}

// TestStorePlansEveryStatementWithItsParameters pins the pool setting that keeps the wide
// statements linear. PostgreSQL moves a prepared statement to a generic plan after five
// executions, and a generic plan cannot see how long the arrays are or how selective the flag
// masks are. Measured on a 40,000-row coin table, the 500-key spend statement went from 8 ms to
// 1,070 ms at exactly the sixth execution. The setting is what stops that, so its absence is a
// regression even though every functional test would still pass.
func TestStorePlansEveryStatementWithItsParameters(t *testing.T) {
	s, ctx := newTestStore(t)

	var mode string
	require.NoError(t, s.pool.QueryRow(ctx, `SHOW plan_cache_mode`).Scan(&mode))
	require.Equal(t, "force_custom_plan", mode)

	var jit string
	require.NoError(t, s.pool.QueryRow(ctx, `SHOW jit`).Scan(&jit))
	require.Equal(t, "off", jit)
}

// TestSpendAndCreateBatchDeferredForgeryStaysRejected is the case the claim snapshot exists
// for. W contests coin P0 with L and lies about coin P1. L is rejected outright for a frozen
// coin. If L takes P0 in the first round, W has lost to a sibling and is judged again by the
// single path after the batch, and by then the first round has overwritten W's lie about P1
// with the truth. Without its claims restored, W would pass the comparison it failed and be
// stored. Both orderings are tried because which item the DELETE serves first is not specified;
// in the other ordering W is rejected outright and the assertion is the same.
func TestSpendAndCreateBatchDeferredForgeryStaysRejected(t *testing.T) {
	s, ctx := newTestStore(t)

	for _, wFirst := range []bool{true, false} {
		ps := parents(t, s, ctx, 3, 100)
		require.NoError(t, s.FreezeUTXOs(ctx, []*utxo.Spend{{TxID: ps[2].TxIDChainHash(), Vout: 0}}, s.settings))

		w := spendOutput(t, ps[0], 0, 1)
		require.NoError(t, w.FromUTXOs(&bt.UTXO{
			TxIDHash: ps[1].TxIDChainHash(), Vout: 0,
			LockingScript: ps[1].Outputs[0].LockingScript, Satoshis: 5_000_000_000,
		}))

		l := spendOutput(t, ps[0], 0, 1)
		require.NoError(t, l.FromUTXOs(&bt.UTXO{
			TxIDHash: ps[2].TxIDChainHash(), Vout: 0,
			LockingScript: ps[2].Outputs[0].LockingScript, Satoshis: 5_000,
		}))

		order := []*spendAndCreateItem{batchItem(w, 101), batchItem(l, 101)}
		if !wFirst {
			order[0], order[1] = order[1], order[0]
		}

		res := flush(t, s, order...)
		byTx := map[*bt.Tx]spendAndCreateResult{order[0].tx: res[0], order[1].tx: res[1]}

		require.ErrorIs(t, byTx[w].err, errors.ErrUtxoError, "wFirst=%v", wFirst)
		require.Equal(t, 0, identityCount(t, s, ctx, w), "the forgery must never be stored (wFirst=%v)", wFirst)
		require.Equal(t, 1, coinCount(t, s, ctx, ps[1]), "the lied-about coin is untouched")
		require.Equal(t, 1, coinCount(t, s, ctx, ps[0]), "nobody got the contested coin")

		require.ErrorIs(t, byTx[l].err, errors.ErrUtxoError)
		require.Equal(t, 0, identityCount(t, s, ctx, l))
	}
}

// TestSpendAndCreateBatchSplitsByBytes: a flush larger than the byte budget is applied as
// several transactions, and every caller is still answered exactly once.
func TestSpendAndCreateBatchSplitsByBytes(t *testing.T) {
	s, ctx := newTestStore(t)

	saved := spendAndCreateBatchByteBudget
	spendAndCreateBatchByteBudget = 1

	t.Cleanup(func() { spendAndCreateBatchByteBudget = saved })

	ps := parents(t, s, ctx, 5, 100)

	items := make([]*spendAndCreateItem, 0, len(ps))
	for _, p := range ps {
		items = append(items, batchItem(spendOutput(t, p, 0, 1), 101))
	}

	for i, r := range flush(t, s, items...) {
		require.NoError(t, r.err, "item %d", i)
		require.Equal(t, 1, identityCount(t, s, ctx, items[i].tx))
	}
}

// TestSpendAndCreateRefusesACancelledCallerBeforeQueueing: a caller whose context is already
// done gets that error and nothing is written. Once queued, a call is applied and answered
// whatever its context does, which the tests through the public method rely on.
func TestSpendAndCreateRefusesACancelledCallerBeforeQueueing(t *testing.T) {
	s, ctx := newTestStore(t)

	p := parents(t, s, ctx, 1, 100)[0]
	c := spendOutput(t, p, 0, 1)

	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	_, _, err := s.SpendAndCreate(cancelled, c, 101)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 0, identityCount(t, s, ctx, c))
	require.Equal(t, 1, coinCount(t, s, ctx, p))
}

// TestSpendAndCreateAfterCloseIsAnErrorNotAPanic: the batcher's channel is closed by Close, and a
// send on it would panic. A late caller takes the single path and gets the pool's error.
func TestSpendAndCreateAfterCloseIsAnErrorNotAPanic(t *testing.T) {
	s, ctx := newTestStore(t)

	p := parents(t, s, ctx, 1, 100)[0]
	c := spendOutput(t, p, 0, 1)

	require.NoError(t, s.Close(ctx))

	_, _, err := s.SpendAndCreate(ctx, c, 101)
	require.Error(t, err)
}

// TestSpendPlanRowsAreSortedAndStillOwned: rows leave planSpends in one global order, and
// every row still knows which item and which input it belongs to.
func TestSpendPlanRowsAreSortedAndStillOwned(t *testing.T) {
	a := mkTx(t, 1, 1)
	require.NoError(t, a.From("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", 3,
		"76a914000000000000000000000000000000000000000088ac", 1))

	b := mkTx(t, 1, 2)

	plan := planSpends([]*spendItem{{tx: a, blockHeight: 1}, {tx: b, blockHeight: 1}})
	require.Len(t, plan.owner, 3)

	for k := 1; k < len(plan.owner); k++ {
		x, y := k-1, k
		less := plan.leaves[x] < plan.leaves[y] ||
			(plan.leaves[x] == plan.leaves[y] && string(plan.ukeys[x][:]) <= string(plan.ukeys[y][:]))
		require.True(t, less, "rows %d and %d out of order", x, y)
		require.Equal(t, int32(k), plan.idx[k]) //nolint:gosec // test
	}

	for k, item := range plan.owner {
		in := plan.itemTxs[item].Inputs[plan.ownerVin[k]]
		require.Equal(t, Pack(in.PreviousTxIDChainHash()[:], in.PreviousTxOutIndex), plan.ukeys[k])
	}
}

// TestSpendAndCreateThroughTheBatcherUnderContention goes through the public method with a real,
// concurrent batcher and a population of competing spends spread across goroutines, so that
// double spends land both inside one flush and across flushes running at once. Every coin must
// end up with exactly one spender and every loser must be told who it was.
func TestSpendAndCreateThroughTheBatcherUnderContention(t *testing.T) {
	s, ctx := newTestStoreWith(t, func(st *settings.Settings) {
		st.UtxoStore.StoreBatcherSize = 32
		st.UtxoStore.StoreBatcherDurationMillis = 2
		st.UtxoStore.BatcherMaxConcurrent = 4
		st.BatcherBackground = true
	})

	require.NotNil(t, s.spendAndCreateBatcher)

	const (
		coins      = 120
		contenders = 3
	)

	ps := parents(t, s, ctx, coins, 100)

	type outcome struct {
		tx     *bt.Tx
		spends []*utxo.Spend
		err    error
	}

	results := make([][]outcome, coins)

	var wg sync.WaitGroup

	for i, p := range ps {
		results[i] = make([]outcome, contenders)

		for c := 0; c < contenders; c++ {
			tx := spendOutput(t, p, 0, 1+c)

			wg.Add(1)

			go func(i, c int, tx *bt.Tx) {
				defer wg.Done()

				_, spends, err := s.SpendAndCreate(ctx, tx, 101)
				results[i][c] = outcome{tx: tx, spends: spends, err: err}
			}(i, c, tx)
		}
	}

	wg.Wait()

	for i, rs := range results {
		var winner *bt.Tx

		for _, r := range rs {
			if r.err == nil {
				require.Nil(t, winner, "coin %d has two winners", i)
				winner = r.tx
			}
		}

		require.NotNil(t, winner, "coin %d has no winner", i)

		for _, r := range rs {
			if r.err == nil {
				continue
			}

			require.ErrorIs(t, r.err, errors.ErrUtxoError, "coin %d", i)
			require.Len(t, r.spends, 1)
			require.ErrorIs(t, r.spends[0].Err, errors.ErrSpent)
			require.NotNil(t, r.spends[0].ConflictingTxID, "coin %d: the loser must be told who won", i)
			require.Equal(t, *winner.TxIDChainHash(), *r.spends[0].ConflictingTxID, "coin %d", i)
			require.Equal(t, 0, identityCount(t, s, ctx, r.tx))
		}

		require.Equal(t, 1, identityCount(t, s, ctx, winner))
		require.Equal(t, 0, coinCount(t, s, ctx, ps[i]))
	}

	require.Equal(t, coins, countRows(t, s, ctx, "tx_ident")-coins, "exactly one child stored per coin")
}

// TestSpendAndCreateThroughTheBatcherPreservesDependencyOrdering: a caller that waits for the
// parent before offering the child must always find the parent committed, whichever flush each
// landed in.
func TestSpendAndCreateThroughTheBatcherPreservesDependencyOrdering(t *testing.T) {
	s, ctx := newTestStoreWith(t, func(st *settings.Settings) {
		st.UtxoStore.StoreBatcherSize = 16
		st.UtxoStore.StoreBatcherDurationMillis = 1
		st.UtxoStore.BatcherMaxConcurrent = 4
		st.BatcherBackground = true
	})

	const chains = 40

	var wg sync.WaitGroup

	failures := make(chan error, chains*4)

	for i := 0; i < chains; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			tx := distinctParent(t, uint32(nextParent.Add(1))) //nolint:gosec // test sequence
			tx.Outputs[0].Satoshis = 50_000

			if _, _, err := s.SpendAndCreate(ctx, tx, 100, utxo.WithCreateOnly()); err != nil {
				failures <- err
				return
			}

			for depth := 0; depth < 4; depth++ {
				child := spendOutput(t, tx, 0, 1)
				child.Outputs[0].Satoshis = 5_000

				if _, spends, err := s.SpendAndCreate(ctx, child, uint32(101+depth)); err != nil {
					failures <- errors.NewProcessingError("depth %d (spends %v)", depth, spends, err)
					return
				}

				tx = child
			}
		}()
	}

	wg.Wait()
	close(failures)

	for err := range failures {
		t.Error(err)
	}
}

// TestSpendAndCreateBatcherCloseAnswersEveryQueuedCaller: a store closed with work queued must
// still answer it, and must not write into a pool that is already gone.
func TestSpendAndCreateBatcherCloseAnswersEveryQueuedCaller(t *testing.T) {
	s, ctx := newTestStoreWith(t, func(st *settings.Settings) {
		st.UtxoStore.StoreBatcherSize = 1000
		st.UtxoStore.StoreBatcherDurationMillis = 60_000
		st.BatcherBackground = true
	})

	ps := parents(t, s, ctx, 8, 100)

	var wg sync.WaitGroup

	errs := make([]error, len(ps))

	for i, p := range ps {
		wg.Add(1)

		go func(i int, p *bt.Tx) {
			defer wg.Done()

			_, _, errs[i] = s.SpendAndCreate(ctx, spendOutput(t, p, 0, 1), 101)
		}(i, p)
	}

	// The window is a minute, so nothing has flushed; Close must drain it.
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, s.Close(ctx))

	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "caller %d", i)
	}
}
