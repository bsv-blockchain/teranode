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
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// waitForLockWait polls until some backend running a statement containing fragment is waiting
// on a lock. A sleep would let the test pass by accident when the statement had not reached
// the lock yet; this proves it is actually blocked before the test moves on.
func waitForLockWait(t *testing.T, s *Store, ctx context.Context, fragment string) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)

	for time.Now().Before(deadline) {
		var n int
		require.NoError(t, s.pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_stat_activity
			 WHERE wait_event_type = 'Lock' AND position($1 in query) > 0`, fragment).Scan(&n))

		if n > 0 {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("no backend running %q ever waited on a lock", fragment)
}

// TestSetConflictingSeesAChildWhoseSpendCommitsWhileItWaits.
//
// The demotion walk asks SetConflicting for the transactions that spent a loser's outputs, and
// marks them next. A child whose spend was in flight when SetConflicting(parent) started holds
// the parent UTXO's row lock, so the flag UPDATE waits for it. When the child commits, the row
// is gone and the UPDATE skips it. The journal read that names the children then has to see
// the child's journal row, which committed after the statement began. In one statement it
// could not: every part of a statement reads the snapshot taken at its start, and READ
// COMMITTED's re-check after a lock wait covers only the row being updated. The parent became
// conflicting and the child stayed spendable, unmarked, with nothing left to find it.
//
// The child's spend runs through the real spend statement inside a transaction the test holds
// open, so it takes exactly the locks a live spend takes.
func TestSetConflictingSeesAChildWhoseSpendCommitsWhileItWaits(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)
	_, err = s.Create(ctx, child, 101)
	require.NoError(t, err)

	require.NoError(t, s.ensureSpendJournalPartition(ctx, 101))

	spendTx, err := s.pool.Begin(ctx)
	require.NoError(t, err)

	defer func() { _ = spendTx.Rollback(context.Background()) }()

	plan := planSpends([]*spendItem{{tx: child, blockHeight: 101}})
	require.NoError(t, s.runSpendPlan(ctx, spendTx, plan))

	for _, sp := range plan.perItem[0] {
		require.NoError(t, sp.Err)
	}

	type result struct {
		marked []chainhash.Hash
		err    error
	}

	done := make(chan result, 1)

	go func() {
		_, marked, merr := utxo.MarkConflictingRecursively(ctx, s, []chainhash.Hash{*parent.TxIDChainHash()})
		done <- result{marked, merr}
	}()

	// The flag statement is blocked on the parent UTXO the child's spend has deleted.
	waitForLockWait(t, s, ctx, "UPDATE utxo u")

	require.NoError(t, spendTx.Commit(ctx))

	var r result

	select {
	case r = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("SetConflicting did not finish after the child's spend committed")
	}

	require.NoError(t, r.err)
	require.Contains(t, r.marked, *child.TxIDChainHash(),
		"the child spent the parent's output before the mark finished, so the walk must reach it")

	got, err := s.Get(ctx, child.TxIDChainHash(), fields.Conflicting)
	require.NoError(t, err)
	require.True(t, got.Conflicting, "a descendant of a conflicting transaction must be conflicting")

	// And the child's own outputs are refused, which is what the mark is for.
	grandchild := spendOutput(t, child, 0, 1)
	spends, err := spendOnly(ctx, s, grandchild, 102)
	require.Error(t, err)
	require.True(t, errors.Is(spends[0].Err, errors.ErrTxConflicting), "got %v", spends[0].Err)
}

// TestSpendWaitingOnSetConflictingIsRefused is the reverse race. A spend that reaches the parent
// UTXO while SetConflicting holds its row lock waits, and when SetConflicting commits, READ
// COMMITTED re-evaluates the spend's DELETE predicate against the new row version, whose
// conflicting bit now fails the flag mask. The spend is refused rather than taking a UTXO of a
// transaction that was just marked a loser. A spend that arrives after the commit sees the bit
// directly, which TestSetConflictingStopsTheUTXOsBeingSpent covers.
func TestSpendWaitingOnSetConflictingIsRefused(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 1)
	_, err = s.Create(ctx, child, 101)
	require.NoError(t, err)

	require.NoError(t, s.ensureSpendJournalPartition(ctx, 101))

	// Hold SetConflicting's own statements open in a transaction, so the UTXO row lock is held
	// exactly as SetConflicting holds it between its update and its commit.
	ph := *parent.TxIDChainHash()
	named := []chainhash.Hash{ph}

	inpoints, err := s.readConflictingInputs(ctx, named)
	require.NoError(t, err)

	cplan := s.planConflicting(named, inpoints)

	markTx, err := s.pool.Begin(ctx)
	require.NoError(t, err)

	defer func() { _ = markTx.Rollback(context.Background()) }()

	_, _, err = s.runConflictingPlan(ctx, markTx, cplan, true)
	require.NoError(t, err)

	done := make(chan error, 1)

	go func() {
		spends, serr := spendOnly(ctx, s, child, 101)
		if serr == nil {
			done <- nil
			return
		}

		done <- spends[0].Err
	}()

	waitForLockWait(t, s, ctx, "DELETE FROM utxo u")

	require.NoError(t, markTx.Commit(ctx))

	select {
	case serr := <-done:
		require.Error(t, serr, "the spend must not take a UTXO that became conflicting while it waited")
		require.True(t, errors.Is(serr, errors.ErrTxConflicting), "got %v", serr)
	case <-time.After(30 * time.Second):
		t.Fatal("the spend did not finish after SetConflicting committed")
	}
}

// TestSetConflictingRetriesADeadlock. SetConflicting holds row locks across one statement per
// leaf group, and within each statement it takes the UTXO rows before the identity rows. A
// caller that takes the same rows in the other order, as setLockedOnce does, can deadlock with
// it, and PostgreSQL then stops one of the two with SQLSTATE 40P01. Conflict resolution must not
// fail on that: SetConflicting runs the whole transaction again.
//
// The deadlock is built by hand. The holder locks the higher leaf's UTXO rows, SetConflicting
// flips the lower leaf and then waits on those rows, and the holder then asks for the lower
// leaf's identity row, which SetConflicting holds. The holder's own deadlock_timeout is raised so
// SetConflicting, which started waiting first, is the side PostgreSQL stops.
func TestSetConflictingRetriesADeadlock(t *testing.T) {
	s, ctx := newTestStore(t)

	var low, high *bt.Tx

	for sats := uint64(5_000); low == nil || high == nil; sats++ {
		tx := mkTx(t, 2, sats)

		switch leaf := LeafFor(tx.TxIDChainHash()[:]); {
		case leaf == 0 && low == nil:
			low = tx
		case leaf == 1 && high == nil:
			high = tx
		}
	}

	for _, tx := range []*bt.Tx{low, high} {
		_, err := s.Create(ctx, tx, 100)
		require.NoError(t, err)
	}

	before := testutil.ToFloat64(deadlockRetried.WithLabelValues("set_conflicting"))

	holder, err := s.pool.Begin(ctx)
	require.NoError(t, err)

	defer func() { _ = holder.Rollback(context.Background()) }()

	// Only a superuser may set deadlock_timeout, and the docker-compose roles are not superusers.
	// Without it the victim is still SetConflicting: PostgreSQL runs the deadlock check in the
	// backend whose deadlock_timeout runs out first, and SetConflicting started waiting first.
	// The raised timeout only widens that margin.
	var superuser string
	require.NoError(t, holder.QueryRow(ctx, `SELECT current_setting('is_superuser')`).Scan(&superuser))

	if superuser == "on" {
		_, err = holder.Exec(ctx, `SET LOCAL deadlock_timeout = '30s'`)
		require.NoError(t, err)
	} else {
		t.Log("not a superuser, so the holder keeps the default deadlock_timeout")
	}

	_, err = holder.Exec(ctx, `UPDATE utxo SET flags = flags WHERE leaf = 1 AND txid = $1`, high.TxIDChainHash()[:])
	require.NoError(t, err)

	done := make(chan error, 1)

	go func() {
		_, _, serr := s.SetConflicting(ctx, []chainhash.Hash{*low.TxIDChainHash(), *high.TxIDChainHash()}, true)
		done <- serr
	}()

	waitForLockWait(t, s, ctx, "UPDATE utxo u")

	// Blocks on the identity row SetConflicting flipped, closing the cycle. It returns once
	// PostgreSQL has stopped SetConflicting's first attempt.
	_, err = holder.Exec(ctx, `UPDATE tx_ident SET flags = flags WHERE leaf = 0 AND txid = $1`, low.TxIDChainHash()[:])
	require.NoError(t, err)
	require.NoError(t, holder.Commit(ctx))

	select {
	case serr := <-done:
		require.NoError(t, serr, "a deadlock is retried, not returned")
	case <-time.After(30 * time.Second):
		t.Fatal("SetConflicting did not finish after the holder committed")
	}

	require.GreaterOrEqual(t, testutil.ToFloat64(deadlockRetried.WithLabelValues("set_conflicting"))-before, 1.0,
		"the first attempt must have been stopped as a deadlock and run again")

	for _, tx := range []*bt.Tx{low, high} {
		got, gerr := s.Get(ctx, tx.TxIDChainHash(), fields.Conflicting)
		require.NoError(t, gerr)
		require.True(t, got.Conflicting, "the retry flags every named transaction")
	}

	var flagged int
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM utxo WHERE flags & $1::smallint <> 0`, FlagConflicting).Scan(&flagged))
	require.Equal(t, 4, flagged, "both outputs of both transactions")
}

// TestSetConflictingSkipsRowsAlreadyAtTheValue. SetConflicting writes only the rows whose bit
// changes, so a mark of a transaction already marked, and a clear of one already clear, take no
// row lock on it. A holder keeps every row of both transactions locked, and both calls must
// still finish. Without the skip each would wait on the holder until its deadline.
func TestSetConflictingSkipsRowsAlreadyAtTheValue(t *testing.T) {
	s, ctx := newTestStore(t)

	// marked is mined, so its flag lives on its containment row and the membership arm is
	// exercised; unmarked is not, so the identity arm is. marked is above the checkpoint, where a
	// mined create keeps its inpoints.
	marked := mkTx(t, 2, 7_001)
	unmarked := mkTx(t, 2, 7_002)

	_, err := s.Create(ctx, marked, 1_000_000, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1_000_000}))
	require.NoError(t, err)

	_, err = s.Create(ctx, unmarked, 100)
	require.NoError(t, err)

	var minedRows int
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM tx_mined WHERE txid = $1`,
		marked.TxIDChainHash()[:]).Scan(&minedRows))
	require.Positive(t, minedRows, "the mined transaction has a containment row")

	_, _, err = s.SetConflicting(ctx, []chainhash.Hash{*marked.TxIDChainHash()}, true)
	require.NoError(t, err)

	holder, err := s.pool.Begin(ctx)
	require.NoError(t, err)

	defer func() { _ = holder.Rollback(context.Background()) }()

	for _, tx := range []*bt.Tx{marked, unmarked} {
		txid := tx.TxIDChainHash()[:]

		for _, stmt := range []string{
			`UPDATE utxo SET flags = flags WHERE txid = $1`,
			`UPDATE tx_ident SET flags = flags WHERE txid = $1`,
			`UPDATE tx_mined SET flags = flags WHERE txid = $1`,
		} {
			_, err = holder.Exec(ctx, stmt, txid)
			require.NoError(t, err)
		}
	}

	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, _, err = s.SetConflicting(callCtx, []chainhash.Hash{*marked.TxIDChainHash()}, true)
	require.NoError(t, err, "marking a transaction already marked must not wait on its rows")

	_, _, err = s.SetConflicting(callCtx, []chainhash.Hash{*unmarked.TxIDChainHash()}, false)
	require.NoError(t, err, "clearing a transaction already clear must not wait on its rows")

	require.NoError(t, holder.Rollback(ctx))

	for tx, want := range map[*bt.Tx]bool{marked: true, unmarked: false} {
		got, gerr := s.Get(ctx, tx.TxIDChainHash(), fields.Conflicting)
		require.NoError(t, gerr)
		require.Equal(t, want, got.Conflicting)
	}
}
