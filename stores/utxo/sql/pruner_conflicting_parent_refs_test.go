package sql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	sqlpruner "github.com/bsv-blockchain/teranode/stores/utxo/sql/pruner"
	"github.com/stretchr/testify/require"
)

// TestPrunerClearsParentRefsOfReapedConflictingLoser verifies the pruner, in defensive mode only,
// removes every reference a surviving parent holds to a conflicting loser before it deletes
// the loser: the parent output spend and the parent conflicting_children entry.
func TestPrunerClearsParentRefsOfReapedConflictingLoser(t *testing.T) {
	for _, defensive := range []bool{false, true} {
		t.Run(fmt.Sprintf("defensive=%v", defensive), func(t *testing.T) {
			ResetPrunerServiceForTests()
			t.Cleanup(ResetPrunerServiceForTests)

			ctx := t.Context()

			store, parent := setup(ctx, t)
			store.settings.Pruner.UTXODefensiveEnabled = defensive

			loser := bt.NewTx()
			require.NoError(t, loser.From(parent.TxIDChainHash().String(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
			require.NoError(t, loser.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", parent.Outputs[0].Satoshis-1000))
			loser.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})

			_, err := store.Create(ctx, parent, 0)
			require.NoError(t, err)

			_, err = store.Create(ctx, loser, 0)
			require.NoError(t, err)

			spends, err := store.Spend(ctx, loser, store.GetBlockHeight()+1)
			require.NoError(t, err)
			require.Len(t, spends, len(loser.Inputs))

			_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*loser.TxIDChainHash()}, true)
			require.NoError(t, err)

			parentSpend := &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0, UTXOHash: spends[0].UTXOHash}

			before, err := store.GetSpend(ctx, parentSpend)
			require.NoError(t, err)
			require.NotNil(t, before.SpendingData, "precondition: the parent output is spent by the loser")

			countChildRefs := func() int {
				var n int

				require.NoError(t, store.db.QueryRowContext(ctx, `
					SELECT COUNT(*) FROM conflicting_children
					WHERE transaction_id = (SELECT id FROM transactions WHERE hash = $1)`, parent.TxIDChainHash()[:]).Scan(&n))

				return n
			}

			require.Equal(t, 1, countChildRefs(), "precondition: the parent lists the loser as a conflicting child")

			provider, ok := any(store).(pruner.PrunerServiceProvider)
			require.True(t, ok)

			prunerSvc, err := provider.GetPrunerService()
			require.NoError(t, err)

			_, err = prunerSvc.Prune(ctx, store.GetBlockHeight()+1000, "test")
			require.NoError(t, err)

			_, err = store.Get(ctx, loser.TxIDChainHash())
			require.ErrorIs(t, err, errors.ErrTxNotFound, "the conflicting loser must be reaped")

			after, err := store.GetSpend(ctx, parentSpend)
			require.NoError(t, err)

			if defensive {
				require.Nil(t, after.SpendingData, "a surviving parent must not name the reaped loser as its spender")
				require.Equal(t, 0, countChildRefs(), "a surviving parent must not list the reaped loser as a conflicting child")
			} else {
				require.NotNil(t, after.SpendingData, "default mode keeps the existing behaviour")
			}
		})
	}
}

// A conflicting loser that has an unstable spending child survives the defensive delete, so the
// references its parent holds to it must survive too.
func TestPrunerKeepsParentRefsOfAConflictingLoserItDoesNotDelete(t *testing.T) {
	ResetPrunerServiceForTests()
	t.Cleanup(ResetPrunerServiceForTests)

	ctx := t.Context()

	store, parent := setup(ctx, t)
	store.settings.Pruner.UTXODefensiveEnabled = true

	loser := bt.NewTx()
	require.NoError(t, loser.From(parent.TxIDChainHash().String(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	require.NoError(t, loser.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", parent.Outputs[0].Satoshis-1000))
	loser.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})

	child := bt.NewTx()
	require.NoError(t, child.From(loser.TxIDChainHash().String(), 0, loser.Outputs[0].LockingScript.String(), loser.Outputs[0].Satoshis))
	require.NoError(t, child.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", loser.Outputs[0].Satoshis-1000))
	child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})

	_, err := store.Create(ctx, parent, 0)
	require.NoError(t, err)

	_, err = store.Create(ctx, loser, 0)
	require.NoError(t, err)

	_, err = store.Create(ctx, child, 0)
	require.NoError(t, err)

	spends, err := store.Spend(ctx, loser, store.GetBlockHeight()+1)
	require.NoError(t, err)
	require.Len(t, spends, len(loser.Inputs))

	_, err = store.Spend(ctx, child, store.GetBlockHeight()+1)
	require.NoError(t, err, "the unmined child spends the loser's output, so it is not stable")

	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*loser.TxIDChainHash()}, true)
	require.NoError(t, err)

	parentSpend := &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0, UTXOHash: spends[0].UTXOHash}

	countChildRefs := func() int {
		var n int

		require.NoError(t, store.db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM conflicting_children
			WHERE transaction_id = (SELECT id FROM transactions WHERE hash = $1)`, parent.TxIDChainHash()[:]).Scan(&n))

		return n
	}

	require.Equal(t, 1, countChildRefs(), "precondition: the parent lists the loser as a conflicting child")

	provider, ok := any(store).(pruner.PrunerServiceProvider)
	require.True(t, ok)

	prunerSvc, err := provider.GetPrunerService()
	require.NoError(t, err)

	_, err = prunerSvc.Prune(ctx, store.GetBlockHeight()+1000, "test")
	require.NoError(t, err)

	_, err = store.Get(ctx, loser.TxIDChainHash())
	require.NoError(t, err, "the loser has an unstable child, so the delete keeps it")

	after, err := store.GetSpend(ctx, parentSpend)
	require.NoError(t, err)
	require.NotNil(t, after.SpendingData, "the kept loser still spends the parent output")
	require.Equal(t, loser.TxIDChainHash().String(), after.SpendingData.TxID.String())
	require.Equal(t, 1, countChildRefs(), "the parent still lists the kept loser as a conflicting child")
}

// A conflicting loser whose only child is itself an expired conflicting loser becomes deletable once
// the child's references are cleared. The delete must not take it in the same cycle, because its own
// parent references were not cleared: it is taken in the next cycle, after they are.
func TestPrunerDeletesOnlyConflictingLosersWhoseParentRefsWereCleared(t *testing.T) {
	ResetPrunerServiceForTests()
	t.Cleanup(ResetPrunerServiceForTests)

	ctx := t.Context()

	store, parent := setup(ctx, t)
	store.settings.Pruner.UTXODefensiveEnabled = true

	middle := bt.NewTx()
	require.NoError(t, middle.From(parent.TxIDChainHash().String(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	require.NoError(t, middle.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", parent.Outputs[0].Satoshis-1000))
	middle.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})

	leaf := bt.NewTx()
	require.NoError(t, leaf.From(middle.TxIDChainHash().String(), 0, middle.Outputs[0].LockingScript.String(), middle.Outputs[0].Satoshis))
	require.NoError(t, leaf.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", middle.Outputs[0].Satoshis-1000))
	leaf.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})

	_, err := store.Create(ctx, parent, 0)
	require.NoError(t, err)

	_, err = store.Create(ctx, middle, 0)
	require.NoError(t, err)

	_, err = store.Create(ctx, leaf, 0)
	require.NoError(t, err)

	spends, err := store.Spend(ctx, middle, store.GetBlockHeight()+1)
	require.NoError(t, err)
	require.Len(t, spends, len(middle.Inputs))

	_, err = store.Spend(ctx, leaf, store.GetBlockHeight()+1)
	require.NoError(t, err)

	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*middle.TxIDChainHash(), *leaf.TxIDChainHash()}, true)
	require.NoError(t, err)

	parentSpend := &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0, UTXOHash: spends[0].UTXOHash}

	countChildRefs := func() int {
		var n int

		require.NoError(t, store.db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM conflicting_children
			WHERE transaction_id = (SELECT id FROM transactions WHERE hash = $1)`, parent.TxIDChainHash()[:]).Scan(&n))

		return n
	}

	provider, ok := any(store).(pruner.PrunerServiceProvider)
	require.True(t, ok)

	prunerSvc, err := provider.GetPrunerService()
	require.NoError(t, err)

	_, err = prunerSvc.Prune(ctx, store.GetBlockHeight()+1000, "test")
	require.NoError(t, err)

	_, err = store.Get(ctx, leaf.TxIDChainHash())
	require.ErrorIs(t, err, errors.ErrTxNotFound, "the conflicting leaf must be reaped")

	after, err := store.GetSpend(ctx, parentSpend)
	require.NoError(t, err)

	if _, getErr := store.Get(ctx, middle.TxIDChainHash()); errors.Is(getErr, errors.ErrTxNotFound) {
		require.Nil(t, after.SpendingData, "a deleted loser must not stay named as the spender of the parent output")
	} else {
		require.NoError(t, getErr)
		require.NotNil(t, after.SpendingData)
		require.Equal(t, middle.TxIDChainHash().String(), after.SpendingData.TxID.String())
		require.Equal(t, 1, countChildRefs(), "the kept loser stays listed as a conflicting child of the parent")
	}

	_, err = prunerSvc.Prune(ctx, store.GetBlockHeight()+1000, "test")
	require.NoError(t, err)

	_, err = store.Get(ctx, middle.TxIDChainHash())
	require.ErrorIs(t, err, errors.ErrTxNotFound, "the next cycle reaps the loser")

	after, err = store.GetSpend(ctx, parentSpend)
	require.NoError(t, err)
	require.Nil(t, after.SpendingData, "the parent output is unspent once the loser is reaped")
	require.Equal(t, 0, countChildRefs())
}

// A conflict promotion that lands after the pruner listed its candidates clears the conflicting flag.
// The reap must keep that record and the spend its parent holds on it.
func TestPrunerKeepsAConflictingLoserPromotedAfterItWasListedForReaping(t *testing.T) {
	ctx := t.Context()

	store, parent := setup(ctx, t)
	store.settings.Pruner.UTXODefensiveEnabled = true

	loser := bt.NewTx()
	require.NoError(t, loser.From(parent.TxIDChainHash().String(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	require.NoError(t, loser.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", parent.Outputs[0].Satoshis-1000))
	loser.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})

	_, err := store.Create(ctx, parent, 0)
	require.NoError(t, err)

	_, err = store.Create(ctx, loser, 0)
	require.NoError(t, err)

	spends, err := store.Spend(ctx, loser, store.GetBlockHeight()+1)
	require.NoError(t, err)
	require.Len(t, spends, len(loser.Inputs))

	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*loser.TxIDChainHash()}, true)
	require.NoError(t, err)

	reapAfterPromotion := func(hookCtx context.Context, txHash chainhash.Hash, blockHeight, safetyWindow uint32) (bool, error) {
		if _, _, conflictErr := store.SetConflicting(hookCtx, []chainhash.Hash{txHash}, false); conflictErr != nil {
			return false, conflictErr
		}

		return store.reapConflicting(hookCtx, txHash, blockHeight, safetyWindow)
	}

	prunerSvc, err := sqlpruner.NewService(store.settings, sqlpruner.Options{
		Logger:          store.logger,
		DB:              store.db,
		Ctx:             ctx,
		ReapConflicting: reapAfterPromotion,
	})
	require.NoError(t, err)

	count, err := prunerSvc.Prune(ctx, store.GetBlockHeight()+1000, "test")
	require.NoError(t, err)
	require.Zero(t, count)

	_, err = store.Get(ctx, loser.TxIDChainHash())
	require.NoError(t, err, "a loser promoted after it was listed for reaping must be kept")

	parentSpend := &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0, UTXOHash: spends[0].UTXOHash}

	after, err := store.GetSpend(ctx, parentSpend)
	require.NoError(t, err)
	require.NotNil(t, after.SpendingData, "the promoted transaction still spends the parent output")
	require.Equal(t, loser.TxIDChainHash().String(), after.SpendingData.TxID.String())
}

// While a conflict intent is pending a promotion may be about to make a conflicting record live, so
// the pruner must keep it and its parent references until the intent completes.
func TestPrunerDefersAConflictingReapWhileAConflictIntentIsPending(t *testing.T) {
	ResetPrunerServiceForTests()
	t.Cleanup(ResetPrunerServiceForTests)

	ctx := t.Context()

	store, parent := setup(ctx, t)
	store.settings.Pruner.UTXODefensiveEnabled = true

	loser := bt.NewTx()
	require.NoError(t, loser.From(parent.TxIDChainHash().String(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	require.NoError(t, loser.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", parent.Outputs[0].Satoshis-1000))
	loser.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})

	_, err := store.Create(ctx, parent, 0)
	require.NoError(t, err)

	_, err = store.Create(ctx, loser, 0)
	require.NoError(t, err)

	spends, err := store.Spend(ctx, loser, store.GetBlockHeight()+1)
	require.NoError(t, err)
	require.Len(t, spends, len(loser.Inputs))

	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*loser.TxIDChainHash()}, true)
	require.NoError(t, err)

	intent := utxo.ConflictIntent{
		Kind:        utxo.ConflictIntentForward,
		BlockHeight: store.GetBlockHeight() + 1,
		BlockHash:   chainhash.Hash{1},
		TxHashes:    []chainhash.Hash{*loser.TxIDChainHash()},
		StartedAt:   1,
	}

	require.NoError(t, store.BeginConflictIntent(ctx, intent))

	parentSpend := &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0, UTXOHash: spends[0].UTXOHash}

	countChildRefs := func() int {
		var n int

		require.NoError(t, store.db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM conflicting_children
			WHERE transaction_id = (SELECT id FROM transactions WHERE hash = $1)`, parent.TxIDChainHash()[:]).Scan(&n))

		return n
	}

	provider, ok := any(store).(pruner.PrunerServiceProvider)
	require.True(t, ok)

	prunerSvc, err := provider.GetPrunerService()
	require.NoError(t, err)

	_, err = prunerSvc.Prune(ctx, store.GetBlockHeight()+1000, "test")
	require.NoError(t, err)

	_, err = store.Get(ctx, loser.TxIDChainHash())
	require.NoError(t, err, "the loser must survive while a conflict intent is pending")

	during, err := store.GetSpend(ctx, parentSpend)
	require.NoError(t, err)
	require.NotNil(t, during.SpendingData, "the parent output still names the loser")
	require.Equal(t, loser.TxIDChainHash().String(), during.SpendingData.TxID.String())
	require.Equal(t, 1, countChildRefs(), "the parent still lists the loser as a conflicting child")

	require.NoError(t, store.CompleteConflictIntent(ctx, intent.IntentID()))

	_, err = prunerSvc.Prune(ctx, store.GetBlockHeight()+1000, "test")
	require.NoError(t, err)

	_, err = store.Get(ctx, loser.TxIDChainHash())
	require.ErrorIs(t, err, errors.ErrTxNotFound, "the loser is reaped once the intent completes")

	after, err := store.GetSpend(ctx, parentSpend)
	require.NoError(t, err)
	require.Nil(t, after.SpendingData, "the parent output is unspent once the loser is reaped")
	require.Equal(t, 0, countChildRefs())
}

// Reaping a loser unspends an output of its parent, so the parent no longer meets the conditions for a
// delete_at_height and the reap must clear one the parent carries.
func TestPrunerReapClearsTheDeleteHeightOfAParentItUnspends(t *testing.T) {
	ctx := t.Context()

	store, parent := setup(ctx, t)
	store.settings.GlobalBlockHeightRetention = 5
	store.settings.Pruner.UTXODefensiveEnabled = true

	loser := bt.NewTx()
	require.NoError(t, loser.From(parent.TxIDChainHash().String(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	require.NoError(t, loser.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", parent.Outputs[0].Satoshis-1000))
	loser.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})

	_, err := store.Create(ctx, parent, 0)
	require.NoError(t, err)

	_, err = store.Create(ctx, loser, 0)
	require.NoError(t, err)

	_, err = store.Spend(ctx, loser, store.GetBlockHeight()+1)
	require.NoError(t, err)

	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*loser.TxIDChainHash()}, true)
	require.NoError(t, err)

	_, err = store.db.ExecContext(ctx, `UPDATE transactions SET delete_at_height = 999999 WHERE hash = $1`, parent.TxIDChainHash()[:])
	require.NoError(t, err)

	reaped, err := store.reapConflicting(ctx, *loser.TxIDChainHash(), store.GetBlockHeight()+1000, store.settings.GlobalBlockHeightRetention)
	require.NoError(t, err)
	require.True(t, reaped)

	var dah sql.NullInt64

	require.NoError(t, store.db.QueryRowContext(ctx, `SELECT delete_at_height FROM transactions WHERE hash = $1`, parent.TxIDChainHash()[:]).Scan(&dah))
	require.False(t, dah.Valid, "the parent has an unspent output again, so it keeps no delete_at_height")

	reaped, err = store.reapConflicting(ctx, *loser.TxIDChainHash(), store.GetBlockHeight()+1000, store.settings.GlobalBlockHeightRetention)
	require.NoError(t, err)
	require.False(t, reaped, "a reaped record is no longer eligible")
}

// The reap locks conflict_intents before it counts them, so an intent begun during the reap waits for it to
// end instead of landing after the count and restoring a parent spend of a record the reap then deletes. The
// reap is parked on a parent output row after it took the table lock, so the test exercises the production lock.
func TestReapConflictingHoldsTheIntentLockUntilCommit_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)
	require.Equal(t, "postgres", store.engine)

	parent := newExtendedTxWithOutputs(t, 1)
	loser := newExtendedSpendingTx(t, parent, 0)
	require.NoError(t, loser.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5_000))

	_, err := store.Create(ctx, parent, 0)
	require.NoError(t, err)

	_, err = store.Create(ctx, loser, 0)
	require.NoError(t, err)

	_, err = store.Spend(ctx, loser, store.GetBlockHeight()+1)
	require.NoError(t, err)

	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*loser.TxIDChainHash()}, true)
	require.NoError(t, err)

	blocker, err := store.db.BeginTx(ctx, nil)
	require.NoError(t, err)

	defer func() {
		_ = blocker.Rollback()
	}()

	_, err = blocker.ExecContext(ctx, `
		SELECT 1 FROM outputs
		WHERE transaction_id = (SELECT id FROM transactions WHERE hash = $1) AND idx = 0
		FOR UPDATE`, parent.TxIDChainHash()[:])
	require.NoError(t, err)

	type reapResult struct {
		reaped bool
		err    error
	}

	reapDone := make(chan reapResult, 1)

	go func() {
		reaped, reapErr := store.reapConflicting(ctx, *loser.TxIDChainHash(), store.GetBlockHeight()+1000, store.settings.GlobalBlockHeightRetention)
		reapDone <- reapResult{reaped, reapErr}
	}()

	require.Eventually(t, func() bool {
		var waiting int

		require.NoError(t, store.db.QueryRowContext(ctx, `
			SELECT count(*) FROM pg_stat_activity
			WHERE wait_event_type = 'Lock' AND query LIKE '%UPDATE outputs%' AND query NOT LIKE '%pg_stat_activity%'`).Scan(&waiting))

		return waiting == 1
	}, 10*time.Second, 50*time.Millisecond, "the reap must park on the parent output row after it took the table lock")

	intent := utxo.ConflictIntent{
		Kind:        utxo.ConflictIntentReverse,
		BlockHeight: 1,
		BlockHash:   chainhash.Hash{2},
		TxHashes:    []chainhash.Hash{*loser.TxIDChainHash()},
		StartedAt:   1,
	}

	intentDone := make(chan error, 1)

	go func() {
		intentDone <- store.BeginConflictIntent(ctx, intent)
	}()

	select {
	case err = <-intentDone:
		require.FailNow(t, "intent creation must wait for the reap lock", "returned early: %v", err)
	case <-time.After(1500 * time.Millisecond):
	}

	require.NoError(t, blocker.Commit())

	select {
	case res := <-reapDone:
		require.NoError(t, res.err)
		require.True(t, res.reaped)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the reap did not finish after the output lock was released")
	}

	select {
	case err = <-intentDone:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "intent creation did not finish after the reap committed")
	}

	pending, err := store.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1)

	_, err = store.Get(ctx, loser.TxIDChainHash())
	require.ErrorIs(t, err, errors.ErrTxNotFound)
}

// A reap that fails part-way rolls back as one transaction, so the loser and the parent spend it holds
// stay in place for the next pruning cycle.
func TestReapConflictingFailureKeepsTheLoserAndItsParentSpend(t *testing.T) {
	cases := []struct {
		name      string
		statement string
	}{
		{name: "conflict intents cannot be counted", statement: "DROP TABLE conflict_intents"},
		{name: "parent spends cannot be cleared", statement: "DROP TABLE inputs"},
		{name: "parent delete height cannot be updated", statement: `CREATE TRIGGER fail_parent_dah BEFORE UPDATE OF delete_at_height ON transactions BEGIN SELECT RAISE(ABORT, 'delete height update refused'); END`},
		{name: "conflicting children cannot be deleted", statement: "DROP TABLE conflicting_children"},
		{name: "the record cannot be locked", statement: "DROP TABLE block_ids"},
		{name: "the commit is refused", statement: `CREATE TABLE deferred_refs (transaction_id INTEGER REFERENCES transactions(id) DEFERRABLE INITIALLY DEFERRED);
			CREATE TRIGGER refuse_commit AFTER DELETE ON transactions BEGIN INSERT INTO deferred_refs VALUES (-1); END`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()

			store, parent := setup(ctx, t)
			store.settings.GlobalBlockHeightRetention = 5

			loser := bt.NewTx()
			require.NoError(t, loser.From(parent.TxIDChainHash().String(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
			require.NoError(t, loser.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", parent.Outputs[0].Satoshis-1000))
			loser.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})

			_, err := store.Create(ctx, parent, 0)
			require.NoError(t, err)

			_, err = store.Create(ctx, loser, 0)
			require.NoError(t, err)

			_, err = store.Spend(ctx, loser, store.GetBlockHeight()+1)
			require.NoError(t, err)

			_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*loser.TxIDChainHash()}, true)
			require.NoError(t, err)

			_, err = store.db.ExecContext(ctx, `UPDATE transactions SET delete_at_height = 999999 WHERE hash = $1`, parent.TxIDChainHash()[:])
			require.NoError(t, err)

			_, err = store.db.ExecContext(ctx, tc.statement)
			require.NoError(t, err)

			reaped, err := store.reapConflicting(ctx, *loser.TxIDChainHash(), store.GetBlockHeight()+1000, store.settings.GlobalBlockHeightRetention)
			require.Error(t, err)
			require.False(t, reaped)

			var count int

			require.NoError(t, store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM transactions WHERE hash = $1`, loser.TxIDChainHash()[:]).Scan(&count))
			require.Equal(t, 1, count, "a failed reap must not delete the loser")

			var spendingData []byte

			require.NoError(t, store.db.QueryRowContext(ctx, `
				SELECT spending_data FROM outputs
				WHERE transaction_id = (SELECT id FROM transactions WHERE hash = $1) AND idx = 0`, parent.TxIDChainHash()[:]).Scan(&spendingData))
			require.NotNil(t, spendingData, "a failed reap must not clear the parent spend")
			require.Equal(t, loser.TxIDChainHash()[:], spendingData[:32])
		})
	}

	t.Run("a cancelled context cannot begin the transaction", func(t *testing.T) {
		ctx := t.Context()

		store, parent := setup(ctx, t)

		_, err := store.Create(ctx, parent, 0)
		require.NoError(t, err)

		cancelled, cancel := context.WithCancel(ctx)
		cancel()

		reaped, err := store.reapConflicting(cancelled, *parent.TxIDChainHash(), 1000, 5)
		require.Error(t, err)
		require.False(t, reaped)
	})
}
