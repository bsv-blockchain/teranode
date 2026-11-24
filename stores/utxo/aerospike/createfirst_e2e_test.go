package aerospike_test

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestSetMined_CreateFirst_RollsForwardBeforeClearing is the ChiR7 regression: both
// setMined write paths erase the creating/unminedSince selectors the sweeper needs, so
// SetMinedMulti must complete the roll-forward of a still-creating record BEFORE clearing.
// Asserted against BOTH clear paths (Lua and filter-expression) since they share no code.
func TestSetMined_CreateFirst_RollsForwardBeforeClearing(t *testing.T) {
	for _, useExpr := range []bool{false, true} {
		useExpr := useExpr

		name := "lua"
		if useExpr {
			name = "filter-expression"
		}

		t.Run(name, func(t *testing.T) {
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.UtxoStore.UseCreateFirstOrder = true
			tSettings.Aerospike.EnableSetMinedFilterExpressions = useExpr

			client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
			t.Cleanup(deferFn)
			cleanDB(t, client)

			// Grandparent (spendable).
			_, err := store.Create(ctx, tests.ParentTx, 100)
			require.NoError(t, err)

			// minedTx spends grandparent output 0, created tentative with its input NOT yet
			// spent — a create-first record abandoned before its spend/finalize completed.
			minedTx := spendableChildTx(t, tests.ParentTx, 0)
			_, err = store.Create(ctx, minedTx, 101, utxo.WithCreating(true))
			require.NoError(t, err)

			got := &meta.Data{}
			require.NoError(t, store.GetMeta(ctx, minedTx.TxIDChainHash(), got))
			require.True(t, got.Creating, "precondition: the record is tentative")

			// The tx is mined. setMined must roll it forward (spend its input + finalize)
			// BEFORE clearing the creating/unminedSince bins — otherwise it would be mined
			// with its grandparent output left permanently unspent (the #1214 shape).
			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{minedTx.TxIDChainHash()}, utxo.MinedBlockInfo{
				BlockID: 1, BlockHeight: 101, SubtreeIdx: 0, OnLongestChain: true,
			})
			require.NoError(t, err)

			// The record is finalized out of the creating state (not merely mined-with-creating-cleared).
			require.NoError(t, store.GetMeta(ctx, minedTx.TxIDChainHash(), got))
			require.False(t, got.Creating, "setMined must finalize a creating record, not just clear the flag")

			// And its input was ACTUALLY spent: a competitor for grandparent output 0 is rejected.
			competitor := spendableChildTx(t, tests.ParentTx, 0)
			competitor.Version = 42 // distinct txid, same input

			_, err = store.Spend(ctx, competitor, 102, utxo.IgnoreFlags{SkipUTXOHashCheck: true})
			require.Error(t, err, "the mined create-first tx must have spent its input; a competitor must be rejected")
			require.True(t, errors.Is(err, errors.ErrSpent), "expected ErrSpent, got: %v", err)
		})
	}
}

// TestSpendAndCreate_CreateFirst_DoubleSpendLeavesNoRecord pins the failed-spend contract of
// the create-first path: when a spend fails after the tentative create, the spends that
// succeeded are reversed and the tentative record is deleted. The loser leaves no record, the
// winner's spend of the contested input is intact, and a free input the loser took is
// unspent again.
func TestSpendAndCreate_CreateFirst_DoubleSpendLeavesNoRecord(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UseCreateFirstOrder = true

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	winner := spendableChildTx(t, tests.ParentTx, 1)
	_, err = store.Spend(ctx, winner, 101, utxo.IgnoreFlags{SkipUTXOHashCheck: true})
	require.NoError(t, err)

	loser := spendableChildTx(t, tests.ParentTx, 1)
	require.NoError(t, loser.From(
		tests.ParentTx.TxIDChainHash().String(), 2,
		tests.ParentTx.Outputs[2].LockingScript.String(),
		tests.ParentTx.Outputs[2].Satoshis,
	))
	loser.Version = 8

	_, _, err = store.SpendAndCreate(ctx, loser, 101)
	require.Error(t, err, "the create-first double-spend loser must fail")

	requireHeld(t, ctx, store, loser.TxIDChainHash(), "the loser's tentative record is replaced by a hold, never left spendable")

	competitor := spendableChildTx(t, tests.ParentTx, 1)
	competitor.Version = 9

	_, err = store.Spend(ctx, competitor, 102, utxo.IgnoreFlags{SkipUTXOHashCheck: true})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrSpent), "the winner's spend must be intact, got: %v", err)

	freeInput := spendableChildTx(t, tests.ParentTx, 2)
	freeInput.Version = 10

	_, err = store.Spend(ctx, freeInput, 102, utxo.IgnoreFlags{SkipUTXOHashCheck: true})
	require.NoError(t, err, "a free input the loser took must be unspent again")
}

// TestSpendAndCreate_CreateFirst_MissingParentLeavesNoRecord pins that the fresh create-first
// spend requires its parent outputs: the tentative record the spend would find is the tx's own,
// and must not bless a missing parent. The call fails with ErrTxNotFound, the tx is not left
// finalized with an input it never spent, and the record is rolled back.
func TestSpendAndCreate_CreateFirst_MissingParentLeavesNoRecord(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UseCreateFirstOrder = true

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	absentParent := spendableChildTx(t, tests.ParentTx, 0)
	absentParent.Version = 99

	orphan := spendableChildTx(t, absentParent, 0)

	_, _, err := store.SpendAndCreate(ctx, orphan, 106)
	require.ErrorIs(t, err, errors.ErrTxNotFound, "a missing parent must fail the fresh create-first spend")

	requireHeld(t, ctx, store, orphan.TxIDChainHash(), "the rolled back record is a hold, not finalized with an input it never spent")
}

// TestSpendAndCreateMulti_CreateFirst_RollsForwardTentativeDuplicate is the quick-validation
// shape: a below-checkpoint list whose first transaction already has a tentative record. Its
// combined call must roll that record forward (spend, finalize) and report MultiTxExisted, so
// the second transaction, which spends its output, is created and spent rather than failing
// with ErrTxCreating on every retry.
func TestSpendAndCreateMulti_CreateFirst_RollsForwardTentativeDuplicate(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UseCreateFirstOrder = true

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	_, err = store.Create(ctx, tx, 101, utxo.WithCreating(true))
	require.NoError(t, err)

	child := spendableChildTx(t, tx, 0)

	results, err := store.SpendAndCreateMulti(ctx, []*bt.Tx{tx, child}, 102, utxo.WithBelowCheckpoint())
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Equal(t, utxo.MultiTxExisted, results[0].Status, "the tentative duplicate is rolled forward: %v", results[0].Err)
	require.Equal(t, utxo.MultiTxCreated, results[1].Status, "its child is created: %v", results[1].Err)
	require.NoError(t, results[1].Err)

	got := &meta.Data{}
	require.NoError(t, store.GetMeta(ctx, tx.TxIDChainHash(), got))
	require.False(t, got.Creating, "the tentative record must be finalized")

	competitor := tx.Clone()
	competitor.Version = 42

	_, err = store.Spend(ctx, competitor, 103, utxo.IgnoreFlags{SkipUTXOHashCheck: true})
	require.ErrorIs(t, err, errors.ErrSpent, "the roll-forward must have spent the duplicate's inputs")
}

// TestSetMined_CreatingRecord_RollsForwardWithFlagOff verifies the setMined pre-flight runs
// whatever UseCreateFirstOrder says: a creating record left on disk by a flag rollback must
// be rolled forward (inputs spent, creating cleared) when it is mined, not stranded or made
// spendable off an unspent input. Asserted against BOTH write paths (Lua and filter-expression).
func TestSetMined_CreatingRecord_RollsForwardWithFlagOff(t *testing.T) {
	for _, useExpr := range []bool{false, true} {
		useExpr := useExpr

		name := "lua"
		if useExpr {
			name = "filter-expression"
		}

		t.Run(name, func(t *testing.T) {
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.UtxoStore.UseCreateFirstOrder = false
			tSettings.Aerospike.EnableSetMinedFilterExpressions = useExpr

			client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
			t.Cleanup(deferFn)
			cleanDB(t, client)

			// Grandparent (spendable).
			_, err := store.Create(ctx, tests.ParentTx, 100)
			require.NoError(t, err)

			// A creating record whose input is NOT spent (abandoned mid-flight / on disk
			// across a flag rollback).
			creatingTx := spendableChildTx(t, tests.ParentTx, 0)
			_, err = store.Create(ctx, creatingTx, 101, utxo.WithCreating(true))
			require.NoError(t, err)

			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{creatingTx.TxIDChainHash()}, utxo.MinedBlockInfo{
				BlockID: 1, BlockHeight: 101, SubtreeIdx: 0, OnLongestChain: true,
			})
			require.NoError(t, err)

			got := &meta.Data{}
			require.NoError(t, store.GetMeta(ctx, creatingTx.TxIDChainHash(), got))
			require.False(t, got.Creating, "setMined must finalize a creating record even with the flag off")

			competitor := spendableChildTx(t, tests.ParentTx, 0)
			competitor.Version = 42

			_, err = store.Spend(ctx, competitor, 102, utxo.IgnoreFlags{SkipUTXOHashCheck: true})
			require.Error(t, err, "the rolled-forward tx must have spent its input; a competitor must be rejected")
			require.True(t, errors.Is(err, errors.ErrSpent), "expected ErrSpent, got: %v", err)
		})
	}
}

// TestCreateFirstLifecycle exercises the full create-first store contract against a
// real Aerospike container: the happy path (create→spend→finalize gates child
// spendability), crash recovery (roll forward an un-finalized creating tx — the
// re-spend MUST be idempotent), and double-spend resolution (the loser is rolled back and deleted).
func TestCreateFirstLifecycle(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	// Funding grandparent (spendable), created normally.
	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	t.Run("happy: child spendable only after finalize", func(t *testing.T) {
		// parent = tx (spends grandparent), created tentative.
		_, err := store.Create(ctx, tx, 101, utxo.WithCreating(true))
		require.NoError(t, err)

		// Spending the parent's own inputs (grandparent outputs) works while creating.
		_, err = store.Spend(ctx, tx, 101)
		require.NoError(t, err)

		// A child spending the parent's output is rejected until finalize.
		child := spendableChildTx(t, tx, 0)
		_, err = store.Spend(ctx, child, 102)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxCreating), "child spend before finalize must be ErrTxCreating, got: %v", err)

		require.NoError(t, store.FinalizeTransaction(ctx, tx, attemptOf(t, ctx, store, tx.TxIDChainHash()), attemptOf(t, ctx, store, tx.TxIDChainHash())))

		_, err = store.Spend(ctx, child, 102)
		require.NoError(t, err, "child spend must succeed after the parent is finalized")
	})

	t.Run("crash recovery: roll forward an un-finalized creating tx", func(t *testing.T) {
		// parent2 spends a different grandparent output so it is independent of the happy path.
		parent2 := spendableChildTx(t, tests.ParentTx, 0)

		_, err := store.Create(ctx, parent2, 103, utxo.WithCreating(true))
		require.NoError(t, err)

		// Simulate a crash: spend the inputs but never finalize.
		_, err = store.Spend(ctx, parent2, 103)
		require.NoError(t, err)

		got := &meta.Data{}
		require.NoError(t, store.GetMeta(ctx, parent2.TxIDChainHash(), got))
		require.True(t, got.Creating, "an un-finalized tx must remain in the creating state")

		child2 := spendableChildTx(t, parent2, 0)
		_, err = store.Spend(ctx, child2, 104)
		require.True(t, errors.Is(err, errors.ErrTxCreating), "child of an un-finalized parent must be gated, got: %v", err)

		// The sweeper query finds it.
		stale, err := store.QueryStaleCreatingTxs(ctx, 110, 0)
		require.NoError(t, err)
		require.Contains(t, stale, *parent2.TxIDChainHash(), "the stale creating tx must be discoverable by the sweeper")

		// Roll forward exactly as the sweeper does: re-spend (MUST be idempotent for the
		// same spender) then finalize. This assertion is load-bearing for the whole design.
		_, err = store.Spend(ctx, parent2, 103)
		require.NoError(t, err, "re-spending the same inputs by the same tx must be idempotent-success (roll-forward safety)")

		require.NoError(t, store.FinalizeTransaction(ctx, parent2, attemptOf(t, ctx, store, parent2.TxIDChainHash()), attemptOf(t, ctx, store, parent2.TxIDChainHash())))

		_, err = store.Spend(ctx, child2, 104)
		require.NoError(t, err, "child must be spendable once the parent is rolled forward")
	})

	t.Run("roll-forward contract: a re-spend of a creating record whose parent is missing fails and is rolled back", func(t *testing.T) {
		// absentGrandparent is a valid tx that is NEVER stored (distinct txid via Version),
		// so the child's input UTXO record does not exist → the spend hits ErrTxNotFound.
		absentGrandparent := spendableChildTx(t, tests.ParentTx, 0)
		absentGrandparent.Version = 99

		child := spendableChildTx(t, absentGrandparent, 0)

		_, err := store.Create(ctx, child, 106, utxo.WithCreating(true))
		require.NoError(t, err)

		// The tentative record is not evidence that its own input was spent, so the missing
		// parent stays an error and the record is never finalized.
		_, spendErr := store.Spend(ctx, child, 106)
		require.ErrorIs(t, spendErr, errors.ErrTxNotFound, "a creating child is not proof that its parent was spent")

		_, _, rfErr := utxo.RollForwardCreating(ctx, logger, store, child, attemptOf(t, ctx, store, child.TxIDChainHash()), 106)
		require.ErrorIs(t, rfErr, errors.ErrTxNotFound)

		got := &meta.Data{}
		require.NoError(t, store.GetMeta(ctx, child.TxIDChainHash(), got))
		require.True(t, got.Creating, "the record is not finalized")

		// The sweep's path for an unwinnable spend error: roll the record back and delete it.
		require.NoError(t, rollBackAsCreator(ctx, logger, store, child, attemptOf(t, ctx, store, child.TxIDChainHash())))
		requireHeld(t, ctx, store, child.TxIDChainHash(), "the rolled back record is a hold")
	})

	t.Run("double-spend: creating loser is rolled back and deleted", func(t *testing.T) {
		// parent3 and a competitor both spend output 1 of the finalized parent from the
		// happy path; parent3 also spends its output 2, which is free, so the rollback has a
		// taken input to reverse.
		parent3 := spendableChildTx(t, tx, 1)
		require.NoError(t, parent3.From(
			tx.TxIDChainHash().String(), 2,
			tx.Outputs[2].LockingScript.String(),
			tx.Outputs[2].Satoshis,
		))

		competitor := spendableChildTx(t, tx, 1)
		competitor.Version = 7 // distinct txid, same contested input

		// The competitor wins the slot first.
		_, err := store.Spend(ctx, competitor, 105, utxo.IgnoreFlags{SkipUTXOHashCheck: true})
		require.NoError(t, err)

		// parent3 is created tentative, then its spend loses the double-spend.
		_, err = store.Create(ctx, parent3, 105, utxo.WithCreating(true))
		require.NoError(t, err)

		spends, err := store.Spend(ctx, parent3, 105)
		require.Error(t, err, "the losing double-spend must fail")

		sawSpent := errors.Is(err, errors.ErrSpent)
		for _, sp := range spends {
			if sp != nil && sp.Err != nil && errors.Is(sp.Err, errors.ErrSpent) {
				sawSpent = true
			}
		}
		require.True(t, sawSpent, "the loser must fail with ErrSpent (top-level or per-spend), got: %v", err)

		// Roll back as the submit path and the sweep do: mark, unspend what was taken, delete.
		require.NoError(t, rollBackAsCreator(ctx, logger, store, parent3, attemptOf(t, ctx, store, parent3.TxIDChainHash())))

		requireHeld(t, ctx, store, parent3.TxIDChainHash(), "the loser must leave no record but a hold")

		// The free input is spendable again, and the winner keeps the contested one.
		freeSpender := spendableChildTx(t, tx, 2)
		freeSpender.Version = 8
		_, err = store.Spend(ctx, freeSpender, 106, utxo.IgnoreFlags{SkipUTXOHashCheck: true})
		require.NoError(t, err, "the input the loser took must be unspent again")

		_, err = store.Spend(ctx, parent3, 106)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrSpent) || errors.Is(err, errors.ErrTxNotFound), "the winner's spend must survive, got: %v", err)
	})

	t.Run("sweep rollback reverses an input that fails the pass for another reason", func(t *testing.T) {
		parentA := spendableChildTx(t, tests.ParentTx, 0)
		parentA.Version = 41

		parentB := spendableChildTx(t, tests.ParentTx, 0)
		parentB.Version = 42

		_, err := store.Create(ctx, parentA, 120)
		require.NoError(t, err)

		_, err = store.Create(ctx, parentB, 120)
		require.NoError(t, err)

		loser := spendableChildTx(t, parentA, 0)
		require.NoError(t, loser.From(
			parentB.TxIDChainHash().String(), 0,
			parentB.Outputs[0].LockingScript.String(),
			parentB.Outputs[0].Satoshis,
		))

		// The original attempt spent both inputs, then crashed before finalize.
		_, err = store.Create(ctx, loser, 121, utxo.WithCreating(true))
		require.NoError(t, err)

		loserSpends, err := utxo.GetSpends(loser)
		require.NoError(t, err)

		_, err = store.Spend(ctx, loser, 121)
		require.NoError(t, err)

		// A rival takes input 0 of the loser's tx, and the parent of input 1 is locked.
		require.NoError(t, store.Unspend(ctx, loserSpends[:1]))

		rival := spendableChildTx(t, parentA, 0)
		rival.Version = 7

		_, err = store.Spend(ctx, rival, 122, utxo.IgnoreFlags{SkipUTXOHashCheck: true})
		require.NoError(t, err)

		require.NoError(t, store.SetLocked(ctx, []chainhash.Hash{*parentB.TxIDChainHash()}, true))

		// The sweep pass: the re-spend fails, input 0 with ErrSpent and input 1 with ErrTxLocked.
		_, spends, err := store.SpendAndCreate(ctx, loser, 123, utxo.WithSpendOnly())
		require.ErrorIs(t, err, errors.ErrSpent)

		var sawLocked bool

		for _, sp := range spends {
			if sp != nil && errors.Is(sp.Err, errors.ErrTxLocked) {
				sawLocked = true
			}
		}

		require.True(t, sawLocked, "input 1 must fail this pass with ErrTxLocked")

		require.NoError(t, rollBackAsCreator(ctx, logger, store, loser, attemptOf(t, ctx, store, loser.TxIDChainHash())))

		requireHeld(t, ctx, store, loser.TxIDChainHash(), "the rolled back record must be a hold")

		resp, err := store.GetSpend(ctx, loserSpends[1])
		require.NoError(t, err)
		require.Nil(t, resp.SpendingData, "input 1 must not stay spent by the deleted record")

		resp, err = store.GetSpend(ctx, loserSpends[0])
		require.NoError(t, err)
		require.NotNil(t, resp.SpendingData, "input 0 must still belong to the rival")
		require.Equal(t, rival.TxIDChainHash().String(), resp.SpendingData.TxID.String())
	})

	rollBackBadInput := func(t *testing.T, version uint32, badInput func(parent *bt.Tx) (vout uint32, script string, satoshis uint64), wantErr error) {
		t.Helper()

		goodParent := spendableChildTx(t, tests.ParentTx, 0)
		goodParent.Version = version

		badParent := spendableChildTx(t, tests.ParentTx, 0)
		badParent.Version = version + 1

		_, err := store.Create(ctx, goodParent, 130)
		require.NoError(t, err)

		_, err = store.Create(ctx, badParent, 130)
		require.NoError(t, err)

		vout, script, satoshis := badInput(badParent)

		child := spendableChildTx(t, goodParent, 0)
		require.NoError(t, child.From(badParent.TxIDChainHash().String(), vout, script, satoshis))

		_, err = store.Create(ctx, child, 131, utxo.WithCreating(true))
		require.NoError(t, err)

		// The first spend of a create-first attempt runs under the lease of its record. The store
		// leaves the spends of such a call to the rollback of that record.
		spendCtx, cancelSpend := context.WithDeadline(utxo.WithLeaseBound(ctx), time.Now().Add(time.Minute))
		_, err = store.Spend(spendCtx, child, 131, utxo.IgnoreFlags{RequireParentOutputs: true})

		cancelSpend()

		require.ErrorIs(t, err, wantErr, "the spend must fail on the bad input")

		inputs, err := utxo.GetSpends(child)
		require.NoError(t, err)
		require.Len(t, inputs, 2)

		before, err := store.GetSpend(ctx, inputs[0])
		require.NoError(t, err)
		require.NotNil(t, before.SpendingData, "the valid input is spent in the failing pass, so the rollback has work to undo")
		require.Equal(t, child.TxIDChainHash().String(), before.SpendingData.TxID.String())

		require.NoError(t, rollBackAsCreator(ctx, logger, store, child, attemptOf(t, ctx, store, child.TxIDChainHash())),
			"an input the tx can never have spent must not fail the rollback")

		requireHeld(t, ctx, store, child.TxIDChainHash(), "the rolled back record must be a hold, not left rolling back with its transaction")

		resp, err := store.GetSpend(ctx, inputs[0])
		require.NoError(t, err)
		require.Nil(t, resp.SpendingData, "the valid input must not stay spent by the deleted record")
	}

	t.Run("rollback skips an input whose UTXO hash does not match", func(t *testing.T) {
		rollBackBadInput(t, 51, func(parent *bt.Tx) (uint32, string, uint64) {
			return 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis + 1
		}, errors.ErrUtxoHashMismatch)
	})

	t.Run("rollback skips an input whose vout does not exist", func(t *testing.T) {
		rollBackBadInput(t, 61, func(parent *bt.Tx) (uint32, string, uint64) {
			return 7, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis
		}, errors.ErrTxNotFound)
	})
}

// TestSpendAndCreate_CreateFirst_Aerospike proves the fold end-to-end: with the
// store-level flag on, SpendAndCreate's combined path persists create-first
// (create tentative → spend → finalize) against a real Aerospike store, leaving a
// finalized, spendable tx.
func TestSpendAndCreate_CreateFirst_Aerospike(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UseCreateFirstOrder = true

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	require.True(t, store.SupportsCreateFirst())

	// Funding grandparent (spendable).
	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	// Combined SpendAndCreate on tx (spends grandparent, creates tx) → create-first internally.
	md, spends, err := store.SpendAndCreate(ctx, tx, 101)
	require.NoError(t, err)
	require.NotEmpty(t, spends, "the combined path must spend the inputs")
	require.False(t, md.Creating, "create-first must finalize the tx out of the creating state")

	// The finalized tx's outputs are immediately spendable (finalize ran).
	got := &meta.Data{}
	require.NoError(t, store.GetMeta(ctx, tx.TxIDChainHash(), got))
	require.False(t, got.Creating)

	child := spendableChildTx(t, tx, 0)
	_, err = store.Spend(ctx, child, 102)
	require.NoError(t, err, "a child of a create-first SpendAndCreate tx must be spendable")
}

func TestRollForwardCreating_RollingBackRecordHasNoParentSpent(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	child := spendableChildTx(t, tests.ParentTx, 0)

	_, err = store.Create(ctx, child, 107, utxo.WithCreating(true))
	require.NoError(t, err)

	began, err := beginAsCreator(ctx, store, child.TxIDChainHash(), attemptOf(t, ctx, store, child.TxIDChainHash()))
	require.NoError(t, err)
	require.True(t, began)

	spends, _, err := utxo.RollForwardCreating(ctx, logger, store, child, attemptOf(t, ctx, store, child.TxIDChainHash()), 107, utxo.WithIgnoreLocked(true))
	require.Error(t, err)
	require.Contains(t, err.Error(), "rolling back")
	require.Nil(t, spends)

	inputs, err := utxo.GetSpends(child)
	require.NoError(t, err)

	resp, err := store.GetSpend(ctx, inputs[0])
	require.NoError(t, err)
	require.Equal(t, int(utxo.Status_OK), resp.Status, "the parent output must still be unspent")
	require.Nil(t, resp.SpendingData, "no spender is recorded on the parent output")
}

// TestCreateFirst_HoldRefusesResubmissionUntilItExpires pins what a rolled back attempt leaves
// behind. The orphan's parent is missing, so the attempt is rolled back. The parent then appears.
// A resubmission is refused with a transient error while the hold stands, and once it has
// expired the same call creates, spends and finalizes the transaction.
func TestCreateFirst_HoldRefusesResubmissionUntilItExpires(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UseCreateFirstOrder = true
	tSettings.UtxoStore.SpendRollbackTimeout = 2 * time.Second

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	missing := spendableChildTx(t, tests.ParentTx, 0)
	missing.Version = 99

	orphan := spendableChildTx(t, missing, 0)
	hash := orphan.TxIDChainHash()

	_, _, err = store.SpendAndCreate(ctx, orphan, 106)
	require.ErrorIs(t, err, errors.ErrTxNotFound, "a missing parent rolls the attempt back")
	requireHeld(t, ctx, store, hash)

	_, err = store.Create(ctx, missing, 106)
	require.NoError(t, err, "the parent arrives")

	_, _, err = store.SpendAndCreate(ctx, orphan, 107)
	require.ErrorIs(t, err, errors.ErrProcessing, "a resubmission inside the hold is refused as transient")
	require.NotErrorIs(t, err, errors.ErrTxExists)
	require.Contains(t, err.Error(), "rolling back")
	requireHeld(t, ctx, store, hash)

	time.Sleep(3 * time.Second)

	md, spends, err := store.SpendAndCreate(ctx, orphan, 108)
	require.NoError(t, err, "the expired hold is deleted by the create that finds it, and the create goes on")
	require.NotNil(t, md)
	require.Len(t, spends, 1)

	got := &meta.Data{}
	require.NoError(t, store.GetMeta(ctx, hash, got))
	require.False(t, got.Creating, "the resubmission finalized")
	require.False(t, got.Hold)

	_, err = store.Spend(ctx, spendableChildTx(t, orphan, 0), 109)
	require.NoError(t, err, "the finalized transaction's outputs are spendable")
}

// TestCreateFirst_BlockContextAdoptsHold pins that a transaction a block contains does not wait out
// the hold of a rolled back attempt: a create that ignores locks, or carries mined block info,
// adopts the hold at once, unless a holder is busy reversing spends, which it waits for.
func TestCreateFirst_BlockContextAdoptsHold(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UseCreateFirstOrder = true

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	blockContexts := map[string][]utxo.CreateOption{
		"ignore locked":    {utxo.WithIgnoreLocked(true)},
		"mined block info": {utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 106, SubtreeIdx: 0})},
	}

	vout := uint32(0)

	for name, opts := range blockContexts {
		t.Run(name, func(t *testing.T) {
			missing := spendableChildTx(t, tests.ParentTx, vout)
			missing.Version = 200 + vout

			orphan := spendableChildTx(t, missing, 0)
			hash := orphan.TxIDChainHash()
			vout++

			_, _, err := store.SpendAndCreate(ctx, orphan, 106)
			require.ErrorIs(t, err, errors.ErrTxNotFound)
			requireHeld(t, ctx, store, hash)

			_, err = store.Create(ctx, missing, 106)
			require.NoError(t, err, "the parent arrives")

			token := attemptOf(t, ctx, store, hash)

			setBusy(t, client, store, hash, time.Now().Unix()+60)

			_, _, err = store.SpendAndCreate(ctx, orphan, 107, opts...)
			require.ErrorIs(t, err, errors.ErrProcessing, "block context waits for a busy holder")
			requireHeld(t, ctx, store, hash)

			require.NoError(t, store.ReleaseRollbackHold(ctx, hash, token))

			_, _, err = store.SpendAndCreate(ctx, orphan, 107)
			require.ErrorIs(t, err, errors.ErrProcessing, "a create that is not in block context waits out the hold")

			md, _, err := store.SpendAndCreate(ctx, orphan, 107, opts...)
			require.NoError(t, err, "block context adopts the hold at once")
			require.NotNil(t, md)

			got := &meta.Data{}
			require.NoError(t, store.GetMeta(ctx, hash, got))
			require.False(t, got.Hold)
			require.False(t, got.Creating, "the adopting create finalized")
		})
	}
}

// hookedUnspendStore runs onUnspend once, just before the real unspend, so a test can act as another
// actor while the owner of a rollback is reversing its spends. Every store call is the real store's.
type hookedUnspendStore struct {
	*teranode_aerospike.Store

	onUnspend func()
}

func (s *hookedUnspendStore) Unspend(ctx context.Context, spends []*utxo.Spend, flagAsLocked ...bool) error {
	if hook := s.onUnspend; hook != nil {
		s.onUnspend = nil

		hook()
	}

	return s.Store.Unspend(ctx, spends, flagAsLocked...)
}

// TestRollBackCreating_OwnerHoldsTheHoldForItsUnspend pins that the caller that owns a rollback is
// fenced for its whole unspend. A pruner sweep that reads the marked record while the owner is
// unspending must fail its take, and a block-context create must not adopt the record, because
// either would let a replacement attempt spend the inputs before the owner's unspend lands and
// clears that spend.
func TestRollBackCreating_OwnerHoldsTheHoldForItsUnspend(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	child := spendableChildTx(t, tests.ParentTx, 0)
	hash := child.TxIDChainHash()

	md, err := store.Create(ctx, child, 101, utxo.WithCreating(true))
	require.NoError(t, err)

	_, err = store.Spend(ctx, child, 101)
	require.NoError(t, err)

	var (
		tookDuringUnspend    bool
		clearedDuringUnspend bool
		hookErr              error
	)

	owner := &hookedUnspendStore{Store: store, onUnspend: func() {
		tookDuringUnspend, hookErr = store.TakeRollbackHold(ctx, hash, md.Attempt)
		if hookErr != nil {
			return
		}

		clearedDuringUnspend, hookErr = store.ClearCreateFirstHold(ctx, hash, true)
	}}

	require.NoError(t, rollBackAsCreator(ctx, logger, owner, child, md.Attempt))
	require.NoError(t, hookErr)
	require.False(t, tookDuringUnspend, "a second actor cannot take the hold while the owner unspends")
	require.False(t, clearedDuringUnspend, "a block-context create cannot adopt the record while the owner unspends")

	requireHeld(t, ctx, store, hash, "the rollback ended with a hold")
	require.NotContains(t, recordBins(t, client, store, hash, 0, fields.Busy), fields.Busy.String(), "the owner released the hold before it ended the rollback")

	inputs, err := utxo.GetSpends(child)
	require.NoError(t, err)

	resp, err := store.GetSpend(ctx, inputs[0])
	require.NoError(t, err)
	require.Nil(t, resp.SpendingData, "the owner's unspend landed")
}

// TestCreateFirst_FailedAdoptionRollsBackOverAConcurrentRollForward pins that a create whose
// surviving pagination children cannot be adopted does not remove the master it has published while
// another actor may roll it forward. An earlier attempt left children and no master. The fresh
// attempt writes the master and its adoption of the children fails. Before the create returns, a
// duplicate submission reads the published master and rolls it forward: it spends the input, and
// its finalize fails on the child that kept the earlier token. The parent output must not then name
// a transaction whose record is gone: the attempt is rolled back, which reverses that spend, and it
// leaves a hold.
func TestCreateFirst_FailedAdoptionRollsBackOverAConcurrentRollForward(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = 2
	tSettings.UtxoStore.UseCreateFirstOrder = true

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	hash := tx.TxIDChainHash()

	inputs, err := utxo.GetSpends(tx)
	require.NoError(t, err)

	_, err = store.Create(ctx, tx, 100, utxo.WithCreating(true))
	require.NoError(t, err)
	require.True(t, childRecordExists(t, client, store, hash, 1), "precondition: the tx paginates")

	deleteRawMaster(t, client, store, hash)

	var (
		rolledForward bool
		rfErr         error
	)

	store.FailChildAdoption(false)
	t.Cleanup(store.RestoreBatchOperate)

	teranode_aerospike.SetAfterChildAdoptionFailure(func() {
		store.RestoreBatchOperate()

		rolledForward = true
		_, _, rfErr = utxo.RollForwardCreating(ctx, logger, store, tx, attemptOf(t, ctx, store, hash), 101)
	})
	t.Cleanup(func() { teranode_aerospike.SetAfterChildAdoptionFailure(nil) })

	_, _, err = store.SpendAndCreate(ctx, tx, 101)
	require.ErrorIs(t, err, errors.ErrProcessing, "an adoption that fails is a transient failure of the create")
	require.True(t, rolledForward, "precondition: the concurrent roll-forward ran")
	require.Error(t, rfErr, "the roll-forward cannot finalize past the child of the earlier attempt")

	resp, err := store.GetSpend(ctx, inputs[0])
	require.NoError(t, err)
	require.Nil(t, resp.SpendingData, "the input must not stay spent by a transaction that is not stored as final")

	requireHeld(t, ctx, store, hash, "the attempt is rolled back to a hold")
}

// leasedRollForwardStore is the store a roll-forward uses in a test that interleaves it with the
// owner of a rollback. afterLease runs once, after the store has granted the roll-forward its
// spend lease and before the roll-forward spends. Every store call is the real store's.
type leasedRollForwardStore struct {
	*teranode_aerospike.Store

	once       sync.Once
	afterLease func()
}

func (s *leasedRollForwardStore) TakeSpendLease(ctx context.Context, hash *chainhash.Hash, attempt []byte) ([]byte, bool, error) {
	lease, took, err := s.Store.TakeSpendLease(ctx, hash, attempt)
	s.once.Do(s.afterLease)

	return lease, took, err
}

// TestRollBack_DoesNotBeginWhileARollForwardHoldsASpendLease pins the fence between a roll-forward
// and the rollback of the same attempt. The roll-forward reads the record as creating and is granted
// its lease. The creator's own spend has failed and it now tries to roll the attempt back. The
// rollback must not begin before the roll-forward's spend has landed: the hold it leaves carries no
// transaction, so a spend that lands after the reversal would name a txid nobody can undo it for.
// The store refuses the rollback claim while the lease is live.
//
// The first case is a roll-forward that spends one input and fails on the other: the rollback then
// begins and its reversal covers that spend. The second is a roll-forward that spends every input:
// it finalizes the record, and the rollback then finds nothing to roll back.
func TestRollBack_DoesNotBeginWhileARollForwardHoldsASpendLease(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	winner := spendableChildTx(t, tests.ParentTx, 1)
	_, err = store.Spend(ctx, winner, 101, utxo.IgnoreFlags{SkipUTXOHashCheck: true})
	require.NoError(t, err)

	partial := spendableChildTx(t, tests.ParentTx, 1)
	require.NoError(t, partial.From(
		tests.ParentTx.TxIDChainHash().String(), 2,
		tests.ParentTx.Outputs[2].LockingScript.String(),
		tests.ParentTx.Outputs[2].Satoshis,
	))
	partial.Version = 61

	complete := spendableChildTx(t, tests.ParentTx, 0)
	complete.Version = 62

	cases := []struct {
		name       string
		tx         *bt.Tx
		freeInput  int
		forwardErr error
		rolledBack bool
	}{
		{name: "a roll-forward whose spend fails on another input", tx: partial, freeInput: 1, forwardErr: errors.ErrSpent, rolledBack: true},
		{name: "a roll-forward that completes the record", tx: complete, freeInput: 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hash := tc.tx.TxIDChainHash()

			inputs, err := utxo.GetSpends(tc.tx)
			require.NoError(t, err)

			md, err := store.Create(ctx, tc.tx, 101, utxo.WithCreating(true))
			require.NoError(t, err)

			var (
				leased  = make(chan struct{})
				proceed = make(chan struct{})
				rfDone  = make(chan error, 1)
				rbDone  = make(chan error, 1)
			)

			forward := &leasedRollForwardStore{Store: store, afterLease: func() {
				close(leased)
				<-proceed
			}}

			go func() {
				_, _, rfErr := utxo.RollForwardCreating(ctx, logger, forward, tc.tx, md.Attempt, 101)
				rfDone <- rfErr
			}()

			<-leased

			go func() {
				rbDone <- rollBackAsCreator(ctx, logger, store, tc.tx, md.Attempt)
			}()

			select {
			case rbErr := <-rbDone:
				t.Fatalf("the rollback returned while a spender held a live lease: %v", rbErr)
			case <-time.After(time.Second):
			}

			got := &meta.Data{}
			require.NoError(t, store.GetMeta(ctx, hash, got))
			require.False(t, got.RollingBack, "no rollback begins while a spend of the attempt may still land")

			close(proceed)

			select {
			case rfErr := <-rfDone:
				if tc.forwardErr != nil {
					require.ErrorIs(t, rfErr, tc.forwardErr)
				} else {
					require.NoError(t, rfErr)
				}
			case <-time.After(60 * time.Second):
				t.Fatal("the roll-forward did not return")
			}

			select {
			case rbErr := <-rbDone:
				require.NoError(t, rbErr)
			case <-time.After(60 * time.Second):
				t.Fatal("the rollback did not return")
			}

			resp, err := store.GetSpend(ctx, inputs[tc.freeInput])
			require.NoError(t, err)

			if tc.rolledBack {
				require.Nil(t, resp.SpendingData, "the spend the roll-forward made must be reversed")
				requireHeld(t, ctx, store, hash, "the rollback ended with a hold")
				require.NotContains(t, recordBins(t, client, store, hash, 0, fields.Busy), fields.Busy.String())

				return
			}

			require.NotNil(t, resp.SpendingData, "a stored transaction keeps its spends")
			require.NoError(t, store.GetMeta(ctx, hash, got))
			require.False(t, got.Creating, "the roll-forward completed the record")
			require.False(t, got.RollingBack, "the rollback found a final record and did nothing")
		})
	}

	partialInputs, err := utxo.GetSpends(partial)
	require.NoError(t, err)

	resp, err := store.GetSpend(ctx, partialInputs[0])
	require.NoError(t, err)
	require.NotNil(t, resp.SpendingData, "the rival's spend is intact")
	require.Equal(t, winner.TxIDChainHash().String(), resp.SpendingData.TxID.String())
}

// TestRollBack_LeavesTheLeaseOfACrashedSpenderToAgeOut pins what a rollback does when a spender of
// its attempt never releases its lease: the spender crashed, or its spend returned with an unknown
// outcome and may still land. The rollback writes and reverses nothing while that lease is live, and
// the record stays creating. Once the lease is stale the rollback runs.
func TestRollBack_LeavesTheLeaseOfACrashedSpenderToAgeOut(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.SpendRollbackTimeout = 2 * time.Second
	tSettings.UtxoStore.SpendWaitTimeout = time.Second

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	child := spendableChildTx(t, tests.ParentTx, 0)
	child.Version = 63
	hash := child.TxIDChainHash()

	inputs, err := utxo.GetSpends(child)
	require.NoError(t, err)

	// The creator spends and then dies: its lease stays on the record.
	md, err := store.Create(ctx, child, 101, utxo.WithCreating(true))
	require.NoError(t, err)

	_, err = store.Spend(ctx, child, 101)
	require.NoError(t, err)

	started := time.Now()
	rbErr := utxo.RollBackCreating(ctx, logger, store, child, md.Attempt)
	require.True(t, utxo.IsSpendLeaseLive(rbErr), "a rollback does not begin over a live lease, got: %v", rbErr)
	require.GreaterOrEqual(t, time.Since(started), time.Second, "the rollback tried for as long as one spend may run")

	resp, err := store.GetSpend(ctx, inputs[0])
	require.NoError(t, err)
	require.NotNil(t, resp.SpendingData, "nothing is reversed while a spend of the attempt may still land")

	got := &meta.Data{}
	require.NoError(t, store.GetMeta(ctx, hash, got))
	require.True(t, got.Creating)
	require.False(t, got.RollingBack, "the record is left creating, so a roll-forward can still complete it")

	require.Eventually(t, func() bool {
		live, liveErr := store.SpendLeasesLive(ctx, hash, md.Attempt)

		return liveErr == nil && !live
	}, 10*time.Second, 100*time.Millisecond, "the lease goes stale on its own")

	require.NoError(t, utxo.RollBackCreating(ctx, logger, store, child, md.Attempt))

	resp, err = store.GetSpend(ctx, inputs[0])
	require.NoError(t, err)
	require.Nil(t, resp.SpendingData, "the input is unspent")
	requireHeld(t, ctx, store, hash)
}

// TestCreateFirst_OpenRollbackIsFinishedByTheNextCreate pins that a rollback its owner left open
// does not wait for the pruner sweep, which runs only when the chain advances. The owner set its
// marker and died. Once its busy marker is stale, the next create-first SpendAndCreate of the txid
// finishes the rollback itself: outside block context it then reports the hold as a transient error,
// and in block context it adopts the hold and stores the transaction.
func TestCreateFirst_OpenRollbackIsFinishedByTheNextCreate(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UseCreateFirstOrder = true
	tSettings.UtxoStore.SpendRollbackTimeout = 2 * time.Second

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	blockContext := utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 102, SubtreeIdx: 0})

	for vout, inBlock := range []bool{false, true} {
		child := spendableChildTx(t, tests.ParentTx, uint32(vout))
		child.Version = uint32(70 + vout)
		hash := child.TxIDChainHash()

		inputs, err := utxo.GetSpends(child)
		require.NoError(t, err)

		md, err := store.Create(ctx, child, 101, utxo.WithCreating(true))
		require.NoError(t, err)

		_, err = store.Spend(ctx, child, 101)
		require.NoError(t, err)

		began, err := beginAsCreator(ctx, store, hash, md.Attempt)
		require.NoError(t, err)
		require.True(t, began, "the owner sets its marker and dies before it reverses a spend")

		_, _, err = store.SpendAndCreate(ctx, child, 102, blockContext)
		require.ErrorIs(t, err, errors.ErrProcessing, "nothing finishes the rollback while the owner's marker is live")

		resp, err := store.GetSpend(ctx, inputs[0])
		require.NoError(t, err)
		require.NotNil(t, resp.SpendingData, "nothing is reversed while the owner may still be reversing")

		// The marker is stale once holdUntil has passed by a further timeout.
		time.Sleep(5 * time.Second)

		if !inBlock {
			_, _, err = store.SpendAndCreate(ctx, child, 102)
			require.ErrorIs(t, err, errors.ErrProcessing, "outside block context the hold is waited out")
			requireHeld(t, ctx, store, hash, "the call finished the open rollback")

			resp, err = store.GetSpend(ctx, inputs[0])
			require.NoError(t, err)
			require.Nil(t, resp.SpendingData, "the call reversed the inputs of the rolled back attempt")

			continue
		}

		stored, _, err := store.SpendAndCreate(ctx, child, 102, blockContext)
		require.NoError(t, err, "block context finishes the rollback, adopts the hold and stores the transaction")
		require.NotNil(t, stored)

		got := &meta.Data{}
		require.NoError(t, store.GetMeta(ctx, hash, got))
		require.False(t, got.Creating)
		require.False(t, got.Hold)

		resp, err = store.GetSpend(ctx, inputs[0])
		require.NoError(t, err)
		require.NotNil(t, resp.SpendingData)
		require.Equal(t, hash.String(), resp.SpendingData.TxID.String())
	}
}

// finalizedElsewhereStore lets another actor finalize the attempt once, just before the first
// FinalizeTransaction of its user reaches the store. The other actor is a roll-forward under its
// own lease when the user is the creator, and the creator when the user is a roll-forward. Every
// store call is the real store's.
type finalizedElsewhereStore struct {
	*teranode_aerospike.Store

	done bool
}

func (s *finalizedElsewhereStore) FinalizeTransaction(ctx context.Context, tx *bt.Tx, attempt, lease []byte) error {
	if !s.done {
		s.done = true

		other := attempt

		if bytes.Equal(lease, attempt) {
			taken, took, err := s.Store.TakeSpendLease(ctx, tx.TxIDChainHash(), attempt)
			if err != nil || !took {
				return errors.NewProcessingError("the other actor could not take a spend lease (took %t)", took, err)
			}

			other = taken
		}

		if err := s.Store.FinalizeTransaction(ctx, tx, attempt, other); err != nil {
			return err
		}
	}

	return s.Store.FinalizeTransaction(ctx, tx, attempt, lease)
}

// TestCreateFirst_OnlyTheFinalizeThatClearedTheStateOwnsTheHandOff pins that the creator of a record
// and a roll-forward of the same attempt cannot both take the transaction as theirs. Both can reach
// the finalize. The validator hands a transaction to block assembly on a nil result or on a
// rolled-forward result, and it unwinds a shed hand-off of a record it created by deleting the
// record. If both actors got those results, the creator could delete a record that the other
// instance has already delivered. The actor whose finalize did not clear the creating state must
// therefore see an ordinary duplicate.
func TestCreateFirst_OnlyTheFinalizeThatClearedTheStateOwnsTheHandOff(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UseCreateFirstOrder = true

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	t.Run("the creator loses the finalize to a roll-forward", func(t *testing.T) {
		child := spendableChildTx(t, tests.ParentTx, 0)
		child.Version = 81

		md, spends, err := utxo.SequentialSpendAndCreate(ctx, logger, &finalizedElsewhereStore{Store: store}, child, 101, true)
		require.ErrorIs(t, err, errors.ErrTxExists, "the creator reports an ordinary duplicate")
		require.False(t, utxo.IsRolledForward(err), "the creator must not take the hand-off, or a shed would unwind a record another instance delivered")
		require.Nil(t, md, "no meta: the validator treats a nil-error result as a record this call created")
		require.Len(t, spends, 1)

		got := &meta.Data{}
		require.NoError(t, store.GetMeta(ctx, child.TxIDChainHash(), got))
		require.False(t, got.Creating, "the record is stored")
	})

	t.Run("a roll-forward loses the finalize to the creator", func(t *testing.T) {
		child := spendableChildTx(t, tests.ParentTx, 1)
		child.Version = 82

		md, err := store.Create(ctx, child, 101, utxo.WithCreating(true))
		require.NoError(t, err)

		spends, completed, err := utxo.RollForwardCreating(ctx, logger, &finalizedElsewhereStore{Store: store}, child, md.Attempt, 101)
		require.NoError(t, err, "the record is complete")
		require.False(t, completed, "this call did not complete it, so it must not hand the transaction off")
		require.Len(t, spends, 1)

		_, _, err = utxo.SequentialSpendAndCreate(ctx, logger, store, child, 101, true)
		require.ErrorIs(t, err, errors.ErrTxExists)
		require.False(t, utxo.IsRolledForward(err), "a later duplicate is an ordinary duplicate")
	})
}

// wideMultiRecordChild spends output vout of parentTx and pays to five outputs, so with
// UtxoBatchSize 2 the store writes it as a master plus pagination children.
func wideMultiRecordChild(t *testing.T, parentTx *bt.Tx, vout uint32, version uint32) *bt.Tx {
	t.Helper()

	child := bt.NewTx()
	require.NoError(t, child.From(
		parentTx.TxIDChainHash().String(), vout,
		parentTx.Outputs[vout].LockingScript.String(),
		parentTx.Outputs[vout].Satoshis,
	))

	for i := 0; i < 5; i++ {
		require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", parentTx.Outputs[vout].Satoshis/10))
	}

	child.Version = version

	return child
}

// TestFlagOffDuplicate_DoesNotFinaliseATokenedMultiRecordRecord is the hazard the duplicate-create
// guard exists for, reproduced on the paginated path with create-first off. A create-only
// duplicate has spent nothing, so it may not turn another attempt's tentative record into a
// final one: a record that stops creating without its input spent is exactly the dangling
// spender of #1214, and clearing the creating bin also hides it from the sweeper, which
// selects on that bin.
func TestFlagOffDuplicate_DoesNotFinaliseATokenedMultiRecordRecord(t *testing.T) {
	t.Run("no rival: a record that stops creating must have its input spent, and a healer must finish it", func(t *testing.T) {
		logger := ulogger.NewErrorTestLogger(t)
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.UtxoStore.UtxoBatchSize = 2
		tSettings.UtxoStore.UseCreateFirstOrder = false

		client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
		t.Cleanup(deferFn)
		cleanDB(t, client)

		_, err := store.Create(ctx, tests.ParentTx, 100)
		require.NoError(t, err)

		a := wideMultiRecordChild(t, tests.ParentTx, 0, 61)
		hash := a.TxIDChainHash()

		inputs, err := utxo.GetSpends(a)
		require.NoError(t, err)

		_, err = store.Create(ctx, a, 100, utxo.WithCreating(true))
		require.NoError(t, err)

		tentative, err := store.Get(ctx, hash, fields.Creating, fields.Attempt)
		require.NoError(t, err)
		require.True(t, tentative.Creating)
		require.NotEmpty(t, tentative.Attempt, "precondition: the record carries an attempt token")

		_, _, err = store.SpendAndCreate(ctx, a, 101, utxo.WithCreateOnly())
		require.ErrorIs(t, err, errors.ErrTxExists)

		got := &meta.Data{}
		require.NoError(t, store.GetMeta(ctx, hash, got))

		resp, err := store.GetSpend(ctx, inputs[0])
		require.NoError(t, err)

		require.True(t, got.Creating || resp.SpendingData != nil, "SAFETY: the record is final but its input was never spent")

		grandchild := spendableChildTx(t, a, 0)
		_, err = store.Spend(ctx, grandchild, 102)
		require.ErrorIs(t, err, errors.ErrTxCreating, "SAFETY: an output of a tx whose input is unspent must not be spendable")

		stale, err := store.QueryStaleCreatingTxs(ctx, 200, 0)
		require.NoError(t, err)
		require.Len(t, stale, 1, "HEALER: the sweep query selects on the creating bin, so it must still find the record")
		require.Equal(t, *hash, stale[0])

		_, completed, err := utxo.RollForwardCreating(ctx, logger, store, a, tentative.Attempt, 102)
		require.NoError(t, err)
		require.True(t, completed, "HEALER: the roll-forward finishes the record with the flag off")

		require.NoError(t, store.GetMeta(ctx, hash, got))
		require.False(t, got.Creating)

		resp, err = store.GetSpend(ctx, inputs[0])
		require.NoError(t, err)
		require.NotNil(t, resp.SpendingData, "HEALER: the roll-forward spent the input")
	})

	t.Run("a rival holds the input: the tentative record must not become spendable, and a healer must roll it back", func(t *testing.T) {
		logger := ulogger.NewErrorTestLogger(t)
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.UtxoStore.UtxoBatchSize = 2
		tSettings.UtxoStore.UseCreateFirstOrder = false
		tSettings.UtxoStore.SpendRollbackTimeout = 2 * time.Second
		tSettings.UtxoStore.SpendWaitTimeout = time.Second

		client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
		t.Cleanup(deferFn)
		cleanDB(t, client)

		_, err := store.Create(ctx, tests.ParentTx, 100)
		require.NoError(t, err)

		a := wideMultiRecordChild(t, tests.ParentTx, 0, 62)
		hash := a.TxIDChainHash()

		inputs, err := utxo.GetSpends(a)
		require.NoError(t, err)

		_, err = store.Create(ctx, a, 100, utxo.WithCreating(true))
		require.NoError(t, err)

		tentative, err := store.Get(ctx, hash, fields.Creating, fields.Attempt)
		require.NoError(t, err)
		require.NotEmpty(t, tentative.Attempt)

		rival := spendableChildTx(t, tests.ParentTx, 0)
		rival.Version = 63

		_, err = store.Spend(ctx, rival, 101, utxo.IgnoreFlags{SkipUTXOHashCheck: true})
		require.NoError(t, err, "precondition: the rival took the input")

		_, _, err = store.SpendAndCreate(ctx, a, 101, utxo.WithCreateOnly(), utxo.WithConflicting(true))
		require.ErrorIs(t, err, errors.ErrTxExists)

		got := &meta.Data{}
		require.NoError(t, store.GetMeta(ctx, hash, got))
		require.True(t, got.Creating, "SAFETY: a duplicate that spent nothing made another attempt's tentative record final while a rival holds its input")

		grandchild := spendableChildTx(t, a, 0)
		_, spendErr := store.Spend(ctx, grandchild, 102)
		require.ErrorIs(t, spendErr, errors.ErrTxCreating, "SAFETY: the validator guard reads !Creating, so a cleared flag makes this output spendable behind the rival")

		_, completed, err := utxo.RollForwardCreating(ctx, logger, store, a, tentative.Attempt, 102)
		require.ErrorIs(t, err, errors.ErrSpent, "HEALER: the roll-forward loses to the rival")
		require.False(t, completed)

		require.Eventually(t, func() bool {
			live, liveErr := store.SpendLeasesLive(ctx, hash, tentative.Attempt)

			return liveErr == nil && !live
		}, 30*time.Second, 250*time.Millisecond, "the lease of the creator that never spent goes stale")

		require.NoError(t, utxo.RollBackCreating(ctx, logger, store, a, tentative.Attempt))

		after := &meta.Data{}
		getErr := store.GetMeta(ctx, hash, after)
		require.True(t, getErr != nil || after.Hold, "HEALER: the rolled back record is gone or a hold")

		resp, err := store.GetSpend(ctx, inputs[0])
		require.NoError(t, err)
		require.NotNil(t, resp.SpendingData, "the rival's spend stands")
		require.Equal(t, rival.TxIDChainHash().String(), resp.SpendingData.TxID.String())
	})
}

// TestFlagOffSpendFirst_RollsForwardACreatingRecord is the strand of a block that holds a
// transaction whose record a create-first attempt left creating. With create-first off, the
// spend-first SpendAndCreate of the block's validation finds that record at its create. It must
// finish the record itself, because the pruner sweep that would is driven by the chain height and a
// block that cannot validate stops the chain. A record that stays creating without the mark makes
// the validator fail closed on every attempt.
func TestFlagOffSpendFirst_RollsForwardACreatingRecord(t *testing.T) {
	for name, wide := range map[string]bool{"single record": false, "multi record": true} {
		t.Run(name, func(t *testing.T) {
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.UtxoStore.UseCreateFirstOrder = false

			if wide {
				tSettings.UtxoStore.UtxoBatchSize = 2
			}

			client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
			t.Cleanup(deferFn)
			cleanDB(t, client)

			_, err := store.Create(ctx, tests.ParentTx, 100)
			require.NoError(t, err)

			child := spendableChildTx(t, tests.ParentTx, 0)
			if wide {
				child = wideMultiRecordChild(t, tests.ParentTx, 0, 71)
			}

			hash := child.TxIDChainHash()

			inputs, err := utxo.GetSpends(child)
			require.NoError(t, err)

			_, err = store.Create(ctx, child, 100, utxo.WithCreating(true))
			require.NoError(t, err)

			tentative := &meta.Data{}
			require.NoError(t, store.GetMeta(ctx, hash, tentative))
			require.True(t, tentative.Creating, "precondition: the record is tentative")

			_, spends, err := store.SpendAndCreate(ctx, child, 101)
			require.ErrorIs(t, err, errors.ErrTxExists)
			require.True(t, utxo.IsRolledForward(err), "the store says it finished the record, so the validator accepts it")
			require.Len(t, spends, len(inputs))

			got := &meta.Data{}
			require.NoError(t, store.GetMeta(ctx, hash, got))
			require.False(t, got.Creating, "the record is final")

			resp, err := store.GetSpend(ctx, inputs[0])
			require.NoError(t, err)
			require.NotNil(t, resp.SpendingData, "the input of the finished record is spent")
			require.Equal(t, hash.String(), resp.SpendingData.TxID.String())

			_, _, err = store.SpendAndCreate(ctx, child, 102)
			require.ErrorIs(t, err, errors.ErrTxExists)
			require.False(t, utxo.IsRolledForward(err), "a second call finds a final record: a plain duplicate")
		})
	}
}

// TestSetMined_MultiRecord_RollsForwardAfterADuplicateWithFlagOff is the multi-record
// counterpart of TestSetMined_CreatingRecord_RollsForwardWithFlagOff with a create-only
// duplicate arriving first. The duplicate must leave the tokened record creating, and the
// setMined pre-flight must then roll it forward: with the flag off the healer finishes the
// record, not the duplicate.
func TestSetMined_MultiRecord_RollsForwardAfterADuplicateWithFlagOff(t *testing.T) {
	for _, useExpr := range []bool{false, true} {
		useExpr := useExpr

		name := "lua"
		if useExpr {
			name = "filter-expression"
		}

		t.Run(name, func(t *testing.T) {
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.UtxoStore.UtxoBatchSize = 2
			tSettings.UtxoStore.UseCreateFirstOrder = false
			tSettings.Aerospike.EnableSetMinedFilterExpressions = useExpr

			client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
			t.Cleanup(deferFn)
			cleanDB(t, client)

			_, err := store.Create(ctx, tests.ParentTx, 100)
			require.NoError(t, err)

			creatingTx := wideMultiRecordChild(t, tests.ParentTx, 0, 64)
			hash := creatingTx.TxIDChainHash()

			_, err = store.Create(ctx, creatingTx, 101, utxo.WithCreating(true))
			require.NoError(t, err)

			_, _, err = store.SpendAndCreate(ctx, creatingTx, 101, utxo.WithCreateOnly())
			require.ErrorIs(t, err, errors.ErrTxExists)

			got := &meta.Data{}
			require.NoError(t, store.GetMeta(ctx, hash, got))
			require.True(t, got.Creating, "a duplicate that spent nothing must leave a tokened record creating")

			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{hash}, utxo.MinedBlockInfo{
				BlockID: 1, BlockHeight: 102, SubtreeIdx: 0, OnLongestChain: true,
			})
			require.NoError(t, err)

			require.NoError(t, store.GetMeta(ctx, hash, got))
			require.False(t, got.Creating, "the setMined pre-flight must roll the record forward even with the flag off")

			competitor := spendableChildTx(t, tests.ParentTx, 0)
			competitor.Version = 65

			_, err = store.Spend(ctx, competitor, 103, utxo.IgnoreFlags{SkipUTXOHashCheck: true})
			require.ErrorIs(t, err, errors.ErrSpent, "the rolled-forward tx took its input, so a competitor must be rejected")
		})
	}
}

// TestRollBack_KeepsTheHistoricalSpendsThatPredateTheRecord pins what a create-first reversal
// clears. The record owns the spends recorded at or after the height it was created at, and one
// recorded before it stays when it is also history (utxo.IsHistoricalSpend): a replay of a pruned
// transaction matches such a spend, and clearing it hands a confirmed output to anyone. A spend that
// is old but young by that rule, and one an old attempt of the same record made, is still cleared.
// Retention 288 makes a spend history from 144 blocks before the current height.
func TestRollBack_KeepsTheHistoricalSpendsThatPredateTheRecord(t *testing.T) {
	const defaultRecordHeight = uint32(900)

	for _, tc := range []struct {
		name         string
		recordHeight uint32
		// noRecordHeight removes the unminedSince of the tentative record, as a record that
		// carries none.
		noRecordHeight bool
		storeHeight    uint32
		callHeight     uint32
		spendHeights   []uint32
		wantKept       []bool
	}{
		{
			name:         "below the record and history is kept, the rest is cleared",
			storeHeight:  1000,
			spendHeights: []uint32{100, 856, 857, 890, 900, 950},
			wantKept:     []bool{true, true, false, false, false, false},
		},
		{
			name:         "a stale attempt of the same record, rolled back much later, is reversed",
			storeHeight:  5000,
			spendHeights: []uint32{900, 901, 950},
			wantKept:     []bool{false, false, false},
		},
		{
			name:         "a call height beyond the store height counts history from it",
			storeHeight:  1000,
			callHeight:   1100,
			spendHeights: []uint32{100, 856, 857, 890, 900, 950},
			wantKept:     []bool{true, true, true, true, false, false},
		},
		// Known limit. A spend recorded below the record's height is kept as history once the tip
		// is a full history age past it, whoever made it, because the record cannot tell its own
		// early spend from a pruned record's. It takes a gap of HistoricalSpendAge (144) blocks
		// between the height of the call and the record's.
		{
			name:         "limit: an own spend recorded two blocks below the record is kept once the tip is a history age past it",
			storeHeight:  1042,
			spendHeights: []uint32{898},
			wantKept:     []bool{true},
		},
		{
			name:         "limit: the same spend is reversed one block before that",
			storeHeight:  1041,
			spendHeights: []uint32{898},
			wantKept:     []bool{false},
		},
		{
			name:         "limit: a call that ran below the record, at a tip far above it, keeps its own spends",
			recordHeight: 910,
			callHeight:   900,
			storeHeight:  1200,
			spendHeights: []uint32{900},
			wantKept:     []bool{true},
		},
		// A record that carries no height leaves the age rule alone to decide, so a spend recorded
		// at or after where the record would have been is kept when it is old enough, and a young
		// one is reversed.
		{
			name:           "a record that carries no height reverses by the age rule alone",
			noRecordHeight: true,
			storeHeight:    1200,
			spendHeights:   []uint32{100, 950, 1056, 1057, 1100},
			wantKept:       []bool{true, true, true, false, false},
		},
	} {
		for _, finish := range []bool{false, true} {
			name := tc.name + "/RollBackCreating"
			if finish {
				name = tc.name + "/FinishRollBackCreating"
			}

			t.Run(name, func(t *testing.T) {
				logger := ulogger.NewErrorTestLogger(t)
				tSettings := test.CreateBaseTestSettings(t)
				tSettings.GlobalBlockHeightRetention = 288

				client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
				t.Cleanup(deferFn)
				cleanDB(t, client)

				parent := conflictingIndexParent(t, ctx, store, 7201, len(tc.spendHeights))

				vouts := make([]uint32, len(tc.spendHeights))
				for i := range vouts {
					vouts[i] = uint32(i)
				}

				child := conflictingIndexTx(t, parent, vouts, 1)
				hash := child.TxIDChainHash()
				inputs := spendHeightSpendsOf(t, child)

				for i, height := range tc.spendHeights {
					res := spendHeightUDF(t, store, parent, inputs[i], height)
					require.Equal(t, teranode_aerospike.LuaStatusOK, res.Status)
				}

				recordHeight := defaultRecordHeight
				if tc.recordHeight != 0 {
					recordHeight = tc.recordHeight
				}

				md, err := store.Create(ctx, child, recordHeight, utxo.WithCreating(true))
				require.NoError(t, err)

				if tc.noRecordHeight {
					key := spendHeightKey(t, store, hash)

					_, opErr := client.Operate(nil, key, aerospike.PutOp(aerospike.NewBin(fields.UnminedSince.String(), nil)))
					require.NoError(t, opErr)

					rec, getErr := client.Get(nil, key, fields.UnminedSince.String())
					require.NoError(t, getErr)
					require.Nil(t, rec.Bins[fields.UnminedSince.String()], "precondition: the record carries no height")
				}

				require.NoError(t, store.SetBlockHeight(tc.storeHeight))

				rbCtx := ctx
				if tc.callHeight != 0 {
					rbCtx = utxo.WithReversalCallHeight(ctx, tc.callHeight)
				}

				if finish {
					began, beginErr := beginAsCreator(rbCtx, store, hash, md.Attempt)
					require.NoError(t, beginErr)
					require.True(t, began)
					require.NoError(t, store.ReleaseRollbackHold(rbCtx, hash, md.Attempt))

					finished, finErr := utxo.FinishRollBackCreating(rbCtx, logger, store, child, md.Attempt, false)
					require.NoError(t, finErr, "a kept spend is not a failure of the rollback")
					require.True(t, finished)
				} else {
					require.NoError(t, rollBackAsCreator(rbCtx, logger, store, child, md.Attempt), "a kept spend is not a failure of the rollback")
				}

				requireHeld(t, ctx, store, hash, "the rollback ended and left a hold")

				for i := range inputs {
					resp, getErr := store.GetSpend(ctx, inputs[i])
					require.NoError(t, getErr)

					if tc.wantKept[i] {
						require.NotNil(t, resp.SpendingData, "the spend recorded at %d predates the record and is history: it stays", tc.spendHeights[i])
						require.Equal(t, *hash, *resp.SpendingData.TxID)

						continue
					}

					require.Nil(t, resp.SpendingData, "the spend recorded at %d is the record's own or young: it is reversed", tc.spendHeights[i])
				}
			})
		}
	}
}
