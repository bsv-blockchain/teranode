package aerospike_test

import (
	"context"
	"testing"
	"time"

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

		require.NoError(t, store.FinalizeTransaction(ctx, tx, attemptOf(t, ctx, store, tx.TxIDChainHash())))

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

		require.NoError(t, store.FinalizeTransaction(ctx, parent2, attemptOf(t, ctx, store, parent2.TxIDChainHash())))

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

		_, rfErr := utxo.RollForwardCreating(ctx, logger, store, child, attemptOf(t, ctx, store, child.TxIDChainHash()), 106)
		require.ErrorIs(t, rfErr, errors.ErrTxNotFound)

		got := &meta.Data{}
		require.NoError(t, store.GetMeta(ctx, child.TxIDChainHash(), got))
		require.True(t, got.Creating, "the record is not finalized")

		// The sweep's path for an unwinnable spend error: roll the record back and delete it.
		require.NoError(t, utxo.RollBackCreating(ctx, logger, store, child, attemptOf(t, ctx, store, child.TxIDChainHash())))
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
		require.NoError(t, utxo.RollBackCreating(ctx, logger, store, parent3, attemptOf(t, ctx, store, parent3.TxIDChainHash())))

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

		require.NoError(t, utxo.RollBackCreating(ctx, logger, store, loser, attemptOf(t, ctx, store, loser.TxIDChainHash())))

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

		_, err = store.Spend(ctx, child, 131, utxo.IgnoreFlags{RequireParentOutputs: true})
		require.ErrorIs(t, err, wantErr, "the spend must fail on the bad input")

		inputs, err := utxo.GetSpends(child)
		require.NoError(t, err)
		require.Len(t, inputs, 2)

		before, err := store.GetSpend(ctx, inputs[0])
		require.NoError(t, err)
		require.NotNil(t, before.SpendingData, "the valid input is spent in the failing pass, so the rollback has work to undo")
		require.Equal(t, child.TxIDChainHash().String(), before.SpendingData.TxID.String())

		require.NoError(t, utxo.RollBackCreating(ctx, logger, store, child, attemptOf(t, ctx, store, child.TxIDChainHash())),
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

	began, err := store.BeginCreateFirstRollback(ctx, child.TxIDChainHash(), attemptOf(t, ctx, store, child.TxIDChainHash()))
	require.NoError(t, err)
	require.True(t, began)

	spends, err := utxo.RollForwardCreating(ctx, logger, store, child, attemptOf(t, ctx, store, child.TxIDChainHash()), 107, utxo.WithIgnoreLocked(true))
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

// swapBeforeFinalizeStore runs swap once, just before the roll-forward's finalize reaches the
// store. It places the interleaving that only timing produces: the attempt being recovered is
// rolled back and replaced after the roll-forward read it, and before its finalize lands. Every
// store call is the real store's.
type swapBeforeFinalizeStore struct {
	*teranode_aerospike.Store

	swap func()
}

func (s *swapBeforeFinalizeStore) FinalizeTransaction(ctx context.Context, tx *bt.Tx, attempt []byte) error {
	if s.swap != nil {
		swap := s.swap
		s.swap = nil

		swap()
	}

	return s.Store.FinalizeTransaction(ctx, tx, attempt)
}

// TestRollForward_CompensationOfAReplacedAttemptLeavesItsSpends pins that the compensation of a
// roll-forward does not reverse spends the txid's later attempt made. Attempt A is read as
// creating and re-spent, then A is rolled back and attempt B creates the same txid and spends the
// same inputs. A's finalize is refused, and A's compensation fails its take of the hold, because
// the record no longer carries A's token and is not rolling back. B's spends, which cannot be told
// from A's, must stay.
func TestRollForward_CompensationOfAReplacedAttemptLeavesItsSpends(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UseCreateFirstOrder = true

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	child := spendableChildTx(t, tests.ParentTx, 0)
	hash := child.TxIDChainHash()

	inputs, err := utxo.GetSpends(child)
	require.NoError(t, err)

	first, err := store.Create(ctx, child, 106, utxo.WithCreating(true))
	require.NoError(t, err)

	var second *meta.Data

	wrapper := &swapBeforeFinalizeStore{Store: store}
	wrapper.swap = func() {
		require.NoError(t, utxo.RollBackCreating(ctx, logger, store, child, first.Attempt), "attempt A is rolled back")

		cleared, clearErr := store.ClearCreateFirstHold(ctx, hash, true)
		require.NoError(t, clearErr)
		require.True(t, cleared, "a block-context create adopts the hold")

		var createErr error

		second, createErr = store.Create(ctx, child, 106, utxo.WithCreating(true))
		require.NoError(t, createErr, "attempt B creates the same txid")

		_, spendErr := store.Spend(ctx, child, 106)
		require.NoError(t, spendErr, "attempt B spends the same inputs")
	}

	_, rfErr := utxo.RollForwardCreating(ctx, logger, wrapper, child, first.Attempt, 106)
	require.ErrorIs(t, rfErr, errors.ErrTxNotFound, "A's finalize is refused on B's record")
	require.NotNil(t, second, "the interleaving ran")

	resp, err := store.GetSpend(ctx, inputs[0])
	require.NoError(t, err)
	require.NotNil(t, resp.SpendingData, "B's spend of the input must survive A's compensation")
	require.Equal(t, hash.String(), resp.SpendingData.TxID.String())

	got := &meta.Data{}
	require.NoError(t, store.GetMeta(ctx, hash, got))
	require.True(t, got.Creating, "B is still creating")
	require.False(t, got.RollingBack, "A's failed take must not have marked B's record")
	require.Equal(t, second.Attempt, got.Attempt)
	require.NotContains(t, recordBins(t, client, store, hash, 0, fields.Busy), fields.Busy.String())

	require.NoError(t, store.FinalizeTransaction(ctx, child, second.Attempt), "B finalizes with its own token")
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

			took, err := store.TakeRollbackHold(ctx, hash, token)
			require.NoError(t, err)
			require.True(t, took, "a holder is reversing spends")

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

	require.NoError(t, utxo.RollBackCreating(ctx, logger, owner, child, md.Attempt))
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
