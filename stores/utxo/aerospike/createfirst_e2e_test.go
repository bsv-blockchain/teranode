package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
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

// TestSpendAndCreate_CreateFirst_DoubleSpendLeftForSweeper pins the behaviour after the
// inline terminal-conflict resolution was removed: a create-first double-spend loser fails
// with a spend error and is LEFT in the tentative creating state as the sweeper's WAL — it
// is NOT resolved (marked conflicting / finalized) inline. Resolving inline on the free
// propagation path was attacker-paceable and, because the delete side does not remove
// parent conflictingChildren refs, manufactured the dangling-ref shape #1214 documents.
func TestSpendAndCreate_CreateFirst_DoubleSpendLeftForSweeper(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UseCreateFirstOrder = true

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	// Grandparent (spendable).
	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	// The winner takes grandparent output 1.
	winner := spendableChildTx(t, tests.ParentTx, 1)
	_, err = store.Spend(ctx, winner, 101, utxo.IgnoreFlags{SkipUTXOHashCheck: true})
	require.NoError(t, err)

	// The loser spends the same output via the create-first combined path.
	loser := spendableChildTx(t, tests.ParentTx, 1)
	loser.Version = 8 // distinct txid, same input

	_, _, err = store.SpendAndCreate(ctx, loser, 101)
	require.Error(t, err, "the create-first double-spend loser must fail")

	// Left as the sweeper's WAL: still creating, NOT resolved inline.
	got := &meta.Data{}
	require.NoError(t, store.GetMeta(ctx, loser.TxIDChainHash(), got))
	require.True(t, got.Creating, "the loser must be left in the creating state for the sweeper")
	require.False(t, got.Conflicting, "the loser must NOT be marked conflicting inline (that is the sweeper's job, off the ingest path)")
}

// TestSetMined_CreateFirst_RefusesToClearWithoutSpending is the icellan-P0 regression: the
// setMined write paths must NEVER nil the creating bin, because setMined does not spend the
// tx's inputs. This is the unconditional safety net for the paths the flag-gated pre-flight
// does not run on — here modelled by the flag being OFF (a rollback with creating records
// on disk). Asserted against BOTH write paths (Lua and filter-expression).
func TestSetMined_CreateFirst_RefusesToClearWithoutSpending(t *testing.T) {
	for _, useExpr := range []bool{false, true} {
		useExpr := useExpr

		name := "lua"
		if useExpr {
			name = "filter-expression"
		}

		t.Run(name, func(t *testing.T) {
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.UtxoStore.UseCreateFirstOrder = false // flag OFF → pre-flight skipped
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

			// Mine it with the pre-flight disabled (flag off). The write path must NOT clear
			// creating, or the outputs would become spendable with the input still unspent.
			_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{creatingTx.TxIDChainHash()}, utxo.MinedBlockInfo{
				BlockID: 1, BlockHeight: 101, SubtreeIdx: 0, OnLongestChain: true,
			})
			require.NoError(t, err)

			got := &meta.Data{}
			require.NoError(t, store.GetMeta(ctx, creatingTx.TxIDChainHash(), got))
			require.True(t, got.Creating, "setMined must NOT clear the creating bin without spending the inputs")

			// The input was never spent, confirming the record was correctly left gated
			// rather than made spendable off an unspent input.
			competitor := spendableChildTx(t, tests.ParentTx, 0)
			competitor.Version = 42

			_, err = store.Spend(ctx, competitor, 102, utxo.IgnoreFlags{SkipUTXOHashCheck: true})
			require.NoError(t, err, "the creating tx's input was never spent, so a competitor can still take it")
		})
	}
}

// TestCreateFirstLifecycle exercises the full create-first store contract against a
// real Aerospike container: the happy path (create→spend→finalize gates child
// spendability), crash recovery (roll forward an un-finalized creating tx — the
// re-spend MUST be idempotent), and double-spend resolution (conflicting terminal state).
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

		require.NoError(t, store.FinalizeTransaction(ctx, tx))

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

		require.NoError(t, store.FinalizeTransaction(ctx, parent2))

		_, err = store.Spend(ctx, child2, 104)
		require.NoError(t, err, "child must be spendable once the parent is rolled forward")
	})

	t.Run("a creating child with an absent input is not falsely blessed", func(t *testing.T) {
		// absentGrandparent is a valid tx that is NEVER stored (distinct txid via Version),
		// so the child's input UTXO record does not exist → the spend hits ErrTxNotFound.
		absentGrandparent := spendableChildTx(t, tests.ParentTx, 0)
		absentGrandparent.Version = 99

		child := spendableChildTx(t, absentGrandparent, 0)

		// The child itself exists in the tentative creating state.
		_, err := store.Create(ctx, child, 106, utxo.WithCreating(true))
		require.NoError(t, err)

		// Spending the child's inputs must surface ErrTxNotFound: the input record is absent
		// and a Creating=true child must NOT satisfy the "already blessed" fallback. Pre-fix,
		// the fallback keyed on bare child existence and would have swallowed the error,
		// letting the sweep finalize a tx whose input was never spent.
		spends, spendErr := store.Spend(ctx, child, 106)

		sawNotFound := errors.Is(spendErr, errors.ErrTxNotFound)
		for _, sp := range spends {
			if sp != nil && sp.Err != nil && errors.Is(sp.Err, errors.ErrTxNotFound) {
				sawNotFound = true
			}
		}

		require.True(t, sawNotFound, "a creating child with an absent input must surface ErrTxNotFound, not be falsely blessed; err=%v spends=%v", spendErr, spends)

		// It must remain creating (never finalized off an unspent input).
		got := &meta.Data{}
		require.NoError(t, store.GetMeta(ctx, child.TxIDChainHash(), got))
		require.True(t, got.Creating, "the child must remain in the creating state, not finalized")
	})

	t.Run("double-spend: creating loser lands in conflicting terminal state", func(t *testing.T) {
		// parent3 and a competitor both spend grandparent output 1.
		parent3 := spendableChildTx(t, tests.ParentTx, 1)

		competitor := parent3.Clone()
		competitor.Version = 7 // distinct txid, same input

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

		// Resolve to the conflicting terminal state (what the validator/sweeper do).
		_, _, err = utxo.MarkConflictingRecursively(ctx, store, []chainhash.Hash{*parent3.TxIDChainHash()})
		require.NoError(t, err)
		require.NoError(t, store.FinalizeTransaction(ctx, parent3))

		got := &meta.Data{}
		require.NoError(t, store.GetMeta(ctx, parent3.TxIDChainHash(), got))
		require.True(t, got.Conflicting, "the loser must be marked conflicting")
		require.False(t, got.Creating, "the loser must be finalized out of the creating state")
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
