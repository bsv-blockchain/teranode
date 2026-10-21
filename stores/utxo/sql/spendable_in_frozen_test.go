package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	utxo2 "github.com/bsv-blockchain/teranode/test/longtest/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestSpendSpendableInHoldYieldsFrozenNotLocked pins that the alert system's
// spendableIn height gate (set by ReAssignUTXO after FreezeUTXOs) classifies
// as ErrFrozen, not ErrTxLocked, when a spend arrives before the held height.
// Conflating the two would make the validator retry a freeze that will not
// clear for up to ReAssignedUtxoSpendableAfterBlocks blocks (default 1000) --
// see the spendableIn branches in sql.go's trySendSpendBatchBulk/PerRow.
//
// There is no existing alert-system test in this package to reuse --
// FreezeUTXOs/ReAssignUTXO are exercised only by the e2e/sequential suites --
// so this drives the real alert-system API directly: FreezeUTXOs then
// ReAssignUTXO. ReAssignUTXO is called with the SAME UTXOHash the output
// already has, so the stored utxo_hash is unchanged and the real spending tx
// (built normally against the original output) still matches it, isolating
// the test to exactly the spendableIn gate rather than also changing which
// UTXO is being spent.
//
// This exercises trySendSpendBatchPerRow (the sqlitememory/non-bulk path);
// trySendSpendBatchBulk is Postgres-only (BatchSQLOperations && engine ==
// "postgres") and requires a TestContainers Postgres instance to reach, which
// is out of scope here. Both branches carry the identical spendableIn check
// and comment (see sql.go), so the fix and this pin apply to both.
func TestSpendSpendableInHoldYieldsFrozenNotLocked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, tx := setup(ctx, t)

	spendTx := utxo2.GetSpendingTx(tx, 0)

	_, err := utxoStore.Create(ctx, tx, 0)
	require.NoError(t, err)

	spends, err := utxo.GetSpends(spendTx)
	require.NoError(t, err)
	require.Len(t, spends, 1)

	err = utxoStore.FreezeUTXOs(ctx, []*utxo.Spend{spends[0]}, utxoStore.settings)
	require.NoError(t, err)

	// Reassign to the same UTXOHash: this is what stamps spendableIn (and
	// clears frozen) without altering which UTXO the real spendTx matches.
	err = utxoStore.ReAssignUTXO(ctx, spends[0], spends[0], utxoStore.settings)
	require.NoError(t, err)

	// spendableIn = GetBlockHeight() (0, unset by this test) + the default
	// ReAssignedUtxoSpendableAfterBlocks (1000); blockHeight 1 is comfortably
	// below that, so the hold must still be in effect.
	_, err = utxoStore.Spend(ctx, spendTx, 1)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrFrozen), "a spendableIn hold must classify as ErrFrozen, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrTxLocked), "a spendableIn hold must not be classified as ErrTxLocked -- that's the parent's own in-flight commit, a different condition")
}

// TestSpendableInHoldRollsBackSiblingSpends pins the consequence of the
// classification above, which is the reason it must not drift back: a
// multi-input spend that fails on one held input must not leave its other
// inputs marked spent.
//
// needsSpendRollback (sql.go) decides whether to undo the spends that already
// succeeded, and it lists ErrSpent/ErrTxConflicting/ErrFrozen/ErrUtxoHashMismatch
// -- not ErrTxLocked. So while the spendableIn hold returned ErrTxLocked, this
// path took the no-rollback branch and stranded every sibling input as spent by
// a transaction that could never be accepted, with nothing to release them.
// Classifying the hold as frozen puts it back in the rollback set, matching what
// Aerospike has always done (its Lua FROZEN_UNTIL maps to a frozen error, which
// its own rollback predicate lists).
//
// Unspend matches on spending_data and leaves the locked flag alone, so the
// rollback is safe and idempotent here.
func TestSpendableInHoldRollsBackSiblingSpends(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, tx := setup(ctx, t)

	// Two inputs: vout 0 spendable, vout 1 put under the alert system's hold.
	spendTx := utxo2.GetSpendingTx(tx, 0, 1)

	_, err := utxoStore.Create(ctx, tx, 0)
	require.NoError(t, err)

	spends, err := utxo.GetSpends(spendTx)
	require.NoError(t, err)
	require.Len(t, spends, 2)

	require.NoError(t, utxoStore.FreezeUTXOs(ctx, []*utxo.Spend{spends[1]}, utxoStore.settings))
	require.NoError(t, utxoStore.ReAssignUTXO(ctx, spends[1], spends[1], utxoStore.settings))

	_, err = utxoStore.Spend(ctx, spendTx, 1)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrFrozen), "expected the held input to fail as frozen, got: %v", err)

	// The unheld sibling must be spendable again. If the rollback did not run it
	// reads back as SPENT, owned by a transaction the store just rejected.
	resp, err := utxoStore.GetSpend(ctx, spends[0])
	require.NoError(t, err)
	require.Equal(t, int(utxo.Status_OK), resp.Status,
		"sibling input was left spent by a rejected transaction -- the failed spend did not roll back")
	require.Empty(t, resp.SpendingData, "sibling input must carry no spending data after the rollback")
}

// TestConsensusFreezeRollsBackSiblingSpends pins ErrUtxoConsensusFrozen's place in the
// rollback set: a multi-input block-context spend that fails on one consensus-frozen
// input must not leave its other inputs marked spent by a transaction the block cannot
// carry (issue #1422). The consensus verdict is its own code, distinct from ErrFrozen,
// so it has to be listed on its own.
func TestConsensusFreezeRollsBackSiblingSpends(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, tx := setup(ctx, t)

	// Two inputs: vout 0 spendable, vout 1 consensus-frozen for [100, 200).
	spendTx := utxo2.GetSpendingTx(tx, 0, 1)

	_, err := utxoStore.Create(ctx, tx, 0)
	require.NoError(t, err)

	spends, err := utxo.GetSpends(spendTx)
	require.NoError(t, err)
	require.Len(t, spends, 2)

	frozen := *spends[1]
	frozen.FreezeFrom, frozen.FreezeUntil = 100, 200
	require.NoError(t, utxoStore.FreezeUTXOs(ctx, []*utxo.Spend{&frozen}, utxoStore.settings))

	// A block-context spend (policy tier bypassed) inside the window.
	_, err = utxoStore.Spend(ctx, spendTx, 150, utxo.IgnoreFlags{IgnorePolicyFreeze: true})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrUtxoConsensusFrozen), "expected the frozen input to fail with the consensus code, got: %v", err)

	resp, err := utxoStore.GetSpend(ctx, spends[0])
	require.NoError(t, err)
	require.Equal(t, int(utxo.Status_OK), resp.Status,
		"sibling input was left spent by a rejected transaction -- the failed spend did not roll back")
	require.Empty(t, resp.SpendingData, "sibling input must carry no spending data after the rollback")
}

// TestConsensusFreezeIsJudgedBeforeSpendableIn pins the order of the alert system's two
// holds on the spend paths (issue #1422): the consensus freeze is judged before the
// reassignment-maturity hold (spendableIn), so an output carrying both is the consensus
// verdict at an in-window height, not the retryable FROZEN_UNTIL-style ErrFrozen. Swapping
// the two checks in trySendSpendBatchPerRow or trySendSpendBatchBulk fails this test.
func TestConsensusFreezeIsJudgedBeforeSpendableIn(t *testing.T) {
	t.Run("per-row", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		store, tx := setup(ctx, t)

		assertConsensusFreezeBeforeSpendableIn(t, ctx, store, tx)
	})

	t.Run("postgres bulk", func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping Postgres integration test in short mode")
		}

		store, ctx := setupPostgresStore(t)
		store.settings.UtxoStore.BatchSQLOperations = true

		require.NoError(t, store.Delete(ctx, tests.TXHash))

		assertConsensusFreezeBeforeSpendableIn(t, ctx, store, tests.Tx)
	})
}

func assertConsensusFreezeBeforeSpendableIn(t *testing.T, ctx context.Context, store *Store, tx *bt.Tx) {
	t.Helper()

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	// A recorded window covering every height, and a maturity hold far above the height
	// of the spend: both hold the output.
	_, err = store.db.ExecContext(ctx, `UPDATE outputs SET frozen = TRUE, freezeFrom = 0, spendableIn = 100000
		WHERE transaction_id = (SELECT id FROM transactions WHERE hash = $1) AND idx = 0`, tx.TxIDChainHash()[:])
	require.NoError(t, err)

	_, err = store.Spend(ctx, utxo2.GetSpendingTx(tx, 0), 100, utxo.IgnoreFlags{IgnorePolicyFreeze: true})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrUtxoConsensusFrozen), "the consensus freeze must decide before the maturity hold, got: %v", err)
}
