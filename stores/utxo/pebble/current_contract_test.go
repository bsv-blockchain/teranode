package pebble

import (
	"context"
	"testing"

	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

func spendAndCreateMultiSuite(t *testing.T, newStore func(t *testing.T) utxostore.Store) {
	t.Run("matches a loop of SpendAndCreate", func(t *testing.T) { tests.SpendAndCreateMultiMatchesLoop(t, newStore(t)) })
	t.Run("a spend fails partway", func(t *testing.T) { tests.SpendAndCreateMultiSpendFailsPartway(t, newStore(t)) })
	t.Run("a parent in the list exists", func(t *testing.T) { tests.SpendAndCreateMultiParentExists(t, newStore(t)) })
	t.Run("a refusal writes nothing", func(t *testing.T) { tests.SpendAndCreateMultiRefusalWritesNothing(t, newStore(t)) })
	t.Run("repeat at every cut point", func(t *testing.T) { tests.SpendAndCreateMultiRepeatAtCutPoints(t, newStore(t)) })
	t.Run("refusals write nothing", func(t *testing.T) { tests.SpendAndCreateMultiRefusals(t, newStore(t)) })
	t.Run("an empty list", func(t *testing.T) { tests.SpendAndCreateMultiEmptyList(t, newStore(t)) })
	t.Run("levels parallel and ordered", func(t *testing.T) { tests.SpendAndCreateMultiLevelsParallelAndOrdered(t, newStore(t)) })
	t.Run("concurrency bound", func(t *testing.T) { tests.SpendAndCreateMultiConcurrencyBound(t, newStore(t)) })
	t.Run("result mapping", func(t *testing.T) { tests.SpendAndCreateMultiResultMapping(t, newStore(t)) })
	t.Run("options pass through", func(t *testing.T) { tests.SpendAndCreateMultiOptionsPassThrough(t, newStore(t)) })
	t.Run("subtree indexes", func(t *testing.T) { tests.SpendAndCreateMultiSubtreeIdxs(t, newStore(t)) })
	t.Run("cancelled between levels", func(t *testing.T) { tests.SpendAndCreateMultiCancelledBetweenLevels(t, newStore(t)) })
	t.Run("parents deduplicated", func(t *testing.T) { tests.SpendAndCreateMultiParentsDeduplicated(t, newStore(t)) })
	t.Run("below checkpoint matches a loop", func(t *testing.T) { tests.SpendAndCreateMultiBelowCheckpointMatchesLoop(t, newStore(t)) })
	t.Run("below checkpoint two waves", func(t *testing.T) { tests.SpendAndCreateMultiBelowCheckpointTwoWaves(t, newStore(t)) })
	t.Run("below checkpoint create fails", func(t *testing.T) { tests.SpendAndCreateMultiBelowCheckpointCreateFails(t, newStore(t)) })
	t.Run("below checkpoint existing respent", func(t *testing.T) { tests.SpendAndCreateMultiBelowCheckpointExistingRespent(t, newStore(t)) })
	t.Run("below checkpoint missing parent", func(t *testing.T) { tests.SpendAndCreateMultiBelowCheckpointMissingParent(t, newStore(t)) })
	t.Run("below checkpoint existing parent pruned", func(t *testing.T) { tests.SpendAndCreateMultiBelowCheckpointExistingParentPruned(t, newStore(t)) })
	t.Run("below checkpoint second wave missing parent", func(t *testing.T) { tests.SpendAndCreateMultiBelowCheckpointSecondWaveMissingParent(t, newStore(t)) })
	t.Run("below checkpoint cancel mid-call", func(t *testing.T) { tests.SpendAndCreateMultiBelowCheckpointCancelMidCall(t, newStore(t)) })
	t.Run("below checkpoint rollback outlives deadline", func(t *testing.T) { tests.SpendAndCreateMultiBelowCheckpointRollbackOutlivesDeadline(t, newStore(t)) })
}

func TestSpendAndCreateMulti(t *testing.T) {
	spendAndCreateMultiSuite(t, func(t *testing.T) utxostore.Store { return newTestStore(t) })
}

func TestParentOutputsForValidation(t *testing.T) {
	tests.ParentOutputsForValidation(t, newTestStore(t))
}

func TestParentOutputsReadsOutputsNotInputs(t *testing.T) {
	tests.ParentOutputsReadsOutputsNotInputs(t, newTestStore(t))
}

func TestParentOutputsFaultIsNotAbsence(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	broken := newExtendedTx(t, 1, 42_000)
	healthy := newExtendedTx(t, 1, 43_000)
	mustCreate(t, store, broken, 100)
	mustCreate(t, store, healthy, 100)

	require.NoError(t, store.deleteDirect(payloadKey(broken.TxIDChainHash()[:])))
	answers, err := store.ParentOutputsForValidation(ctx, []utxostore.Outpoint{
		{TxID: *broken.TxIDChainHash()},
		{TxID: *healthy.TxIDChainHash()},
	})
	require.NoError(t, err)
	require.Len(t, answers, 2)
	require.Error(t, answers[0].Err)
	require.Equal(t, utxostore.ParentOutputUnknown, answers[0].Status)
	require.NoError(t, answers[1].Err)
	require.Equal(t, utxostore.ParentOutputNotMined, answers[1].Status)
	require.Equal(t, healthy.Outputs[0].Satoshis, answers[1].Satoshis)

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = store.ParentOutputsForValidation(cancelled, nil)
	require.ErrorIs(t, err, context.Canceled)

	require.NoError(t, store.Close(ctx))
	_, err = store.ParentOutputsForValidation(ctx, []utxostore.Outpoint{{TxID: *healthy.TxIDChainHash()}})
	require.Error(t, err)
}
