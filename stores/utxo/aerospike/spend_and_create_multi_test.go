package aerospike_test

import (
	"testing"

	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
)

func TestSpendAndCreateMulti(t *testing.T) {
	t.Parallel()

	// Every case runs in parallel with a store of its own on the shared server.
	newStore := func(t *testing.T) *teranode_aerospike.Store {
		t.Parallel()

		_, store, _, deferFn := initAerospike(t, test.CreateBaseTestSettings(t), ulogger.NewErrorTestLogger(t))
		t.Cleanup(deferFn)

		return store
	}

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
