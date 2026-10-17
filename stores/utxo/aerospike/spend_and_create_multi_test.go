package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
)

func TestSpendAndCreateMulti(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	_, store, _, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	t.Run("matches a loop of SpendAndCreate", func(t *testing.T) { tests.SpendAndCreateMultiMatchesLoop(t, store) })
	t.Run("a spend fails partway", func(t *testing.T) { tests.SpendAndCreateMultiSpendFailsPartway(t, store) })
	t.Run("a parent in the list exists", func(t *testing.T) { tests.SpendAndCreateMultiParentExists(t, store) })
	t.Run("a refusal writes nothing", func(t *testing.T) { tests.SpendAndCreateMultiRefusalWritesNothing(t, store) })
	t.Run("repeat at every cut point", func(t *testing.T) { tests.SpendAndCreateMultiRepeatAtCutPoints(t, store) })
	t.Run("refusals write nothing", func(t *testing.T) { tests.SpendAndCreateMultiRefusals(t, store) })
	t.Run("an empty list", func(t *testing.T) { tests.SpendAndCreateMultiEmptyList(t, store) })
	t.Run("levels parallel and ordered", func(t *testing.T) { tests.SpendAndCreateMultiLevelsParallelAndOrdered(t, store) })
	t.Run("concurrency bound", func(t *testing.T) { tests.SpendAndCreateMultiConcurrencyBound(t, store) })
	t.Run("result mapping", func(t *testing.T) { tests.SpendAndCreateMultiResultMapping(t, store) })
	t.Run("options pass through", func(t *testing.T) { tests.SpendAndCreateMultiOptionsPassThrough(t, store) })
	t.Run("subtree indexes", func(t *testing.T) { tests.SpendAndCreateMultiSubtreeIdxs(t, store) })
	t.Run("cancelled between levels", func(t *testing.T) { tests.SpendAndCreateMultiCancelledBetweenLevels(t, store) })
	t.Run("parents deduplicated", func(t *testing.T) { tests.SpendAndCreateMultiParentsDeduplicated(t, store) })
}
