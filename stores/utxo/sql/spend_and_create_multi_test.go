package sql

import (
	"context"
	"testing"

	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
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
	t.Run("cancelled between levels", func(t *testing.T) { tests.SpendAndCreateMultiCancelledBetweenLevels(t, newStore(t)) })
	t.Run("parents deduplicated", func(t *testing.T) { tests.SpendAndCreateMultiParentsDeduplicated(t, newStore(t)) })
}

func TestSpendAndCreateMultiSQLite(t *testing.T) {
	spendAndCreateMultiSuite(t, func(t *testing.T) utxostore.Store {
		db, _ := setup(context.Background(), t)
		return db
	})
}

func TestSpendAndCreateMultiPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	db, _ := setupPostgresStore(t)

	spendAndCreateMultiSuite(t, func(t *testing.T) utxostore.Store { return db })
}
