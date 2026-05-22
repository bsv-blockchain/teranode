package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

// TestMinedThenSpendAllPrunes_SQLite exercises the per-row spend path
// (trySendSpendBatchPerRow) via the in-memory SQLite backend.
func TestMinedThenSpendAllPrunes_SQLite(t *testing.T) {
	// Pruner service is a process-wide singleton; reset so it binds to THIS
	// test's Store rather than a stale one from a different backend or run.
	ResetPrunerServiceForTests()
	t.Cleanup(ResetPrunerServiceForTests)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _ := setup(ctx, t)

	provider, ok := any(store).(pruner.PrunerServiceProvider)
	require.True(t, ok, "store must implement pruner.PrunerServiceProvider")
	prunerSvc, err := provider.GetPrunerService()
	require.NoError(t, err)
	require.NotNil(t, prunerSvc, "pruner service must be available")
	prunerSvc.Start(ctx)

	tests.MinedThenSpendAllPrunes(t, store, prunerSvc)
}

// TestMinedThenSpendAllPrunes_Postgres exercises the Postgres bulk spend path
// (trySendSpendBatchBulk) via a testcontainer-backed Postgres database.
func TestMinedThenSpendAllPrunes_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	ResetPrunerServiceForTests()
	t.Cleanup(ResetPrunerServiceForTests)

	store, baseCtx := setupPostgresStore(t)

	// Wrap in a cancelable context and cancel in t.Cleanup. t.Cleanup runs in
	// LIFO order, so this cancel (registered AFTER ResetPrunerServiceForTests)
	// executes FIRST — stopping the pruner goroutine before the singleton
	// pointer is cleared. ResetPrunerServiceForTests only clears the pointer;
	// it does NOT stop a started pruner, so without explicit cancellation the
	// goroutine would keep running after the test returns and the testcontainer
	// terminates.
	ctx, cancel := context.WithCancel(baseCtx)
	t.Cleanup(cancel)

	provider, ok := any(store).(pruner.PrunerServiceProvider)
	require.True(t, ok, "store must implement pruner.PrunerServiceProvider")
	prunerSvc, err := provider.GetPrunerService()
	require.NoError(t, err)
	require.NotNil(t, prunerSvc, "pruner service must be available")
	prunerSvc.Start(ctx)

	tests.MinedThenSpendAllPrunes(t, store, prunerSvc)
}
