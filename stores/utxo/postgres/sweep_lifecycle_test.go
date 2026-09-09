package postgres

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestStoreStart_LaunchesSweepOnce asserts the lifecycle-ordering fix:
//
//  1. store.Start() launches the DAH sweep (Worker 2) via the store's OWN pruner
//     service — the same cached instance the external pruner Server obtains
//     through GetPrunerService(). Because Start() runs after New()/createSchema,
//     the sweep never races the startup migration's ACCESS EXCLUSIVE lock.
//  2. The launch is idempotent: a second Start() on the same service (as the
//     external pruner Server issues at services/pruner/server.go) is a no-op —
//     the cursor is started exactly once, not double-launched.
func TestStoreStart_LaunchesSweepOnce(t *testing.T) {
	store, ctx := setupTestStore(t)

	// setupTestStore does NOT call Start(); the cursor must not be running yet.
	svc, err := store.GetPrunerService()
	require.NoError(t, err)
	pgps, ok := svc.(*postgresPrunerService)
	require.True(t, ok, "postgres store must provide a *postgresPrunerService")

	pgps.mu.Lock()
	startedBefore := pgps.cursorStarted
	pgps.mu.Unlock()
	require.False(t, startedBefore, "sweep must not be running before store.Start()")

	// store.Start() must launch the sweep on this exact cached instance.
	store.Start(ctx)

	pgps.mu.Lock()
	startedAfter := pgps.cursorStarted
	firstCancel := pgps.cursorCancel
	pgps.mu.Unlock()
	require.True(t, startedAfter, "store.Start() must launch the DAH sweep")
	require.NotNil(t, firstCancel, "cursor cancel must be set once launched")

	// GetPrunerService() must return the SAME instance store.Start launched, so
	// the external pruner Server drives the identical cursor (not a second one).
	svc2, err := store.GetPrunerService()
	require.NoError(t, err)
	require.Same(t, svc, svc2, "GetPrunerService must return the store's cached service")

	// The external pruner Server's later Start(ctx) call must be a no-op. The
	// cursorStarted guard returns BEFORE assigning cursorCancel, so on a no-op the
	// field is the identical value store.Start set; a real re-launch would install
	// a fresh context.WithCancel and, critically, break the single-cursor
	// invariant that stop() relies on (a double Add(2) on cursorWg would make the
	// eventual stop() wait on goroutines that were never spawned, deadlocking).
	svc2.Start(ctx)

	pgps.mu.Lock()
	secondCancel := pgps.cursorCancel
	pgps.mu.Unlock()
	require.NotNil(t, secondCancel)
	require.Equal(t,
		funcPtr(firstCancel), funcPtr(secondCancel),
		"external prunerService.Start must not replace the cursor cancel — sweep launches exactly once")

	// The single-launch invariant is what makes teardown safe: stop() waits out
	// exactly the goroutines one launch spawned. If the second Start had
	// re-launched (double Add on cursorWg without matching Done), this stop() would
	// block forever. Completing it proves the second Start was a genuine no-op.
	done := make(chan struct{})
	go func() {
		defer close(done)
		pgps.stop()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stop() hung — the second Start double-launched the cursor (broke the single-launch invariant)")
	}
}

// TestSpentBitmapMigration_SkippedWhenColumnsExist covers the skip-if-applied
// guard on the startup migration. A fresh createSchema has already added both
// fold columns, so applySpentBitmapMigration must report applied=false (it must
// NOT re-run the ALTER, which would re-take the ACCESS EXCLUSIVE lock that
// deadlocked the sweep). Calling it a second time must likewise be a no-op.
func TestSpentBitmapMigration_SkippedWhenColumnsExist(t *testing.T) {
	store, ctx := setupTestStore(t)

	// Sanity: createSchema (run in New) has already added both columns.
	var n int
	require.NoError(t, store.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM information_schema.columns
		WHERE table_name = 'txs'
		  AND column_name IN ('spent_bits', 'last_spend_height')`).Scan(&n))
	require.Equal(t, 2, n, "both fold columns must exist after createSchema")

	// The guard must skip the ALTER because both columns already exist.
	applied, err := applySpentBitmapMigration(ctx, store.pool)
	require.NoError(t, err)
	require.False(t, applied, "migration must be SKIPPED when both columns already exist (no redundant ACCESS EXCLUSIVE lock)")

	// Idempotent: a second call is still a skip.
	applied, err = applySpentBitmapMigration(ctx, store.pool)
	require.NoError(t, err)
	require.False(t, applied, "second call must also skip")
}

// TestSpentBitmapMigration_RunsWhenColumnsMissing covers the fresh-DB path: with
// the columns dropped, the guard must run the ALTER (applied=true) and recreate
// them — proving the skip is genuinely conditional, not an unconditional no-op.
func TestSpentBitmapMigration_RunsWhenColumnsMissing(t *testing.T) {
	store, ctx := setupTestStore(t)

	// Drop the fold columns to simulate a pre-migration (fresh v15) DB.
	_, err := store.pool.Exec(ctx, `
		ALTER TABLE txs DROP COLUMN IF EXISTS spent_bits;
		ALTER TABLE txs DROP COLUMN IF EXISTS last_spend_height;`)
	require.NoError(t, err)

	var n int
	require.NoError(t, store.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM information_schema.columns
		WHERE table_name = 'txs'
		  AND column_name IN ('spent_bits', 'last_spend_height')`).Scan(&n))
	require.Zero(t, n, "precondition: fold columns dropped")

	// With columns missing, the guard MUST run the ALTER.
	applied, err := applySpentBitmapMigration(ctx, store.pool)
	require.NoError(t, err)
	require.True(t, applied, "migration must RUN when columns are missing")

	require.NoError(t, store.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM information_schema.columns
		WHERE table_name = 'txs'
		  AND column_name IN ('spent_bits', 'last_spend_height')`).Scan(&n))
	require.Equal(t, 2, n, "both columns recreated by the migration")
}

// funcPtr returns a comparable identity token for a context.CancelFunc. Function
// values are not directly comparable in Go, so we use the underlying code/closure
// pointer via reflection: a re-launch would install a different CancelFunc value.
func funcPtr(f context.CancelFunc) uintptr {
	return reflect.ValueOf(f).Pointer()
}
