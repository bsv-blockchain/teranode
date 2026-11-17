package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// newPostgresChainWriteStore opens a PostgreSQL store holding block1 to block3 on the main
// chain and blockAlternative2 on a fork.
func newPostgresChainWriteStore(t *testing.T) *SQL {
	t.Helper()

	if testing.Short() {
		t.Skip("Skipping PostgreSQL tests in short mode")
	}

	s, cleanup := setupPostgresTestStore(t)
	t.Cleanup(cleanup)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	waitForStartupRebuild(t, s)

	storeBlocks(t, s, block1, block2, block3, blockAlternative2)

	return s
}

// hideStateTable renames the state table until the test ends, so every statement on the
// shared row fails the way a lost connection or a dropped table would.
func hideStateTable(t *testing.T, s *SQL) {
	t.Helper()

	_, err := s.db.Exec(`ALTER TABLE state RENAME TO state_hidden`)
	require.NoError(t, err)

	t.Cleanup(func() { _, _ = s.db.Exec(`ALTER TABLE state_hidden RENAME TO state`) })
}

// TestChainWriteLockFailureStopsEachWriter_PostgreSQL: a chain write that cannot lock the
// shared row must not go ahead without it, or a walk in another process could commit its
// older view over the write. Each writer returns the error and leaves the blocks as they were.
func TestChainWriteLockFailureStopsEachWriter_PostgreSQL(t *testing.T) {
	s := newPostgresChainWriteStore(t)
	ctx := context.Background()

	hideStateTable(t, s)

	t.Run("DeleteBlock", func(t *testing.T) {
		require.Error(t, s.DeleteBlock(ctx, block3.Hash()))

		exists, err := s.GetBlockExists(ctx, block3.Hash())
		require.NoError(t, err)
		require.True(t, exists, "the delete must roll back")
	})

	t.Run("InvalidateBlock", func(t *testing.T) {
		_, err := s.InvalidateBlock(ctx, block3.Hash())
		require.Error(t, err)

		var invalid bool
		require.NoError(t, s.db.QueryRow(`SELECT invalid FROM blocks WHERE hash = $1`, block3.Hash().CloneBytes()).Scan(&invalid))
		require.False(t, invalid, "the invalidation must roll back")
		require.True(t, getOnMainChain(t, s, block3.Hash().CloneBytes()))
	})

	t.Run("RevalidateBlock", func(t *testing.T) {
		_, err := s.db.Exec(`UPDATE blocks SET invalid = true WHERE hash = $1`, blockAlternative2.Hash().CloneBytes())
		require.NoError(t, err)

		require.Error(t, s.RevalidateBlock(ctx, blockAlternative2.Hash()))

		var invalid bool
		require.NoError(t, s.db.QueryRow(`SELECT invalid FROM blocks WHERE hash = $1`, blockAlternative2.Hash().CloneBytes()).Scan(&invalid))
		require.True(t, invalid, "the revalidation must roll back")
	})

	t.Run("StoreBlock on a fork", func(t *testing.T) {
		alt3 := createBlock3OnFork(blockAlternative2)

		_, _, err := s.StoreBlock(ctx, alt3, "")
		require.Error(t, err)

		exists, err := s.GetBlockExists(ctx, alt3.Hash())
		require.NoError(t, err)
		require.False(t, exists, "the insert must roll back")
	})
}

// TestChainWriteLockWalkCheckFailureFailsTheWalk_PostgreSQL: a walk that cannot check the
// shared row cannot know whether a write landed after its snapshot, so it must not commit.
// The error is not a serialization failure, so it is reported rather than retried.
func TestChainWriteLockWalkCheckFailureFailsTheWalk_PostgreSQL(t *testing.T) {
	s := newPostgresChainWriteStore(t)

	// A stale flag the walk would correct if it committed.
	_, err := s.db.Exec(`UPDATE blocks SET on_main_chain = false WHERE hash = $1`, block2.Hash().CloneBytes())
	require.NoError(t, err)

	hideStateTable(t, s)

	err = s.rebuildOnMainChainFlag(context.Background(), true)
	require.ErrorContains(t, err, "failed to check for chain writes since the snapshot")
	require.False(t, getOnMainChain(t, s, block2.Hash().CloneBytes()), "the walk must roll back")
}

// TestChainWriteLockWalkWithoutTheRow_PostgreSQL: New creates the shared row before any
// walk runs, so a walk that finds none has nothing to check against and commits rather than
// failing every repair on a database where the row was removed by hand.
func TestChainWriteLockWalkWithoutTheRow_PostgreSQL(t *testing.T) {
	s := newPostgresChainWriteStore(t)

	_, err := s.db.Exec(`UPDATE blocks SET on_main_chain = false WHERE hash = $1`, block2.Hash().CloneBytes())
	require.NoError(t, err)

	_, err = s.db.Exec(`DELETE FROM state WHERE key = $1`, chainWriteStateKey)
	require.NoError(t, err)

	require.NoError(t, s.rebuildOnMainChainFlag(context.Background(), true))
	require.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()))
}

// TestEnsureChainWriteRowReportsFailure_PostgreSQL: New must not start a store whose walks
// would find no row while a write is creating it, so a failure to create the row is an error.
func TestEnsureChainWriteRowReportsFailure_PostgreSQL(t *testing.T) {
	s := newPostgresChainWriteStore(t)

	hideStateTable(t, s)

	require.ErrorContains(t, s.ensureChainWriteRow(context.Background()), chainWriteStateKey)
}
