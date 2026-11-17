package sql

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
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

// openPostgresChainWriteStore opens a store on a new PostgreSQL database and returns it
// with the database's URL, so a test can open a second store on the same database the way
// a second process would.
func openPostgresChainWriteStore(t *testing.T) (*SQL, *url.URL) {
	t.Helper()

	if testing.Short() {
		t.Skip("Skipping PostgreSQL tests in short mode")
	}

	connStr, teardown, err := postgres.SetupTestPostgresContainer()
	if err != nil {
		t.Skipf("PostgreSQL container not available: %v", err)
	}

	t.Cleanup(func() { _ = teardown() })

	storeURL, err := url.Parse(connStr)
	require.NoError(t, err)

	s := openStoreOn(t, storeURL)

	return s, storeURL
}

func openStoreOn(t *testing.T, storeURL *url.URL) *SQL {
	t.Helper()

	s, err := New(ulogger.TestLogger{}, storeURL, test.CreateBaseTestSettings(t))
	require.NoError(t, err)

	t.Cleanup(func() { _ = s.Close(context.Background()) })
	waitForStartupRebuild(t, s)

	return s
}

// TestNewCreatesTheChainWriteRow_PostgreSQL: a walk that finds no row cannot see a write
// that is creating it, so New must create the row before any walk runs. On a new database
// the genesis StoreBlock creates it anyway; the case New covers is a database that already
// holds its chain but has no row, such as one written before the row existed.
func TestNewCreatesTheChainWriteRow_PostgreSQL(t *testing.T) {
	first, storeURL := openPostgresChainWriteStore(t)

	_, err := first.db.Exec(`DELETE FROM state WHERE key = $1`, chainWriteStateKey)
	require.NoError(t, err)

	openStoreOn(t, storeURL)

	var count int
	require.NoError(t, first.db.QueryRow(`SELECT COUNT(*) FROM state WHERE key = $1`, chainWriteStateKey).Scan(&count))
	require.Equal(t, 1, count)
}

// TestNewDoesNotWaitOnAnOpenChainWrite_PostgreSQL: a store opening on a database where
// another process holds the shared row in an open chain write must not wait for that
// write to end. The holder can be a writer queued behind a full on_main_chain walk, so
// the wait could last as long as the walk.
func TestNewDoesNotWaitOnAnOpenChainWrite_PostgreSQL(t *testing.T) {
	holder, storeURL := openPostgresChainWriteStore(t)

	tx, err := holder.db.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	t.Cleanup(func() { _ = tx.Rollback() })

	require.NoError(t, holder.lockChainWrites(context.Background(), tx))

	opened := make(chan error, 1)

	go func() {
		second, newErr := New(ulogger.TestLogger{}, storeURL, test.CreateBaseTestSettings(t))
		if newErr == nil {
			_ = second.Close(context.Background())
		}

		opened <- newErr
	}()

	select {
	case newErr := <-opened:
		require.NoError(t, newErr)
	case <-time.After(10 * time.Second):
		require.NoError(t, tx.Rollback(), "release the row so New can return")
		_ = <-opened
		t.Fatal("New waited on another process's open chain write")
	}
}
