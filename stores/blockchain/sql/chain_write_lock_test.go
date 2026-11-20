package sql

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
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
//
// Each subtest acts on the fork block blockAlternative2, so the best tip never moves and
// recordBestTipDrop never reads the state table: the lock is the only statement that can
// fail. Each also has its own store, so no subtest leaves a block invalid for the next.
func TestChainWriteLockFailureStopsEachWriter_PostgreSQL(t *testing.T) {
	ctx := context.Background()

	t.Run("DeleteBlock", func(t *testing.T) {
		s := newPostgresChainWriteStore(t)
		hideStateTable(t, s)

		require.Error(t, s.DeleteBlock(ctx, blockAlternative2.Hash()))

		exists, err := s.GetBlockExists(ctx, blockAlternative2.Hash())
		require.NoError(t, err)
		require.True(t, exists, "the delete must roll back")
	})

	t.Run("InvalidateBlock", func(t *testing.T) {
		s := newPostgresChainWriteStore(t)
		hideStateTable(t, s)

		_, err := s.InvalidateBlock(ctx, blockAlternative2.Hash())
		require.Error(t, err)

		var invalid bool
		require.NoError(t, s.db.QueryRow(`SELECT invalid FROM blocks WHERE hash = $1`, blockAlternative2.Hash().CloneBytes()).Scan(&invalid))
		require.False(t, invalid, "the invalidation must roll back")
	})

	t.Run("RevalidateBlock", func(t *testing.T) {
		s := newPostgresChainWriteStore(t)

		_, err := s.db.Exec(`UPDATE blocks SET invalid = true WHERE hash = $1`, blockAlternative2.Hash().CloneBytes())
		require.NoError(t, err)

		hideStateTable(t, s)

		require.Error(t, s.RevalidateBlock(ctx, blockAlternative2.Hash()))

		var invalid bool
		require.NoError(t, s.db.QueryRow(`SELECT invalid FROM blocks WHERE hash = $1`, blockAlternative2.Hash().CloneBytes()).Scan(&invalid))
		require.True(t, invalid, "the revalidation must roll back")
	})

	t.Run("StoreBlock on a fork", func(t *testing.T) {
		s := newPostgresChainWriteStore(t)
		hideStateTable(t, s)

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
// failing every repair on a database where the row was removed by hand. It warns when it
// does, because that warning is the only sign the cross-process check is off.
func TestChainWriteLockWalkWithoutTheRow_PostgreSQL(t *testing.T) {
	s := newPostgresChainWriteStore(t)

	_, err := s.db.Exec(`UPDATE blocks SET on_main_chain = false WHERE hash = $1`, block2.Hash().CloneBytes())
	require.NoError(t, err)

	_, err = s.db.Exec(`DELETE FROM state WHERE key = $1`, chainWriteStateKey)
	require.NoError(t, err)

	logger := &warnRecordingLogger{}
	s.logger = logger

	require.NoError(t, s.rebuildOnMainChainFlag(context.Background(), true))
	require.True(t, getOnMainChain(t, s, block2.Hash().CloneBytes()))
	require.True(t, logger.warnedAbout(chainWriteStateKey), "a walk that commits without the row must warn")
}

// warnRecordingLogger records each Warnf line, so a test can assert a path warned.
// Everything else is TestLogger's.
type warnRecordingLogger struct {
	ulogger.TestLogger

	mu    sync.Mutex
	warns []string
}

func (l *warnRecordingLogger) Warnf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.warns = append(l.warns, fmt.Sprintf(format, args...))
}

func (l *warnRecordingLogger) warnedAbout(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, w := range l.warns {
		if strings.Contains(w, substr) {
			return true
		}
	}

	return false
}

// TestEnsureChainWriteRowReportsFailure_PostgreSQL: New must not start a store whose walks
// would find no row while a write is creating it, so a failure to read or to create the
// row is an error.
func TestEnsureChainWriteRowReportsFailure_PostgreSQL(t *testing.T) {
	t.Run("read fails", func(t *testing.T) {
		s := newPostgresChainWriteStore(t)

		hideStateTable(t, s)

		require.ErrorContains(t, s.ensureChainWriteRow(context.Background()), "could not read")
	})

	t.Run("insert fails", func(t *testing.T) {
		s := newPostgresChainWriteStore(t)

		_, err := s.db.Exec(`DELETE FROM state WHERE key = $1`, chainWriteStateKey)
		require.NoError(t, err)

		_, err = s.db.Exec(`
			CREATE OR REPLACE FUNCTION refuse_state_insert() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN
				RAISE EXCEPTION 'state insert refused';
			END $$`)
		require.NoError(t, err)

		_, err = s.db.Exec(`CREATE TRIGGER refuse_state_insert BEFORE INSERT ON state
			FOR EACH ROW EXECUTE FUNCTION refuse_state_insert()`)
		require.NoError(t, err)

		t.Cleanup(func() { _, _ = s.db.Exec(`DROP TRIGGER IF EXISTS refuse_state_insert ON state`) })

		require.ErrorContains(t, s.ensureChainWriteRow(context.Background()), "could not create")
	})
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
// that is creating it, so New must create the row. It checks only that the row exists once
// New has returned, not that New creates it before its own startup walk. On a new database
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
	tSettings := test.CreateBaseTestSettings(t)

	go func() {
		second, newErr := New(ulogger.TestLogger{}, storeURL, tSettings)
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

// holdBlocksWrites makes the next transaction that inserts, updates or deletes a row of
// blocks stop inside that statement, holding every lock it has taken so far, until the
// returned release is called. It is a trigger rather than a hook in the store, so it holds
// any writer at the same point without a seam in production code. Rows the on_main_chain
// walk leaves unchanged fire no row trigger, so a walk is never held by it.
func holdBlocksWrites(t *testing.T, s *SQL) (release func()) {
	t.Helper()

	ctx := context.Background()

	_, err := s.db.Exec(`
		CREATE FUNCTION hold_blocks_write() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			PERFORM pg_advisory_xact_lock(2218);
			RETURN NULL;
		END $$`)
	require.NoError(t, err)

	_, err = s.db.Exec(`CREATE TRIGGER hold_blocks_write AFTER INSERT OR UPDATE OR DELETE ON blocks
		FOR EACH ROW EXECUTE FUNCTION hold_blocks_write()`)
	require.NoError(t, err)

	conn, err := s.db.Conn(ctx)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, `SELECT pg_advisory_lock(2218)`)
	require.NoError(t, err)

	var once sync.Once

	release = func() {
		once.Do(func() {
			_, _ = conn.ExecContext(ctx, `SELECT pg_advisory_unlock(2218)`)
			_ = conn.Close()
			_, _ = s.db.Exec(`DROP TRIGGER IF EXISTS hold_blocks_write ON blocks`)
		})
	}

	t.Cleanup(release)

	return release
}

// waitForHeldBlocksWrite waits until a writer is stopped by holdBlocksWrites.
func waitForHeldBlocksWrite(t *testing.T, s *SQL) {
	t.Helper()

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var waiting int
		if !assert.NoError(c, s.db.QueryRow(`SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted`).Scan(&waiting)) {
			return
		}

		assert.Positive(c, waiting, "the writer did not reach its blocks statement")
	}, 10*time.Second, 10*time.Millisecond)
}

// TestChainWriteLockIsTakenBeforeTheWritersFlags_PostgreSQL: each chain writer must lock
// the shared row before it writes blocks. A writer that locked last would leave a window,
// between its flag writes and its lock, where a walk in another process passes its check
// and commits the view it read before the write. Each writer here is stopped inside its
// first write to blocks and held open, the way a writer queued behind a long walk is; a
// walk run in that window must fail its check with 55P03 rather than commit.
func TestChainWriteLockIsTakenBeforeTheWritersFlags_PostgreSQL(t *testing.T) {
	writers := []struct {
		name  string
		setup func(t *testing.T, s *SQL)
		write func(ctx context.Context, s *SQL) error
	}{
		{
			name: "DeleteBlock",
			write: func(ctx context.Context, s *SQL) error {
				return s.DeleteBlock(ctx, block3.Hash())
			},
		},
		{
			name: "InvalidateBlock",
			write: func(ctx context.Context, s *SQL) error {
				_, err := s.InvalidateBlock(ctx, block3.Hash())
				return err
			},
		},
		{
			name: "RevalidateBlock",
			setup: func(t *testing.T, s *SQL) {
				_, err := s.db.Exec(`UPDATE blocks SET invalid = true WHERE hash = $1`, blockAlternative2.Hash().CloneBytes())
				require.NoError(t, err)
			},
			write: func(ctx context.Context, s *SQL) error {
				return s.RevalidateBlock(ctx, blockAlternative2.Hash())
			},
		},
		{
			name: "StoreBlock on a fork",
			write: func(ctx context.Context, s *SQL) error {
				_, _, err := s.StoreBlock(ctx, createBlock3OnFork(blockAlternative2), "")
				return err
			},
		},
	}

	for _, writer := range writers {
		t.Run(writer.name, func(t *testing.T) {
			walker, other := newPostgresStorePair(t)
			ctx := context.Background()

			storeBlocks(t, walker, block1, block2, block3, blockAlternative2)

			if writer.setup != nil {
				writer.setup(t, walker)
			}

			release := holdBlocksWrites(t, walker)

			written := make(chan error, 1)

			go func() { written <- writer.write(ctx, other) }()

			waitForHeldBlocksWrite(t, walker)

			walkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			walkErr := walker.rebuildOnMainChainFlagTx(walkCtx, true, noRepairFloor)

			release()
			require.NoError(t, <-written)

			var pgErr *pgconn.PgError
			require.True(t, errors.As(walkErr, &pgErr), "the walk must fail its chain-write check, got %v", walkErr)
			require.Equal(t, "55P03", pgErr.Code)
		})
	}
}
