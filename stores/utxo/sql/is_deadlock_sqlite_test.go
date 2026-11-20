package sql

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/stretchr/testify/require"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// TestIsDeadlock_SQLiteSharedCacheTableLockIsRetryable produces the error two
// pooled connections raise against each other on the sqlitememory engine and
// asserts that both retry classifiers, isDeadlock for the spend batch and
// isLockError for create, treat it as retryable.
//
// InitSQLiteDB opens sqlitememory as a shared-cache in-memory database with a
// pool of five connections. A writer that wants a table another connection's
// transaction holds waits for it, and when the two wait for each other the
// engine breaks the cycle with SQLITE_LOCKED and the message "database table is
// locked: database is deadlocked", not SQLITE_BUSY's "database is locked". The
// legacy historical replay (services/legacy/netsync,
// TestLegacyHistoricalTestnetSync/default-settings) hits this collision when
// subtree validation runs a block's creates and spend batches in parallel over
// this store.
//
// This is a regression guard on the real engine error, not coverage of the
// extended-code mask. On modernc.org/sqlite v1.54.0 the cycle is reported with
// the plain primary code 6, not an extended LOCKED_SHAREDCACHE (262), so both
// classifiers already matched it before the mask was added to isLockError, and
// this test stays green with the mask removed.
// TestIsSQLiteLockCode_ExtendedCodesAreStillLocks in util/usql and
// TestIsLockError_SQLiteBusySnapshotIsRetryable below are the tests that pin
// the mask. String fixtures for the same message are in spend_order_test.go and
// parent_outputs_test.go; this test exists because only a real *sqlite.Error
// reaches the typed code arms.
func TestIsDeadlock_SQLiteSharedCacheTableLockIsRetryable(t *testing.T) {
	ctx := context.Background()

	db, err := usql.Open("sqlite", "file:is_deadlock_sqlite_test?mode=memory&cache=shared")
	require.NoError(t, err)
	t.Cleanup(func() { closeWithin(t, "database", db.Close) })

	db.SetMaxOpenConns(5)

	_, err = db.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `CREATE TABLE u (id INTEGER PRIMARY KEY, v INTEGER)`)
	require.NoError(t, err)

	// A shared-cache read lock is held for the rest of the transaction, so a
	// transaction that has read a table keeps every other writer of it waiting.
	// That is the spend batch's shape: its SELECT over outputs holds the table
	// while a create's transaction wants to write it.
	begin := func(table string) *sql.Tx {
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { closeWithin(t, "connection", conn.Close) })

		txn, err := conn.BeginTx(ctx, nil)
		require.NoError(t, err)

		var n int
		require.NoError(t, txn.QueryRowContext(ctx, `SELECT count(*) FROM `+table).Scan(&n))

		return txn
	}

	// Each transaction holds a read lock on one table and then wants to write
	// the other's, so each waits for the other and the engine breaks the cycle.
	holdsT := begin("t")
	holdsU := begin("u")

	type outcome struct {
		txn *sql.Tx
		err error
	}

	results := make(chan outcome, 2)

	go func() {
		_, err := holdsT.ExecContext(ctx, `INSERT INTO u (v) VALUES (2)`)
		results <- outcome{txn: holdsT, err: err}
	}()
	go func() {
		_, err := holdsU.ExecContext(ctx, `INSERT INTO t (v) VALUES (2)`)
		results <- outcome{txn: holdsU, err: err}
	}()

	var refused outcome

	select {
	case refused = <-results:
	case <-time.After(30 * time.Second):
		t.Fatal("neither writer was refused within 30s: the engine did not report the lock cycle")
	}

	require.Error(t, refused.err, "one writer of the cycle must be refused")
	require.Contains(t, refused.err.Error(), "table is locked", "the engine must have raised the shared-cache table lock, not something else: %v", refused.err)

	// Release the refused transaction so the other writer's wait ends, then
	// release that one too. Neither outcome is the claim.
	require.NoError(t, refused.txn.Rollback())

	select {
	case other := <-results:
		_ = other.txn.Rollback()
	case <-time.After(30 * time.Second):
		t.Fatal("the surviving writer did not finish once the refused transaction rolled back")
	}

	require.True(t, isDeadlock(refused.err), "the spend batch must retry a shared-cache table lock, got a non-retryable classification for: %v", refused.err)
	require.True(t, isLockError(refused.err), "the create path must retry the same shared-cache table lock as the spend path, got a non-retryable classification for: %v", refused.err)
}

// closeWithin runs a cleanup close but gives up after a bounded wait, so a
// failed lock-cycle test cannot hang the package. A writer waiting on a
// shared-cache table lock is parked in modernc.org/sqlite until SQLite's
// unlock-notify callback releases it, and a cancelled or expired context does
// not wake it (v1.54.0 conn.go retry). Closing its connection, or the
// database, waits for that statement, so an unbounded close would block
// forever. On a timeout the close is left running and the goroutine leaks; the
// test has already failed by then.
func closeWithin(t *testing.T, what string, closeFn func() error) {
	t.Helper()

	done := make(chan error, 1)

	go func() { done <- closeFn() }()

	select {
	case err := <-done:
		require.NoError(t, err, "closing the %s", what)
	case <-time.After(5 * time.Second):
		t.Logf("the %s did not close within 5s: a writer is still parked in the engine, leaving it behind", what)
	}
}

// TestIsLockError_SQLiteBusySnapshotIsRetryable produces a real
// SQLITE_BUSY_SNAPSHOT (517) and asserts the create path retries it.
//
// In WAL mode a read transaction pins a snapshot. If another connection commits
// after that, the reader cannot upgrade to a writer, because its snapshot is
// stale, and the engine refuses the write with BUSY_SNAPSHOT at once, whatever
// busy_timeout says. isLockError's typed arm used to compare the whole code
// against SQLITE_BUSY (5) and return false without reaching the "database is
// locked" string fallback, so this error was not retried. Each Create retry
// opens a fresh transaction, so a retry gets a new snapshot.
func TestIsLockError_SQLiteBusySnapshotIsRetryable(t *testing.T) {
	ctx := context.Background()

	dsn := "file:" + filepath.Join(t.TempDir(), "busy_snapshot.db") + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(0)"

	db, err := usql.Open("sqlite", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	_, err = db.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)`)
	require.NoError(t, err)

	reader, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })

	writer, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = writer.Close() })

	txn, err := reader.BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = txn.Rollback() })

	var n int
	require.NoError(t, txn.QueryRowContext(ctx, `SELECT count(*) FROM t`).Scan(&n))

	_, err = writer.ExecContext(ctx, `INSERT INTO t (v) VALUES (1)`)
	require.NoError(t, err)

	_, err = txn.ExecContext(ctx, `INSERT INTO t (v) VALUES (2)`)
	require.Error(t, err, "a write on a stale WAL snapshot must be refused")

	var sqliteErr *sqlite.Error
	require.True(t, errors.As(err, &sqliteErr), "the engine must return a *sqlite.Error, got %T: %v", err, err)
	require.Equal(t, sqlite3.SQLITE_BUSY_SNAPSHOT, sqliteErr.Code(), "the engine must report BUSY_SNAPSHOT, not something else: %v", err)

	require.True(t, isLockError(err), "the create path must retry a BUSY_SNAPSHOT, got a non-retryable classification for: %v", err)
}

// TestIsLockError_WrappedSQLiteLockedIsRetryable covers the shape create-path
// insert errors actually arrive in. classifyInsertError wraps the driver error
// with NewStorageError, which keeps only its message, so the typed arm never
// sees it and the string fallback decides. SQLITE_LOCKED's message is "database
// table is locked"; without a "deadlock" suffix it used to fall through as not
// retryable, while isDeadlock retried the same message.
func TestIsLockError_WrappedSQLiteLockedIsRetryable(t *testing.T) {
	err := errors.NewStorageError("Failed to insert %s", "transaction", errors.NewProcessingError("database table is locked: transactions (262)"))

	require.True(t, isLockError(err), "a wrapped SQLITE_LOCKED must be retried by create: %v", err)
	require.True(t, isDeadlock(err), "isDeadlock already retries the same message: %v", err)
}
