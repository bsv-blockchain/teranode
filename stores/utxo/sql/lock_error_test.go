package sql

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/teranode/util/usql"
	pq "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// lockTestWrap wraps an error behind an Unwrap boundary, as a caller adding
// context with %w would, so the test exercises errors.As not a type assertion.
type lockTestWrap struct{ err error }

func (w lockTestWrap) Error() string { return "wrapped: " + w.err.Error() }
func (w lockTestWrap) Unwrap() error { return w.err }

// TestIsLockError_ExtendedSQLiteAndWrapped covers #1876: isLockError used a bare
// type assertion and an exact `Code() == SQLITE_BUSY` compare, so it missed both
// wrapped errors and SQLite extended result codes (e.g. 517 BUSY_SNAPSHOT).
func TestIsLockError_ExtendedSQLiteAndWrapped(t *testing.T) {
	// lib/pq deadlock, direct and wrapped (errors.As, not a type assertion).
	pqDeadlock := &pq.Error{Code: usql.PgErrDeadlockDetected}
	require.True(t, isLockError(pqDeadlock), "lib/pq deadlock")
	require.True(t, isLockError(lockTestWrap{pqDeadlock}), "wrapped lib/pq deadlock")

	// Real extended SQLite BUSY (517), direct and wrapped.
	busyErr := sqliteBusySnapshot(t)
	require.True(t, isLockError(busyErr), "extended SQLITE_BUSY_SNAPSHOT (517)")
	require.True(t, isLockError(lockTestWrap{busyErr}), "wrapped extended busy")
}

// sqliteBusySnapshot produces a real SQLITE_BUSY_SNAPSHOT (517): a read
// transaction whose snapshot goes stale tries to upgrade to a write. The sqlite
// driver is registered via the package's modernc.org/sqlite import.
func sqliteBusySnapshot(t *testing.T) error {
	t.Helper()

	dsn := "file:" + filepath.Join(t.TempDir(), "lock.db") + "?_pragma=journal_mode(WAL)"
	open := func() *sql.DB {
		db, err := sql.Open("sqlite", dsn)
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		t.Cleanup(func() { _ = db.Close() })
		return db
	}

	a := open()
	_, err := a.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
	require.NoError(t, err)
	_, err = a.Exec("INSERT INTO t (id, v) VALUES (1, 0)")
	require.NoError(t, err)
	b := open()

	ctx := context.Background()
	tx, err := a.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	var v int
	require.NoError(t, tx.QueryRow("SELECT v FROM t WHERE id = 1").Scan(&v))

	_, err = b.Exec("UPDATE t SET v = v + 1 WHERE id = 1")
	require.NoError(t, err)

	_, err = tx.Exec("UPDATE t SET v = v + 1 WHERE id = 1")
	require.Error(t, err)

	return err
}
