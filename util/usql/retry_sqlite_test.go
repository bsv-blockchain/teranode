package usql

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// modernc.org/sqlite enables extended result codes on every connection, so a
// busy or I/O error carries a detail in the high bits of its code.
func TestIsRetriableSQLiteCode(t *testing.T) {
	tests := []struct {
		name     string
		code     int
		expected bool
	}{
		{"SQLITE_BUSY", sqlite3.SQLITE_BUSY, true},
		{"SQLITE_BUSY_SNAPSHOT", sqlite3.SQLITE_BUSY_SNAPSHOT, true},
		{"SQLITE_BUSY_RECOVERY", sqlite3.SQLITE_BUSY_RECOVERY, true},
		{"SQLITE_LOCKED", sqlite3.SQLITE_LOCKED, true},
		{"SQLITE_LOCKED_SHAREDCACHE", sqlite3.SQLITE_LOCKED_SHAREDCACHE, true},
		{"SQLITE_IOERR", sqlite3.SQLITE_IOERR, true},
		{"SQLITE_IOERR_WRITE", sqlite3.SQLITE_IOERR_WRITE, true},
		{"SQLITE_CANTOPEN", sqlite3.SQLITE_CANTOPEN, true},
		{"SQLITE_CANTOPEN_NOTEMPDIR", sqlite3.SQLITE_CANTOPEN_NOTEMPDIR, true},
		{"SQLITE_IOERR_DATA (corruption, non-retriable)", sqlite3.SQLITE_IOERR_DATA, false},
		{"SQLITE_IOERR_CORRUPTFS (corruption, non-retriable)", sqlite3.SQLITE_IOERR_CORRUPTFS, false},
		{"SQLITE_CANTOPEN_ISDIR (bad path, non-retriable)", sqlite3.SQLITE_CANTOPEN_ISDIR, false},
		{"SQLITE_CANTOPEN_FULLPATH (bad path, non-retriable)", sqlite3.SQLITE_CANTOPEN_FULLPATH, false},
		{"SQLITE_CANTOPEN_CONVPATH (bad path, non-retriable)", sqlite3.SQLITE_CANTOPEN_CONVPATH, false},
		{"SQLITE_CANTOPEN_SYMLINK (bad path, non-retriable)", sqlite3.SQLITE_CANTOPEN_SYMLINK, false},
		{"SQLITE_CONSTRAINT_UNIQUE (non-retriable)", sqlite3.SQLITE_CONSTRAINT_UNIQUE, false},
		{"SQLITE_CONSTRAINT_PRIMARYKEY (non-retriable)", sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY, false},
		{"SQLITE_ERROR (non-retriable)", sqlite3.SQLITE_ERROR, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, isRetriableSQLiteCode(tt.code), "code %d", tt.code)
		})
	}
}

// openWALPair opens two connections to the same file-backed WAL database.
func openWALPair(t *testing.T) (*sql.DB, *sql.DB) {
	t.Helper()

	dsn := "file:" + filepath.Join(t.TempDir(), "retry.db") + "?_pragma=journal_mode(WAL)"

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

	return a, open()
}

// busySnapshotError produces a real SQLITE_BUSY_SNAPSHOT (517): a read
// transaction whose snapshot goes stale tries to upgrade to a write.
func busySnapshotError(t *testing.T) error {
	t.Helper()

	a, b := openWALPair(t)
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

func TestIsRetriable_SQLiteExtendedCodeFromDriver(t *testing.T) {
	err := busySnapshotError(t)

	var sqliteErr *sqlite.Error
	require.True(t, errors.As(err, &sqliteErr), "expected *sqlite.Error, got %T: %v", err, err)
	require.Equal(t, sqlite3.SQLITE_BUSY_SNAPSHOT, sqliteErr.Code(), "driver should report the extended code")

	require.True(t, isRetriable(err), "SQLITE_BUSY_SNAPSHOT must be retriable: %v", err)
	require.True(t, isRetriable(errors.NewStorageError("store block", err)), "wrapped SQLite busy error must be retriable")
}

func TestIsRetriable_SQLiteConstraintFromDriver(t *testing.T) {
	a, _ := openWALPair(t)

	_, err := a.Exec("INSERT INTO t (id, v) VALUES (1, 0)")
	require.Error(t, err)

	var sqliteErr *sqlite.Error
	require.True(t, errors.As(err, &sqliteErr), "expected *sqlite.Error, got %T: %v", err, err)
	require.Equal(t, sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY, sqliteErr.Code())

	require.False(t, isRetriable(err), "a constraint violation must not be retried")
	require.False(t, isRetriable(errors.NewStorageError("insert", err)), "a wrapped constraint violation must not be retried")
}
