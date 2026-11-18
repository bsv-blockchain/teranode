package pruner

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// TestIsPruneRetryableSeesTheDriverErrorThroughAStepError pins why the pruning
// transaction reports statement failures as pruneStepError and not as
// errors.NewStorageError: the latter replaces a foreign wrapped error with a
// bare *errors.Error carrying only its message, so the retry predicate could
// never see the driver's serialization code and the retry never fired.
func TestIsPruneRetryableSeesTheDriverErrorThroughAStepError(t *testing.T) {
	serialization := &pgconn.PgError{Code: "40001"}

	require.True(t, isPruneRetryable(serialization, "postgres"), "raw driver error")
	require.True(t, isPruneRetryable(&pruneStepError{step: "failed to delete transactions", err: serialization}, "postgres"),
		"the step error must keep the driver error reachable")
	require.False(t, isPruneRetryable(errors.NewStorageError("failed to delete transactions", serialization), "postgres"),
		"control: NewStorageError loses the driver type, which is why it is not used inside the attempt")
	require.False(t, isPruneRetryable(&pruneStepError{step: "x", err: &pgconn.PgError{Code: "23505"}}, "postgres"),
		"a non-conflict code is not retried")
}

// TestIsPruneRetryableMessageFallbackIsSQLiteOnly: the "database is locked"
// message match exists for SQLite errors that reach the predicate without their
// driver type. On Postgres a conflict always carries its SQLSTATE, so an error
// that only mentions the text must not buy three retries.
func TestIsPruneRetryableMessageFallbackIsSQLiteOnly(t *testing.T) {
	locked := errors.NewStorageError("wrapped: database is locked")

	require.True(t, isPruneRetryable(locked, "sqlite"))
	require.True(t, isPruneRetryable(locked, "sqlitememory"))
	require.False(t, isPruneRetryable(locked, "postgres"))
}

// TestIsPruneRetryableSeesASQLiteWriterUpgradeConflict raises the real error a deferred
// SQLite transaction gets when it reads, another connection commits, and it then tries
// to write: SQLITE_BUSY_SNAPSHOT. busy_timeout cannot wait that out, so the pruning
// transaction has to be re-run, and the predicate has to see the code through the
// driver error.
func TestIsPruneRetryableSeesASQLiteWriterUpgradeConflict(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "busy.db")+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(50)")
	require.NoError(t, err)

	defer db.Close()

	_, err = db.Exec("CREATE TABLE t (id INTEGER)")
	require.NoError(t, err)

	ctx := context.Background()

	reader, err := db.Conn(ctx)
	require.NoError(t, err)

	defer reader.Close()

	_, err = reader.ExecContext(ctx, "BEGIN")
	require.NoError(t, err)

	var n int

	require.NoError(t, reader.QueryRowContext(ctx, "SELECT count(*) FROM t").Scan(&n))

	_, err = db.Exec("INSERT INTO t VALUES (1)")
	require.NoError(t, err)

	_, err = reader.ExecContext(ctx, "INSERT INTO t VALUES (2)")
	require.Error(t, err)

	var sqliteErr *sqlite.Error

	require.True(t, errors.As(err, &sqliteErr), "unexpected error type: %v", err)
	require.Equal(t, sqlite3.SQLITE_BUSY_SNAPSHOT, sqliteErr.Code(), "setup: the writer upgrade must fail as BUSY_SNAPSHOT")

	require.True(t, isPruneRetryable(&pruneStepError{step: "failed to mark pruned children", err: err}, "sqlite"))
}
