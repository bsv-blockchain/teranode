package pruner

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lib/pq"
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

// TestIsPruneRetryableSeesALibpqError: the store opens Postgres through pgx, but a *pq.Error that
// reaches the predicate carries the same SQLSTATE and is read the same way. On Postgres the message
// fallback is off, so only the typed branch can make it retryable.
func TestIsPruneRetryableSeesALibpqError(t *testing.T) {
	require.True(t, isPruneRetryable(&pq.Error{Code: "40001"}, "postgres"), "serialization failure")
	require.True(t, isPruneRetryable(&pq.Error{Code: "40P01"}, "postgres"), "deadlock")
	require.True(t, isPruneRetryable(&pruneStepError{step: "x", err: &pq.Error{Code: "40001"}}, "postgres"), "through a step error")
	require.False(t, isPruneRetryable(&pq.Error{Code: "23505"}, "postgres"), "a non-conflict code is not retried")
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

	// The driver's message for this code also says "database is locked", so on a SQLite engine
	// the message fallback would retry it alone. The Postgres engine has no fallback, so this
	// isolates the typed code: dropping SQLITE_BUSY_SNAPSHOT from the switch fails here.
	require.True(t, isPruneRetryable(err, "postgres"), "the typed code must be retryable without the message fallback")
}

// TestTakeSQLiteWriteLockMakesTheTransactionAWriter: the statement that opens a pruning
// transaction on SQLite writes nothing, and still takes the write lock when it starts. Another
// connection then cannot write until the transaction ends, so the first real write of the prune
// cannot fail with SQLITE_BUSY_SNAPSHOT because someone committed after its reads. A deferred
// transaction that only read (the control) lets the other connection write.
func TestTakeSQLiteWriteLockMakesTheTransactionAWriter(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "lock.db")+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(50)")
	require.NoError(t, err)

	defer db.Close()

	_, err = db.Exec("CREATE TABLE deleted_children (parent_id INTEGER, child_hash BLOB)")
	require.NoError(t, err)

	ctx := context.Background()

	reader, err := db.Conn(ctx)
	require.NoError(t, err)

	defer reader.Close()

	_, err = reader.ExecContext(ctx, "BEGIN")
	require.NoError(t, err)

	var n int

	require.NoError(t, reader.QueryRowContext(ctx, "SELECT count(*) FROM deleted_children").Scan(&n))

	_, err = db.Exec("INSERT INTO deleted_children VALUES (1, x'01')")
	require.NoError(t, err, "control: a transaction that only read does not stop another connection from writing")

	_, err = reader.ExecContext(ctx, "ROLLBACK")
	require.NoError(t, err)

	writer, err := db.Conn(ctx)
	require.NoError(t, err)

	defer writer.Close()

	_, err = writer.ExecContext(ctx, "BEGIN")
	require.NoError(t, err)

	_, err = writer.ExecContext(ctx, takeSQLiteWriteLock)
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO deleted_children VALUES (2, x'02')")
	require.Error(t, err, "another connection cannot write while the pruning transaction holds the write lock")
	require.Contains(t, err.Error(), "database is locked")

	_, err = writer.ExecContext(ctx, "ROLLBACK")
	require.NoError(t, err)
}
