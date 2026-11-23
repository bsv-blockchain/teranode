package sql

import (
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// A teranode error wrap keeps only the text of the driver error, so isDeadlock must
// classify the wrapped error from that text.
func TestIsDeadlockSeesThroughAStorageErrorWrap(t *testing.T) {
	deadlock := &pgconn.PgError{Severity: "ERROR", Code: "40P01", Message: "deadlock detected"}
	require.True(t, isDeadlock(deadlock))
	require.True(t, isDeadlock(errors.NewStorageError("[setDAH] error locking transaction %d", 7, deadlock)))

	require.False(t, isDeadlock(errors.NewStorageError("unrelated")))

	unrelated := &pgconn.PgError{Severity: "ERROR", Code: "23505", Message: "duplicate key value violates unique constraint"}
	require.False(t, isDeadlock(errors.NewStorageError("[setDAH] error locking transaction %d", 7, unrelated)))
	require.False(t, isDeadlock(unrelated))
}

// The SQLite lock results reach isDeadlock wrapped the same way.
func TestIsDeadlockSeesThroughAStorageErrorWrapForSQLite(t *testing.T) {
	for _, tc := range []struct {
		name string
		msg  string
		want bool
	}{
		{"shared cache table lock", "database table is locked: database is deadlocked (6) (SQLITE_LOCKED)", true},
		{"busy", "database is locked (5) (SQLITE_BUSY)", true},
		{"constraint", "constraint failed: UNIQUE constraint failed: transactions.hash (2067)", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isDeadlock(errors.NewStorageError("[Spend] failed", errors.NewStorageError(tc.msg))))
		})
	}
}

// An Unspend that has used its retry budget returns an error whose message still names the deadlock.
// isDeadlock must report false for it, or a caller that retries on isDeadlock retries again.
func TestIsDeadlockIsFalseForAnExhaustedRetryError(t *testing.T) {
	deadlock := &pgconn.PgError{Severity: "ERROR", Code: "40P01", Message: "deadlock detected"}
	cause := errors.NewStorageError("[setDAH] error locking transaction %d", 7, deadlock)
	require.True(t, isDeadlock(cause), "setup: the cause alone is a deadlock")

	exhausted := &deadlockRetriesExhaustedError{errors.NewStorageError("[Unspend] deadlock persisted after %d retries", 3, cause)}

	require.Contains(t, exhausted.Error(), "SQLSTATE 40P01", "setup: the message names the deadlock")
	require.False(t, isDeadlock(exhausted))
	require.True(t, errors.Is(exhausted, errors.ErrStorageError), "the exhausted error stays a storage error")
}
