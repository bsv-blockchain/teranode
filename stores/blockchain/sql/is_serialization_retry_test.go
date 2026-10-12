package sql

import (
	stdsql "database/sql"
	"testing"

	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// wrappedErr wraps err behind an Unwrap method so errors.As traverses to it,
// mirroring how fmt.Errorf("%w", ...) chains a typed driver error in production
// (fmt.Errorf is forbidden repo-wide by forbidigo).
type wrappedErr struct {
	msg string
	err error
}

func (w *wrappedErr) Error() string { return w.msg + ": " + w.err.Error() }
func (w *wrappedErr) Unwrap() error { return w.err }

// TestIsSerializationRetry locks in the classification the rebuildOnMainChainFlag
// retry loop depends on: only Postgres serialization_failure (40001) and
// deadlock_detected (40P01) — raised via either the pgx (*pgconn.PgError) or the
// lib/pq (*pq.Error) driver — are retryable. Everything else (including SQLite,
// which never produces these codes) is not.
func TestIsSerializationRetry(t *testing.T) {
	s := &SQL{}

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"pgx serialization_failure", &pgconn.PgError{Code: usql.PgErrSerializationFail}, true},
		{"pgx deadlock_detected", &pgconn.PgError{Code: usql.PgErrDeadlockDetected}, true},
		{"pq serialization_failure", &pq.Error{Code: pq.ErrorCode(usql.PgErrSerializationFail)}, true},
		{"pq deadlock_detected", &pq.Error{Code: pq.ErrorCode(usql.PgErrDeadlockDetected)}, true},
		{"wrapped pgx serialization_failure", &wrappedErr{msg: "rebuild", err: &pgconn.PgError{Code: usql.PgErrSerializationFail}}, true},
		{"pgx unique_violation (non-retryable)", &pgconn.PgError{Code: "23505"}, false},
		{"pq unique_violation (non-retryable)", &pq.Error{Code: "23505"}, false},
		{"sql.ErrNoRows", stdsql.ErrNoRows, false},
		{"plain error", stdsql.ErrConnDone, false},
		{"nil", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, s.isSerializationRetry(tt.err))
		})
	}
}
