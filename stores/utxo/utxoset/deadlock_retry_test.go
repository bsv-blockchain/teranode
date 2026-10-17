package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestWithDeadlockRetry pins the retry. The store wraps a statement's error in a storage error,
// which keeps the message and drops the *pgconn.PgError, so the first version of the check
// found no deadlock and never retried. A wrapped deadlock is retried until it succeeds; any
// other error is returned at once; and a deadlock that persists stops after deadlockRetries.
func TestWithDeadlockRetry(t *testing.T) {
	ctx := context.Background()
	wrapped := errors.NewStorageError("[utxoset][SetLocked] ident", &pgconn.PgError{Code: "40P01", Message: "deadlock detected"})

	require.True(t, isDeadlock(wrapped), "a wrapped deadlock is still a deadlock")
	require.False(t, isDeadlock(errors.NewStorageError("[utxoset] other", &pgconn.PgError{Code: "23505"})))
	require.False(t, isDeadlock(nil))

	t.Run("a wrapped deadlock is retried until it succeeds", func(t *testing.T) {
		before := testutil.ToFloat64(deadlockRetried.WithLabelValues("test"))
		calls := 0

		err := withDeadlockRetry(ctx, "test", func() error {
			calls++
			if calls < 3 {
				return wrapped
			}

			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 3, calls)
		require.Equal(t, before+2, testutil.ToFloat64(deadlockRetried.WithLabelValues("test")))
	})

	t.Run("another error is not retried", func(t *testing.T) {
		calls := 0
		other := errors.NewStorageError("[utxoset] other")

		err := withDeadlockRetry(ctx, "test", func() error {
			calls++

			return other
		})
		require.ErrorIs(t, err, other)
		require.Equal(t, 1, calls)
	})

	t.Run("a deadlock that persists stops after the bound", func(t *testing.T) {
		calls := 0

		err := withDeadlockRetry(ctx, "test", func() error {
			calls++

			return wrapped
		})
		require.True(t, isDeadlock(err))
		require.Equal(t, deadlockRetries+1, calls)
	})
}
