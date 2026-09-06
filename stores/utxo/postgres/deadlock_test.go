// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// isPgDeadlock / deadlockRetryBackoff (deadlock.go) are shared by three retry sites:
// create-batch INSERT (create.go), SetMinedMulti's UPDATE batch (mined.go), and the
// pruner's cascade-delete batch (pruner_provider.go). Site-specific loop-bound tests
// live next to each site (mined_deadlock_test.go, pruner_deadlock_test.go).

// TestIsPgDeadlock: only SQLSTATE 40P01 (deadlock) is treated as retryable, raw or
// wrapped; other pg errors and plain errors are not.
func TestIsPgDeadlock(t *testing.T) {
	require.True(t, isPgDeadlock(&pgconn.PgError{Code: "40P01"}), "raw 40P01 is a deadlock")
	// runChunk calls isPgDeadlock on the RAW error returned by attemptChunk (before
	// it is wrapped in a teranode StorageError), because a teranode-wrapped error
	// does not expose its cause to errors.As — so detection must happen on the raw
	// pgconn error, which is exactly the production path.
	require.False(t, isPgDeadlock(errors.NewStorageError("create batch rows error", &pgconn.PgError{Code: "40P01"})),
		"a teranode-wrapped error is NOT traversable by errors.As — production checks the raw error")
	require.False(t, isPgDeadlock(&pgconn.PgError{Code: "23505"}), "unique-violation is NOT a deadlock")
	require.False(t, isPgDeadlock(&pgconn.PgError{Code: "40001"}), "serialization-failure is NOT 40P01")
	require.False(t, isPgDeadlock(errors.NewStorageError("plain")), "plain error is not a deadlock")
	require.False(t, isPgDeadlock(nil), "nil is not a deadlock")
}

// TestDeadlockRetryBackoff: bounded, positive, jittered, and capped at 500ms across
// all attempts (including well past the retry cap, so the shift cannot overflow).
func TestDeadlockRetryBackoff(t *testing.T) {
	for attempt := 0; attempt < 64; attempt++ {
		b := deadlockRetryBackoff(attempt)
		require.Greater(t, b, time.Duration(0), "backoff must be positive (attempt %d)", attempt)
		require.LessOrEqual(t, b, 500*time.Millisecond, "backoff must be capped at 500ms (attempt %d)", attempt)
	}

	// Full-jitter should produce a spread (not a constant) at a given attempt, which
	// is what desynchronises concurrently-deadlocked batchers.
	seen := map[time.Duration]struct{}{}
	for i := 0; i < 50; i++ {
		seen[deadlockRetryBackoff(5)] = struct{}{}
	}
	require.Greater(t, len(seen), 1, "jitter must vary the backoff, not return a constant")
}

// TestRetryOnPgDeadlock covers the generic retry loop shared by the SetMinedMulti
// (mined.go) and pruner cascade-delete (pruner_provider.go) sites: bounded attempts,
// jittered wait, ctx-cancellation respected inside the wait, and non-deadlock errors
// returned immediately without retrying. create.go's own inline loop predates this
// helper and is not covered here (see the file-level comment above
// retryOnPgDeadlock in deadlock.go).
func TestRetryOnPgDeadlock(t *testing.T) {
	deadlock := func() error { return &pgconn.PgError{Code: "40P01"} }

	t.Run("succeeds on the first attempt without retrying", func(t *testing.T) {
		calls := 0
		result, err := retryOnPgDeadlock(context.Background(),
			func() (int, error) { calls++; return 42, nil },
			func(int, time.Duration) { t.Fatal("onRetry must not be called when the first attempt succeeds") },
		)
		require.NoError(t, err)
		require.Equal(t, 42, result)
		require.Equal(t, 1, calls)
	})

	t.Run("retries on 40P01 then succeeds, calling onRetry once per retry", func(t *testing.T) {
		calls := 0
		var retryAttempts []int
		result, err := retryOnPgDeadlock(context.Background(),
			func() (int, error) {
				calls++
				if calls <= 2 {
					return 0, deadlock()
				}
				return 7, nil
			},
			func(attemptNum int, backoff time.Duration) {
				retryAttempts = append(retryAttempts, attemptNum)
				require.Greater(t, backoff, time.Duration(0), "backoff passed to onRetry must be positive")
			},
		)
		require.NoError(t, err)
		require.Equal(t, 7, result)
		require.Equal(t, 3, calls, "first two attempts deadlock, third succeeds")
		require.Equal(t, []int{1, 2}, retryAttempts, "onRetry called once per retry, numbered from 1")
	})

	t.Run("exhausts pgDeadlockMaxRetries then returns the final error and last result", func(t *testing.T) {
		calls := 0
		result, err := retryOnPgDeadlock(context.Background(),
			func() (string, error) { calls++; return "last-attempt-value", deadlock() },
			func(int, time.Duration) {},
		)
		require.Error(t, err)
		require.True(t, isPgDeadlock(err), "the final error must still classify as a deadlock")
		require.Equal(t, "last-attempt-value", result, "T is the LAST attempt's result, not a zero value")
		require.Equal(t, pgDeadlockMaxRetries+1, calls, "one initial attempt plus pgDeadlockMaxRetries retries")
	})

	t.Run("a non-deadlock error returns immediately without retrying", func(t *testing.T) {
		calls := 0
		plainErr := errors.NewStorageError("boom")
		result, err := retryOnPgDeadlock(context.Background(),
			func() (int, error) { calls++; return -1, plainErr },
			func(int, time.Duration) { t.Fatal("onRetry must not be called for a non-deadlock error") },
		)
		require.ErrorIs(t, err, plainErr)
		require.Equal(t, -1, result)
		require.Equal(t, 1, calls, "must not retry a non-deadlock error")
	})

	t.Run("ctx cancellation during the backoff wait returns ctx.Err and the last result", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		result, err := retryOnPgDeadlock(ctx,
			func() (int, error) {
				calls++
				cancel() // cancel while this attempt's retry backoff is about to be awaited
				return 99, deadlock()
			},
			func(int, time.Duration) {},
		)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 99, result, "the last attempt's result is preserved across ctx cancellation")
		require.Equal(t, 1, calls, "must not attempt again once ctx is cancelled")
	})
}
