// Copyright (c) 2024 The bsv-blockchain developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package postgres

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5/pgconn"
)

// pgDeadlockMaxRetries bounds how many times a single statement/batch is retried
// after a Postgres deadlock (SQLSTATE 40P01) before the error propagates. Shared by
// every deadlock-retry site in this store (create-batch INSERT in create.go,
// SetMinedMulti's UPDATE batch in mined.go, the pruner's cascade-delete batch in
// pruner_provider.go) so all three back off with the same bound and cadence.
//
// This is defence-in-depth, not a primary fix: each site's root-cause concurrency
// pattern (e.g. the same block committed twice in one window racing create-vs-create
// on the same keys) is addressed upstream where possible. The retry remains for any
// residual concurrent conflict Postgres resolves by killing one side.
const pgDeadlockMaxRetries = 5

// isPgDeadlock reports whether err is (or wraps) a Postgres deadlock, SQLSTATE
// 40P01. Callers must pass the RAW driver error — a teranode-wrapped *errors.Error
// is not traversable by errors.As down to the pgconn cause, so classification must
// happen before wrapping (every retry site here does this: it classifies the raw
// error and only wraps once retries are exhausted or the error is not a deadlock).
func isPgDeadlock(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "40P01"
}

// deadlockRetryBackoff is an exponential backoff with full jitter for deadlock
// retries: attempt 0 -> up to 10ms, doubling per attempt, capped at 500ms. The
// jitter desynchronises multiple deadlocked callers so their retries do not
// re-collide in lockstep (a fixed backoff would just re-deadlock together).
func deadlockRetryBackoff(attempt int) time.Duration {
	const base = 10 * time.Millisecond
	const maxBackoff = 500 * time.Millisecond

	window := base << attempt //nolint:gosec // attempt is bounded by pgDeadlockMaxRetries
	if window > maxBackoff || window <= 0 {
		window = maxBackoff
	}

	return time.Duration(rand.Int64N(int64(window)) + 1)
}

// retryOnPgDeadlock is a generic, SQL-agnostic retry loop shared by the SetMinedMulti
// (mined.go) and pruner cascade-delete (pruner_provider.go) sites. create.go's
// create-batch INSERT predates this helper and keeps its own inline loop (same bound
// and backoff, just not refactored to share this shape) — left alone to keep this
// change minimal.
//
// attempt performs (and, on a deadlock, safely RE-performs from scratch) exactly one
// try of whatever unit of work is atomic at the call site — see the site-specific
// comments in mined.go/pruner_provider.go for why a full re-run of that unit is safe
// there. attempt must return the RAW/unwrapped error so isPgDeadlock can classify it;
// retryOnPgDeadlock does not wrap errors itself, so the caller wraps the final
// (non-nil) error however that site's error convention requires.
//
// onRetry is called once per retry, after classifying the error as a deadlock and
// before sleeping, so each site can emit its own single-line Warnf naming itself and
// the unit that failed. It is never called for a non-deadlock error or once retries
// are exhausted.
//
// On any final failure — retries exhausted, a non-deadlock error, or ctx cancelled
// mid-backoff — the T returned is the one from the LAST completed attempt (not a
// zero value), so a caller that packs diagnostic fields (e.g. which chunk failed)
// into T can still report them even when the loop exits via ctx.Done().
func retryOnPgDeadlock[T any](ctx context.Context, attempt func() (T, error), onRetry func(attemptNum int, backoff time.Duration)) (T, error) {
	for n := 0; ; n++ {
		result, err := attempt()
		if err == nil {
			return result, nil
		}

		if n < pgDeadlockMaxRetries && isPgDeadlock(err) {
			backoff := deadlockRetryBackoff(n)
			if onRetry != nil {
				onRetry(n+1, backoff)
			}

			select {
			case <-time.After(backoff):
				continue
			case <-ctx.Done():
				return result, ctx.Err()
			}
		}

		return result, err
	}
}
