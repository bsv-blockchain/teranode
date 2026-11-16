package utxoset

import (
	"context"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// deadlockRetries is how many times a transaction PostgreSQL stopped as a deadlock is run again.
const deadlockRetries = 10

// deadlockRetryBase is the first back-off before a retry; it doubles each time, with jitter, up
// to deadlockRetryMax. PostgreSQL's detection of the deadlock, a second by default, has already
// passed by the time a retry is due, so the back-off only spreads the retries out.
const deadlockRetryBase = 5 * time.Millisecond

// deadlockRetryMax caps one back-off.
const deadlockRetryMax = 200 * time.Millisecond

var deadlockRetried = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "utxoset_deadlock_retries_total",
	Help: "Transactions PostgreSQL stopped as a deadlock (40P01) and the store ran again, by site",
}, []string{"site"})

// isDeadlock reports whether err is PostgreSQL's deadlock error, SQLSTATE 40P01.
//
// The type check alone is not enough here. The store wraps a statement's error in a storage
// error (errors.NewStorageError), and that wrapper keeps the message but not the *pgconn.PgError
// underneath, so errors.As finds nothing. pgconn always writes the state into the message as
// "(SQLSTATE 40P01)", so the message is checked too.
func isDeadlock(err error) bool {
	if err == nil {
		return false
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "40P01" {
		return true
	}

	return strings.Contains(err.Error(), "(SQLSTATE 40P01)")
}

// withDeadlockRetry runs fn, and runs it again when it fails with a deadlock, up to
// deadlockRetries times, with a doubling back-off and jitter. fn must be one whole database
// transaction, so a retry starts from nothing.
//
// The lock release (setLockedDirect), the mine (recordMined) and the un-mine (unMine) all update
// the identity and containment rows of whichever transactions they name, in an order the
// statements choose. Two of them that share transactions can therefore lock those rows in
// different orders, and PostgreSQL then stops one of them. Ordering them with per-transaction
// advisory locks was tried and fails at scale: a mine names thousands of transactions, and the
// locks of many concurrent callers overflow PostgreSQL's shared lock table ("out of shared
// memory"). A deadlock is rare, PostgreSQL detects it, and a retry from the start is safe, so
// that is the cure. The sql store does the same for its spends (isDeadlock in stores/utxo/sql).
// Each retry waits first for PostgreSQL's deadlock_timeout, the time it takes to detect the
// cycle, so many deadlocks cost time, and the counter shows how many there are. The updates
// write only rows whose value changes (setLockedSQL, clearMarkerSQL), which keeps the overlap,
// and so the deadlocks, small: in a test of 24 callers on 60 transactions that change took the
// mine test from failing after two minutes to passing in about ten seconds.
//
// A retried recordMined counts its fence no-op metrics again, because they are counted before
// the commit. That over-counts by one for each retried call and changes nothing else.
//
// The per-transaction advisory locks were rejected for a measured reason: PostgreSQL's lock
// table holds max_locks_per_transaction times max_connections entries, 12,800 on a default
// server, and a mine of 4,000 transactions by each of 12 miners overflowed it.
func withDeadlockRetry(ctx context.Context, site string, fn func() error) error {
	err := fn()

	for attempt := 0; attempt < deadlockRetries && isDeadlock(err); attempt++ {
		deadlockRetried.WithLabelValues(site).Inc()

		wait := min(deadlockRetryBase<<attempt, deadlockRetryMax)
		wait += time.Duration(rand.Int64N(int64(wait))) //nolint:gosec // jitter, not security

		select {
		case <-ctx.Done():
			return err
		case <-time.After(wait):
		}

		err = fn()
	}

	return err
}
