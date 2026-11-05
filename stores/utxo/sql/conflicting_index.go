package sql

import (
	"context"
	"database/sql"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// conflictingIndexLockID serializes the px_transactions_conflicting build across
// processes sharing the database. It is only ever taken with
// pg_try_advisory_lock: a session waiting in pg_advisory_lock holds a snapshot,
// CREATE INDEX CONCURRENTLY waits for every older snapshot, and the lock holder
// would wait for the build, so a blocking wait would hang both.
const conflictingIndexLockID int64 = 7_265_726_118

// holdConflictingIndexBuildLock takes the build lock on conn for schema startup
// and returns its release. Schema startup runs ALTER TABLE on transactions,
// which takes ACCESS EXCLUSIVE even when there is nothing to change: queued
// behind a running concurrent build, it would block every later read and write
// of the table for the rest of the build. So startup waits for the build
// instead, polling with pg_try_advisory_lock for the reason given on
// conflictingIndexLockID.
func holdConflictingIndexBuildLock(ctx context.Context, logger ulogger.Logger, conn *sql.Conn) (release func(), err error) {
	for waiting := false; ; waiting = true {
		var locked bool
		if err = conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, conflictingIndexLockID).Scan(&locked); err != nil {
			return nil, errors.NewStorageError("could not take the index build lock", err)
		}

		if locked {
			break
		}

		if !waiting {
			logger.Infof("[createPostgresSchema] another process is building px_transactions_conflicting, waiting for it before changing the schema")
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}

	return func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, _ = conn.ExecContext(unlockCtx, `SELECT pg_advisory_unlock($1)`, conflictingIndexLockID)
	}, nil
}

// conflictingIndexRetryInterval is how long the background build waits before
// trying again after it failed or found another process building.
const conflictingIndexRetryInterval = time.Minute

// startPostgresConflictingIndexBuild builds px_transactions_conflicting in the
// background. ConflictingTxHashes answers correctly without it, only slower, so
// startup does not wait for the build and a failed build is retried.
func (s *Store) startPostgresConflictingIndexBuild(ctx context.Context) {
	ctx, s.conflictingIndexCancel = context.WithCancel(ctx)
	s.conflictingIndexDone = make(chan struct{})

	go func() {
		defer close(s.conflictingIndexDone)

		for {
			built, err := s.buildPostgresConflictingIndex(ctx)
			if ctx.Err() != nil {
				return
			}

			if err != nil {
				s.logger.Warnf("[ConflictingTxHashes] could not build px_transactions_conflicting, retrying in %s: %v", conflictingIndexRetryInterval, err)
			}

			if built {
				s.conflictingIndexReady.Store(true)
				return
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(conflictingIndexRetryInterval):
			}
		}
	}()
}

// stopPostgresConflictingIndexBuild cancels the background build and waits for it.
func (s *Store) stopPostgresConflictingIndexBuild() {
	if s.conflictingIndexCancel == nil {
		return
	}

	s.conflictingIndexCancel()
	<-s.conflictingIndexDone
}

// buildPostgresConflictingIndex makes px_transactions_conflicting exist and be
// valid, building it with CREATE INDEX CONCURRENTLY so writes to transactions
// are not blocked. built is false when another process holds the build lock.
//
// Every statement runs on one pinned connection, so the advisory lock is taken
// and released on the same session. CREATE INDEX CONCURRENTLY cannot run inside
// a transaction, and none is open here.
func (s *Store) buildPostgresConflictingIndex(ctx context.Context) (built bool, err error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return false, errors.NewStorageError("could not get a connection", err)
	}

	defer func() {
		_ = conn.Close()
	}()

	// A valid index needs no lock: checking first keeps this process off the
	// fallback while another process holds the lock for its schema startup.
	valid, err := conflictingIndexValidity(ctx, conn)
	if err != nil {
		return false, err
	}

	if valid.Valid && valid.Bool {
		return true, nil
	}

	var locked bool
	if err = conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, conflictingIndexLockID).Scan(&locked); err != nil {
		return false, errors.NewStorageError("could not take the build lock", err)
	}

	if !locked {
		return false, nil
	}

	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, _ = conn.ExecContext(unlockCtx, `SELECT pg_advisory_unlock($1)`, conflictingIndexLockID)
	}()

	// Checked again under the lock: another process may have finished a build
	// meanwhile. CREATE INDEX takes its table lock before it checks IF NOT
	// EXISTS, so a valid index is left alone rather than queued behind, say, a
	// vacuum.
	//
	// A cancelled or failed concurrent build leaves an INVALID index that
	// CREATE INDEX IF NOT EXISTS would keep forever. Only the holder of the
	// build lock builds, so an invalid index seen here is such a leftover.
	if valid, err = conflictingIndexValidity(ctx, conn); err != nil {
		return false, err
	}

	if valid.Valid && valid.Bool {
		return true, nil
	}

	if valid.Valid {
		s.logger.Warnf("[ConflictingTxHashes] dropping the invalid px_transactions_conflicting index left by a failed build")

		if _, err = conn.ExecContext(ctx, `DROP INDEX CONCURRENTLY IF EXISTS px_transactions_conflicting`); err != nil {
			return false, errors.NewStorageError("could not drop the invalid px_transactions_conflicting index", err)
		}
	}

	if _, err = conn.ExecContext(ctx, `CREATE INDEX CONCURRENTLY IF NOT EXISTS px_transactions_conflicting ON transactions (hash) WHERE conflicting = true`); err != nil {
		return false, errors.NewStorageError("could not create px_transactions_conflicting", err)
	}

	return true, nil
}

// conflictingIndexValidity reads whether px_transactions_conflicting is valid;
// it is NULL when the index does not exist.
func conflictingIndexValidity(ctx context.Context, conn *sql.Conn) (sql.NullBool, error) {
	var valid sql.NullBool
	if err := conn.QueryRowContext(ctx, `SELECT (SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('px_transactions_conflicting'))`).Scan(&valid); err != nil {
		return valid, errors.NewStorageError("could not read the state of px_transactions_conflicting", err)
	}

	return valid, nil
}
