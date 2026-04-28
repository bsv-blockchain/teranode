package usql

import (
	"context"
	"fmt"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
)

// advisoryUnlockTimeout bounds the unlock call in the defer so a degraded or
// broken connection cannot hang the caller after fn() returns.
const advisoryUnlockTimeout = 5 * time.Second

// WithAdvisoryLock acquires a PostgreSQL session-level advisory lock, executes fn,
// then releases the lock. This serializes concurrent DDL operations (like CREATE TABLE
// IF NOT EXISTS) across multiple pods sharing the same database, preventing race
// conditions on PostgreSQL system catalog indexes.
//
// The lockID must be a unique constant per schema-creation context (e.g. one for
// blockchain store, one for UTXO store, one for banlist). Stable across releases.
//
// Connection pinning: pg_advisory_lock / pg_advisory_unlock are session-scoped.
// When called via *sql.DB the driver can pick any pooled connection, so a naive
// lock-via-pool / unlock-via-pool pattern can lock on session X and unlock on
// session Y — leaving X's lock held until the session terminates, permanently
// blocking other pods. We pin a dedicated *sql.Conn from the pool for the
// entire lock/fn/unlock window so both statements run on the same session, then
// return the connection to the pool in a deferred Close.
//
// Lock lifetime:
//  1. The lock is held only for the duration of fn() execution.
//  2. It is released explicitly before this function returns (or on panic via defer).
//  3. It is also auto-released by postgres if the pinned session terminates
//     (e.g. network drop) — so a crashed pod cannot permanently block others.
//
// Callers are responsible for engine-gating: this is a no-op wrapper for
// non-PostgreSQL databases — check the engine type before calling.
func WithAdvisoryLock(ctx context.Context, db *DB, lockID int64, fn func() error) error {
	// Pin a single connection so lock + unlock run on the same session.
	conn, err := db.DB.Conn(ctx)
	if err != nil {
		return errors.New(errors.ERR_ERROR, "advisory lock %d: failed to acquire connection: %w", lockID, err)
	}
	defer func() {
		// Returns the pinned connection to the pool; the advisory lock must
		// be released *before* this runs, otherwise the pool can hand the
		// session to another caller while it still holds the lock.
		_ = conn.Close()
	}()

	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SELECT pg_advisory_lock(%d)", lockID)); err != nil {
		return errors.New(errors.ERR_ERROR, "failed to acquire advisory lock %d: %w", lockID, err)
	}

	defer func() {
		// Release on a fresh bounded context: the parent ctx may have been
		// cancelled by the caller, but we still need to release the lock on
		// the same pinned session so other pods are not blocked. The timeout
		// protects against a slow/broken connection hanging cleanup.
		unlockCtx, cancel := context.WithTimeout(context.Background(), advisoryUnlockTimeout)
		defer cancel()
		_, _ = conn.ExecContext(unlockCtx, fmt.Sprintf("SELECT pg_advisory_unlock(%d)", lockID))
	}()

	return fn()
}
