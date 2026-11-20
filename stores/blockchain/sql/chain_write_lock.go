package sql

import (
	"context"
	"database/sql"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util"
)

// chainWriteStateKey is the state row the chain-membership writes on PostgreSQL lock,
// and every on_main_chain walk checks before it commits. The writes that lock it are
// DeleteBlock, InvalidateBlock, RevalidateBlock, the fork path of StoreBlock, and the
// extend path's later clear of the new block's own flag when its best-block read after
// the insert names another block. The extend path's insert takes no lock: it decides
// on_main_chain from this process's best-block cache, which a lock taken inside its
// transaction could not correct.
//
// The walks read their rows from a REPEATABLE READ snapshot and most run without
// slowPathMu, which in any case only orders writers in this process. A chain write that
// commits during a walk usually touches rows the walk only reads, so PostgreSQL aborts
// neither, and the walk can then commit its older view over the write. The in-process
// watch (see watchOnMainChainWrites) catches that only for writers in this process. It
// cannot see DeleteBlock from the rewindblockchain tool, or any write from a second
// instance on the same database.
//
// The shared row makes PostgreSQL catch it for every process:
//
//   - A chain write locks the row as the first statement of its transaction (see
//     lockChainWrites), so it either commits before a walk checks the row or reads only
//     after that walk has committed.
//   - A walk locks the row with NOWAIT as its last statement before commit (see
//     checkNoChainWriteSinceSnapshot). If a write committed after the walk's snapshot,
//     the row has a version the snapshot cannot see and PostgreSQL raises 40001. If a
//     write still holds the row, NOWAIT raises 55P03 at once rather than waiting while
//     the walk holds its own row locks, which the write may be queued on. Either way the
//     walk rolls back and runs again from a fresh snapshot.
//
// SQLite takes no lock: it lets one writer in at a time, and its unlocked startup rebuild
// relies on the in-process watch, as before.
const chainWriteStateKey = "on_main_chain_write"

// chainWriteLocking reports whether this store's engine uses the shared row.
func (s *SQL) chainWriteLocking() bool {
	return s.engine == util.Postgres
}

// ensureChainWriteRow creates the shared row if no process has yet. A walk that finds no
// row cannot see a write that is creating it, so New creates it before any walk runs.
//
// It reads before it inserts. An INSERT ... ON CONFLICT waits for any open transaction
// that has locked the row, and a chain write queued behind a full walk can hold it for as
// long as the walk runs, so a starting process would sit in New with no deadline. A plain
// read never waits on a row lock. On a new database the genesis StoreBlock's lock has
// already created the row, so only a database that holds its chain but has no row reaches
// the insert, such as one written before the row existed. There the usual holder is
// another process's own insert in New, which lasts milliseconds.
func (s *SQL) ensureChainWriteRow(ctx context.Context) error {
	if !s.chainWriteLocking() {
		return nil
	}

	var one int

	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM state WHERE key = $1`, chainWriteStateKey).Scan(&one)
	if err == nil {
		return nil
	}

	if !errors.Is(err, sql.ErrNoRows) {
		return errors.NewStorageError("could not read the %s state row", chainWriteStateKey, err)
	}

	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO state (key, data) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING`,
		chainWriteStateKey, []byte{}); err != nil {
		return errors.NewStorageError("could not create the %s state row", chainWriteStateKey, err)
	}

	return nil
}

// lockChainWrites locks the shared row for a chain-membership write. Call it as the first
// statement of the transaction that moves chain membership, before anything it reads.
// The error is the driver's own, unwrapped, so RetryTx can classify it.
func (s *SQL) lockChainWrites(ctx context.Context, tx *sql.Tx) error {
	if !s.chainWriteLocking() {
		return nil
	}

	_, err := tx.ExecContext(ctx,
		`INSERT INTO state (key, data, updated_at) VALUES ($1, $2, CURRENT_TIMESTAMP)
		 ON CONFLICT (key) DO UPDATE SET updated_at = CURRENT_TIMESTAMP`,
		chainWriteStateKey, []byte{})

	return err
}

// checkNoChainWriteSinceSnapshot fails a walk's transaction when a chain write committed
// after its snapshot or is still open. Call it as the walk's last statement before
// commit. The error is the driver's own, 40001 or 55P03, so the walk's retry can
// classify it.
func (s *SQL) checkNoChainWriteSinceSnapshot(ctx context.Context, tx *sql.Tx) error {
	if !s.chainWriteLocking() {
		return nil
	}

	var one int

	err := tx.QueryRowContext(ctx, `SELECT 1 FROM state WHERE key = $1 FOR UPDATE NOWAIT`, chainWriteStateKey).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		// New creates the row, so it is missing only if removed by hand. Writes re-create
		// it, but a walk already in flight cannot see that, so it commits unchecked.
		s.logger.Warnf("[checkNoChainWriteSinceSnapshot] the %s state row is missing; committing the on_main_chain walk without the cross-process check", chainWriteStateKey)

		return nil
	}

	return err
}
