package pruner

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lib/pq"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// Ensure Store implements the Pruner Service interface
var _ pruner.Service = (*Service)(nil)

// Service implements the utxo.CleanupService interface for SQL-based UTXO stores
type Service struct {
	safetyWindow     uint32 // Block height retention for child stability verification
	defensiveEnabled bool   // Enable defensive checks before deleting UTXO transactions
	engine           string // URL scheme of the store ("postgres" or "sqlite*"); selects engine-specific SQL
	logger           ulogger.Logger
	settings         *settings.Settings
	db               *usql.DB
	ctx              context.Context
}

// Options contains configuration options for the cleanup service
type Options struct {
	// Logger is the logger to use
	Logger ulogger.Logger

	// DB is the SQL database connection
	DB *usql.DB

	// Ctx is the context to use to signal shutdown
	Ctx context.Context

	// SafetyWindow is the number of blocks a child must be stable before parent deletion
	// If not specified, defaults to global_blockHeightRetention (288 blocks)
	SafetyWindow uint32

	// Engine is the store URL scheme ("postgres", "sqlite", "sqlitememory").
	// Only ON COMMIT DROP support differs, so an empty value is treated as
	// "not Postgres" and falls back to the portable form.
	Engine string
}

// NewService creates a new cleanup service for the SQL store
func NewService(tSettings *settings.Settings, opts Options) (*Service, error) {
	if opts.Logger == nil {
		return nil, errors.NewProcessingError("logger is required")
	}

	if tSettings == nil {
		return nil, errors.NewProcessingError("settings is required")
	}

	if opts.DB == nil {
		return nil, errors.NewProcessingError("db is required")
	}

	safetyWindow := opts.SafetyWindow
	if safetyWindow == 0 {
		// Default to global retention setting (288 blocks)
		safetyWindow = tSettings.GlobalBlockHeightRetention
	}

	service := &Service{
		safetyWindow:     safetyWindow,
		defensiveEnabled: tSettings.Pruner.UTXODefensiveEnabled,
		engine:           opts.Engine,
		logger:           opts.Logger,
		settings:         tSettings,
		db:               opts.DB,
		ctx:              opts.Ctx,
	}

	return service, nil
}

// Start starts the cleanup service
func (s *Service) Start(ctx context.Context) {
	s.logger.Infof("[SQLCleanupService] service ready")
}

// AddObserver adds an observer to be notified when pruning completes.
// This is a no-op for the SQL pruner service as it doesn't support observers yet.
func (s *Service) AddObserver(observer pruner.Observer) {
	// No-op: SQL pruner doesn't support observers yet
}

// Prune removes transactions marked for deletion at or before the specified height.
// Returns the number of records processed and any error encountered.
// This method is synchronous and blocks until pruning completes or context is cancelled.
func (s *Service) Prune(ctx context.Context, blockHeight uint32, blockHashStr string) (int64, error) {
	if blockHeight == 0 {
		return 0, errors.NewProcessingError("Cannot prune at block height 0")
	}

	startTime := time.Now()

	// Log start of cleanup
	s.logger.Infof("[pruner][%s:%d] phase 2: starting cleanup scan (delete_at_height <= %d)",
		blockHashStr, blockHeight, blockHeight)

	// Execute the cleanup
	deletedCount, err := s.deleteTombstoned(ctx, blockHeight)
	if err != nil {
		s.logger.Errorf("[pruner][%s:%d] phase 2: cleanup failed: %v", blockHashStr, blockHeight, err)
		return 0, err
	}

	// Calculate throughput
	elapsed := time.Since(startTime)
	tps := float64(deletedCount) / elapsed.Seconds()

	// Format TPS for readability
	var tpsStr string
	if tps >= 1_000_000 {
		tpsStr = fmt.Sprintf("%.1fM records/sec", tps/1_000_000)
	} else if tps >= 1_000 {
		tpsStr = fmt.Sprintf("%.1fK records/sec", tps/1_000)
	} else {
		tpsStr = fmt.Sprintf("%.2f records/sec", tps)
	}

	s.logger.Infof("[pruner][%s:%d] phase 2: completed cleanup in %v: deleted %s records (%s)",
		blockHashStr, blockHeight, elapsed, util.FormatComma(deletedCount), tpsStr)

	return deletedCount, nil
}

// Retry budget for the pruning transaction. The transaction below is opened
// with s.db.BeginTx, which resolves to the embedded stdlib method: every
// txn.ExecContext on it is a raw *sql.Tx call that does NOT go through usql's
// per-statement retry or its circuit breaker. On Postgres a SERIALIZABLE
// transaction can abort with 40001 against the concurrent spend traffic, and on
// SQLite a deferred transaction that starts as a reader can fail to upgrade with
// SQLITE_BUSY_SNAPSHOT, which busy_timeout cannot wait out. Both are safe to
// retry by re-running the whole transaction, so the retry lives here.
// Mirrors stores/blockchain/sql/sql.go rebuildOnMainChainFlag.
const (
	maxPruneAttempts   = 3
	pruneRetryBaseWait = 100 * time.Millisecond
)

// isPruneRetryable reports whether err is a conflict that re-running the whole
// pruning transaction can clear: a Postgres serialization failure or deadlock,
// or a SQLite lock/snapshot conflict. usql's own isRetriable covers the same
// ground for statements that go through usql, but this transaction does not,
// and its SQLite branch matches only the primary SQLITE_BUSY code, never the
// extended SQLITE_BUSY_SNAPSHOT a WAL writer-upgrade raises.
func isPruneRetryable(err error) bool {
	if err == nil {
		return false
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == usql.PgErrSerializationFail || pgErr.Code == usql.PgErrDeadlockDetected
	}

	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		code := string(pqErr.Code)

		return code == usql.PgErrSerializationFail || code == usql.PgErrDeadlockDetected
	}

	var sqliteErr *sqlite.Error
	if errors.As(err, &sqliteErr) {
		switch sqliteErr.Code() {
		case sqlite3.SQLITE_BUSY, sqlite3.SQLITE_BUSY_SNAPSHOT, sqlite3.SQLITE_LOCKED, sqlite3.SQLITE_LOCKED_SHAREDCACHE:
			return true
		}
	}

	msg := strings.ToLower(err.Error())

	return strings.Contains(msg, "database is locked") || strings.Contains(msg, "database table is locked")
}

// deleteTombstoned removes transactions that have passed their expiration time,
// re-running the whole transaction on a serialization or lock conflict.
//
// Only deletes parent transactions if their last spending child is mined and
// stable (defensive mode only).
func (s *Service) deleteTombstoned(ctx context.Context, blockHeight uint32) (int64, error) {
	var (
		count int64
		err   error
	)

	for attempt := 0; attempt < maxPruneAttempts; attempt++ {
		count, err = s.deleteTombstonedTx(ctx, blockHeight)
		if err == nil {
			return count, nil
		}

		if !isPruneRetryable(err) {
			return 0, errors.NewStorageError("pruning transaction failed", err)
		}

		if attempt == maxPruneAttempts-1 {
			break
		}

		backoff := pruneRetryBaseWait << uint(attempt)

		s.logger.Warnf("[pruner] serialization/lock conflict on pruning transaction (attempt %d/%d), retrying in %s: %v",
			attempt+1, maxPruneAttempts, backoff, err)

		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(backoff):
		}
	}

	s.logger.Warnf("[pruner] serialization/lock conflict persisted after %d attempts: %v", maxPruneAttempts, err)

	return 0, errors.NewStorageError("pruning transaction failed after %d attempts", maxPruneAttempts, err)
}

// pruneStepError is how one attempt of the pruning transaction reports a
// failed statement to deleteTombstoned. It keeps the driver's own error
// reachable through Unwrap, which errors.NewStorageError does not: that
// constructor replaces a foreign wrapped error with a bare *errors.Error
// carrying only its message, so isPruneRetryable could never see a
// *pgconn.PgError or *sqlite.Error through it and the retry never fired.
// deleteTombstoned wraps the final error for its caller once the retry
// decision has been made.
type pruneStepError struct {
	step string
	err  error
}

func (e *pruneStepError) Error() string { return e.step + ": " + e.err.Error() }

func (e *pruneStepError) Unwrap() error { return e.err }

// deleteTombstonedTx runs one attempt of the pruning transaction: select the
// candidates, write a replay marker on every parent of every candidate, then
// delete the candidates. Marker-before-delete is the whole point, so the two
// must commit together.
//
// LIMITATION - the protection is not retroactive. A marker only exists for a
// child this code pruned. On a node that was already pruning before this landed,
// every child pruned by the old code left no marker, and the child row is gone,
// so nothing can reconstruct one. Two consequences, both permanent for that
// backlog:
//
//   - Spend path: a replay of such a child still takes the idempotent re-spend
//     branch in Store.trySendSpendBatchBulk / trySendSpendBatchPerRow and the
//     caller can recreate the transaction. That is the bug this change fixes,
//     and it stays open for children pruned before the upgrade on every
//     deployment that was already pruning.
//   - Prune path (defensive mode only): the deleted_children escape clause below
//     fires only for markered children, so a parent whose child was pruned by
//     the old code keeps failing the unstable-child test and can never be
//     pruned. Not a regression - the old code had no escape clause at all, so
//     such a parent was already unprunable - but this change does not clear it
//     either. Defensive mode is off by default (settings.conf
//     pruner_utxoDefensiveEnabled = false), so the deployed configuration is
//     unaffected on the prune side.
//
// Neither can be backfilled from inside the pruner. "Child row absent" is NOT a
// safe substitute for a marker: SequentialSpendAndCreate spends before it
// creates, so a legitimately in-flight child is also absent while its parent's
// output already names it, and inferring "pruned" there would let the pruner
// delete a parent out from under a child that is about to exist.
//
// An operator who wants the backlog cleared can run the equivalent one-shot
// backfill with the node stopped, when no create can be in flight:
//
//	INSERT INTO deleted_children (parent_id, child_hash)
//	SELECT DISTINCT o.transaction_id, substr(o.spending_data, 1, 32)
//	FROM outputs o
//	WHERE o.spending_data IS NOT NULL
//	  AND NOT EXISTS (SELECT 1 FROM transactions c WHERE c.hash = substr(o.spending_data, 1, 32))
//	ON CONFLICT DO NOTHING;
//
// That is a full scan of outputs and is deliberately not run automatically: on a
// mainnet-sized store it is hours of work inside one transaction, which is not
// something a node should do on every boot.
func (s *Service) deleteTombstonedTx(ctx context.Context, blockHeight uint32) (int64, error) {
	// Every statement below is a complete literal. Nothing is concatenated at
	// runtime and no part of the SQL is ever built from data.
	//
	// Non-defensive mode (the deployed configuration) needs no candidates table:
	// its predicate cannot be changed by anything inside this transaction, so
	// the marker INSERT and the DELETE can share the same sub-select. That also
	// makes the first statement a write, so SQLite takes its write lock straight
	// away instead of upgrading later.
	//
	// Defensive mode has to materialise, because the marker INSERT changes the
	// defensive predicate: a newly marked parent becomes eligible, and
	// re-evaluating in the DELETE would remove parents that never got markers of
	// their own.
	if !s.defensiveEnabled {
		return s.pruneWithoutDefensiveCheck(ctx, blockHeight)
	}

	return s.pruneWithDefensiveCheck(ctx, blockHeight)
}

// beginPruneTx opens the pruning transaction.
// pruneTxOptions is the isolation the pruning transaction asks for.
//
// Isolation note, corrected. LevelSerializable is honoured by Postgres only.
// modernc.org/sqlite's conn.BeginTx reads opts.ReadOnly and the DSN _txlock mode
// and never inspects opts.Isolation, and because the driver implements
// driver.ConnBeginTx, database/sql does not reject the level either. util/sql.go
// builds the SQLite DSN with no _txlock, so what we actually get there is a
// plain deferred BEGIN. The cross-statement atomicity the pruner needs still
// holds on SQLite - one writer at a time, and the transaction rolls back as a
// unit - but it holds because SQLite serialises writers, not because the level
// was asked for. A deferred BEGIN that starts as a reader can also fail to
// upgrade with SQLITE_BUSY_SNAPSHOT, which is why deleteTombstoned retries the
// whole transaction.
var pruneTxOptions = &sql.TxOptions{Isolation: sql.LevelSerializable}

// finishPrune reads the delete's row count and commits.
func finishPrune(txn *sql.Tx, result sql.Result) (int64, error) {
	count, err := result.RowsAffected()
	if err != nil {
		return 0, &pruneStepError{step: "failed to get rows affected", err: err}
	}

	if err := txn.Commit(); err != nil {
		return 0, &pruneStepError{step: "failed to commit pruning transaction", err: err}
	}

	return count, nil
}

// pruneWithoutDefensiveCheck marks and deletes every transaction past its
// expiration, with no child-stability verification.
func (s *Service) pruneWithoutDefensiveCheck(ctx context.Context, blockHeight uint32) (int64, error) {
	const markerQuery = `INSERT INTO deleted_children (parent_id, child_hash)
  SELECT DISTINCT parent.id, child.hash
  FROM transactions child
  JOIN inputs i ON i.transaction_id = child.id
  JOIN transactions parent ON parent.hash = i.previous_transaction_hash
  WHERE child.delete_at_height IS NOT NULL
    AND child.delete_at_height <= $1
  ON CONFLICT (parent_id, child_hash) DO NOTHING`

	const deleteQuery = `DELETE FROM transactions
  WHERE delete_at_height IS NOT NULL
    AND delete_at_height <= $1`

	txn, err := s.db.BeginTx(ctx, pruneTxOptions)
	if err != nil {
		return 0, &pruneStepError{step: "failed to begin pruning transaction", err: err}
	}

	defer func() { _ = txn.Rollback() }()

	if _, err := txn.ExecContext(ctx, markerQuery, blockHeight); err != nil {
		return 0, &pruneStepError{step: "failed to mark pruned children", err: err}
	}

	result, err := txn.ExecContext(ctx, deleteQuery, blockHeight)
	if err != nil {
		return 0, &pruneStepError{step: "failed to delete transactions", err: err}
	}

	return finishPrune(txn, result)
}

// pruneWithDefensiveCheck verifies that every spending child of a candidate is
// mined and stable before deleting it, so no child is orphaned.
//
// The candidates are materialised into a temp table first. Temporary tables are
// connection-local and this transaction owns the connection until commit or
// rollback. Postgres drops it at COMMIT via ON COMMIT DROP; SQLite has no such
// clause and keeps a committed temp table on the connection, so the leading
// DROP TABLE IF EXISTS clears any leftover. Neither adds a failure path AFTER
// the delete has already happened, which is the point: an explicit DROP running
// after RowsAffected throws away a completed prune if it errors.
func (s *Service) pruneWithDefensiveCheck(ctx context.Context, blockHeight uint32) (int64, error) {
	const dropStale = `DROP TABLE IF EXISTS utxo_prune_candidates`

	const createCandidatesPostgres = `CREATE TEMP TABLE utxo_prune_candidates ON COMMIT DROP AS
			SELECT t.id
			FROM transactions t
			WHERE t.delete_at_height IS NOT NULL
			  AND t.delete_at_height <= $1
			  AND NOT EXISTS (
			    -- Find ANY unstable child - if found, parent cannot be deleted
			    -- This ensures ALL children must be stable before parent deletion
			    SELECT 1
			    FROM outputs o
			    WHERE o.transaction_id = t.id
			      AND o.spending_data IS NOT NULL
			      AND NOT EXISTS (
			          SELECT 1 FROM deleted_children d
			          WHERE d.parent_id = t.id
			            AND d.child_hash = substr(o.spending_data, 1, 32)
			      )
			      AND NOT EXISTS (
			        -- Extract child TX hash from spending_data (first 32 bytes)
			        -- Check if this child is NOT stable
			        SELECT 1
			        FROM transactions child
			        INNER JOIN block_ids child_blocks ON child.id = child_blocks.transaction_id
			        WHERE child.hash = substr(o.spending_data, 1, 32)
			          AND child.unmined_since IS NULL  -- Child must be mined
			          AND child_blocks.block_height <= ($1 - $2)  -- Child must be stable
			      )
			  )`

	const createCandidatesPortable = `CREATE TEMP TABLE utxo_prune_candidates AS
			SELECT t.id
			FROM transactions t
			WHERE t.delete_at_height IS NOT NULL
			  AND t.delete_at_height <= $1
			  AND NOT EXISTS (
			    SELECT 1
			    FROM outputs o
			    WHERE o.transaction_id = t.id
			      AND o.spending_data IS NOT NULL
			      AND NOT EXISTS (
			          SELECT 1 FROM deleted_children d
			          WHERE d.parent_id = t.id
			            AND d.child_hash = substr(o.spending_data, 1, 32)
			      )
			      AND NOT EXISTS (
			        SELECT 1
			        FROM transactions child
			        INNER JOIN block_ids child_blocks ON child.id = child_blocks.transaction_id
			        WHERE child.hash = substr(o.spending_data, 1, 32)
			          AND child.unmined_since IS NULL
			          AND child_blocks.block_height <= ($1 - $2)
			      )
			  )`

	const markerQuery = `INSERT INTO deleted_children (parent_id, child_hash)
  SELECT DISTINCT parent.id, child.hash
  FROM utxo_prune_candidates candidate
  JOIN transactions child ON child.id = candidate.id
  JOIN inputs i ON i.transaction_id = child.id
  JOIN transactions parent ON parent.hash = i.previous_transaction_hash
  ON CONFLICT (parent_id, child_hash) DO NOTHING`

	const deleteQuery = `DELETE FROM transactions WHERE id IN (SELECT id FROM utxo_prune_candidates)`

	createCandidates := createCandidatesPortable
	if s.engine == "postgres" {
		createCandidates = createCandidatesPostgres
	}

	txn, err := s.db.BeginTx(ctx, pruneTxOptions)
	if err != nil {
		return 0, &pruneStepError{step: "failed to begin pruning transaction", err: err}
	}

	defer func() { _ = txn.Rollback() }()

	if _, err := txn.ExecContext(ctx, dropStale); err != nil {
		return 0, &pruneStepError{step: "failed to clear stale pruning candidates", err: err}
	}

	if _, err := txn.ExecContext(ctx, createCandidates, blockHeight, s.safetyWindow); err != nil {
		return 0, &pruneStepError{step: "failed to select pruning candidates", err: err}
	}

	if _, err := txn.ExecContext(ctx, markerQuery); err != nil {
		return 0, &pruneStepError{step: "failed to mark pruned children", err: err}
	}

	result, err := txn.ExecContext(ctx, deleteQuery)
	if err != nil {
		return 0, &pruneStepError{step: "failed to delete transactions", err: err}
	}

	return finishPrune(txn, result)
}
