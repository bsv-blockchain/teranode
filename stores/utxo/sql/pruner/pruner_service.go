package pruner

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// Ensure Store implements the Pruner Service interface
var _ pruner.Service = (*Service)(nil)

var (
	// prometheusSQLPrunerChildrenHeldBack counts, per prune, the transactions
	// past their delete-at-height that the DELETE refused because a parent
	// output they claim is present but unspent or malformed. The same rows are
	// refused every cycle until that output records their spend, so a count
	// that keeps rising means a held-back set that is not draining: nothing is
	// pruned there and the UTXO set grows by those rows.
	prometheusSQLPrunerChildrenHeldBack prometheus.Counter

	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(func() {
		prometheusSQLPrunerChildrenHeldBack = promauto.NewCounter(prometheus.CounterOpts{
			Name: "utxo_sql_pruner_children_held_back_total",
			Help: "Transactions past their delete-at-height left in place by a prune because a parent output they claim is present but unspent or malformed, so no replay marker can be justified; counted again on every prune that refuses them",
		})
	})
}

// Service implements the utxo.CleanupService interface for SQL-based UTXO stores
type Service struct {
	safetyWindow     uint32 // Block height retention for child stability verification
	defensiveEnabled bool   // Enable defensive checks before deleting UTXO transactions
	engine           string // URL scheme of the store ("postgres" or "sqlite*"); selects engine-specific SQL
	logger           ulogger.Logger
	settings         *settings.Settings
	db               *usql.DB
	ctx              context.Context
	reapConflicting  func(ctx context.Context, txHash chainhash.Hash, blockHeight, safetyWindow uint32) (bool, error)
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

	// Engine is the store URL scheme, "postgres", "sqlite" or "sqlitememory", and NewService
	// rejects anything else, empty included. It selects the engine-specific SQL: on "postgres"
	// the candidate row locks (FOR NO KEY UPDATE SKIP LOCKED, which make a concurrent setDAH or
	// replay wait for the prune), ON COMMIT DROP for the candidates table and the retry
	// predicate for serialization failures and deadlocks; on the SQLite engines the write-lock
	// statement that opens the transaction as a writer and the message match of the retry
	// predicate. A Postgres store given a SQLite engine would prune without the row locks.
	Engine string

	// ReapConflicting deletes one expired conflicting record and the references its parents hold
	// to it in one transaction. It returns false when the record is no longer eligible or when a
	// conflict intent is pending, and the pruner then keeps the record for a later block.
	ReapConflicting func(ctx context.Context, txHash chainhash.Hash, blockHeight, safetyWindow uint32) (bool, error)
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

	switch opts.Engine {
	case "postgres", "sqlite", "sqlitememory":
	default:
		return nil, errors.NewProcessingError("engine must be postgres, sqlite or sqlitememory, got %q", opts.Engine)
	}

	safetyWindow := opts.SafetyWindow
	if safetyWindow == 0 {
		// Default to global retention setting (288 blocks)
		safetyWindow = tSettings.GlobalBlockHeightRetention
	}

	initPrometheusMetrics()

	service := &Service{
		safetyWindow:     safetyWindow,
		defensiveEnabled: tSettings.Pruner.UTXODefensiveEnabled,
		engine:           opts.Engine,
		logger:           opts.Logger,
		settings:         tSettings,
		db:               opts.DB,
		ctx:              opts.Ctx,
		reapConflicting:  opts.ReapConflicting,
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
	deletedCount, heldBack, err := s.deleteTombstoned(ctx, blockHeight)
	if err != nil {
		s.logger.Errorf("[pruner][%s:%d] phase 2: cleanup failed: %v", blockHashStr, blockHeight, err)
		return 0, err
	}

	if heldBack > 0 {
		prometheusSQLPrunerChildrenHeldBack.Add(float64(heldBack))

		s.logger.Warnf("[pruner][%s:%d] phase 2: held back %s transactions past their delete-at-height because a parent output they claim is unspent or malformed; they are retried on every prune",
			blockHashStr, blockHeight, util.FormatComma(heldBack))
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
// per-statement retry or its circuit breaker. On Postgres the transaction can be
// chosen as the victim of a deadlock (40P01). On SQLite the transaction opens
// with a write (takeSQLiteWriteLock), so it cannot hit SQLITE_BUSY_SNAPSHOT from a
// reader upgrade; it can still get SQLITE_BUSY or SQLITE_LOCKED once busy_timeout
// runs out. Both are safe to retry by re-running the whole transaction, so the
// retry lives here.
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
//
// engine is the store's URL scheme. The message fallback at the end applies to
// SQLite only: on Postgres a conflict always arrives with its SQLSTATE, and a
// wrapped error whose text merely contains "database is locked" is not one.
func isPruneRetryable(err error, engine string) bool {
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

	if engine == "postgres" {
		return false
	}

	msg := strings.ToLower(err.Error())

	return strings.Contains(msg, "database is locked") || strings.Contains(msg, "database table is locked")
}

// DefensiveDeleteFilter selects the records the defensive pruner may delete. It takes the block height as
// $1 and the safety window as $2 and expects the transactions table aliased as t. A spent output does not
// hold its parent back once the pruner has marked that output's spender as deleted, because a marked child
// is already gone. The SQL store's reapConflicting re-checks it under the record's lock.
const DefensiveDeleteFilter = `t.delete_at_height IS NOT NULL
			  AND t.delete_at_height <= $1
			  AND (t.unmined_since IS NULL OR t.conflicting)
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

// excludeConflicting is appended to the defensive candidate selection when a reap function is set.
const excludeConflicting = `
			  AND NOT t.conflicting`

// reapExpiredConflicting reaps every expired conflicting record the defensive delete skipped. Each
// record is reaped in its own transaction by the store, which keeps a record that was promoted or that
// has a conflict intent pending. A record that cannot be reaped is kept and retried next block.
func (s *Service) reapExpiredConflicting(ctx context.Context, blockHeight, safetyWindow uint32) (int64, error) {
	if s.reapConflicting == nil {
		return 0, nil
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT t.hash FROM transactions t
		WHERE `+DefensiveDeleteFilter+`
		  AND t.conflicting
	`, blockHeight, safetyWindow)
	if err != nil {
		return 0, errors.NewStorageError("failed to list expired conflicting transactions", err)
	}

	defer rows.Close()

	var hashes []chainhash.Hash

	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return 0, errors.NewStorageError("failed to scan expired conflicting transaction", err)
		}

		hash, hashErr := chainhash.NewHash(raw)
		if hashErr != nil {
			continue
		}

		hashes = append(hashes, *hash)
	}

	if err = rows.Err(); err != nil {
		return 0, errors.NewStorageError("failed to read expired conflicting transactions", err)
	}

	if err = rows.Close(); err != nil {
		return 0, errors.NewStorageError("failed to close expired conflicting transactions", err)
	}

	var reaped int64

	for i := range hashes {
		ok, reapErr := s.reapConflicting(ctx, hashes[i], blockHeight, safetyWindow)
		if reapErr != nil {
			s.logger.Errorf("[pruner] keeping conflicting transaction %s: %v", hashes[i].String(), reapErr)

			continue
		}

		if ok {
			reaped++
		}
	}

	return reaped, nil
}

// deleteTombstoned removes transactions that have passed their expiration time,
// re-running the whole transaction on a serialization or lock conflict.
//
// Only deletes parent transactions if their last spending child is mined and
// stable (defensive mode only).
//
// Records that are currently unmined (unmined_since set) are skipped unless they
// are conflicting, even if they still carry a delete_at_height:
// MarkTransactionsOnLongestChain(false) does not clear it. This is defence in
// depth: the check and the delete share one transaction, but a record that
// becomes unmined concurrently with it is not covered. A skipped record has its
// delete_at_height cleared (unless preserve_until is set), matching the store
// invariant that an unmined transaction has no DAH, so it no longer matches the
// DAH index in later cycles. The marker INSERT carries the same clause, so a
// skipped record never gets a marker either.
func (s *Service) deleteTombstoned(ctx context.Context, blockHeight uint32) (deleted, heldBack int64, err error) {
	s.clearUnminedDeleteAtHeight(ctx, blockHeight)

	for attempt := 0; attempt < maxPruneAttempts; attempt++ {
		deleted, heldBack, err = s.deleteTombstonedTx(ctx, blockHeight)
		if err == nil {
			if !s.defensiveEnabled {
				return deleted, heldBack, nil
			}

			// The defensive transaction left expired conflicting records out
			// when a reap function is set. Each is reaped on its own, after the
			// commit, so a reap that fails never rolls back the prune above.
			reaped, reapErr := s.reapExpiredConflicting(ctx, blockHeight, s.safetyWindow)

			return deleted + reaped, heldBack, reapErr
		}

		if !isPruneRetryable(err, s.engine) {
			return 0, 0, errors.NewStorageError("pruning transaction failed", err)
		}

		if attempt == maxPruneAttempts-1 {
			break
		}

		backoff := pruneRetryBaseWait << uint(attempt)

		s.logger.Warnf("[pruner] serialization/lock conflict on pruning transaction (attempt %d/%d), retrying in %s: %v",
			attempt+1, maxPruneAttempts, backoff, err)

		select {
		case <-ctx.Done():
			return 0, 0, errors.NewContextCanceledError("[pruner] cancelled while waiting to retry the pruning transaction", ctx.Err())
		case <-time.After(backoff):
		}
	}

	s.logger.Warnf("[pruner] serialization/lock conflict persisted after %d attempts: %v", maxPruneAttempts, err)

	return 0, 0, errors.NewStorageError("pruning transaction failed after %d attempts", maxPruneAttempts, err)
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
// candidates, write a replay marker on each parent output that records the
// candidate as its spender (held-back candidates get none), then delete the
// candidates. Marker-before-delete is the whole point, so the two
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
// A one-shot backfill keyed on "the spender has no row" is the obvious way to
// clear the backlog, and it is NOT safe. Pruning is not the only producer of
// that state. The validator's shed unwind deletes a transaction's record first
// and reverses its spends afterwards (services/validator unwindShed, which calls
// DeleteComplete and has arms that return before the Unspend), so a node that
// sheds transaction X under backpressure and then fails between those two steps
// leaves X's spend recorded on a parent output with no row for X. X is a valid
// unmined transaction whose sender will resubmit it. A backfill cannot tell the
// two apart, and marking (parent, X) is worse than the gap it closes: every
// future spend of that output by X is refused permanently with
// ERR_UTXO_SPENDING_TX_PRUNED, while no one else can take it either, because the
// output still records X as its spender. The output is burned. Raised by
// icellan in review.
//
// So the backlog has no safe automatic remedy from inside the store, and this
// package deliberately offers no recipe for one. What an operator can do is
// re-sync the affected store, which rebuilds both the records and their markers
// from the chain. Anything narrower needs a source of truth this table does not
// have: whether the absent spender was mined and pruned, or merely lost.
func (s *Service) deleteTombstonedTx(ctx context.Context, blockHeight uint32) (deleted, heldBack int64, err error) {
	// Every statement below is built from string constants only. The runtime
	// joins append the constants excludeConflicting and candidateLockSuffix, and
	// no part of the SQL is ever built from data.
	//
	// The transaction runs at READ COMMITTED. Its three decisions (which rows are
	// candidates, which of them are held back, and which markers to write) are
	// each made once, into the utxo_prune_candidates temp table, and the marker
	// INSERT and the DELETE are both driven from that table by id. They cannot
	// disagree about the set: a child is deleted exactly when it is in the
	// table and not held back, and it gets its markers in the same transaction.
	//
	// What isolation alone could not give, the row locks do. On Postgres the
	// candidate rows are locked (FOR NO KEY UPDATE, ascending id, SKIP LOCKED)
	// by the statement that selects them, so a writer that touches a candidate
	// afterwards, such as setDAH bumping the stamp of a parent whose child is
	// replayed, waits for this transaction and then finds the row gone. A row
	// another transaction already holds is skipped and tried again next block.
	// SERIALIZABLE gave the same protection by aborting the whole prune with a
	// serialization failure on the first such touch, which any caller could cause
	// by resubmitting an already mined transaction. A locked row is skipped here
	// instead, so a write to a candidate row no longer aborts the transaction.
	// The DELETE's cascade into outputs can still wait on a spend batch, and that
	// batch can then wait for a candidate: Postgres ends the cycle with 40P01,
	// and both sides retry (see candidateLockSuffix).
	//
	// SQLite has one writer. The transaction opens with a statement that writes
	// nothing and takes the write lock (takeSQLiteWriteLock), so no other writer
	// can commit between its reads and its first real write.
	//
	// The hold-back verdict is a later statement, so it reads the parents'
	// outputs after the locks are held. It still reads rows this transaction does
	// not lock (a parent output can be released between the verdict and the
	// DELETE), which is the same state as an Unspend that lands just after the
	// prune commits: the marker then names an output that no longer records its
	// child, and the spend path only refuses a marked child while the output
	// still names it.
	if s.defensiveEnabled {
		return s.pruneCandidates(ctx, blockHeight, defensiveCandidatesQuery(s.reapConflicting != nil), s.safetyWindow)
	}

	return s.pruneCandidates(ctx, blockHeight, nonDefensiveCandidatesQuery, nil)
}

// pruneTxOptions is the isolation the pruning transaction asks for.
//
// READ COMMITTED, the default. See deleteTombstonedTx for what the temp table
// and the row locks give in place of a serializable snapshot. modernc.org/sqlite's
// conn.BeginTx reads opts.ReadOnly and the DSN _txlock mode and never inspects
// opts.Isolation, and util/sql.go builds the SQLite DSN with no _txlock, so what
// SQLite gets is a plain deferred BEGIN. The cross-statement atomicity the pruner
// needs still holds there: one writer at a time, and the transaction rolls back as
// a unit. The transaction opens with takeSQLiteWriteLock, so it is a writer before
// it reads anything and cannot fail to upgrade from a reader with
// SQLITE_BUSY_SNAPSHOT. deleteTombstoned still retries the whole transaction on
// SQLITE_BUSY or SQLITE_LOCKED, which busy_timeout can run out on.
var pruneTxOptions = &sql.TxOptions{Isolation: sql.LevelReadCommitted}

// finishPrune reads the delete's row count and commits. Nothing runs between
// the DELETE and the COMMIT except RowsAffected, which reads a value the driver
// already holds: any statement placed there would throw away a completed prune
// if it failed, and on Postgres a failed statement aborts the transaction, so
// even ignoring its error would not save the DELETE. That is why the held-back
// count comes from the hold-back statement, which runs before the DELETE.
func finishPrune(txn *sql.Tx, result sql.Result) (int64, error) {
	deleted, err := result.RowsAffected()
	if err != nil {
		return 0, &pruneStepError{step: "failed to get rows affected", err: err}
	}

	if err := txn.Commit(); err != nil {
		return 0, &pruneStepError{step: "failed to commit pruning transaction", err: err}
	}

	return deleted, nil
}

// candidateLockSuffix is appended to a candidate selection on Postgres. It takes
// the row lock the DELETE needs, in the order the SetMinedMulti lock uses, and
// skips a row another transaction holds rather than waiting for it, so selecting
// the candidates never waits. The DELETE can still join a lock cycle with a spend
// batch through its cascade into outputs: a batch that spends an unspent output
// of a conflicting candidate holds that output row, then waits in setDAH for the
// candidate this prune holds, while the cascade waits for the output row.
// Postgres breaks the cycle with 40P01, and both sides retry: the prune through
// isPruneRetryable, the batch through its deadlock retry.
const candidateLockSuffix = `
ORDER BY t.id FOR NO KEY UPDATE OF t SKIP LOCKED`

// withCandidateLock appends candidateLockSuffix to a candidate selection. Both
// parts are constants.
func withCandidateLock(candidatesQuery string) string {
	return candidatesQuery + candidateLockSuffix
}

// nonDefensiveCandidatesQuery selects every record past its expiration.
const nonDefensiveCandidatesQuery = `INSERT INTO utxo_prune_candidates (id)
SELECT t.id
FROM transactions t
WHERE t.delete_at_height IS NOT NULL
  AND t.delete_at_height <= $1
  AND (t.unmined_since IS NULL OR t.conflicting)`

// defensiveCandidatesQuery selects the records whose spending children are all
// mined and stable. With a reap function, expired conflicting records are left
// to reapExpiredConflicting, which clears the spends their parents hold to them
// before deleting them. Both fragments are constants.
func defensiveCandidatesQuery(reap bool) string {
	q := `INSERT INTO utxo_prune_candidates (id)
SELECT t.id
FROM transactions t
WHERE ` + DefensiveDeleteFilter

	if reap {
		q += excludeConflicting
	}

	return q
}

// takeSQLiteWriteLock: SQLite starts a deferred transaction as a reader and fails the first write
// after another writer committed with SQLITE_BUSY_SNAPSHOT, which busy_timeout
// cannot wait out. A statement that writes takes the write lock when it starts,
// whether or not it changes a row, so this one makes the transaction a writer
// before it reads anything. Postgres needs no such statement: the candidate
// locks do the work.
const takeSQLiteWriteLock = `DELETE FROM deleted_children WHERE 1 = 0`

// pruneCandidates runs the pruning transaction: select and lock the candidates,
// decide which of them are held back, write a marker for each parent output that
// records a deleted candidate as its spender, then delete the candidates that
// were not held back.
//
// candidatesQuery inserts the candidate ids into utxo_prune_candidates. Its
// arguments are blockHeight followed by safetyWindow when one is given.
func (s *Service) pruneCandidates(ctx context.Context, blockHeight uint32, candidatesQuery string, safetyWindow any) (deleted, heldBack int64, err error) {
	const dropStale = `DROP TABLE IF EXISTS utxo_prune_candidates`

	// held_back is 0 until the hold-back statement flags a candidate.
	const createCandidatesPostgres = `CREATE TEMP TABLE utxo_prune_candidates (id BIGINT PRIMARY KEY, held_back INTEGER NOT NULL DEFAULT 0) ON COMMIT DROP`

	const createCandidatesPortable = `CREATE TEMP TABLE utxo_prune_candidates (id BIGINT PRIMARY KEY, held_back INTEGER NOT NULL DEFAULT 0)`

	// The unverified-claim clause. A candidate is held back when one of its
	// inputs names a parent record that is present but whose output does not
	// carry well-formed spending data: no such output row, NULL spending data, or
	// fewer than the 36 bytes of txid and vin. That is what a rolled-back spend
	// leaves. Such a child would get no marker, and a replay of it would then be
	// recreated and spend that output cleanly, so it stays. A parent output
	// naming a different, well-formed spender (a conflicting loser) does not hold
	// the child back, and a parent that is gone needs no marker.
	//
	// The rule is per input here and per parent record in the Aerospike
	// pruner's keepSpendHolders, which holds a child back only when no claimed
	// output on a record names it and at least one is unspent or malformed. Take
	// a child with inputs P:0 and P:1 on one record, P:0 naming it and P:1 unspent
	// after a truncated rollback. Aerospike marks P and deletes the child. This
	// query holds the child back, and it stays and is counted on every prune.
	// Both are replay-safe: P:0's marker refuses a replay, and the rollback
	// reverses P:1.
	//
	// The one statement feeds both the marker INSERT and the DELETE, so a
	// held-back child gets no marker anywhere: the spend path refuses an
	// idempotent re-spend by a marked child, so a marker on a child that stays
	// would refuse that live child's own re-spend.
	//
	// A conflicting row never matches. ProcessConflicting releases every input of
	// a loser and of each descendant it cascades to, the uncontested ones too, so
	// such a loser has an input that looks like a rolled-back claim, and holding
	// it back would keep it forever. It needs no marker for those inputs: a replay
	// of it meets the winner's spend and is stored as conflicting again. Not every
	// path that marks a record conflicting releases its claims: SetConflicting
	// returns the parent spends without clearing them, and the callers of
	// MarkConflictingRecursively in block assembly and the validator drop them.
	// A parent output that still names such a loser gets a marker from the
	// query below, like any other output that names a deleted child.
	const holdBackQuery = `UPDATE utxo_prune_candidates SET held_back = 1
WHERE EXISTS (SELECT 1
  FROM transactions child
  JOIN inputs hi ON hi.transaction_id = child.id
  JOIN transactions hp ON hp.hash = hi.previous_transaction_hash
  LEFT JOIN outputs ho ON ho.transaction_id = hp.id AND ho.idx = hi.previous_tx_idx
  WHERE child.id = utxo_prune_candidates.id
    AND NOT child.conflicting
    AND (ho.transaction_id IS NULL OR ho.spending_data IS NULL OR length(ho.spending_data) < 36))`

	// The join to outputs is load-bearing, not a tidier way to reach the parent.
	// An input naming a parent outpoint proves only that this child ASKED for it;
	// a conflicting loser references the same outpoint and never won it, and
	// losers are pruning candidates like anything else. The spend path refuses a
	// spender only while the output records it, so a marker for a loser is inert
	// today, but it would refuse that loser's own spend if it ever won the output
	// after an Unspend. Only the child the output actually records as its spender
	// gets a marker, the rule the Aerospike pruner's filter applies.
	//
	// A parent that is itself deleted by this prune gets no marker: the cascade
	// would remove it again before the commit, after a foreign-key lock and a
	// heap write for nothing.
	const markerQuery = `INSERT INTO deleted_children (parent_id, child_hash)
  SELECT DISTINCT parent.id, child.hash
  FROM utxo_prune_candidates candidate
  JOIN transactions child ON child.id = candidate.id
  JOIN inputs i ON i.transaction_id = child.id
  JOIN transactions parent ON parent.hash = i.previous_transaction_hash
  JOIN outputs o ON o.transaction_id = parent.id
    AND o.idx = i.previous_tx_idx
    AND substr(o.spending_data, 1, 32) = child.hash
  WHERE candidate.held_back = 0
    AND NOT EXISTS (SELECT 1 FROM utxo_prune_candidates doomed
      WHERE doomed.id = parent.id AND doomed.held_back = 0)
  ON CONFLICT (parent_id, child_hash) DO NOTHING`

	const deleteQuery = `DELETE FROM transactions
  WHERE id IN (SELECT id FROM utxo_prune_candidates WHERE held_back = 0)`

	createCandidates := createCandidatesPortable
	if s.engine == "postgres" {
		createCandidates = createCandidatesPostgres
		candidatesQuery = withCandidateLock(candidatesQuery)
	}

	args := []any{blockHeight}
	if safetyWindow != nil {
		args = append(args, safetyWindow)
	}

	txn, err := s.db.BeginTx(ctx, pruneTxOptions)
	if err != nil {
		return 0, 0, &pruneStepError{step: "failed to begin pruning transaction", err: err}
	}

	defer func() { _ = txn.Rollback() }()

	if s.engine != "postgres" {
		if _, err := txn.ExecContext(ctx, takeSQLiteWriteLock); err != nil {
			return 0, 0, &pruneStepError{step: "failed to take the write lock", err: err}
		}
	}

	// Postgres drops the table at COMMIT. SQLite has no such clause and keeps a
	// committed temp table on the connection, so this clears any leftover. Neither
	// adds a failure path after the delete has already happened.
	if _, err := txn.ExecContext(ctx, dropStale); err != nil {
		return 0, 0, &pruneStepError{step: "failed to clear stale pruning candidates", err: err}
	}

	if _, err := txn.ExecContext(ctx, createCandidates); err != nil {
		return 0, 0, &pruneStepError{step: "failed to create the pruning candidates table", err: err}
	}

	if _, err := txn.ExecContext(ctx, candidatesQuery, args...); err != nil {
		return 0, 0, &pruneStepError{step: "failed to select pruning candidates", err: err}
	}

	held, err := txn.ExecContext(ctx, holdBackQuery)
	if err != nil {
		return 0, 0, &pruneStepError{step: "failed to hold back unverified children", err: err}
	}

	if heldBack, err = held.RowsAffected(); err != nil {
		return 0, 0, &pruneStepError{step: "failed to count held-back children", err: err}
	}

	if _, err := txn.ExecContext(ctx, markerQuery); err != nil {
		return 0, 0, &pruneStepError{step: "failed to mark pruned children", err: err}
	}

	result, err := txn.ExecContext(ctx, deleteQuery)
	if err != nil {
		return 0, 0, &pruneStepError{step: "failed to delete transactions", err: err}
	}

	if deleted, err = finishPrune(txn, result); err != nil {
		return 0, 0, err
	}

	return deleted, heldBack, nil
}

// clearUnminedDeleteAtHeight clears delete_at_height on unmined, non-conflicting, non-preserved records
// that are due for deletion, so the delete below skips them and later cycles no longer match them.
// A failure is only logged: nothing is deleted on its account and the rows are retried next cycle.
func (s *Service) clearUnminedDeleteAtHeight(ctx context.Context, blockHeight uint32) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE transactions
		SET delete_at_height = NULL
		WHERE delete_at_height IS NOT NULL
		  AND delete_at_height <= $1
		  AND unmined_since IS NOT NULL
		  AND NOT conflicting
		  AND preserve_until IS NULL
	`, blockHeight)
	if err != nil {
		s.logger.Errorf("[pruner] failed to clear delete_at_height of unmined records: %v", err)
		return
	}

	if count, err := result.RowsAffected(); err == nil && count > 0 {
		s.logger.Infof("[pruner] cleared delete_at_height of %d unmined records", count)
	}
}
