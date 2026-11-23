package sql

import (
	"context"
	"database/sql"
	"net/url"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/stretchr/testify/require"
)

func conflictingIndexSettings(t *testing.T) *settings.Settings {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 30 * time.Second
	tSettings.BatcherDrainMode = true

	return tSettings
}

// openConflictingIndexStore opens a store on dbURL within timeout, failing the
// test if New does not return, and closes it at cleanup.
func openConflictingIndexStore(t *testing.T, dbURL *url.URL, timeout time.Duration) *Store {
	t.Helper()

	return openConflictingIndexStoreWith(t, dbURL, timeout, conflictingIndexSettings(t))
}

func openConflictingIndexStoreWith(t *testing.T, dbURL *url.URL, timeout time.Duration, tSettings *settings.Settings) *Store {
	t.Helper()

	type result struct {
		store *Store
		err   error
	}

	done := make(chan result, 1)

	go func() {
		store, err := New(context.Background(), ulogger.TestLogger{}, tSettings, dbURL)
		done <- result{store, err}
	}()

	select {
	case res := <-done:
		require.NoError(t, res.err)
		t.Cleanup(func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			require.NoError(t, res.store.Close(closeCtx))
		})

		return res.store
	case <-time.After(timeout):
		require.FailNow(t, "store startup did not return", "waited %s", timeout)
		return nil
	}
}

// postgresConflictingIndex reports whether px_transactions_conflicting exists,
// whether Postgres lists it valid, and its definition.
func postgresConflictingIndex(t *testing.T, db *usql.DB) (exists, valid bool, definition string) {
	t.Helper()

	err := db.QueryRow(`SELECT i.indisvalid, pg_get_indexdef(i.indexrelid) FROM pg_index i WHERE i.indexrelid = to_regclass('px_transactions_conflicting')`).Scan(&valid, &definition)
	if err == sql.ErrNoRows {
		return false, false, ""
	}

	require.NoError(t, err)

	return true, valid, definition
}

func awaitValidConflictingIndex(t *testing.T, db *usql.DB) {
	t.Helper()

	require.Eventually(t, func() bool {
		_, valid, _ := postgresConflictingIndex(t, db)
		return valid
	}, 60*time.Second, 100*time.Millisecond, "px_transactions_conflicting never became valid")

	_, _, definition := postgresConflictingIndex(t, db)
	require.Contains(t, definition, "ON public.transactions USING btree (hash) WHERE (conflicting = true)")
	require.NotContains(t, definition, "UNIQUE")
}

func openAdminDB(t *testing.T, dbURL *url.URL) *usql.DB {
	t.Helper()

	db, err := util.InitSQLDB(ulogger.TestLogger{}, dbURL, conflictingIndexSettings(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	return db
}

func conflictingIndexTestTx(t *testing.T) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.From("4ff7de8f0b6d5c4a3e2f1d0c9b8a7968574635241302f1e0d9c8b7a695847362", 0, "51", 10_000))
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1_000))
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{bscript.OpTRUE})

	return tx
}

// TestPostgresConflictingIndex_BuiltConcurrentlyAfterStartup pins that the
// index is built with CREATE INDEX CONCURRENTLY after schema startup: while an
// older transaction holds a snapshot the build has to wait for, the store still
// starts, accepts writes to transactions, and the index only becomes valid once
// that transaction ends. A plain CREATE INDEX at startup would block writes for
// the whole build instead.
func TestPostgresConflictingIndex_BuiltConcurrentlyAfterStartup(t *testing.T) {
	dbURL := startPostgres(t)
	admin := openAdminDB(t, dbURL)

	openConflictingIndexStore(t, dbURL, 60*time.Second)
	awaitValidConflictingIndex(t, admin)

	_, err := admin.Exec(`DROP INDEX px_transactions_conflicting`)
	require.NoError(t, err)

	holder, err := admin.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	require.NoError(t, err)

	defer func() { _ = holder.Rollback() }()

	var one int
	require.NoError(t, holder.QueryRow(`SELECT 1`).Scan(&one))

	store := openConflictingIndexStore(t, dbURL, 30*time.Second)

	writeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = store.Create(writeCtx, conflictingIndexTestTx(t), 0)
	require.NoError(t, err, "a write to transactions was blocked by the index build")

	require.Eventually(t, func() bool {
		exists, _, _ := postgresConflictingIndex(t, admin)
		return exists
	}, 30*time.Second, 50*time.Millisecond, "the index build never started")

	_, valid, _ := postgresConflictingIndex(t, admin)
	require.False(t, valid, "the index was built without waiting for older snapshots, so not concurrently")

	// Until the build finishes the store sends block validation to its
	// per-transaction fallback instead of scanning the table on every block.
	hashes, indexed, err := store.ConflictingTxHashes(context.Background())
	require.NoError(t, err)
	require.False(t, indexed)
	require.Nil(t, hashes)

	require.NoError(t, holder.Commit())
	awaitValidConflictingIndex(t, admin)

	require.Eventually(t, func() bool {
		_, indexed, err := store.ConflictingTxHashes(context.Background())
		return err == nil && indexed
	}, 30*time.Second, 50*time.Millisecond, "the store never answered from the built index")
}

// TestPostgresConflictingIndex_RebuildsAnInvalidLeftover pins that an INVALID
// index left by a failed concurrent build is dropped and rebuilt, rather than
// kept forever by CREATE INDEX IF NOT EXISTS.
func TestPostgresConflictingIndex_RebuildsAnInvalidLeftover(t *testing.T) {
	dbURL := startPostgres(t)
	admin := openAdminDB(t, dbURL)

	first := openConflictingIndexStore(t, dbURL, 60*time.Second)
	awaitValidConflictingIndex(t, admin)

	_, err := first.Create(context.Background(), conflictingIndexTestTx(t), 0)
	require.NoError(t, err)

	_, err = admin.Exec(`DROP INDEX px_transactions_conflicting`)
	require.NoError(t, err)

	// Fails on the row created above and leaves an INVALID index behind.
	_, err = admin.Exec(`CREATE INDEX CONCURRENTLY px_transactions_conflicting ON transactions ((1 / (id - id)))`)
	require.Error(t, err)

	exists, valid, _ := postgresConflictingIndex(t, admin)
	require.True(t, exists)
	require.False(t, valid)

	openConflictingIndexStore(t, dbURL, 60*time.Second)
	awaitValidConflictingIndex(t, admin)
}

// TestPostgresConflictingIndex_SchemaStartupWaitsForABuild pins that schema
// startup does not run while another process builds the index. Its ALTER TABLE
// on transactions would queue an ACCESS EXCLUSIVE lock behind the build, and
// every later read and write of the table behind that.
func TestPostgresConflictingIndex_SchemaStartupWaitsForABuild(t *testing.T) {
	dbURL := startPostgres(t)
	admin := openAdminDB(t, dbURL)

	openConflictingIndexStore(t, dbURL, 60*time.Second)
	awaitValidConflictingIndex(t, admin)

	builder, err := admin.Conn(context.Background())
	require.NoError(t, err)

	defer func() { _ = builder.Close() }()

	_, err = builder.ExecContext(context.Background(), `SELECT pg_advisory_lock($1)`, conflictingIndexLockID)
	require.NoError(t, err)

	started := make(chan error, 1)

	go func() {
		store, newErr := New(context.Background(), ulogger.TestLogger{}, conflictingIndexSettings(t), dbURL)
		if newErr == nil {
			t.Cleanup(func() {
				closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				_ = store.Close(closeCtx)
			})
		}

		started <- newErr
	}()

	select {
	case newErr := <-started:
		require.FailNow(t, "schema startup ran while the index build lock was held", "New returned %v", newErr)
	case <-time.After(3 * time.Second):
	}

	_, err = builder.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, conflictingIndexLockID)
	require.NoError(t, err)

	select {
	case newErr := <-started:
		require.NoError(t, newErr)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "schema startup did not resume once the index build lock was released")
	}
}

// TestPostgresConflictingIndex_ValidIndexTakesNoTableLock pins that a store
// finding the index already valid does not issue CREATE INDEX again: that
// statement takes its table lock before it checks IF NOT EXISTS, and would wait
// behind anything holding a conflicting lock, such as an anti-wraparound vacuum.
func TestPostgresConflictingIndex_ValidIndexTakesNoTableLock(t *testing.T) {
	dbURL := startPostgres(t)
	admin := openAdminDB(t, dbURL)

	store := openConflictingIndexStore(t, dbURL, 60*time.Second)
	awaitValidConflictingIndex(t, admin)

	vacuum, err := admin.BeginTx(context.Background(), nil)
	require.NoError(t, err)

	defer func() { _ = vacuum.Rollback() }()

	_, err = vacuum.Exec(`LOCK TABLE transactions IN SHARE UPDATE EXCLUSIVE MODE`)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	built, err := store.buildPostgresConflictingIndex(ctx)
	require.NoError(t, err)
	require.True(t, built)
}

var (
	tryBuildLockQuery = regexp.QuoteMeta(`SELECT pg_try_advisory_lock($1)`)
	buildUnlockExec   = regexp.QuoteMeta(`SELECT pg_advisory_unlock($1)`)
	indexStateQuery   = regexp.QuoteMeta(`SELECT (SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('px_transactions_conflicting'))`)
	dropIndexExec     = regexp.QuoteMeta(`DROP INDEX CONCURRENTLY IF EXISTS px_transactions_conflicting`)
	createIndexExec   = regexp.QuoteMeta(`CREATE INDEX CONCURRENTLY IF NOT EXISTS px_transactions_conflicting ON transactions (hash) WHERE conflicting = true`)
)

func expectIndexState(m sqlmock.Sqlmock, valid any) {
	m.ExpectQuery(indexStateQuery).WillReturnRows(sqlmock.NewRows([]string{"indisvalid"}).AddRow(valid))
}

func expectBuildLock(m sqlmock.Sqlmock, locked bool) {
	m.ExpectQuery(tryBuildLockQuery).WithArgs(conflictingIndexLockID).WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(locked))
}

func expectBuildUnlock(m sqlmock.Sqlmock) {
	m.ExpectExec(buildUnlockExec).WithArgs(conflictingIndexLockID).WillReturnResult(sqlmock.NewResult(0, 0))
}

// TestBuildPostgresConflictingIndex_Steps pins each outcome of one build
// attempt: an error at any step is reported and the lock is released, and a
// lock held elsewhere is not an error but not built either.
func TestBuildPostgresConflictingIndex_Steps(t *testing.T) {
	failure := errors.NewStorageError("connection reset")

	tests := map[string]struct {
		expect    func(sqlmock.Sqlmock)
		wantBuilt bool
		wantErr   string
	}{
		"index already valid, lock never taken": {
			expect:    func(m sqlmock.Sqlmock) { expectIndexState(m, true) },
			wantBuilt: true,
		},
		"index state unreadable before the lock": {
			expect: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(indexStateQuery).WillReturnError(failure)
			},
			wantErr: "could not read the state",
		},
		"lock query fails": {
			expect: func(m sqlmock.Sqlmock) {
				expectIndexState(m, nil)
				m.ExpectQuery(tryBuildLockQuery).WithArgs(conflictingIndexLockID).WillReturnError(failure)
			},
			wantErr: "could not take the build lock",
		},
		"another process holds the lock": {
			expect: func(m sqlmock.Sqlmock) {
				expectIndexState(m, nil)
				expectBuildLock(m, false)
			},
		},
		"index state unreadable under the lock": {
			expect: func(m sqlmock.Sqlmock) {
				expectIndexState(m, nil)
				expectBuildLock(m, true)
				m.ExpectQuery(indexStateQuery).WillReturnError(failure)
				expectBuildUnlock(m)
			},
			wantErr: "could not read the state",
		},
		"built by another process meanwhile": {
			expect: func(m sqlmock.Sqlmock) {
				expectIndexState(m, nil)
				expectBuildLock(m, true)
				expectIndexState(m, true)
				expectBuildUnlock(m)
			},
			wantBuilt: true,
		},
		"invalid leftover cannot be dropped": {
			expect: func(m sqlmock.Sqlmock) {
				expectIndexState(m, false)
				expectBuildLock(m, true)
				expectIndexState(m, false)
				m.ExpectExec(dropIndexExec).WillReturnError(failure)
				expectBuildUnlock(m)
			},
			wantErr: "could not drop the invalid",
		},
		"build fails": {
			expect: func(m sqlmock.Sqlmock) {
				expectIndexState(m, nil)
				expectBuildLock(m, true)
				expectIndexState(m, nil)
				m.ExpectExec(createIndexExec).WillReturnError(failure)
				expectBuildUnlock(m)
			},
			wantErr: "could not create px_transactions_conflicting",
		},
		"invalid leftover is rebuilt": {
			expect: func(m sqlmock.Sqlmock) {
				expectIndexState(m, false)
				expectBuildLock(m, true)
				expectIndexState(m, false)
				m.ExpectExec(dropIndexExec).WillReturnResult(sqlmock.NewResult(0, 0))
				m.ExpectExec(createIndexExec).WillReturnResult(sqlmock.NewResult(0, 0))
				expectBuildUnlock(m)
			},
			wantBuilt: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			store, mock := CreateMockStore(ulogger.TestLogger{})
			tc.expect(mock)

			built, err := store.buildPostgresConflictingIndex(context.Background())
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantErr)
			}

			require.Equal(t, tc.wantBuilt, built)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestPostgresConflictingIndexBuild_FailedBuildLeavesTheStoreNotIndexed pins
// that a build that fails leaves ConflictingTxHashes on the fallback and waits
// to retry, and that stopping the build does not wait out the retry delay.
func TestPostgresConflictingIndexBuild_FailedBuildLeavesTheStoreNotIndexed(t *testing.T) {
	store, mock := CreateMockStore(ulogger.TestLogger{})
	expectIndexState(mock, nil)
	mock.ExpectQuery(tryBuildLockQuery).WithArgs(conflictingIndexLockID).WillReturnError(errors.NewStorageError("connection reset"))

	store.startPostgresConflictingIndexBuild(context.Background())

	require.Eventually(t, func() bool {
		return mock.ExpectationsWereMet() == nil
	}, 5*time.Second, 10*time.Millisecond)

	hashes, indexed, err := store.ConflictingTxHashes(context.Background())
	require.NoError(t, err)
	require.False(t, indexed)
	require.Nil(t, hashes)

	stopped := make(chan struct{})

	go func() {
		store.stopPostgresConflictingIndexBuild()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "stopping the build waited for the retry delay")
	}
}

// TestHoldConflictingIndexBuildLock_Errors pins that schema startup reports a
// failed lock query, and gives up when its context ends while another process
// is building.
func TestHoldConflictingIndexBuildLock_Errors(t *testing.T) {
	t.Run("lock query fails", func(t *testing.T) {
		store, mock := CreateMockStore(ulogger.TestLogger{})
		mock.ExpectQuery(tryBuildLockQuery).WithArgs(conflictingIndexLockID).WillReturnError(errors.NewStorageError("connection reset"))

		conn, err := store.db.Conn(context.Background())
		require.NoError(t, err)

		release, err := holdConflictingIndexBuildLock(context.Background(), ulogger.TestLogger{}, conn)
		require.ErrorContains(t, err, "could not take the index build lock")
		require.Nil(t, release)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("context ends while another process builds", func(t *testing.T) {
		store, mock := CreateMockStore(ulogger.TestLogger{})
		expectBuildLock(mock, false)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		conn, err := store.db.Conn(context.Background())
		require.NoError(t, err)

		release, err := holdConflictingIndexBuildLock(ctx, ulogger.TestLogger{}, conn)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, release)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestPostgresConflictingIndex_StartsWithTwoConnections pins that schema startup
// fits in a pool of two connections: it holds the index build lock and the
// schema lock and runs the schema statements on one connection, so it never
// waits for a connection it is itself holding.
func TestPostgresConflictingIndex_StartsWithTwoConnections(t *testing.T) {
	dbURL := startPostgres(t)

	tSettings := conflictingIndexSettings(t)
	tSettings.Postgres.MaxOpenConns = 2
	tSettings.Postgres.MaxIdleConns = 2

	if tSettings.UtxoStore.PostgresPool != nil {
		tSettings.UtxoStore.PostgresPool.MaxOpenConns = 2
		tSettings.UtxoStore.PostgresPool.MaxIdleConns = 2
	}

	store := openConflictingIndexStoreWith(t, dbURL, 30*time.Second, tSettings)
	require.Equal(t, 2, store.db.Stats().MaxOpenConnections)

	awaitValidConflictingIndex(t, openAdminDB(t, dbURL))
}

// TestPostgresConflictingIndex_CloseCancelsTheBuildOnTheServer pins that closing
// a store mid-build stops CREATE INDEX CONCURRENTLY on the server and frees the
// build lock. A backend left building would hold the lock, and every process
// starting afterwards would wait in New for it. pgx sends a CancelRequest when a
// context interrupts a statement; this keeps that true.
func TestPostgresConflictingIndex_CloseCancelsTheBuildOnTheServer(t *testing.T) {
	dbURL := startPostgres(t)
	admin := openAdminDB(t, dbURL)

	openConflictingIndexStore(t, dbURL, 60*time.Second)
	awaitValidConflictingIndex(t, admin)

	_, err := admin.Exec(`DROP INDEX px_transactions_conflicting`)
	require.NoError(t, err)

	// An older snapshot the build has to wait for keeps it running.
	holder, err := admin.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	require.NoError(t, err)

	defer func() { _ = holder.Rollback() }()

	var one int
	require.NoError(t, holder.QueryRow(`SELECT 1`).Scan(&one))

	store, err := New(context.Background(), ulogger.TestLogger{}, conflictingIndexSettings(t), dbURL)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		exists, _, _ := postgresConflictingIndex(t, admin)
		return exists
	}, 30*time.Second, 50*time.Millisecond, "the index build never started")

	closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	require.NoError(t, store.Close(closeCtx))

	require.Eventually(t, func() bool {
		var building int
		err := admin.QueryRow(`SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND query LIKE 'CREATE INDEX CONCURRENTLY%'`).Scan(&building)

		return err == nil && building == 0
	}, 10*time.Second, 100*time.Millisecond, "the index build kept running on the server after Close")

	conn, err := admin.Conn(context.Background())
	require.NoError(t, err)

	defer func() { _ = conn.Close() }()

	// The backend takes a moment to notice the dropped connection and exit.
	require.Eventually(t, func() bool {
		var locked bool
		err := conn.QueryRowContext(context.Background(), `SELECT pg_try_advisory_lock($1)`, conflictingIndexLockID).Scan(&locked)

		return err == nil && locked
	}, 10*time.Second, 100*time.Millisecond, "the build lock is still held after Close")

	_, err = conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, conflictingIndexLockID)
	require.NoError(t, err)
}
