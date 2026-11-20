package banlist

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// Statements Remove issues, matched exactly. If product text drifts, the armed
// fault never fires and the fired-count assertions fail loudly.
const (
	selectAllBanKeys   = "SELECT key FROM bans"
	deleteBanKey       = "DELETE FROM bans WHERE key = $1"
	sqliteDSNLogFormat = "Using sqlite DB: %s"
	sqlGateTimeout     = 10 * time.Second
)

// injectedSQLFault is synthetic and non-retriable: it models application-level
// accounting/publication failures around real SQLite work, not a physical fault.
var injectedSQLFault = errors.NewStorageError("injected banlist SQL fault")

// dsnCapture records the DSN util.InitSQLiteDB logs, so a second pool can reach
// the store's named shared in-memory database.
type dsnCapture struct {
	mu   sync.Mutex
	dsns []string
}

func (c *dsnCapture) record(dsn string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.dsns = append(c.dsns, dsn)
}

func (c *dsnCapture) only(t *testing.T) string {
	t.Helper()

	c.mu.Lock()
	defer c.mu.Unlock()

	require.Len(t, c.dsns, 1, "expected exactly one %q log from util.InitSQLiteDB; format drift breaks this fixture", sqliteDSNLogFormat)

	return c.dsns[0]
}

// dsnCaptureLogger shares one capture across New/Duplicate, because the store
// derives its SQL logger from the one passed in.
type dsnCaptureLogger struct {
	ulogger.TestLogger
	capture *dsnCapture
}

func (l dsnCaptureLogger) Infof(format string, args ...interface{}) {
	if !strings.HasPrefix(format, "Using sqlite DB") {
		return
	}

	dsn, ok := "", len(args) == 1
	if ok {
		dsn, ok = args[0].(string)
	}

	if format != sqliteDSNLogFormat || !ok {
		dsn = "unexpected sqlite DSN log format: " + format
	}

	l.capture.record(dsn)
}

func (l dsnCaptureLogger) New(string, ...ulogger.Option) ulogger.Logger { return l }

func (l dsnCaptureLogger) Duplicate(...ulogger.Option) ulogger.Logger { return l }

func (l dsnCaptureLogger) WithTraceContext(context.Context) ulogger.Logger { return l }

type sqlPhase string

const (
	phasePrepare          sqlPhase = "prepare"            // fail instead of preparing
	phaseExec             sqlPhase = "exec"               // fail instead of executing
	phaseRowsAffected     sqlPhase = "rows-affected"      // real exec, then RowsAffected fails
	phaseZeroRowsAffected sqlPhase = "zero-rows-affected" // real exec, then RowsAffected reports 0
	phaseRowsNext         sqlPhase = "rows-next"          // real rows exhausted, then Next fails instead of io.EOF
	phaseRowsNextRow      sqlPhase = "rows-next-row"      // real row read, then Next fails instead of returning it
	phaseRowsClose        sqlPhase = "rows-close"         // real close, then Close fails
)

type sqlFaultRule struct {
	phase       sqlPhase
	query       string
	skip, limit int
	err         error
	seen, fired int
}

// sqlInstrument holds armed faults, the rollback gate and actual driver counts.
type sqlInstrument struct {
	mu             sync.Mutex
	rules          []*sqlFaultRule
	rollbackGate   *sqlGate
	statementGates []statementGate
	gates          []*sqlGate
	begins         int
	rollbacks      int
	executions     map[string]int
}

// arm fails `limit` occurrences of query at phase after letting `skip` through.
func (i *sqlInstrument) arm(phase sqlPhase, query string, skip, limit int, err error) *sqlFaultRule {
	i.mu.Lock()
	defer i.mu.Unlock()

	rule := &sqlFaultRule{phase: phase, query: query, skip: skip, limit: limit, err: err}
	i.rules = append(i.rules, rule)

	return rule
}

func (i *sqlInstrument) disarm() {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.rules = nil
	i.rollbackGate = nil
	i.statementGates = nil
}

func (i *sqlInstrument) fire(phase sqlPhase, query string) (bool, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	for _, rule := range i.rules {
		if rule.phase != phase || rule.query != query {
			continue
		}

		rule.seen++
		if rule.seen > rule.skip && rule.seen <= rule.skip+rule.limit {
			rule.fired++
			return true, rule.err
		}
	}

	return false, nil
}

func (i *sqlInstrument) firedCount(rule *sqlFaultRule) int {
	i.mu.Lock()
	defer i.mu.Unlock()

	return rule.fired
}

// armRollbackGate pauses the next actual driver rollback after it completes,
// before RetryTx can retry (the failed callback has already returned).
func (i *sqlInstrument) armRollbackGate() *sqlGate {
	i.mu.Lock()
	defer i.mu.Unlock()

	gate := &sqlGate{entered: make(chan struct{}), release: make(chan struct{})}
	i.rollbackGate = gate
	i.gates = append(i.gates, gate)

	return gate
}

type sqlGatePoint string

const (
	gateBeforeStatement sqlGatePoint = "before-statement" // before the real ExecContext/QueryContext
	gateAfterRowsClose  sqlGatePoint = "after-rows-close" // after the real rows closed, before Close returns
)

type statementGate struct {
	point    sqlGatePoint
	fragment string
	gate     *sqlGate
}

// armGate pauses the first statement containing fragment at point, once.
func (i *sqlInstrument) armGate(point sqlGatePoint, fragment string) *sqlGate {
	i.mu.Lock()
	defer i.mu.Unlock()

	gate := &sqlGate{entered: make(chan struct{}), release: make(chan struct{})}
	i.statementGates = append(i.statementGates, statementGate{point: point, fragment: fragment, gate: gate})
	i.gates = append(i.gates, gate)

	return gate
}

func (i *sqlInstrument) pauseAt(point sqlGatePoint, query string) {
	i.mu.Lock()

	var gate *sqlGate

	for index, candidate := range i.statementGates {
		if candidate.point == point && strings.Contains(query, candidate.fragment) {
			gate = candidate.gate
			i.statementGates = append(i.statementGates[:index], i.statementGates[index+1:]...)

			break
		}
	}

	i.mu.Unlock()

	if gate != nil {
		gate.pause()
	}
}

func (i *sqlInstrument) releaseGates() {
	i.mu.Lock()
	gates := i.gates
	i.mu.Unlock()

	for _, gate := range gates {
		gate.open()
	}
}

func (i *sqlInstrument) resetCounts() {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.begins, i.rollbacks = 0, 0
	i.executions = make(map[string]int)
}

func (i *sqlInstrument) recordBegin() {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.begins++
}

func (i *sqlInstrument) recordExecution(query string) {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.executions[query]++
}

func (i *sqlInstrument) afterRollback() {
	i.mu.Lock()
	i.rollbacks++
	gate := i.rollbackGate
	i.rollbackGate = nil
	i.mu.Unlock()

	if gate != nil {
		gate.pause()
	}
}

func (i *sqlInstrument) beginCount() int {
	i.mu.Lock()
	defer i.mu.Unlock()

	return i.begins
}

func (i *sqlInstrument) rollbackCount() int {
	i.mu.Lock()
	defer i.mu.Unlock()

	return i.rollbacks
}

func (i *sqlInstrument) executionCount(query string) int {
	i.mu.Lock()
	defer i.mu.Unlock()

	return i.executions[query]
}

// sqlGate pauses a driver call. The deadline escape keeps a failed test from
// wedging the driver; cleanup releases every gate.
type sqlGate struct {
	entered     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
}

func (g *sqlGate) pause() {
	close(g.entered)

	select {
	case <-g.release:
	case <-time.After(sqlGateTimeout):
	}
}

func (g *sqlGate) open() { g.releaseOnce.Do(func() { close(g.release) }) }

func (g *sqlGate) awaitEntered(t *testing.T) {
	t.Helper()

	select {
	case <-g.entered:
	case <-time.After(sqlGateTimeout):
		t.Fatal("SQL gate was never reached")
	}
}

// instrumentedDriver delegates to the real sqlite driver.
type instrumentedDriver struct {
	inner      driver.Driver
	instrument *sqlInstrument
}

func (d *instrumentedDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.inner.Open(name)
	if err != nil {
		return nil, err
	}

	return &instrumentedConn{Conn: conn, instrument: d.instrument}, nil
}

// instrumentedConn deliberately withholds Queryer/Execer, so database/sql always
// prepares statements and every statement passes through the wrapper.
type instrumentedConn struct {
	driver.Conn

	instrument *sqlInstrument
}

func (c *instrumentedConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *instrumentedConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if fired, err := c.instrument.fire(phasePrepare, query); fired {
		return nil, err
	}

	var (
		stmt driver.Stmt
		err  error
	)

	if preparer, ok := c.Conn.(driver.ConnPrepareContext); ok {
		stmt, err = preparer.PrepareContext(ctx, query)
	} else {
		stmt, err = c.Conn.Prepare(query)
	}

	if err != nil {
		return nil, err
	}

	return &instrumentedStmt{Stmt: stmt, query: query, instrument: c.instrument}, nil
}

func (c *instrumentedConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	var (
		tx  driver.Tx
		err error
	)

	if beginner, ok := c.Conn.(driver.ConnBeginTx); ok {
		tx, err = beginner.BeginTx(ctx, opts)
	} else {
		tx, err = c.Conn.Begin() //nolint:staticcheck // fallback for drivers without BeginTx
	}

	if err != nil {
		return nil, err
	}

	c.instrument.recordBegin()

	return &instrumentedTx{Tx: tx, instrument: c.instrument}, nil
}

type instrumentedTx struct {
	driver.Tx

	instrument *sqlInstrument
}

func (tx *instrumentedTx) Rollback() error {
	err := tx.Tx.Rollback()
	tx.instrument.afterRollback()

	return err
}

type instrumentedStmt struct {
	driver.Stmt

	query      string
	instrument *sqlInstrument
}

func (s *instrumentedStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	s.instrument.pauseAt(gateBeforeStatement, s.query)

	if fired, err := s.instrument.fire(phaseExec, s.query); fired {
		return nil, err
	}

	execer, ok := s.Stmt.(driver.StmtExecContext)
	if !ok {
		return nil, errors.NewStorageError("wrapped sqlite statement lacks ExecContext")
	}

	result, err := execer.ExecContext(ctx, args)
	if err != nil {
		return nil, err
	}

	s.instrument.recordExecution(s.query)

	if fired, err := s.instrument.fire(phaseRowsAffected, s.query); fired {
		return instrumentedResult{Result: result, rowsAffectedErr: err}, nil
	}

	if fired, _ := s.instrument.fire(phaseZeroRowsAffected, s.query); fired {
		return instrumentedResult{Result: result, reportZero: true}, nil
	}

	return result, nil
}

func (s *instrumentedStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	s.instrument.pauseAt(gateBeforeStatement, s.query)

	queryer, ok := s.Stmt.(driver.StmtQueryContext)
	if !ok {
		return nil, errors.NewStorageError("wrapped sqlite statement lacks QueryContext")
	}

	rows, err := queryer.QueryContext(ctx, args)
	if err != nil {
		return nil, err
	}

	return &instrumentedRows{Rows: rows, query: s.query, instrument: s.instrument}, nil
}

type instrumentedResult struct {
	driver.Result

	rowsAffectedErr error
	reportZero      bool
}

func (r instrumentedResult) RowsAffected() (int64, error) {
	if r.rowsAffectedErr != nil {
		return 0, r.rowsAffectedErr
	}

	if r.reportZero {
		return 0, nil
	}

	return r.Result.RowsAffected()
}

type instrumentedRows struct {
	driver.Rows

	query      string
	instrument *sqlInstrument
}

func (r *instrumentedRows) Next(dest []driver.Value) error {
	err := r.Rows.Next(dest)
	if err == nil {
		if fired, faultErr := r.instrument.fire(phaseRowsNextRow, r.query); fired {
			return faultErr
		}
	}

	if err == io.EOF { // the driver contract returns io.EOF itself
		if fired, faultErr := r.instrument.fire(phaseRowsNext, r.query); fired {
			return faultErr
		}
	}

	return err
}

func (r *instrumentedRows) Close() error {
	if err := r.Rows.Close(); err != nil {
		return err
	}

	r.instrument.pauseAt(gateAfterRowsClose, r.query)

	if fired, err := r.instrument.fire(phaseRowsClose, r.query); fired {
		return err
	}

	return nil
}

type faultFixture struct {
	bl         *BanList
	original   *usql.DB
	pool       *sql.DB
	instrument *sqlInstrument
}

var banListFaultDriverSeq atomic.Int64

// newFaultBanList builds a real in-memory blockchain store, which stays open,
// and a BanList over a second pool reaching the same named shared-cache
// database through the instrumented real driver. Retries are bounded to three
// attempts with a millisecond base delay.
func newFaultBanList(t *testing.T) *faultFixture {
	t.Helper()

	ctx := context.Background()

	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	capture := &dsnCapture{}

	store, err := blockchain.NewStore(dsnCaptureLogger{capture: capture}, storeURL, test.CreateBaseTestSettings(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	dsn := capture.only(t)
	require.True(t, strings.HasPrefix(dsn, "file:") && strings.Contains(dsn, "mode=memory") && strings.Contains(dsn, "cache=shared"),
		"captured DSN %q is not a named shared in-memory database", dsn)

	instrument := &sqlInstrument{executions: make(map[string]int)}
	driverName := fmt.Sprintf("banlist-instrumented-sqlite-%d", banListFaultDriverSeq.Add(1))
	sql.Register(driverName, &instrumentedDriver{inner: store.GetDB().Driver(), instrument: instrument})

	pool, err := sql.Open(driverName, dsn)
	require.NoError(t, err)
	pool.SetMaxOpenConns(5)
	// Registered after the store cleanup, so it runs first.
	t.Cleanup(func() {
		instrument.disarm()
		instrument.releaseGates()

		_ = pool.Close()
	})

	bl := New(usql.WrapDB(pool), util.SqliteMemory, ulogger.TestLogger{})
	bl.db.SetRetryConfig(usql.RetryConfig{MaxAttempts: 2, BaseDelay: time.Millisecond, Enabled: true})
	require.NoError(t, bl.Init(ctx))

	original := store.GetDB()
	requireSharedSentinel(t, bl.db, original, "sentinel-through-instrumented")
	requireSharedSentinel(t, original, bl.db, "sentinel-through-original")
	instrument.resetCounts()

	return &faultFixture{bl: bl, original: original, pool: pool, instrument: instrument}
}

// requireSharedSentinel proves both handles reach one database, so DSN drift
// fails here rather than silently testing an empty database.
func requireSharedSentinel(t *testing.T, writer, reader *usql.DB, key string) {
	t.Helper()

	ctx := context.Background()

	_, err := writer.ExecContext(ctx, "INSERT INTO bans (key, expiration_time, subnet) VALUES ($1, $2, $3)", key, activeExpiration, "")
	require.NoError(t, err)

	var count int

	require.NoError(t, reader.QueryRowContext(ctx, "SELECT COUNT(*) FROM bans WHERE key = $1", key).Scan(&count))
	require.Equal(t, 1, count, "the instrumented and original handles reached different databases")

	_, err = writer.ExecContext(ctx, "DELETE FROM bans WHERE key = $1", key)
	require.NoError(t, err)
}

func execSQL(t *testing.T, db *usql.DB, statements ...string) {
	t.Helper()

	for _, statement := range statements {
		_, err := db.ExecContext(context.Background(), statement)
		require.NoError(t, err, statement)
	}
}

// seedFaultAliases loads two host aliases into memory and SQL, adds one
// SQL-only alias and an unrelated control, and returns the aliases.
func seedFaultAliases(t *testing.T, bl *BanList) []string {
	t.Helper()

	loadSeededBans(t, bl,
		activeBan("192.0.2.7", "192.0.2.7/32"),
		expiredBan("192.0.2.7:8333", "192.0.2.7/32"),
		activeBan("192.0.2.8", "192.0.2.8/32"),
	)
	seedBans(t, bl, activeBan("[::ffff:192.0.2.7]:8334", "192.0.2.7/32"))

	return []string{"192.0.2.7", "192.0.2.7:8333", "[::ffff:192.0.2.7]:8334"}
}

func hostEvents(keys ...string) map[string]string {
	events := make(map[string]string, len(keys))
	for _, key := range keys {
		events[key] = "192.0.2.7/32"
	}

	return events
}

func requireUnpublishedFailure(t *testing.T, bl *BanList, events chan BanEvent, beforeMemory map[string]BanInfo, beforeSQL []string) {
	t.Helper()

	require.Equal(t, beforeMemory, bl.BannedPeers(), "memory must stay unpublished")
	require.ElementsMatch(t, beforeSQL, persistedKeys(t, bl), "definite rollback must retain SQL")
	requireNoEvent(t, events)
}

// TestBanList_RemoveRollback catches partial alias deletion: a real SQLite
// trigger aborts the second delete regardless of alias order, and the whole
// transaction (aliases plus the trigger's own counter) must roll back with
// memory and events unpublished.
func TestBanList_RemoveRollback(t *testing.T) {
	fx := newFaultBanList(t)
	bl := fx.bl
	ctx := context.Background()
	aliases := seedFaultAliases(t, bl)

	execSQL(t, bl.db,
		"CREATE TABLE ban_delete_attempts (n INTEGER NOT NULL)",
		"INSERT INTO ban_delete_attempts (n) VALUES (0)",
		`CREATE TRIGGER abort_second_ban_delete BEFORE DELETE ON bans
		BEGIN
			UPDATE ban_delete_attempts SET n = n + 1;
			SELECT RAISE(ABORT, 'injected failure on second ban delete')
			WHERE (SELECT n FROM ban_delete_attempts) >= 2;
		END`,
	)

	events := bl.Subscribe()
	defer bl.Unsubscribe(events)

	beforeMemory, beforeSQL := bl.BannedPeers(), persistedKeys(t, bl)

	require.Error(t, bl.Remove(ctx, "192.0.2.7"))
	requireUnpublishedFailure(t, bl, events, beforeMemory, beforeSQL)

	var attempts int

	require.NoError(t, bl.db.QueryRowContext(ctx, "SELECT n FROM ban_delete_attempts").Scan(&attempts))
	require.Zero(t, attempts, "transaction rollback must undo the first delete's trigger increment")
	require.Equal(t, 1, fx.instrument.beginCount())
	require.Equal(t, 1, fx.instrument.rollbackCount())

	execSQL(t, bl.db, "DROP TRIGGER abort_second_ban_delete")

	require.NoError(t, bl.Remove(ctx, "192.0.2.7"))
	require.Equal(t, hostEvents(aliases...), collectRemoveEvents(t, events, len(aliases)))
	requireNoEvent(t, events)
	requireBanState(t, bl, []string{"192.0.2.8"}, []string{"192.0.2.8"})

	execSQL(t, bl.db, "DROP TABLE ban_delete_attempts")
}

// TestBanList_RemoveSQLFailures catches publication or silent success after a
// failed read, accounting or transaction step. Faults are synthetic driver
// errors around real SQLite work (plus a real NULL key for Scan), so they test
// application behavior, not physical database failure or PostgreSQL.
func TestBanList_RemoveSQLFailures(t *testing.T) {
	cases := []struct {
		name       string
		phase      sqlPhase
		query      string
		limit      int
		err        error
		nullKey    bool
		canceled   bool
		wantBegins int
	}{
		{name: "query_error", phase: phasePrepare, query: selectAllBanKeys, limit: 1, err: injectedSQLFault, wantBegins: 1},
		{name: "scan_failure_null_key", nullKey: true, wantBegins: 1},
		{name: "row_iteration_error", phase: phaseRowsNext, query: selectAllBanKeys, limit: 1, err: injectedSQLFault, wantBegins: 1},
		{name: "rows_affected_error_after_real_delete", phase: phaseRowsAffected, query: deleteBanKey, limit: 1, err: injectedSQLFault, wantBegins: 1},
		{
			name: "retry_exhausted", phase: phasePrepare, query: selectAllBanKeys, limit: 10,
			err: &pgconn.PgError{Code: usql.PgErrSerializationFail}, wantBegins: 3,
		},
		{name: "canceled_context", canceled: true, wantBegins: 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFaultBanList(t)
			bl := fx.bl
			seedFaultAliases(t, bl)

			events := bl.Subscribe()
			defer bl.Unsubscribe(events)

			beforeMemory, beforeSQL := bl.BannedPeers(), persistedKeys(t, bl)

			if tc.nullKey {
				// SQLite permits NULL in a non-integer TEXT PRIMARY KEY; scanning it
				// into a string fails for real.
				_, err := bl.db.ExecContext(context.Background(),
					"INSERT INTO bans (key, expiration_time, subnet) VALUES (NULL, $1, $2)", activeExpiration, "")
				require.NoError(t, err)
			}

			var rule *sqlFaultRule
			if tc.phase != "" {
				rule = fx.instrument.arm(tc.phase, tc.query, 0, tc.limit, tc.err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), sqlGateTimeout)
			defer cancel()

			if tc.canceled {
				cancel()
			}

			err := bl.Remove(ctx, "192.0.2.7")
			fx.instrument.disarm()

			require.Error(t, err)

			if rule != nil {
				require.Positive(t, fx.instrument.firedCount(rule), "fault never reached the driver")
			}

			if tc.nullKey {
				_, cleanupErr := bl.db.ExecContext(context.Background(), "DELETE FROM bans WHERE key IS NULL")
				require.NoError(t, cleanupErr)
			}

			require.Equal(t, tc.wantBegins, fx.instrument.beginCount(), "begun attempts")
			require.Equal(t, tc.wantBegins, fx.instrument.rollbackCount(), "every begun attempt must roll back")
			requireUnpublishedFailure(t, bl, events, beforeMemory, beforeSQL)
		})
	}

	t.Run("row_close_error", func(t *testing.T) {
		fx := newFaultBanList(t)
		bl := fx.bl
		aliases := seedFaultAliases(t, bl)
		surfaced := closeFaultSurfaces(t, fx)

		events := bl.Subscribe()
		defer bl.Unsubscribe(events)

		beforeMemory, beforeSQL := bl.BannedPeers(), persistedKeys(t, bl)
		rule := fx.instrument.arm(phaseRowsClose, selectAllBanKeys, 0, 1, injectedSQLFault)

		err := bl.Remove(context.Background(), "192.0.2.7")
		fx.instrument.disarm()
		require.Equal(t, 1, fx.instrument.firedCount(rule), "fault never reached the driver")

		if !surfaced {
			// Limitation: database/sql auto-closes rows when Next reaches EOF and
			// discards the driver's close error, so no caller can observe it.
			t.Log("database/sql discarded the driver close error; asserting the successful outcome instead")
			require.NoError(t, err)
			require.Equal(t, hostEvents(aliases...), collectRemoveEvents(t, events, len(aliases)))
			requireBanState(t, bl, []string{"192.0.2.8"}, []string{"192.0.2.8"})

			return
		}

		require.Error(t, err)
		requireUnpublishedFailure(t, bl, events, beforeMemory, beforeSQL)
	})

	t.Run("one_connection_pool", func(t *testing.T) {
		fx := newFaultBanList(t)
		bl := fx.bl
		aliases := seedFaultAliases(t, bl)
		// A pool-based delete inside the transaction, or an unclosed result set,
		// would wait for the only connection until the context expires.
		fx.pool.SetMaxOpenConns(1)

		events := bl.Subscribe()
		defer bl.Unsubscribe(events)

		ctx, cancel := context.WithTimeout(context.Background(), sqlGateTimeout)
		defer cancel()

		require.NoError(t, bl.Remove(ctx, "192.0.2.7"))
		require.Equal(t, hostEvents(aliases...), collectRemoveEvents(t, events, len(aliases)))
		requireBanState(t, bl, []string{"192.0.2.8"}, []string{"192.0.2.8"})
		require.Equal(t, 1, fx.instrument.beginCount())
	})
}

// closeFaultSurfaces probes whether database/sql reports a driver close error
// after full iteration on this toolchain.
func closeFaultSurfaces(t *testing.T, fx *faultFixture) bool {
	t.Helper()

	const probe = "SELECT COUNT(*) FROM bans"

	fx.instrument.arm(phaseRowsClose, probe, 0, 1, injectedSQLFault)
	defer fx.instrument.disarm()

	rows, err := fx.pool.QueryContext(context.Background(), probe)
	require.NoError(t, err)

	for rows.Next() {
	}

	iterationErr := rows.Err()
	closeErr := rows.Close()

	return iterationErr != nil || closeErr != nil
}

// TestBanList_RemoveRetryReselects catches stale publication across RetryTx
// attempts: attempt one really deletes A, then B's delete fails with a raw
// retriable serialization error. After the actual rollback, A is deleted and
// alias C inserted through the original store handle. The final attempt must
// reselect B and C and publish events for those keys only. This exercises retry
// mechanics on SQLite, not PostgreSQL isolation.
func TestBanList_RemoveRetryReselects(t *testing.T) {
	fx := newFaultBanList(t)
	bl := fx.bl

	const aliasA, aliasB, aliasC = "192.0.2.7:8333", "[::ffff:192.0.2.7]:8334", "::ffff:c000:207"

	loadSeededBans(t, bl, activeBan("192.0.2.8", "192.0.2.8/32"))
	seedBans(t, bl, activeBan(aliasA, "192.0.2.7/32"), activeBan(aliasB, "192.0.2.7/32"))

	events := bl.Subscribe()
	defer bl.Unsubscribe(events)

	rule := fx.instrument.arm(phaseExec, deleteBanKey, 1, 1, &pgconn.PgError{Code: usql.PgErrSerializationFail})
	gate := fx.instrument.armRollbackGate()

	ctx, cancel := context.WithTimeout(context.Background(), sqlGateTimeout)
	defer cancel()

	result := make(chan error, 1)

	var worker sync.WaitGroup

	worker.Add(1)

	go func() {
		defer worker.Done()

		result <- bl.Remove(ctx, "192.0.2.7")
	}()

	defer func() {
		gate.open()
		worker.Wait()
	}()

	gate.awaitEntered(t)

	require.Equal(t, 1, fx.instrument.firedCount(rule))
	require.Equal(t, 1, fx.instrument.executionCount(deleteBanKey), "attempt one really deleted one alias")
	require.Equal(t, 1, fx.instrument.rollbackCount())
	require.ElementsMatch(t, []string{"192.0.2.8"}, bl.ListBanned(), "no memory publication before success")
	require.ElementsMatch(t, []string{"192.0.2.8", aliasA, aliasB}, persistedKeys(t, bl), "rollback restored attempt one")
	requireNoEvent(t, events)

	_, err := fx.original.ExecContext(context.Background(), "DELETE FROM bans WHERE key = $1", aliasA)
	require.NoError(t, err)
	_, err = fx.original.ExecContext(context.Background(),
		"INSERT INTO bans (key, expiration_time, subnet) VALUES ($1, $2, $3)", aliasC, activeExpiration, "192.0.2.7/32")
	require.NoError(t, err)

	gate.open()

	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(sqlGateTimeout):
		t.Fatal("Remove did not finish after the retry was released")
	}

	require.Equal(t, 2, fx.instrument.beginCount())
	require.Equal(t, 1, fx.instrument.rollbackCount())
	require.Equal(t, 3, fx.instrument.executionCount(deleteBanKey))
	require.Equal(t, hostEvents(aliasB, aliasC), collectRemoveEvents(t, events, 2))
	requireNoEvent(t, events)
	requireBanState(t, bl, []string{"192.0.2.8"}, []string{"192.0.2.8"})
}

// TestBanList_RemoveRowsAffectedZero separates legitimate zero-row deletes from
// SQL accounting. A memory-only alias affects no rows yet is removed and
// announced once. The synthetic subtest reports zero after a real delete of a
// SQL-only key: SQL stays absent and no SQL-accounting event is emitted. It
// probes result accounting only and does not model delete-suppressing triggers.
func TestBanList_RemoveRowsAffectedZero(t *testing.T) {
	t.Run("memory_only_alias", func(t *testing.T) {
		bl := newTestBanList(t)
		loadSeededBans(t, bl, activeBan("::ffff:192.0.2.7", "192.0.2.7/32"), activeBan("192.0.2.8", "192.0.2.8/32"))
		execSQL(t, bl.db, "DELETE FROM bans WHERE key = '::ffff:192.0.2.7'")

		events := bl.Subscribe()
		defer bl.Unsubscribe(events)

		require.NoError(t, bl.Remove(context.Background(), "192.0.2.7"))
		require.Equal(t, hostEvents("::ffff:192.0.2.7"), collectRemoveEvents(t, events, 1))
		requireNoEvent(t, events)
		requireBanState(t, bl, []string{"192.0.2.8"}, []string{"192.0.2.8"})
	})

	t.Run("synthetic_zero_after_real_delete", func(t *testing.T) {
		fx := newFaultBanList(t)
		bl := fx.bl
		loadSeededBans(t, bl, activeBan("192.0.2.8", "192.0.2.8/32"))
		seedBans(t, bl, activeBan("192.0.2.7:8333", "192.0.2.7/32"))

		events := bl.Subscribe()
		defer bl.Unsubscribe(events)

		rule := fx.instrument.arm(phaseZeroRowsAffected, deleteBanKey, 0, 1, nil)

		require.NoError(t, bl.Remove(context.Background(), "192.0.2.7"))
		fx.instrument.disarm()

		require.Equal(t, 1, fx.instrument.firedCount(rule))
		require.Equal(t, 1, fx.instrument.executionCount(deleteBanKey), "the delete really ran")
		requireBanState(t, bl, []string{"192.0.2.8"}, []string{"192.0.2.8"})
		requireNoEvent(t, events)
	})
}

// TestBanList_RemoveSubscriberBackpressure catches removal or a later Add
// waiting on subscriber delivery: with the subscription channel full and the
// receiver paused, both complete within a bounded context. Every queued send
// is then drained so notification goroutines exit before the leak check.
func TestBanList_RemoveSubscriberBackpressure(t *testing.T) {
	bl := newTestBanList(t)
	loadSeededBans(t, bl, activeBan("192.0.2.7", "192.0.2.7/32"), activeBan("[::ffff:192.0.2.7]:8333", "192.0.2.7/32"))

	events := bl.Subscribe()
	defer bl.Unsubscribe(events)

	for i := 0; i < cap(events); i++ {
		events <- BanEvent{Action: "filler"}
	}

	ctx, cancel := context.WithTimeout(context.Background(), sqlGateTimeout)
	defer cancel()

	done := make(chan error, 1)

	var worker sync.WaitGroup

	worker.Add(1)

	go func() {
		defer worker.Done()

		if err := bl.Remove(ctx, "192.0.2.7"); err != nil {
			done <- err
			return
		}

		done <- bl.Add(ctx, "198.51.100.7", time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC))
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("Remove or Add blocked behind a full subscriber")
	}

	worker.Wait()
	requireBanState(t, bl, []string{"198.51.100.7"}, []string{"198.51.100.7"})

	for i := 0; i < cap(events); i++ {
		select {
		case event := <-events:
			require.Equal(t, "filler", event.Action)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout draining filler events")
		}
	}

	delivered := make(map[string]string, 3)
	for len(delivered) < 3 {
		select {
		case event := <-events:
			delivered[event.IP] = event.Action
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout after %d queued notifications: %v", len(delivered), delivered)
		}
	}

	require.Equal(t, map[string]string{
		"192.0.2.7":               "remove",
		"[::ffff:192.0.2.7]:8333": "remove",
		"198.51.100.7":            "add",
	}, delivered)
	requireNoEvent(t, events)
}
