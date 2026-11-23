package sql

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// startLanePositionPostgres starts a Postgres container and returns its URL.
func startLanePositionPostgres(t *testing.T) *url.URL {
	t.Helper()

	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:13",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Minute),
		),
	)
	test.SkipIfContainerUnavailable(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pgContainer.Terminate(ctx))
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	dbURL, err := url.Parse(connStr)
	require.NoError(t, err)

	return dbURL
}

func lanePositionSettings(t *testing.T) *settings.Settings {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 30 * time.Second
	tSettings.BatcherDrainMode = true
	tSettings.Pruner.UTXODefensiveEnabled = false

	return tSettings
}

// sqlLaneRules: a create the SQL store refuses as ErrTxExists, with a block or without, wrote nothing
// unless an earlier attempt of it may have committed (see markRefusedCreate and
// TestLanePositionNotReturnedAfterAConflictLinkLockError), and a list goes through SpendAndCreate.
var sqlLaneRules = tests.LaneReturnRules{Exists: true, MinedExists: true, ListExists: true}

// sqlLaneRows counts the transaction rows carrying any of positions, the one column the store
// writes a position to.
func sqlLaneRows(s *Store) tests.LanePositionCounter {
	return func(t testing.TB, positions []uint64) int {
		t.Helper()

		total := 0

		for _, p := range positions {
			var n int
			require.NoError(t, s.db.QueryRowContext(context.Background(),
				`SELECT count(*) FROM transactions WHERE lane_position = $1`, lane.ToSigned(p)).Scan(&n))

			total += n
		}

		return total
	}
}

func TestLanePositionContractSQLite(t *testing.T) {
	db, _ := setup(context.Background(), t)

	tests.LanePositionContract(t, db)
	t.Run("burns", func(t *testing.T) { tests.LanePositionBurns(t, db, "sql") })
	t.Run("returns", func(t *testing.T) { tests.LanePositionReturns(t, db, "sql", sqlLaneRules, sqlLaneRows(db)) })
}

// Every Postgres create path writes the lane position: the create batcher's
// pipelined CTEs, a single CTE, and the plain per-row INSERT.
func TestLanePositionContractPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	variants := []struct {
		name   string
		tweaks func(*settings.Settings)
	}{
		{"create batcher", func(*settings.Settings) {}},
		{"single CTE", func(s *settings.Settings) { s.UtxoStore.StoreBatcherSize = 1 }},
		{"plain insert", func(s *settings.Settings) {
			s.UtxoStore.StoreBatcherSize = 1
			s.UtxoStore.BatchSQLOperations = false
		}},
	}

	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			dbURL := startLanePositionPostgres(t)

			tSettings := lanePositionSettings(t)
			v.tweaks(tSettings)

			db, err := New(context.Background(), ulogger.TestLogger{}, tSettings, dbURL)
			require.NoError(t, err)

			tests.LanePositionContract(t, db)
			t.Run("burns", func(t *testing.T) { tests.LanePositionBurns(t, db, "sql") })
			t.Run("returns", func(t *testing.T) { tests.LanePositionReturns(t, db, "sql", sqlLaneRules, sqlLaneRows(db)) })
		})
	}
}

// BatchDecorate returns the lane position when the caller names it, and a Get
// that does not name it leaves the field zero.
func TestLanePositionBatchDecorateAndUnnamedField(t *testing.T) {
	ctx := context.Background()
	db, _ := setup(ctx, t)

	w := tests.BuildMultiWorkload(t, 0xe0, 1, 1)
	w.StoreRoots(t, db, 10)

	want := lane.Pack(200, 12345)

	_, _, err := db.SpendAndCreate(ctx, w.Txs[0], 11, utxostore.WithLanePosition(want))
	require.NoError(t, err)

	hash := w.Txs[0].TxIDChainHash()

	items := []*utxostore.UnresolvedMetaData{{Hash: *hash, Idx: 0}}
	require.NoError(t, db.BatchDecorate(ctx, items, fields.LanePosition))
	require.NoError(t, items[0].Err)
	require.Equal(t, want, items[0].Data.LanePosition)

	md, err := db.Get(ctx, hash, fields.Fee)
	require.NoError(t, err)
	require.Equal(t, lane.None, md.LanePosition, "not read unless named")
}

const oldSQLiteTransactionsTable = `
CREATE TABLE transactions (
	 id               INTEGER PRIMARY KEY AUTOINCREMENT
	,hash             BLOB NOT NULL
	,version          BIGINT NOT NULL
	,lock_time        BIGINT NOT NULL
	,fee              BIGINT NOT NULL
	,size_in_bytes    BIGINT NOT NULL
	,coinbase         BOOLEAN DEFAULT FALSE NOT NULL
	,frozen           BOOLEAN DEFAULT FALSE NOT NULL
	,conflicting      BOOLEAN DEFAULT FALSE NOT NULL
	,locked           BOOLEAN DEFAULT FALSE NOT NULL
	,delete_at_height BIGINT
	,unmined_since    BIGINT
	,preserve_until   BIGINT
	,inserted_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
)`

const oldPostgresTransactionsTable = `
CREATE TABLE transactions (
	 id               BIGSERIAL PRIMARY KEY
	,hash             BYTEA NOT NULL
	,version          BIGINT NOT NULL
	,lock_time        BIGINT NOT NULL
	,fee              BIGINT NOT NULL
	,size_in_bytes    BIGINT NOT NULL
	,coinbase         BOOLEAN DEFAULT FALSE NOT NULL
	,frozen           BOOLEAN DEFAULT FALSE NOT NULL
	,conflicting      BOOLEAN DEFAULT FALSE NOT NULL
	,locked           BOOLEAN DEFAULT FALSE NOT NULL
	,delete_at_height BIGINT
	,unmined_since    BIGINT
	,preserve_until   BIGINT
	,inserted_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
)`

// requireOldRowReadsNoLanePosition checks a store opened on a database whose
// transactions table predates lane_position: the old row reads lane.None and
// stays NULL (no backfill), and a new record keeps its lane position.
func requireOldRowReadsNoLanePosition(t *testing.T, inspect *sql.DB, store *Store, oldHash chainhash.Hash) {
	t.Helper()

	ctx := context.Background()

	md, err := store.Get(ctx, &oldHash, fields.LanePosition)
	require.NoError(t, err)
	require.Equal(t, lane.None, md.LanePosition)

	var nulls int
	require.NoError(t, inspect.QueryRow(`SELECT count(*) FROM transactions WHERE lane_position IS NULL`).Scan(&nulls))
	require.Equal(t, 1, nulls, "the old row is not backfilled")

	w := tests.BuildMultiWorkload(t, 0xe1, 1, 1)
	w.StoreRoots(t, store, 10)

	want := lane.Pack(255, 77)

	_, _, err = store.SpendAndCreate(ctx, w.Txs[0], 11, utxostore.WithLanePosition(want))
	require.NoError(t, err)

	md, err = store.Get(ctx, w.Txs[0].TxIDChainHash(), fields.LanePosition)
	require.NoError(t, err)
	require.Equal(t, want, md.LanePosition)

	// The other lane columns arrived with it, empty: the spend above wrote its spending data and
	// no spender position, and no child got parent positions. Running the explicit schema
	// preparation again changes nothing.
	for _, c := range sqlLaneColumnsWant[1:] {
		var n int
		require.NoError(t, inspect.QueryRow(`SELECT count(*) FROM `+c[0]+` WHERE `+c[1]+` IS NOT NULL`).Scan(&n), "%s.%s exists", c[0], c[1])
		require.Zero(t, n, "%s.%s holds nothing", c[0], c[1])
	}

	var spent int
	require.NoError(t, inspect.QueryRow(`SELECT count(*) FROM outputs WHERE spending_data IS NOT NULL`).Scan(&spent))
	require.Positive(t, spent)

	preparer, ok := utxostore.As[utxostore.LaneSchemaPreparer](store)
	require.True(t, ok)
	require.NoError(t, preparer.PrepareLaneSchema(ctx))
}

func TestLanePositionMigrationSQLite(t *testing.T) {
	tSettings := lanePositionSettings(t)
	tSettings.DataFolder = t.TempDir()

	dbURL, err := url.Parse("sqlite:///lane_old_schema")
	require.NoError(t, err)

	// Build a database as an earlier release left it: no lane_position column.
	oldDB, err := sql.Open("sqlite", filepath.Join(tSettings.DataFolder, "lane_old_schema.db"))
	require.NoError(t, err)

	_, err = oldDB.Exec(oldSQLiteTransactionsTable)
	require.NoError(t, err)

	oldHash := chainhash.HashH([]byte("old row"))
	_, err = oldDB.Exec(`INSERT INTO transactions (hash, version, lock_time, fee, size_in_bytes) VALUES (?, 1, 0, 5, 100)`, oldHash[:])
	require.NoError(t, err)

	store, err := New(context.Background(), ulogger.TestLogger{}, tSettings, dbURL)
	require.NoError(t, err)

	requireOldRowReadsNoLanePosition(t, oldDB, store, oldHash)
	require.NoError(t, oldDB.Close())

	// Opening again finds the column already there.
	_, err = New(context.Background(), ulogger.TestLogger{}, tSettings, dbURL)
	require.NoError(t, err)
}

func TestLanePositionMigrationPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	dbURL := startLanePositionPostgres(t)

	oldDB, err := sql.Open("pgx", dbURL.String())
	require.NoError(t, err)

	_, err = oldDB.Exec(oldPostgresTransactionsTable)
	require.NoError(t, err)

	oldHash := chainhash.HashH([]byte("old row"))
	_, err = oldDB.Exec(`INSERT INTO transactions (hash, version, lock_time, fee, size_in_bytes) VALUES ($1, 1, 0, 5, 100)`, oldHash[:])
	require.NoError(t, err)

	store, err := New(context.Background(), ulogger.TestLogger{}, lanePositionSettings(t), dbURL)
	require.NoError(t, err)

	requireOldRowReadsNoLanePosition(t, oldDB, store, oldHash)
	require.NoError(t, oldDB.Close())
}

// On a database from before lane_position, an open transaction on transactions
// keeps the ADD COLUMN from getting its lock. The store retries within
// laneColumnMigrationLimit and then refuses to open, rather than waiting with
// no deadline while every query on transactions queues behind it; a reader on
// another connection is never held for long. Once the transaction ends, the
// next attempt adds the column.
func TestLanePositionMigrationPostgresBoundsTheLockWait(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	dbURL := startLanePositionPostgres(t)

	oldDB, err := sql.Open("pgx", dbURL.String())
	require.NoError(t, err)

	defer func() { require.NoError(t, oldDB.Close()) }()

	_, err = oldDB.Exec(oldPostgresTransactionsTable)
	require.NoError(t, err)

	saved := laneColumnMigrationLimit
	t.Cleanup(func() { laneColumnMigrationLimit = saved })

	holder, err := oldDB.Begin()
	require.NoError(t, err)

	_, err = holder.Exec(`SELECT count(*) FROM transactions`)
	require.NoError(t, err)

	// A reader on its own connection, polling while the store retries. Each
	// read may queue behind one ALTER attempt, so at most about the 200ms
	// lock_timeout.
	readerDone := make(chan time.Duration)

	go func() {
		var worst time.Duration

		for end := time.Now().Add(1500 * time.Millisecond); time.Now().Before(end); {
			started := time.Now()

			var n int
			if err := oldDB.QueryRow(`SELECT count(*) FROM transactions`).Scan(&n); err != nil {
				worst = time.Hour
				break
			}

			worst = max(worst, time.Since(started))
			time.Sleep(20 * time.Millisecond)
		}
		readerDone <- worst
	}()

	laneColumnMigrationLimit = time.Second
	started := time.Now()

	_, err = New(context.Background(), ulogger.TestLogger{}, lanePositionSettings(t), dbURL)
	require.Error(t, err)
	require.Contains(t, err.Error(), "could not add lane_position column")
	require.Less(t, time.Since(started), 30*time.Second)
	require.Less(t, <-readerDone, time.Second)

	// The holder ends while the store is retrying: the column goes in.
	laneColumnMigrationLimit = time.Minute

	go func() {
		time.Sleep(500 * time.Millisecond)
		_ = holder.Rollback()
	}()

	store, err := New(context.Background(), ulogger.TestLogger{}, lanePositionSettings(t), dbURL)
	require.NoError(t, err)

	var columns int
	require.NoError(t, oldDB.QueryRow(`SELECT count(*) FROM information_schema.columns WHERE table_name = 'transactions' AND column_name = 'lane_position'`).Scan(&columns))
	require.Equal(t, 1, columns)
	require.NotNil(t, store)
}

// A conflicting create on the single-CTE path commits its record, then writes the parent links in a
// transaction of its own. When that second transaction dies on a lock error, Create retries, and the
// retry meets the record the first attempt committed. That ErrTxExists is this create's own record,
// which carries the position, so the position must not go back for reuse. A serialization failure
// is recognised only through the wrapped PgError, since its message names no lock.
func TestLanePositionNotReturnedAfterAConflictLinkLockError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	dbURL := startLanePositionPostgres(t)

	tSettings := lanePositionSettings(t)
	tSettings.UtxoStore.StoreBatcherSize = 1

	db, err := New(context.Background(), ulogger.TestLogger{}, tSettings, dbURL)
	require.NoError(t, err)
	require.Nil(t, db.createBatcher, "the test needs the single-CTE path")
	require.True(t, db.settings.UtxoStore.BatchSQLOperations, "the test needs the single-CTE path")

	for i, c := range []struct {
		name string
		err  *pgconn.PgError
	}{
		{"a deadlock", &pgconn.PgError{Code: usql.PgErrDeadlockDetected, Message: "deadlock detected"}},
		{"a serialization failure", &pgconn.PgError{Code: usql.PgErrSerializationFail, Message: "could not serialize access due to concurrent update"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			faults := 0
			conflictingChildrenFault = func() error {
				faults++
				return c.err
			}

			t.Cleanup(func() { conflictingChildrenFault = nil })

			first := lane.Pack(46, uint64(2*i+1)) //nolint:gosec // i is 0 or 1
			source := tests.NewReturningLanePositions(first, first+1)
			db.SetLanePositionSource(source)
			t.Cleanup(func() { db.SetLanePositionSource(nil) })

			tx := tests.BuildMultiWorkload(t, byte(0xab+i), 1, 1).Roots[0] //nolint:gosec // i is 0 or 1

			_, err := db.Create(context.Background(), tx, 960, utxostore.WithConflicting(true))
			require.ErrorIs(t, err, errors.ErrTxExists, "the retry meets the record the first attempt committed")
			require.Equal(t, 1, faults, "only the first attempt reached the conflict links")

			require.Empty(t, source.Returned(), "the record carries the position")

			md, err := db.Get(context.Background(), tx.TxIDChainHash(), fields.LanePosition)
			require.NoError(t, err)
			require.Equal(t, first, md.LanePosition)
		})
	}
}

// markRefusedCreate marks only ErrTxExists, and only when no earlier attempt may have committed.
func TestMarkRefusedCreate(t *testing.T) {
	require.True(t, utxostore.LanePositionUnwritten(markRefusedCreate(errors.NewTxExistsError("exists"), false)))
	require.False(t, utxostore.LanePositionUnwritten(markRefusedCreate(errors.NewTxExistsError("exists"), true)),
		"an earlier attempt may have written the record")
	require.False(t, utxostore.LanePositionUnwritten(markRefusedCreate(errors.NewStorageError("disk"), false)),
		"only a refusal proves the position unwritten")
	require.False(t, utxostore.LanePositionUnwritten(markRefusedCreate(errors.NewUtxoError("refused"), false)))
	require.NoError(t, markRefusedCreate(nil, false))
}

// The conflicting create on the single-CTE path writes its parents' conflict links after the record
// commits, the write TestLanePositionNotReturnedAfterAConflictLinkLockError replaces with a fault.
// Without it the parent has no link to the child.
func TestConflictingCreateOnTheSingleCTEPathLinksItsParent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	dbURL := startLanePositionPostgres(t)

	tSettings := lanePositionSettings(t)
	tSettings.UtxoStore.StoreBatcherSize = 1

	db, err := New(context.Background(), ulogger.TestLogger{}, tSettings, dbURL)
	require.NoError(t, err)
	require.Nil(t, db.createBatcher, "the test needs the single-CTE path")
	require.True(t, db.settings.UtxoStore.BatchSQLOperations, "the test needs the single-CTE path")

	w := tests.BuildMultiWorkload(t, 0xad, 1, 1)
	w.StoreRoots(t, db, 960)

	child := w.Txs[0]

	_, err = db.Create(context.Background(), child, 961, utxostore.WithConflicting(true))
	require.NoError(t, err)

	var links int
	require.NoError(t, db.db.QueryRowContext(context.Background(), `
		SELECT count(*) FROM conflicting_children c
		  JOIN transactions p ON p.id = c.transaction_id
		  JOIN transactions k ON k.id = c.child_transaction_id
		 WHERE p.hash = $1 AND k.hash = $2`, w.Roots[0].TxIDChainHash()[:], child.TxIDChainHash()[:]).Scan(&links))
	require.Equal(t, 1, links, "the parent links to its conflicting child")
}

// Once the lane columns exist, opening the store asks for no ACCESS EXCLUSIVE lock on outputs:
// the catalog check skips the ALTER of spending_lane_position. A transaction that has only read
// outputs holds ACCESS SHARE, which conflicts with ACCESS EXCLUSIVE alone. Without the check, the
// ALTER ... IF NOT EXISTS would still queue for ACCESS EXCLUSIVE before finding the column, time
// out after 200ms each try, and the open would fail at the one-second limit. (transactions is not
// held here: an older migration's ALTER of conflict_owner takes it at every open, with no timeout.)
func TestLaneColumnsOnACurrentSchemaTakeNoOutputsLock(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	dbURL := startLanePositionPostgres(t)

	_, err := New(context.Background(), ulogger.TestLogger{}, lanePositionSettings(t), dbURL)
	require.NoError(t, err)

	reader, err := sql.Open("pgx", dbURL.String())
	require.NoError(t, err)

	defer func() { require.NoError(t, reader.Close()) }()

	holder, err := reader.Begin()
	require.NoError(t, err)

	defer func() { _ = holder.Rollback() }()

	_, err = holder.Exec(`SELECT count(*) FROM outputs`)
	require.NoError(t, err)

	saved := laneColumnMigrationLimit
	t.Cleanup(func() { laneColumnMigrationLimit = saved })

	laneColumnMigrationLimit = time.Second

	store, err := New(context.Background(), ulogger.TestLogger{}, lanePositionSettings(t), dbURL)
	require.NoError(t, err)
	require.NoError(t, store.PrepareLaneSchema(context.Background()))
}

// sqlLaneColumnsWant is every lane column of the SQL store, written out here rather than read
// from sqlLaneColumns so a column dropped from that list fails these tests.
// Each entry is table, column, the Postgres data_type and the SQLite declared type: a lane
// position is a signed 64-bit integer and a parent list is eight bytes per parent.
var sqlLaneColumnsWant = [][4]string{
	{"transactions", "lane_position", "bigint", "BIGINT"},
	{"transactions", "parent_lane_positions", "bytea", "BLOB"},
	{"outputs", "spending_lane_position", "bigint", "BIGINT"},
}

// laneColumnState reports whether table has column and, if so, whether it is nullable with no
// default, and its type as the engine names it.
func laneColumnState(t *testing.T, store *Store, table, column string) (exists, nullableNoDefault bool, typ string) {
	t.Helper()

	db := store.RawDB()

	if store.engine == string(util.Postgres) {
		var nullable string

		var dflt sql.NullString

		err := db.QueryRow(`SELECT is_nullable, column_default, data_type FROM information_schema.columns
		                     WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2`, table, column).Scan(&nullable, &dflt, &typ)
		if err == sql.ErrNoRows {
			return false, false, ""
		}

		require.NoError(t, err)

		return true, nullable == "YES" && !dflt.Valid, typ
	}

	var notNull int

	var dflt sql.NullString

	err := db.QueryRow(`SELECT "notnull", dflt_value, type FROM pragma_table_info('`+table+`') WHERE name = $1`, column).Scan(&notNull, &dflt, &typ)
	if err == sql.ErrNoRows {
		return false, false, ""
	}

	require.NoError(t, err)

	return true, notNull == 0 && !dflt.Valid, typ
}

// laneRowsSnapshot is what PrepareLaneSchema must not change: every transaction's lane position by
// hash, and every output's spending data by transaction hash and index.
func laneRowsSnapshot(t *testing.T, store *Store) (positions, spends map[string]string) {
	t.Helper()

	positions = map[string]string{}
	spends = map[string]string{}

	rows, err := store.RawDB().Query(`SELECT hash, lane_position FROM transactions`)
	require.NoError(t, err)

	for rows.Next() {
		var (
			hash []byte
			pos  sql.NullInt64
		)

		require.NoError(t, rows.Scan(&hash, &pos))

		positions[hex.EncodeToString(hash)] = fmt.Sprint(pos)
	}

	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	rows, err = store.RawDB().Query(`SELECT t.hash, o.idx, o.spending_data FROM outputs o JOIN transactions t ON t.id = o.transaction_id`)
	require.NoError(t, err)

	for rows.Next() {
		var (
			hash, data []byte
			idx        int64
		)

		require.NoError(t, rows.Scan(&hash, &idx, &data))

		spends[fmt.Sprintf("%x:%d", hash, idx)] = fmt.Sprintf("%x/%t", data, data != nil)
	}

	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	return positions, spends
}

// requirePrepareLaneSchemaRestoresColumns stores records with a lane position and a spend, drops
// the two lane columns nothing in this build writes, then has PrepareLaneSchema put them back,
// nullable, with no default, with the right type, and with no value written to any row.
func requirePrepareLaneSchemaRestoresColumns(t *testing.T, store *Store) {
	t.Helper()

	ctx := context.Background()

	w := tests.BuildMultiWorkload(t, 0xe2, 1, 1)
	w.StoreRoots(t, store, 10)

	_, _, err := store.SpendAndCreate(ctx, w.Txs[0], 11, utxostore.WithLanePosition(lane.Pack(9, 5)))
	require.NoError(t, err)

	positionsBefore, spendsBefore := laneRowsSnapshot(t, store)
	require.Greater(t, len(positionsBefore), 1, "transactions are stored")
	require.Contains(t, positionsBefore, hex.EncodeToString(w.Txs[0].TxIDChainHash()[:]))
	require.Equal(t, fmt.Sprint(sql.NullInt64{Int64: int64(lane.Pack(9, 5)), Valid: true}), positionsBefore[hex.EncodeToString(w.Txs[0].TxIDChainHash()[:])], "a transaction has a lane position")

	spent := 0

	for _, v := range spendsBefore {
		if strings.HasSuffix(v, "/true") {
			spent++
		}
	}

	require.Positive(t, spent, "an output is spent")

	for _, c := range sqlLaneColumnsWant[1:] {
		_, err := store.RawDB().Exec(`ALTER TABLE ` + c[0] + ` DROP COLUMN ` + c[1])
		require.NoError(t, err, "%s.%s", c[0], c[1])

		exists, _, _ := laneColumnState(t, store, c[0], c[1])
		require.False(t, exists, "%s.%s dropped", c[0], c[1])
	}

	require.NoError(t, store.PrepareLaneSchema(ctx))
	require.NoError(t, store.PrepareLaneSchema(ctx), "a second run is a no-op")

	wantType := 2
	if store.engine != string(util.Postgres) {
		wantType = 3
	}

	for _, c := range sqlLaneColumnsWant {
		exists, nullable, typ := laneColumnState(t, store, c[0], c[1])
		require.True(t, exists, "%s.%s", c[0], c[1])
		require.True(t, nullable, "%s.%s is nullable with no default", c[0], c[1])
		require.Equal(t, c[wantType], typ, "%s.%s type", c[0], c[1])
	}

	for _, c := range sqlLaneColumnsWant[1:] {
		var n int
		require.NoError(t, store.RawDB().QueryRow(`SELECT count(*) FROM `+c[0]+` WHERE `+c[1]+` IS NOT NULL`).Scan(&n))
		require.Zero(t, n, "%s.%s holds nothing", c[0], c[1])
	}

	positionsAfter, spendsAfter := laneRowsSnapshot(t, store)
	require.Equal(t, positionsBefore, positionsAfter, "every transaction keeps its lane position")
	require.Equal(t, spendsBefore, spendsAfter, "every output keeps its spending data")
}

func TestPrepareLaneSchemaRestoresColumnsSQLite(t *testing.T) {
	store, _ := setup(context.Background(), t)

	requirePrepareLaneSchemaRestoresColumns(t, store)
}

func TestPrepareLaneSchemaRestoresColumnsPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, err := New(context.Background(), ulogger.TestLogger{}, lanePositionSettings(t), startLanePositionPostgres(t))
	require.NoError(t, err)

	requirePrepareLaneSchemaRestoresColumns(t, store)
}

// holdTable opens a transaction that reads table, holding a lock that ADD COLUMN must wait for,
// and returns its release.
func holdTable(t *testing.T, store *Store, table string) func() {
	t.Helper()

	holder, err := store.RawDB().Begin()
	require.NoError(t, err)

	var n int64
	require.NoError(t, holder.QueryRow(`SELECT count(*) FROM `+table).Scan(&n))

	release := func() { _ = holder.Rollback() }
	t.Cleanup(release)

	return release
}

// PrepareLaneSchema on Postgres: on a current schema the catalog check means no lock is asked
// for, so a held table does not delay it; with a column missing it waits out a short hold within
// its limit; and it stops when the caller's context ends instead of retrying to the limit.
func TestPrepareLaneSchemaLocksPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	ctx := context.Background()

	store, err := New(ctx, ulogger.TestLogger{}, lanePositionSettings(t), startLanePositionPostgres(t))
	require.NoError(t, err)

	saved := laneColumnMigrationLimit
	t.Cleanup(func() { laneColumnMigrationLimit = saved })

	t.Run("current schema takes no lock", func(t *testing.T) {
		release := holdTable(t, store, "outputs")
		defer release()

		laneColumnMigrationLimit = time.Second
		require.NoError(t, store.PrepareLaneSchema(ctx))
	})

	t.Run("waits out a short hold", func(t *testing.T) {
		_, err := store.RawDB().Exec(`ALTER TABLE outputs DROP COLUMN spending_lane_position`)
		require.NoError(t, err)

		release := holdTable(t, store, "outputs")

		go func() {
			time.Sleep(time.Second)
			release()
		}()

		laneColumnMigrationLimit = time.Minute
		require.NoError(t, store.PrepareLaneSchema(ctx))

		exists, _, _ := laneColumnState(t, store, "outputs", "spending_lane_position")
		require.True(t, exists)
	})

	t.Run("stops when the context ends", func(t *testing.T) {
		_, err := store.RawDB().Exec(`ALTER TABLE outputs DROP COLUMN spending_lane_position`)
		require.NoError(t, err)

		release := holdTable(t, store, "outputs")
		defer release()

		laneColumnMigrationLimit = 30 * time.Second

		cctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()

		started := time.Now()
		require.Error(t, store.PrepareLaneSchema(cctx))
		require.Less(t, time.Since(started), 10*time.Second, "it gave up when the context ended, not at the limit")
	})
}

// lockedLaneExecutor stands in for Postgres while another transaction holds a table: every lane
// column statement fails with lock_not_available until its table is released at the time given.
type lockedLaneExecutor struct {
	freeAt map[string]time.Time
}

func (e lockedLaneExecutor) Exec(query string, _ ...interface{}) (sql.Result, error) {
	for column, at := range e.freeAt {
		if strings.Contains(query, "attname = '"+column+"'") && time.Now().Before(at) {
			return nil, &pgconn.PgError{Code: usql.PgErrLockNotAvailable}
		}
	}

	return nil, nil
}

func (e lockedLaneExecutor) Close() error { return nil }

// The lane columns share one deadline. Here lane_position is free after 1.5s of a 2s limit and
// parent_lane_positions never is: with one deadline the pass gives up a little after 2s; a fresh
// limit per column would keep the schema lock for 3.5s or more.
func TestAddLanePositionColumnSharesOneDeadline(t *testing.T) {
	started := time.Now()

	err := addLanePositionColumn(lockedLaneExecutor{freeAt: map[string]time.Time{
		"lane_position":         started.Add(1500 * time.Millisecond),
		"parent_lane_positions": started.Add(time.Hour),
	}}, 2*time.Second)

	require.Error(t, err)
	require.Contains(t, err.Error(), "parent_lane_positions")
	require.Less(t, time.Since(started), 3500*time.Millisecond)
}
