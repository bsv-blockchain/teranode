package sql

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// newSchemaTestDB spins up a fresh postgres container and returns:
//   - dbURL parsed for blockchain.New
//   - a raw *usql.DB for direct probes
//   - the terminate func (registered via t.Cleanup)
func newSchemaTestDB(t *testing.T) (*url.URL, *usql.DB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

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
	require.NoError(t, err)
	t.Cleanup(func() { _ = pgContainer.Terminate(context.Background()) })

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	dbURL, err := url.Parse(connStr)
	require.NoError(t, err)

	db, err := usql.Open("postgres", connStr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	return dbURL, db
}

func TestIsBlockchainSchemaCurrent_EmptyDB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres-backed test in -short mode")
	}

	_, db := newSchemaTestDB(t)

	current, err := isBlockchainSchemaCurrent(db, true)
	require.NoError(t, err)
	assert.False(t, current, "empty DB must not be reported as current")
}

func TestIsBlockchainSchemaCurrent_AfterCreate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres-backed test in -short mode")
	}

	dbURL, db := newSchemaTestDB(t)

	// Creating the store runs createPostgresSchema end-to-end, so after New()
	// returns the probe must report the schema as current.
	tSettings := test.CreateBaseTestSettings(t)
	s, err := New(ulogger.TestLogger{}, dbURL, tSettings)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	current, err := isBlockchainSchemaCurrent(db, true)
	require.NoError(t, err)
	assert.True(t, current, "schema should be current after New() completes")
}

func TestIsBlockchainSchemaCurrent_MissingColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres-backed test in -short mode")
	}

	dbURL, db := newSchemaTestDB(t)

	tSettings := test.CreateBaseTestSettings(t)
	s, err := New(ulogger.TestLogger{}, dbURL, tSettings)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	// Drop one of the expected columns to simulate an out-of-date schema. The
	// probe must then report not-current so the DDL path runs.
	_, err = db.Exec(`ALTER TABLE blocks DROP COLUMN on_main_chain`)
	require.NoError(t, err)

	current, err := isBlockchainSchemaCurrent(db, true)
	require.NoError(t, err)
	assert.False(t, current, "schema should not be current after column drop")
}

func TestIsBlockchainSchemaCurrent_MissingIndex(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres-backed test in -short mode")
	}

	dbURL, db := newSchemaTestDB(t)

	tSettings := test.CreateBaseTestSettings(t)
	s, err := New(ulogger.TestLogger{}, dbURL, tSettings)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	_, err = db.Exec(`DROP INDEX IF EXISTS idx_on_main_chain_height`)
	require.NoError(t, err)

	current, err := isBlockchainSchemaCurrent(db, true)
	require.NoError(t, err)
	assert.False(t, current, "schema should not be current after index drop")
}

func TestIsBlockchainSchemaCurrent_WrongPeerIDType(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres-backed test in -short mode")
	}

	dbURL, db := newSchemaTestDB(t)

	tSettings := test.CreateBaseTestSettings(t)
	s, err := New(ulogger.TestLogger{}, dbURL, tSettings)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	// Flip peer_id back to an older type so the probe must return false.
	// TEXT -> VARCHAR(64) — the ALTER COLUMN peer_id TYPE TEXT in
	// createPostgresSchemaUnlocked is meant to pick this up.
	_, err = db.Exec(`ALTER TABLE blocks ALTER COLUMN peer_id TYPE VARCHAR(64)`)
	require.NoError(t, err)

	current, err := isBlockchainSchemaCurrent(db, true)
	require.NoError(t, err)
	assert.False(t, current, "schema should not be current when peer_id is not TEXT")
}

func TestIsBlockchainSchemaCurrent_MissingPeerIDColumn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres-backed test in -short mode")
	}

	dbURL, db := newSchemaTestDB(t)

	tSettings := test.CreateBaseTestSettings(t)
	s, err := New(ulogger.TestLogger{}, dbURL, tSettings)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	// Drop the peer_id column entirely. Per the function contract, a missing
	// column must surface as (false, nil), not (false, sql.ErrNoRows). Without
	// the EXISTS pattern, QueryRow().Scan() on no rows would propagate as an
	// error and trigger the misleading "probe failed" warning.
	_, err = db.Exec(`ALTER TABLE blocks DROP COLUMN peer_id`)
	require.NoError(t, err)

	current, err := isBlockchainSchemaCurrent(db, true)
	require.NoError(t, err, "missing column must not be reported as a transport error")
	assert.False(t, current, "schema must not be current when peer_id column is missing")
}

func TestCreatePostgresSchema_FastPathOnSecondCall(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres-backed test in -short mode")
	}

	dbURL, db := newSchemaTestDB(t)

	tSettings := test.CreateBaseTestSettings(t)
	s1, err := New(ulogger.TestLogger{}, dbURL, tSettings)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s1.Close() })

	// The second call must detect the schema as current and return without
	// running DDL. The cheapest proof that no DDL ran is that the call is
	// fast and produces no errors against a populated schema. We also check
	// idempotence: object oids remain stable (no DROP/CREATE cycle).
	var oidBefore int64
	require.NoError(t, db.QueryRow(
		`SELECT oid FROM pg_class WHERE relname = 'idx_on_main_chain_height'`,
	).Scan(&oidBefore))

	require.NoError(t, createPostgresSchema(ulogger.TestLogger{}, db, true))

	var oidAfter int64
	require.NoError(t, db.QueryRow(
		`SELECT oid FROM pg_class WHERE relname = 'idx_on_main_chain_height'`,
	).Scan(&oidAfter))

	assert.Equal(t, oidBefore, oidAfter, "fast path must not re-create objects")
}
