package collect

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	postgresutils "github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

type fakeClock struct {
	now time.Time
}

func (c *fakeClock) read() time.Time { return c.now }

func (c *fakeClock) advance(d time.Duration) { c.now = c.now.Add(d) }

func skipShort(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping postgres-backed test in -short mode")
	}
}

func newTestDatabase(t *testing.T) (string, *url.URL) {
	t.Helper()
	skipShort(t)

	connStr, cleanup, err := postgresutils.SetupTestPostgresContainer()
	require.NoError(t, err)

	t.Cleanup(func() { require.NoError(t, cleanup()) })

	u, err := url.Parse(connStr)
	require.NoError(t, err)

	return connStr, u
}

func adminConn(t *testing.T, connStr string) *pgx.Conn {
	t.Helper()

	ctx := context.Background()

	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)

	t.Cleanup(func() { _ = conn.Close(ctx) })

	return conn
}

func apply(t *testing.T, patch snapshot.Patch) snapshot.StoreStats {
	t.Helper()
	require.NotNil(t, patch)

	snap := snapshot.Snapshot{}
	patch(&snap)

	return snap.Stores.Postgres
}

func TestCollectSize(t *testing.T) {
	connStr, u := newTestDatabase(t)
	admin := adminConn(t, connStr)
	ctx := context.Background()

	p, err := NewPostgres(u, 5*time.Second, 100)
	require.NoError(t, err)

	defer p.Close()

	patch, err := p.Collect(ctx)
	require.NoError(t, err)

	before := apply(t, patch)
	require.True(t, before.Configured)
	require.Equal(t, snapshot.StatusOK, before.SizeGB.Status)
	require.Greater(t, before.SizeGB.Value, 0.0)
	require.Equal(t, snapshot.StatusOK, before.DiskFill.Status)
	require.InDelta(t, before.SizeGB.Value/100, before.DiskFill.Value, 1e-12)

	_, err = admin.Exec(ctx, "CREATE TABLE blob (id bigserial PRIMARY KEY, payload text)")
	require.NoError(t, err)

	_, err = admin.Exec(ctx, "INSERT INTO blob (payload) SELECT repeat(md5(g::text), 100) FROM generate_series(1, 20000) g")
	require.NoError(t, err)

	var wantBytes int64

	require.NoError(t, admin.QueryRow(ctx, "SELECT pg_database_size(current_database())").Scan(&wantBytes))

	patch, err = p.Collect(ctx)
	require.NoError(t, err)

	after := apply(t, patch)
	require.Greater(t, after.SizeGB.Value, before.SizeGB.Value)
	require.InDelta(t, float64(wantBytes)/1e9, after.SizeGB.Value, 1e-3)
}

func TestCommitRate(t *testing.T) {
	connStr, u := newTestDatabase(t)
	admin := adminConn(t, connStr)
	ctx := context.Background()

	_, err := admin.Exec(ctx, "CREATE TABLE t (id int)")
	require.NoError(t, err)

	clock := &fakeClock{now: time.Unix(1_000_000, 0)}

	p, err := NewPostgres(u, 5*time.Second, 0)
	require.NoError(t, err)

	p.now = clock.read

	defer p.Close()

	time.Sleep(1500 * time.Millisecond)

	patch, err := p.Collect(ctx)
	require.NoError(t, err)

	first := apply(t, patch)
	require.Equal(t, snapshot.StatusNoData, first.CommitsPerSec.Status)
	require.Equal(t, snapshot.StatusNoData, first.ReadsPerSec.Status)
	require.Equal(t, snapshot.StatusNoData, first.WritesPerSec.Status)

	const commits = 200

	for i := 0; i < commits; i++ {
		_, err = admin.Exec(ctx, "INSERT INTO t VALUES ($1)", i)
		require.NoError(t, err)
	}

	require.NoError(t, admin.Close(ctx))
	time.Sleep(500 * time.Millisecond)
	clock.advance(10 * time.Second)

	patch, err = p.Collect(ctx)
	require.NoError(t, err)

	second := apply(t, patch)
	require.Equal(t, snapshot.StatusOK, second.CommitsPerSec.Status)
	require.GreaterOrEqual(t, second.CommitsPerSec.Value*10, float64(commits))
	require.LessOrEqual(t, second.CommitsPerSec.Value*10, float64(commits)+20)

	require.Equal(t, snapshot.StatusOK, second.WritesPerSec.Status)
	require.GreaterOrEqual(t, second.WritesPerSec.Value*10, float64(commits))
}

func TestReadAndWriteRatesAreSplit(t *testing.T) {
	connStr, u := newTestDatabase(t)
	ctx := context.Background()

	admin := adminConn(t, connStr)
	_, err := admin.Exec(ctx, "CREATE TABLE rw (id int)")
	require.NoError(t, err)
	require.NoError(t, admin.Close(ctx))

	clock := &fakeClock{now: time.Unix(1_000_000, 0)}

	p, err := NewPostgres(u, 5*time.Second, 0)
	require.NoError(t, err)

	p.now = clock.read

	defer p.Close()

	time.Sleep(1500 * time.Millisecond)

	patch, err := p.Collect(ctx)
	require.NoError(t, err)

	first := apply(t, patch)
	require.Equal(t, snapshot.StatusNoData, first.ReadsPerSec.Status)
	require.Equal(t, snapshot.StatusNoData, first.WritesPerSec.Status)

	const inserted, updated, deleted = 300, 100, 50

	writer := adminConn(t, connStr)
	_, err = writer.Exec(ctx, "INSERT INTO rw SELECT generate_series(1, $1)", inserted)
	require.NoError(t, err)
	_, err = writer.Exec(ctx, "UPDATE rw SET id = id + 1000 WHERE id <= $1", updated)
	require.NoError(t, err)
	_, err = writer.Exec(ctx, "DELETE FROM rw WHERE id > $1 AND id <= $2", inserted-deleted, inserted)
	require.NoError(t, err)
	require.NoError(t, writer.Close(ctx))

	time.Sleep(1500 * time.Millisecond)
	clock.advance(10 * time.Second)

	patch, err = p.Collect(ctx)
	require.NoError(t, err)

	afterWrites := apply(t, patch)
	require.Equal(t, snapshot.StatusOK, afterWrites.WritesPerSec.Status)
	require.GreaterOrEqual(t, afterWrites.WritesPerSec.Value*10, float64(inserted+updated+deleted))

	reader := adminConn(t, connStr)

	const scans = 20

	for i := 0; i < scans; i++ {
		var n int
		require.NoError(t, reader.QueryRow(ctx, "SELECT count(*) FROM (SELECT id FROM rw) x").Scan(&n))
		require.Equal(t, inserted-deleted, n)
	}

	require.NoError(t, reader.Close(ctx))

	time.Sleep(1500 * time.Millisecond)
	clock.advance(10 * time.Second)

	patch, err = p.Collect(ctx)
	require.NoError(t, err)

	afterReads := apply(t, patch)
	require.Equal(t, snapshot.StatusOK, afterReads.ReadsPerSec.Status)
	require.Equal(t, snapshot.StatusOK, afterReads.WritesPerSec.Status)
	require.GreaterOrEqual(t, afterReads.ReadsPerSec.Value*10, float64(scans*(inserted-deleted)))
	require.Less(t, afterReads.WritesPerSec.Value*10, float64(inserted+updated+deleted)/10)
	require.Greater(t, afterReads.ReadsPerSec.Value, afterReads.WritesPerSec.Value*100)
}

func TestReadOnlyConnection(t *testing.T) {
	connStr, u := newTestDatabase(t)
	admin := adminConn(t, connStr)
	ctx := context.Background()

	_, err := admin.Exec(ctx, "CREATE TABLE guarded (id int)")
	require.NoError(t, err)

	p, err := NewPostgres(u, 5*time.Second, 0)
	require.NoError(t, err)

	defer p.Close()

	conn, err := pgx.ConnectConfig(ctx, p.connConfig())
	require.NoError(t, err)

	defer conn.Close(ctx)

	var mode string

	require.NoError(t, conn.QueryRow(ctx, "SHOW default_transaction_read_only").Scan(&mode))
	require.Equal(t, "on", mode)

	_, err = conn.Exec(ctx, "INSERT INTO guarded VALUES (1)")
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "read-only")

	var rows int

	require.NoError(t, admin.QueryRow(ctx, "SELECT count(*) FROM guarded").Scan(&rows))
	require.Equal(t, 0, rows)
}

func TestReadLatencyNeedsExtensionWhenAbsent(t *testing.T) {
	_, u := newTestDatabase(t)

	p, err := NewPostgres(u, 5*time.Second, 0)
	require.NoError(t, err)

	defer p.Close()

	for i := 0; i < 2; i++ {
		patch, cerr := p.Collect(context.Background())
		require.NoError(t, cerr)

		st := apply(t, patch)
		require.Equal(t, snapshot.StatusNoData, st.ReadP99Sec.Status)
		require.Equal(t, "needs pg_stat_statements", st.ReadP99Sec.Note)
		require.Equal(t, snapshot.StatusNoData, st.DiskFill.Status)
		require.Equal(t, "disk capacity not set", st.DiskFill.Note)
	}
}

func TestReadLatencyIsIntervalMeanWithExtension(t *testing.T) {
	skipShort(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	container, err := tcpostgres.Run(ctx, "docker.io/postgres:16-alpine",
		tcpostgres.WithDatabase("stmts"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("password"),
		testcontainers.WithCmdArgs("-c", "shared_preload_libraries=pg_stat_statements"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(90*time.Second),
			wait.ForListeningPort("5432/tcp"),
		),
	)
	require.NoError(t, err)

	defer func() { _ = container.Terminate(context.Background()) }()

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	admin := adminConn(t, connStr)

	_, err = admin.Exec(ctx, "CREATE EXTENSION pg_stat_statements")
	require.NoError(t, err)

	u, err := url.Parse(connStr)
	require.NoError(t, err)

	clock := &fakeClock{now: time.Unix(2_000_000, 0)}

	p, err := NewPostgres(u, 5*time.Second, 0)
	require.NoError(t, err)

	p.now = clock.read

	defer p.Close()

	patch, err := p.Collect(ctx)
	require.NoError(t, err)

	first := apply(t, patch)
	require.Equal(t, snapshot.StatusNoData, first.ReadP99Sec.Status)

	for i := 0; i < 50; i++ {
		_, err = admin.Exec(ctx, "SELECT pg_sleep(0.02)")
		require.NoError(t, err)
	}

	clock.advance(15 * time.Second)

	patch, err = p.Collect(ctx)
	require.NoError(t, err)

	second := apply(t, patch)
	require.Equal(t, snapshot.StatusOK, second.ReadP99Sec.Status)
	require.Equal(t, "mean", second.ReadP99Sec.Note)
	require.GreaterOrEqual(t, second.ReadP99Sec.Value, 0.01)
	require.Less(t, second.ReadP99Sec.Value, 0.5)
}
