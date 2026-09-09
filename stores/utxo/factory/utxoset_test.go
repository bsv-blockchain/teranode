package factory

import (
	"context"
	"net/url"
	"os"
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/utxoset"
	tpostgres "github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// The PostgreSQL this test connects to. It used to default to localhost:5441 and
// skip when nothing answered, which meant it skipped on every CI run: nothing was
// checking that the utxoset scheme is reachable through the factory, which is the
// one thing between the package and a deployable store.
//
// UTXOSET_TEST_DSN still wins when set, for pointing at a local instance;
// otherwise this starts the shared container the rest of the repo uses. The only
// skip left is SkipIfContainerUnavailable, which fires when Docker itself is
// missing and fails for every other reason.
var (
	utxosetDSNOnce    sync.Once
	utxosetDSN        string
	utxosetDSNCleanup func() error
	utxosetDSNErr     error
)

func utxosetTestDSN(t *testing.T) string {
	t.Helper()

	if v := os.Getenv("UTXOSET_TEST_DSN"); v != "" {
		return v
	}

	utxosetDSNOnce.Do(func() {
		utxosetDSN, utxosetDSNCleanup, utxosetDSNErr = tpostgres.SetupTestPostgresContainer()
	})

	if utxosetDSNErr != nil {
		test.SkipIfContainerUnavailable(t, utxosetDSNErr)
		t.Fatalf("postgres container unavailable: %v", utxosetDSNErr)
	}

	return utxosetDSN
}

// TestMain drops the database once the whole binary is done. It cannot be a
// t.Cleanup inside utxosetTestDSN: the sync.Once hands the same DSN to every
// caller, so dropping after the first test would leave the next one pointed at a
// database that no longer exists.
func TestMain(m *testing.M) {
	code := m.Run()

	if utxosetDSNCleanup != nil {
		_ = utxosetDSNCleanup()
	}

	os.Exit(code)
}

// TestUtxosetSchemeIsRegistered proves the delete-on-spend store can actually be
// instantiated by the factory.
//
// Until it is in availableDatabases no settings URL can reach it, so the store cannot be
// deployed, cannot be measured end to end, and every claim about its behaviour is
// theoretical. Registration is the whole difference between a package and a store.
//
// The scheme is deliberately its own rather than a query parameter on "postgres": the two
// stores have incompatible schemas, and a typo in a parameter that silently selected the
// wrong one against a live database is not a failure mode worth having.
func TestUtxosetSchemeIsRegistered(t *testing.T) {
	dbInit, ok := availableDatabases["utxoset"]
	require.True(t, ok, "utxoset scheme must be registered in the factory")

	ctx := context.Background()
	dsn := utxosetTestDSN(t)

	// Prove postgres is reachable using a plain postgres:// URL BEFORE the
	// utxoset:// attempt. Doing it the other way round would swallow the very bug
	// this test exists to catch: pgx does not know the scheme, folds the whole URL
	// into a config parameter and falls back to a unix socket, which surfaces as an
	// unreachable server rather than as the scheme error it is.
	probe, err := url.Parse(dsn)
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, probe.String())
	require.NoError(t, err, "opening the probe pool")
	require.NoError(t, pool.Ping(ctx), "reaching the test postgres")
	pool.Close()

	u, err := url.Parse(dsn)
	require.NoError(t, err)

	u.Scheme = "utxoset" // exactly what a settings URL would carry

	store, err := dbInit(ctx, ulogger.TestLogger{}, settings.NewSettings(), u)
	require.NoError(t, err, "the factory must connect from a utxoset:// URL")

	defer func() { _ = store.Close(ctx) }()

	require.IsType(t, &utxoset.Store{}, store)
	require.True(t, store.SupportsOutpointOnlySpend(),
		"the below-checkpoint fast path is the reason this store exists")
}
