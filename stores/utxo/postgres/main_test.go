package postgres

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	pgtest "github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestMain makes the postgres-backed test suite self-provisioning.
//
// Historically these tests connected to a hardcoded local postgres (testDSN) and
// SKIPPED when it was unreachable. In CI no such postgres exists, so the entire
// suite skipped and contributed ~0 coverage of this store — which is why SonarQube
// reported a low new-code coverage number for the PR.
//
// Now: if a local postgres is reachable we use it (fast inner loop for developers);
// otherwise we spin up a throwaway postgres:16-alpine testcontainer (the same shared
// helper the sql store and the rest of the repo use) and point testDSN at it for the
// duration of the run. If neither is available (e.g. no docker), we still run — the
// individual tests skip themselves on connect failure, preserving prior behaviour.
func TestMain(m *testing.M) {
	if dsnReachable(testDSN) {
		os.Exit(m.Run())
	}

	connStr, cleanup, err := pgtest.SetupTestPostgresContainer()
	if err != nil {
		// No local DB and no container — let the tests skip themselves as before.
		fmt.Printf("[postgres TestMain] no local postgres and could not start a container (%v); tests will skip\n", err)
		os.Exit(m.Run())
	}

	testDSN = connStr
	code := m.Run()
	_ = cleanup()
	os.Exit(code)
}

// dsnReachable reports whether a postgres accepting connections is listening at dsn.
func dsnReachable(dsn string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return false
	}
	defer pool.Close()

	return pool.Ping(ctx) == nil
}
