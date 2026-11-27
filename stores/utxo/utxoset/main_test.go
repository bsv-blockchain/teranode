package utxoset

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	tpostgres "github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
)

// externalDSNEnv names the variable that points the suite at an existing PostgreSQL
// database instead of the shared test container.
const externalDSNEnv = "UTXOSET_TEST_DSN"

// The PostgreSQL instance every test in this package runs against, started once
// per test binary.
//
// This used to be a package-level DSN string pointing at localhost:5441 with a
// t.Skipf fallback when nothing answered there. CI has nothing listening on
// that port, so all 66 tests in the package skipped on every run and this store
// was verified nowhere but the author's laptop. The green "test" check meant
// nothing, because a package that skips is a package that passes.
//
// So there is no reachability skip any more. The only skip left is the
// repo-wide test.SkipIfContainerUnavailable, which fires when the container
// runtime itself is missing and fails the test for every other reason -- an
// image that will not pull or a container that will not become ready is a real
// failure, not an absent dependency.

// testDSN returns the DSN a test should connect through.
//
// UTXOSET_TEST_DSN still wins when it is set. That is how a developer points
// the suite at a local instance, and how it is aimed at the database the
// mainnet soak box is already running against. Every call then returns that one
// database, and newTestStore drops and recreates the schema in it, so the tests
// must run one at a time: parallelOnOwnDatabase does not let them go parallel.
//
// Unset, the first call for a test (or subtest) creates a fresh, empty database
// of its own on the shared postgres:16-alpine container the rest of the repo's
// integration tests use (test/utils/postgres), dropped when the test ends. Later
// calls for the same t return that database again, so a second newTestStore in a
// test drops and recreates the schema under the first store, as it did when the
// whole package shared one database. A subtest that opens a second store on its
// parent's database takes the DSN from storeDSN.
//
// Takes testing.TB rather than *testing.T so the throughput benchmarks in
// bench_test.go run against the same instance the tests verify. A benchmark
// that started its own database would be measuring a different machine.
func testDSN(t testing.TB) string {
	t.Helper()

	if v := os.Getenv(externalDSNEnv); v != "" {
		return v
	}

	if dsn, ok := testDSNs.Load(t); ok {
		return dsn.(string)
	}

	dsn, teardown, err := tpostgres.SetupTestPostgresContainer()
	if err != nil {
		// Skips when Docker is genuinely absent, fails otherwise.
		test.SkipIfContainerUnavailable(t, err)
		t.Fatalf("postgres container unavailable: %v", err)
	}

	testDSNs.Store(t, dsn)
	t.Cleanup(func() {
		testDSNs.Delete(t)
		assert.NoError(t, teardown(), "dropping the test database")
	})

	return dsn
}

// testDSNs holds the database testDSN created for each test or subtest still running.
var testDSNs sync.Map // testing.TB -> string

// parallelSlots bounds how many parallel tests run their stores at once. Each test
// opens a store with its own connection pool and batchers, and under -race a few
// hundred megabytes of heap, so the bound keeps the test binary's peak memory and
// the shared server's connection count in check.
var parallelSlots = make(chan struct{}, 4)

// parallelOnOwnDatabase marks the test parallel when it runs on a database of its
// own, that is unless UTXOSET_TEST_DSN points every test at one shared database.
//
// Only a test that touches no package-level state may call it. Tests that set a
// fault or tuning hook (multiChunkFault, nettedBelowChunkTxs and the rest) or read
// a process-wide metric stay sequential: Go runs every sequential top-level test
// before it releases the parallel ones, so a hook a sequential test sets is never
// seen by a parallel store.
func parallelOnOwnDatabase(t *testing.T) {
	t.Helper()

	if os.Getenv(externalDSNEnv) == "" {
		t.Parallel()

		parallelSlots <- struct{}{}
		t.Cleanup(func() { <-parallelSlots })
	}
}

// storeDSNs maps each store the helpers open to the DSN it was opened on.
var storeDSNs sync.Map // *Store -> string

// rememberDSN records the DSN s was opened on, for storeDSN.
func rememberDSN(t testing.TB, s *Store, dsn string) {
	t.Helper()

	storeDSNs.Store(s, dsn)

	held := s
	t.Cleanup(func() {
		storeDSNs.Delete(held)
		held = nil // see closeOnCleanup
	})
}

// closeOnCleanup closes s when the test ends and then lets go of it.
//
// The testing package keeps a finished parallel test, cleanup closures included, until
// every parallel test in its group has finished. A closure that still referenced the
// store would keep each closed store's memory alive to the end of the run: with the
// tests parallel, peak memory went from 0.4 GB to 2.7 GB under -race even one test at
// a time. Clearing the captured variable after Close lets the collector free it.
func closeOnCleanup(t testing.TB, s *Store) {
	t.Helper()

	held := s
	t.Cleanup(func() {
		_ = held.Close(context.Background())
		held = nil
	})
}

// storeDSN returns the DSN a helper opened s on, so a test can open a second store
// on the same database.
func storeDSN(t testing.TB, s *Store) string {
	t.Helper()

	dsn, ok := storeDSNs.Load(s)
	if !ok {
		t.Fatalf("storeDSN: the store was not opened by newTestStoreWith or newSmallPoolStore")
	}

	return dsn.(string)
}

// TestMain stops the shared container once the suite is done. CI turns off the
// testcontainers reaper, which would otherwise remove it when this process exits.
func TestMain(m *testing.M) {
	code := m.Run()

	if err := tpostgres.TerminateSharedContainer(); err != nil {
		fmt.Fprintf(os.Stderr, "terminate shared postgres container: %v\n", err)
	}

	os.Exit(code)
}
