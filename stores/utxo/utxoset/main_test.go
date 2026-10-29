package utxoset

import (
	"os"
	"sync"
	"testing"

	tpostgres "github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/util/test"
)

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
var (
	containerOnce    sync.Once
	containerDSN     string
	containerCleanup func() error
	containerErr     error
)

// testDSN returns the DSN a test should connect through.
//
// UTXOSET_TEST_DSN still wins when it is set. That is how a developer points
// the suite at a local instance, and how it is aimed at the database the
// mainnet soak box is already running against. Unset, it starts the shared
// postgres:16-alpine container the rest of the repo's integration tests use.
//
// One database is shared by the whole binary, and newTestStore drops and
// recreates the schema on entry, so two tests running at once would pull each
// other's tables away mid-run. That is safe today only because nothing here
// calls t.Parallel, which is what AGENTS.md asks for unless a test is
// specifically exercising concurrency. Anything that does add t.Parallel to
// this package has to give each test its own database first --
// tpostgres.SetupTestPostgresContainer already returns one per call, so the
// change is to stop sharing through the sync.Once, not to write new plumbing.
//
// Takes testing.TB rather than *testing.T so the throughput benchmarks in
// bench_test.go run against the same instance the tests verify. A benchmark
// that started its own database would be measuring a different machine.
func testDSN(t testing.TB) string {
	t.Helper()

	if v := os.Getenv("UTXOSET_TEST_DSN"); v != "" {
		return v
	}

	containerOnce.Do(func() {
		containerDSN, containerCleanup, containerErr = tpostgres.SetupTestPostgresContainer()
	})

	if containerErr != nil {
		// Skips when Docker is genuinely absent, fails otherwise.
		test.SkipIfContainerUnavailable(t, containerErr)
		t.Fatalf("postgres container unavailable: %v", containerErr)
	}

	return containerDSN
}

// TestMain drops the per-binary database once the suite is done. The container
// behind it is reaped by testcontainers when this process exits.
func TestMain(m *testing.M) {
	code := m.Run()

	if containerCleanup != nil {
		_ = containerCleanup()
	}

	os.Exit(code)
}
