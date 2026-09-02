package test

import (
	"context"
	"net/url"
	"os"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/testcontainers/testcontainers-go"
)

type TestingT interface {
	Errorf(format string, args ...interface{})
	Logf(format string, args ...interface{})
	TempDir() string
}

func CreateBaseTestSettings(t TestingT) *settings.Settings {
	tSettings := settings.NewSettings()
	tSettings.DataFolder = t.TempDir()
	t.Logf("using temp data folder: %s", tSettings.DataFolder)

	// Create a copy of RegressionNetParams to avoid race conditions
	chainParams := chaincfg.RegressionNetParams
	chainParams.CoinbaseMaturity = 1
	tSettings.ChainCfgParams = &chainParams
	tSettings.GlobalBlockHeightRetention = 10
	tSettings.BlockValidation.OptimisticMining = false
	tSettings.BlockAssembly.StoreTxInpointsForSubtreeMeta = true
	// Shrink the in-memory currentTxMap shard count for tests. The production default
	// (16384 buckets) pre-allocates ~107MB of empty swiss-map shards per SubtreeProcessor;
	// across the ~100 SubtreeProcessors a single package's tests construct sequentially,
	// that dominated blockassembly.test / subtreeprocessor.test peak RSS under -race
	// (#1051). Bucket count only affects sharding/lock-contention, not results, so test
	// outcomes are unchanged; 16 is the value the subtreeprocessor unit tests already use.
	tSettings.BlockAssembly.SplitMapBuckets = 16

	// We sometimes get 'hot key' errors while running the test
	// To mitigate this, we use more aggressive retry settings with exponential backoff
	tSettings.Aerospike.WritePolicyURL = &url.URL{
		Scheme:   "aerospike",
		RawQuery: "MaxRetries=30&SleepBetweenRetries=50ms&SleepMultiplier=2&TotalTimeout=30s&SocketTimeout=10s",
	}

	// AEROSPIKE_EXPECT_NATIVE_OPS drives the fork-image CI job: it points
	// AEROSPIKE_CONTAINER_IMAGE at the BSV fork of aerospike-server and runs
	// the full suite with the native operate-path enabled, asserting in the
	// store tests that the capability probe actually engaged it. Enabling the
	// setting here (rather than per test) routes every store the suite builds
	// through the same probe production uses; without the env var nothing
	// changes and the default UDF path is tested.
	if os.Getenv("AEROSPIKE_EXPECT_NATIVE_OPS") == "true" {
		tSettings.Aerospike.UseNativeTeranodeOps = true
	}

	return tSettings
}

// containerRuntimeHealthProbe reports whether the TestContainers-backed
// container runtime (Docker/OrbStack) is reachable and healthy. It is a
// package variable so tests can substitute a fake probe; production code
// always uses dockerRuntimeHealth.
var containerRuntimeHealthProbe = dockerRuntimeHealth

// dockerRuntimeHealth asks testcontainers-go itself whether the configured
// Docker provider is reachable, the same check testcontainers-go's own
// SkipIfProviderIsNotHealthy performs. Reusing it avoids re-implementing
// fragile matching against Docker's connection-error strings.
func dockerRuntimeHealth(ctx context.Context) error {
	provider, err := testcontainers.ProviderDocker.GetProvider()
	if err != nil {
		return err
	}

	return provider.Health(ctx)
}

// skipT is the subset of *testing.T (and testing.TB) SkipIfContainerUnavailable
// needs. Declared as an interface, rather than requiring *testing.T directly,
// so tests can substitute a fake and assert the skip-vs-fail decision without
// the assertion itself skipping or failing the real test.
type skipT interface {
	Helper()
	Skipf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
}

// SkipIfContainerUnavailable skips the current test only when the container
// runtime backing TestContainers (Docker/OrbStack) is genuinely unreachable.
// Pass the error returned directly from the container-start call: if it is
// nil, this is a no-op. If it is non-nil, the runtime is probed independently
// of the error's content - a failure unrelated to runtime availability (a
// missing fixture, an image-pull failure, a readiness-check timeout, etc.)
// fails the test instead of being silently reported as skipped.
func SkipIfContainerUnavailable(t skipT, err error) {
	t.Helper()

	if err == nil {
		return
	}

	if probeErr := containerRuntimeHealthProbe(context.Background()); probeErr != nil {
		t.Skipf("skipping: container runtime unavailable: %v", probeErr)
		return
	}

	t.Fatalf("container setup failed for a reason other than runtime unavailability: %v", err)
}
