package test

import (
	"net/url"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/settings"
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

	return tSettings
}
