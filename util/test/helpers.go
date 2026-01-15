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

	// We sometimes get 'hot key' errors while running the test
	// To mitigate this, we use more aggressive retry settings with exponential backoff
	tSettings.Aerospike.WritePolicyURL = &url.URL{
		Scheme:   "aerospike",
		RawQuery: "MaxRetries=30&SleepBetweenRetries=50ms&SleepMultiplier=2&TotalTimeout=30s&SocketTimeout=10s",
	}

	return tSettings
}
