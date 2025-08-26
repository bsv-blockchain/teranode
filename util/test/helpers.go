package test

import (
	"net/url"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bsv-blockchain/go-chaincfg"
)

// CreateBaseTestSettings initializes a base settings configuration for testing purposes.
func CreateBaseTestSettings() *settings.Settings {
	baseSettings := settings.NewSettings()
	baseSettings.ChainCfgParams = &chaincfg.RegressionNetParams
	baseSettings.GlobalBlockHeightRetention = 10
	baseSettings.BlockValidation.OptimisticMining = false
	baseSettings.ChainCfgParams.CoinbaseMaturity = 1

	// We sometimes get 'hot key' errors while running the test
	// To mitigate this, we use more aggressive retry settings with exponential backoff
	baseSettings.Aerospike.WritePolicyURL = &url.URL{
		Scheme:   "aerospike",
		RawQuery: "MaxRetries=30&SleepBetweenRetries=50ms&SleepMultiplier=2&TotalTimeout=30s&SocketTimeout=10s",
	}

	// Initialize adjustment values to 0 for tests (use global value by default)
	baseSettings.UtxoStore.BlockHeightRetentionAdjustment = 0
	baseSettings.SubtreeValidation.BlockHeightRetentionAdjustment = 0

	return baseSettings
}
