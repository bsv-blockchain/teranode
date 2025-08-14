package test

import (
	"net/url"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bsv-blockchain/go-chaincfg"
)

func CreateBaseTestSettings() *settings.Settings {
	settings := settings.NewSettings()
	settings.ChainCfgParams = &chaincfg.RegressionNetParams
	settings.GlobalBlockHeightRetention = 10
	settings.BlockValidation.OptimisticMining = false
	settings.ChainCfgParams.CoinbaseMaturity = 1

	// We sometimes get 'hot key' errors while running the test
	// To mitigate this, we use more aggressive retry settings with exponential backoff
	settings.Aerospike.WritePolicyURL = &url.URL{
		Scheme:   "aerospike",
		RawQuery: "MaxRetries=30&SleepBetweenRetries=50ms&SleepMultiplier=2&TotalTimeout=30s&SocketTimeout=10s",
	}

	// Initialize adjustment values to 0 for tests (use global value by default)
	settings.UtxoStore.BlockHeightRetentionAdjustment = 0
	settings.SubtreeValidation.BlockHeightRetentionAdjustment = 0

	return settings
}
