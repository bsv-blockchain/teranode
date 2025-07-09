package test

import (
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bsv-blockchain/go-chaincfg"
)

func CreateBaseTestSettings() *settings.Settings {
	settings := settings.NewSettings()
	settings.ChainCfgParams = &chaincfg.RegressionNetParams
	settings.GlobalBlockHeightRetention = 10
	settings.BlockValidation.OptimisticMining = false
	settings.ChainCfgParams.CoinbaseMaturity = 1

	// Initialize adjustment values to 0 for tests (use global value by default)
	settings.UtxoStore.BlockHeightRetentionAdjustment = 0
	settings.SubtreeValidation.BlockHeightRetentionAdjustment = 0

	return settings
}
