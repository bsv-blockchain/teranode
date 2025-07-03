package test

import (
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bsv-blockchain/go-chaincfg"
)

func CreateBaseTestSettings() *settings.Settings {
	settings := settings.NewSettings()
	settings.ChainCfgParams = &chaincfg.RegressionNetParams
	settings.UtxoStore.BlockHeightRetention = 10
	settings.BlockValidation.OptimisticMining = false
	settings.ChainCfgParams.CoinbaseMaturity = 1

	return settings
}
