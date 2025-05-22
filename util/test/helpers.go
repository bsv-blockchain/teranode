package test

import (
	"github.com/bitcoin-sv/teranode/chaincfg"
	"github.com/bitcoin-sv/teranode/settings"
)

func CreateBaseTestSettings() *settings.Settings {
	settings := settings.NewSettings()
	settings.ChainCfgParams = &chaincfg.RegressionNetParams
	settings.UtxoStore.BlockHeightRetention = 1
	settings.BlockValidation.OptimisticMining = false

	return settings
}
