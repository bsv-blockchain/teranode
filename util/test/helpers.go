package test

import (
	"github.com/bitcoin-sv/teranode/chaincfg"
	"github.com/bitcoin-sv/teranode/settings"
)

func CreateBaseTestSettings() *settings.Settings {
	settings := settings.NewSettings()
	settings.ChainCfgParams = &chaincfg.RegressionNetParams

	return settings
}
