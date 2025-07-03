package settings

import (
	"testing"

	"github.com/bitcoin-sv/teranode/pkg/go-chaincfg"
	"github.com/stretchr/testify/require"
)

// check settings object is initialised
func TestInitialiseSettings(t *testing.T) {
	tSettings := NewSettings()

	if tSettings.ChainCfgParams == nil {
		t.Errorf("ChainCfgParams is nil")
	}

	require.NotNil(t, tSettings.Policy)
	require.NotNil(t, tSettings.BlockAssembly)
	require.NotNil(t, tSettings.SubtreeValidation)
	require.NotNil(t, tSettings.BlockChain)
	require.NotNil(t, tSettings.BlockValidation)

	require.NotNil(t, tSettings.BlockChain)
	require.NotNil(t, tSettings.BlockChain.StoreURL)

	require.NotNil(t, tSettings.UtxoStore)

	require.NotNil(t, tSettings.Block)
}

func TestGenesisActivationHeight(t *testing.T) {
	tests := []struct {
		name   string
		params *chaincfg.Params
		expect uint32
	}{
		{"RegressionNet", &chaincfg.RegressionNetParams, 10000},
		{"TestNet", &chaincfg.TestNetParams, 1344302},
		{"MainNet", &chaincfg.MainNetParams, 620538},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tSettings := NewSettings()
			tSettings.ChainCfgParams = tt.params
			require.Equal(t, tt.expect, tSettings.ChainCfgParams.GenesisActivationHeight)
		})
	}
}
