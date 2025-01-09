package settings

import (
	"testing"

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
