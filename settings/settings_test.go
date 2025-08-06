package settings

import (
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
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

func TestBootstrapPersistentSetting(t *testing.T) {
	tests := []struct {
		name         string
		envValue     string
		expectedBool bool
	}{
		{"Default false", "", false},
		{"Explicit false", "false", false},
		{"Explicit true", "true", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set environment variable for testing
			if tt.envValue != "" {
				t.Setenv("p2p_bootstrap_persistent", tt.envValue)
			}

			tSettings := NewSettings()
			require.Equal(t, tt.expectedBool, tSettings.P2P.BootstrapPersistent)
		})
	}
}

func TestBlockHeightRetentionAdjustments(t *testing.T) {
	t.Run("DefaultAdjustmentValues", func(t *testing.T) {
		tSettings := NewSettings()
		tSettings.GlobalBlockHeightRetention = 100

		// Test that default adjustment values are 0
		require.Equal(t, int32(0), tSettings.UtxoStore.BlockHeightRetentionAdjustment)
		require.Equal(t, int32(0), tSettings.SubtreeValidation.BlockHeightRetentionAdjustment)

		// Test that calculated values equal global value when adjustments are 0
		require.Equal(t, uint32(100), tSettings.GetUtxoStoreBlockHeightRetention())
		require.Equal(t, uint32(100), tSettings.GetSubtreeValidationBlockHeightRetention())
	})

	t.Run("PositiveAdjustments", func(t *testing.T) {
		tSettings := NewSettings()
		tSettings.GlobalBlockHeightRetention = 100
		tSettings.UtxoStore.BlockHeightRetentionAdjustment = 50
		tSettings.SubtreeValidation.BlockHeightRetentionAdjustment = 25

		// Test positive adjustments increase the effective values
		require.Equal(t, uint32(150), tSettings.GetUtxoStoreBlockHeightRetention())
		require.Equal(t, uint32(125), tSettings.GetSubtreeValidationBlockHeightRetention())
	})

	t.Run("NegativeAdjustments", func(t *testing.T) {
		tSettings := NewSettings()
		tSettings.GlobalBlockHeightRetention = 100
		tSettings.UtxoStore.BlockHeightRetentionAdjustment = -30
		tSettings.SubtreeValidation.BlockHeightRetentionAdjustment = -20

		// Test negative adjustments decrease the effective values
		require.Equal(t, uint32(70), tSettings.GetUtxoStoreBlockHeightRetention())
		require.Equal(t, uint32(80), tSettings.GetSubtreeValidationBlockHeightRetention())
	})

	t.Run("BoundsChecking", func(t *testing.T) {
		tSettings := NewSettings()
		tSettings.GlobalBlockHeightRetention = 50
		tSettings.UtxoStore.BlockHeightRetentionAdjustment = -100
		tSettings.SubtreeValidation.BlockHeightRetentionAdjustment = -75

		// Test that negative results are clamped to 0
		require.Equal(t, uint32(0), tSettings.GetUtxoStoreBlockHeightRetention())
		require.Equal(t, uint32(0), tSettings.GetSubtreeValidationBlockHeightRetention())
	})

	t.Run("LargeValues", func(t *testing.T) {
		tSettings := NewSettings()
		tSettings.GlobalBlockHeightRetention = 1000000
		tSettings.UtxoStore.BlockHeightRetentionAdjustment = 500000
		tSettings.SubtreeValidation.BlockHeightRetentionAdjustment = -250000

		// Test with large values to ensure no overflow issues
		require.Equal(t, uint32(1500000), tSettings.GetUtxoStoreBlockHeightRetention())
		require.Equal(t, uint32(750000), tSettings.GetSubtreeValidationBlockHeightRetention())
	})

	t.Run("ZeroGlobalValue", func(t *testing.T) {
		tSettings := NewSettings()
		tSettings.GlobalBlockHeightRetention = 0
		tSettings.UtxoStore.BlockHeightRetentionAdjustment = 100
		tSettings.SubtreeValidation.BlockHeightRetentionAdjustment = -50

		// Test behavior with zero global value
		require.Equal(t, uint32(100), tSettings.GetUtxoStoreBlockHeightRetention())
		require.Equal(t, uint32(0), tSettings.GetSubtreeValidationBlockHeightRetention())
	})
}
