package test

import (
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateBaseTestSettings(t *testing.T) {
	t.Run("basic functionality", func(t *testing.T) {
		settings := CreateBaseTestSettings(t)
		require.NotNil(t, settings, "CreateBaseTestSettings should return non-nil settings")
	})

	t.Run("chain configuration", func(t *testing.T) {
		settings := CreateBaseTestSettings(t)

		assert.NotNil(t, settings.ChainCfgParams, "ChainCfgParams should not be nil")
		assert.Equal(t, &chaincfg.RegressionNetParams, settings.ChainCfgParams,
			"ChainCfgParams should be set to RegressionNetParams")
		assert.Equal(t, uint16(1), settings.ChainCfgParams.CoinbaseMaturity,
			"CoinbaseMaturity should be set to 1")
	})

	t.Run("global block height retention", func(t *testing.T) {
		settings := CreateBaseTestSettings(t)

		assert.Equal(t, uint32(10), settings.GlobalBlockHeightRetention,
			"GlobalBlockHeightRetention should be set to 10")
	})

	t.Run("block validation settings", func(t *testing.T) {
		settings := CreateBaseTestSettings(t)

		assert.False(t, settings.BlockValidation.OptimisticMining,
			"OptimisticMining should be disabled for tests")
	})

	t.Run("aerospike write policy configuration", func(t *testing.T) {
		settings := CreateBaseTestSettings(t)

		require.NotNil(t, settings.Aerospike.WritePolicyURL,
			"Aerospike WritePolicyURL should not be nil")

		writePolicyURL := settings.Aerospike.WritePolicyURL
		assert.Equal(t, "aerospike", writePolicyURL.Scheme,
			"WritePolicyURL scheme should be 'aerospike'")

		// Parse query parameters
		values, err := url.ParseQuery(writePolicyURL.RawQuery)
		require.NoError(t, err, "Should be able to parse WritePolicyURL query parameters")

		expectedParams := map[string]string{
			"MaxRetries":          "30",
			"SleepBetweenRetries": "50ms",
			"SleepMultiplier":     "2",
			"TotalTimeout":        "30s",
			"SocketTimeout":       "10s",
		}

		for param, expectedValue := range expectedParams {
			actualValue := values.Get(param)
			assert.Equal(t, expectedValue, actualValue,
				"WritePolicyURL parameter %s should be %s", param, expectedValue)
		}
	})

	t.Run("store retention adjustments", func(t *testing.T) {
		settings := CreateBaseTestSettings(t)

		assert.Equal(t, int32(0), settings.UtxoStore.BlockHeightRetentionAdjustment,
			"UtxoStore.BlockHeightRetentionAdjustment should be 0")
		assert.Equal(t, int32(0), settings.SubtreeValidation.BlockHeightRetentionAdjustment,
			"SubtreeValidation.BlockHeightRetentionAdjustment should be 0")
	})

	t.Run("consistency across multiple calls", func(t *testing.T) {
		settings1 := CreateBaseTestSettings(t)
		settings2 := CreateBaseTestSettings(t)

		// Verify they are different objects
		assert.NotSame(t, settings1, settings2,
			"Multiple calls should return different instances")

		// But with same configuration values
		assert.Equal(t, settings1.GlobalBlockHeightRetention, settings2.GlobalBlockHeightRetention,
			"GlobalBlockHeightRetention should be consistent across calls")
		assert.Equal(t, settings1.ChainCfgParams, settings2.ChainCfgParams,
			"ChainCfgParams should be consistent across calls")
		assert.Equal(t, settings1.BlockValidation.OptimisticMining, settings2.BlockValidation.OptimisticMining,
			"OptimisticMining should be consistent across calls")
		assert.Equal(t, settings1.UtxoStore.BlockHeightRetentionAdjustment, settings2.UtxoStore.BlockHeightRetentionAdjustment,
			"UtxoStore.BlockHeightRetentionAdjustment should be consistent across calls")
		assert.Equal(t, settings1.SubtreeValidation.BlockHeightRetentionAdjustment, settings2.SubtreeValidation.BlockHeightRetentionAdjustment,
			"SubtreeValidation.BlockHeightRetentionAdjustment should be consistent across calls")

		// Verify Aerospike URLs are equivalent but different instances
		assert.NotSame(t, settings1.Aerospike.WritePolicyURL, settings2.Aerospike.WritePolicyURL,
			"WritePolicyURL instances should be different objects")
		assert.Equal(t, settings1.Aerospike.WritePolicyURL.String(), settings2.Aerospike.WritePolicyURL.String(),
			"WritePolicyURL values should be equivalent")
	})

	t.Run("regression net params properties", func(t *testing.T) {
		settings := CreateBaseTestSettings(t)

		// Verify key properties of RegressionNetParams that are important for testing
		assert.NotEmpty(t, settings.ChainCfgParams.Name, "Chain params should have a name")
		assert.NotNil(t, settings.ChainCfgParams.GenesisBlock, "Chain params should have a genesis block")
		assert.NotNil(t, settings.ChainCfgParams.GenesisHash, "Chain params should have a genesis hash")
	})
}
