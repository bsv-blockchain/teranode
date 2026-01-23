package settings

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPolicySettings(t *testing.T) {
	t.Run("CreatesValidInstance", func(t *testing.T) {
		ps := NewPolicySettings()

		require.NotNil(t, ps)

		// Verify all fields are initialized to zero values
		assert.Equal(t, 0, ps.ExcessiveBlockSize)
		assert.Equal(t, 0, ps.BlockMaxSize)
		assert.Equal(t, 0, ps.MaxTxSizePolicy)
		assert.Equal(t, 0, ps.MaxOrphanTxSize)
		assert.Equal(t, int64(0), ps.DataCarrierSize)
		assert.Equal(t, 0, ps.MaxScriptSizePolicy)
		assert.Equal(t, int64(0), ps.MaxOpsPerScriptPolicy)
		assert.Equal(t, 0, ps.MaxScriptNumLengthPolicy)
		assert.Equal(t, int64(0), ps.MaxPubKeysPerMultisigPolicy)
		assert.Equal(t, int64(0), ps.MaxTxSigopsCountsPolicy)
		assert.Equal(t, 0, ps.MaxStackMemoryUsagePolicy)
		assert.Equal(t, 0, ps.MaxStackMemoryUsageConsensus)
		assert.Equal(t, 0, ps.LimitAncestorCount)
		assert.Equal(t, 0, ps.LimitCPFPGroupMembersCount)
		assert.Equal(t, false, ps.AcceptNonStdOutputs)
		assert.Equal(t, false, ps.DataCarrier)
		assert.Equal(t, 0.0, ps.MinMiningTxFee)
		assert.Equal(t, 0, ps.MaxStdTxValidationDuration)
		assert.Equal(t, 0, ps.MaxNonStdTxValidationDuration)
		assert.Equal(t, 0, ps.MaxTxChainValidationBudget)
		assert.Equal(t, false, ps.ValidationClockCPU)
		assert.Equal(t, 0, ps.MinConsolidationFactor)
		assert.Equal(t, 0, ps.MaxConsolidationInputScriptSize)
		assert.Equal(t, 0, ps.MinConfConsolidationInput)
		assert.Equal(t, 0, ps.MinConsolidationInputMaturity)
		assert.Equal(t, false, ps.AcceptNonStdConsolidationInput)
	})
}

func TestPolicySettings_BlockSizeSettings(t *testing.T) {
	ps := NewPolicySettings()

	t.Run("SetAndGetExcessiveBlockSize", func(t *testing.T) {
		testValue := 128000000
		ps.SetExcessiveBlockSize(testValue)
		assert.Equal(t, testValue, ps.GetExcessiveBlockSize())
	})

	t.Run("SetAndGetBlockMaxSize", func(t *testing.T) {
		testValue := 32000000
		ps.SetBlockMaxSize(testValue)
		assert.Equal(t, testValue, ps.GetBlockMaxSize())
	})

	t.Run("ExcessiveBlockSizeZeroValue", func(t *testing.T) {
		ps.SetExcessiveBlockSize(0)
		assert.Equal(t, 0, ps.GetExcessiveBlockSize())
	})

	t.Run("ExcessiveBlockSizeNegativeValue", func(t *testing.T) {
		ps.SetExcessiveBlockSize(-1)
		assert.Equal(t, -1, ps.GetExcessiveBlockSize())
	})

	t.Run("ExcessiveBlockSizeMaxInt", func(t *testing.T) {
		maxValue := 2147483647 // Max int32
		ps.SetExcessiveBlockSize(maxValue)
		assert.Equal(t, maxValue, ps.GetExcessiveBlockSize())
	})
}

func TestPolicySettings_TransactionSizeSettings(t *testing.T) {
	ps := NewPolicySettings()

	t.Run("SetAndGetMaxTxSizePolicy", func(t *testing.T) {
		testValue := 100000000
		ps.SetMaxTxSizePolicy(testValue)
		assert.Equal(t, testValue, ps.GetMaxTxSizePolicy())
	})

	t.Run("SetAndGetMaxOrphanTxSize", func(t *testing.T) {
		testValue := 100000
		ps.SetMaxOrphanTxSize(testValue)
		assert.Equal(t, testValue, ps.GetMaxOrphanTxSize())
	})

	t.Run("MaxTxSizePolicyBoundaryValues", func(t *testing.T) {
		// Test zero
		ps.SetMaxTxSizePolicy(0)
		assert.Equal(t, 0, ps.GetMaxTxSizePolicy())

		// Test large value
		largeValue := 1000000000
		ps.SetMaxTxSizePolicy(largeValue)
		assert.Equal(t, largeValue, ps.GetMaxTxSizePolicy())
	})
}

func TestPolicySettings_DataCarrierSettings(t *testing.T) {
	ps := NewPolicySettings()

	t.Run("SetAndGetDataCarrierSize", func(t *testing.T) {
		testValue := int64(40)
		ps.SetDataCarrierSize(testValue)
		assert.Equal(t, testValue, ps.GetDataCarrierSize())
	})

	t.Run("SetAndGetDataCarrier", func(t *testing.T) {
		ps.SetDataCarrier(true)
		assert.Equal(t, true, ps.GetDataCarrier())

		ps.SetDataCarrier(false)
		assert.Equal(t, false, ps.GetDataCarrier())
	})

	t.Run("DataCarrierSizeLargeValue", func(t *testing.T) {
		testValue := int64(9223372036854775807) // Max int64
		ps.SetDataCarrierSize(testValue)
		assert.Equal(t, testValue, ps.GetDataCarrierSize())
	})

	t.Run("DataCarrierSizeNegativeValue", func(t *testing.T) {
		testValue := int64(-1)
		ps.SetDataCarrierSize(testValue)
		assert.Equal(t, testValue, ps.GetDataCarrierSize())
	})
}

func TestPolicySettings_ScriptSettings(t *testing.T) {
	ps := NewPolicySettings()

	t.Run("SetAndGetMaxScriptSizePolicy", func(t *testing.T) {
		testValue := 1000000
		ps.SetMaxScriptSizePolicy(testValue)
		assert.Equal(t, testValue, ps.GetMaxScriptSizePolicy())
	})

	t.Run("SetAndGetMaxOpsPerScriptPolicy", func(t *testing.T) {
		testValue := int64(500000)
		ps.SetMaxOpsPerScriptPolicy(testValue)
		assert.Equal(t, testValue, ps.GetMaxOpsPerScriptPolicy())
	})

	t.Run("SetAndGetMaxScriptNumLengthPolicy", func(t *testing.T) {
		testValue := 750000
		ps.SetMaxScriptNumLengthPolicy(testValue)
		assert.Equal(t, testValue, ps.GetMaxScriptNumLengthPolicy())
	})

	t.Run("SetAndGetMaxPubKeysPerMultisigPolicy", func(t *testing.T) {
		testValue := int64(2147483647)
		ps.SetMaxPubKeysPerMultisigPolicy(testValue)
		assert.Equal(t, testValue, ps.GetMaxPubKeysPerMultisigPolicy())
	})

	t.Run("SetAndGetMaxTxSigopsCountsPolicy", func(t *testing.T) {
		testValue := int64(4294967295)
		ps.SetMaxTxSigopsCountsPolicy(testValue)
		assert.Equal(t, testValue, ps.GetMaxTxSigopsCountsPolicy())
	})
}

func TestPolicySettings_MemorySettings(t *testing.T) {
	ps := NewPolicySettings()

	t.Run("SetAndGetMaxStackMemoryUsagePolicy", func(t *testing.T) {
		testValue := 100000000
		ps.SetMaxStackMemoryUsagePolicy(testValue)
		assert.Equal(t, testValue, ps.GetMaxStackMemoryUsagePolicy())
	})

	t.Run("SetAndGetMaxStackMemoryUsageConsensus", func(t *testing.T) {
		testValue := 200000000
		ps.SetMaxStackMemoryUsageConsensus(testValue)
		assert.Equal(t, testValue, ps.GetMaxStackMemoryUsageConsensus())
	})

	t.Run("StackMemoryUsageZeroValues", func(t *testing.T) {
		ps.SetMaxStackMemoryUsagePolicy(0)
		ps.SetMaxStackMemoryUsageConsensus(0)

		assert.Equal(t, 0, ps.GetMaxStackMemoryUsagePolicy())
		assert.Equal(t, 0, ps.GetMaxStackMemoryUsageConsensus())
	})
}

func TestPolicySettings_LimitSettings(t *testing.T) {
	ps := NewPolicySettings()

	t.Run("SetAndGetLimitAncestorCount", func(t *testing.T) {
		testValue := 1000
		ps.SetLimitAncestorCount(testValue)
		assert.Equal(t, testValue, ps.GetLimitAncestorCount())
	})

	t.Run("SetAndGetLimitCPFPGroupMembersCount", func(t *testing.T) {
		testValue := 25
		ps.SetLimitCPFPGroupMembersCount(testValue)
		assert.Equal(t, testValue, ps.GetLimitCPFPGroupMembersCount())
	})

	t.Run("LimitSettingsBoundaryValues", func(t *testing.T) {
		// Test with negative values
		ps.SetLimitAncestorCount(-1)
		assert.Equal(t, -1, ps.GetLimitAncestorCount())

		// Test with large values
		ps.SetLimitCPFPGroupMembersCount(2147483647)
		assert.Equal(t, 2147483647, ps.GetLimitCPFPGroupMembersCount())
	})
}

func TestPolicySettings_AcceptanceSettings(t *testing.T) {
	ps := NewPolicySettings()

	t.Run("SetAndGetAcceptNonStdOutputs", func(t *testing.T) {
		ps.SetAcceptNonStdOutputs(true)
		assert.Equal(t, true, ps.GetAcceptNonStdOutputs())

		ps.SetAcceptNonStdOutputs(false)
		assert.Equal(t, false, ps.GetAcceptNonStdOutputs())
	})

	t.Run("SetAndGetAcceptNonStdConsolidationInput", func(t *testing.T) {
		ps.SetAcceptNonStdConsolidationInput(true)
		assert.Equal(t, true, ps.GetAcceptNonStdConsolidationInput())

		ps.SetAcceptNonStdConsolidationInput(false)
		assert.Equal(t, false, ps.GetAcceptNonStdConsolidationInput())
	})

	t.Run("BooleanSettingsToggle", func(t *testing.T) {
		// Test multiple toggles
		ps.SetAcceptNonStdOutputs(true)
		assert.Equal(t, true, ps.GetAcceptNonStdOutputs())

		ps.SetAcceptNonStdOutputs(false)
		assert.Equal(t, false, ps.GetAcceptNonStdOutputs())

		ps.SetAcceptNonStdOutputs(true)
		assert.Equal(t, true, ps.GetAcceptNonStdOutputs())
	})
}

func TestPolicySettings_FeeSettings(t *testing.T) {
	ps := NewPolicySettings()

	t.Run("SetAndGetMinMiningTxFee", func(t *testing.T) {
		testValue := 0.00001
		ps.SetMinMiningTxFee(testValue)
		assert.Equal(t, testValue, ps.GetMinMiningTxFee())
	})

	t.Run("MinMiningTxFeeZeroValue", func(t *testing.T) {
		ps.SetMinMiningTxFee(0.0)
		assert.Equal(t, 0.0, ps.GetMinMiningTxFee())
	})

	t.Run("MinMiningTxFeeNegativeValue", func(t *testing.T) {
		testValue := -0.00001
		ps.SetMinMiningTxFee(testValue)
		assert.Equal(t, testValue, ps.GetMinMiningTxFee())
	})

	t.Run("MinMiningTxFeeLargeValue", func(t *testing.T) {
		testValue := 21000000.0
		ps.SetMinMiningTxFee(testValue)
		assert.Equal(t, testValue, ps.GetMinMiningTxFee())
	})

	t.Run("MinMiningTxFeePrecision", func(t *testing.T) {
		testValue := 0.123456789
		ps.SetMinMiningTxFee(testValue)
		assert.Equal(t, testValue, ps.GetMinMiningTxFee())
	})
}

func TestPolicySettings_ValidationSettings(t *testing.T) {
	ps := NewPolicySettings()

	t.Run("SetAndGetMaxStdTxValidationDuration", func(t *testing.T) {
		testValue := 3000
		ps.SetMaxStdTxValidationDuration(testValue)
		assert.Equal(t, testValue, ps.GetMaxStdTxValidationDuration())
	})

	t.Run("SetAndGetMaxNonStdTxValidationDuration", func(t *testing.T) {
		testValue := 1000
		ps.SetMaxNonStdTxValidationDuration(testValue)
		assert.Equal(t, testValue, ps.GetMaxNonStdTxValidationDuration())
	})

	t.Run("SetAndGetMaxTxChainValidationBudget", func(t *testing.T) {
		testValue := 50
		ps.SetMaxTxChainValidationBudget(testValue)
		assert.Equal(t, testValue, ps.GetMaxTxChainValidationBudget())
	})

	t.Run("SetAndGetValidationClockCPU", func(t *testing.T) {
		ps.SetValidationClockCPU(true)
		assert.Equal(t, true, ps.GetValidationClockCPU())

		ps.SetValidationClockCPU(false)
		assert.Equal(t, false, ps.GetValidationClockCPU())
	})

	t.Run("ValidationDurationBoundaryValues", func(t *testing.T) {
		// Test zero values
		ps.SetMaxStdTxValidationDuration(0)
		ps.SetMaxNonStdTxValidationDuration(0)

		assert.Equal(t, 0, ps.GetMaxStdTxValidationDuration())
		assert.Equal(t, 0, ps.GetMaxNonStdTxValidationDuration())

		// Test negative values
		ps.SetMaxStdTxValidationDuration(-1)
		ps.SetMaxNonStdTxValidationDuration(-1)

		assert.Equal(t, -1, ps.GetMaxStdTxValidationDuration())
		assert.Equal(t, -1, ps.GetMaxNonStdTxValidationDuration())
	})
}

func TestPolicySettings_ConsolidationSettings(t *testing.T) {
	ps := NewPolicySettings()

	t.Run("SetAndGetMinConsolidationFactor", func(t *testing.T) {
		testValue := 20
		ps.SetMinConsolidationFactor(testValue)
		assert.Equal(t, testValue, ps.GetMinConsolidationFactor())
	})

	t.Run("SetAndGetMaxConsolidationInputScriptSize", func(t *testing.T) {
		testValue := 150
		ps.SetMaxConsolidationInputScriptSize(testValue)
		assert.Equal(t, testValue, ps.GetMaxConsolidationInputScriptSize())
	})

	t.Run("SetAndGetMinConfConsolidationInput", func(t *testing.T) {
		testValue := 6
		ps.SetMinConfConsolidationInput(testValue)
		assert.Equal(t, testValue, ps.GetMinConfConsolidationInput())
	})

	t.Run("SetAndGetMinConsolidationInputMaturity", func(t *testing.T) {
		testValue := 6
		ps.SetMinConsolidationInputMaturity(testValue)
		assert.Equal(t, testValue, ps.GetMinConsolidationInputMaturity())
	})

	t.Run("ConsolidationBoundaryValues", func(t *testing.T) {
		// Test zero values
		ps.SetMinConsolidationFactor(0)
		ps.SetMaxConsolidationInputScriptSize(0)
		ps.SetMinConfConsolidationInput(0)
		ps.SetMinConsolidationInputMaturity(0)

		assert.Equal(t, 0, ps.GetMinConsolidationFactor())
		assert.Equal(t, 0, ps.GetMaxConsolidationInputScriptSize())
		assert.Equal(t, 0, ps.GetMinConfConsolidationInput())
		assert.Equal(t, 0, ps.GetMinConsolidationInputMaturity())

		// Test negative values
		ps.SetMinConsolidationFactor(-1)
		ps.SetMaxConsolidationInputScriptSize(-1)
		ps.SetMinConfConsolidationInput(-1)
		ps.SetMinConsolidationInputMaturity(-1)

		assert.Equal(t, -1, ps.GetMinConsolidationFactor())
		assert.Equal(t, -1, ps.GetMaxConsolidationInputScriptSize())
		assert.Equal(t, -1, ps.GetMinConfConsolidationInput())
		assert.Equal(t, -1, ps.GetMinConsolidationInputMaturity())
	})
}

func TestPolicySettings_FieldIndependence(t *testing.T) {
	ps := NewPolicySettings()

	t.Run("SettingOneFieldDoesNotAffectOthers", func(t *testing.T) {
		// Set various fields to non-zero values
		ps.SetExcessiveBlockSize(1000)
		ps.SetDataCarrierSize(50)
		ps.SetAcceptNonStdOutputs(true)
		ps.SetMinMiningTxFee(0.01)
		ps.SetValidationClockCPU(true)

		// Verify they are set correctly
		assert.Equal(t, 1000, ps.GetExcessiveBlockSize())
		assert.Equal(t, int64(50), ps.GetDataCarrierSize())
		assert.Equal(t, true, ps.GetAcceptNonStdOutputs())
		assert.Equal(t, 0.01, ps.GetMinMiningTxFee())
		assert.Equal(t, true, ps.GetValidationClockCPU())

		// Verify other fields remain at zero/default values
		assert.Equal(t, 0, ps.GetBlockMaxSize())
		assert.Equal(t, 0, ps.GetMaxTxSizePolicy())
		assert.Equal(t, false, ps.GetDataCarrier())
		assert.Equal(t, 0, ps.GetMaxStdTxValidationDuration())
		assert.Equal(t, false, ps.GetAcceptNonStdConsolidationInput())
	})
}

func TestPolicySettings_MultipleUpdates(t *testing.T) {
	ps := NewPolicySettings()

	t.Run("MultipleUpdatesToSameField", func(t *testing.T) {
		// Test multiple updates to the same field
		ps.SetExcessiveBlockSize(100)
		assert.Equal(t, 100, ps.GetExcessiveBlockSize())

		ps.SetExcessiveBlockSize(200)
		assert.Equal(t, 200, ps.GetExcessiveBlockSize())

		ps.SetExcessiveBlockSize(0)
		assert.Equal(t, 0, ps.GetExcessiveBlockSize())

		ps.SetExcessiveBlockSize(-100)
		assert.Equal(t, -100, ps.GetExcessiveBlockSize())
	})

	t.Run("BooleanFieldToggling", func(t *testing.T) {
		// Test boolean field toggling
		assert.Equal(t, false, ps.GetDataCarrier()) // Initial state

		ps.SetDataCarrier(true)
		assert.Equal(t, true, ps.GetDataCarrier())

		ps.SetDataCarrier(false)
		assert.Equal(t, false, ps.GetDataCarrier())

		ps.SetDataCarrier(true)
		assert.Equal(t, true, ps.GetDataCarrier())

		ps.SetDataCarrier(true) // Setting same value again
		assert.Equal(t, true, ps.GetDataCarrier())
	})
}

func TestPolicySettings_DataTypes(t *testing.T) {
	ps := NewPolicySettings()

	t.Run("IntFields", func(t *testing.T) {
		intFields := []struct {
			name   string
			setter func(int)
			getter func() int
		}{
			{"ExcessiveBlockSize", ps.SetExcessiveBlockSize, ps.GetExcessiveBlockSize},
			{"BlockMaxSize", ps.SetBlockMaxSize, ps.GetBlockMaxSize},
			{"MaxTxSizePolicy", ps.SetMaxTxSizePolicy, ps.GetMaxTxSizePolicy},
			{"MaxOrphanTxSize", ps.SetMaxOrphanTxSize, ps.GetMaxOrphanTxSize},
			{"MaxScriptSizePolicy", ps.SetMaxScriptSizePolicy, ps.GetMaxScriptSizePolicy},
			{"MaxScriptNumLengthPolicy", ps.SetMaxScriptNumLengthPolicy, ps.GetMaxScriptNumLengthPolicy},
			{"MaxStackMemoryUsagePolicy", ps.SetMaxStackMemoryUsagePolicy, ps.GetMaxStackMemoryUsagePolicy},
			{"MaxStackMemoryUsageConsensus", ps.SetMaxStackMemoryUsageConsensus, ps.GetMaxStackMemoryUsageConsensus},
			{"LimitAncestorCount", ps.SetLimitAncestorCount, ps.GetLimitAncestorCount},
			{"LimitCPFPGroupMembersCount", ps.SetLimitCPFPGroupMembersCount, ps.GetLimitCPFPGroupMembersCount},
			{"MaxStdTxValidationDuration", ps.SetMaxStdTxValidationDuration, ps.GetMaxStdTxValidationDuration},
			{"MaxNonStdTxValidationDuration", ps.SetMaxNonStdTxValidationDuration, ps.GetMaxNonStdTxValidationDuration},
			{"MaxTxChainValidationBudget", ps.SetMaxTxChainValidationBudget, ps.GetMaxTxChainValidationBudget},
			{"MinConsolidationFactor", ps.SetMinConsolidationFactor, ps.GetMinConsolidationFactor},
			{"MaxConsolidationInputScriptSize", ps.SetMaxConsolidationInputScriptSize, ps.GetMaxConsolidationInputScriptSize},
			{"MinConfConsolidationInput", ps.SetMinConfConsolidationInput, ps.GetMinConfConsolidationInput},
			{"MinConsolidationInputMaturity", ps.SetMinConsolidationInputMaturity, ps.GetMinConsolidationInputMaturity},
		}

		for _, field := range intFields {
			t.Run(field.name, func(t *testing.T) {
				testValue := 12345
				field.setter(testValue)
				assert.Equal(t, testValue, field.getter(), "Field %s should store and retrieve int values correctly", field.name)
			})
		}
	})

	t.Run("Int64Fields", func(t *testing.T) {
		int64Fields := []struct {
			name   string
			setter func(int64)
			getter func() int64
		}{
			{"DataCarrierSize", ps.SetDataCarrierSize, ps.GetDataCarrierSize},
			{"MaxOpsPerScriptPolicy", ps.SetMaxOpsPerScriptPolicy, ps.GetMaxOpsPerScriptPolicy},
			{"MaxPubKeysPerMultisigPolicy", ps.SetMaxPubKeysPerMultisigPolicy, ps.GetMaxPubKeysPerMultisigPolicy},
			{"MaxTxSigopsCountsPolicy", ps.SetMaxTxSigopsCountsPolicy, ps.GetMaxTxSigopsCountsPolicy},
		}

		for _, field := range int64Fields {
			t.Run(field.name, func(t *testing.T) {
				testValue := int64(123456789012345)
				field.setter(testValue)
				assert.Equal(t, testValue, field.getter(), "Field %s should store and retrieve int64 values correctly", field.name)
			})
		}
	})

	t.Run("BoolFields", func(t *testing.T) {
		boolFields := []struct {
			name   string
			setter func(bool)
			getter func() bool
		}{
			{"AcceptNonStdOutputs", ps.SetAcceptNonStdOutputs, ps.GetAcceptNonStdOutputs},
			{"DataCarrier", ps.SetDataCarrier, ps.GetDataCarrier},
			{"ValidationClockCPU", ps.SetValidationClockCPU, ps.GetValidationClockCPU},
			{"AcceptNonStdConsolidationInput", ps.SetAcceptNonStdConsolidationInput, ps.GetAcceptNonStdConsolidationInput},
		}

		for _, field := range boolFields {
			t.Run(field.name, func(t *testing.T) {
				// Test true
				field.setter(true)
				assert.Equal(t, true, field.getter(), "Field %s should store and retrieve true correctly", field.name)

				// Test false
				field.setter(false)
				assert.Equal(t, false, field.getter(), "Field %s should store and retrieve false correctly", field.name)
			})
		}
	})

	t.Run("Float64Fields", func(t *testing.T) {
		testValue := 3.14159265359
		ps.SetMinMiningTxFee(testValue)
		assert.Equal(t, testValue, ps.GetMinMiningTxFee(), "MinMiningTxFee should store and retrieve float64 values correctly")
	})
}
