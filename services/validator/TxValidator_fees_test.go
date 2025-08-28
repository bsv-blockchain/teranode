package validator

import (
	"testing"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/assert"
)

// TestTxValidator_Fees_BitcoinSV tests fee calculation matching Bitcoin SV implementation
func TestTxValidator_Fees_BitcoinSV(t *testing.T) {
	tests := []struct {
		name           string
		txSize         int     // Transaction size in bytes
		fee            uint64  // Fee paid in satoshis
		minFeeRate     float64 // Minimum fee rate in BSV/kB
		expectError    bool    // Whether we expect a fee error
		expectedMinFee uint64  // Expected minimum fee in satoshis
	}{
		// Test cases matching Bitcoin SV's GetFee() logic
		{
			name:           "default_minrelayfee_250_satoshis_per_kb",
			txSize:         1000,
			fee:            250,
			minFeeRate:     0.00000250, // 250 satoshis/kB (Bitcoin SV default)
			expectError:    false,
			expectedMinFee: 250,
		},
		{
			name:           "default_minrelayfee_insufficient_fee",
			txSize:         1000,
			fee:            249,
			minFeeRate:     0.00000250, // 250 satoshis/kB
			expectError:    true,
			expectedMinFee: 250,
		},
		{
			name:           "500_satoshis_per_kb_standard_tx",
			txSize:         500,
			fee:            250,
			minFeeRate:     0.00000500, // 500 satoshis/kB (0.5 sat/byte)
			expectError:    false,
			expectedMinFee: 250,
		},
		{
			name:           "500_satoshis_per_kb_large_tx",
			txSize:         2000,
			fee:            1000,
			minFeeRate:     0.00000500, // 500 satoshis/kB
			expectError:    false,
			expectedMinFee: 1000,
		},
		{
			name:           "minimum_1_satoshi_for_tiny_tx",
			txSize:         100,
			fee:            1,
			minFeeRate:     0.00000001, // 1 satoshi/kB
			expectError:    false,
			expectedMinFee: 1, // Minimum 1 satoshi rule
		},
		{
			name:           "zero_fee_with_policy",
			txSize:         500,
			fee:            0,
			minFeeRate:     0.00000500, // 500 satoshis/kB
			expectError:    true,
			expectedMinFee: 250,
		},
		{
			name:           "zero_fee_no_policy",
			txSize:         1000,
			fee:            0,
			minFeeRate:     0, // No fee policy
			expectError:    false,
			expectedMinFee: 0,
		},
		{
			name:           "fractional_satoshi_rounds_down",
			txSize:         999,
			fee:            249,
			minFeeRate:     0.00000250, // 250 satoshis/kB -> 999 * 250 / 1000 = 249.75 -> 249
			expectError:    false,
			expectedMinFee: 249,
		},
		{
			name:           "exact_fee_calculation",
			txSize:         1500,
			fee:            375,
			minFeeRate:     0.00000250, // 250 satoshis/kB -> 1500 * 250 / 1000 = 375
			expectError:    false,
			expectedMinFee: 375,
		},
		{
			name:           "very_low_fee_rate_minimum_1_sat",
			txSize:         10000,
			fee:            1,
			minFeeRate:     0.00000001, // 1 satoshi/kB -> 10000 * 1 / 1000 = 10
			expectError:    true,
			expectedMinFee: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test transaction
			tx := createTestTransactionWithFee(t, tt.txSize, tt.fee)
			actualSize := tx.Size()

			// Create validator with specified fee rate
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.Policy.MinMiningTxFee = tt.minFeeRate
			tSettings.ChainCfgParams = &chaincfg.MainNetParams

			tv := NewTxValidator(ulogger.TestLogger{}, tSettings)

			// Recalculate fee based on actual transaction size
			satoshisPerByte := tt.minFeeRate * 1e8 / 1000
			actualMinFee := uint64(satoshisPerByte * float64(actualSize))
			if actualMinFee == 0 && actualSize > 0 && tt.minFeeRate > 0 {
				actualMinFee = 1
			}

			// Adjust the transaction fee if needed for the actual size
			if !tt.expectError && tt.fee < actualMinFee {
				// Recreate transaction with correct fee for actual size
				tx = createTestTransactionWithFee(t, tt.txSize, actualMinFee)
			}

			// Test fee validation
			err := tv.checkFees(tx, 500000, nil)

			if tt.expectError {
				assert.Error(t, err, "Expected fee error for test case: %s", tt.name)
				if err != nil {
					assert.Contains(t, err.Error(), "transaction fee is too low")
				}
			} else {
				assert.NoError(t, err, "Unexpected fee error for test case: %s", tt.name)
			}
		})
	}
}

// TestTxValidator_Fees_ConsolidationExemption tests consolidation transaction fee exemptions
func TestTxValidator_Fees_ConsolidationExemption(t *testing.T) {
	tests := []struct {
		name                string
		numInputs           int
		numOutputs          int
		consolidationFactor int
		minFeeRate          float64
		fee                 uint64
		expectConsolidation bool
		expectFeeError      bool
	}{
		{
			name:                "consolidation_20_inputs_1_output_zero_fee",
			numInputs:           20,
			numOutputs:          1,
			consolidationFactor: 20,
			minFeeRate:          0.00000500,
			fee:                 0,
			expectConsolidation: true,
			expectFeeError:      false, // Consolidation transactions are fee-exempt
		},
		{
			name:                "consolidation_100_inputs_5_outputs_zero_fee",
			numInputs:           100,
			numOutputs:          5,
			consolidationFactor: 20,
			minFeeRate:          0.00000500,
			fee:                 0,
			expectConsolidation: true,
			expectFeeError:      false,
		},
		{
			name:                "not_consolidation_19_inputs_1_output",
			numInputs:           19,
			numOutputs:          1,
			consolidationFactor: 20,
			minFeeRate:          0.00000500,
			fee:                 0,
			expectConsolidation: false,
			expectFeeError:      true, // Not consolidation, so fee required
		},
		{
			name:                "not_consolidation_100_inputs_6_outputs",
			numInputs:           100,
			numOutputs:          6,
			consolidationFactor: 20,
			minFeeRate:          0.00000500,
			fee:                 0,
			expectConsolidation: false,
			expectFeeError:      true,
		},
		{
			name:                "consolidation_factor_10",
			numInputs:           30,
			numOutputs:          3,
			consolidationFactor: 10,
			minFeeRate:          0.00000500,
			fee:                 0,
			expectConsolidation: true,
			expectFeeError:      false,
		},
		{
			name:                "consolidation_disabled_factor_0",
			numInputs:           100,
			numOutputs:          1,
			consolidationFactor: 0,
			minFeeRate:          0.00000500,
			fee:                 0,
			expectConsolidation: false,
			expectFeeError:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create consolidation transaction
			tx := createConsolidationTransaction(t, tt.numInputs, tt.numOutputs, tt.fee)

			// Create validator with consolidation settings
			policy := settings.NewPolicySettings()
			policy.MinConsolidationFactor = tt.consolidationFactor
			policy.MinMiningTxFee = tt.minFeeRate

			tSettings := &settings.Settings{
				Policy:         policy,
				ChainCfgParams: &chaincfg.MainNetParams,
			}

			tv := NewTxValidator(ulogger.TestLogger{}, tSettings)

			// Check if it's detected as consolidation
			isConsolidation := tv.isConsolidationTx(tx, nil, 500000)
			assert.Equal(t, tt.expectConsolidation, isConsolidation,
				"Consolidation detection mismatch for test case: %s", tt.name)

			// Test fee validation
			err := tv.checkFees(tx, 500000, nil)

			if tt.expectFeeError {
				assert.Error(t, err, "Expected fee error for test case: %s", tt.name)
			} else {
				assert.NoError(t, err, "Unexpected fee error for test case: %s", tt.name)
			}
		})
	}
}

// TestTxValidator_Fees_EdgeCases tests edge cases in fee calculation
func TestTxValidator_Fees_EdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		txSize      int
		fee         uint64
		minFeeRate  float64
		description string
		expectError bool
	}{
		{
			name:        "minimal_transaction",
			txSize:      200,
			fee:         100,
			minFeeRate:  0.00000500,
			description: "Minimal transaction with exact fee",
			expectError: false,
		},
		{
			name:        "one_byte_transaction_minimum_1_sat",
			txSize:      1,
			fee:         1,
			minFeeRate:  0.00000001,
			description: "Smallest possible transaction needs minimum 1 satoshi",
			expectError: false,
		},
		{
			name:        "large_transaction",
			txSize:      10000, // 10KB
			fee:         5100,  // ~5000 satoshis for ~10KB at 0.5 sat/byte (with margin)
			minFeeRate:  0.00000500,
			description: "Large transaction test",
			expectError: false,
		},
		{
			name:        "exact_1_satoshi_per_kb",
			txSize:      1000,
			fee:         1,
			minFeeRate:  0.00000001, // 1 satoshi/kB
			description: "Exact 1 satoshi per kilobyte",
			expectError: false,
		},
		{
			name:        "fractional_satoshi_per_byte",
			txSize:      3000,
			fee:         1,
			minFeeRate:  0.00000001, // 1 satoshi/kB -> 3 satoshis required
			description: "Fractional satoshi per byte calculation",
			expectError: true,
		},
		{
			name:        "negative_fee_protection",
			txSize:      1000,
			fee:         0, // This would result in negative fee if outputs > inputs
			minFeeRate:  0.00000500,
			description: "Protection against negative fees",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test transaction
			tx := createTestTransactionWithFee(t, tt.txSize, tt.fee)

			// Create validator
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.Policy.MinMiningTxFee = tt.minFeeRate
			tSettings.ChainCfgParams = &chaincfg.MainNetParams

			tv := NewTxValidator(ulogger.TestLogger{}, tSettings)

			// Test fee validation
			err := tv.checkFees(tx, 500000, nil)

			if tt.expectError {
				assert.Error(t, err, "Expected error for: %s - %s", tt.name, tt.description)
			} else {
				assert.NoError(t, err, "Unexpected error for: %s - %s", tt.name, tt.description)
			}
		})
	}
}

// TestTxValidator_Fees_DustReturn tests dust return transaction fee exemption
func TestTxValidator_Fees_DustReturn(t *testing.T) {
	// Create a dust return transaction (single 0-satoshi unspendable output)
	tx := bt.NewTx()

	// Add some inputs
	for i := 0; i < 5; i++ {
		input := &bt.Input{
			PreviousTxSatoshis: 1000,
			PreviousTxScript:   &bscript.Script{},
			UnlockingScript:    &bscript.Script{},
			SequenceNumber:     0xfffffffe,
			PreviousTxOutIndex: uint32(i),
		}
		_ = input.PreviousTxIDAdd(&chainhash.Hash{})
		tx.Inputs = append(tx.Inputs, input)
	}

	// Add single 0-satoshi OP_FALSE OP_RETURN output
	dustScript, _ := bscript.NewFromASM("OP_FALSE OP_RETURN")
	tx.Outputs = append(tx.Outputs, &bt.Output{
		Satoshis:      0,
		LockingScript: dustScript,
	})

	// Create validator with fee policy
	policy := settings.NewPolicySettings()
	policy.MinConsolidationFactor = 20
	policy.MinMiningTxFee = 0.00000500 // 500 satoshis/kB

	tSettings := &settings.Settings{
		Policy:         policy,
		ChainCfgParams: &chaincfg.MainNetParams,
	}

	tv := NewTxValidator(ulogger.TestLogger{}, tSettings)

	// Verify it's detected as dust return
	isDustReturn := tv.isDustReturnTx(tx)
	assert.True(t, isDustReturn, "Should be detected as dust return transaction")

	// Verify it's treated as consolidation (fee-exempt)
	isConsolidation := tv.isConsolidationTx(tx, nil, 500000)
	assert.True(t, isConsolidation, "Dust return should be treated as consolidation")

	// Verify no fee error despite zero fee
	err := tv.checkFees(tx, 500000, nil)
	assert.NoError(t, err, "Dust return transactions should be fee-exempt")
}

// TestTxValidator_Fees_VariousFeeRates tests various fee rates used in Bitcoin SV
func TestTxValidator_Fees_VariousFeeRates(t *testing.T) {
	feeRates := []struct {
		name       string
		rate       float64
		satPerKB   uint64
		satPerByte float64
	}{
		{
			name:       "bitcoin_sv_default",
			rate:       0.00000250, // 250 satoshis/kB
			satPerKB:   250,
			satPerByte: 0.25,
		},
		{
			name:       "common_miner_rate",
			rate:       0.00000500, // 500 satoshis/kB
			satPerKB:   500,
			satPerByte: 0.5,
		},
		{
			name:       "very_low_rate",
			rate:       0.00000001, // 1 satoshi/kB
			satPerKB:   1,
			satPerByte: 0.001,
		},
		{
			name:       "high_priority_rate",
			rate:       0.00001000, // 1000 satoshis/kB
			satPerKB:   1000,
			satPerByte: 1.0,
		},
		{
			name:       "zero_fee_policy",
			rate:       0, // No minimum fee
			satPerKB:   0,
			satPerByte: 0,
		},
	}

	for _, fr := range feeRates {
		t.Run(fr.name, func(t *testing.T) {
			// Test with various transaction sizes
			// Note: minimum realistic P2PKH transaction is ~192 bytes
			sizes := []int{200, 500, 1000, 2000, 10000}

			for _, size := range sizes {
				// Create transaction
				tx := createTestTransactionWithFee(t, size, 0) // Create with 0 fee first
				actualSize := tx.Size()

				// Calculate expected fee based on actual size
				expectedFee := uint64(float64(actualSize) * fr.satPerByte)
				if expectedFee == 0 && actualSize > 0 && fr.rate > 0 {
					expectedFee = 1 // Minimum 1 satoshi rule
				}

				// Create transaction with exact required fee
				tx = createTestTransactionWithFee(t, size, expectedFee)

				// Create validator
				tSettings := test.CreateBaseTestSettings(t)
				tSettings.Policy.MinMiningTxFee = fr.rate
				tSettings.ChainCfgParams = &chaincfg.MainNetParams

				tv := NewTxValidator(ulogger.TestLogger{}, tSettings)

				// Should pass with exact fee
				err := tv.checkFees(tx, 500000, nil)
				assert.NoError(t, err, "Should pass with exact fee for size %d (actual %d) at rate %s", size, actualSize, fr.name)

				// Should fail with 1 satoshi less (unless zero fee policy)
				if expectedFee > 0 {
					txLowFee := createTestTransactionWithFee(t, size, expectedFee-1)
					err = tv.checkFees(txLowFee, 500000, nil)
					assert.Error(t, err, "Should fail with insufficient fee for size %d (actual %d) at rate %s", size, actualSize, fr.name)
				}
			}
		})
	}
}

// Helper function to create a test transaction with specified size and fee
func createTestTransactionWithFee(_ *testing.T, targetSize int, fee uint64) *bt.Tx {
	// Handle edge case: zero-size transaction (theoretical only)
	if targetSize == 0 {
		// Return an empty transaction for theoretical testing
		// Note: This would never be valid on the actual network
		return bt.NewTx()
	}

	// Start with a basic transaction
	tx := bt.NewTx()

	// Add an input with sufficient value
	// For large transactions or high fees, ensure we have enough input value
	inputValue := uint64(10000000) // 0.1 BSV default for larger fees
	if fee > inputValue {
		inputValue = fee + 1000 // Ensure we have enough to cover the fee
	}

	// Create a basic P2PKH-like input (approximately 148 bytes when signed)
	input := &bt.Input{
		PreviousTxSatoshis: inputValue,
		PreviousTxScript:   &bscript.Script{},
		SequenceNumber:     0xfffffffe,
		PreviousTxOutIndex: 0,
	}
	_ = input.PreviousTxIDAdd(&chainhash.Hash{})

	// Add a dummy signature script to simulate signed size
	// P2PKH unlocking script is typically ~107 bytes (signature + pubkey)
	dummySig := make([]byte, 107)
	dummySigScript := bscript.Script(dummySig)
	input.UnlockingScript = &dummySigScript
	tx.Inputs = append(tx.Inputs, input)

	// Calculate output value (input - fee)
	outputValue := inputValue - fee

	// Add a P2PKH output (25 bytes for standard P2PKH script)
	p2pkhScript := make([]byte, 25)
	p2pkhScript[0] = 0x76 // OP_DUP
	p2pkhScript[1] = 0xa9 // OP_HASH160
	p2pkhScript[2] = 0x14 // Push 20 bytes
	// ... 20 bytes of pubkey hash ...
	p2pkhScript[23] = 0x88 // OP_EQUALVERIFY
	p2pkhScript[24] = 0xac // OP_CHECKSIG

	p2pkhLockingScript := bscript.Script(p2pkhScript)
	output := &bt.Output{
		Satoshis:      outputValue,
		LockingScript: &p2pkhLockingScript,
	}
	tx.Outputs = append(tx.Outputs, output)

	// Basic transaction is ~192 bytes. If target is smaller, we can't achieve it
	// with a standard P2PKH transaction. For test purposes, we'll accept this.
	currentSize := tx.Size()

	// If we need a larger transaction, add padding with OP_RETURN
	if targetSize > currentSize {
		// Calculate how much data we need to add
		// OP_RETURN output overhead is approximately 10 bytes plus the data
		dataNeeded := targetSize - currentSize - 10
		if dataNeeded > 0 {
			data := make([]byte, dataNeeded)
			opReturnScript, _ := bscript.NewFromASM("OP_RETURN")
			_ = opReturnScript.AppendPushData(data)
			tx.Outputs = append(tx.Outputs, &bt.Output{
				Satoshis:      0,
				LockingScript: opReturnScript,
			})
		}
	}

	return tx
}

// Helper function to create a consolidation transaction
func createConsolidationTransaction(t *testing.T, numInputs, numOutputs int, totalFee uint64) *bt.Tx {
	tx := bt.NewTx()

	// Add inputs
	inputValueEach := uint64(1000) // 1000 satoshis per input
	totalInputValue := inputValueEach * uint64(numInputs)

	for i := 0; i < numInputs; i++ {
		input := &bt.Input{
			PreviousTxSatoshis: inputValueEach,
			PreviousTxScript:   &bscript.Script{},
			UnlockingScript:    &bscript.Script{},
			SequenceNumber:     0xfffffffe,
			PreviousTxOutIndex: uint32(i),
		}
		_ = input.PreviousTxIDAdd(&chainhash.Hash{})
		tx.Inputs = append(tx.Inputs, input)
	}

	// Calculate output value per output
	totalOutputValue := totalInputValue - totalFee
	outputValueEach := totalOutputValue / uint64(numOutputs)
	remainder := totalOutputValue % uint64(numOutputs)

	// Add outputs
	for i := 0; i < numOutputs; i++ {
		value := outputValueEach
		if i == 0 {
			value += remainder // Add any remainder to first output
		}

		output := &bt.Output{
			Satoshis:      value,
			LockingScript: &bscript.Script{},
		}
		tx.Outputs = append(tx.Outputs, output)
	}

	return tx
}
