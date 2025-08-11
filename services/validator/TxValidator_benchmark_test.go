package validator

import (
	"testing"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
)

// BenchmarkIsConsolidationTxForFees measures the performance of the simplified consolidation check
func BenchmarkIsConsolidationTxForFees(b *testing.B) {
	policy := settings.NewPolicySettings()
	policy.MinConsolidationFactor = 20

	tv := &TxValidator{
		logger: ulogger.TestLogger{},
		settings: &settings.Settings{
			Policy:         policy,
			ChainCfgParams: &chaincfg.MainNetParams,
		},
	}

	// Create a consolidation transaction with 100 inputs and 5 outputs
	tx := createBenchmarkConsolidationTx(100, 5)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tv.isConsolidationTx(tx, nil, 0)
	}
}

// BenchmarkIsConsolidationTxFull measures the full consolidation validation with all checks
func BenchmarkIsConsolidationTxFull(b *testing.B) {
	policy := settings.NewPolicySettings()
	policy.MinConsolidationFactor = 20
	policy.MaxConsolidationInputScriptSize = 150
	policy.MinConfConsolidationInput = 6
	policy.AcceptNonStdConsolidationInput = false

	tv := &TxValidator{
		logger: ulogger.TestLogger{},
		settings: &settings.Settings{
			Policy:         policy,
			ChainCfgParams: &chaincfg.MainNetParams,
		},
	}

	benchmarks := []struct {
		name        string
		numInputs   int
		numOutputs  int
		scriptSize  int
		description string
	}{
		{
			name:        "small_consolidation_20_inputs",
			numInputs:   20,
			numOutputs:  1,
			scriptSize:  107, // Standard P2PKH unlock
			description: "Minimum consolidation size",
		},
		{
			name:        "medium_consolidation_100_inputs",
			numInputs:   100,
			numOutputs:  5,
			scriptSize:  107,
			description: "Typical consolidation",
		},
		{
			name:        "large_consolidation_1000_inputs",
			numInputs:   1000,
			numOutputs:  50,
			scriptSize:  107,
			description: "Large consolidation",
		},
		{
			name:        "max_script_size_consolidation",
			numInputs:   50,
			numOutputs:  2,
			scriptSize:  150, // Maximum allowed script size
			description: "Consolidation with max script sizes",
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			tx := createBenchmarkConsolidationTxWithScriptSize(bm.numInputs, bm.numOutputs, bm.scriptSize)
			utxoHeights := make([]uint32, bm.numInputs)
			// All inputs have sufficient confirmations
			for i := range utxoHeights {
				utxoHeights[i] = 490000
			}
			currentHeight := uint32(500000)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = tv.isConsolidationTx(tx, utxoHeights, currentHeight)
			}
		})
	}
}

// BenchmarkIsStandardInputScript measures the performance of script validation
func BenchmarkIsStandardInputScript(b *testing.B) {
	uahfHeight := uint32(478559)
	postUAHFHeight := uint32(500000)

	benchmarks := []struct {
		name   string
		script *bscript.Script
	}{
		{
			name:   "empty_script",
			script: &bscript.Script{},
		},
		{
			name:   "standard_p2pkh_unlock",
			script: createP2PKHUnlockScript(),
		},
		{
			name: "complex_push_only",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				// Multiple push operations
				for i := 0; i < 10; i++ {
					*s = append(*s, 0x14)                // OP_DATA_20
					*s = append(*s, make([]byte, 20)...) // 20 bytes of data
				}
				return s
			}(),
		},
		{
			name: "non_standard_with_ops",
			script: &bscript.Script{
				0x76, // OP_DUP
				0xa9, // OP_HASH160
				0x14, // OP_DATA_20
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = isStandardInputScript(bm.script, postUAHFHeight, uahfHeight)
			}
		})
	}
}

// BenchmarkConsolidationValidationImpact compares validation with and without consolidation checks
func BenchmarkConsolidationValidationImpact(b *testing.B) {
	policy := settings.NewPolicySettings()
	policy.MinConsolidationFactor = 20
	policy.MinMiningTxFee = 0.5 // 0.5 satoshis per byte

	tv := &TxValidator{
		logger: ulogger.TestLogger{},
		settings: &settings.Settings{
			Policy:         policy,
			ChainCfgParams: &chaincfg.MainNetParams,
		},
		interpreter: &scriptVerifierGoBDK{},
	}

	// Create transactions with varying characteristics
	benchmarks := []struct {
		name            string
		tx              *bt.Tx
		isConsolidation bool
	}{
		{
			name:            "regular_tx_2_inputs_2_outputs",
			tx:              createBenchmarkConsolidationTx(2, 2),
			isConsolidation: false,
		},
		{
			name:            "consolidation_tx_20_inputs_1_output",
			tx:              createBenchmarkConsolidationTx(20, 1),
			isConsolidation: true,
		},
		{
			name:            "large_regular_tx_10_inputs_10_outputs",
			tx:              createBenchmarkConsolidationTx(10, 10),
			isConsolidation: false,
		},
		{
			name:            "large_consolidation_100_inputs_5_outputs",
			tx:              createBenchmarkConsolidationTx(100, 5),
			isConsolidation: true,
		},
	}

	b.Run("checkFees_with_consolidation_check", func(b *testing.B) {
		for _, bm := range benchmarks {
			b.Run(bm.name, func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_ = tv.checkFees(bm.tx, 500000, nil)
				}
			})
		}
	})

	// Simulate old behavior without consolidation check
	b.Run("checkFees_without_consolidation_check", func(b *testing.B) {
		for _, bm := range benchmarks {
			b.Run(bm.name, func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					// Simulate old checkFees without consolidation check
					_ = checkFeesWithoutConsolidation(bm.tx, policy.GetMinMiningTxFee())
				}
			})
		}
	})
}

// Helper function to create a benchmark consolidation transaction
func createBenchmarkConsolidationTx(numInputs, numOutputs int) *bt.Tx {
	tx := bt.NewTx()

	// Add inputs with standard P2PKH unlocking scripts
	for i := 0; i < numInputs; i++ {
		input := &bt.Input{
			PreviousTxSatoshis: 1000,
			PreviousTxScript:   createP2PKHLockingScript(),
			UnlockingScript:    createP2PKHUnlockScript(),
			SequenceNumber:     0xfffffffe,
			PreviousTxOutIndex: uint32(i),
		}
		_ = input.PreviousTxIDAdd(&chainhash.Hash{byte(i)})
		tx.Inputs = append(tx.Inputs, input)
	}

	// Add outputs
	satoshisPerOutput := uint64(numInputs * 900 / numOutputs) // Leave some for fees
	for i := 0; i < numOutputs; i++ {
		tx.Outputs = append(tx.Outputs, &bt.Output{
			Satoshis:      satoshisPerOutput,
			LockingScript: &bscript.Script{},
		})
	}

	return tx
}

// Helper function to create a transaction with specific script sizes
func createBenchmarkConsolidationTxWithScriptSize(numInputs, numOutputs, scriptSize int) *bt.Tx {
	tx := bt.NewTx()

	// Create unlocking script of specific size
	unlockScript := make(bscript.Script, 0, scriptSize)
	// Start with signature push
	unlockScript = append(unlockScript, 0x47) // OP_DATA_71 (typical sig size)
	sigData := make([]byte, 71)
	sigData[70] = 0x01 // SIGHASH_ALL
	unlockScript = append(unlockScript, sigData...)

	// Add padding to reach desired size
	remaining := scriptSize - len(unlockScript) - 2
	if remaining > 0 {
		unlockScript = append(unlockScript, byte(remaining))
		unlockScript = append(unlockScript, make([]byte, remaining)...)
	}

	// Add inputs
	for i := 0; i < numInputs; i++ {
		input := &bt.Input{
			PreviousTxSatoshis: 1000,
			PreviousTxScript:   createP2PKHLockingScript(),
			UnlockingScript:    &unlockScript,
			SequenceNumber:     0xfffffffe,
			PreviousTxOutIndex: uint32(i),
		}
		_ = input.PreviousTxIDAdd(&chainhash.Hash{byte(i)})
		tx.Inputs = append(tx.Inputs, input)
	}

	// Add outputs
	satoshisPerOutput := uint64(numInputs * 900 / numOutputs)
	for i := 0; i < numOutputs; i++ {
		tx.Outputs = append(tx.Outputs, &bt.Output{
			Satoshis:      satoshisPerOutput,
			LockingScript: &bscript.Script{},
		})
	}

	return tx
}

// Simplified version of checkFees without consolidation check for comparison
func checkFeesWithoutConsolidation(tx *bt.Tx, minFeeRate float64) error {
	// Just calculate standard fees without consolidation exemption
	// Estimate tx size (simplified)
	txSize := 10                   // overhead
	txSize += len(tx.Inputs) * 148 // typical input size
	txSize += len(tx.Outputs) * 34 // typical output size

	// Calculate required fee
	requiredFee := uint64(float64(txSize) * minFeeRate)

	// Simulate basic fee validation
	totalIn := tx.TotalInputSatoshis()
	totalOut := tx.TotalOutputSatoshis()

	if totalIn < totalOut+requiredFee {
		return errors.NewProcessingError("insufficient fee")
	}

	return nil
}
