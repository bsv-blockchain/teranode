package consensus

import (
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// createTestTransaction creates a transaction for script validation testing
func createTestTransaction(sigScript, pubKeyScript []byte) *bt.Tx {
	// Create a dummy previous transaction hash
	prevTxHash, _ := chainhash.NewHashFromStr(testPrevTxID)

	// Create transaction
	tx := bt.NewTx()
	tx.Version = 1
	tx.LockTime = 0

	// Add input with the signature script
	input := &bt.Input{
		PreviousTxOutIndex: 0,
		UnlockingScript:    (*bscript.Script)(&sigScript),
		SequenceNumber:     0xffffffff,
		PreviousTxSatoshis: 100000000, // 1 BTC
		PreviousTxScript:   (*bscript.Script)(&pubKeyScript),
	}
	_ = input.PreviousTxIDAdd(prevTxHash)
	tx.Inputs = append(tx.Inputs, input)

	// Add a dummy output
	output := &bt.Output{
		Satoshis:      99999000, // Slightly less for fee
		LockingScript: &bscript.Script{},
	}
	tx.Outputs = append(tx.Outputs, output)

	return tx
}

// TestValidatorAvailability checks which validators are available
func TestValidatorAvailability(t *testing.T) {
	validator := NewValidatorIntegration()

	// Create a simple test transaction
	tx := bt.NewTx()
	tx.Version = 1
	tx.LockTime = 0

	// Add a simple input and output
	input := &bt.Input{
		PreviousTxOutIndex: 0,
		UnlockingScript:    &bscript.Script{},
		SequenceNumber:     0xffffffff,
		PreviousTxSatoshis: 100000000,
		PreviousTxScript:   &bscript.Script{},
	}
	prevTxHash, _ := chainhash.NewHashFromStr(testPrevTxID)
	_ = input.PreviousTxIDAdd(prevTxHash)
	tx.Inputs = append(tx.Inputs, input)

	tx.Outputs = append(tx.Outputs, &bt.Output{
		Satoshis:      99999000,
		LockingScript: &bscript.Script{},
	})

	// Test each validator
	// Only use GoBDK validator - go-bt and go-sdk disabled
	validators := []ValidatorType{ValidatorGoBDK}

	for _, v := range validators {
		t.Run(string(v), func(t *testing.T) {
			result := validator.ValidateTransaction(v, tx, 0, []uint32{0})
			if result.Error != nil && result.Error.Error() == fmt.Sprintf("%s validator not registered", v) {
				t.Skipf("%s validator not available", v)
			} else {
				t.Logf("%s validator is available", v)
			}
		})
	}
}

// TestSingleScriptWithAllValidators tests a specific script with all validators
func TestSingleScriptWithAllValidators(t *testing.T) {
	parser := NewScriptParser()
	validator := NewValidatorIntegration()

	// Test case: simple OP_1 OP_1 OP_EQUAL
	testCases := []struct {
		name         string
		scriptSig    string
		scriptPubKey string
		expected     bool
	}{
		{
			name:         "Simple equality",
			scriptSig:    "1",
			scriptPubKey: "1 EQUAL",
			expected:     true,
		},
		{
			name:         "Simple addition",
			scriptSig:    "1 1",
			scriptPubKey: "ADD 2 EQUAL",
			expected:     true,
		},
		{
			name:         "False equality",
			scriptSig:    "1",
			scriptPubKey: "2 EQUAL",
			expected:     false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Parse scripts
			sigBytes, err := parser.ParseScript(tc.scriptSig)
			require.NoError(t, err, "Failed to parse scriptSig")

			pubKeyBytes, err := parser.ParseScript(tc.scriptPubKey)
			require.NoError(t, err, "Failed to parse scriptPubKey")

			// Create transaction
			tx := createTestTransaction(sigBytes, pubKeyBytes)

			// Validate with all validators
			results := validator.ValidateWithAllValidators(tx, 0, []uint32{0})

			// Check results
			for validatorType, result := range results {
				if result.Error != nil && result.Error.Error() == fmt.Sprintf("%s validator not registered", validatorType) {
					t.Logf("%s: Not available", validatorType)
					continue
				}

				if tc.expected && !result.Success {
					t.Errorf("%s: Expected success but got error: %v", validatorType, result.Error)
				} else if !tc.expected && result.Success {
					t.Errorf("%s: Expected failure but got success", validatorType)
				} else {
					status := "FAIL"
					if result.Success {
						status = "OK"
					}
					t.Logf("%s: %s (as expected)", validatorType, status)
				}
			}
		})
	}
}

// TestParseHexOnlyScripts tests parsing of hex-only scripts
func TestParseHexOnlyScripts(t *testing.T) {
	parser := NewScriptParser()

	// Test hex values without 0x prefix (from script_tests.json)
	hexScripts := []string{
		"0x2e7068c3b4f2b7e7627c7473",
		"0x4c",
		"0x00",
	}

	for _, hexScript := range hexScripts {
		t.Run(hexScript, func(t *testing.T) {
			bytes, err := parser.ParseScript(hexScript)
			if err != nil {
				t.Errorf("Failed to parse hex script %s: %v", hexScript, err)
			} else {
				t.Logf("Parsed %s -> %x", hexScript, bytes)
			}
		})
	}
}
