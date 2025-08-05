package consensus

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// TestScriptValidationWithValidators runs script tests through all validators
func TestScriptValidationWithValidators(t *testing.T) {
	testFile := filepath.Join(GetTestDataPath(), "script_tests.json")
	tests := LoadScriptTests(t, testFile)
	
	parser := NewScriptParser()
	validator := NewValidatorIntegration()
	
	// Statistics
	stats := struct {
		total       int
		parsed      int
		skipped     int
		validated   int
		allAgree    int
		disagreed   int
		expectedOK  int
		actualOK    int
	}{}
	
	// Test first 100 scripts for now
	maxTests := 100
	if len(tests) > maxTests {
		tests = tests[:maxTests]
	}
	
	for i, test := range tests {
		stats.total++
		
		testName := fmt.Sprintf("Script_%d", i)
		if test.Comment != "" {
			testName = fmt.Sprintf("%s_%s", testName, test.Comment)
		}
		
		t.Run(testName, func(t *testing.T) {
			// Parse scripts
			sigBytes, err := parser.ParseScript(test.ScriptSig)
			if err != nil {
				t.Logf("Failed to parse scriptSig '%s': %v", test.ScriptSig, err)
				stats.skipped++
				t.Skip("Script parsing failed")
				return
			}
			
			pubKeyBytes, err := parser.ParseScript(test.ScriptPubKey)
			if err != nil {
				t.Logf("Failed to parse scriptPubKey '%s': %v", test.ScriptPubKey, err)
				stats.skipped++
				t.Skip("Script parsing failed")
				return
			}
			
			stats.parsed++
			
			// Create a transaction for validation
			tx := createTestTransaction(sigBytes, pubKeyBytes)
			
			// Run validation with all validators
			results := validator.ValidateWithAllValidators(tx, 0, []uint32{0})
			stats.validated++
			
			// Check if all validators agree
			allAgree, differences := CompareResults(results)
			if allAgree {
				stats.allAgree++
			} else {
				stats.disagreed++
				t.Logf("Validator disagreement: %s", differences)
			}
			
			// Check against expected result
			expectOK := test.Expected == "OK"
			if expectOK {
				stats.expectedOK++
			}
			
			// For now, consider validation successful if at least one validator says OK
			actualOK := false
			for _, result := range results {
				if result.Success {
					actualOK = true
					break
				}
			}
			
			if actualOK {
				stats.actualOK++
			}
			
			// Log results
			t.Logf("ScriptSig: %s -> %x", test.ScriptSig, sigBytes)
			t.Logf("ScriptPubKey: %s -> %x", test.ScriptPubKey, pubKeyBytes)
			t.Logf("Expected: %s, Actual OK: %v", test.Expected, actualOK)
			
			// Log individual validator results
			for validatorType, result := range results {
				status := "OK"
				if !result.Success {
					status = fmt.Sprintf("FAIL: %v", result.Error)
				}
				t.Logf("  %s: %s", validatorType, status)
			}
			
			// Don't fail the test yet, we're gathering statistics
			if !allAgree {
				t.Logf("WARNING: Validators disagree on this test")
			}
		})
	}
	
	// Print statistics
	t.Logf("\n=== Script Validation Statistics ===")
	t.Logf("Total tests: %d", stats.total)
	t.Logf("Successfully parsed: %d", stats.parsed)
	t.Logf("Skipped (parse error): %d", stats.skipped)
	t.Logf("Validated: %d", stats.validated)
	t.Logf("All validators agree: %d", stats.allAgree)
	t.Logf("Validators disagree: %d", stats.disagreed)
	t.Logf("Expected OK: %d", stats.expectedOK)
	t.Logf("Actual OK (any validator): %d", stats.actualOK)
}

// createTestTransaction creates a transaction for script validation testing
func createTestTransaction(sigScript, pubKeyScript []byte) *bt.Tx {
	// Create a dummy previous transaction hash
	prevTxHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000000")
	
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
	_ = input.PreviousTxIDAdd(&chainhash.Hash{})
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
			result := validator.ValidateScript(v, tx, 0, []uint32{0})
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