package consensus

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// TestScriptValidationSimple tests basic script validation with simple test vectors
func TestScriptValidationSimple(t *testing.T) {
	testFile := filepath.Join(GetTestDataPath(), "script_tests.json")
	tests := LoadScriptTests(t, testFile)

	require.NotEmpty(t, tests, "No tests loaded from %s", testFile)
	t.Logf("Loaded %d script tests", len(tests))

	// Count different types of tests
	validTests := 0

	for _, test := range tests {
		// Skip comment entries
		if test.ScriptSig != "" || test.ScriptPubKey != "" {
			validTests++
		}
	}

	t.Logf("Found %d valid script tests", validTests)

	// Run first several tests
	runCount := 0
	maxRun := 20 // Increased to test more cases

	for i, test := range tests {
		if runCount >= maxRun {
			break
		}

		// Skip comment entries
		if test.ScriptSig == "" && test.ScriptPubKey == "" {
			continue
		}

		testName := fmt.Sprintf("Test_%d", i)
		if test.Comment != "" {
			testName = fmt.Sprintf("%s_%s", testName, test.Comment)
		}

		t.Run(testName, func(t *testing.T) {
			t.Logf("ScriptSig: %s", test.ScriptSig)
			t.Logf("ScriptPubKey: %s", test.ScriptPubKey)
			t.Logf("Expected: %s", test.Expected)

			// Parse the scripts using the script parser
			parser := NewScriptParser()

			sigBytes, err := parser.ParseScript(test.ScriptSig)
			require.NoError(t, err, "Failed to parse scriptSig")
			sigScript := bscript.Script(sigBytes)

			pubKeyBytes, err := parser.ParseScript(test.ScriptPubKey)
			require.NoError(t, err, "Failed to parse scriptPubKey")
			pubKeyScript := bscript.Script(pubKeyBytes)

			// Create a test transaction
			tx := CreateExtendedTx()

			// Set up the previous transaction ID for the input
			// Use a dummy transaction ID (32 zero bytes)
			prevTxIDBytes := make([]byte, 32)
			prevTxID, _ := chainhash.NewHash(prevTxIDBytes)
			_ = tx.Inputs[0].PreviousTxIDAdd(prevTxID)

			// Set up the scripts
			tx.Inputs[0].UnlockingScript = &sigScript
			tx.Outputs[0].LockingScript = &pubKeyScript
			tx.Inputs[0].PreviousTxScript = &pubKeyScript

			// Ensure the input has the correct output index
			tx.Inputs[0].PreviousTxOutIndex = 0

			// Validate the script
			validatorIntegration := NewValidatorIntegration()

			// Use block height 1 (after genesis) and UTXO height 0 for the single input
			blockHeight := uint32(1)
			utxoHeights := []uint32{0} // One height per input

			// Validate with the GoBDK validator
			result := validatorIntegration.ValidateScript(ValidatorGoBDK, tx, blockHeight, utxoHeights)

			// Check if the result matches expectations
			if test.Expected == "OK" {
				if !result.Success {
					t.Errorf("Expected script to validate successfully, but got error: %v", result.Error)
				}
			} else {
				if result.Success {
					t.Errorf("Expected script to fail validation with %s, but it succeeded", test.Expected)
				} else {
					// Log the error for debugging
					t.Logf("Script failed as expected with error: %v", result.Error)
				}
			}
		})

		runCount++
	}
}

// TestLoadScriptTests verifies the test loader works correctly
func TestLoadScriptTests(t *testing.T) {
	testFile := filepath.Join(GetTestDataPath(), "script_tests.json")
	tests := LoadScriptTests(t, testFile)

	require.NotEmpty(t, tests, "Should load some tests")

	// Check first non-comment test
	var firstTest *ScriptTest
	for _, test := range tests {
		if test.ScriptSig != "" || test.ScriptPubKey != "" {
			firstTest = &test
			break
		}
	}

	require.NotNil(t, firstTest, "Should find at least one non-comment test")
	require.NotEmpty(t, firstTest.Expected, "Test should have expected result")

	t.Logf("First test: sig=%q pubkey=%q expected=%q",
		firstTest.ScriptSig, firstTest.ScriptPubKey, firstTest.Expected)
}
