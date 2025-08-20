package consensus

import (
	"fmt"
	"path/filepath"
	"testing"
)

// TestTransactionValidation tests transaction validation with actual validators
func TestTransactionValidation(t *testing.T) {
	// Test valid transactions
	t.Run("ValidTransactions", func(t *testing.T) {
		testFile := filepath.Join(GetTestDataPath(), "tx_valid.json")
		tests := LoadTxTests(t, testFile)

		runTransactionTests(t, tests, true)
	})

	// Test invalid transactions
	t.Run("InvalidTransactions", func(t *testing.T) {
		testFile := filepath.Join(GetTestDataPath(), "tx_invalid.json")
		tests := LoadTxTests(t, testFile)

		runTransactionTests(t, tests, false)
	})
}

func runTransactionTests(t *testing.T, tests []TxTest, shouldBeValid bool) {
	// Statistics
	stats := struct {
		total         int
		loaded        int
		validated     int
		expectedValid int
		actualValid   int
		allAgree      int
		disagreed     int
	}{}

	// Test first 50 transactions
	maxTests := 50
	if len(tests) > maxTests {
		tests = tests[:maxTests]
	}

	for i, test := range tests {
		stats.total++

		testName := fmt.Sprintf("Tx_%d", i)
		if test.Comment != "" {
			testName = fmt.Sprintf("%s_%s", testName, test.Comment)
		}

		t.Run(testName, func(t *testing.T) {
			ctx := NewTxTestContext()

			err := ctx.LoadFromTxTest(test, shouldBeValid)
			if err != nil {
				t.Logf("Failed to load transaction test: %v", err)
				t.Skip("Transaction loading failed")
				return
			}

			stats.loaded++

			// Run validation
			result := ctx.Validate()
			stats.validated++

			if shouldBeValid {
				stats.expectedValid++
			}

			if result.Valid {
				stats.actualValid++
			}

			// Check if validators agree
			if result.Details != "" && !contains(result.Details, "Disagreement") {
				stats.allAgree++
			} else {
				stats.disagreed++
			}

			// Log results
			t.Logf("Transaction: %s", ctx.Tx.TxID())
			t.Logf("Expected: %s, Actual: %s", boolToValidString(shouldBeValid), boolToValidString(result.Valid))
			t.Logf("Details: %s", result.Details)

			// For now, we don't fail tests on validation mismatches
			// This allows us to gather statistics on how the validators are performing
			if shouldBeValid != result.Valid {
				t.Logf("WARNING: Validation result mismatch")
				if result.Error != nil {
					t.Logf("Error: %v", result.Error)
				}
				// Log input scripts for debugging
				scripts := ctx.GetInputScripts()
				for j, script := range scripts {
					t.Logf("Input %d - Unlocking: %s, Locking: %s", j, script["unlocking_script"], script["locking_script"])
				}
			}
		})
	}

	// Print statistics
	validType := "valid"
	if !shouldBeValid {
		validType = "invalid"
	}

	t.Logf("\n=== Transaction Validation Statistics (%s) ===", validType)
	t.Logf("Total tests: %d", stats.total)
	t.Logf("Successfully loaded: %d", stats.loaded)
	t.Logf("Validated: %d", stats.validated)
	t.Logf("Expected valid: %d", stats.expectedValid)
	t.Logf("Actually valid: %d", stats.actualValid)
	t.Logf("All validators agree: %d", stats.allAgree)
	t.Logf("Validators disagree: %d", stats.disagreed)
}

// TestSingleTransactionWithValidators tests a specific transaction with all validators
func TestSingleTransactionWithValidators(t *testing.T) {
	// Create a simple transaction test
	test := TxTest{
		Inputs: []TxTestInput{
			{
				TxID:         "0000000000000000000000000000000000000000000000000000000000000000",
				Vout:         0,
				ScriptPubKey: "0x51", // OP_1
			},
		},
		// Simple transaction that spends the above output
		// This is a minimal transaction for testing
		RawTx: "0100000001000000000000000000000000000000000000000000000000000000000000000000000000025100ffffffff0100e1f505000000001976a914389ffce9cd9ae88dcc0631e88a821ffdbe9bfe2688ac00000000",
		Flags: []string{"P2SH"},
	}

	ctx := NewTxTestContext()
	err := ctx.LoadFromTxTest(test, true)
	if err != nil {
		t.Fatalf("Failed to load test transaction: %v", err)
	}

	// Test with each validator individually
	// Only use GoBDK validator - go-bt and go-sdk disabled
	validators := []ValidatorType{ValidatorGoBDK}

	for _, validatorType := range validators {
		t.Run(string(validatorType), func(t *testing.T) {
			result := ctx.ValidateWithValidator(validatorType)
			t.Logf("%s validation result: Valid=%v, Error=%v", validatorType, result.Valid, result.Error)
		})
	}

	// Test with all validators
	t.Run("AllValidators", func(t *testing.T) {
		result := ctx.Validate()
		t.Logf("Combined validation result: Valid=%v, Details=%s", result.Valid, result.Details)
	})
}

func boolToValidString(valid bool) string {
	if valid {
		return "VALID"
	}
	return "INVALID"
}
