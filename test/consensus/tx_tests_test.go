package consensus

import (
	"encoding/hex"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTransactionTestFramework tests the transaction test framework
func TestTransactionTestFramework(t *testing.T) {
	// Load valid transaction tests
	testFile := filepath.Join(GetTestDataPath(), "tx_valid.json")
	tests := LoadTxTests(t, testFile)
	
	require.NotEmpty(t, tests, "Should load some transaction tests")
	t.Logf("Loaded %d valid transaction tests", len(tests))
	
	// Test first few transactions
	maxTests := 5
	if len(tests) > maxTests {
		tests = tests[:maxTests]
	}
	
	for i, test := range tests {
		testName := fmt.Sprintf("ValidTx_%d", i)
		if test.Comment != "" {
			testName = fmt.Sprintf("%s_%s", testName, test.Comment)
		}
		
		t.Run(testName, func(t *testing.T) {
			ctx := NewTxTestContext()
			
			err := ctx.LoadFromTxTest(test, true)
			require.NoError(t, err, "Failed to load transaction test")
			
			// Verify transaction was loaded properly
			assert.NotNil(t, ctx.Tx, "Transaction should be loaded")
			assert.Equal(t, len(test.Inputs), len(ctx.Tx.Inputs),
				"Input count should match")
			
			// Check that transaction is extended
			assert.True(t, ctx.IsExtended(), "Transaction should be extended")
			
			// Get transaction info
			info := ctx.GetTransactionInfo()
			t.Logf("Transaction info: %+v", info)
			
			// Get script info
			scripts := ctx.GetInputScripts()
			for j, script := range scripts {
				t.Logf("Input %d scripts: %+v", j, script)
			}
			
			// Run validation (placeholder for now)
			result := ctx.Validate()
			t.Logf("Validation result: %+v", result)
		})
	}
}

// TestTransactionTestInvalid tests invalid transaction loading
func TestTransactionTestInvalid(t *testing.T) {
	// Load invalid transaction tests
	testFile := filepath.Join(GetTestDataPath(), "tx_invalid.json")
	tests := LoadTxTests(t, testFile)
	
	require.NotEmpty(t, tests, "Should load some invalid transaction tests")
	t.Logf("Loaded %d invalid transaction tests", len(tests))
	
	// Test first few invalid transactions
	maxTests := 5
	if len(tests) > maxTests {
		tests = tests[:maxTests]
	}
	
	validParsed := 0
	invalidParsed := 0
	
	for i, test := range tests {
		testName := fmt.Sprintf("InvalidTx_%d", i)
		if test.Comment != "" {
			testName = fmt.Sprintf("%s_%s", testName, test.Comment)
		}
		
		t.Run(testName, func(t *testing.T) {
			ctx := NewTxTestContext()
			
			err := ctx.LoadFromTxTest(test, false)
			if err != nil {
				t.Logf("Failed to load invalid transaction (expected): %v", err)
				invalidParsed++
				return
			}
			
			validParsed++
			
			// Some invalid transactions can still be parsed (they're invalid for other reasons)
			assert.NotNil(t, ctx.Tx, "Transaction should be loaded")
			
			// Get transaction info
			info := ctx.GetTransactionInfo()
			t.Logf("Invalid transaction info: %+v", info)
		})
	}
	
	t.Logf("Invalid transaction parsing: %d successfully parsed, %d failed to parse",
		validParsed, invalidParsed)
}

// TestTransactionCreation tests creating transactions from scratch
func TestTransactionCreation(t *testing.T) {
	tests := []struct {
		name    string
		inputs  []TxTestInput
		outputs []TestOutput
	}{
		{
			name: "Simple P2PKH",
			inputs: []TxTestInput{
				{
					TxID:         "0000000000000000000000000000000000000000000000000000000000000001",
					Vout:         0,
					ScriptPubKey: "DUP HASH160 0x14 0x89abcdefabbaabbaabbaabbaabbaabbaabbaabba EQUALVERIFY CHECKSIG",
				},
			},
			outputs: []TestOutput{
				{
					Satoshis: 99000000,
					Script:   "DUP HASH160 0x14 0x12345678901234567890123456789012345678901234567890 EQUALVERIFY CHECKSIG",
				},
			},
		},
		{
			name: "Multi-input transaction",
			inputs: []TxTestInput{
				{
					TxID:         "0000000000000000000000000000000000000000000000000000000000000001",
					Vout:         0,
					ScriptPubKey: "DUP HASH160 0x14 0x89abcdefabbaabbaabbaabbaabbaabbaabbaabba EQUALVERIFY CHECKSIG",
				},
				{
					TxID:         "0000000000000000000000000000000000000000000000000000000000000002",
					Vout:         1,
					ScriptPubKey: "DUP HASH160 0x14 0xfedcba0987654321fedcba0987654321fedcba09 EQUALVERIFY CHECKSIG",
				},
			},
			outputs: []TestOutput{
				{
					Satoshis: 199000000,
					Script:   "DUP HASH160 0x14 0x12345678901234567890123456789012345678901234567890 EQUALVERIFY CHECKSIG",
				},
			},
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx, err := CreateTestTransaction(tt.inputs, tt.outputs)
			require.NoError(t, err, "Failed to create test transaction")
			
			assert.NotNil(t, tx, "Transaction should be created")
			assert.Equal(t, len(tt.inputs), len(tx.Inputs), "Input count should match")
			assert.Equal(t, len(tt.outputs), len(tx.Outputs), "Output count should match")
			
			// Verify inputs are extended
			for i, input := range tx.Inputs {
				assert.NotNil(t, input.PreviousTxScript, "Input %d should have previous script", i)
				assert.Greater(t, input.PreviousTxSatoshis, uint64(0), "Input %d should have satoshi amount", i)
			}
			
			t.Logf("Created transaction: %s", tx.TxID())
			t.Logf("Size: %d bytes", tx.Size())
			t.Logf("Inputs: %d, Outputs: %d", len(tx.Inputs), len(tx.Outputs))
		})
	}
}

// TestTxTestStats shows statistics about the transaction test vectors
func TestTxTestStats(t *testing.T) {
	// Valid transactions
	validFile := filepath.Join(GetTestDataPath(), "tx_valid.json")
	validTests := LoadTxTests(t, validFile)
	
	// Invalid transactions
	invalidFile := filepath.Join(GetTestDataPath(), "tx_invalid.json")
	invalidTests := LoadTxTests(t, invalidFile)
	
	t.Logf("Transaction test statistics:")
	t.Logf("  Valid transactions: %d", len(validTests))
	t.Logf("  Invalid transactions: %d", len(invalidTests))
	t.Logf("  Total transactions: %d", len(validTests)+len(invalidTests))
	
	// Analyze input/output patterns
	analyzeInputOutputPatterns(t, "Valid", validTests)
	analyzeInputOutputPatterns(t, "Invalid", invalidTests)
	
	// Analyze flags
	analyzeFlagUsage(t, "Valid", validTests)
	analyzeFlagUsage(t, "Invalid", invalidTests)
}

func analyzeInputOutputPatterns(t *testing.T, category string, tests []TxTest) {
	inputCounts := make(map[int]int)
	outputCounts := make(map[int]int)
	
	for _, test := range tests {
		inputCounts[len(test.Inputs)]++
		
		// Parse transaction to get output count
		if txBytes, err := hex.DecodeString(test.RawTx); err == nil {
			if tx, err := bt.NewTxFromBytes(txBytes); err == nil {
				outputCounts[len(tx.Outputs)]++
			}
		}
	}
	
	t.Logf("%s transaction input patterns:", category)
	for inputs, count := range inputCounts {
		if count > 0 {
			t.Logf("  %d inputs: %d transactions", inputs, count)
		}
	}
	
	t.Logf("%s transaction output patterns:", category)
	for outputs, count := range outputCounts {
		if count > 0 {
			t.Logf("  %d outputs: %d transactions", outputs, count)
		}
	}
}

func analyzeFlagUsage(t *testing.T, category string, tests []TxTest) {
	flagCounts := make(map[string]int)
	
	for _, test := range tests {
		for _, flag := range test.Flags {
			flagCounts[flag]++
		}
	}
	
	t.Logf("%s transaction flag usage:", category)
	for flag, count := range flagCounts {
		if count > 0 {
			t.Logf("  %s: %d transactions", flag, count)
		}
	}
}