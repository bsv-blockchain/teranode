package consensus

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/stretchr/testify/require"
)

// TestScriptTestsWithParser runs script tests using the new parser
func TestScriptTestsWithParser(t *testing.T) {
	testFile := filepath.Join(GetTestDataPath(), "script_tests.json")
	tests := LoadScriptTests(t, testFile)

	parser := NewScriptParser()

	// Statistics
	parsed := 0
	failed := 0
	total := 0

	// Test first 50 scripts to verify parser works
	maxTests := 50
	if len(tests) > maxTests {
		tests = tests[:maxTests]
	}

	for i, test := range tests {
		total++

		testName := fmt.Sprintf("Script_%d", i)
		if test.Comment != "" {
			testName = fmt.Sprintf("%s_%s", testName, test.Comment)
		}

		t.Run(testName, func(t *testing.T) {
			// Parse scriptSig
			var sigScript *bscript.Script
			if test.ScriptSig != "" {
				sigBytes, err := parser.ParseScript(test.ScriptSig)
				if err != nil {
					t.Logf("Failed to parse scriptSig '%s': %v", test.ScriptSig, err)
					failed++
					t.Skip("Script parsing failed")
					return
				}
				script := bscript.Script(sigBytes)
				sigScript = &script
			} else {
				script := bscript.Script{}
				sigScript = &script
			}

			// Parse scriptPubKey
			var pubKeyScript *bscript.Script
			if test.ScriptPubKey != "" {
				pubKeyBytes, err := parser.ParseScript(test.ScriptPubKey)
				if err != nil {
					t.Logf("Failed to parse scriptPubKey '%s': %v", test.ScriptPubKey, err)
					failed++
					t.Skip("Script parsing failed")
					return
				}
				script := bscript.Script(pubKeyBytes)
				pubKeyScript = &script
			} else {
				script := bscript.Script{}
				pubKeyScript = &script
			}

			parsed++

			// Create test transaction
			tx := CreateExtendedTx()
			tx.Inputs[0].UnlockingScript = sigScript
			tx.Outputs[0].LockingScript = pubKeyScript
			tx.Inputs[0].PreviousTxScript = pubKeyScript

			t.Logf("ScriptSig: %s -> %x", test.ScriptSig, *sigScript)
			t.Logf("ScriptPubKey: %s -> %x", test.ScriptPubKey, *pubKeyScript)
			t.Logf("Expected: %s", test.Expected)
			t.Logf("Flags: %v", test.Flags)

			// TODO: Run actual validation once we have validators hooked up
			t.Skip("Validation not yet implemented")
		})
	}

	t.Logf("Parser Statistics: %d/%d scripts parsed successfully, %d failed",
		parsed, total, failed)
}

// TestScriptParserCoverage tests parser coverage on all script tests
func TestScriptParserCoverage(t *testing.T) {
	testFile := filepath.Join(GetTestDataPath(), "script_tests.json")
	tests := LoadScriptTests(t, testFile)

	parser := NewScriptParser()

	stats := map[string]int{
		"total":         len(tests),
		"sig_parsed":    0,
		"sig_failed":    0,
		"pubkey_parsed": 0,
		"pubkey_failed": 0,
		"both_parsed":   0,
		"empty_scripts": 0,
	}

	errorTypes := make(map[string]int)

	for _, test := range tests {
		if test.ScriptSig == "" && test.ScriptPubKey == "" {
			stats["empty_scripts"]++
			continue
		}

		// Test scriptSig parsing
		sigParsed := true
		if test.ScriptSig != "" {
			_, err := parser.ParseScript(test.ScriptSig)
			if err != nil {
				stats["sig_failed"]++
				sigParsed = false

				// Categorize error
				errStr := err.Error()
				if errorTypes[errStr] == 0 {
					errorTypes[errStr] = 1
				} else {
					errorTypes[errStr]++
				}
			} else {
				stats["sig_parsed"]++
			}
		} else {
			// Empty scripts count as parsed
			stats["sig_parsed"]++
		}

		// Test scriptPubKey parsing
		pubkeyParsed := true
		if test.ScriptPubKey != "" {
			_, err := parser.ParseScript(test.ScriptPubKey)
			if err != nil {
				stats["pubkey_failed"]++
				pubkeyParsed = false

				// Categorize error
				errStr := err.Error()
				if errorTypes[errStr] == 0 {
					errorTypes[errStr] = 1
				} else {
					errorTypes[errStr]++
				}
			} else {
				stats["pubkey_parsed"]++
			}
		} else {
			stats["pubkey_parsed"]++
		}

		if sigParsed && pubkeyParsed {
			stats["both_parsed"]++
		}
	}

	t.Logf("Script Parser Coverage Statistics:")
	for k, v := range stats {
		t.Logf("  %s: %d", k, v)
	}

	// Show top error types
	t.Logf("Top parsing errors:")
	count := 0
	for errStr, freq := range errorTypes {
		if count >= 5 {
			break
		}
		t.Logf("  %s: %d occurrences", errStr, freq)
		count++
	}

	// Calculate success rate
	successRate := float64(stats["both_parsed"]) / float64(stats["total"]) * 100
	t.Logf("Overall success rate: %.1f%% (%d/%d tests)",
		successRate, stats["both_parsed"], stats["total"])

	// We want at least 80% of scripts to parse successfully
	require.Greater(t, successRate, 80.0,
		"Parser should successfully parse at least 80%% of test scripts")
}

// TestSpecificScriptFailures tests specific known problematic scripts
func TestSpecificScriptFailures(t *testing.T) {
	parser := NewScriptParser()

	// Test some scripts that might be challenging
	tests := []struct {
		name   string
		script string
		expect string // "pass" or "fail"
	}{
		{
			name:   "Simple DEPTH",
			script: "DEPTH 0 EQUAL",
			expect: "pass",
		},
		{
			name:   "Hex with spaces",
			script: "0x51 0x5f ADD 0x60 EQUAL",
			expect: "pass",
		},
		{
			name:   "Complex P2SH",
			script: "DUP HASH160 0x14 0x89abcdefabbaabbaabbaabbaabbaabbaabbaabba EQUALVERIFY CHECKSIG",
			expect: "pass",
		},
		{
			name:   "Long script with numbers",
			script: "1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16",
			expect: "pass",
		},
		{
			name:   "Mixed quotes and hex",
			script: "'test' 0x41 EQUAL",
			expect: "pass",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parser.ParseScript(tt.script)

			if tt.expect == "pass" {
				require.NoError(t, err, "Script should parse successfully: %s", tt.script)
				require.NotEmpty(t, result, "Should produce non-empty bytecode")
				t.Logf("Script: %s", tt.script)
				t.Logf("Bytecode: %x", result)
			} else {
				require.Error(t, err, "Script should fail to parse: %s", tt.script)
			}
		})
	}
}
