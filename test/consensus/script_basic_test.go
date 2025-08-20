package consensus

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBasicScriptTests demonstrates basic script test loading and parsing
func TestBasicScriptTests(t *testing.T) {
	testFile := filepath.Join(GetTestDataPath(), "script_tests.json")
	tests := LoadScriptTests(t, testFile)

	t.Logf("Loaded %d script tests total", len(tests))

	// Test that we can parse various script formats
	parser := NewScriptParser()

	// Count different types of scripts
	stats := map[string]int{
		"empty":        0,
		"numeric":      0,
		"hex_prefixed": 0,
		"opcodes":      0,
		"mixed":        0,
		"parse_errors": 0,
	}

	// Sample first 100 tests
	sampleSize := 100
	if len(tests) < sampleSize {
		sampleSize = len(tests)
	}

	for i := 0; i < sampleSize; i++ {
		test := tests[i]

		// Categorize script type
		if test.ScriptSig == "" && test.ScriptPubKey == "" {
			stats["empty"]++
		} else if isNumericOnly(test.ScriptSig) || isNumericOnly(test.ScriptPubKey) {
			stats["numeric"]++
		} else if hasHexPrefix(test.ScriptSig) || hasHexPrefix(test.ScriptPubKey) {
			stats["hex_prefixed"]++
		} else if hasOpcodes(test.ScriptSig) || hasOpcodes(test.ScriptPubKey) {
			stats["opcodes"]++
		} else {
			stats["mixed"]++
		}

		// Try parsing both scripts
		if test.ScriptSig != "" {
			_, err := parser.ParseScript(test.ScriptSig)
			if err != nil {
				stats["parse_errors"]++
			}
		}

		if test.ScriptPubKey != "" {
			_, err := parser.ParseScript(test.ScriptPubKey)
			if err != nil && test.ScriptSig == "" { // Don't double count
				stats["parse_errors"]++
			}
		}
	}

	t.Logf("Script type statistics (first %d tests):", sampleSize)
	for k, v := range stats {
		t.Logf("  %s: %d", k, v)
	}

	// Test a few specific interesting scripts
	interestingTests := []struct {
		name         string
		scriptSig    string
		scriptPubKey string
	}{
		{
			name:         "Simple numeric",
			scriptSig:    "1",
			scriptPubKey: "1 EQUAL",
		},
		{
			name:         "Hex with opcode",
			scriptSig:    "0x51",
			scriptPubKey: "0x5f ADD 0x60 EQUAL",
		},
		{
			name:         "P2PKH template",
			scriptSig:    "",
			scriptPubKey: "DUP HASH160 0x14 0x89abcdefabbaabbaabbaabbaabbaabbaabbaabba EQUALVERIFY CHECKSIG",
		},
	}

	for _, tt := range interestingTests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.scriptSig != "" {
				sigBytes, err := parser.ParseScript(tt.scriptSig)
				require.NoError(t, err, "Failed to parse scriptSig")
				t.Logf("ScriptSig: %s -> %x", tt.scriptSig, sigBytes)
			}

			if tt.scriptPubKey != "" {
				pubKeyBytes, err := parser.ParseScript(tt.scriptPubKey)
				require.NoError(t, err, "Failed to parse scriptPubKey")
				t.Logf("ScriptPubKey: %s -> %x", tt.scriptPubKey, pubKeyBytes)
			}
		})
	}
}

// TestEmptyScripts tests handling of empty scripts
func TestEmptyScripts(t *testing.T) {
	testFile := filepath.Join(GetTestDataPath(), "script_tests.json")
	tests := LoadScriptTests(t, testFile)

	emptyCount := 0
	for _, test := range tests {
		if test.ScriptSig == "" && test.ScriptPubKey == "" {
			emptyCount++
		}
	}

	t.Logf("Found %d tests with both scripts empty out of %d total", emptyCount, len(tests))
}

// TestScriptStatistics shows statistics about the script tests
func TestScriptStatistics(t *testing.T) {
	testFile := filepath.Join(GetTestDataPath(), "script_tests.json")
	tests := LoadScriptTests(t, testFile)

	stats := map[string]int{
		"total":          len(tests),
		"ok_expected":    0,
		"err_expected":   0,
		"has_comment":    0,
		"p2sh_flag":      0,
		"strictenc_flag": 0,
		"utxo_flag":      0,
	}

	for _, test := range tests {
		if test.Expected == "OK" {
			stats["ok_expected"]++
		} else {
			stats["err_expected"]++
		}

		if test.Comment != "" {
			stats["has_comment"]++
		}

		for _, flag := range test.Flags {
			switch flag {
			case "P2SH":
				stats["p2sh_flag"]++
			case "STRICTENC":
				stats["strictenc_flag"]++
			case "UTXO_AFTER_GENESIS":
				stats["utxo_flag"]++
			}
		}
	}

	t.Logf("Script test statistics:")
	for k, v := range stats {
		if k != "total" {
			percentage := float64(v) / float64(stats["total"]) * 100
			t.Logf("  %s: %d (%.1f%%)", k, v, percentage)
		} else {
			t.Logf("  %s: %d", k, v)
		}
	}
}

// isNumericOnly checks if string contains only digits
func isNumericOnly(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// hasHexPrefix checks if string contains hex values with 0x prefix
func hasHexPrefix(s string) bool {
	return len(s) > 2 && (s[:2] == "0x" || s[:2] == "0X")
}

// hasOpcodes checks if string contains opcode names
func hasOpcodes(s string) bool {
	// Simple check for common opcodes
	opcodes := []string{"DUP", "HASH", "EQUAL", "VERIFY", "CHECK", "IF", "ELSE", "ENDIF", "ADD", "SUB"}
	for _, op := range opcodes {
		if contains(s, op) {
			return true
		}
	}
	return false
}

// contains checks if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && containsAt(s, substr, 0)
}

// containsAt checks if a string contains a substring at any position
func containsAt(s, substr string, start int) bool {
	for i := start; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// isPureHex was removed as it was causing issues - scripts like "1" are not hex strings
// parseSimpleHex was removed as it's not needed
