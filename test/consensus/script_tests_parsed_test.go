package consensus

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
