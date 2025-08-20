package consensus

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScriptParser_BasicOpcodes(t *testing.T) {
	parser := NewScriptParser()

	tests := []struct {
		name     string
		script   string
		expected string // hex encoded expected result
	}{
		{
			name:     "Simple ADD",
			script:   "ADD",
			expected: "93",
		},
		{
			name:     "DUP HASH160",
			script:   "DUP HASH160",
			expected: "76a9",
		},
		{
			name:     "Multiple opcodes",
			script:   "1 2 ADD 3 EQUAL",
			expected: "5152935387",
		},
		{
			name:     "OP_RETURN",
			script:   "RETURN",
			expected: "6a",
		},
		{
			name:     "Numeric literals",
			script:   "0 1 2 16",
			expected: "00515260",
		},
		{
			name:     "Empty script",
			script:   "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parser.ParseScript(tt.script)
			require.NoError(t, err)

			actual := hex.EncodeToString(result)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestScriptParser_HexValues(t *testing.T) {
	parser := NewScriptParser()

	tests := []struct {
		name     string
		script   string
		expected string
	}{
		{
			name:     "Simple hex",
			script:   "0x51",
			expected: "0151",
		},
		{
			name:     "Hex with opcode",
			script:   "0x51 ADD 0x60",
			expected: "0151930160",
		},
		{
			name:     "Multi-byte hex",
			script:   "0x0100",
			expected: "020100",
		},
		{
			name:     "Empty hex",
			script:   "0x",
			expected: "00",
		},
		{
			name:     "Odd length hex",
			script:   "0x5",
			expected: "0105",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parser.ParseScript(tt.script)
			require.NoError(t, err, "Failed to parse script: %s", tt.script)

			actual := hex.EncodeToString(result)
			assert.Equal(t, tt.expected, actual, "Script: %s", tt.script)
		})
	}
}

func TestScriptParser_Numbers(t *testing.T) {
	parser := NewScriptParser()

	tests := []struct {
		name     string
		script   string
		expected string
	}{
		{
			name:     "Zero",
			script:   "0",
			expected: "00",
		},
		{
			name:     "One",
			script:   "1",
			expected: "51",
		},
		{
			name:     "Sixteen",
			script:   "16",
			expected: "60",
		},
		{
			name:     "Negative one",
			script:   "-1",
			expected: "4f",
		},
		{
			name:     "Large number",
			script:   "100",
			expected: "0164", // Push 1 byte: 0x64 (100)
		},
		{
			name:     "Large negative",
			script:   "-100",
			expected: "01e4", // Push 1 byte: 0xe4 (100 with sign bit)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parser.ParseScript(tt.script)
			require.NoError(t, err)

			actual := hex.EncodeToString(result)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestScriptParser_Strings(t *testing.T) {
	parser := NewScriptParser()

	tests := []struct {
		name     string
		script   string
		expected string
	}{
		{
			name:     "Simple string",
			script:   "'A'",
			expected: "0141",
		},
		{
			name:     "Multi-char string",
			script:   "'Az'",
			expected: "02417a",
		},
		{
			name:     "String with opcode",
			script:   "'Az' EQUAL",
			expected: "02417a87",
		},
		{
			name:     "Empty string",
			script:   "''",
			expected: "00",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parser.ParseScript(tt.script)
			require.NoError(t, err)

			actual := hex.EncodeToString(result)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestScriptParser_PushData(t *testing.T) {
	parser := NewScriptParser()

	tests := []struct {
		name     string
		script   string
		expected string
	}{
		{
			name:     "PUSHDATA1",
			script:   "PUSHDATA1 0x01 0x07",
			expected: "4c0107",
		},
		{
			name:     "PUSHDATA2",
			script:   "PUSHDATA2 0x0100 0x08",
			expected: "4d010008",
		},
		{
			name:     "PUSHDATA4",
			script:   "PUSHDATA4 0x01000000 0x09",
			expected: "4e0100000009",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parser.ParseScript(tt.script)
			require.NoError(t, err)

			actual := hex.EncodeToString(result)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestScriptParser_ComplexScripts(t *testing.T) {
	parser := NewScriptParser()

	tests := []struct {
		name     string
		script   string
		expected string
	}{
		{
			name:     "P2PKH template",
			script:   "DUP HASH160 0x14 0x89abcdefabbaabbaabbaabbaabbaabbaabbaabba EQUALVERIFY CHECKSIG",
			expected: "76a91489abcdefabbaabbaabbaabbaabbaabbaabbaabba88ac",
		},
		{
			name:     "Complex with numbers",
			script:   "1 2 ADD 3 EQUAL",
			expected: "5152935387",
		},
		{
			name:     "Mixed format",
			script:   "0x51 ADD 0x60 EQUAL",
			expected: "015193016087",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parser.ParseScript(tt.script)
			require.NoError(t, err)

			actual := hex.EncodeToString(result)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestScriptParser_RealScriptTests(t *testing.T) {
	parser := NewScriptParser()

	// Test some real scripts from the script_tests.json
	tests := []struct {
		name   string
		script string
	}{
		{
			name:   "DEPTH test",
			script: "DEPTH 0 EQUAL",
		},
		{
			name:   "EQUALVERIFY test",
			script: "2 EQUALVERIFY 1 EQUAL",
		},
		{
			name:   "IF block",
			script: "IF 1 ENDIF",
		},
		{
			name:   "Hex with ADD",
			script: "0x51 0x5f ADD 0x60 EQUAL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parser.ParseScript(tt.script)
			require.NoError(t, err, "Failed to parse: %s", tt.script)

			t.Logf("Script: %s", tt.script)
			t.Logf("Result: %x", result)

			// Basic validation - should produce some bytecode
			assert.NotEmpty(t, result, "Should produce non-empty bytecode")
		})
	}
}

func TestScriptParser_ErrorCases(t *testing.T) {
	parser := NewScriptParser()

	tests := []struct {
		name        string
		script      string
		expectError bool
	}{
		{
			name:        "Invalid opcode",
			script:      "INVALID_OPCODE",
			expectError: true,
		},
		{
			name:        "Invalid hex",
			script:      "0xgg",
			expectError: true,
		},
		{
			name:        "Unclosed quote",
			script:      "'unclosed",
			expectError: true,
		},
		{
			name:        "Invalid PUSHDATA1",
			script:      "PUSHDATA1 0x02 0x01", // length mismatch
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parser.ParseScript(tt.script)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGetOpcodeValue(t *testing.T) {
	tests := []struct {
		name     string
		expected byte
		exists   bool
	}{
		{"ADD", 0x93, true},
		{"OP_ADD", 0x93, true},
		{"DUP", 0x76, true},
		{"HASH160", 0xa9, true},
		{"CHECKSIG", 0xac, true},
		{"INVALID", 0x00, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, err := GetOpcodeValue(tt.name)
			if tt.exists {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, value)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
