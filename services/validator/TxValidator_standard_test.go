package validator

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/stretchr/testify/assert"
)

func TestIsStandardInputScript(t *testing.T) {
	// Use a height after UAHF for standard tests
	postUAHFHeight := uint32(500000)
	uahfHeight := uint32(478559)

	tests := []struct {
		name     string
		script   *bscript.Script
		expected bool
	}{
		{
			name:     "nil script is not standard",
			script:   nil,
			expected: false,
		},
		{
			name:     "empty script is standard",
			script:   &bscript.Script{},
			expected: true,
		},
		{
			name:     "simple push data is standard",
			script:   &bscript.Script{0x03, 0x01, 0x02, 0x03}, // Push 3 bytes
			expected: true,
		},
		{
			name:     "OP_0 is standard",
			script:   &bscript.Script{0x00},
			expected: true,
		},
		{
			name:     "OP_1 through OP_16 are standard",
			script:   &bscript.Script{0x51, 0x52, 0x60}, // OP_1, OP_2, OP_16
			expected: true,
		},
		{
			name:     "OP_1NEGATE is standard",
			script:   &bscript.Script{0x4f},
			expected: true,
		},
		{
			name:     "OP_RESERVED is standard (matches SV node)",
			script:   &bscript.Script{0x50},
			expected: true,
		},
		{
			name:     "OP_PUSHDATA1 is standard",
			script:   &bscript.Script{0x4c, 0x02, 0xaa, 0xbb}, // PUSHDATA1 2 bytes
			expected: true,
		},
		{
			name:     "OP_PUSHDATA2 is standard",
			script:   &bscript.Script{0x4d, 0x02, 0x00, 0xaa, 0xbb}, // PUSHDATA2 2 bytes
			expected: true,
		},
		{
			name:     "OP_PUSHDATA4 is standard",
			script:   &bscript.Script{0x4e, 0x02, 0x00, 0x00, 0x00, 0xaa, 0xbb}, // PUSHDATA4 2 bytes
			expected: true,
		},
		{
			name:     "typical P2PKH unlock script is standard",
			script:   createP2PKHUnlockScript(),
			expected: true,
		},
		{
			name:     "script with OP_NOP (0x61) is not standard",
			script:   &bscript.Script{0x61}, // OP_NOP - first opcode after OP_16
			expected: false,
		},
		{
			name:     "script with OP_DUP is not standard",
			script:   &bscript.Script{0x76}, // OP_DUP
			expected: false,
		},
		{
			name:     "script with OP_HASH160 is not standard",
			script:   &bscript.Script{0xa9}, // OP_HASH160
			expected: false,
		},
		{
			name:     "script with OP_EQUAL is not standard",
			script:   &bscript.Script{0x87}, // OP_EQUAL
			expected: false,
		},
		{
			name:     "script with OP_CHECKSIG is not standard",
			script:   &bscript.Script{0xac}, // OP_CHECKSIG
			expected: false,
		},
		{
			name:     "script with arithmetic opcodes is not standard",
			script:   &bscript.Script{0x93}, // OP_ADD
			expected: false,
		},
		{
			name:     "script with flow control is not standard",
			script:   &bscript.Script{0x63}, // OP_IF
			expected: false,
		},
		{
			name:     "push with insufficient data is not standard",
			script:   &bscript.Script{0x05, 0x01, 0x02}, // Says push 5 bytes but only has 2
			expected: false,
		},
		{
			name:     "OP_PUSHDATA1 with insufficient length byte is not standard",
			script:   &bscript.Script{0x4c}, // Missing length byte
			expected: false,
		},
		{
			name:     "OP_PUSHDATA2 with insufficient length bytes is not standard",
			script:   &bscript.Script{0x4d, 0x02}, // Missing second length byte
			expected: false,
		},
		{
			name:     "OP_PUSHDATA4 with insufficient length bytes is not standard",
			script:   &bscript.Script{0x4e, 0x02, 0x00, 0x00}, // Missing fourth length byte
			expected: false,
		},
		{
			name:     "mixed push and non-push opcodes is not standard",
			script:   &bscript.Script{0x02, 0xaa, 0xbb, 0x76}, // Push 2 bytes then OP_DUP
			expected: false,
		},
		{
			name: "complex standard script with multiple pushes",
			script: &bscript.Script{
				0x00,                   // OP_0
				0x4f,                   // OP_1NEGATE
				0x51,                   // OP_1
				0x03, 0x01, 0x02, 0x03, // Push 3 bytes
				0x4c, 0x02, 0xaa, 0xbb, // PUSHDATA1 2 bytes
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isStandardInputScript(tt.script, postUAHFHeight, uahfHeight)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestIsStandardInputScript_PreUAHF tests that before UAHF height, all scripts are considered standard
func TestIsStandardInputScript_PreUAHF(t *testing.T) {
	// Use a height before UAHF
	preUAHFHeight := uint32(470000)
	uahfHeight := uint32(478559)

	tests := []struct {
		name     string
		script   *bscript.Script
		expected bool
	}{
		{
			name:     "nil script is not standard even pre-UAHF",
			script:   nil,
			expected: false,
		},
		{
			name:     "empty script is standard pre-UAHF",
			script:   &bscript.Script{},
			expected: true,
		},
		{
			name:     "script with OP_DUP is standard pre-UAHF",
			script:   &bscript.Script{0x76}, // OP_DUP
			expected: true,
		},
		{
			name:     "script with OP_CHECKSIG is standard pre-UAHF",
			script:   &bscript.Script{0xac}, // OP_CHECKSIG
			expected: true,
		},
		{
			name:     "script with arithmetic opcodes is standard pre-UAHF",
			script:   &bscript.Script{0x93}, // OP_ADD
			expected: true,
		},
		{
			name:     "complex non-push script is standard pre-UAHF",
			script:   &bscript.Script{0x76, 0xa9, 0x14, 0x87}, // OP_DUP OP_HASH160 OP_DATA_20 OP_EQUAL
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isStandardInputScript(tt.script, preUAHFHeight, uahfHeight)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// createP2PKHUnlockScript creates a typical P2PKH unlocking script (signature + pubkey)
func createP2PKHUnlockScript() *bscript.Script {
	// Typical P2PKH unlock: <sig> <pubkey>
	// Signature is ~71 bytes, pubkey is 33 bytes (compressed)
	script := bscript.Script{}

	// Push signature (71 bytes)
	script = append(script, 0x47) // Push 71 bytes
	for i := 0; i < 71; i++ {
		script = append(script, 0xaa)
	}

	// Push pubkey (33 bytes)
	script = append(script, 0x21) // Push 33 bytes
	for i := 0; i < 33; i++ {
		script = append(script, 0xbb)
	}

	return &script
}
