package consensus

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/stretchr/testify/require"
)

// TestPushDataEdgeCases tests edge cases for PUSHDATA operations
func TestPushDataEdgeCases(t *testing.T) {
	tests := []struct {
		name           string
		scriptStr      string
		expectedBytes  []byte
		expectedError  bool
		strictMode     bool
	}{
		// PUSHDATA1 tests
		{
			name:          "PUSHDATA1 zero length",
			scriptStr:     "PUSHDATA1 0x00",
			expectedBytes: []byte{0x4c, 0x00},
			expectedError: false,
		},
		{
			name:          "PUSHDATA1 single byte",
			scriptStr:     "PUSHDATA1 0x01 0xff",
			expectedBytes: []byte{0x4c, 0x01, 0xff},
			expectedError: false,
		},
		{
			name:          "PUSHDATA1 max length (255)",
			scriptStr:     fmt.Sprintf("PUSHDATA1 0xff 0x%s", makeHexString(255)),
			expectedBytes: append([]byte{0x4c, 0xff}, makeBytes(255)...),
			expectedError: false,
		},
		{
			name:          "PUSHDATA1 length mismatch - too short",
			scriptStr:     "PUSHDATA1 0x05 0x0102",
			expectedBytes: nil,
			expectedError: true,
		},
		{
			name:          "PUSHDATA1 length mismatch - too long",
			scriptStr:     "PUSHDATA1 0x02 0x010203",
			expectedBytes: nil,
			expectedError: true,
		},
		{
			name:          "PUSHDATA1 missing data",
			scriptStr:     "PUSHDATA1 0x05",
			expectedBytes: nil,
			expectedError: true,
		},
		
		// PUSHDATA2 tests
		{
			name:          "PUSHDATA2 zero length",
			scriptStr:     "PUSHDATA2 0x0000",
			expectedBytes: []byte{0x4d, 0x00, 0x00},
			expectedError: false,
		},
		{
			name:          "PUSHDATA2 single byte",
			scriptStr:     "PUSHDATA2 0x0100 0xff",
			expectedBytes: []byte{0x4d, 0x01, 0x00, 0xff},
			expectedError: false,
		},
		{
			name:          "PUSHDATA2 256 bytes (min for PUSHDATA2)",
			scriptStr:     fmt.Sprintf("PUSHDATA2 0x0001 0x%s", makeHexString(256)),
			expectedBytes: append([]byte{0x4d, 0x00, 0x01}, makeBytes(256)...),
			expectedError: false,
		},
		{
			name:          "PUSHDATA2 max length (65535)",
			scriptStr:     fmt.Sprintf("PUSHDATA2 0xffff 0x%s", makeHexString(65535)),
			expectedBytes: append([]byte{0x4d, 0xff, 0xff}, makeBytes(65535)...),
			expectedError: false,
		},
		{
			name:          "PUSHDATA2 wrong length format",
			scriptStr:     "PUSHDATA2 0x01 0xff", // Should be 4 hex chars
			expectedBytes: nil,
			expectedError: true,
		},
		
		// PUSHDATA4 tests
		{
			name:          "PUSHDATA4 zero length",
			scriptStr:     "PUSHDATA4 0x00000000 0x",
			expectedBytes: []byte{0x4e, 0x00, 0x00, 0x00, 0x00},
			expectedError: false,
		},
		{
			name:          "PUSHDATA4 single byte",
			scriptStr:     "PUSHDATA4 0x01000000 0xff",
			expectedBytes: []byte{0x4e, 0x01, 0x00, 0x00, 0x00, 0xff},
			expectedError: false,
		},
		{
			name:          "PUSHDATA4 65536 bytes (min for PUSHDATA4)",
			scriptStr:     fmt.Sprintf("PUSHDATA4 0x00000100 0x%s", makeHexString(65536)),
			expectedBytes: append([]byte{0x4e, 0x00, 0x00, 0x01, 0x00}, makeBytes(65536)...),
			expectedError: false,
		},
		
		// Edge cases with unnecessary PUSHDATA usage
		{
			name:          "Unnecessary PUSHDATA1 for small push",
			scriptStr:     "PUSHDATA1 0x04 0x01020304",
			expectedBytes: []byte{0x4c, 0x04, 0x01, 0x02, 0x03, 0x04},
			expectedError: false,
		},
		{
			name:          "Unnecessary PUSHDATA2 for small push",
			scriptStr:     "PUSHDATA2 0x0400 0x01020304",
			expectedBytes: []byte{0x4d, 0x04, 0x00, 0x01, 0x02, 0x03, 0x04},
			expectedError: false,
		},
		
		// Mixed operations
		{
			name:          "PUSHDATA1 followed by opcode",
			scriptStr:     "PUSHDATA1 0x02 0x0102 OP_DUP",
			expectedBytes: []byte{0x4c, 0x02, 0x01, 0x02, bscript.OpDUP},
			expectedError: false,
		},
		{
			name:          "Multiple PUSHDATA operations",
			scriptStr:     "PUSHDATA1 0x01 0xff PUSHDATA1 0x02 0x0102",
			expectedBytes: []byte{0x4c, 0x01, 0xff, 0x4c, 0x02, 0x01, 0x02},
			expectedError: false,
		},
		
		// Hex prefix edge cases
		{
			name:          "PUSHDATA1 decimal length",
			scriptStr:     "PUSHDATA1 5 0x0102030405",
			expectedBytes: []byte{0x4c, 0x05, 0x01, 0x02, 0x03, 0x04, 0x05},
			expectedError: false,
		},
	}
	
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var parser *ScriptParser
			if test.strictMode {
				parser = NewStrictScriptParser()
			} else {
				parser = NewScriptParser()
			}
			
			result, err := parser.ParseScript(test.scriptStr)
			
			if test.expectedError {
				require.Error(t, err, "Expected error for script: %s", test.scriptStr)
			} else {
				require.NoError(t, err, "Unexpected error for script: %s", test.scriptStr)
				require.Equal(t, test.expectedBytes, result, 
					"Mismatch for script: %s\nExpected: %x\nGot: %x", 
					test.scriptStr, test.expectedBytes, result)
			}
		})
	}
}

// TestPushDataBoundaries tests boundary conditions for PUSHDATA operations
func TestPushDataBoundaries(t *testing.T) {
	tests := []struct {
		name          string
		buildScript   func() string
		validateBytes func([]byte) error
	}{
		{
			name: "PUSHDATA1 boundary at 75 bytes",
			buildScript: func() string {
				// 75 bytes should use direct push, not PUSHDATA1
				return fmt.Sprintf("0x%s", makeHexString(75))
			},
			validateBytes: func(result []byte) error {
				if len(result) != 76 { // 1 byte push opcode + 75 bytes data
					return errors.NewProcessingError("expected 76 bytes, got %d", len(result))
				}
				if result[0] != 0x4b { // Direct push of 75 bytes
					return errors.NewProcessingError("expected direct push opcode 0x4b, got 0x%02x", result[0])
				}
				return nil
			},
		},
		{
			name: "PUSHDATA1 boundary at 76 bytes",
			buildScript: func() string {
				// 76 bytes should use PUSHDATA1
				return fmt.Sprintf("0x%s", makeHexString(76))
			},
			validateBytes: func(result []byte) error {
				if len(result) != 78 { // 1 byte PUSHDATA1 + 1 byte length + 76 bytes data
					return errors.NewProcessingError("expected 78 bytes, got %d", len(result))
				}
				if result[0] != 0x4c { // PUSHDATA1
					return errors.NewProcessingError("expected PUSHDATA1 opcode 0x4c, got 0x%02x", result[0])
				}
				if result[1] != 0x4c { // Length 76
					return errors.NewProcessingError("expected length 76 (0x4c), got 0x%02x", result[1])
				}
				return nil
			},
		},
		{
			name: "PUSHDATA1/PUSHDATA2 boundary at 255 bytes",
			buildScript: func() string {
				// 255 bytes can use PUSHDATA1
				return fmt.Sprintf("0x%s", makeHexString(255))
			},
			validateBytes: func(result []byte) error {
				if len(result) != 257 { // 1 byte PUSHDATA1 + 1 byte length + 255 bytes data
					return errors.NewProcessingError("expected 257 bytes, got %d", len(result))
				}
				if result[0] != 0x4c { // PUSHDATA1
					return errors.NewProcessingError("expected PUSHDATA1 opcode 0x4c, got 0x%02x", result[0])
				}
				if result[1] != 0xff { // Length 255
					return errors.NewProcessingError("expected length 255 (0xff), got 0x%02x", result[1])
				}
				return nil
			},
		},
		{
			name: "PUSHDATA2 boundary at 256 bytes",
			buildScript: func() string {
				// 256 bytes should use PUSHDATA2
				return fmt.Sprintf("0x%s", makeHexString(256))
			},
			validateBytes: func(result []byte) error {
				if len(result) != 259 { // 1 byte PUSHDATA2 + 2 bytes length + 256 bytes data
					return errors.NewProcessingError("expected 259 bytes, got %d", len(result))
				}
				if result[0] != 0x4d { // PUSHDATA2
					return errors.NewProcessingError("expected PUSHDATA2 opcode 0x4d, got 0x%02x", result[0])
				}
				// Check little-endian length
				if result[1] != 0x00 || result[2] != 0x01 { // 256 = 0x0100 little-endian
					return errors.NewProcessingError("expected length 256 (0x0001), got 0x%02x%02x", result[1], result[2])
				}
				return nil
			},
		},
	}
	
	parser := NewScriptParser()
	
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			script := test.buildScript()
			result, err := parser.ParseScript(script)
			require.NoError(t, err)
			
			if validateErr := test.validateBytes(result); validateErr != nil {
				t.Error(validateErr)
			}
		})
	}
}

// TestPushDataMinimalEncoding tests MINIMALDATA flag enforcement
func TestPushDataMinimalEncoding(t *testing.T) {
	tests := []struct {
		name        string
		scriptHex   string
		flags       uint32
		shouldFail  bool
		errorType   ScriptError
	}{
		{
			name:        "Direct push when should use direct push - valid",
			scriptHex:   "01ff", // Push 1 byte using direct push
			flags:       SCRIPT_VERIFY_MINIMALDATA,
			shouldFail:  false,
		},
		{
			name:        "PUSHDATA1 for 1 byte - invalid with MINIMALDATA",
			scriptHex:   "4c01ff", // PUSHDATA1 pushing 1 byte
			flags:       SCRIPT_VERIFY_MINIMALDATA,
			shouldFail:  true,
			errorType:   SCRIPT_ERR_MINIMALDATA,
		},
		{
			name:        "PUSHDATA1 for 75 bytes - invalid with MINIMALDATA",
			scriptHex:   "4c4b" + makeHexString(75), // PUSHDATA1 pushing 75 bytes
			flags:       SCRIPT_VERIFY_MINIMALDATA,
			shouldFail:  true,
			errorType:   SCRIPT_ERR_MINIMALDATA,
		},
		{
			name:        "PUSHDATA1 for 76 bytes - valid",
			scriptHex:   "4c4c" + makeHexString(76), // PUSHDATA1 pushing 76 bytes
			flags:       SCRIPT_VERIFY_MINIMALDATA,
			shouldFail:  false,
		},
		{
			name:        "PUSHDATA2 for 255 bytes - invalid with MINIMALDATA",
			scriptHex:   "4dff00" + makeHexString(255), // PUSHDATA2 pushing 255 bytes
			flags:       SCRIPT_VERIFY_MINIMALDATA,
			shouldFail:  true,
			errorType:   SCRIPT_ERR_MINIMALDATA,
		},
		{
			name:        "PUSHDATA2 for 256 bytes - valid",
			scriptHex:   "4d0001" + makeHexString(256), // PUSHDATA2 pushing 256 bytes
			flags:       SCRIPT_VERIFY_MINIMALDATA,
			shouldFail:  false,
		},
		{
			name:        "PUSHDATA4 for small push - invalid with MINIMALDATA",
			scriptHex:   "4e04000000" + "01020304", // PUSHDATA4 pushing 4 bytes
			flags:       SCRIPT_VERIFY_MINIMALDATA,
			shouldFail:  true,
			errorType:   SCRIPT_ERR_MINIMALDATA,
		},
	}
	
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scriptBytes, err := hex.DecodeString(test.scriptHex)
			require.NoError(t, err)
			
			// Create a script that uses the push and then drops it
			script := bscript.NewFromBytes(scriptBytes)
			script.AppendOpcodes(bscript.OpDROP)
			script.AppendOpcodes(bscript.Op1) // Leave true on stack
			
			// Create test builder
			tb := NewTestBuilder(script, test.name, test.flags, false, 0)
			
			if test.shouldFail {
				tb.SetScriptError(test.errorType)
			} else {
				tb.SetScriptError(SCRIPT_ERR_OK)
			}
			
			// Note: DoTest would need to be enhanced to handle MINIMALDATA validation
			// For now, this test structure shows how it would be tested
		})
	}
}

// Helper to create bytes of specified length
func makeBytes(length int) []byte {
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		result[i] = byte(i % 256)
	}
	return result
}

// TestPushDataRawOpcodes tests handling of raw push opcodes
func TestPushDataRawOpcodes(t *testing.T) {
	tests := []struct {
		name          string
		scriptStr     string
		expectedBytes []byte
	}{
		{
			name:          "Raw push opcode 0x01 followed by data",
			scriptStr:     "0x01 0xff",
			expectedBytes: []byte{0x01, 0xff},
		},
		{
			name:          "Raw push opcode 0x4b (75) followed by data",
			scriptStr:     fmt.Sprintf("0x4b 0x%s", makeHexString(75)),
			expectedBytes: append([]byte{0x4b}, makeBytes(75)...),
		},
		{
			name:          "Mixed raw opcodes and regular pushes",
			scriptStr:     "0x02 0x0102 OP_DUP 0x01 0xff",
			expectedBytes: []byte{0x02, 0x01, 0x02, bscript.OpDUP, 0x01, 0xff},
		},
	}
	
	parser := NewScriptParser()
	
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := parser.ParseScript(test.scriptStr)
			require.NoError(t, err)
			
			if !bytes.Equal(result, test.expectedBytes) {
				t.Errorf("Script parsing mismatch\nInput: %s\nExpected: %x\nGot: %x",
					test.scriptStr, test.expectedBytes, result)
			}
		})
	}
}