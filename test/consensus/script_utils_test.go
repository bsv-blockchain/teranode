package consensus

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/stretchr/testify/require"
)

// TestGetScriptAsm tests script disassembly
func TestGetScriptAsm(t *testing.T) {
	tests := []struct {
		name     string
		script   *bscript.Script
		expected string
	}{
		{
			name:     "Empty script",
			script:   &bscript.Script{},
			expected: "",
		},
		{
			name: "Simple opcodes",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendOpcodes(bscript.OpDUP)
				_ = s.AppendOpcodes(bscript.OpHASH160)
				return s
			}(),
			expected: "OP_DUP OP_HASH160",
		},
		{
			name: "Push data",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendPushData([]byte{0x01, 0x02, 0x03})
				return s
			}(),
			expected: "010203",
		},
		{
			name: "Mixed opcodes and data",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendOpcodes(bscript.OpDUP)
				_ = s.AppendPushData([]byte{0xaa, 0xbb})
				_ = s.AppendOpcodes(bscript.OpEQUAL)
				return s
			}(),
			expected: "OP_DUP aabb OP_EQUAL",
		},
		{
			name: "Numbers",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendOpcodes(bscript.Op1)
				_ = s.AppendOpcodes(bscript.Op2)
				_ = s.AppendOpcodes(bscript.Op16)
				_ = s.AppendOpcodes(bscript.Op1NEGATE)
				return s
			}(),
			expected: "OP_1 OP_2 OP_16 OP_1NEGATE",
		},
		{
			name: "PUSHDATA operations",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				// PUSHDATA1
				_ = s.AppendPushData(make([]byte, 76))
				return s
			}(),
			expected: strings.Repeat("00", 76),
		},
		{
			name: "P2PKH script",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendOpcodes(bscript.OpDUP)
				_ = s.AppendOpcodes(bscript.OpHASH160)
				_ = s.AppendPushData(make([]byte, 20))
				_ = s.AppendOpcodes(bscript.OpEQUALVERIFY)
				_ = s.AppendOpcodes(bscript.OpCHECKSIG)
				return s
			}(),
			expected: "OP_DUP OP_HASH160 " + strings.Repeat("00", 20) + " OP_EQUALVERIFY OP_CHECKSIG",
		},
		{
			name: "Multisig script",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendOpcodes(bscript.Op2)
				_ = s.AppendPushData(make([]byte, 33))
				_ = s.AppendPushData(make([]byte, 33))
				_ = s.AppendPushData(make([]byte, 33))
				_ = s.AppendOpcodes(bscript.Op3)
				_ = s.AppendOpcodes(bscript.OpCHECKMULTISIG)
				return s
			}(),
			expected: "OP_2 " + strings.Repeat("00", 33) + " " + strings.Repeat("00", 33) + " " + strings.Repeat("00", 33) + " OP_3 OP_CHECKMULTISIG",
		},
		{
			name: "OP_RETURN data",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendOpcodes(bscript.OpRETURN)
				_ = s.AppendPushData([]byte("Hello, Bitcoin!"))
				return s
			}(),
			expected: "OP_RETURN 48656c6c6f2c20426974636f696e21",
		},
		{
			name: "Control flow",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendOpcodes(bscript.OpIF)
				_ = s.AppendOpcodes(bscript.Op1)
				_ = s.AppendOpcodes(bscript.OpELSE)
				_ = s.AppendOpcodes(bscript.Op0)
				_ = s.AppendOpcodes(bscript.OpENDIF)
				return s
			}(),
			expected: "OP_IF OP_1 OP_ELSE OP_0 OP_ENDIF",
		},
		{
			name: "Invalid opcode",
			script: func() *bscript.Script {
				return bscript.NewFromBytes([]byte{0xff})
			}(),
			expected: "OP_INVALIDOPCODE",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Get ASM representation
			asm := GetScriptAsm(test.script)
			require.Equal(t, test.expected, asm)
		})
	}
}

// GetScriptAsm converts a script to its ASM representation
func GetScriptAsm(script *bscript.Script) string {
	if script == nil || len(*script) == 0 {
		return ""
	}

	var parts []string
	s := *script
	i := 0

	for i < len(s) {
		opcode := s[i]
		i++

		// Handle push operations
		if opcode <= bscript.OpPUSHDATA4 {
			var dataLen int
			if opcode < bscript.OpPUSHDATA1 {
				// OP_0 (0x00) is a special case - it's an opcode, not a push
				if opcode == 0 {
					parts = append(parts, GetOpName(opcode))
					continue
				}
				dataLen = int(opcode)
			} else if opcode == bscript.OpPUSHDATA1 {
				if i >= len(s) {
					parts = append(parts, "PUSHDATA1[error]")
					break
				}
				dataLen = int(s[i])
				i++
			} else if opcode == bscript.OpPUSHDATA2 {
				if i+1 >= len(s) {
					parts = append(parts, "PUSHDATA2[error]")
					break
				}
				dataLen = int(s[i]) | int(s[i+1])<<8
				i += 2
			} else if opcode == bscript.OpPUSHDATA4 {
				if i+3 >= len(s) {
					parts = append(parts, "PUSHDATA4[error]")
					break
				}
				dataLen = int(s[i]) | int(s[i+1])<<8 | int(s[i+2])<<16 | int(s[i+3])<<24
				i += 4
			}

			if i+dataLen > len(s) {
				parts = append(parts, "[error]")
				break
			}

			if dataLen > 0 {
				data := s[i : i+dataLen]
				parts = append(parts, hex.EncodeToString(data))
				i += dataLen
			}
		} else {
			// Regular opcode
			parts = append(parts, GetOpName(opcode))
		}
	}

	return strings.Join(parts, " ")
}

// GetOpName returns the name of an opcode
func GetOpName(opcode uint8) string {
	switch opcode {
	// Push value
	case bscript.Op0: // OpFALSE is the same value
		return "OP_0"
	case bscript.Op1NEGATE:
		return "OP_1NEGATE"
	case bscript.Op1: // OpTRUE is the same value
		return "OP_1"
	case bscript.Op2:
		return "OP_2"
	case bscript.Op3:
		return "OP_3"
	case bscript.Op4:
		return "OP_4"
	case bscript.Op5:
		return "OP_5"
	case bscript.Op6:
		return "OP_6"
	case bscript.Op7:
		return "OP_7"
	case bscript.Op8:
		return "OP_8"
	case bscript.Op9:
		return "OP_9"
	case bscript.Op10:
		return "OP_10"
	case bscript.Op11:
		return "OP_11"
	case bscript.Op12:
		return "OP_12"
	case bscript.Op13:
		return "OP_13"
	case bscript.Op14:
		return "OP_14"
	case bscript.Op15:
		return "OP_15"
	case bscript.Op16:
		return "OP_16"

	// Control
	case bscript.OpNOP:
		return "OP_NOP"
	case bscript.OpVER:
		return "OP_VER"
	case bscript.OpIF:
		return "OP_IF"
	case bscript.OpNOTIF:
		return "OP_NOTIF"
	case bscript.OpVERIF:
		return "OP_VERIF"
	case bscript.OpVERNOTIF:
		return "OP_VERNOTIF"
	case bscript.OpELSE:
		return "OP_ELSE"
	case bscript.OpENDIF:
		return "OP_ENDIF"
	case bscript.OpVERIFY:
		return "OP_VERIFY"
	case bscript.OpRETURN:
		return "OP_RETURN"

	// Stack ops
	case bscript.OpTOALTSTACK:
		return "OP_TOALTSTACK"
	case bscript.OpFROMALTSTACK:
		return "OP_FROMALTSTACK"
	case bscript.Op2DROP:
		return "OP_2DROP"
	case bscript.Op2DUP:
		return "OP_2DUP"
	case bscript.Op3DUP:
		return "OP_3DUP"
	case bscript.Op2OVER:
		return "OP_2OVER"
	case bscript.Op2ROT:
		return "OP_2ROT"
	case bscript.Op2SWAP:
		return "OP_2SWAP"
	case bscript.OpIFDUP:
		return "OP_IFDUP"
	case bscript.OpDEPTH:
		return "OP_DEPTH"
	case bscript.OpDROP:
		return "OP_DROP"
	case bscript.OpDUP:
		return "OP_DUP"
	case bscript.OpNIP:
		return "OP_NIP"
	case bscript.OpOVER:
		return "OP_OVER"
	case bscript.OpPICK:
		return "OP_PICK"
	case bscript.OpROLL:
		return "OP_ROLL"
	case bscript.OpROT:
		return "OP_ROT"
	case bscript.OpSWAP:
		return "OP_SWAP"
	case bscript.OpTUCK:
		return "OP_TUCK"

	// Splice ops
	case bscript.OpCAT:
		return "OP_CAT"
	case bscript.OpSPLIT:
		return "OP_SPLIT"
	case bscript.OpNUM2BIN:
		return "OP_NUM2BIN"
	case bscript.OpBIN2NUM:
		return "OP_BIN2NUM"
	case bscript.OpSIZE:
		return "OP_SIZE"

	// Bit logic
	case bscript.OpINVERT:
		return "OP_INVERT"
	case bscript.OpAND:
		return "OP_AND"
	case bscript.OpOR:
		return "OP_OR"
	case bscript.OpXOR:
		return "OP_XOR"
	case bscript.OpEQUAL:
		return "OP_EQUAL"
	case bscript.OpEQUALVERIFY:
		return "OP_EQUALVERIFY"

	// Numeric
	case bscript.Op1ADD:
		return "OP_1ADD"
	case bscript.Op1SUB:
		return "OP_1SUB"
	case bscript.Op2MUL:
		return "OP_2MUL"
	case bscript.Op2DIV:
		return "OP_2DIV"
	case bscript.OpNEGATE:
		return "OP_NEGATE"
	case bscript.OpABS:
		return "OP_ABS"
	case bscript.OpNOT:
		return "OP_NOT"
	case bscript.Op0NOTEQUAL:
		return "OP_0NOTEQUAL"
	case bscript.OpADD:
		return "OP_ADD"
	case bscript.OpSUB:
		return "OP_SUB"
	case bscript.OpMUL:
		return "OP_MUL"
	case bscript.OpDIV:
		return "OP_DIV"
	case bscript.OpMOD:
		return "OP_MOD"
	case bscript.OpLSHIFT:
		return "OP_LSHIFT"
	case bscript.OpRSHIFT:
		return "OP_RSHIFT"
	case bscript.OpBOOLAND:
		return "OP_BOOLAND"
	case bscript.OpBOOLOR:
		return "OP_BOOLOR"
	case bscript.OpNUMEQUAL:
		return "OP_NUMEQUAL"
	case bscript.OpNUMEQUALVERIFY:
		return "OP_NUMEQUALVERIFY"
	case bscript.OpNUMNOTEQUAL:
		return "OP_NUMNOTEQUAL"
	case bscript.OpLESSTHAN:
		return "OP_LESSTHAN"
	case bscript.OpGREATERTHAN:
		return "OP_GREATERTHAN"
	case bscript.OpLESSTHANOREQUAL:
		return "OP_LESSTHANOREQUAL"
	case bscript.OpGREATERTHANOREQUAL:
		return "OP_GREATERTHANOREQUAL"
	case bscript.OpMIN:
		return "OP_MIN"
	case bscript.OpMAX:
		return "OP_MAX"
	case bscript.OpWITHIN:
		return "OP_WITHIN"

	// Crypto
	case bscript.OpRIPEMD160:
		return "OP_RIPEMD160"
	case bscript.OpSHA1:
		return "OP_SHA1"
	case bscript.OpSHA256:
		return "OP_SHA256"
	case bscript.OpHASH160:
		return "OP_HASH160"
	case bscript.OpHASH256:
		return "OP_HASH256"
	case bscript.OpCODESEPARATOR:
		return "OP_CODESEPARATOR"
	case bscript.OpCHECKSIG:
		return "OP_CHECKSIG"
	case bscript.OpCHECKSIGVERIFY:
		return "OP_CHECKSIGVERIFY"
	case bscript.OpCHECKMULTISIG:
		return "OP_CHECKMULTISIG"
	case bscript.OpCHECKMULTISIGVERIFY:
		return "OP_CHECKMULTISIGVERIFY"

	// Expansion
	case bscript.OpNOP1:
		return "OP_NOP1"
	case bscript.OpCHECKLOCKTIMEVERIFY:
		return "OP_CHECKLOCKTIMEVERIFY"
	case bscript.OpCHECKSEQUENCEVERIFY:
		return "OP_CHECKSEQUENCEVERIFY"
	case bscript.OpNOP4:
		return "OP_NOP4"
	case bscript.OpNOP5:
		return "OP_NOP5"
	case bscript.OpNOP6:
		return "OP_NOP6"
	case bscript.OpNOP7:
		return "OP_NOP7"
	case bscript.OpNOP8:
		return "OP_NOP8"
	case bscript.OpNOP9:
		return "OP_NOP9"
	case bscript.OpNOP10:
		return "OP_NOP10"

	case 0xff:
		return "OP_INVALIDOPCODE"

	default:
		return "OP_UNKNOWN"
	}
}

// TestIsPushOnly tests IsPushOnly functionality
func TestIsPushOnly(t *testing.T) {
	tests := []struct {
		name       string
		script     *bscript.Script
		isPushOnly bool
	}{
		{
			name:       "Empty script",
			script:     &bscript.Script{},
			isPushOnly: true,
		},
		{
			name: "Single push",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendPushData([]byte{0x01, 0x02})
				return s
			}(),
			isPushOnly: true,
		},
		{
			name: "Multiple pushes",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendPushData([]byte{0x01})
				_ = s.AppendPushData([]byte{0x02, 0x03})
				_ = s.AppendPushData([]byte{0x04, 0x05, 0x06})
				return s
			}(),
			isPushOnly: true,
		},
		{
			name: "Push with opcode",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendPushData([]byte{0x01})
				_ = s.AppendOpcodes(bscript.OpCHECKSIG)
				return s
			}(),
			isPushOnly: false,
		},
		{
			name: "Only opcodes",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendOpcodes(bscript.OpDUP)
				_ = s.AppendOpcodes(bscript.OpHASH160)
				return s
			}(),
			isPushOnly: false,
		},
		{
			name: "Number pushes",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendOpcodes(bscript.Op1)
				_ = s.AppendOpcodes(bscript.Op2)
				_ = s.AppendOpcodes(bscript.Op16)
				return s
			}(),
			isPushOnly: true,
		},
		{
			name: "OP_1NEGATE",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendOpcodes(bscript.Op1NEGATE)
				return s
			}(),
			isPushOnly: true,
		},
		{
			name: "PUSHDATA operations",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				// Small push
				_ = s.AppendPushData(make([]byte, 75))
				// PUSHDATA1
				_ = s.AppendPushData(make([]byte, 76))
				// PUSHDATA2
				_ = s.AppendPushData(make([]byte, 256))
				return s
			}(),
			isPushOnly: true,
		},
		{
			name: "Invalid push - truncated",
			script: func() *bscript.Script {
				// Create invalid script with truncated push
				return bscript.NewFromBytes([]byte{0x01}) // Says push 1 byte but no data
			}(),
			isPushOnly: false,
		},
		{
			name: "Invalid PUSHDATA1 - truncated",
			script: func() *bscript.Script {
				// PUSHDATA1 without length byte
				return bscript.NewFromBytes([]byte{bscript.OpPUSHDATA1})
			}(),
			isPushOnly: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := IsPushOnly(test.script)
			require.Equal(t, test.isPushOnly, result)
		})
	}
}

// IsPushOnly returns true if the script only contains push operations
func IsPushOnly(script *bscript.Script) bool {
	if script == nil {
		return true
	}

	s := *script
	i := 0

	for i < len(s) {
		opcode := s[i]
		i++

		// Check if it's a push operation
		if opcode > bscript.OpPUSHDATA4 {
			// Special case for number opcodes
			if opcode == bscript.Op1NEGATE || (opcode >= bscript.Op1 && opcode <= bscript.Op16) {
				continue
			}
			return false
		}

		// Handle push data lengths
		var dataLen int
		if opcode < bscript.OpPUSHDATA1 {
			dataLen = int(opcode)
		} else if opcode == bscript.OpPUSHDATA1 {
			if i >= len(s) {
				return false // Truncated
			}
			dataLen = int(s[i])
			i++
		} else if opcode == bscript.OpPUSHDATA2 {
			if i+1 >= len(s) {
				return false // Truncated
			}
			dataLen = int(s[i]) | int(s[i+1])<<8
			i += 2
		} else if opcode == bscript.OpPUSHDATA4 {
			if i+3 >= len(s) {
				return false // Truncated
			}
			dataLen = int(s[i]) | int(s[i+1])<<8 | int(s[i+2])<<16 | int(s[i+3])<<24
			i += 4
		}

		// Check if we have enough data
		if i+dataLen > len(s) {
			return false // Truncated data
		}

		i += dataLen
	}

	return true
}

// TestFindAndDelete tests FindAndDelete functionality
func TestFindAndDelete(t *testing.T) {
	tests := []struct {
		name     string
		script   []byte
		pattern  []byte
		expected []byte
	}{
		{
			name:     "Empty script",
			script:   []byte{},
			pattern:  []byte{0x01},
			expected: []byte{},
		},
		{
			name:     "Pattern not found",
			script:   []byte{0x01, 0x02, 0x03},
			pattern:  []byte{0x04},
			expected: []byte{0x01, 0x02, 0x03},
		},
		{
			name:     "Single occurrence",
			script:   []byte{0x01, 0x02, 0x03},
			pattern:  []byte{0x02},
			expected: []byte{0x01, 0x03},
		},
		{
			name:     "Multiple occurrences",
			script:   []byte{0x01, 0x02, 0x01, 0x02, 0x03},
			pattern:  []byte{0x01, 0x02},
			expected: []byte{0x03},
		},
		{
			name:     "Pattern at start",
			script:   []byte{0x01, 0x02, 0x03},
			pattern:  []byte{0x01},
			expected: []byte{0x02, 0x03},
		},
		{
			name:     "Pattern at end",
			script:   []byte{0x01, 0x02, 0x03},
			pattern:  []byte{0x03},
			expected: []byte{0x01, 0x02},
		},
		{
			name:     "Entire script is pattern",
			script:   []byte{0x01, 0x02},
			pattern:  []byte{0x01, 0x02},
			expected: []byte{},
		},
		{
			name:     "Overlapping pattern",
			script:   []byte{0x01, 0x01, 0x01},
			pattern:  []byte{0x01, 0x01},
			expected: []byte{0x01},
		},
		{
			name:     "Complex script with opcodes",
			script:   []byte{bscript.OpDUP, 0x14, bscript.OpHASH160, bscript.OpDUP},
			pattern:  []byte{bscript.OpDUP},
			expected: []byte{0x14, bscript.OpHASH160},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := FindAndDelete(test.script, test.pattern)
			require.True(t, bytes.Equal(result, test.expected),
				"Expected %x, got %x", test.expected, result)
		})
	}
}

// FindAndDelete removes all occurrences of pattern from script
func FindAndDelete(script []byte, pattern []byte) []byte {
	if len(pattern) == 0 || len(script) == 0 {
		return script
	}

	result := make([]byte, 0, len(script))
	i := 0

	for i < len(script) {
		// Check if pattern matches at current position
		if i+len(pattern) <= len(script) && bytes.Equal(script[i:i+len(pattern)], pattern) {
			// Skip pattern
			i += len(pattern)
		} else {
			// Copy byte
			result = append(result, script[i])
			i++
		}
	}

	return result
}

// TestScriptStandardPush tests standard push operations
func TestScriptStandardPush(t *testing.T) {
	// Test that push operations create expected byte sequences
	tests := []struct {
		name     string
		value    int64
		expected []byte
	}{
		{
			name:     "Push -1",
			value:    -1,
			expected: []byte{bscript.Op1NEGATE},
		},
		{
			name:     "Push 0",
			value:    0,
			expected: []byte{bscript.Op0},
		},
		{
			name:     "Push 1",
			value:    1,
			expected: []byte{bscript.Op1},
		},
		{
			name:     "Push 16",
			value:    16,
			expected: []byte{bscript.Op16},
		},
		{
			name:     "Push 17",
			value:    17,
			expected: []byte{0x01, 0x11}, // Push 1 byte: 0x11
		},
		{
			name:     "Push 75",
			value:    75,
			expected: []byte{0x01, 0x4b}, // Push 1 byte: 0x4b
		},
		{
			name:     "Push 128",
			value:    128,
			expected: []byte{0x02, 0x80, 0x00}, // Push 2 bytes: 0x0080 (little-endian)
		},
		{
			name:     "Push 255",
			value:    255,
			expected: []byte{0x02, 0xff, 0x00}, // Push 2 bytes: 0x00ff
		},
		{
			name:     "Push 256",
			value:    256,
			expected: []byte{0x02, 0x00, 0x01}, // Push 2 bytes: 0x0100
		},
		{
			name:     "Push 32767",
			value:    32767,
			expected: []byte{0x02, 0xff, 0x7f}, // Push 2 bytes: 0x7fff
		},
		{
			name:     "Push 32768",
			value:    32768,
			expected: []byte{0x03, 0x00, 0x80, 0x00}, // Push 3 bytes: 0x008000
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			script := &bscript.Script{}
			AppendPushInt(script, test.value)

			result := []byte(*script)
			require.True(t, bytes.Equal(result, test.expected),
				"For value %d, expected %x, got %x", test.value, test.expected, result)
		})
	}
}

// TestIsUnspendable tests script unspendability detection
func TestIsUnspendable(t *testing.T) {
	tests := []struct {
		name          string
		script        *bscript.Script
		isGenesis     bool
		isUnspendable bool
	}{
		{
			name: "OP_RETURN only",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendOpcodes(bscript.OpRETURN)
				return s
			}(),
			isGenesis:     false,
			isUnspendable: true,
		},
		{
			name: "OP_FALSE OP_RETURN",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendOpcodes(bscript.OpFALSE)
				_ = s.AppendOpcodes(bscript.OpRETURN)
				return s
			}(),
			isGenesis:     false,
			isUnspendable: true,
		},
		{
			name: "OP_RETURN with data",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendOpcodes(bscript.OpRETURN)
				_ = s.AppendPushData([]byte("data"))
				return s
			}(),
			isGenesis:     false,
			isUnspendable: true,
		},
		{
			name: "OP_RETURN in Genesis",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendOpcodes(bscript.OpRETURN)
				return s
			}(),
			isGenesis:     true,
			isUnspendable: false, // OP_RETURN is spendable in Genesis
		},
		{
			name: "Normal P2PKH",
			script: func() *bscript.Script {
				s := &bscript.Script{}
				_ = s.AppendOpcodes(bscript.OpDUP)
				_ = s.AppendOpcodes(bscript.OpHASH160)
				_ = s.AppendPushData(make([]byte, 20))
				_ = s.AppendOpcodes(bscript.OpEQUALVERIFY)
				_ = s.AppendOpcodes(bscript.OpCHECKSIG)
				return s
			}(),
			isGenesis:     false,
			isUnspendable: false,
		},
		{
			name: "Script size > MAX_SCRIPT_SIZE",
			script: func() *bscript.Script {
				// In pre-Genesis, scripts > 10KB are unspendable
				s := bscript.NewFromBytes(make([]byte, 10001))
				return s
			}(),
			isGenesis:     false,
			isUnspendable: true,
		},
		{
			name: "Large script in Genesis",
			script: func() *bscript.Script {
				// In Genesis, large scripts are spendable
				s := bscript.NewFromBytes(make([]byte, 10001))
				return s
			}(),
			isGenesis:     true,
			isUnspendable: false,
		},
		{
			name:          "Empty script",
			script:        &bscript.Script{},
			isGenesis:     false,
			isUnspendable: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := IsUnspendable(test.script, test.isGenesis)
			require.Equal(t, test.isUnspendable, result)
		})
	}
}

// IsUnspendable returns true if the script is provably unspendable
func IsUnspendable(script *bscript.Script, isGenesis bool) bool {
	if script == nil || len(*script) == 0 {
		return false
	}

	s := *script

	// Check for OP_RETURN at start
	if len(s) > 0 && s[0] == bscript.OpRETURN {
		// In Genesis, OP_RETURN is spendable
		return !isGenesis
	}

	// Check for OP_FALSE OP_RETURN pattern
	if len(s) >= 2 && s[0] == bscript.OpFALSE && s[1] == bscript.OpRETURN {
		return !isGenesis
	}

	// In pre-Genesis, scripts larger than MAX_SCRIPT_SIZE are unspendable
	if !isGenesis && len(s) > 10000 {
		return true
	}

	return false
}
