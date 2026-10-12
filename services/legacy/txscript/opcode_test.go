// Copyright (c) 2013-2017 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package txscript

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOpcodeDisabled tests the opcodeDisabled function manually because all
// disabled opcodes result in a script execution failure when executed normally,
// so the function is not called under normal circumstances.
func TestOpcodeDisabled(t *testing.T) {
	t.Parallel()

	tests := []byte{OP_2MUL, OP_2DIV}
	for _, opcodeVal := range tests {
		pop := parsedOpcode{opcode: &opcodeArray[opcodeVal], data: nil}

		err := opcodeDisabled(&pop, nil)
		if !IsErrorCode(err, ErrDisabledOpcode) {
			t.Errorf("opcodeDisabled: unexpected error - got %v, "+
				"want %v", err, ErrDisabledOpcode)
			continue
		}
	}
}

// TestOpcodeDisasm tests the print function for all opcodes in both the oneline
// and full modes to ensure it provides the expected disassembly.
func TestOpcodeDisasm(t *testing.T) {
	t.Parallel()

	// First, test the oneline disassembly.

	// The expected strings for the data push opcodes are replaced in the
	// test loops below since they involve repeating bytes.  Also, the
	// OP_NOP# and OP_UNKNOWN# are replaced below too, since it's easier
	// than manually listing them here.
	oneBytes := []byte{0x01}
	oneStr := "01"

	expectedStrings := [256]string{0x00: "0", 0x4f: "-1",
		0x50: "OP_RESERVED", 0x61: "OP_NOP", 0x62: "OP_VER",
		0x63: "OP_IF", 0x64: "OP_NOTIF", 0x65: "OP_VERIF",
		0x66: "OP_VERNOTIF", 0x67: "OP_ELSE", 0x68: "OP_ENDIF",
		0x69: "OP_VERIFY", 0x6a: "OP_RETURN", 0x6b: "OP_TOALTSTACK",
		0x6c: "OP_FROMALTSTACK", 0x6d: "OP_2DROP", 0x6e: "OP_2DUP",
		0x6f: "OP_3DUP", 0x70: "OP_2OVER", 0x71: "OP_2ROT",
		0x72: "OP_2SWAP", 0x73: "OP_IFDUP", 0x74: "OP_DEPTH",
		0x75: "OP_DROP", 0x76: "OP_DUP", 0x77: "OP_NIP",
		0x78: "OP_OVER", 0x79: "OP_PICK", 0x7a: "OP_ROLL",
		0x7b: "OP_ROT", 0x7c: "OP_SWAP", 0x7d: "OP_TUCK",
		0x7e: "OP_CAT", 0x7f: "OP_SPLIT", 0x80: "OP_NUM2BIN",
		0x81: "OP_BIN2NUM", 0x82: "OP_SIZE", 0x83: "OP_INVERT",
		0x84: "OP_AND", 0x85: "OP_OR", 0x86: "OP_XOR",
		0x87: "OP_EQUAL", 0x88: "OP_EQUALVERIFY", 0x89: "OP_RESERVED1",
		0x8a: "OP_RESERVED2", 0x8b: "OP_1ADD", 0x8c: "OP_1SUB",
		0x8d: "OP_2MUL", 0x8e: "OP_2DIV", 0x8f: "OP_NEGATE",
		0x90: "OP_ABS", 0x91: "OP_NOT", 0x92: "OP_0NOTEQUAL",
		0x93: "OP_ADD", 0x94: "OP_SUB", 0x95: "OP_MUL", 0x96: "OP_DIV",
		0x97: "OP_MOD", 0x98: "OP_LSHIFT", 0x99: "OP_RSHIFT",
		0x9a: "OP_BOOLAND", 0x9b: "OP_BOOLOR", 0x9c: "OP_NUMEQUAL",
		0x9d: "OP_NUMEQUALVERIFY", 0x9e: "OP_NUMNOTEQUAL",
		0x9f: "OP_LESSTHAN", 0xa0: "OP_GREATERTHAN",
		0xa1: "OP_LESSTHANOREQUAL", 0xa2: "OP_GREATERTHANOREQUAL",
		0xa3: "OP_MIN", 0xa4: "OP_MAX", 0xa5: "OP_WITHIN",
		0xa6: "OP_RIPEMD160", 0xa7: "OP_SHA1", 0xa8: "OP_SHA256",
		0xa9: "OP_HASH160", 0xaa: "OP_HASH256", 0xab: "OP_CODESEPARATOR",
		0xac: "OP_CHECKSIG", 0xad: "OP_CHECKSIGVERIFY",
		0xae: "OP_CHECKMULTISIG", 0xaf: "OP_CHECKMULTISIGVERIFY",
		0xfa: "OP_SMALLINTEGER", 0xfb: "OP_PUBKEYS",
		0xfd: "OP_PUBKEYHASH", 0xfe: "OP_PUBKEY",
		0xff: "OP_INVALIDOPCODE",
	}
	for opcodeVal, expectedStr := range expectedStrings {
		var data []byte

		switch {
		// OP_DATA_1 through OP_DATA_65 display the pushed data.
		case opcodeVal >= 0x01 && opcodeVal < 0x4c:
			data = bytes.Repeat(oneBytes, opcodeVal)
			expectedStr = strings.Repeat(oneStr, opcodeVal)

		// OP_PUSHDATA1.
		case opcodeVal == 0x4c:
			data = bytes.Repeat(oneBytes, 1)
			expectedStr = strings.Repeat(oneStr, 1)

		// OP_PUSHDATA2.
		case opcodeVal == 0x4d:
			data = bytes.Repeat(oneBytes, 2)
			expectedStr = strings.Repeat(oneStr, 2)

		// OP_PUSHDATA4.
		case opcodeVal == 0x4e:
			data = bytes.Repeat(oneBytes, 3)
			expectedStr = strings.Repeat(oneStr, 3)

		// OP_1 through OP_16 display the numbers themselves.
		case opcodeVal >= 0x51 && opcodeVal <= 0x60:
			val := byte(opcodeVal - (0x51 - 1))
			data = []byte{val}
			expectedStr = strconv.Itoa(int(val))

		// OP_NOP1 through OP_NOP10.
		case opcodeVal >= 0xb0 && opcodeVal <= 0xb9:
			switch opcodeVal {
			case 0xb1:
				// OP_NOP2 is an alias of OP_CHECKLOCKTIMEVERIFY
				expectedStr = "OP_CHECKLOCKTIMEVERIFY"
			case 0xb2:
				// OP_NOP3 is an alias of OP_CHECKSEQUENCEVERIFY
				expectedStr = "OP_CHECKSEQUENCEVERIFY"
			default:
				val := byte(opcodeVal - (0xb0 - 1))
				expectedStr = "OP_NOP" + strconv.Itoa(int(val))
			}

		// OP_UNKNOWN#.
		case opcodeVal >= 0xba && opcodeVal <= 0xf9 || opcodeVal == 0xfc:
			expectedStr = "OP_UNKNOWN" + strconv.Itoa(opcodeVal)
		}

		pop := parsedOpcode{opcode: &opcodeArray[opcodeVal], data: data}

		gotStr := pop.print(true)
		if gotStr != expectedStr {
			t.Errorf("pop.print (opcode %x): Unexpected disasm "+
				"string - got %v, want %v", opcodeVal, gotStr,
				expectedStr)

			continue
		}
	}

	// Now, replace the relevant fields and test the full disassembly.
	expectedStrings[0x00] = "OP_0"
	expectedStrings[0x4f] = "OP_1NEGATE"

	for opcodeVal, expectedStr := range expectedStrings {
		var data []byte

		switch {
		// OP_DATA_1 through OP_DATA_65 display the opcode followed by
		// the pushed data.
		case opcodeVal >= 0x01 && opcodeVal < 0x4c:
			data = bytes.Repeat(oneBytes, opcodeVal)
			expectedStr = fmt.Sprintf("OP_DATA_%d 0x%s", opcodeVal,
				strings.Repeat(oneStr, opcodeVal))

		// OP_PUSHDATA1.
		case opcodeVal == 0x4c:
			data = bytes.Repeat(oneBytes, 1)
			expectedStr = fmt.Sprintf("OP_PUSHDATA1 0x%02x 0x%s",
				len(data), strings.Repeat(oneStr, 1))

		// OP_PUSHDATA2.
		case opcodeVal == 0x4d:
			data = bytes.Repeat(oneBytes, 2)
			expectedStr = fmt.Sprintf("OP_PUSHDATA2 0x%04x 0x%s",
				len(data), strings.Repeat(oneStr, 2))

		// OP_PUSHDATA4.
		case opcodeVal == 0x4e:
			data = bytes.Repeat(oneBytes, 3)
			expectedStr = fmt.Sprintf("OP_PUSHDATA4 0x%08x 0x%s",
				len(data), strings.Repeat(oneStr, 3))

		// OP_1 through OP_16.
		case opcodeVal >= 0x51 && opcodeVal <= 0x60:
			val := byte(opcodeVal - (0x51 - 1))
			data = []byte{val}
			expectedStr = "OP_" + strconv.Itoa(int(val))

		// OP_NOP1 through OP_NOP10.
		case opcodeVal >= 0xb0 && opcodeVal <= 0xb9:
			switch opcodeVal {
			case 0xb1:
				// OP_NOP2 is an alias of OP_CHECKLOCKTIMEVERIFY
				expectedStr = "OP_CHECKLOCKTIMEVERIFY"
			case 0xb2:
				// OP_NOP3 is an alias of OP_CHECKSEQUENCEVERIFY
				expectedStr = "OP_CHECKSEQUENCEVERIFY"
			default:
				val := byte(opcodeVal - (0xb0 - 1))
				expectedStr = "OP_NOP" + strconv.Itoa(int(val))
			}

		// OP_UNKNOWN#.
		case opcodeVal >= 0xba && opcodeVal <= 0xf9 || opcodeVal == 0xfc:
			expectedStr = "OP_UNKNOWN" + strconv.Itoa(int(opcodeVal))
		}

		pop := parsedOpcode{opcode: &opcodeArray[opcodeVal], data: data}

		gotStr := pop.print(false)
		if gotStr != expectedStr {
			t.Errorf("pop.print (opcode %x): Unexpected disasm "+
				"string - got %v, want %v", opcodeVal, gotStr,
				expectedStr)

			continue
		}
	}
}

// TestOpcodeInvert_NoAliasing ensures opcodeInvert does not mutate stack items
// that share a backing array with the top of stack (e.g. after OP_DUP).
func TestOpcodeInvert_NoAliasing(t *testing.T) {
	originalBytes := []byte{0x00, 0x0F, 0xF0, 0xFF}
	expectedInverted := []byte{0xFF, 0xF0, 0x0F, 0x00}

	vm := &Engine{}
	vm.dstack.PushByteArray(originalBytes)

	require.NoError(t, vm.dstack.DupN(1))
	require.Equal(t, int32(2), vm.dstack.Depth())

	pop := parsedOpcode{opcode: &opcodeArray[OP_INVERT], data: nil}
	require.NoError(t, opcodeInvert(&pop, vm))

	top, err := vm.dstack.PeekByteArray(0)
	require.NoError(t, err)
	require.Equal(t, expectedInverted, top, "top of stack must contain bitwise NOT of original bytes")

	below, err := vm.dstack.PeekByteArray(1)
	require.NoError(t, err)
	require.Equal(t, []byte{0x00, 0x0F, 0xF0, 0xFF}, below,
		"duplicate copy on stack must remain unchanged after OP_INVERT")
}

// TestOpcodeInvert_BasicCorrectness ensures opcodeInvert performs a bitwise
// NOT on the top stack item.
func TestOpcodeInvert_BasicCorrectness(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		want []byte
	}{
		{"empty", []byte{}, []byte{}},
		{"single byte zero", []byte{0x00}, []byte{0xFF}},
		{"single byte ff", []byte{0xFF}, []byte{0x00}},
		{"mixed", []byte{0xAA, 0x55, 0x0F, 0xF0}, []byte{0x55, 0xAA, 0xF0, 0x0F}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			vm := &Engine{}
			vm.dstack.PushByteArray(tc.in)

			pop := parsedOpcode{opcode: &opcodeArray[OP_INVERT], data: nil}
			require.NoError(t, opcodeInvert(&pop, vm))

			got, err := vm.dstack.PeekByteArray(0)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestOpcodeNum2binSizeTooBig verifies that OP_NUM2BIN rejects sizes greater
// than MaxScriptElementSize and that the returned error message references
// the correct constant (the value of MaxScriptElementSize, not
// defaultScriptNumLen). Regression for issue #4591.
func TestOpcodeNum2binSizeTooBig(t *testing.T) {
	vm := Engine{}

	vm.dstack.PushByteArray([]byte{0x01})
	vm.dstack.PushInt(scriptNum(MaxScriptElementSize + 1))

	pop := parsedOpcode{opcode: &opcodeArray[OP_NUM2BIN], data: nil}

	err := opcodeNum2bin(&pop, &vm)
	require.Error(t, err)
	require.True(t, IsErrorCode(err, ErrNumberTooBig),
		"expected ErrNumberTooBig, got %v", err)
	require.Contains(t, err.Error(), strconv.Itoa(MaxScriptElementSize),
		"error message should reference MaxScriptElementSize (%d), got %q",
		MaxScriptElementSize, err.Error())
}
