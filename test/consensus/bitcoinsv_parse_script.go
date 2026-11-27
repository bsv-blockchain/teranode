package consensus

import (
	"encoding/binary"
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
)

// ParseScriptBitcoinSV parses the script notation of bitcoin-sv's test vectors exactly as
// bitcoin-sv's ParseScript (src/core_read.cpp) does:
//
//   - a decimal number is pushed as a script number: OP_0, OP_1NEGATE or OP_1..OP_16
//     where one fits, otherwise the minimal CScriptNum encoding in a data push;
//   - 0x<hex> is inserted as raw bytes, not pushed;
//   - 'text' is pushed as data;
//   - an opcode name, with or without the OP_ prefix, from OP_PUSHDATA1 up, is the opcode
//     (bitcoinSVOpNames: case-sensitive, bitcoin-sv v1.2.3's names).
//
// After a push opcode written as raw hex or a name, the following tokens must add exactly
// the bytes it announces, or parsing fails, as in bitcoin-sv.
//
// ScriptParser.ParseScript differs: it pushes most 0x tokens and drops the sign byte of
// numbers such as 65535, so it must not be used for bitcoin-sv's vectors.
func ParseScriptBitcoinSV(s string) ([]byte, error) {
	var (
		result                 []byte
		pushSize, nextPushSize int
		pushDataSize           int
	)

	for _, w := range strings.Fields(s) {
		scriptSize := len(result)
		pushSize, nextPushSize = nextPushSize, 0

		var err error

		result, err = appendScriptToken(result, w)
		if err != nil {
			return nil, errors.NewProcessingError("error parsing script %q", s, err)
		}

		sizeChange := len(result) - scriptSize

		if pushSize != 0 && sizeChange != pushSize {
			return nil, errors.NewProcessingError("wrong number of bytes being pushed in %q: expected %d, pushed %d", s, pushSize, sizeChange)
		}

		// The token after OP_PUSHDATAn is its little-endian length; the token after that
		// must be that long.
		if pushSize != 0 && pushDataSize != 0 {
			added := result[scriptSize:]

			switch pushDataSize {
			case 1:
				nextPushSize = int(added[0])
			case 2:
				nextPushSize = int(binary.LittleEndian.Uint16(added))
			case 4:
				nextPushSize = int(binary.LittleEndian.Uint32(added))
			}

			pushDataSize = 0
		}

		if pushSize == 0 && sizeChange == 1 {
			op := result[len(result)-1]

			switch {
			case op < bscript.OpPUSHDATA1:
				nextPushSize = int(op)
			case op == bscript.OpPUSHDATA1:
				pushDataSize, nextPushSize = 1, 1
			case op == bscript.OpPUSHDATA2:
				pushDataSize, nextPushSize = 2, 2
			case op == bscript.OpPUSHDATA4:
				pushDataSize, nextPushSize = 4, 4
			}
		}
	}

	return result, nil
}

func appendScriptToken(result []byte, w string) ([]byte, error) {
	if isDecimal(w) {
		if w == "-" {
			return appendScriptInt(result, 0), nil // atoi64("-") is 0
		}

		n, err := strconv.ParseInt(w, 10, 64)
		if err != nil {
			return nil, err
		}

		return appendScriptInt(result, n), nil
	}

	if strings.HasPrefix(w, "0x") && len(w) > 2 {
		raw, err := hex.DecodeString(w[2:])
		if err != nil {
			return nil, errors.NewProcessingError("hex numbers must be full bytes, got %q", w, err)
		}

		return append(result, raw...), nil
	}

	if len(w) >= 2 && strings.HasPrefix(w, "'") && strings.HasSuffix(w, "'") {
		return appendPushData(result, []byte(w[1:len(w)-1])), nil
	}

	if op, ok := bitcoinSVOpNames[w]; ok {
		return append(result, op), nil
	}

	return nil, errors.NewProcessingError("unknown token %q", w)
}

// isDecimal reports whether w is digits, optionally after a leading '-', as bitcoin-sv's
// check is. A lone "-" counts, and atoi64 reads it as 0.
func isDecimal(w string) bool {
	digits := strings.TrimPrefix(w, "-")

	for _, r := range digits {
		if r < '0' || r > '9' {
			return false
		}
	}

	return digits != "" || w == "-"
}

// appendScriptInt is CScript::push_int64.
func appendScriptInt(result []byte, n int64) []byte {
	switch {
	case n == 0:
		return append(result, bscript.Op0)
	case n == -1 || (n >= 1 && n <= 16):
		return append(result, byte(int64(bscript.Op1)+n-1))
	default:
		return appendPushData(result, scriptNumBytes(n))
	}
}

// appendPushData is CScript::operator<<(std::vector<uint8_t>): the smallest push opcode
// for the length, then the data.
func appendPushData(result, data []byte) []byte {
	n := len(data)

	switch {
	case n < int(bscript.OpPUSHDATA1):
		result = append(result, byte(n))
	case n <= 0xff:
		result = append(result, bscript.OpPUSHDATA1, byte(n))
	case n <= 0xffff:
		result = append(result, bscript.OpPUSHDATA2, byte(n), byte(n>>8))
	default:
		result = append(result, bscript.OpPUSHDATA4)
		result = binary.LittleEndian.AppendUint32(result, uint32(n)) //nolint:gosec // script vectors are far below 4 GiB
	}

	return append(result, data...)
}
