package util

import (
	"bytes"
	"encoding/binary"
	"math"
	"strings"
	"unicode"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
)

const (
	// minerSlashTruncationCount defines the number of slashes after which to truncate miner tags
	minerSlashTruncationCount = 2
	// maxHeightBytes is the maximum allowed number of data bytes in the coinbase height push.
	// Canonical BIP34 heights never exceed 5 bytes (a uint32 encoded as a CScriptNum); 8 is a
	// defensive upper bound that also caps how many bytes decodeScriptNum has to consider.
	maxHeightBytes = 8
	// unicodeReplacementChar is the Unicode replacement character to filter out
	unicodeReplacementChar = 0xFFFD
)

// ExtractCoinbaseHeight extracts the block height from a coinbase transaction's input script.
// The height is encoded at the beginning of the coinbase script according to BIP 34.
func ExtractCoinbaseHeight(coinbaseTx *bt.Tx) (uint32, error) {
	height, _, err := extractCoinbaseHeightAndText(*coinbaseTx.Inputs[0].UnlockingScript, false)
	return height, err
}

// ExtractCoinbaseMiner extracts the miner identification string from a coinbase transaction.
// This parses the arbitrary text portion of the coinbase script, cleaning and formatting it.
// By default, non-printable characters are filtered and the text is sanitized.
func ExtractCoinbaseMiner(coinbaseTx *bt.Tx) (string, error) {
	return ExtractCoinbaseMinerRaw(coinbaseTx, false)
}

// ExtractCoinbaseMinerRaw extracts the miner identification string from a coinbase transaction.
// When raw is true, the arbitrary text is returned without any sanitization or filtering.
// When raw is false, non-printable UTF-8 characters are filtered, whitespace is trimmed,
// and the text is truncated after the second slash.
func ExtractCoinbaseMinerRaw(coinbaseTx *bt.Tx, raw bool) (string, error) {
	if len(coinbaseTx.Inputs) == 0 {
		return "", errors.NewBlockCoinbaseMissingHeightError("coinbase transaction has no inputs")
	}

	// Extract both height and miner text from the first input of the coinbase transaction
	_, miner, err := extractCoinbaseHeightAndText(*coinbaseTx.Inputs[0].UnlockingScript, raw)
	if err != nil && errors.Is(err, errors.ErrBlockCoinbaseMissingHeight) {
		err = nil
	}

	return miner, err
}

// extractCoinbaseHeightAndText parses the BIP34 block height and the trailing arbitrary text from a
// coinbase signature script.
//
// The height is the first data push of the script. It is decoded with real Bitcoin script
// push-opcode semantics (OP_0, OP_1..OP_16, direct pushes, OP_PUSHDATA1/2/4) and interpreted as a
// CScriptNum, then the encoding is required to be the canonical minimal push. This matches
// bitcoin-sv's ContextualCheckBlock, which builds expect = CScript() << nHeight and requires the
// scriptSig to begin with exactly those bytes: a non-minimal push, an OP_PUSHDATA-prefixed push, a
// wrong small-int form, or a negative/out-of-range value all fail that prefix-equality check, so we
// reject them here too. This keeps Teranode's accept/reject behaviour in parity with SV Node.
//
// On a decode/parity failure that still yielded a valid push boundary, the arbitrary text is
// returned alongside the error (best-effort) so ExtractCoinbaseMinerRaw — which suppresses
// ErrBlockCoinbaseMissingHeight — can still surface a miner tag for legacy non-canonical coinbases.
func extractCoinbaseHeightAndText(sigScript bscript.Script, raw bool) (uint32, string, error) {
	value, consumed, err := decodeCoinbaseHeightPush(sigScript)
	if err != nil {
		return 0, "", err
	}

	arbitraryText := textForMode(string(sigScript[consumed:]), raw)

	// Reject negative or out-of-range heights. OP_1NEGATE and sign-bit CScriptNums decode negative;
	// a value above MaxUint32 can never match a real block height and would overflow the cast below.
	if value < 0 || value > math.MaxUint32 {
		return 0, arbitraryText, errors.NewBlockCoinbaseMissingHeightError("the coinbase signature script block height is not a valid height")
	}

	// Enforce canonical minimal encoding (SV Node prefix-equality parity): the consumed prefix must
	// equal CScript() << value byte-for-byte. value is guaranteed to be in [0, MaxUint32] here.
	if !bytes.Equal(EncodeCoinbaseHeightPush(uint32(value)), sigScript[:consumed]) {
		return 0, arbitraryText, errors.NewBlockCoinbaseMissingHeightError("the coinbase signature script block height is not minimally encoded")
	}

	return uint32(value), arbitraryText, nil
}

// textForMode returns the coinbase arbitrary text either raw or run through the miner sanitizer.
func textForMode(text string, raw bool) string {
	if raw {
		return text
	}

	return extractMiner(text)
}

// decodeCoinbaseHeightPush reads the first data push of a coinbase scriptSig using Bitcoin script
// push-opcode semantics and returns the pushed value interpreted as a CScriptNum together with the
// number of script bytes the push occupied. It does not enforce minimal encoding; the caller does
// that by re-encoding the value canonically. All failures return ErrBlockCoinbaseMissingHeight so
// that the miner-display path (which suppresses that error) keeps working.
func decodeCoinbaseHeightPush(s bscript.Script) (int64, int, error) {
	missing := func(msg string) (int64, int, error) {
		return 0, 0, errors.NewBlockCoinbaseMissingHeightError(msg)
	}

	if len(s) < 1 {
		return missing("the coinbase signature script must start with the serialized block height")
	}

	op := s[0]

	// pushData reads a length-prefixed push whose header (opcode + length bytes) is headerLen bytes
	// long and whose data length is n, returning the decoded CScriptNum and total bytes consumed.
	pushData := func(headerLen int, n uint64) (int64, int, error) {
		if n > maxHeightBytes {
			return missing("serialized block height too large")
		}

		end := headerLen + int(n)
		if len(s) < end {
			return missing("the coinbase signature script is too short for the serialized block height")
		}

		return decodeScriptNum(s[headerLen:end]), end, nil
	}

	switch {
	case op == bscript.Op0: // OP_0 pushes an empty array => value 0
		return 0, 1, nil
	case op == bscript.Op1NEGATE: // OP_1NEGATE => -1 (rejected as an invalid height by the caller)
		return -1, 1, nil
	case op >= bscript.Op1 && op <= bscript.Op16: // OP_1..OP_16 => small integers 1..16
		return int64(op-bscript.Op1) + 1, 1, nil
	case op >= bscript.OpDATA1 && op <= bscript.OpDATA75: // direct push of `op` bytes
		return pushData(1, uint64(op))
	case op == bscript.OpPUSHDATA1:
		if len(s) < 2 {
			return missing("the coinbase signature script is too short for an OP_PUSHDATA1 block height")
		}

		return pushData(2, uint64(s[1]))
	case op == bscript.OpPUSHDATA2:
		if len(s) < 3 {
			return missing("the coinbase signature script is too short for an OP_PUSHDATA2 block height")
		}

		return pushData(3, uint64(binary.LittleEndian.Uint16(s[1:3])))
	case op == bscript.OpPUSHDATA4:
		if len(s) < 5 {
			return missing("the coinbase signature script is too short for an OP_PUSHDATA4 block height")
		}

		return pushData(5, uint64(binary.LittleEndian.Uint32(s[1:5])))
	default: // first opcode is not a data push, so no block height is present
		return missing("the coinbase signature script must start with the serialized block height")
	}
}

// decodeScriptNum decodes up to maxHeightBytes bytes as a Bitcoin CScriptNum: little-endian,
// sign-magnitude, where the most-significant bit of the last byte is the sign. The caller guarantees
// len(b) <= maxHeightBytes (8), so the magnitude always fits in a uint64 after the sign bit is
// cleared and the result fits in int64.
func decodeScriptNum(b []byte) int64 {
	if len(b) == 0 {
		return 0
	}

	var mag uint64
	for i := 0; i < len(b); i++ {
		mag |= uint64(b[i]) << (8 * uint(i))
	}

	if b[len(b)-1]&0x80 != 0 {
		mag &^= uint64(0x80) << (8 * uint(len(b)-1)) // clear the sign bit
		return -int64(mag)
	}

	return int64(mag)
}

// EncodeCoinbaseHeightPush returns the canonical BIP34 coinbase-height push for height, matching
// bitcoin-sv's CScript() << nHeight (push_int64) for non-negative heights:
//
//   - 0        => OP_0
//   - 1..16    => OP_1..OP_16
//   - otherwise => a minimal-length CScriptNum push (0x01<b>, 0x02<b><b>, ...)
//
// It is the single source of truth for the encoding: extractCoinbaseHeightAndText uses it to enforce
// SV Node parity, and makeCoinbase1 uses it to build Teranode's own coinbase. The uint32 argument
// makes the non-negative contract explicit — block heights are always non-negative and fit in
// uint32, and OP_1NEGATE (the push_int64 case for -1) can never be a height.
func EncodeCoinbaseHeightPush(height uint32) []byte {
	switch {
	case height == 0:
		return []byte{bscript.Op0}
	case height <= 16:
		return []byte{bscript.Op1 + byte(height-1)}
	default:
		num := encodeScriptNumMinimal(height)
		out := make([]byte, 0, len(num)+1)
		out = append(out, byte(len(num)))
		out = append(out, num...)

		return out
	}
}

// encodeScriptNumMinimal encodes a non-negative n as a minimal little-endian CScriptNum (the byte
// form pushed by bitcoin-sv's CScriptNum::serialize), appending a 0x00 padding byte when the top bit
// would otherwise be misread as the sign.
func encodeScriptNumMinimal(n uint32) []byte {
	var result []byte
	for n > 0 {
		result = append(result, byte(n&0xff))
		n >>= 8
	}

	if len(result) > 0 && result[len(result)-1]&0x80 != 0 {
		result = append(result, 0x00) // sign-bit pad
	}

	return result
}

func extractMiner(data string) string {
	if len(data) == 0 {
		return ""
	}

	// Simple approach: keep only printable UTF-8 characters
	// This preserves human-readable text while removing binary data
	var result strings.Builder

	for _, r := range data {
		// Keep printable characters that are valid UTF-8
		if unicode.IsPrint(r) && r != unicodeReplacementChar {
			result.WriteRune(r)
		}
	}

	// Trim any leading/trailing spaces and quotes
	cleaned := strings.TrimSpace(result.String())
	cleaned = strings.Trim(cleaned, "\"")

	// Find the first slash
	firstSlash := strings.Index(cleaned, "/")
	if firstSlash == -1 {
		// No slashes, return as is
		return cleaned
	}

	// Remove everything before the first slash
	cleaned = cleaned[firstSlash:]

	// If it has 2 slashes, remove everything after the 2nd slash
	slashCount := 0
	for i, r := range cleaned {
		if r == '/' {
			slashCount++
			if slashCount == minerSlashTruncationCount {
				// Truncate after this slash (the 2nd slash)
				return cleaned[:i+1]
			}
		}
	}

	return cleaned
}
