package consensus

import (
	"encoding/binary"
	"encoding/hex"
	"strconv"
	"strings"
	"unicode"

	"github.com/bitcoin-sv/teranode/errors"
)

// ScriptParser parses Bitcoin script strings into bytecode
type ScriptParser struct {
	strict bool // If true, match C++ ParseScript behavior exactly
}

// NewScriptParser creates a new script parser
func NewScriptParser() *ScriptParser {
	return &ScriptParser{strict: false}
}

// NewStrictScriptParser creates a parser that matches C++ behavior
func NewStrictScriptParser() *ScriptParser {
	return &ScriptParser{strict: true}
}

// ParseScript parses a script string into bytecode
// Handles mixed format: opcodes, hex values (0x...), numbers, quoted strings
func (p *ScriptParser) ParseScript(script string) ([]byte, error) {
	if script == "" {
		return []byte{}, nil
	}

	tokens, err := p.tokenize(script)
	if err != nil {
		return nil, errors.NewProcessingError("tokenization failed", err)
	}

	var result []byte
	i := 0

	for i < len(tokens) {
		token := tokens[i]

		if token == "" {
			i++
			continue
		}

		// Handle hex values with 0x prefix
		if strings.HasPrefix(token, "0x") {
			hexData := token[2:]

			// In strict mode, validate hex length
			if p.strict {
				if len(hexData) == 0 {
					return nil, errors.NewProcessingError("empty hex string")
				}
				if len(hexData)%2 != 0 {
					return nil, errors.NewProcessingError("hex string has odd number of characters")
				}
			}

			// Check if this should be treated as a raw byte or wrapped in a push
			// Single byte hex values in range 0x01-0x4b are often meant as raw push opcodes
			if len(hexData) == 2 { // Single byte
				if val, err := strconv.ParseUint(hexData, 16, 8); err == nil {
					if val >= 1 && val <= 75 { // 0x01 to 0x4b are push opcodes
						// Treat as raw byte (push opcode)
						result = append(result, byte(val))

						// Special handling: if the next token is hex data of the exact length
						// specified by this push opcode, parse it as raw bytes without wrapping
						// it in another push operation (because we already have the push opcode)
						if i+1 < len(tokens) && strings.HasPrefix(tokens[i+1], "0x") {
							nextHexData := tokens[i+1][2:]
							expectedLen := int(val) * 2 // Each byte is 2 hex chars
							if len(nextHexData) == expectedLen {
								// Parse the next token as raw hex without push wrapper
								rawBytes, err := p.parseRawHex(tokens[i+1])
								if err != nil {
									return nil, errors.NewProcessingError("invalid hex data after push opcode", err)
								}
								result = append(result, rawBytes...)
								i += 2 // Skip both tokens
								continue
							}
						}

						i++
						continue
					}
				}
			}

			// Otherwise, parse as normal hex data with push operation
			bytes, err := p.parseHex(token)
			if err != nil {
				return nil, errors.NewProcessingError("invalid hex value '%s'", token, err)
			}
			result = append(result, bytes...)
			i++
			continue
		}

		// Handle quoted strings
		if strings.HasPrefix(token, "'") && strings.HasSuffix(token, "'") {
			if len(token) < 2 {
				return nil, errors.NewProcessingError("invalid quoted string: %s", token)
			}
			str := token[1 : len(token)-1] // Remove quotes
			pushOp := CreatePushOp([]byte(str))
			result = append(result, pushOp...)
			i++
			continue
		}

		// Handle numeric literals
		if opcode, isNum := IsNumericLiteral(token); isNum {
			result = append(result, opcode)
			i++
			continue
		}

		// Handle larger numbers that need push operations
		if num, err := strconv.ParseInt(token, 10, 64); err == nil {
			pushOp, err := p.createNumberPush(num)
			if err != nil {
				return nil, errors.NewProcessingError("failed to create push for number %d", num, err)
			}
			result = append(result, pushOp...)
			i++
			continue
		}

		// Handle special cases like PUSHDATA operations BEFORE checking opcodes
		if token == "PUSHDATA1" || token == "PUSHDATA2" || token == "PUSHDATA4" {
			pushBytes, consumed, err := p.parsePushData(tokens[i:])
			if err != nil {
				return nil, errors.NewProcessingError("failed to parse PUSHDATA", err)
			}
			result = append(result, pushBytes...)
			i += consumed
			continue
		}

		// Handle opcodes
		if IsValidOpcode(token) {
			// In strict mode, disallow OP_0 through OP_16 (use numeric literals instead)
			if p.strict && isNumericOpcode(token) {
				return nil, errors.NewProcessingError("use numeric value, not %s", token)
			}

			opcode, _ := GetOpcodeValue(token)
			result = append(result, opcode)
			i++
			continue
		}

		return nil, errors.NewProcessingError("unknown token: %s", token)
	}

	return result, nil
}

// tokenize splits a script string into tokens
func (p *ScriptParser) tokenize(script string) ([]string, error) {
	var tokens []string
	var currentToken strings.Builder
	var inQuotes bool
	var inHex bool

	for i, r := range script {
		switch {
		case r == '\'' && !inHex:
			// Handle quoted strings
			if inQuotes {
				currentToken.WriteRune(r)
				tokens = append(tokens, currentToken.String())
				currentToken.Reset()
				inQuotes = false
			} else {
				if currentToken.Len() > 0 {
					tokens = append(tokens, currentToken.String())
					currentToken.Reset()
				}
				currentToken.WriteRune(r)
				inQuotes = true
			}

		case inQuotes:
			// Inside quotes, add everything
			currentToken.WriteRune(r)

		case unicode.IsSpace(r):
			// Whitespace separator
			if currentToken.Len() > 0 {
				tokens = append(tokens, currentToken.String())
				currentToken.Reset()
			}
			inHex = false

		case r == '0' && i+1 < len(script) && script[i+1] == 'x':
			// Start of hex value
			if currentToken.Len() > 0 {
				tokens = append(tokens, currentToken.String())
				currentToken.Reset()
			}
			currentToken.WriteRune(r)
			inHex = true

		default:
			currentToken.WriteRune(r)
		}
	}

	// Add final token
	if currentToken.Len() > 0 {
		tokens = append(tokens, currentToken.String())
	}

	return tokens, nil
}

// parseHex parses a hex string with 0x prefix
func (p *ScriptParser) parseHex(hexStr string) ([]byte, error) {
	if !strings.HasPrefix(hexStr, "0x") {
		return nil, errors.NewProcessingError("hex string must start with 0x")
	}

	hexData := hexStr[2:]
	if len(hexData) == 0 {
		return CreatePushOp([]byte{}), nil
	}

	// Handle odd length hex strings by padding with leading zero
	if len(hexData)%2 == 1 {
		hexData = "0" + hexData
	}

	bytes, err := hex.DecodeString(hexData)
	if err != nil {
		return nil, errors.NewProcessingError("invalid hex data", err)
	}

	// Create push operation for the hex data
	return CreatePushOp(bytes), nil
}

// parseRawHex parses a hex string and returns raw bytes without push operation
func (p *ScriptParser) parseRawHex(hexStr string) ([]byte, error) {
	if !strings.HasPrefix(hexStr, "0x") {
		return nil, errors.NewProcessingError("hex string must start with 0x")
	}

	hexData := hexStr[2:]
	if len(hexData) == 0 {
		return []byte{}, nil
	}

	// Handle odd length hex strings by padding with leading zero
	if len(hexData)%2 == 1 {
		hexData = "0" + hexData
	}

	bytes, err := hex.DecodeString(hexData)
	if err != nil {
		return nil, errors.NewProcessingError("invalid hex data", err)
	}

	return bytes, nil
}

// createNumberPush creates a push operation for a number
func (p *ScriptParser) createNumberPush(num int64) ([]byte, error) {
	if num == -1 {
		return []byte{0x4f}, nil // OP_1NEGATE
	}

	if num == 0 {
		return []byte{0x00}, nil // OP_0
	}

	if num >= 1 && num <= 16 {
		return []byte{byte(0x50 + num)}, nil // OP_1 through OP_16
	}

	// For other numbers, encode as little-endian bytes
	var bytes []byte
	negative := num < 0
	if negative {
		num = -num
	}

	// Convert to little-endian bytes
	for num > 0 {
		bytes = append(bytes, byte(num&0xff))
		num >>= 8
	}

	// Add sign bit if negative
	if negative {
		if len(bytes) == 0 {
			bytes = []byte{0x80}
		} else if bytes[len(bytes)-1]&0x80 != 0 {
			// Need extra byte for sign
			bytes = append(bytes, 0x80)
		} else {
			bytes[len(bytes)-1] |= 0x80
		}
	}

	return CreatePushOp(bytes), nil
}

// parsePushData parses PUSHDATA operations
func (p *ScriptParser) parsePushData(tokens []string) ([]byte, int, error) {
	if len(tokens) == 0 {
		return nil, 0, errors.NewProcessingError("empty token list")
	}

	pushType := tokens[0]

	switch pushType {
	case "PUSHDATA1":
		if len(tokens) < 2 {
			return nil, 0, errors.NewProcessingError("PUSHDATA1 requires length")
		}

		lengthToken := tokens[1]

		// Parse length
		var lengthValue uint8
		if strings.HasPrefix(lengthToken, "0x") {
			// Parse as hex byte value
			val, err := strconv.ParseUint(lengthToken[2:], 16, 8)
			if err != nil {
				return nil, 0, errors.NewProcessingError("invalid PUSHDATA1 length", err)
			}
			lengthValue = uint8(val)
		} else {
			// Parse as decimal number
			val, err := strconv.ParseUint(lengthToken, 10, 8)
			if err != nil {
				return nil, 0, errors.NewProcessingError("invalid PUSHDATA1 length", err)
			}
			lengthValue = uint8(val)
		}

		// Handle zero-length data
		if lengthValue == 0 {
			result := []byte{0x4c, 0x00}
			return result, 2, nil
		}

		// For non-zero length, we need the data token
		if len(tokens) < 3 {
			return nil, 0, errors.NewProcessingError("PUSHDATA1 with length %d requires data", lengthValue)
		}

		dataToken := tokens[2]

		// Parse data using raw hex parser (no push wrapper)
		dataBytes, err := p.parseRawHex(dataToken)
		if err != nil {
			return nil, 0, errors.NewProcessingError("invalid PUSHDATA1 data", err)
		}

		// Verify length matches
		if int(lengthValue) != len(dataBytes) {
			return nil, 0, errors.NewProcessingError("PUSHDATA1 length mismatch: expected %d, got %d", lengthValue, len(dataBytes))
		}

		result := []byte{0x4c, byte(lengthValue)}
		result = append(result, dataBytes...)
		return result, 3, nil

	case "PUSHDATA2":
		if len(tokens) < 2 {
			return nil, 0, errors.NewProcessingError("PUSHDATA2 requires length")
		}

		lengthToken := tokens[1]

		// Parse length value (expecting hex representation of 2-byte little-endian)
		if !strings.HasPrefix(lengthToken, "0x") {
			return nil, 0, errors.NewProcessingError("PUSHDATA2 length must be hex")
		}
		// The length token should be the hex of the 2-byte little-endian length
		lengthHex := lengthToken[2:]
		if len(lengthHex) != 4 { // 2 bytes = 4 hex chars
			return nil, 0, errors.NewProcessingError("PUSHDATA2 length must be 2 bytes (4 hex chars)")
		}
		lengthBytes, err := hex.DecodeString(lengthHex)
		if err != nil {
			return nil, 0, errors.NewProcessingError("invalid PUSHDATA2 length hex", err)
		}

		// Check the actual length value
		expectedLen := binary.LittleEndian.Uint16(lengthBytes)

		// Handle zero-length data
		if expectedLen == 0 {
			result := []byte{0x4d}
			result = append(result, lengthBytes...)
			return result, 2, nil
		}

		// For non-zero length, we need the data token
		if len(tokens) < 3 {
			return nil, 0, errors.NewProcessingError("PUSHDATA2 with length %d requires data", expectedLen)
		}

		dataToken := tokens[2]

		// Parse data using raw hex parser (no push wrapper)
		dataBytes, err := p.parseRawHex(dataToken)
		if err != nil {
			return nil, 0, errors.NewProcessingError("invalid PUSHDATA2 data", err)
		}

		// Verify length matches
		if int(expectedLen) != len(dataBytes) {
			return nil, 0, errors.NewProcessingError("PUSHDATA2 length mismatch: expected %d, got %d", expectedLen, len(dataBytes))
		}

		result := []byte{0x4d}
		result = append(result, lengthBytes...)
		result = append(result, dataBytes...)
		return result, 3, nil

	case "PUSHDATA4":
		if len(tokens) < 3 {
			return nil, 0, errors.NewProcessingError("PUSHDATA4 requires length and data")
		}

		lengthToken := tokens[1]
		dataToken := tokens[2]

		// Parse length value (expecting hex representation of 4-byte little-endian)
		if !strings.HasPrefix(lengthToken, "0x") {
			return nil, 0, errors.NewProcessingError("PUSHDATA4 length must be hex")
		}
		// The length token should be the hex of the 4-byte little-endian length
		lengthHex := lengthToken[2:]
		if len(lengthHex) != 8 { // 4 bytes = 8 hex chars
			return nil, 0, errors.NewProcessingError("PUSHDATA4 length must be 4 bytes (8 hex chars)")
		}
		lengthBytes, err := hex.DecodeString(lengthHex)
		if err != nil {
			return nil, 0, errors.NewProcessingError("invalid PUSHDATA4 length hex", err)
		}

		// Parse data using raw hex parser (no push wrapper)
		dataBytes, err := p.parseRawHex(dataToken)
		if err != nil {
			return nil, 0, errors.NewProcessingError("invalid PUSHDATA4 data", err)
		}

		// Verify length matches (little-endian)
		expectedLen := binary.LittleEndian.Uint32(lengthBytes)
		if int(expectedLen) != len(dataBytes) {
			return nil, 0, errors.NewProcessingError("PUSHDATA4 length mismatch: expected %d, got %d", expectedLen, len(dataBytes))
		}

		result := []byte{0x4e}
		result = append(result, lengthBytes...)
		result = append(result, dataBytes...)
		return result, 3, nil

	default:
		return nil, 0, errors.NewProcessingError("unknown PUSHDATA type: %s", pushType)
	}
}

// isNumericOpcode checks if the token is OP_0 through OP_16
func isNumericOpcode(token string) bool {
	switch token {
	case "OP_0", "OP_FALSE", "OP_1", "OP_TRUE", "OP_2", "OP_3", "OP_4", "OP_5",
		"OP_6", "OP_7", "OP_8", "OP_9", "OP_10", "OP_11", "OP_12", "OP_13",
		"OP_14", "OP_15", "OP_16":
		return true
	}
	return false
}

// ParseScriptStrict parses a script string with C++ compatible behavior
func ParseScriptStrict(script string) ([]byte, error) {
	parser := NewStrictScriptParser()
	return parser.ParseScript(script)
}
