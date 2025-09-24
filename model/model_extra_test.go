package model

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test data helpers
func createTestMiningCandidate() *MiningCandidate {
	return &MiningCandidate{
		Id:            []byte{0x01, 0x02, 0x03, 0x04},
		PreviousHash:  []byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30},
		CoinbaseValue: 625000000,
		Version:       1,
		NBits:         []byte{0x20, 0x7f, 0xff, 0xff},
		Time:          1609459200,
		Height:        700000,
		MerkleProof:   [][]byte{[]byte{0x31, 0x32}, []byte{0x33, 0x34}},
	}
}

func createTestMiningSolution() *MiningSolution {
	time := uint32(1609459200)
	version := uint32(1)
	return &MiningSolution{
		Id:       []byte{0x01, 0x02, 0x03, 0x04},
		Coinbase: []byte{0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00},
		Time:     &time,
		Nonce:    123456789,
		Version:  &version,
	}
}

func createTestMiningSolutionWithNilFields() *MiningSolution {
	return &MiningSolution{
		Id:       []byte{0x01, 0x02, 0x03, 0x04},
		Coinbase: []byte{0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00},
		Time:     nil,
		Nonce:    987654321,
		Version:  nil,
	}
}

func createTestNotification() *Notification {
	hash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	return &Notification{
		Type: NotificationType_Block,
		Hash: hash,
	}
}

func createTestBlockInfo() *BlockInfo {
	// Create a simple block header with valid data
	blockHeader := make([]byte, 80)
	// Version (4 bytes, little-endian)
	blockHeader[0] = 0x01
	blockHeader[1] = 0x00
	blockHeader[2] = 0x00
	blockHeader[3] = 0x00

	// Previous block hash (32 bytes)
	for i := 4; i < 36; i++ {
		blockHeader[i] = byte(i - 4)
	}

	// Merkle root (32 bytes)
	for i := 36; i < 68; i++ {
		blockHeader[i] = byte(i - 36)
	}

	// Timestamp (4 bytes, little-endian) - Unix timestamp 1609459200 (2021-01-01)
	blockHeader[68] = 0x00
	blockHeader[69] = 0x90
	blockHeader[70] = 0xf6
	blockHeader[71] = 0x5f

	// Bits (4 bytes)
	blockHeader[72] = 0xff
	blockHeader[73] = 0xff
	blockHeader[74] = 0x7f
	blockHeader[75] = 0x20

	// Nonce (4 bytes)
	blockHeader[76] = 0x15
	blockHeader[77] = 0xcd
	blockHeader[78] = 0x5b
	blockHeader[79] = 0x07

	return &BlockInfo{
		Height:           700000,
		BlockHeader:      blockHeader,
		Miner:            "Test Miner",
		CoinbaseValue:    625000000,
		TransactionCount: 2500,
		Size:             1048576,
	}
}

func createTestBlockInfoWithSpecialChars() *BlockInfo {
	blockHeader := make([]byte, 80)
	// Simple valid header
	blockHeader[0] = 0x01
	blockHeader[68] = 0x00
	blockHeader[69] = 0x90
	blockHeader[70] = 0xf6
	blockHeader[71] = 0x5f

	return &BlockInfo{
		Height:           700001,
		BlockHeader:      blockHeader,
		Miner:            "Test\"Miner\\With/Special\tChars\n",
		CoinbaseValue:    625000000,
		TransactionCount: 1000,
		Size:             512000,
	}
}

func TestMiningCandidate_Stringify_Short(t *testing.T) {
	mc := createTestMiningCandidate()

	result := mc.Stringify(false)

	// Should contain short format info
	assert.Contains(t, result, "Mining Candidate")
	assert.Contains(t, result, "transactions")

	// Should calculate transaction count as 2^(merkle proof length) = 2^2 = 4
	assert.Contains(t, result, "4 transactions")

	// Should not contain detailed info
	assert.NotContains(t, result, "Job ID:")
	assert.NotContains(t, result, "Previous hash:")
	assert.NotContains(t, result, "\n\t")
}

func TestMiningCandidate_Stringify_Long(t *testing.T) {
	mc := createTestMiningCandidate()

	result := mc.Stringify(true)

	// Should contain all detailed info
	assert.Contains(t, result, "Mining Candidate")
	assert.Contains(t, result, "4 transactions") // 2^2 = 4
	assert.Contains(t, result, "Job ID:")
	assert.Contains(t, result, "Previous hash:")
	assert.Contains(t, result, "Coinbase value:")
	assert.Contains(t, result, "Version:")
	assert.Contains(t, result, "nBits:")
	assert.Contains(t, result, "Time:")
	assert.Contains(t, result, "Height:")

	// Should contain specific values
	assert.Contains(t, result, "625000000")  // Coinbase value
	assert.Contains(t, result, "1609459200") // Time
	assert.Contains(t, result, "700000")     // Height
	assert.Contains(t, result, "1")          // Version

	// Should contain formatting
	assert.Contains(t, result, "\n\t")

	// Job ID should be hex-encoded and reversed
	assert.Contains(t, result, "04030201")
}

func TestMiningCandidate_Stringify_EmptyMerkleProof(t *testing.T) {
	mc := &MiningCandidate{
		Id:           []byte{0x01, 0x02},
		PreviousHash: []byte{0x11, 0x12},
		MerkleProof:  [][]byte{}, // Empty merkle proof
		Height:       100,
	}

	result := mc.Stringify(false)

	// Should handle empty merkle proof: 2^0 = 1
	assert.Contains(t, result, "1 transactions")
}

func TestMiningCandidate_Stringify_LargeMerkleProof(t *testing.T) {
	mc := &MiningCandidate{
		Id:           []byte{0x01, 0x02},
		PreviousHash: []byte{0x11, 0x12},
		MerkleProof:  make([][]byte, 10), // 10 elements = 2^10 = 1024 transactions
		Height:       100,
	}

	result := mc.Stringify(false)

	// Should calculate correctly: 2^10 = 1024
	assert.Contains(t, result, "1024 transactions")
}

func TestMiningSolution_Stringify_Short(t *testing.T) {
	ms := createTestMiningSolution()

	result := ms.Stringify(false)

	// Should contain short format
	assert.Contains(t, result, "Mining Solution for job")
	assert.Contains(t, result, "04030201") // Reversed hex ID

	// Should not contain detailed info
	assert.NotContains(t, result, "Nonce:")
	assert.NotContains(t, result, "Time:")
	assert.NotContains(t, result, "\n\t")
}

func TestMiningSolution_Stringify_Long(t *testing.T) {
	ms := createTestMiningSolution()

	result := ms.Stringify(true)

	// Should contain all detailed info
	assert.Contains(t, result, "Mining Solution")
	assert.Contains(t, result, "Job ID:")
	assert.Contains(t, result, "Nonce:")
	assert.Contains(t, result, "Time:")
	assert.Contains(t, result, "Version:")
	assert.Contains(t, result, "CoinbaseTX:")

	// Should contain specific values
	assert.Contains(t, result, "04030201")   // Job ID (reversed hex)
	assert.Contains(t, result, "123456789")  // Nonce
	assert.Contains(t, result, "1609459200") // Time
	assert.Contains(t, result, "1")          // Version

	// Should contain formatting
	assert.Contains(t, result, "\n\t")

	// Coinbase should be hex encoded
	assert.Contains(t, result, "01000000010000000000")
}

func TestMiningSolution_Stringify_WithNilFields(t *testing.T) {
	ms := createTestMiningSolutionWithNilFields()

	result := ms.Stringify(true)

	// Should handle nil Time field
	assert.Contains(t, result, "Time:           <nil>")

	// Should handle nil Version field
	assert.Contains(t, result, "Version:        <nil>")

	// Should still contain non-nil fields
	assert.Contains(t, result, "Job ID:")
	assert.Contains(t, result, "Nonce:")
	assert.Contains(t, result, "987654321") // Nonce value
}

func TestNotification_Stringify(t *testing.T) {
	n := createTestNotification()

	result := n.Stringify()

	// Should contain type and hash
	assert.Contains(t, result, "Type:")
	assert.Contains(t, result, "Hash:")
	assert.Contains(t, result, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
}

func TestNotification_Stringify_NilHash(t *testing.T) {
	n := &Notification{
		Type: NotificationType_Block,
		Hash: nil,
	}

	// Should panic with nil hash because Hash.String() is called
	assert.Panics(t, func() {
		_ = n.Stringify()
	})
}

func TestBlockInfo_MarshalJSON(t *testing.T) {
	bi := createTestBlockInfo()

	jsonBytes, err := bi.MarshalJSON()
	require.NoError(t, err)
	require.NotNil(t, jsonBytes)

	// Parse the JSON to verify structure
	var parsed map[string]interface{}
	err = json.Unmarshal(jsonBytes, &parsed)
	require.NoError(t, err)

	// Verify all expected fields are present
	assert.Contains(t, parsed, "height")
	assert.Contains(t, parsed, "hash")
	assert.Contains(t, parsed, "previousblockhash")
	assert.Contains(t, parsed, "coinbaseValue")
	assert.Contains(t, parsed, "timestamp")
	assert.Contains(t, parsed, "transactionCount")
	assert.Contains(t, parsed, "size")
	assert.Contains(t, parsed, "miner")

	// Verify specific values
	assert.Equal(t, float64(700000), parsed["height"])
	assert.Equal(t, float64(625000000), parsed["coinbaseValue"])
	assert.Equal(t, float64(2500), parsed["transactionCount"])
	assert.Equal(t, float64(1048576), parsed["size"])
	assert.Equal(t, "Test Miner", parsed["miner"])

	// Verify timestamp format (ISO 8601)
	timestamp, ok := parsed["timestamp"].(string)
	require.True(t, ok)
	timestampRegex := regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z`)
	assert.True(t, timestampRegex.MatchString(timestamp), "Timestamp should match ISO 8601 format")

	// Verify hash format (64 character hex string)
	hash, ok := parsed["hash"].(string)
	require.True(t, ok)
	assert.Len(t, hash, 64)
	hashRegex := regexp.MustCompile(`^[a-fA-F0-9]{64}$`)
	assert.True(t, hashRegex.MatchString(hash), "Hash should be 64 character hex string")

	// Verify previous block hash format
	prevHash, ok := parsed["previousblockhash"].(string)
	require.True(t, ok)
	assert.Len(t, prevHash, 64)
	prevHashRegex := regexp.MustCompile(`^[a-fA-F0-9]{64}$`)
	assert.True(t, prevHashRegex.MatchString(prevHash), "Previous hash should be 64 character hex string")
}

func TestBlockInfo_MarshalJSON_WithSpecialChars(t *testing.T) {
	bi := createTestBlockInfoWithSpecialChars()

	jsonBytes, err := bi.MarshalJSON()
	require.NoError(t, err)
	require.NotNil(t, jsonBytes)

	// Parse the JSON to verify it's valid
	var parsed map[string]interface{}
	err = json.Unmarshal(jsonBytes, &parsed)
	require.NoError(t, err)

	// Verify special characters are properly escaped
	miner, ok := parsed["miner"].(string)
	require.True(t, ok)
	assert.Equal(t, "Test\"Miner\\With/Special\tChars\n", miner)
}

func TestBlockInfo_MarshalJSON_InvalidBlockHeader(t *testing.T) {
	bi := &BlockInfo{
		Height:           700000,
		BlockHeader:      []byte{0x01, 0x02}, // Invalid - too short
		Miner:            "Test Miner",
		CoinbaseValue:    625000000,
		TransactionCount: 1000,
		Size:             512000,
	}

	jsonBytes, err := bi.MarshalJSON()

	// Should return error for invalid block header
	assert.Error(t, err)
	assert.Nil(t, jsonBytes)
}

func TestBlockInfo_MarshalJSON_TimestampFormat(t *testing.T) {
	// Create block header with specific timestamp
	blockHeader := make([]byte, 80)
	blockHeader[0] = 0x01 // Version

	// Set timestamp to Unix epoch (0) - should be 1970-01-01T00:00:00.000Z
	blockHeader[68] = 0x00
	blockHeader[69] = 0x00
	blockHeader[70] = 0x00
	blockHeader[71] = 0x00

	bi := &BlockInfo{
		Height:      1,
		BlockHeader: blockHeader,
		Miner:       "Genesis",
	}

	jsonBytes, err := bi.MarshalJSON()
	require.NoError(t, err)

	var parsed map[string]interface{}
	err = json.Unmarshal(jsonBytes, &parsed)
	require.NoError(t, err)

	timestamp := parsed["timestamp"].(string)
	assert.Equal(t, "1970-01-01T00:00:00.000Z", timestamp)
}

func TestEscapeJSON(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Simple string",
			input:    "Hello World",
			expected: "Hello World",
		},
		{
			name:     "String with quotes",
			input:    "Hello \"World\"",
			expected: "Hello \\\"World\\\"",
		},
		{
			name:     "String with backslashes",
			input:    "Hello\\World",
			expected: "Hello\\\\World",
		},
		{
			name:     "String with newlines",
			input:    "Hello\nWorld",
			expected: "Hello\\nWorld",
		},
		{
			name:     "String with tabs",
			input:    "Hello\tWorld",
			expected: "Hello\\tWorld",
		},
		{
			name:     "String with carriage returns",
			input:    "Hello\rWorld",
			expected: "Hello\\rWorld",
		},
		{
			name:     "Empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "String with forward slashes",
			input:    "Hello/World",
			expected: "Hello/World", // Forward slashes don't need escaping in JSON strings
		},
		{
			name:     "Complex string with multiple special chars",
			input:    "Test\"Line1\nLine2\tTab\\Backslash",
			expected: "Test\\\"Line1\\nLine2\\tTab\\\\Backslash",
		},
		{
			name:     "Unicode characters",
			input:    "Hello 世界",
			expected: "Hello 世界",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := escapeJSON(tc.input)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestEscapeJSON_EdgeCases(t *testing.T) {
	// Test with all control characters
	controlChars := "\u0000\u0001\u0002\u0003\u0004\u0005\u0006\u0007\u0008\u0009\u000a\u000b\u000c\u000d\u000e\u000f"
	result, err := escapeJSON(controlChars)
	require.NoError(t, err)
	assert.NotEmpty(t, result)

	// The result should be properly escaped
	assert.Contains(t, result, "\\u0000")
	assert.Contains(t, result, "\\t") // Tab character
	assert.Contains(t, result, "\\n") // Newline character
	assert.Contains(t, result, "\\r") // Carriage return
}

func TestDateFormat_Constant(t *testing.T) {
	// Test that the date format constant is correct ISO 8601 format
	testTime := time.Date(2021, 1, 1, 15, 30, 45, 123000000, time.UTC)
	formatted := testTime.Format(dateFormat)
	assert.Equal(t, "2021-01-01T15:30:45.123Z", formatted)
}

// Integration tests combining multiple functions
func TestMiningCandidate_Stringify_Integration(t *testing.T) {
	mc := &MiningCandidate{
		Id:            []byte{0xde, 0xad, 0xbe, 0xef},
		PreviousHash:  make([]byte, 32),
		CoinbaseValue: 1000000000,
		Version:       536870912,
		NBits:         []byte{0x18, 0x01, 0x23, 0x45},
		Time:          uint32(time.Now().Unix()),
		Height:        800000,
		MerkleProof:   make([][]byte, 16), // 2^16 = 65536 transactions
	}

	// Fill previous hash with pattern
	for i := 0; i < 32; i++ {
		mc.PreviousHash[i] = byte(i)
	}

	// Test short format
	shortResult := mc.Stringify(false)
	assert.Contains(t, shortResult, "65536 transactions")
	assert.NotContains(t, shortResult, "\n")

	// Test long format
	longResult := mc.Stringify(true)
	assert.Contains(t, longResult, "65536 transactions")
	assert.Contains(t, longResult, "efbeadde")   // Reversed hex ID
	assert.Contains(t, longResult, "1000000000") // Coinbase value
	assert.Contains(t, longResult, "536870912")  // Version
	assert.Contains(t, longResult, "800000")     // Height
	assert.Contains(t, longResult, "45230118")   // Reversed nBits
	assert.Contains(t, longResult, "\n\t")       // Formatting
}

func TestMiningSolution_Stringify_Integration(t *testing.T) {
	// Test with maximum values
	maxTime := uint32(4294967295)
	maxVersion := uint32(4294967295)

	ms := &MiningSolution{
		Id:       []byte{0xff, 0xff, 0xff, 0xff},
		Coinbase: []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff},
		Time:     &maxTime,
		Nonce:    4294967295,
		Version:  &maxVersion,
	}

	result := ms.Stringify(true)

	// Should handle maximum values correctly
	assert.Contains(t, result, "ffffffff")                   // Max ID reversed
	assert.Contains(t, result, "4294967295")                 // Max nonce
	assert.Contains(t, result, "4294967295")                 // Max time
	assert.Contains(t, result, "4294967295")                 // Max version
	assert.Contains(t, result, "010000000000000000000000ff") // Coinbase hex (without leading zero)
}

func TestBlockInfo_MarshalJSON_Integration(t *testing.T) {
	// Test with realistic block data
	bi := createTestBlockInfo()

	// Test multiple marshaling calls to ensure consistency
	for i := 0; i < 3; i++ {
		jsonBytes, err := bi.MarshalJSON()
		require.NoError(t, err)

		var parsed map[string]interface{}
		err = json.Unmarshal(jsonBytes, &parsed)
		require.NoError(t, err)

		// Verify consistency across multiple calls
		assert.Equal(t, float64(700000), parsed["height"])
		assert.Equal(t, "Test Miner", parsed["miner"])

		// Verify JSON is properly formatted
		jsonStr := string(jsonBytes)
		assert.Contains(t, jsonStr, `"height": 700000`)
		assert.Contains(t, jsonStr, `"miner": "Test Miner"`)
		assert.Contains(t, jsonStr, `"coinbaseValue": 625000000`)
		assert.Contains(t, jsonStr, `"transactionCount": 2500`)

		// Verify proper JSON structure (no trailing commas, proper braces)
		lines := strings.Split(strings.TrimSpace(jsonStr), "\n")
		assert.True(t, strings.HasPrefix(strings.TrimSpace(lines[0]), "{"))
		assert.True(t, strings.HasSuffix(strings.TrimSpace(lines[len(lines)-1]), "}"))
	}
}

// Benchmark tests
func BenchmarkMiningCandidate_Stringify_Short(b *testing.B) {
	mc := createTestMiningCandidate()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = mc.Stringify(false)
	}
}

func BenchmarkMiningCandidate_Stringify_Long(b *testing.B) {
	mc := createTestMiningCandidate()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = mc.Stringify(true)
	}
}

func BenchmarkMiningSolution_Stringify(b *testing.B) {
	ms := createTestMiningSolution()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ms.Stringify(true)
	}
}

func BenchmarkBlockInfo_MarshalJSON(b *testing.B) {
	bi := createTestBlockInfo()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = bi.MarshalJSON()
	}
}

func BenchmarkEscapeJSON(b *testing.B) {
	input := "Test\"String\\With\nSpecial\tChars"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = escapeJSON(input)
	}
}
