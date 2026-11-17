package util

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExtractHeightAndMiner consolidates all transaction-based coinbase extraction tests
func TestExtractHeightAndMiner(t *testing.T) {
	testCases := []struct {
		name           string
		tx             string
		expectedHeight uint32
		expectedMiner  string
		heightError    bool
		minerError     bool
	}{
		{
			// Non-minimal 3-byte push (0x03 0910 00) of height 4105 — the encoding the old
			// makeCoinbase1 emitted. Canonical is the 2-byte push 0x02 0910, so SV Node rejects it and
			// so does the parser now. Height extraction errors; miner display still works because
			// ExtractCoinbaseMiner suppresses the missing-height error and returns best-effort text.
			name:          "non-minimal 3-byte height 4105 with m5-cc1 miner is rejected",
			tx:            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff18030910002f6d352d6363312fdcce95f3c057431c486ae662ffffffff0a0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00000000",
			expectedMiner: "/m5-cc1/",
			heightError:   true,
			minerError:    false,
		},
		{
			// Teratestnet block 1 regression (issue #1142): coinbase scriptSig starts with 0x51
			// (OP_1), the canonical BIP34 encoding of height 1. The old parser read 0x51 as a length
			// of 81 bytes and returned BLOCK_COINBASE_MISSING_HEIGHT, stalling from-genesis IBD.
			name:           "teratestnet block 1 OP_1 height with Galts-Gulch miner",
			tx:             "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1251000f222f47616c74732d47756c63682f22ffffffff0100f2052a010000001976a914042848a901a9f0d79eb42c52813a7646c4a81bfd88ac00000000",
			expectedHeight: 1,
			expectedMiner:  "/Galts-Gulch/",
		},
		{
			name:           "2 byte teratestnet-v2 miner",
			tx:             "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1201240f222f47616c74732d47756c63682f22ffffffff0100f2052a010000001976a914042848a901a9f0d79eb42c52813a7646c4a81bfd88ac00000000",
			expectedHeight: 36,
			expectedMiner:  "/Galts-Gulch/",
		},
		{
			name:           "block 514587 with binary miner data",
			tx:             "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff14031bda07074125205a6ad8648d3b00009de70700ffffffff017777954a000000001976a9144770c259bc03c8dc36b853ed19fbb3514190be2e88ac00000000",
			expectedHeight: 514587,
			expectedMiner:  "A% Zjd;",
		},
		{
			name:           "2-byte height 166 CPU miner with quoted tag",
			tx:             "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1002a6000c222f74746e2d65752d312f22ffffffff0100f2052a010000001976a9143f3409ec46b92b65ea9fd16e42345917c9ba2a5088ac00000000",
			expectedHeight: 166,
			expectedMiner:  "/ttn-eu-1/",
		},
		{
			name:           "mainnet block 623947 with ViaBTC miner",
			tx:             "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff5b034b8509182f5669614254432f4d696e6564206279206274633638382f2cfabe6d6d973712210987f693fbe6222fe3705e4655de7d08492d230fb29022778d0ab9b5100000000000000010a837d5171314ce4c77483dc463d38411ffffffff011624a04a000000001976a914f1c075a01882ae0972f95d3a4177c86c852b7d9188ac00000000",
			expectedHeight: 623947,
			expectedMiner:  "/ViaBTC/",
		},
		{
			name:           "3-byte height 856618",
			tx:             "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17032a120d2f71646c6e6b2ffa3e9e2068b1e1743dc80d00ffffffff014864a012000000001976a91417db35d440a673a218e70a5b9d07f895facf50d288ac00000000",
			expectedHeight: 856618,
			expectedMiner:  "/qdlnk/",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tx, err := bt.NewTxFromString(tc.tx)
			require.NoError(t, err)

			// Test height extraction
			height, err := ExtractCoinbaseHeight(tx)
			if tc.heightError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedHeight, height)
			}

			// Test miner extraction
			miner, err := ExtractCoinbaseMiner(tx)
			if tc.minerError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedMiner, miner)
			}
		})
	}
}

// TestExtractCoinbaseHeightPreBIP34RealTx documents that a real pre-BIP34 testnet coinbase whose
// scriptSig happens to begin with 0x60 (OP_16) now decodes to height 16 under push-opcode semantics,
// where the old length-prefix parser errored. This is not a consensus change: both consensus call
// sites skip blocks below the network's BIP34 activation height, so this coinbase is never height-
// checked in production. The trailing bytes are arbitrary extranonce data; only the height decode
// and the absence of an error are asserted (the sanitized miner is meaningless binary noise).
func TestExtractCoinbaseHeightPreBIP34RealTx(t *testing.T) {
	const txHex = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff4a60248ab1830b9f12c743e837ef4feb32dda0739ee6e3aaf3b24aa86883c592f2f3a20df867825d91bc096f3260b20ae49176d3bb8d02a107af4065a59fffeb9235fc991145224be83446ffffffff01e0850a2a010000002321038a6ca672c189b3af86b9894fcd40a3af6bbae7b45db9ee89258c848b68d66af3ac00000000"

	tx, err := bt.NewTxFromString(txHex)
	require.NoError(t, err)

	height, err := ExtractCoinbaseHeight(tx)
	require.NoError(t, err)
	require.Equal(t, uint32(16), height)

	// Miner extraction must not error on this input (its value is arbitrary binary noise).
	_, err = ExtractCoinbaseMiner(tx)
	require.NoError(t, err)
}

// TestExtractCoinbaseHeightAndText_Scripts tests direct script parsing
func TestExtractCoinbaseHeightAndTextScripts(t *testing.T) {
	testCases := []struct {
		name           string
		script         string
		expectedHeight uint32
		expectedMiner  string
		expectError    bool
	}{
		// --- accepts: canonical encodings ---
		{
			name:           "3-byte height 807495 with taal.com miner",
			script:         "0347520c2f7461616c2e636f6d2f79b010ec60689edf8d3a0000",
			expectedHeight: 807495,
			expectedMiner:  "/taal.com/",
		},
		{
			name:           "OP_0 height 0",
			script:         "00",
			expectedHeight: 0,
			expectedMiner:  "",
		},
		{
			name:           "OP_1 height 1 (teratestnet block 1) with Galts-Gulch miner",
			script:         "51000f222f47616c74732d47756c63682f22",
			expectedHeight: 1,
			expectedMiner:  "/Galts-Gulch/",
		},
		{
			name:           "OP_1 height 1 with satoshi miner (canonical form of old fixture)",
			script:         "512f7361746f7368692f",
			expectedHeight: 1,
			expectedMiner:  "/satoshi/",
		},
		{
			name:           "OP_16 height 16",
			script:         "60",
			expectedHeight: 16,
			expectedMiner:  "",
		},
		{
			name:           "1-byte push height 17",
			script:         "0111",
			expectedHeight: 17,
			expectedMiner:  "",
		},
		{
			name:           "2-byte push height 128 (sign-bit pad)",
			script:         "028000",
			expectedHeight: 128,
			expectedMiner:  "",
		},
		{
			name:           "2-byte push height 255 (sign-bit pad)",
			script:         "02ff00",
			expectedHeight: 255,
			expectedMiner:  "",
		},
		{
			name:           "2-byte push height 256",
			script:         "020001",
			expectedHeight: 256,
			expectedMiner:  "",
		},
		{
			name:           "2-byte push height 4105 (canonical m5-cc1) with miner",
			script:         "0209102f6d352d6363312f",
			expectedHeight: 4105,
			expectedMiner:  "/m5-cc1/",
		},
		{
			name:           "3-byte push height 518847",
			script:         "03bfea07",
			expectedHeight: 518847,
			expectedMiner:  "",
		},
		{
			name:           "4-byte push height 67305985 (minimal, still accepted)",
			script:         "0401020304",
			expectedHeight: 0x4030201,
			expectError:    false,
		},
		// --- rejects: invalid or non-canonical encodings (SV Node parity) ---
		{
			name:        "empty script",
			script:      "",
			expectError: true,
		},
		{
			name:        "insufficient data for push",
			script:      "02ab",
			expectError: true,
		},
		{
			name:        "OP_DATA3 declares 3 bytes, only 2 present",
			script:      "03bfea",
			expectError: true,
		},
		{
			name:        "OP_PUSHDATA1 with no length byte",
			script:      "4c",
			expectError: true,
		},
		{
			name:        "OP_PUSHDATA1 declares 3 bytes, only 2 present",
			script:      "4c03bfea",
			expectError: true,
		},
		{
			name:        "OP_PUSHDATA2 with truncated length header",
			script:      "4d03",
			expectError: true,
		},
		{
			name:        "OP_PUSHDATA4 with truncated length header",
			script:      "4e030000",
			expectError: true,
		},
		{
			name:        "OP_PUSHDATA4 declares 3 bytes, payload truncated",
			script:      "4e03000000",
			expectError: true,
		},
		{
			name:        "non-minimal 2-byte push of height 1 (canonical is OP_1)",
			script:      "0201002f7361746f7368692f",
			expectError: true,
		},
		{
			name:        "non-minimal 3-byte push of height 0 (canonical is OP_0)",
			script:      "03000000",
			expectError: true,
		},
		{
			name:        "non-minimal 3-byte push of height 4105 (canonical is 2-byte)",
			script:      "030910002f6d352d6363312f",
			expectError: true,
		},
		{
			name:        "non-minimal 4-byte push of height 518847 (canonical is 3-byte)",
			script:      "04bfea0700",
			expectError: true,
		},
		{
			name:        "OP_PUSHDATA1 push of height 518847 (never canonical)",
			script:      "4c03bfea07",
			expectError: true,
		},
		{
			name:        "OP_PUSHDATA2 push of height 518847 (never canonical)",
			script:      "4d0300bfea07",
			expectError: true,
		},
		{
			name:        "OP_PUSHDATA4 push of height 518847 (never canonical)",
			script:      "4e03000000bfea07",
			expectError: true,
		},
		{
			name:        "OP_1NEGATE is not a valid height",
			script:      "4f",
			expectError: true,
		},
		{
			name:        "1-byte push of -1 (sign bit set) is not a valid height",
			script:      "0181",
			expectError: true,
		},
		{
			name:        "push longer than maxHeightBytes",
			script:      "090102030405060708ff",
			expectError: true,
		},
		{
			name:        "first opcode is not a data push",
			script:      "a90102",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			script, err := bscript.NewFromHexString(tc.script)
			require.NoError(t, err)

			height, miner, err := extractCoinbaseHeightAndText(*script, false)
			if tc.expectError {
				require.Error(t, err)
				require.ErrorIs(t, err, errors.ErrBlockCoinbaseMissingHeight)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedHeight, height)
				assert.Equal(t, tc.expectedMiner, miner)
			}
		})
	}
}

func TestExtractMiner(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "clean miner tag with extra path",
			input:    "/taal.com/US/dksjk",
			expected: "/taal.com/", // Truncated after 2nd slash
		},
		{
			name:     "no slashes",
			input:    "taal.com",
			expected: "taal.com",
		},
		{
			name:     "single segment tag",
			input:    "/taal.com",
			expected: "/taal.com", // Simple UTF-8 extraction
		},
		{
			name:     "tag with trailing slash",
			input:    "/taal.com/",
			expected: "/taal.com/",
		},
		{
			name:     "quoted tag",
			input:    "\f\"/ttn-eu-1/\"",
			expected: "/ttn-eu-1/", // Quotes removed
		},
		{
			name:     "tag with binary data after",
			input:    "/pool-name/\x00\x01\x02\x03",
			expected: "/pool-name/",
		},
		{
			name:     "binary data with embedded tag",
			input:    "\x00\x01/mining-pool/extra/\xff\xfe",
			expected: "/mining-pool/", // Truncated after 2nd slash
		},
		{
			name:     "multiple tags picks first valid one",
			input:    "/short/ some text /longer-pool-name/",
			expected: "/short/", // Truncated after 2nd slash
		},
		{
			name:     "no recognizable pattern",
			input:    "\x00\x01\x02\x03\x04",
			expected: "", // Non-printable characters removed
		},
		{
			name:     "tag with ASCII char before binary",
			input:    "/taal.com/y\xb0\x10",
			expected: "/taal.com/", // Truncated after 2nd slash
		},
		{
			name:     "text before first slash is removed",
			input:    "some text /actual-miner/",
			expected: "/actual-miner/",
		},
		{
			name:     "complex text before miner tag",
			input:    "block mined by /ViaBTC/extra/data",
			expected: "/ViaBTC/", // Text before removed, truncated after 2nd slash
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := extractMiner(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestExtractMinerEdgeCases(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "quoted empty string",
			input:    `""`,
			expected: ``, // Quotes removed, becomes empty
		},
		{
			name:     "quoted non-slash string",
			input:    `"pool-name"`,
			expected: `pool-name`, // Quotes removed
		},
		{
			name:     "malformed quotes",
			input:    `"unclosed quote`,
			expected: `unclosed quote`, // Leading quote removed
		},
		{
			name:     "multiple quoted strings, first wins",
			input:    `"not-slash" "/winner/"`,
			expected: `/winner/`, // Everything before first slash removed
		},
		{
			name:     "tag too short",
			input:    "/a/",
			expected: "/a/", // Falls through to raw data (too short for regex)
		},
		{
			name:     "tag too long",
			input:    "/" + strings.Repeat("a", 60) + "/",
			expected: "/" + strings.Repeat("a", 60) + "/", // Falls through to raw data
		},
		{
			name:     "only binary data",
			input:    "\x00\x01\x02\x03",
			expected: "", // Non-printable characters removed
		},
		{
			name:     "unicode in tag",
			input:    "/pool-ñame/",
			expected: "/pool-ñame/", // Unicode is preserved
		},
		{
			name:     "invalid UTF-8 sequence",
			input:    "\xff\xfe\xfd",
			expected: "", // Invalid UTF-8 characters removed
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := extractMiner(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestExtractCoinbaseMinerErrorHandling(t *testing.T) {
	// Test error suppression for missing height (returns empty miner instead of error)
	emptyScript := bscript.Script{}
	tx := &bt.Tx{
		Inputs: []*bt.Input{
			{
				UnlockingScript: &emptyScript, // Empty script
			},
		},
	}

	miner, err := ExtractCoinbaseMiner(tx)
	require.NoError(t, err)    // Error is suppressed for missing height
	assert.Equal(t, "", miner) // Should return empty string
}

// TestExtractCoinbaseMinerRaw tests the raw mode extraction that returns unsanitized miner text
func TestExtractCoinbaseMinerRaw(t *testing.T) {
	testCases := []struct {
		name              string
		tx                string
		expectedSanitized string
		expectedRaw       string
	}{
		{
			name: "block 514587 with binary miner data",
			// This block has binary data that gets sanitized differently
			tx:                "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff14031bda07074125205a6ad8648d3b00009de70700ffffffff017777954a000000001976a9144770c259bc03c8dc36b853ed19fbb3514190be2e88ac00000000",
			expectedSanitized: "A% Zjd;",
			expectedRaw:       "\aA% Zj\xd8d\x8d;\x00\x00\x9d\xe7\a\x00", // Raw arbitrary text including non-printable chars
		},
		{
			name:              "clean miner tag - both modes should be similar",
			tx:                "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff18030910002f6d352d6363312fdcce95f3c057431c486ae662ffffffff0a0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00000000",
			expectedSanitized: "/m5-cc1/",
			expectedRaw:       "/m5-cc1/\xdc\xce\x95\xf3\xc0WC\x1cHj\xe6b", // Raw includes trailing binary data
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tx, err := bt.NewTxFromString(tc.tx)
			require.NoError(t, err)

			// Test sanitized mode (default)
			sanitized, err := ExtractCoinbaseMinerRaw(tx, false)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedSanitized, sanitized)

			// Test raw mode
			raw, err := ExtractCoinbaseMinerRaw(tx, true)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedRaw, raw)

			// Verify ExtractCoinbaseMiner (no param) matches sanitized behavior
			defaultMiner, err := ExtractCoinbaseMiner(tx)
			require.NoError(t, err)
			assert.Equal(t, sanitized, defaultMiner)
		})
	}
}

// referenceHeightPush is a deliberately independent implementation of bitcoin-sv's
// CScript() << nHeight (push_int64). It is written separately from the production
// EncodeCoinbaseHeightPush so the round-trip test below cross-checks the two against each other
// rather than against a shared implementation. Heights are always non-negative, hence uint32.
func referenceHeightPush(n uint32) []byte {
	if n == 0 {
		return []byte{0x00} // OP_0
	}

	if n <= 16 {
		return []byte{byte(0x50 + n)} // OP_1..OP_16
	}

	// minimal little-endian magnitude bytes; append 0x00 if the top bit would read as the sign
	var num []byte
	for v := n; v > 0; v >>= 8 {
		num = append(num, byte(v&0xff))
	}

	if num[len(num)-1]&0x80 != 0 {
		num = append(num, 0x00)
	}

	return append([]byte{byte(len(num))}, num...)
}

// TestCoinbaseHeightPushRoundTrip cross-checks the canonical encoder against an independent
// reference and verifies that every canonically-encoded height round-trips back through the
// extractor. This is the oracle test for SV Node encoding parity.
func TestCoinbaseHeightPushRoundTrip(t *testing.T) {
	// push of the 15-byte tag "\"/Galts-Gulch/\"", appended after the height so the extractor also
	// has trailing arbitrary text to skip over.
	const minerTagHex = "0f222f47616c74732d47756c63682f22"

	heights := []uint32{0, 1, 2, 16, 17, 127, 128, 255, 256, 1000, 21111, 227931, 518847, 4294967295}

	for _, h := range heights {
		t.Run(fmt.Sprintf("height_%d", h), func(t *testing.T) {
			ref := referenceHeightPush(h)

			// 1. the production encoder matches the independent reference encoder.
			require.Equal(t, ref, EncodeCoinbaseHeightPush(h), "canonical encoding mismatch")

			// 2. the encoded height round-trips back through the extractor to the same value, and the
			//    trailing tag is still recovered.
			script, err := bscript.NewFromHexString(hex.EncodeToString(ref) + minerTagHex)
			require.NoError(t, err)

			height, miner, err := extractCoinbaseHeightAndText(*script, false)
			require.NoError(t, err)
			require.Equal(t, h, height)
			require.Equal(t, "/Galts-Gulch/", miner)
		})
	}
}

// TestExtractCoinbaseMinerRawPreservesAllBytes verifies raw mode preserves all arbitrary text bytes
// while sanitized mode strips them. Each case appends a different set of trailing bytes after a
// clean "/miner/" tag so we exercise null bytes, high bytes, and control characters distinctly.
func TestExtractCoinbaseMinerRawPreservesAllBytes(t *testing.T) {
	// heightPrefix encodes a 3-byte block height (0x010203): the leading 0x03 is the length.
	const heightPrefix = "03010203"
	// minerTagHex is the hex for "/miner/".
	const minerTagHex = "2f6d696e65722f"

	testCases := []struct {
		name        string
		trailingHex string // bytes appended after "/miner/"
	}{
		{
			name:        "script with null bytes",
			trailingHex: "000000",
		},
		{
			name:        "script with high bytes",
			trailingHex: "fffefd",
		},
		{
			name:        "script with control characters",
			trailingHex: "01020304",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			script, err := bscript.NewFromHexString(heightPrefix + minerTagHex + tc.trailingHex)
			require.NoError(t, err)

			trailingBytes, err := hex.DecodeString(tc.trailingHex)
			require.NoError(t, err)

			// Create a minimal transaction with this script
			tx := &bt.Tx{
				Inputs: []*bt.Input{
					{
						UnlockingScript: script,
					},
				},
			}

			// Raw mode must preserve every byte after the height, including the trailing bytes.
			raw, err := ExtractCoinbaseMinerRaw(tx, true)
			require.NoError(t, err)
			assert.Equal(t, "/miner/"+string(trailingBytes), raw)

			// Sanitized mode strips the non-printable trailing bytes, leaving the clean tag.
			sanitized, err := ExtractCoinbaseMinerRaw(tx, false)
			require.NoError(t, err)
			assert.Equal(t, "/miner/", sanitized)
		})
	}
}

// TestExtractCoinbaseMinerRawJSONRoundTrip documents what API/WebSocket clients actually receive.
//
// Raw mode returns the exact coinbase bytes in a Go string, but the asset service serialises the
// miner field to JSON. Go's encoding/json replaces invalid UTF-8 bytes with the Unicode replacement
// character (U+FFFD) and escapes control characters. So a JSON client does NOT get byte-exact data;
// it gets a representation that mirrors how explorers such as WhatsOnChain render the same bytes
// (replacement characters for the invalid sequences). This test locks that behaviour in so a future
// change to the encoding path is caught.
func TestExtractCoinbaseMinerRawJSONRoundTrip(t *testing.T) {
	// Block 514587 coinbase: the arbitrary text contains invalid UTF-8 and control bytes.
	const txHex = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff14031bda07074125205a6ad8648d3b00009de70700ffffffff017777954a000000001976a9144770c259bc03c8dc36b853ed19fbb3514190be2e88ac00000000"

	tx, err := bt.NewTxFromString(txHex)
	require.NoError(t, err)

	raw, err := ExtractCoinbaseMinerRaw(tx, true)
	require.NoError(t, err)

	// The Go string preserves every byte, including invalid UTF-8 and control bytes.
	require.Equal(t, "\aA% Zj\xd8d\x8d;\x00\x00\x9d\xe7\a\x00", raw)
	require.False(t, utf8.ValidString(raw), "fixture should contain invalid UTF-8")

	// Marshalling to JSON and reading it back is LOSSY: the invalid bytes do not survive, so a
	// JSON client cannot reconstruct the original coinbase bytes from this field. The replacement
	// character it gets instead is the same thing explorers like WhatsOnChain display.
	out, err := json.Marshal(map[string]string{"miner": raw})
	require.NoError(t, err)

	var decoded map[string]string
	require.NoError(t, json.Unmarshal(out, &decoded))

	require.NotEqual(t, raw, decoded["miner"], "invalid bytes must not survive a JSON round-trip")
	require.True(t, utf8.ValidString(decoded["miner"]), "JSON output is always valid UTF-8")
	require.Contains(t, decoded["miner"], string(utf8.RuneError), "invalid bytes become the replacement character")

	// A sanitized (clean ASCII) tag round-trips through JSON unchanged.
	sanitized, err := ExtractCoinbaseMinerRaw(tx, false)
	require.NoError(t, err)

	out, err = json.Marshal(map[string]string{"miner": sanitized})
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(out, &decoded))
	require.Equal(t, sanitized, decoded["miner"], "clean tags survive a JSON round-trip intact")
}
