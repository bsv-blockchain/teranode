package util

import (
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
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
			name:           "3-byte height 4105 with m5-cc1 miner",
			tx:             "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff18030910002f6d352d6363312fdcce95f3c057431c486ae662ffffffff0a0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00000000",
			expectedHeight: 4105,
			expectedMiner:  "/m5-cc1/",
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
		{
			name:           "old-style 4-byte push (unsupported)",
			tx:             "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff3504eae0041a0127522cfabe6d6d620af8ed2d63d1c7ad3a61afe44ef47d3acf88f45d4936621a238ba23355f8ea0800000000000000ffffffff01d06e44950000000043410427d214c2e4907d96f67f545fe4f84b8d71856e52bb3d7738890003985a8e0271bd12bf98c6050878c1e3e2684faaad9b5efb2a1e912177e4e1c200dde79806eeac00000000",
			expectedHeight: 0,
			expectedMiner:  "",
			heightError:    true,
			minerError:     false, // Error is suppressed for ExtractCoinbaseMiner
		},
		{
			name:           "testnet pre-BIP34 transaction",
			tx:             "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff4a60248ab1830b9f12c743e837ef4feb32dda0739ee6e3aaf3b24aa86883c592f2f3a20df867825d91bc096f3260b20ae49176d3bb8d02a107af4065a59fffeb9235fc991145224be83446ffffffff01e0850a2a010000002321038a6ca672c189b3af86b9894fcd40a3af6bbae7b45db9ee89258c848b68d66af3ac00000000",
			expectedHeight: 0,
			expectedMiner:  "",
			heightError:    true,
			minerError:     false, // Error is suppressed for ExtractCoinbaseMiner
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

// TestExtractCoinbaseHeightAndText_Scripts tests direct script parsing
func TestExtractCoinbaseHeightAndText_Scripts(t *testing.T) {
	testCases := []struct {
		name           string
		script         string
		expectedHeight uint32
		expectedMiner  string
		expectError    bool
	}{
		{
			name:           "3-byte height 807495 with taal.com miner",
			script:         "0347520c2f7461616c2e636f6d2f79b010ec60689edf8d3a0000",
			expectedHeight: 807495,
			expectedMiner:  "/taal.com/",
		},
		{
			name:           "3-byte height 0 with no miner",
			script:         "03000000",
			expectedHeight: 0,
			expectedMiner:  "",
		},
		{
			name:           "2-byte height 1 with satoshi miner",
			script:         "0201002f7361746f7368692f",
			expectedHeight: 1,
			expectedMiner:  "/satoshi/",
		},
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
			name:        "unsupported 4-byte length",
			script:      "0401020304",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			script, err := bscript.NewFromHexString(tc.script)
			require.NoError(t, err)

			height, miner, err := extractCoinbaseHeightAndText(*script)
			if tc.expectError {
				require.Error(t, err)
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

func TestExtractCoinbaseMiner_ErrorHandling(t *testing.T) {
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
