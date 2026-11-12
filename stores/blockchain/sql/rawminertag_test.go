package sql

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestRawMinerTagSettingWiring verifies that the blockchain_raw_miner_tag setting is honoured
// end-to-end: from the gocore config key, through settings.NewSettings(), into the SQL store.
// With it off the miner is sanitized, with it on the raw coinbase text is returned.
// coinbaseTx2 contains binary/non-printable bytes after a clean "/m1-eu/" tag, so the two modes
// produce genuinely different results.
//
// The setting is applied via gocore.Config().Set + NewSettings() (not by assigning the struct
// field directly) so this test would fail if the loader ever stopped reading
// blockchain_raw_miner_tag into BlockChain.RawMinerTag.
func TestRawMinerTagSettingWiring(t *testing.T) {
	// Sanity check on the fixture: raw and sanitized extraction must differ, otherwise this test
	// would pass trivially regardless of which path the store takes.
	sanitizedExpected, err := util.ExtractCoinbaseMinerRaw(coinbaseTx2, false)
	require.NoError(t, err)

	rawExpected, err := util.ExtractCoinbaseMinerRaw(coinbaseTx2, true)
	require.NoError(t, err)

	require.NotEqual(t, sanitizedExpected, rawExpected, "fixture must differ between raw and sanitized modes")

	testCases := []struct {
		name       string
		rawSetting string
		expected   string
	}{
		{name: "sanitized (default)", rawSetting: "false", expected: sanitizedExpected},
		{name: "raw", rawSetting: "true", expected: rawExpected},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gocore.Config().Set("blockchain_raw_miner_tag", tc.rawSetting)
			t.Cleanup(func() { gocore.Config().Set("blockchain_raw_miner_tag", "") })

			tSettings := test.CreateBaseTestSettings(t)
			require.Equal(t, tc.rawSetting == "true", tSettings.BlockChain.RawMinerTag,
				"NewSettings() must reflect blockchain_raw_miner_tag, otherwise this test can't detect broken wiring")

			storeURL, err := url.Parse("sqlitememory:///")
			require.NoError(t, err)

			s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
			require.NoError(t, err)

			_, _, err = s.StoreBlock(context.Background(), block1, "test_peer")
			require.NoError(t, err)

			_, _, err = s.StoreBlock(context.Background(), block2, "test_peer")
			require.NoError(t, err)

			_, meta, err := s.GetBestBlockHeader(context.Background())
			require.NoError(t, err)

			require.Equal(t, tc.expected, meta.Miner)
		})
	}
}
