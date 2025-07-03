package sql

import (
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenesisHashNewChain(t *testing.T) {
	testCases := []struct {
		name     string
		settings *settings.Settings
	}{
		{
			name:     "MainNet",
			settings: settings.NewSettings(),
		},
		{
			name:     "TestNet",
			settings: settings.NewSettings(),
		},
		{
			name:     "RegTest",
			settings: settings.NewSettings(),
		},
		// {
		// 	name:        "STN",
		// 	chainParams: &chaincfg.StnParams,
		// },
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup test logger
			logger := ulogger.TestLogger{}

			// Setup test database URL
			dbURL, err := url.Parse("sqlitememory:///")
			require.NoError(t, err)

			// Create new SQL store - this should insert the genesis block
			store, err := New(logger, dbURL, tc.settings)
			require.NoError(t, err)
			defer store.Close()

			// Query the genesis block hash from the database
			var hash []byte
			err = store.db.QueryRow(`
				SELECT hash
				FROM blocks
				WHERE height = 0
			`).Scan(&hash)
			require.NoError(t, err)

			// Verify the hash matches the expected genesis block hash
			assert.Equal(t, tc.settings.ChainCfgParams.GenesisHash[:], hash)
		})
	}
}

func TestGenesisHashWrongParams(t *testing.T) {
	testCases := []struct {
		name          string
		initialParams *chaincfg.Params
		wrongParams   *chaincfg.Params
	}{
		{
			name:          "MainNet to TestNet",
			initialParams: &chaincfg.MainNetParams,
			wrongParams:   &chaincfg.TestNetParams,
		},
		{
			name:          "TestNet to RegTest",
			initialParams: &chaincfg.TestNetParams,
			wrongParams:   &chaincfg.RegressionNetParams,
		},
		{
			name:          "RegTest to STN",
			initialParams: &chaincfg.RegressionNetParams,
			wrongParams:   &chaincfg.StnParams,
		},
		{
			name:          "STN to MainNet",
			initialParams: &chaincfg.StnParams,
			wrongParams:   &chaincfg.MainNetParams,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup test logger and database
			logger := ulogger.TestLogger{}
			dbURL, err := url.Parse("sqlitememory:///")
			require.NoError(t, err)

			s := settings.NewSettings()
			s.ChainCfgParams = tc.initialParams

			// First create a store with initial params
			store, err := New(logger, dbURL, s)
			require.NoError(t, err)
			defer store.Close()

			// Now try to insert the wrong genesis block
			store.chainParams = tc.wrongParams

			err = store.insertGenesisTransaction(logger)

			// Verify we get a configuration error about mismatched genesis hash
			assert.ErrorContains(t, err, "genesis block hash mismatch")
		})
	}
}
