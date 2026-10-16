package monitor

import (
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// TestSettingsViewRedactsBlockchainStorePassword covers the monitor's settings
// screen. The blockchain store URL is usually a postgres URL with the password
// in its userinfo, and the screen is shown on shared terminals and in
// screenshots, so the password must be masked there too.
func TestSettingsViewRedactsBlockchainStorePassword(t *testing.T) {
	const password = "canary-monitor-store-password"

	storeURL, err := url.Parse("postgres://teranode:" + password + "@db:5432/blockchain")
	require.NoError(t, err)

	s := &settings.Settings{ChainCfgParams: &chaincfg.MainNetParams}
	s.BlockChain.StoreURL = storeURL

	// A tall terminal, so the store row is inside the visible window.
	view := Model{settings: s, height: 500}.renderSettingsView()

	require.NotContains(t, view, password, "the settings view shows the blockchain store password")
	require.Contains(t, view, "db:5432", "the store host should still be shown")
}
