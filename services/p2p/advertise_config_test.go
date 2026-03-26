package p2p

import (
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSilentModeAdvertiseAddressesSuppressed verifies that silent mode always returns an
// empty advertise address list regardless of AdvertiseAddresses or SharePrivateAddresses.
func TestSilentModeAdvertiseAddressesSuppressed(t *testing.T) {
	listenAddresses := []string{"/ip4/10.0.0.1/tcp/9905"}

	t.Run("silent_overrides_explicit_advertise_addresses", func(t *testing.T) {
		result := resolveAdvertiseAddresses(settings.ListenModeSilent,
			[]string{"/ip4/203.0.113.1/tcp/9905"}, false, listenAddresses)
		require.Empty(t, result, "silent mode must suppress explicitly configured AdvertiseAddresses")
	})

	t.Run("silent_overrides_share_private_addresses", func(t *testing.T) {
		result := resolveAdvertiseAddresses(settings.ListenModeSilent,
			[]string{}, true, listenAddresses)
		require.Empty(t, result, "silent mode must suppress address sharing even when SharePrivateAddresses=true")
	})

	t.Run("silent_with_no_configured_addresses", func(t *testing.T) {
		result := resolveAdvertiseAddresses(settings.ListenModeSilent,
			[]string{}, false, listenAddresses)
		require.Empty(t, result, "silent mode must always return empty advertise addresses")
	})
}

// TestAdvertiseConfiguration verifies that advertise addresses are configured correctly
// With go-p2p v1.2.1, the library handles address advertisement intelligently
func TestAdvertiseConfiguration(t *testing.T) {
	t.Run("empty_advertise_addresses_when_share_private_is_false", func(t *testing.T) {
		// When SharePrivateAddresses is false and no explicit addresses configured,
		// go-p2p v1.2.1 will automatically filter private IPs
		testSettings := &settings.Settings{}
		testSettings.P2P.AdvertiseAddresses = []string{}
		testSettings.P2P.SharePrivateAddresses = false

		listenAddresses := []string{"/ip4/192.168.1.1/tcp/9905", "/ip4/1.2.3.4/tcp/9905"}
		advertiseAddresses := resolveAdvertiseAddresses(
			testSettings.P2P.ListenMode,
			testSettings.P2P.AdvertiseAddresses,
			testSettings.P2P.SharePrivateAddresses,
			listenAddresses,
		)

		// Should be empty - go-p2p will auto-detect public addresses only
		assert.Empty(t, advertiseAddresses, "advertiseAddresses should be empty for go-p2p to filter private IPs")
	})

	t.Run("uses_listen_addresses_when_share_private_is_true", func(t *testing.T) {
		// When SharePrivateAddresses is true (default), use listen addresses
		// This enables local/test environments to work properly
		testSettings := &settings.Settings{}
		testSettings.P2P.AdvertiseAddresses = []string{}
		testSettings.P2P.SharePrivateAddresses = true

		listenAddresses := []string{"/ip4/192.168.1.1/tcp/9905", "/ip4/10.0.0.1/tcp/9905"}
		advertiseAddresses := resolveAdvertiseAddresses(
			testSettings.P2P.ListenMode,
			testSettings.P2P.AdvertiseAddresses,
			testSettings.P2P.SharePrivateAddresses,
			listenAddresses,
		)

		// Should use listen addresses for local connectivity
		assert.Equal(t, listenAddresses, advertiseAddresses, "should use listen addresses when SharePrivateAddresses is true")
	})

	t.Run("uses_explicit_advertise_addresses_when_configured", func(t *testing.T) {
		testSettings := &settings.Settings{}
		testSettings.P2P.AdvertiseAddresses = []string{
			"/ip4/203.0.113.1/tcp/9905", // Explicitly configured public address
		}
		testSettings.P2P.SharePrivateAddresses = false // Should be ignored when explicit addresses are set

		listenAddresses := []string{"/ip4/192.168.1.1/tcp/9905"}
		advertiseAddresses := resolveAdvertiseAddresses(
			testSettings.P2P.ListenMode,
			testSettings.P2P.AdvertiseAddresses,
			testSettings.P2P.SharePrivateAddresses,
			listenAddresses,
		)

		// Should use the explicitly configured addresses
		assert.Equal(t, testSettings.P2P.AdvertiseAddresses, advertiseAddresses)
	})
}

// resolveAdvertiseAddresses mirrors the address resolution logic in NewServer, including
// the silent mode override. Tests should use this helper rather than reimplementing the
// logic inline.
func resolveAdvertiseAddresses(listenMode string, configuredAddresses []string, sharePrivateAddresses bool, listenAddresses []string) []string {
	if listenMode == settings.ListenModeSilent {
		return []string{}
	}

	if len(configuredAddresses) > 0 {
		return configuredAddresses
	}

	if sharePrivateAddresses {
		return listenAddresses
	}

	return []string{}
}
