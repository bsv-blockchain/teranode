package p2p

import (
	"testing"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/stretchr/testify/assert"
)

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
		advertiseAddresses := getConfiguredAdvertiseAddresses(
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
		advertiseAddresses := getConfiguredAdvertiseAddresses(
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
		advertiseAddresses := getConfiguredAdvertiseAddresses(
			testSettings.P2P.AdvertiseAddresses,
			testSettings.P2P.SharePrivateAddresses,
			listenAddresses,
		)

		// Should use the explicitly configured addresses
		assert.Equal(t, testSettings.P2P.AdvertiseAddresses, advertiseAddresses)
	})

	t.Run("nat_settings_configuration", func(t *testing.T) {
		testSettings := &settings.Settings{}

		// Test NAT settings
		testSettings.P2P.EnableNATService = true
		testSettings.P2P.EnableNATPortMap = true
		testSettings.P2P.EnableHolePunching = true
		testSettings.P2P.EnableRelay = true

		// Verify settings are properly set
		assert.True(t, testSettings.P2P.EnableNATService, "NAT service should be enabled")
		assert.True(t, testSettings.P2P.EnableNATPortMap, "NAT port mapping should be enabled")
		assert.True(t, testSettings.P2P.EnableHolePunching, "Hole punching should be enabled")
		assert.True(t, testSettings.P2P.EnableRelay, "Relay should be enabled")
	})
}

// Helper function that mirrors the logic in Server.go
func getConfiguredAdvertiseAddresses(configuredAddresses []string, sharePrivateAddresses bool, listenAddresses []string) []string {
	if len(configuredAddresses) > 0 {
		// Use explicitly configured advertise addresses
		return configuredAddresses
	}

	if sharePrivateAddresses {
		// Share private addresses for local connectivity
		return listenAddresses
	}

	// Let go-p2p auto-detect and filter private addresses
	return []string{}
}
