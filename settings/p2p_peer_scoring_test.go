package settings

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestP2PPeerScoring_Defaults guards against the loader-vs-struct-tag class of bug:
// EnablePeerScoring carries default:"true", but only the explicit getBool call in
// NewSettings actually populates it. A missing loader entry would leave the field at
// the Go zero value (false), silently disabling GossipSub Sybil resistance in every
// deployment that does not set the key explicitly - the opposite of the documented
// default, and a security regression.
func TestP2PPeerScoring_Defaults(t *testing.T) {
	tSettings := NewSettings()

	require.NotNil(t, tSettings)
	require.True(t, tSettings.P2P.EnablePeerScoring,
		"default EnablePeerScoring must be true. If this fails the loader in settings.go is "+
			`missing the getBool("p2p_enable_peer_scoring", true, alternativeContext...) entry `+
			"and peer scoring is silently disabled in prod.")
	require.False(t, tSettings.P2P.DisablePeerExchange,
		"default DisablePeerExchange must be false (peer exchange stays on, gated by scoring)")
}

// TestP2PPeerScoring_Overrides verifies the keys are actually read, not just defaulted:
// an explicit override must flow through NewSettings to the runtime struct.
func TestP2PPeerScoring_Overrides(t *testing.T) {
	t.Setenv("p2p_enable_peer_scoring", "false")
	t.Setenv("p2p_disable_peer_exchange", "true")

	tSettings := NewSettings()

	require.NotNil(t, tSettings)
	require.False(t, tSettings.P2P.EnablePeerScoring,
		"explicit p2p_enable_peer_scoring=false must be honored by the loader")
	require.True(t, tSettings.P2P.DisablePeerExchange,
		"explicit p2p_disable_peer_exchange=true must be honored by the loader")
}
