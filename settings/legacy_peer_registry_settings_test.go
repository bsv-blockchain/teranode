package settings

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestLegacyPeerRegistryDefaults checks the two new keys are wired through
// NewSettings. A struct tag alone is documentation: an unwired key stays at its
// zero value, which would silently disable the feature and set a zero interval.
func TestLegacyPeerRegistryDefaults(t *testing.T) {
	s := NewSettings()

	require.True(t, s.Legacy.PeerRegistryEnabled,
		"legacy_peerRegistryEnabled must default to true")
	require.Equal(t, 10*time.Second, s.Legacy.PeerRegistrySyncInterval,
		"legacy_peerRegistrySyncInterval must default to 10s")
}
