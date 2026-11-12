package settings

import (
	"testing"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestLegacySettings_UpnpLoaderReadsKey guards against the field-exists-but-
// loader-never-reads-it bug: Upnp has a `key:"legacy_upnp"` struct tag, but
// if NewSettings() does not call getBool for it, the value stays at Go zero
// (false) and the documented setting is silently unreadable. The default is
// also false, so a default-value assertion alone would pass spuriously — the
// honest test is: set an override via gocore, call NewSettings(), assert the
// field changed.
func TestLegacySettings_UpnpLoaderReadsKey(t *testing.T) {
	def := NewSettings()
	require.False(t, def.Legacy.Upnp, "default must stay disabled")

	gocore.Config().Set("legacy_upnp", "true")
	t.Cleanup(func() { gocore.Config().Set("legacy_upnp", "") })

	s := NewSettings()
	require.True(t, s.Legacy.Upnp)
}
