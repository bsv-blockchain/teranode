package settings

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// winKeyFor returns the key spelling gocore resolves first under the active
// context, so a test sets the value at the precedence that wins.
func winKeyFor(key string) string {
	if ctx := gocore.Config().GetContext(); ctx != "" {
		return key + "." + ctx
	}

	return key
}

func setForTest(t *testing.T, key, value string) {
	t.Helper()

	k := winKeyFor(key)
	gocore.Config().Set(k, value)
	t.Cleanup(func() { gocore.Config().Set(k, "") })
}

// captureStderr runs fn with os.Stderr redirected and returns what it wrote.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()

	r, w, err := os.Pipe()
	require.NoError(t, err)

	orig := os.Stderr
	os.Stderr = w

	defer func() { os.Stderr = orig }()

	fn()

	require.NoError(t, w.Close())

	out, err := io.ReadAll(r)
	require.NoError(t, err)

	return string(out)
}

// The legacy service reads `legacy_config_DisableBanning` through its
// reflective loader, a namespace that is deleted with the legacy service at
// cutover. svp2p owns `legacy_disableBanning`; an operator who only set the old
// key must keep the behaviour they configured, and be told to move.
func TestDisableBanning_LegacyConfigKeyIsHonouredAsFallback(t *testing.T) {
	setForTest(t, "legacy_config_DisableBanning", "true")

	var s *Settings

	out := captureStderr(t, func() { s = NewSettings() })

	require.True(t, s.Legacy.DisableBanning, "the old key alone must still disable banning")
	require.Contains(t, out, "legacy_config_DisableBanning")
	require.Contains(t, out, "legacy_disableBanning")
	require.Contains(t, strings.ToLower(out), "deprecated")
}

func TestDisableBanning_NewKeyWinsOverLegacyConfigKey(t *testing.T) {
	setForTest(t, "legacy_config_DisableBanning", "true")
	setForTest(t, "legacy_disableBanning", "false")

	var s *Settings

	out := captureStderr(t, func() { s = NewSettings() })

	require.False(t, s.Legacy.DisableBanning, "an explicit new key must win over the old one")
	require.NotContains(t, out, "legacy_config_DisableBanning", "no deprecation warning when the new key is set")
}

// `legacy_config_MinSyncPeerNetworkSpeed` steers the legacy service's floor
// only; `legacy_minSyncPeerNetworkSpeed` steers svp2p's only. Until cutover
// reconciles them, svp2p must honour an operator who set only the old key.
func TestMinSyncPeerNetworkSpeed_LegacyConfigKeyIsHonouredAsFallback(t *testing.T) {
	setForTest(t, "legacy_config_MinSyncPeerNetworkSpeed", "777")

	var s *Settings

	out := captureStderr(t, func() { s = NewSettings() })

	require.Equal(t, uint64(777), s.Legacy.MinSyncPeerNetworkSpeed)
	require.Contains(t, out, "legacy_config_MinSyncPeerNetworkSpeed")
	require.Contains(t, out, "legacy_minSyncPeerNetworkSpeed")
}

func TestMinSyncPeerNetworkSpeed_NewKeyWinsOverLegacyConfigKey(t *testing.T) {
	setForTest(t, "legacy_config_MinSyncPeerNetworkSpeed", "777")
	setForTest(t, "legacy_minSyncPeerNetworkSpeed", "12345")

	var s *Settings

	out := captureStderr(t, func() { s = NewSettings() })

	require.Equal(t, uint64(12345), s.Legacy.MinSyncPeerNetworkSpeed)
	require.NotContains(t, out, "legacy_config_MinSyncPeerNetworkSpeed")
}

// legacy_disableDNSSeed is the bsvd --nodnsseed switch for svp2p; the legacy
// service spells it legacy_config_DisableDNSSeed.
func TestDisableDNSSeed_LoaderReadsKeyAndFallback(t *testing.T) {
	require.False(t, NewSettings().Legacy.DisableDNSSeed, "DNS seeding is on by default")

	setForTest(t, "legacy_config_DisableDNSSeed", "true")

	var s *Settings

	out := captureStderr(t, func() { s = NewSettings() })
	require.True(t, s.Legacy.DisableDNSSeed)
	require.Contains(t, out, "legacy_disableDNSSeed")

	setForTest(t, "legacy_disableDNSSeed", "false")
	require.False(t, NewSettings().Legacy.DisableDNSSeed, "the new key wins")
}
