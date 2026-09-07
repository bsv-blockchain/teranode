package settings

import (
	"testing"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestDisableBanning_LoaderReadsKey guards legacy_config_DisableBanning against
// the field-exists-but-loader-never-reads-it bug: a `key:` tag is documentation
// only, so without a getBool call in NewSettings the field stays at the Go zero
// value.
//
// This is NOT a new key. It is the bsvd `--nobanning` switch, already honored by
// the legacy service through its reflective loader
// (services/legacy/config.go:773 setConfigValuesFromSettings maps the
// `legacy_config_<Field>` prefix onto config.go:155's DisableBanning field, so
// the `nobanning` struct tag itself is inert — see
// services/legacy/peer_server_test.go:1040-1041). The typed field exists so the
// svp2p bridge can honor the SAME operator switch without reaching into gocore
// config, and both services must agree on it: an operator who turns banning off
// for legacy would otherwise find svp2p still scoring peers.
//
// A silently-zero field here would be the safe direction (banning stays on), but
// it would make the switch a no-op for svp2p with no signal at all.
func TestDisableBanning_LoaderReadsKey(t *testing.T) {
	const key = "legacy_disableBanning"

	ctx := gocore.Config().GetContext()

	// The default matches legacy's zero value: banning on.
	require.False(t, NewSettings().Legacy.DisableBanning,
		"%s must default to banning enabled", key)

	// Loader wiring: gocore resolves key.<context> ahead of the base key, so
	// set at the precedence that wins.
	winKey := key
	if ctx != "" {
		winKey = key + "." + ctx
	}

	gocore.Config().Set(winKey, "true")
	t.Cleanup(func() { gocore.Config().Set(winKey, "") })

	require.True(t, NewSettings().Legacy.DisableBanning,
		"loader must read %s under context %q", key, ctx)
}
