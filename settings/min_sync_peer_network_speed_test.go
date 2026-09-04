package settings

import (
	"strconv"
	"testing"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestMinSyncPeerNetworkSpeed_LoaderReadsKey guards
// legacy_minSyncPeerNetworkSpeed against the field-exists-but-loader-never-
// reads-it bug: a `key:` tag is documentation only, so without a getUint64 call
// in NewSettings the field stays at the Go zero value.
//
// A zero here is not a harmless miss. Zero is the value that DISABLES the
// sync-peer rotation rate floor (legacy services/legacy/netsync/manager.go:266
// compares unsigned, so nothing is ever below a floor of 0), so a missing
// loader call would silently ship a node that never rotates a sync peer for
// being slow. The default is legacy's own defaultMinSyncPeerNetworkSpeed
// (services/legacy/config.go:48).
func TestMinSyncPeerNetworkSpeed_LoaderReadsKey(t *testing.T) {
	const key = "legacy_minSyncPeerNetworkSpeed"

	ctx := gocore.Config().GetContext()

	// Asserted under EVERY context: no .conf file declares this key, so no
	// context can override it and there is nothing to exempt.
	require.Equal(t, uint64(51200), NewSettings().Legacy.MinSyncPeerNetworkSpeed,
		"%s must default to legacy's defaultMinSyncPeerNetworkSpeed", key)

	// Loader wiring, under every context: gocore resolves key.<context> ahead
	// of the base key, so set at the precedence that wins.
	winKey := key
	if ctx != "" {
		winKey = key + "." + ctx
	}

	gocore.Config().Set(winKey, "12345")
	t.Cleanup(func() { gocore.Config().Set(winKey, "") })

	require.Equal(t, uint64(12345), NewSettings().Legacy.MinSyncPeerNetworkSpeed,
		"loader must read %s under context %q", key, ctx)

	// 0 is an operator value, not an absent one: it disables the floor. The
	// loader must carry it rather than fall back to the default.
	gocore.Config().Set(winKey, strconv.Itoa(0))

	require.Zero(t, NewSettings().Legacy.MinSyncPeerNetworkSpeed,
		"a configured 0 must reach the settings as 0 — it is legacy's disable value")
}
