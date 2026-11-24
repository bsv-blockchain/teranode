package settings

import (
	"testing"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestTargetOutboundPeers_LoaderReadsKey guards legacy_targetOutboundPeers
// against the field-exists-but-loader-never-reads-it bug: a `key:` tag is
// documentation only, so without a getInt call in NewSettings the field stays
// at the Go zero value.
//
// A zero here disables the svp2p addrman-driven dialer completely and silently
// — the node would listen, accept inbound peers, and never open an outbound
// connection of its own. The default is SVNode's own
// DEFAULT_MAX_OUTBOUND_CONNECTIONS (net.h:96), so out of the box the behavior
// is SVNode's and only an operator moves it.
func TestTargetOutboundPeers_LoaderReadsKey(t *testing.T) {
	const key = "legacy_targetOutboundPeers"

	ctx := gocore.Config().GetContext()

	// The default is asserted under EVERY context, unlike the older loader
	// guards in this package: no .conf file declares this key at all, so no
	// context can override it and there is nothing to exempt.
	require.Equal(t, 8, NewSettings().Legacy.TargetOutboundPeers,
		"%s must default to SVNode's DEFAULT_MAX_OUTBOUND_CONNECTIONS", key)

	// Loader wiring, under every context: gocore resolves key.<context> ahead
	// of the base key, so set at the precedence that wins.
	winKey := key
	if ctx != "" {
		winKey = key + "." + ctx
	}

	gocore.Config().Set(winKey, "13")
	t.Cleanup(func() { gocore.Config().Set(winKey, "") })

	require.Equal(t, 13, NewSettings().Legacy.TargetOutboundPeers,
		"loader must read %s under context %q", key, ctx)
}
