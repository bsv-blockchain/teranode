package svp2p

import (
	"testing"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestSyncPeerRateFloorReachesTheDownloader is the whole chain of the
// legacy_minSyncPeerNetworkSpeed setting, in one test: the operator's config key
// through settings.NewSettings (which is what the harness builds its settings
// with), through Server.startSync's SyncConfig, into the block downloader that
// reads the floor on every stall check.
//
// settings/min_sync_peer_network_speed_test.go proves the first hop alone and
// protocol.TestConfigureSync_CarriesTheSyncPeerRateFloor the last one; neither
// can prove the hop in between, which is Server.go's own composition — the one
// place a new setting is silently dropped by being left out of a struct
// literal.
//
// The floor is set to a value nothing else in the tree uses, so a passing
// assertion cannot be the default arriving by coincidence.
func TestSyncPeerRateFloorReachesTheDownloader(t *testing.T) {
	const (
		key   = "legacy_minSyncPeerNetworkSpeed"
		floor = uint64(7331)
	)

	// gocore resolves key.<context> ahead of the bare key, so the value has to
	// be set at the precedence that wins under the test context.
	winKey := key
	if ctx := gocore.Config().GetContext(); ctx != "" {
		winKey = key + "." + ctx
	}

	gocore.Config().Set(winKey, "7331")
	t.Cleanup(func() { gocore.Config().Set(winKey, "") })

	// No connect peers: this leg needs the node built and configured, not a
	// sync. Every service behind it is the real one, as in every other leg
	// here.
	h := newSyncHarness(t, "ratefloor", nil, 0)

	require.Equal(t, floor, h.server.settings.Legacy.MinSyncPeerNetworkSpeed,
		"the harness settings must carry the configured key, or the rest proves nothing")

	h.start(t)

	require.Equal(t, floor, h.server.manager.SyncRateFloor(),
		"the configured floor must reach the block downloader through Server.startSync")
}
