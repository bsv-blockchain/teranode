package subtreevalidation

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestValidSubtreeIsNotReportedForALegacyPeer pins a ten-second stall per block on a node that
// syncs over the legacy protocol. After validating a subtree the node reports the serving peer's
// good behaviour to the P2P service, and a legacy peer's id is namespaced "legacy:". Reputation
// is a P2P concept the P2P service cannot apply to such an id, and on mainnet P2P is switched off,
// so every report waited about ten seconds for a connection and failed: block 945,079 spent 10.3
// of its 11.1 seconds of subtree checking there, on 2026-09-28.
func TestValidSubtreeIsNotReportedForALegacyPeer(t *testing.T) {
	require.False(t, reportsValidSubtreeTo(""), "no peer, nothing to report")
	require.False(t, reportsValidSubtreeTo("legacy:57.128.233.172:8333"), "a legacy peer has no P2P reputation")
	require.True(t, reportsValidSubtreeTo("12D3KooWPeer"), "a P2P peer is still credited")
}
