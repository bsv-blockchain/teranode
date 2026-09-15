package daemon

import (
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// TestApplyPeerURLPolicy_FollowsAllowPrivateIPs pins the wiring: p2p_allow_private_ips is
// what lets a peer-supplied hostname resolve to a private-network address (issue 4843).
func TestApplyPeerURLPolicy_FollowsAllowPrivateIPs(t *testing.T) {
	orig := util.SSRFAllowPrivateNetworks()
	defer util.SetSSRFAllowPrivateNetworks(orig)

	for _, allowed := range []bool{true, false, true} {
		applyPeerURLPolicy(&settings.Settings{P2P: settings.P2PSettings{AllowPrivateIPs: allowed}})
		require.Equal(t, allowed, util.SSRFAllowPrivateNetworks())
	}
}
