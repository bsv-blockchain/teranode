package parity

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/svp2p/svp2ptest"
	"github.com/stretchr/testify/require"
)

// TestRunParity_HonestSyncMatches is the runner's own test: one honest peer,
// a 20-block chain, both implementations reach the tip and the observations
// agree on everything Compare looks at.
func TestRunParity_HonestSyncMatches(t *testing.T) {
	obs, res := RunParity(t, Scenario{
		Name:  "honest-sync",
		Chain: 20,
		Peers: func(t *testing.T, chain *svp2ptest.FixtureChain, net wire.BitcoinNet) []*svp2ptest.ScriptedPeer {
			return []*svp2ptest.ScriptedPeer{svp2ptest.NewScriptedPeer(t, chain, net, svp2ptest.Script{}, true)}
		},
		Drive: func(t *testing.T, n *nodeUnderTest, _ []*svp2ptest.ScriptedPeer) {
			n.WaitForHeight(t, 20, 90*time.Second)
		},
		Observe: ObserveDefault,
	})

	require.Empty(t, res.Diffs)
	require.Equal(t, uint32(20), obs[Legacy].BlocksAccepted)
	require.Equal(t, uint32(20), obs[Svp2p].BlocksAccepted)
	require.Equal(t, 20, obs[Svp2p].Served["peer0"])
}
