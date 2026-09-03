package parity

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/svp2p/svp2ptest"
	"github.com/bsv-blockchain/teranode/util/test"
)

// TestNewNode_BothImplsSyncAScriptedChain is the harness's own smoke test:
// the same scripted peer serves a 20-block chain to a legacy node and to an
// svp2p node, and both reach its tip.
func TestNewNode_BothImplsSyncAScriptedChain(t *testing.T) {
	for _, impl := range []Impl{Legacy, Svp2p} {
		t.Run(string(impl), func(t *testing.T) {
			tSettings := test.CreateBaseTestSettings(t)
			chain := svp2ptest.BuildFixtureChain(t, tSettings, 20)
			peer := svp2ptest.NewScriptedPeer(t, chain, tSettings.ChainCfgParams.Net, svp2ptest.Script{}, true)

			n := newNode(t, impl, []string{peer.Addr})
			defer n.Stop()

			n.WaitForHeight(t, 20, 90*time.Second)
		})
	}
}
