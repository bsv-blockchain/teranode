package parity

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/svp2p/svp2ptest"
	"github.com/stretchr/testify/require"
)

// TestParity_ColdStartBootstrap — watch-list scenario 8. No configured peers
// and an empty address table. svp2p now carries SVNode's DNS seeding and
// fixed-seed fallback (protocol/seeds.go), but regtest declares neither a DNS
// seed nor a fixed seed, and legacy has no regtest seeds either, so both sit at
// zero here. The bootstrap itself is pinned in protocol/seeds_test.go with an
// injected resolver; this row records that nothing dials out on regtest.
func TestParity_ColdStartBootstrap(t *testing.T) {
	obs, _ := RunParity(t, Scenario{
		Name:  "cold-start-bootstrap",
		Chain: 3,
		Peers: func(*testing.T, *svp2ptest.FixtureChain, wire.BitcoinNet) []*svp2ptest.ScriptedPeer { return nil },
		Drive: func(t *testing.T, n *nodeUnderTest, _ []*svp2ptest.ScriptedPeer) {
			time.Sleep(10 * time.Second)

			n.notes = map[string]string{"connected-after-10s": fmt.Sprint(n.ConnectedCount(t))}
		},
		Observe: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) Observation {
			o := ObserveDefault(t, n, peers)
			o.Notes = n.notes

			return o
		},
	})

	require.Equal(t, "0", obs[Svp2p].Notes["connected-after-10s"], "regtest has no DNS or fixed seeds, so an empty table must stay empty and nothing may dial out")
	t.Logf("cold start: legacy connected %s, svp2p connected %s", obs[Legacy].Notes["connected-after-10s"], obs[Svp2p].Notes["connected-after-10s"])
}

// TestParity_NoUnsolicitedSelfAdvertisement — watch-list scenario 7. SVNode
// advertises its own address to an outbound peer unprompted
// (net_processing.cpp:1847-1864); svp2p advertises only inside a getaddr
// reply, legacy's shape. The peer never sends getaddr, so any addr it receives
// is a self-advertisement.
func TestParity_NoUnsolicitedSelfAdvertisement(t *testing.T) {
	obs, _ := RunParity(t, Scenario{
		Name:  "self-advertisement",
		Chain: 3,
		Peers: honestPeers(1),
		Drive: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) {
			n.WaitForHeight(t, 3, 30*time.Second)
			time.Sleep(8 * time.Second)

			n.notes = map[string]string{"addr-received": fmt.Sprint(peers[0].Transcript.Count(svp2ptest.In, "addr"))}
		},
		Observe: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) Observation {
			o := ObserveDefault(t, n, peers)
			o.Notes = n.notes

			return o
		},
	})

	require.Equal(t, "0", obs[Svp2p].Notes["addr-received"], "svp2p never self-advertises unprompted (documented divergence from SVNode)")
	t.Logf("self-advertisement: legacy sent %s addr, svp2p sent %s addr, SVNode would send 1", obs[Legacy].Notes["addr-received"], obs[Svp2p].Notes["addr-received"])
}

// TestParity_AddrForwardingWidths — watch-list scenario 11. An OUTBOUND peer
// (peer0, dialled by the node) announces one fresh routable address; two
// INBOUND peers (dialled in) are the relay candidates. svp2p's RelayAddress
// port forwards to one or two inbound peers, as SVNode does
// (net_processing.cpp:998-1041); legacy dropped forwarding. The sender must
// be outbound: svp2p accepts an unrequested addr from an inbound peer only if
// it is that peer's own address (addrrelay.go processAddrEntries).
func TestParity_AddrForwardingWidths(t *testing.T) {
	obs, _ := RunParity(t, Scenario{
		Name:  "addr-forwarding-widths",
		Chain: 3,
		Peers: func(t *testing.T, c *svp2ptest.FixtureChain, netMagic wire.BitcoinNet) []*svp2ptest.ScriptedPeer {
			return []*svp2ptest.ScriptedPeer{
				svp2ptest.NewScriptedPeer(t, c, netMagic, svp2ptest.Script{}, true),  // peer0: the node dials it
				svp2ptest.NewScriptedPeer(t, c, netMagic, svp2ptest.Script{}, false), // peer1: dials in
				svp2ptest.NewScriptedPeer(t, c, netMagic, svp2ptest.Script{}, false), // peer2: dials in
			}
		},
		// All three stay in legacy_connect_peers: legacy caps its peer count at
		// the number of configured peers ("Max peers reached"), and the two
		// non-listening addresses only cost the node a refused dial each.
		Drive: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) {
			n.WaitForHeight(t, 3, 30*time.Second)

			require.NoError(t, peers[1].Dial(n.PeerListen))
			require.NoError(t, peers[2].Dial(n.PeerListen))

			n.WaitFor(t, func() bool { return n.ConnectedCount(t) == 3 }, 30*time.Second, "the inbound peers never connected")
			time.Sleep(2 * time.Second)

			addr := wire.NewMsgAddr()
			na := wire.NewNetAddressIPPort(net.ParseIP("8.8.8.8"), 8333, wire.SFNodeNetwork)
			na.Timestamp = time.Now()
			require.NoError(t, addr.AddAddress(na))
			peers[0].Send(addr)

			forwarded := func() int {
				return boolToInt(peers[1].Transcript.Count(svp2ptest.In, "addr") > 0) + boolToInt(peers[2].Transcript.Count(svp2ptest.In, "addr") > 0)
			}

			n.WaitFor(t, func() bool { return forwarded() > 0 }, 20*time.Second, "")

			n.notes = map[string]string{"forwarded-to-peers": fmt.Sprint(forwarded())}
		},
		Observe: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) Observation {
			o := ObserveDefault(t, n, peers)
			o.Notes = n.notes

			return o
		},
	})

	svp := obs[Svp2p].Notes["forwarded-to-peers"]
	require.Contains(t, []string{"1", "2"}, svp, "svp2p forwards a fresh address to one or two inbound peers (RelayAddress widths)")
	require.Equal(t, "0", obs[Legacy].Notes["forwarded-to-peers"], "legacy dropped addr forwarding")
	t.Logf("addr forwarding: legacy to %s peers, svp2p to %s peers", obs[Legacy].Notes["forwarded-to-peers"], svp)
}

func boolToInt(b bool) int {
	if b {
		return 1
	}

	return 0
}
