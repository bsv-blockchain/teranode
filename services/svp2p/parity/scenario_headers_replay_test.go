package parity

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/svp2p/svp2ptest"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// headersFirstParams makes both implementations run a headers-first round over
// the fixture chain. Both gate the round on "not regtest": svp2p by the wire
// magic (headersync.go isRegtest), legacy by pointer equality with
// chaincfg.RegressionNetParams (netsync manager.go startSync). A copy of the
// regtest params with its own magic and a checkpoint at the fixture tip
// satisfies both, and both nodes and peers frame with that magic.
//
// A checkpoint AT the fixture tip ends the round when the tip is reached; one
// BEYOND the tip (fabricated hash, never reached) keeps the round running for
// the whole scenario, which is the live IBD shape — the round owner is judged
// by the no-progress terminator on every reply.
func headersFirstParams(checkpointHeight int) func(Impl, *svp2ptest.FixtureChain, *settings.Settings) {
	return func(_ Impl, chain *svp2ptest.FixtureChain, s *settings.Settings) {
		params := *s.ChainCfgParams
		params.Net = wire.Custom

		hash := chainhash.Hash{0xC0}
		if checkpointHeight <= len(chain.Headers) {
			hash = chain.Headers[checkpointHeight-1].BlockHash()
		}

		params.Checkpoints = []chaincfg.Checkpoint{{Height: int32(checkpointHeight), Hash: &hash}} //nolint:gosec // fixture height is small
		s.ChainCfgParams = &params
	}
}

// replayScript answers every getheaders with the batch it answered the FIRST
// one with — the peer parity-watchlist scenario 1 asks about.
func replayScript() svp2ptest.Script {
	var (
		mu    sync.Mutex
		first *wire.MsgHeaders
	)

	return svp2ptest.Script{OnGetHeaders: func(p *svp2ptest.ScriptedPeer, m *wire.MsgGetHeaders) []wire.Message {
		mu.Lock()
		defer mu.Unlock()

		if first == nil {
			first = p.HeadersFor(m)
		}

		return []wire.Message{first}
	}}
}

func onePeer(script svp2ptest.Script) func(*testing.T, *svp2ptest.FixtureChain, wire.BitcoinNet) []*svp2ptest.ScriptedPeer {
	return func(t *testing.T, chain *svp2ptest.FixtureChain, net wire.BitcoinNet) []*svp2ptest.ScriptedPeer {
		return []*svp2ptest.ScriptedPeer{svp2ptest.NewScriptedPeer(t, chain, net, script, true)}
	}
}

// TestParity_HeadersReplayIsDropped — watch-list scenario 1. A peer that
// replays its previous headers batch makes no progress; both implementations
// must drop it rather than loop on it.
func TestParity_HeadersReplayIsDropped(t *testing.T) {
	// More than one batch, so the round has to ask twice and the replay of the
	// first batch is what answers the second request.
	const chain = wire.MaxBlockHeadersPerMsg + 100

	obs, _ := RunParity(t, Scenario{
		Name:   "headers-replay",
		Chain:  chain,
		Tweaks: []func(Impl, *svp2ptest.FixtureChain, *settings.Settings){headersFirstParams(chain)},
		Peers:  onePeer(replayScript()),
		Drive: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) {
			n.WaitFor(t, func() bool { return peers[0].Transcript.ClosedBy() == "node" }, 60*time.Second,
				"the node never dropped the replaying peer")
		},
		Observe: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) Observation {
			o := ObserveDefault(t, n, peers)
			o.Notes = map[string]string{"disconnect": disconnectReason(n)}

			return o
		},
		Accepted: []Divergence{
			{Field: "Requests", Reason: "a round that ends at the first replay never reaches getdata on either side; counts are timing-dependent"},
			{Field: "Served", Reason: "as Requests"},
			{Field: "BlocksAccepted", Reason: "whatever each side ingested before dropping the peer is timing-dependent; the drop is the assertion"},
		},
	})

	require.Equal(t, "node", obs[Legacy].Disconnected["peer0"])
	require.Equal(t, "node", obs[Svp2p].Disconnected["peer0"])
	require.GreaterOrEqual(t, obs[Legacy].GetHeadersIn, 2, "legacy must have asked twice before dropping the peer")
	require.GreaterOrEqual(t, obs[Svp2p].GetHeadersIn, 2, "svp2p must have asked twice before dropping the peer")
}

// TestParity_HonestDuplicateReplyMidRoundStaysConnected pins fix 79f0870ba:
// a block announced by inv while the round runs must not cost the honest sync
// peer its connection. The peer serves honestly and, on its first headers
// reply, also announces the chain tip by inv, which before the fix drew a
// second getheaders whose identical answer tripped the no-progress terminator.
func TestParity_HonestDuplicateReplyMidRoundStaysConnected(t *testing.T) {
	const chain = wire.MaxBlockHeadersPerMsg + 100

	var once sync.Once

	script := svp2ptest.Script{OnGetHeaders: func(p *svp2ptest.ScriptedPeer, m *wire.MsgGetHeaders) []wire.Message {
		out := []wire.Message{p.HeadersFor(m)}

		once.Do(func() {
			inv := wire.NewMsgInv()
			tip := p.Chain.Tip()
			_ = inv.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &tip))
			out = append(out, inv)
		})

		return out
	}}

	obs, _ := RunParity(t, Scenario{
		Name:   "honest-duplicate-mid-round",
		Chain:  chain,
		Tweaks: []func(Impl, *svp2ptest.FixtureChain, *settings.Settings){headersFirstParams(chain + 100000)},
		Peers:  onePeer(script),
		Drive: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) {
			// The round asks for the second batch (so the inv landed mid-round
			// and, before the fix, drew its duplicate request), then blocks flow.
			n.WaitFor(t, func() bool { return peers[0].Transcript.Count(svp2ptest.In, "getheaders") >= 2 },
				60*time.Second, "the round never asked for a second batch")
			n.WaitFor(t, func() bool { return n.BestHeight(t) >= 40 }, 90*time.Second,
				"blocks stopped flowing after the mid-round inv")
		},
		Observe: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) Observation {
			o := ObserveDefault(t, n, peers)
			o.Notes = map[string]string{"no-progress": fmt.Sprint(n.Logger.Contains("no sync progress"))}

			return o
		},
		Only: []Impl{Svp2p},
	})

	require.Empty(t, obs[Svp2p].Disconnected, "the honest round owner must stay connected")
	require.Equal(t, "false", obs[Svp2p].Notes["no-progress"], "the terminator must not fire on the inv-drawn duplicate")
	require.GreaterOrEqual(t, obs[Svp2p].BlocksAccepted, uint32(40))
}

// disconnectReason is the line each implementation logs when it drops a peer:
// legacy "Disconnecting (<peer>) reason: ...", svp2p "peer <addr> done: ...".
func disconnectReason(n *nodeUnderTest) string {
	for _, needle := range []string{"Disconnecting (", "] peer 127.0.0.1"} {
		if lines := n.Logger.Matching(needle); len(lines) > 0 {
			return lines[0]
		}
	}

	return "<no disconnect line logged>"
}
