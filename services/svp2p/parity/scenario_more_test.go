package parity

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/svp2p/svp2ptest"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

func honestPeers(n int) func(*testing.T, *svp2ptest.FixtureChain, wire.BitcoinNet) []*svp2ptest.ScriptedPeer {
	return func(t *testing.T, chain *svp2ptest.FixtureChain, net wire.BitcoinNet) []*svp2ptest.ScriptedPeer {
		peers := make([]*svp2ptest.ScriptedPeer, n)
		for i := range peers {
			peers[i] = svp2ptest.NewScriptedPeer(t, chain, net, svp2ptest.Script{}, true)
		}

		return peers
	}
}

// waitForQuiet blocks until peer's inbound count of cmd stops changing for
// quiet, so a sampled "before" count does not land mid-burst of the node's
// own startup traffic (e.g. its headers-first sync tail). Fails the test if
// the count never settles within max.
func waitForQuiet(t *testing.T, peer *svp2ptest.ScriptedPeer, cmd string, quiet, max time.Duration) {
	t.Helper()

	deadline := time.Now().Add(max)
	last := peer.Transcript.Count(svp2ptest.In, cmd)
	lastChange := time.Now()

	for {
		time.Sleep(50 * time.Millisecond)

		cur := peer.Transcript.Count(svp2ptest.In, cmd)
		if cur != last {
			last, lastChange = cur, time.Now()
		}

		if time.Since(lastChange) >= quiet {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("%s count never went quiet for %s within %s (stuck at %d)", cmd, quiet, max, last)
		}
	}
}

// countStoppedGetHeaders counts inbound getheaders carrying a non-zero
// HashStop: the shape OnInv builds for each fabricated inv (hashStop = that
// entry's own hash), never the zero-HashStop getheaders the node's own
// sync-peer sweep can also send while a peer stays near tip. Sampling on
// this filter, rather than on the raw command count, keeps the amplification
// count exact regardless of when a sweep round lands inside the wait.
func countStoppedGetHeaders(peer *svp2ptest.ScriptedPeer) int {
	n := 0

	for _, e := range peer.Transcript.Snapshot() {
		if e.Dir != svp2ptest.In || e.Cmd != "getheaders" {
			continue
		}

		if gh, ok := e.Msg.(*wire.MsgGetHeaders); ok && gh.HashStop != (chainhash.Hash{}) {
			n++
		}
	}

	return n
}

// firstAsked returns the index of the peer whose transcript holds the earliest
// inbound getheaders, and -1 if none was asked.
func firstAsked(peers []*svp2ptest.ScriptedPeer) int {
	best, at := -1, time.Time{}

	for i, p := range peers {
		for _, e := range p.Transcript.Snapshot() {
			if e.Dir == svp2ptest.In && (e.Cmd == "getheaders" || e.Cmd == "getblocks") {
				if best == -1 || e.At.Before(at) {
					best, at = i, e.At
				}

				break
			}
		}
	}

	return best
}

// TestParity_SyncPeerElectionOrder — watch-list scenario 2. Three peers claim
// heights 10, 200 and 50 (the chain is 200 long and every peer serves all of
// it). Legacy ranks candidates by height; svp2p elects the first eligible
// peer. The verdict records who each side asked first and how long the sync
// took; both must reach the tip.
func TestParity_SyncPeerElectionOrder(t *testing.T) {
	const chain = 60

	claim := func(h int32) svp2ptest.Script {
		return svp2ptest.Script{Version: func(v *wire.MsgVersion) { v.LastBlock = h }}
	}

	obs, _ := RunParity(t, Scenario{
		Name:  "election-order",
		Chain: chain,
		Peers: func(t *testing.T, c *svp2ptest.FixtureChain, net wire.BitcoinNet) []*svp2ptest.ScriptedPeer {
			return []*svp2ptest.ScriptedPeer{
				svp2ptest.NewScriptedPeer(t, c, net, claim(10), true),
				svp2ptest.NewScriptedPeer(t, c, net, claim(200), true),
				svp2ptest.NewScriptedPeer(t, c, net, claim(50), true),
			}
		},
		Drive: func(t *testing.T, n *nodeUnderTest, _ []*svp2ptest.ScriptedPeer) {
			n.WaitForHeight(t, chain, 120*time.Second)
		},
		Observe: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) Observation {
			o := ObserveDefault(t, n, peers)
			o.Notes = map[string]string{"first-asked": fmt.Sprintf("peer%d", firstAsked(peers))}

			return o
		},
		Accepted: []Divergence{
			{Field: "Requests", Reason: "legacy downloads from the sync peer it ranked tallest; svp2p spreads blocks over every peer"},
			{Field: "Served", Reason: "as Requests"},
			{Field: "Disconnected", Reason: "a peer claiming height 10 to a node at 60 may be dropped by one side and kept by the other"},
		},
	})

	require.Equal(t, uint32(chain), obs[Legacy].BlocksAccepted)
	require.Equal(t, uint32(chain), obs[Svp2p].BlocksAccepted)
	// Legacy ranks by claimed height among the candidates PRESENT when startSync
	// runs — often only the first peer to complete its handshake — so which peer
	// it picks varies run to run; what is fixed is that it downloads the whole
	// chain from that one peer and nothing from the others.
	legacyServing := 0
	for _, name := range []string{"peer0", "peer1", "peer2"} {
		if obs[Legacy].Requests[name] > 0 {
			legacyServing++
			require.Equal(t, chain, obs[Legacy].Requests[name], "legacy downloads the whole chain from its single sync peer")
		}
	}

	require.Equal(t, 1, legacyServing, "legacy schedules blocks from exactly one peer")
	require.Positive(t, obs[Svp2p].Requests["peer0"]+obs[Svp2p].Requests["peer2"], "svp2p spreads the window over every useful peer regardless of election")
	t.Logf("election: legacy asked %s first (%s), svp2p asked %s first (%s)",
		obs[Legacy].Notes["first-asked"], obs[Legacy].WallClock.Round(time.Millisecond),
		obs[Svp2p].Notes["first-asked"], obs[Svp2p].WallClock.Round(time.Millisecond))
}

// TestParity_UnsolicitedHeadersScore — watch-list scenario 3. While a
// headers-first round runs on peer0, peer1 pushes one unsolicited 2000-header
// batch and then five honest announcement-sized batches. svp2p's policy scores
// the bulk batch once (20) and the announcements not at all; the honest peer
// must never approach the ban threshold. Legacy drops any unrequested headers.
func TestParity_UnsolicitedHeadersScore(t *testing.T) {
	const chain = wire.MaxBlockHeadersPerMsg + 40

	obs, _ := RunParity(t, Scenario{
		Name:  "unsolicited-headers-score",
		Chain: chain,
		// Checkpoint AT the tip: legacy's headers-first round downloads no blocks
		// until its headers reach the checkpoint, so an unreachable one would
		// stall its leg; both rounds still run long enough for the push below.
		Tweaks: []func(Impl, *svp2ptest.FixtureChain, *settings.Settings){headersFirstParams(chain)},
		Peers:  honestPeers(2),
		Drive: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) {
			sampler := n.sampleScores()

			// Let the round start on whichever peer was elected; the OTHER peer
			// then pushes headers nobody asked it for.
			n.WaitFor(t, func() bool { return firstAsked(peers) != -1 }, 30*time.Second, "no round started")

			pusher := peers[1-firstAsked(peers)]
			n.notes = map[string]string{"pusher": peerName(1 - firstAsked(peers))}

			bulk := wire.NewMsgHeaders()
			for _, h := range pusher.Chain.Headers[:wire.MaxBlockHeadersPerMsg] {
				_ = bulk.AddBlockHeader(h)
			}

			pusher.Send(bulk)

			for i := 0; i < 5; i++ {
				pusher.Send(headersMsg(pusher.Chain.Headers[chain-5+i]))
			}

			n.WaitFor(t, func() bool { return n.BestHeight(t) >= 30 }, 120*time.Second, "blocks stopped flowing")

			n.scores = sampler.Result()
		},
		Observe: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) Observation {
			o := ObserveDefault(t, n, peers)
			o.Scores = map[string]int{}
			o.Notes = n.notes

			for addr, v := range n.scores {
				for i, p := range peers {
					if p.Addr == addr {
						o.Scores[peerName(i)] = v
					}
				}
			}

			return o
		},
		Accepted: []Divergence{
			{Field: "Requests", Reason: "distribution differs by design"},
			{Field: "Served", Reason: "as Requests"},
			{Field: "BlocksAccepted", Reason: "timing"},
			{Field: "Scores", Reason: "legacy does not score unsolicited headers, it disconnects; svp2p scores a bulk batch 20 (Task 11 policy)"},
			{Field: "Disconnected", Reason: "legacy drops a peer for unrequested headers; svp2p keeps an honest peer below the threshold"},
		},
	})

	s := obs[Svp2p]
	pusher := s.Notes["pusher"]
	require.Less(t, s.Scores[pusher], 100, "an honest peer that announces headers must stay far below the ban threshold")
	require.LessOrEqual(t, s.Scores[pusher], 20, "one unsolicited bulk batch is worth 20 at most; announcements are free")
	require.NotEqual(t, "node", s.Disconnected[pusher], "svp2p must keep the honest peer")
	require.Equal(t, "node", obs[Legacy].Disconnected[obs[Legacy].Notes["pusher"]], "legacy drops any peer that sends unrequested headers")
	t.Logf("scores: legacy=%v svp2p=%v; disconnected legacy=%v svp2p=%v", obs[Legacy].Scores, s.Scores, obs[Legacy].Disconnected, s.Disconnected)
}

// TestParity_GetHeadersFlood — watch-list scenario 6. A peer asks for headers
// 300 times as fast as it can. Both nodes must answer every request, keep the
// connection, and not grow the heap without bound.
func TestParity_GetHeadersFlood(t *testing.T) {
	const chain, floods = 30, 300

	obs, _ := RunParity(t, Scenario{
		Name:  "getheaders-flood",
		Chain: chain,
		Peers: honestPeers(1),
		Drive: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) {
			n.WaitForHeight(t, chain, 60*time.Second)

			var before, after runtime.MemStats

			runtime.GC()
			runtime.ReadMemStats(&before)

			genesis := peers[0].Chain.Headers[0].PrevBlock

			for i := 0; i < floods; i++ {
				peers[0].Send(getHeadersFrom(genesis))
			}

			n.WaitFor(t, func() bool { return peers[0].Transcript.Count(svp2ptest.In, "headers") >= floods }, 60*time.Second,
				fmt.Sprintf("only %d of %d getheaders were answered", peers[0].Transcript.Count(svp2ptest.In, "headers"), floods))

			runtime.GC()
			runtime.ReadMemStats(&after)

			n.notes = map[string]string{
				"headers-answered": fmt.Sprint(peers[0].Transcript.Count(svp2ptest.In, "headers")),
				"heap-delta-MiB":   fmt.Sprintf("%.1f", (float64(after.HeapAlloc)-float64(before.HeapAlloc))/(1<<20)),
			}
		},
		Observe: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) Observation {
			o := ObserveDefault(t, n, peers)
			o.Notes = n.notes

			return o
		},
	})

	for _, impl := range []Impl{Legacy, Svp2p} {
		require.Empty(t, obs[impl].Disconnected, "%s must keep a flooding-but-valid peer", impl)
		require.Equal(t, fmt.Sprint(floods), obs[impl].Notes["headers-answered"], "%s must answer every getheaders", impl)
	}

	t.Logf("flood: legacy heap delta %s MiB, svp2p heap delta %s MiB", obs[Legacy].Notes["heap-delta-MiB"], obs[Svp2p].Notes["heap-delta-MiB"])
}

// TestParity_InvGetHeadersAmplification — watch-list scenario 10. A peer
// announces 500 distinct block hashes that no chain contains. SVNode answers
// each with its own getheaders, and svp2p carries that rule unchanged; the
// verdict records the amplification on both sides.
func TestParity_InvGetHeadersAmplification(t *testing.T) {
	const chain, fabricated = 20, 500

	obs, _ := RunParity(t, Scenario{
		Name:  "inv-getheaders-amplification",
		Chain: chain,
		Peers: honestPeers(1),
		Drive: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) {
			n.WaitForHeight(t, chain, 60*time.Second)
			waitForQuiet(t, peers[0], "getheaders", 1500*time.Millisecond, 15*time.Second)

			before := countStoppedGetHeaders(peers[0])

			inv := wire.NewMsgInv()
			for i := 0; i < fabricated; i++ {
				h := chainhash.Hash{0xFA, byte(i >> 8), byte(i)}
				_ = inv.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &h))
			}

			peers[0].Send(inv)

			time.Sleep(3 * time.Second)

			n.notes = map[string]string{"getheaders-drawn": fmt.Sprint(countStoppedGetHeaders(peers[0]) - before)}
		},
		Observe: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) Observation {
			o := ObserveDefault(t, n, peers)
			o.Notes = n.notes

			return o
		},
		Accepted: []Divergence{
			{Field: "Disconnected", Reason: "recorded: whether a side drops a peer that announces unknown hashes"},
			{Field: "Requests", Reason: "legacy (bsvd shape) answers an unknown block inv with a block GETDATA; SVNode and svp2p answer with getheaders"},
		},
	})

	require.Equal(t, fmt.Sprint(fabricated), obs[Svp2p].Notes["getheaders-drawn"], "svp2p answers every distinct unknown hash, as SVNode does")
	require.Equal(t, "0", obs[Legacy].Notes["getheaders-drawn"], "legacy asks for the blocks themselves instead")
	require.GreaterOrEqual(t, obs[Legacy].Requests["peer0"], fabricated, "legacy sent a getdata per fabricated hash")
	t.Logf("amplification: legacy drew %s getheaders, svp2p drew %s (input %d hashes)", obs[Legacy].Notes["getheaders-drawn"], obs[Svp2p].Notes["getheaders-drawn"], fabricated)
}

// TestParity_UserAgentFence — watch-list scenario 12. A peer announcing an
// agent without "Bitcoin SV" or "BSV": legacy rejects and bans it, svp2p (and
// SVNode) accept it. Pinned as a divergence for the cutover decision.
func TestParity_UserAgentFence(t *testing.T) {
	obs, _ := RunParity(t, Scenario{
		Name:  "user-agent-fence",
		Chain: 5,
		Peers: func(t *testing.T, c *svp2ptest.FixtureChain, net wire.BitcoinNet) []*svp2ptest.ScriptedPeer {
			return []*svp2ptest.ScriptedPeer{svp2ptest.NewScriptedPeer(t, c, net,
				svp2ptest.Script{Version: func(v *wire.MsgVersion) { v.UserAgent = "/scriptpeer:0.1/" }}, true)}
		},
		Drive: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) {
			n.WaitFor(t, func() bool { return peers[0].Transcript.ClosedBy() == "node" || n.BestHeight(t) == 5 }, 30*time.Second, "")
		},
		Observe: ObserveDefault,
		Accepted: []Divergence{
			{Field: "Disconnected", Reason: "legacy rejects and bans a non-BSV user agent (peer_server.go:617); svp2p and SVNode have no such fence"},
			{Field: "Requests", Reason: "follows from the fence"},
			{Field: "Served", Reason: "follows from the fence"},
			{Field: "BlocksAccepted", Reason: "follows from the fence"},
		},
	})

	require.Equal(t, "node", obs[Legacy].Disconnected["peer0"], "legacy drops the foreign agent")
	require.Empty(t, obs[Svp2p].Disconnected, "svp2p accepts it")
	require.Equal(t, uint32(5), obs[Svp2p].BlocksAccepted)
}

func getHeadersFrom(locator chainhash.Hash) *wire.MsgGetHeaders {
	m := wire.NewMsgGetHeaders()
	m.ProtocolVersion = wire.ProtocolVersion
	_ = m.AddBlockLocatorHash(&locator)

	return m
}
