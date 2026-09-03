package parity

import (
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/svp2p/protocol"
	"github.com/bsv-blockchain/teranode/services/svp2p/svp2ptest"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// TestParity_MultiPeerDistribution — watch-list scenario 5, the lead item of
// Phase 3. Three peers over one chain: fast (honest), slow (20 ms per block),
// silent (answers headers and pings, withholds every block). The fast peer is
// listed first so legacy's height-ranked election takes it as the single sync
// peer and its leg stays bounded; legacy never downloads from the others, and
// that is the baseline svp2p's distribution is recorded against.
func TestParity_MultiPeerDistribution(t *testing.T) {
	const chain = 200

	dial := func(_ Impl, _ *svp2ptest.FixtureChain, s *settings.Settings) {
		// One percent of the regtest block interval is six seconds: the silent
		// peer's front block times out inside the test instead of in ten minutes.
		s.Legacy.BlockDownloadTimeoutBasePercent = 1
		s.Legacy.BlockDownloadTimeoutBaseIBDPercent = 1
		s.Legacy.BlockDownloadTimeoutPerPeerPercent = 0
		// The parallel-fetch fuse keeps SVNode's 30 s default on purpose: at the
		// in-process ingest pace every in-flight block crosses a 1 s fuse and is
		// raced to three holders, which is correct behaviour but swamps the
		// duplicate-fetch figure this scenario measures (1004 serves from the
		// slow peer for a 200-block chain, 2026-08-26).
	}

	slowScript := svp2ptest.Script{WriteDelay: func(msg wire.Message, _ int) time.Duration {
		if msg.Command() == "block" {
			return 20 * time.Millisecond
		}

		return 0
	}}

	silentScript := svp2ptest.Script{OnGetData: func(*svp2ptest.ScriptedPeer, *wire.MsgGetData) []wire.Message { return nil }}

	obs, res := RunParity(t, Scenario{
		Name:   "multi-peer-distribution",
		Chain:  chain,
		Pad:    200_000, // see BuildFixtureChainPadded: legacy's byte-rate floor
		Tweaks: []func(Impl, *svp2ptest.FixtureChain, *settings.Settings){dial},
		Peers: func(t *testing.T, c *svp2ptest.FixtureChain, net wire.BitcoinNet) []*svp2ptest.ScriptedPeer {
			return []*svp2ptest.ScriptedPeer{
				svp2ptest.NewScriptedPeer(t, c, net, svp2ptest.Script{}, true), // peer0 fast
				svp2ptest.NewScriptedPeer(t, c, net, slowScript, true),         // peer1 slow
				svp2ptest.NewScriptedPeer(t, c, net, silentScript, true),       // peer2 silent
			}
		},
		Drive: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) {
			n.WaitForHeight(t, chain, 240*time.Second)

			// How long the silent peer had been owing its LAST batch when the
			// sync finished: a peer inside its per-block budget is rightly kept.
			owing := time.Duration(0)
			for _, e := range peers[2].Transcript.Snapshot() {
				if e.Dir == svp2ptest.In && e.Cmd == "getdata" {
					owing = time.Since(e.At)
				}
			}

			n.notes = map[string]string{"silent-owing-at-end": owing.Round(time.Millisecond).String()}
		},
		Observe: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) Observation {
			o := ObserveDefault(t, n, peers)
			o.Notes = map[string]string{}
			for k, v := range n.notes {
				o.Notes[k] = v
			}

			for _, needle := range []string{"disconnecting", "rotating", "ingest failed", "admission", "not mined", "parent", "unrequested", "timed out", "block download timeout", "stalling"} {
				o.Notes[needle] = fmt.Sprint(len(n.Logger.Matching(needle)))
			}

			return o
		},
		Accepted: []Divergence{
			{Field: "Requests", Reason: "legacy schedules blocks from its single sync peer only; svp2p from every useful peer (SVNode's model)"},
			{Field: "Served", Reason: "as Requests"},
			{Field: "Disconnected", Reason: "legacy never judges a non-sync peer; svp2p disconnects the silent holder on the per-block timeout"},
		},
	})

	require.Empty(t, res.Diffs)

	s := obs[Svp2p]
	require.Equal(t, uint32(chain), s.BlocksAccepted)
	// The silent peer is either dropped on the carried per-block clock while
	// it still owes blocks, or — once its first batch has been re-homed and the
	// serving peers take the rest of the window — it is handed nothing more and,
	// owing nothing, is rotated but kept, as SVNode would keep it. Either way
	// it never gets more than one batch per connection plus one re-hand per
	// rotation inside the budget (3 s window, 6 s budget: three batches).
	budget := 6 * time.Second
	batchesPerConn := int(budget/maxLastBlockTime) + 1
	require.LessOrEqual(t, s.Requests["peer2"], batchesPerConn*protocol.MaxBlocksInTransitPerPeer*s.Connections["peer2"],
		"the silent peer is handed at most %d batches per connection", batchesPerConn)
	owing, err := time.ParseDuration(s.Notes["silent-owing-at-end"])
	require.NoError(t, err)
	// Three legitimate outcomes: dropped on the carried clock; handed only its
	// first batch, which the rotation released so it owed nothing more (the
	// serving peers took the rest of the window); or re-handed so late that its
	// budget had not run out when the sync finished.
	require.True(t, s.Disconnected["peer2"] == "node" ||
		s.Requests["peer2"] <= protocol.MaxBlocksInTransitPerPeer ||
		owing < budget+maxLastBlockTime,
		"a silent peer re-handed blocks and owing them past its budget must be dropped (requests=%d, owing %s at the end, disconnected=%q)",
		s.Requests["peer2"], owing, s.Disconnected["peer2"])

	require.GreaterOrEqual(t, s.Served["peer0"]+s.Served["peer1"], chain, "every block crossed the wire from a serving peer")
	require.Positive(t, s.Served["peer1"], "the slow peer must have been given work too")

	// Duplicate fetches come from two rules only: the parallel-fetch race (at
	// most legacy_blockDownloadMaxParallelFetch holders per contested block, one
	// batch at a time) and the blocks re-homed from the silent peer after each
	// per-block timeout. A block that arrives before its parent is RETAINED
	// (orphanBlocks) and ingested from the spool, never fetched again; before
	// that fix this harness measured ~1,100 duplicates on this scenario
	// (ledger residual 1, closed 2026-08-26).
	dup := s.Served["peer0"] + s.Served["peer1"] - chain
	ruleBound := 3*protocol.MaxBlocksInTransitPerPeer + s.Requests["peer2"]

	t.Logf("duplicate fetches: %d (rule-derived bound %d)", dup, ruleBound)
	require.LessOrEqual(t, dup, ruleBound, "duplicate fetches must be bounded by racing plus re-homing; retention must stop parent-missing refetches")

	l := obs[Legacy]
	require.Equal(t, uint32(chain), l.BlocksAccepted)
	t.Logf("legacy baseline: requests=%v served=%v disconnected=%v connections=%v wall=%s", l.Requests, l.Served, l.Disconnected, l.Connections, l.WallClock)
	t.Logf("svp2p:          requests=%v served=%v disconnected=%v connections=%v wall=%s", s.Requests, s.Served, s.Disconnected, s.Connections, s.WallClock)
}
