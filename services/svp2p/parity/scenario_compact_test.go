package parity

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/svp2p/svp2ptest"
	"github.com/bsv-blockchain/teranode/settings"
	inmemorykafka "github.com/bsv-blockchain/teranode/util/kafka/in_memory_kafka"
	"github.com/stretchr/testify/require"
)

// The fixture chain row 15 runs on. Five blocks is enough for a coinbase to
// mature — util/test.CreateBaseTestSettings already sets CoinbaseMaturity to 1
// on its own copy of the regtest parameters, so a coinbase at height 1 spent in
// block 6 is five blocks deep — and still leaves the run short.
const (
	compactChain = 5
	compactFee   = uint64(2000)
)

// compactNonce keys the short IDs of every announcement in this file. Any value
// serves; the peer and the node derive the same (k0,k1) from it and the header.
const compactNonce = uint64(0x5643_0F15)

// compactEnabled turns compact blocks on for the svp2p leg.
//
// Applied to ONE leg because legacy_compactBlocks is unread by services/legacy:
// setting it for both would be noise. The txmeta topic the index is fed from is
// NOT set here — newNode gives every leg its own (isolateTxMetaTopic), so no
// scenario has to remember to.
func compactEnabled(impl Impl, _ *svp2ptest.FixtureChain, s *settings.Settings) {
	if impl != Svp2p {
		return
	}

	s.Legacy.CompactBlocks = true
	s.Legacy.CompactBlocksRecentTxs = 1024
}

// compactTweaks is the settings hook every scenario in this file uses.
var compactTweaks = []func(Impl, *svp2ptest.FixtureChain, *settings.Settings){compactEnabled}

// compactRecorder records every getblocktxn the node sends and answers it
// honestly from the fixture chain, which is what ScriptedPeer does with no
// script at all — the override exists to keep the requested slot list, the fact
// row 15 is a verdict on.
type compactRecorder struct {
	mu       sync.Mutex
	requests [][]uint32
}

func (r *compactRecorder) script() svp2ptest.Script {
	return svp2ptest.Script{
		OnGetBlockTxn: func(p *svp2ptest.ScriptedPeer, _ net.Conn, m *wire.MsgGetBlockTxn) []wire.Message {
			r.record(m.Indexes)

			reply := p.BlockTxnFor(m)
			if reply == nil {
				return nil
			}

			return []wire.Message{reply}
		},
	}
}

func (r *compactRecorder) record(indexes []uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()

	kept := make([]uint32, len(indexes))
	copy(kept, indexes)

	r.requests = append(r.requests, kept)
}

func (r *compactRecorder) seen() [][]uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([][]uint32, len(r.requests))
	copy(out, r.requests)

	return out
}

// seedCompactBlock brings one leg to the point an announcement can be made: the
// node is synced to the fixture tip, one transaction of the block to come has
// travelled the production relay path into the recent-transaction index, and
// the block itself is built and serveable but unannounced.
//
// It returns the block, the transaction the node now holds, and the one it has
// never seen — the single slot a getblocktxn must ask for.
//
// The waits it performs are only meaningful on the svp2p leg; on the legacy leg
// there is no index and no topic, and it stops after the relay.
func seedCompactBlock(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) (*wire.MsgBlock, *bt.Tx, *bt.Tx) {
	t.Helper()

	n.WaitForHeight(t, compactChain, 60*time.Second)

	if n.Impl == Svp2p {
		// The in-memory broker drops what it is given while a topic has no
		// consumer, and the txmeta consumer only starts once its FSM gate has
		// ticked (bridge.pollTxRunningGate, one second). A transaction relayed
		// before that is never indexed, so the relay below waits for the
		// consumer to be registered.
		name := n.txMetaTopic()

		n.WaitFor(t, func() bool { return inmemorykafka.GetSharedBroker().HasConsumer(name) },
			60*time.Second, "the txmeta consumer never registered on "+name)
	}

	chain := peers[0].Chain

	// held travels to the node before the announcement, so the index can fill
	// its slot; gap never does, so it is the one slot a getblocktxn has to ask
	// for.
	held := chain.SpendCoinbase(t, 1, compactFee)
	gap := chain.SpendCoinbase(t, 2, compactFee)

	block := chain.BuildNextBlock(t, n.Settings, []*bt.Tx{held, gap})

	peers[0].Send(svp2ptest.WireTx(t, held))

	if n.Impl == Svp2p {
		// The barrier is the index itself, not the node's own inv for the
		// transaction: the inv is raised by the peer-sourced path
		// (txIngestor.Ingest) as well, which never touches the index, so it
		// would let the announcement race the Kafka round trip that fills it.
		n.WaitFor(t, func() bool { return n.RecentTxIndexLen() > 0 },
			60*time.Second, "the relayed transaction never reached the recent-transaction index")
	}

	return block, held, gap
}

// TestParity_CompactBlockReceive — watch-list scenario 15.
//
// A scripted peer syncs both legs to the fixture tip, relays one transaction,
// and then announces a block holding that transaction and one the node has
// never seen. svp2p must reconstruct the block from its recent-transaction
// index and ONE getblocktxn for the single gap, never asking for the block
// itself; legacy, which has no compact block support and therefore never sent
// sendcmpct, is announced to by inv — SVNode's own rule — and downloads the
// whole block.
//
// The relay is not a shortcut around the index: the transaction travels the
// production path end to end (peer tx message, validator, txmeta topic,
// bridge.StartTxMetaConsumer, RecentTxIndex.Add), and the node's own inv for
// it is what tells the scenario the round trip has finished.
func TestParity_CompactBlockReceive(t *testing.T) {
	recorder := &compactRecorder{}

	obs, _ := RunParity(t, Scenario{
		Name:   "compact-block-receive",
		Chain:  compactChain,
		Tweaks: compactTweaks,
		Peers: func(t *testing.T, chain *svp2ptest.FixtureChain, netMagic wire.BitcoinNet) []*svp2ptest.ScriptedPeer {
			return []*svp2ptest.ScriptedPeer{
				svp2ptest.NewScriptedPeer(t, chain, netMagic, svp2ptest.Script{}, true),
				svp2ptest.NewScriptedPeer(t, chain, netMagic, recorder.script(), true),
			}
		},
		Drive: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) {
			block, held, gap := seedCompactBlock(t, n, peers)
			hash := block.Header.BlockHash()

			if n.Impl == Svp2p {
				require.NoError(t, peers[1].AnnounceCompact(block, compactNonce, []int{0}))
			} else {
				// legacy sent no sendcmpct, so SVNode's rule makes this an inv
				// announcement; legacy learns the header from the headers round
				// the inv draws, which is why the header is published here and
				// not for the svp2p leg.
				peers[0].Chain.PublishHeader(t, block)

				inv := wire.NewMsgInv()
				require.NoError(t, inv.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &hash)))

				peers[1].Send(inv)
			}

			n.WaitForHeight(t, compactChain+1, 90*time.Second)

			n.notes = map[string]string{
				"announced-block":   hash.String(),
				"held-tx":           held.TxID(),
				"gap-tx":            gap.TxID(),
				"getblocktxn":       fmt.Sprint(peers[1].Transcript.Count(svp2ptest.In, wire.CmdGetBlockTxn)),
				"gap-requests":      fmt.Sprint(recorder.seen()),
				"getdata-for-block": fmt.Sprint(blockRequests(peers, hash)),
				"sendcmpct":         fmt.Sprint(peers[1].Transcript.Count(svp2ptest.In, wire.CmdSendcmpct)),
				"ingested-height":   fmt.Sprint(n.BestHeight(t)),
			}
		},
		Observe: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) Observation {
			o := ObserveDefault(t, n, peers)
			o.Notes = n.notes

			return o
		},
		Accepted: []Divergence{
			{Field: "Requests", Reason: "the announced block is a getdata on legacy and a getblocktxn on svp2p; that difference IS scenario 15"},
			{Field: "Served", Reason: "as Requests"},
		},
	})

	svp := obs[Svp2p].Notes
	legacyNotes := obs[Legacy].Notes

	require.Equal(t, "1", svp["sendcmpct"], "svp2p must have offered compact blocks before the peer may announce one")
	require.Equal(t, "1", svp["getblocktxn"], "one gap must cost exactly one getblocktxn")
	require.Equal(t, "[[2]]", svp["gap-requests"], "only the slot the index could not fill may be requested")
	require.Equal(t, "0", svp["getdata-for-block"], "a reconstructed block must never be downloaded as well")
	require.Equal(t, fmt.Sprint(compactChain+1), svp["ingested-height"])

	require.Equal(t, "0", legacyNotes["sendcmpct"], "legacy has no compact block support and must never offer one")
	require.Equal(t, "0", legacyNotes["getblocktxn"], "legacy must not answer a compact exchange it never joined")
	require.Equal(t, "1", legacyNotes["getdata-for-block"], "legacy must download the whole block")
	require.Equal(t, fmt.Sprint(compactChain+1), legacyNotes["ingested-height"])

	require.Equal(t, obs[Legacy].BlocksAccepted, obs[Svp2p].BlocksAccepted, "both legs must end on the same height")

	t.Logf("compact block receive: svp2p asked %s getblocktxn for slots %s and %s getdata; legacy asked %s getdata",
		svp["getblocktxn"], svp["gap-requests"], svp["getdata-for-block"], legacyNotes["getdata-for-block"])
}

// blockRequests counts the getdata for one block hash across every peer.
func blockRequests(peers []*svp2ptest.ScriptedPeer, hash chainhash.Hash) int {
	count := 0

	for _, p := range peers {
		for _, e := range p.Transcript.Snapshot() {
			if e.Dir != svp2ptest.In {
				continue
			}

			getData, ok := e.Msg.(*wire.MsgGetData)
			if !ok {
				continue
			}

			for _, vect := range getData.InvList {
				if vect.Type == wire.InvTypeBlock && vect.Hash.IsEqual(&hash) {
					count++
				}
			}
		}
	}

	return count
}

// TestParity_CompactBlockShortBlockTxnIsBanned — watch-list scenario 15, the
// invalid reply sub-case.
//
// The gap request is answered with a blocktxn carrying nothing, so the arity
// check FillBlock performs (blockencodings.cpp:264-285) fails. svp2p must take
// ProcessBlockTxnMessage's READ_STATUS_INVALID branch
// (net_processing.cpp:3610-3616): mark the block failed, score the peer 100 for
// "invalid-cmpctblk-txns", and drop it. The block must not be ingested.
//
// svp2p alone: legacy joins no compact exchange, so there is nothing to compare.
func TestParity_CompactBlockShortBlockTxnIsBanned(t *testing.T) {
	// The reply the node asked for, emptied. An empty blocktxn is a legal wire
	// message; it is the count that is the lie.
	short := svp2ptest.Script{
		OnGetBlockTxn: func(_ *svp2ptest.ScriptedPeer, _ net.Conn, m *wire.MsgGetBlockTxn) []wire.Message {
			return []wire.Message{wire.NewMsgBlockTxn(&m.BlockHash)}
		},
	}

	obs, _ := RunParity(t, Scenario{
		Name:   "compact-block-short-blocktxn",
		Chain:  compactChain,
		Only:   []Impl{Svp2p},
		Tweaks: compactTweaks,
		Peers: func(t *testing.T, chain *svp2ptest.FixtureChain, netMagic wire.BitcoinNet) []*svp2ptest.ScriptedPeer {
			return []*svp2ptest.ScriptedPeer{
				svp2ptest.NewScriptedPeer(t, chain, netMagic, svp2ptest.Script{}, true),
				svp2ptest.NewScriptedPeer(t, chain, netMagic, short, true),
			}
		},
		Drive: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) {
			sampler := n.sampleScores()

			block, _, _ := seedCompactBlock(t, n, peers)

			require.NoError(t, peers[1].AnnounceCompact(block, compactNonce, []int{0}))

			n.WaitFor(t, func() bool { return peers[1].Transcript.ClosedBy() != "" }, 60*time.Second,
				"the peer that answered with a short blocktxn was never dropped")

			n.scores = sampler.Result()

			n.notes = map[string]string{
				"getblocktxn":     fmt.Sprint(peers[1].Transcript.Count(svp2ptest.In, wire.CmdGetBlockTxn)),
				"peer1-score":     fmt.Sprint(n.scores[peers[1].Addr]),
				"disconnect-line": fmt.Sprint(n.Logger.Matching("done:")),
				"ingested-height": fmt.Sprint(n.BestHeight(t)),
			}
		},
		Observe: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) Observation {
			o := ObserveDefault(t, n, peers)
			o.Notes = n.notes
			o.Scores = n.scores

			return o
		},
	})

	notes := obs[Svp2p].Notes

	require.Equal(t, "1", notes["getblocktxn"], "the gap must have been asked for once")
	require.Equal(t, "100", notes["peer1-score"], "an invalid blocktxn is worth SVNode's own DoS(100)")
	require.Equal(t, fmt.Sprint(compactChain), notes["ingested-height"], "a block that failed reconstruction must not be ingested")
	require.Equal(t, "node", obs[Svp2p].Disconnected["peer1"], "the node must drop the peer")
}

// unexpectedBlockTxnLog is the Debugf compactdispatch.go BlockTxn writes on the
// unsolicited path (net_processing.cpp:3602-3606, "Peer %d sent us block
// transactions for block we weren't expecting"). RecordingLogger records Debugf,
// so it is an observable fact and not just a log line.
const unexpectedBlockTxnLog = "sent us block transactions for block"

// TestParity_UnsolicitedBlockTxnIsDroppedUnscored — watch-list scenario 15, the
// unsolicited reply sub-case.
//
// A blocktxn nobody asked for is logged and dropped, with no Misbehaving call
// and no MarkBlockAsFailed (net_processing.cpp:3602-3606). It is a timing
// artefact — a reply racing a claim the node released — not evidence of malice,
// so the peer keeps both its score of zero and its connection.
//
// svp2p alone, for the same reason as the sub-case above.
func TestParity_UnsolicitedBlockTxnIsDroppedUnscored(t *testing.T) {
	obs, _ := RunParity(t, Scenario{
		Name:   "compact-unsolicited-blocktxn",
		Chain:  compactChain,
		Only:   []Impl{Svp2p},
		Tweaks: compactTweaks,
		Peers:  honestPeers(2),
		Drive: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) {
			sampler := n.sampleScores()

			n.WaitForHeight(t, compactChain, 60*time.Second)

			// A well-formed reply to a request that was never made: the block
			// is one the node holds, and no cmpctblock for it was ever sent.
			chain := peers[0].Chain
			tip := chain.Tip()

			reply := wire.NewMsgBlockTxn(&tip)
			require.NoError(t, reply.AddTransaction(chain.Blocks[tip].Transactions[0]))

			peers[1].Send(reply)

			// GATE, not a budget. The node's own Debugf for this branch
			// (compactdispatch.go BlockTxn, "sent us block transactions for
			// block we weren't expecting") is the positive fact that the
			// message was routed to PeerManager.BlockTxn and dropped there.
			// Waiting on it first is what makes the silence below evidence of
			// a decision rather than of a message still in flight.
			n.WaitFor(t, func() bool { return n.Logger.Contains(unexpectedBlockTxnLog) }, 60*time.Second,
				"the blocktxn never reached the unsolicited branch")

			// Only now is a bounded wait meaningful: the branch has run, so a
			// score or a disconnect would already be on its way.
			n.WaitFor(t, func() bool { return peers[1].Transcript.ClosedBy() != "" }, 5*time.Second, "")

			n.scores = sampler.Result()

			n.notes = map[string]string{
				"unsolicited-branch": fmt.Sprint(n.Logger.Contains(unexpectedBlockTxnLog)),
				"peer1-score":        fmt.Sprint(n.scores[peers[1].Addr]),
				"still-connected":    fmt.Sprint(n.ConnectedCount(t)),
				"ingested-height":    fmt.Sprint(n.BestHeight(t)),
			}
		},
		Observe: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) Observation {
			o := ObserveDefault(t, n, peers)
			o.Notes = n.notes
			o.Scores = n.scores

			return o
		},
	})

	notes := obs[Svp2p].Notes

	require.Equal(t, "true", notes["unsolicited-branch"],
		"the test must prove the message was handled and dropped, not merely that nothing happened")
	require.Equal(t, "0", notes["peer1-score"], "an unsolicited blocktxn earns no score")
	require.Equal(t, "2", notes["still-connected"], "neither peer may be dropped for it")
	require.Equal(t, fmt.Sprint(compactChain), notes["ingested-height"], "the chain must not move")
	require.Empty(t, obs[Svp2p].Disconnected, "no peer may be dropped")
}

// unreconstructableLog is the Debugf manager.go writes on the READ_STATUS_FAILED
// branch (net_processing.cpp:3618-3623, "Might have collided, fall back to
// getdata now"). No score is applied there, so this line is the only evidence
// the branch ran.
const unreconstructableLog = "unreconstructable, falling back to getdata"

// TestParity_CompactBlockWrongGapFallsBackToGetData — watch-list scenario 15,
// the READ_STATUS_FAILED sub-case.
//
// The gap request is answered with the right COUNT and the wrong CONTENT: one
// transaction, but not the one the slot asked for. The arity check therefore
// passes and this is NOT the invalid branch; the assembler's own short-ID check
// fails the slot instead (compactblock.go readGap, which sets readFailed).
//
// A short ID is 48 bits, so an honest peer's transaction can hash onto the slot
// we asked about. net_processing.cpp:3618-3623 treats that as a possible
// collision and not as malice — "Might have collided, fall back to getdata now"
// — with no Misbehaving call: the block goes back on offer and the ordinary
// getdata path fetches it. Both legs must reach the same height.
//
// This row is what found the defect the preceding commit fixes: BlockDone's
// readFailed case used to log the fallback and then leave the score and the
// disconnect the ingest failure had already set, so a collision cost an honest
// peer the same 100 as a malicious fill and stranded the block with it. The
// paired unit test is protocol.TestBlockDoneCompactStatusDecidesThePeersFate;
// 15b is the control that readInvalid still scores 100 and still drops.
func TestParity_CompactBlockWrongGapFallsBackToGetData(t *testing.T) {
	// The block's coinbase: a transaction the peer certainly holds, that decodes
	// cleanly, and whose short ID is not the requested slot's.
	wrongGap := svp2ptest.Script{
		OnGetBlockTxn: func(p *svp2ptest.ScriptedPeer, _ net.Conn, m *wire.MsgGetBlockTxn) []wire.Message {
			block, known := p.Chain.Block(m.BlockHash)
			if !known {
				return nil
			}

			reply := wire.NewMsgBlockTxn(&m.BlockHash)

			for range m.Indexes {
				_ = reply.AddTransaction(block.Transactions[0])
			}

			return []wire.Message{reply}
		},
	}

	obs, _ := RunParity(t, Scenario{
		Name:   "compact-block-wrong-gap",
		Chain:  compactChain,
		Tweaks: compactTweaks,
		Peers: func(t *testing.T, chain *svp2ptest.FixtureChain, netMagic wire.BitcoinNet) []*svp2ptest.ScriptedPeer {
			return []*svp2ptest.ScriptedPeer{
				svp2ptest.NewScriptedPeer(t, chain, netMagic, svp2ptest.Script{}, true),
				svp2ptest.NewScriptedPeer(t, chain, netMagic, wrongGap, true),
			}
		},
		Drive: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) {
			sampler := n.sampleScores()

			block, _, _ := seedCompactBlock(t, n, peers)
			hash := block.Header.BlockHash()

			if n.Impl == Svp2p {
				require.NoError(t, peers[1].AnnounceCompact(block, compactNonce, []int{0}))

				// GATE: the fallback branch ran. Asserted before the height
				// below, so a block that arrived some other way cannot pass for
				// the fallback.
				n.WaitFor(t, func() bool { return n.Logger.Contains(unreconstructableLog) }, 60*time.Second,
					"the wrong gap transaction never reached the fallback branch")

				// Nothing else is done here. The cmpctblock already recorded
				// that peer1 holds this block (updateBlockAvailability), and
				// BlockFailed put it back on offer, so the next syncPass walk
				// must re-offer it to that same peer on its own. Prompting a
				// second peer with an inv would prove only that the rig can
				// route around the fallback.
			} else {
				peers[0].Chain.PublishHeader(t, block)

				inv := wire.NewMsgInv()
				require.NoError(t, inv.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &hash)))

				peers[1].Send(inv)
			}

			n.WaitForHeight(t, compactChain+1, 90*time.Second)

			n.scores = sampler.Result()

			n.notes = map[string]string{
				"fallback-branch":   fmt.Sprint(n.Logger.Contains(unreconstructableLog)),
				"getblocktxn":       fmt.Sprint(peers[1].Transcript.Count(svp2ptest.In, wire.CmdGetBlockTxn)),
				"getdata-for-block": fmt.Sprint(blockRequests(peers, hash)),
				"peer1-score":       fmt.Sprint(n.scores[peers[1].Addr]),
				"ingested-height":   fmt.Sprint(n.BestHeight(t)),
			}
		},
		Observe: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) Observation {
			o := ObserveDefault(t, n, peers)
			o.Notes = n.notes
			o.Scores = n.scores

			return o
		},
		Accepted: []Divergence{
			{Field: "Requests", Reason: "svp2p pays one getblocktxn and one wasted reconstruction before the getdata legacy makes straight away"},
			{Field: "Served", Reason: "as Requests"},
			{Field: "Scores", Reason: "legacy's figure is scraped from its log, which never names a zero; svp2p reports a live one"},
		},
	})

	svp := obs[Svp2p].Notes

	require.Equal(t, "true", svp["fallback-branch"], "the READ_STATUS_FAILED branch must be the one that ran")
	require.Equal(t, "1", svp["getblocktxn"], "the gap must have been asked for once")
	require.Equal(t, "0", svp["peer1-score"], "a possible short-ID collision is not malice and earns no score")
	require.Empty(t, obs[Svp2p].Disconnected, "no peer may be dropped for a collision")
	require.Equal(t, fmt.Sprint(compactChain+1), svp["ingested-height"], "the block must still arrive")

	getData, err := strconv.Atoi(svp["getdata-for-block"])
	require.NoError(t, err)
	require.GreaterOrEqual(t, getData, 1, "the block must arrive by the ordinary getdata path")

	require.Equal(t, obs[Legacy].BlocksAccepted, obs[Svp2p].BlocksAccepted, "both legs must end on the same height")

	t.Logf("compact wrong gap: svp2p asked %s getblocktxn, fell back to %s getdata, scored the peer %s, reached height %s",
		svp["getblocktxn"], svp["getdata-for-block"], svp["peer1-score"], svp["ingested-height"])
}

// notWantedLog is the Debugf claimCompactBlock writes when wantCompact refuses
// an announcement (net_processing.cpp:3825, the height ceiling).
const notWantedLog = "not wanted at tip height"

// gatedServer serves fixture blocks only while its gate is open. It is how a
// scenario holds the node's ACTIVE tip still while its header index runs ahead:
// the peer keeps answering getheaders, and stops answering getdata.
type gatedServer struct {
	open atomic.Bool
}

func (g *gatedServer) script() svp2ptest.Script {
	return svp2ptest.Script{
		OnGetData: func(p *svp2ptest.ScriptedPeer, m *wire.MsgGetData) []wire.Message {
			if !g.open.Load() {
				return nil
			}

			var out []wire.Message

			for _, inv := range m.InvList {
				if inv == nil || inv.Type != wire.InvTypeBlock {
					continue
				}

				if block, known := p.Chain.Block(inv.Hash); known {
					out = append(out, block)
				}
			}

			return out
		},
	}
}

// TestParity_CompactBlockAboveHeightCeilingIsDeclined — watch-list scenario 15,
// the height ceiling sub-case.
//
// ProcessCompactBlockMessage declines to reconstruct a block more than
// MaxCompactBlockHeightAhead (2) above the ACTIVE tip (net_processing.cpp:3825,
// blockdownload.go wantCompact). The header is still accepted — this port runs
// the header accept unconditionally, ahead of every guard, which is what the
// :3913-3921 "same treatment as a header message" branch achieves — and the
// block arrives later by the ordinary getdata path.
//
// The rig holds the node's active tip at 5 while its header index reaches 7: the
// peer publishes headers 6 and 7 and then withholds every block body. Block 8 is
// then announced by cmpctblock. Of the four guards wantCompact applies, only the
// ceiling can be the one that refuses:
//
//   - hasData: the node has never held block 8.
//   - chain work: block 8 is three blocks above the tip.
//   - CanDirectFetch: the fixture tip is ~30 minutes old and the regtest window
//     is 600s * 20 = 3h20m, so this passes (blockdownload.go canDirectFetch).
//   - the ceiling: 8 > 5 + 2.
//
// svp2p alone: legacy joins no compact exchange.
func TestParity_CompactBlockAboveHeightCeilingIsDeclined(t *testing.T) {
	gate := &gatedServer{}
	gate.open.Store(true)

	obs, _ := RunParity(t, Scenario{
		Name:   "compact-block-above-ceiling",
		Chain:  compactChain,
		Only:   []Impl{Svp2p},
		Tweaks: compactTweaks,
		Peers: func(t *testing.T, chain *svp2ptest.FixtureChain, netMagic wire.BitcoinNet) []*svp2ptest.ScriptedPeer {
			return []*svp2ptest.ScriptedPeer{svp2ptest.NewScriptedPeer(t, chain, netMagic, gate.script(), true)}
		},
		Drive: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) {
			sampler := n.sampleScores()
			peer := peers[0]
			chain := peer.Chain

			n.WaitForHeight(t, compactChain, 60*time.Second)

			// From here the peer answers no getdata, so the active tip stays at
			// 5 however many headers it goes on to publish.
			gate.open.Store(false)

			sixth := chain.BuildNextBlock(t, n.Settings, nil)
			seventh := chain.BuildBlockOn(t, n.Settings, sixth.Header.BlockHash(), nil)
			eighth := chain.BuildBlockOn(t, n.Settings, seventh.Header.BlockHash(), nil)

			// 6 and 7 are announced so block 8's parent is in the node's header
			// index and the announcement connects; 8 is not, so the node can
			// only learn it from the cmpctblock.
			chain.PublishHeader(t, sixth)
			chain.PublishHeader(t, seventh)

			hash := eighth.Header.BlockHash()

			n.WaitFor(t, func() bool {
				_, known := n.HeaderKnown(seventh.Header.BlockHash())
				return known
			}, 60*time.Second, "the node never took the headers that put block 8 above its tip")

			require.Equal(t, uint32(compactChain), n.BestHeight(t), "the active tip must still be behind")

			require.NoError(t, peer.AnnounceCompact(eighth, compactNonce, []int{0}))

			n.WaitFor(t, func() bool { return n.Logger.Contains(notWantedLog) }, 60*time.Second,
				"the announcement above the ceiling was never declined")

			height, headerKnown := n.HeaderKnown(hash)

			// Serving resumes: the declined block must still arrive, by the
			// path the ceiling sends it down.
			gate.open.Store(true)

			n.WaitForHeight(t, compactChain+3, 120*time.Second)

			n.scores = sampler.Result()

			n.notes = map[string]string{
				"declined":          fmt.Sprint(n.Logger.Contains(notWantedLog)),
				"header-accepted":   fmt.Sprint(headerKnown),
				"header-height":     fmt.Sprint(height),
				"getblocktxn":       fmt.Sprint(peer.Transcript.Count(svp2ptest.In, wire.CmdGetBlockTxn)),
				"getdata-for-block": fmt.Sprint(blockRequests(peers, hash)),
				"peer0-score":       fmt.Sprint(n.scores[peer.Addr]),
				"ingested-height":   fmt.Sprint(n.BestHeight(t)),
			}
		},
		Observe: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) Observation {
			o := ObserveDefault(t, n, peers)
			o.Notes = n.notes
			o.Scores = n.scores

			return o
		},
	})

	notes := obs[Svp2p].Notes

	require.Equal(t, "true", notes["declined"], "the announcement must be refused by wantCompact")
	require.Equal(t, "0", notes["getblocktxn"], "a declined announcement must cost no gap request")
	require.Equal(t, "true", notes["header-accepted"],
		"the header accept runs ahead of every guard, so a declined block is still in the index")
	require.Equal(t, fmt.Sprint(compactChain+3), notes["header-height"])
	require.Equal(t, "0", notes["peer0-score"], "announcing too far ahead is not misbehaviour")
	getData, err := strconv.Atoi(notes["getdata-for-block"])
	require.NoError(t, err)
	require.GreaterOrEqual(t, getData, 1, "the block must arrive by the ordinary getdata path")
	require.Equal(t, fmt.Sprint(compactChain+3), notes["ingested-height"])
	require.Empty(t, obs[Svp2p].Disconnected, "the peer must keep its connection")

	t.Logf("compact above ceiling: declined at tip %d, header accepted at height %s, %s getblocktxn, %s getdata",
		compactChain, notes["header-height"], notes["getblocktxn"], notes["getdata-for-block"])
}
