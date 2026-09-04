package parity

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/svp2p/svp2ptest"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// scoreRow is one line of parity-watchlist scenario 4: a peer that commits one
// offence, the svp2p total the table predicts, and whether the peer must go.
type scoreRow struct {
	// afterSync is sent by Drive once the node has the whole chain, so no
	// getheaders of the node's own is outstanding: legacy treats headers that
	// arrive while one is as solicited, which made the header rows racy when
	// they were pushed at handshake time.
	afterSync  func(chain *svp2ptest.FixtureChain) []wire.Message
	name       string
	script     func(chain *svp2ptest.FixtureChain) svp2ptest.Script
	svp2pScore int
	dropped    bool
	// decoder marks offences go-wire refuses to decode (headers > 2000, addr
	// > 1000): the connection fails before any scorer runs, on both sides.
	decoder bool
	// legacyDrops is what legacy does with the same peer (observed 2026-08-26
	// and pinned here so the baseline cannot drift unnoticed).
	legacyDrops bool
	accepted    []Divergence
}

// svnodeKeeps is the accepted divergence for rows where svp2p follows SVNode's
// Misbehaving(n) and keeps the connection while legacy disconnects outright.
func svnodeKeeps(cite string) []Divergence {
	return []Divergence{
		{Field: "Scores", Reason: "legacy does not score this, it disconnects; svp2p carries SVNode's " + cite},
		{Field: "Disconnected", Reason: "legacy disconnects; SVNode and svp2p keep the connection below the ban threshold"},
	}
}

func versionMsg() *wire.MsgVersion {
	me := wire.NewNetAddressIPPort(net.ParseIP("127.0.0.1"), 0, wire.SFNodeNetwork)
	v := wire.NewMsgVersion(me, me, 99, 0)
	v.UserAgent = "/Bitcoin SV:1.0.16/"

	return v
}

func headersMsg(hdrs ...*wire.BlockHeader) *wire.MsgHeaders {
	m := wire.NewMsgHeaders()
	for _, h := range hdrs {
		_ = m.AddBlockHeader(h)
	}

	return m
}

// unconnected is a copy of the chain's first header whose parent nobody has.
func unconnected(chain *svp2ptest.FixtureChain, salt byte) *wire.BlockHeader {
	h := *chain.Headers[0]
	h.PrevBlock = chainhash.Hash{0xEE, salt}

	return &h
}

// rawHeaders encodes count copies of one header under "headers", above what
// go-wire's typed message will build.
func rawHeaders(h *wire.BlockHeader, count int) *svp2ptest.Raw {
	var buf bytes.Buffer

	_ = wire.WriteVarInt(&buf, wire.ProtocolVersion, uint64(count)) //nolint:gosec // small

	var one bytes.Buffer
	_ = h.Serialize(&one)
	one.WriteByte(0) // tx count, as headers carry

	for i := 0; i < count; i++ {
		buf.Write(one.Bytes())
	}

	return &svp2ptest.Raw{Cmd: "headers", Payload: buf.Bytes()}
}

// rawAddr encodes count identical addresses under "addr".
func rawAddr(count int) *svp2ptest.Raw {
	var buf bytes.Buffer

	_ = wire.WriteVarInt(&buf, wire.ProtocolVersion, uint64(count)) //nolint:gosec // small

	// One addr entry: time(4) services(8) ip(16) port(2, big-endian) = 30 bytes.
	entry := make([]byte, 30)
	binary.LittleEndian.PutUint32(entry[0:4], uint32(time.Now().Unix())) //nolint:gosec // seconds fit
	binary.LittleEndian.PutUint64(entry[4:12], uint64(wire.SFNodeNetwork))
	copy(entry[12:28], net.ParseIP("10.1.2.3").To16())
	binary.BigEndian.PutUint16(entry[28:30], 8333)

	for i := 0; i < count; i++ {
		buf.Write(entry)
	}

	return &svp2ptest.Raw{Cmd: "addr", Payload: buf.Bytes()}
}

func scoreRows() []scoreRow {
	return []scoreRow{
		{name: "multiple-version", svp2pScore: 1,
			accepted: []Divergence{{Field: "Scores", Reason: "legacy ignores a second version silently; svp2p carries Misbehaving(1, \"multiple-version\")"}},
			script: func(*svp2ptest.FixtureChain) svp2ptest.Script {
				return svp2ptest.Script{OnConnect: []wire.Message{versionMsg()}}
			}},
		{name: "missing-version", svp2pScore: 1, legacyDrops: true,
			accepted: svnodeKeeps("Misbehaving(1, \"missing-version\")"),
			script: func(*svp2ptest.FixtureChain) svp2ptest.Script {
				return svp2ptest.Script{BeforeVersion: []wire.Message{wire.NewMsgPing(7)}}
			}},
		{name: "unconnected-headers-x10", svp2pScore: 20, legacyDrops: true,
			accepted: svnodeKeeps("Misbehaving(20, \"too-many-unconnected-headers\"); legacy drops any unrequested headers"),
			script:   func(*svp2ptest.FixtureChain) svp2ptest.Script { return svp2ptest.Script{} },
			afterSync: func(chain *svp2ptest.FixtureChain) []wire.Message {
				msgs := make([]wire.Message, 0, 10)
				for i := 0; i < 10; i++ {
					msgs = append(msgs, headersMsg(unconnected(chain, byte(i))))
				}

				return msgs
			}},
		{name: "non-continuous-headers", svp2pScore: 20, legacyDrops: true,
			accepted: svnodeKeeps("Misbehaving(20, \"disconnected headers\"); legacy drops any unrequested headers"),
			script:   func(*svp2ptest.FixtureChain) svp2ptest.Script { return svp2ptest.Script{} },
			afterSync: func(chain *svp2ptest.FixtureChain) []wire.Message {
				return []wire.Message{headersMsg(chain.Headers[0], chain.Headers[2])}
			}},
		// A block whose BODY is invalid — a second coinbase under the requested
		// header. The bridge's createTxMap judges it as a block failure
		// (CheckBlock bad-cb-multiple) before any transaction reaches the
		// validator, so the peer is blamed: svp2p DoS(100) and disconnect, as
		// SVNode; legacy disconnects on the block error without a logged score.
		// (Closed 2026-08-26; was ledger carried residual 15.)
		{name: "invalid-block", svp2pScore: 100, dropped: true, legacyDrops: true,
			accepted: []Divergence{{Field: "Scores", Reason: "legacy disconnects on the block error without a logged score; svp2p carries SVNode's DoS(100)"}},
			script: func(chain *svp2ptest.FixtureChain) svp2ptest.Script {
				return svp2ptest.Script{OnGetData: func(p *svp2ptest.ScriptedPeer, m *wire.MsgGetData) []wire.Message {
					var out []wire.Message

					for _, inv := range m.InvList {
						if inv == nil || inv.Type != wire.InvTypeBlock {
							continue
						}

						if block, ok := p.Chain.Blocks[inv.Hash]; ok {
							// Same header, so the same hash we were asked for; a second
							// copy of the coinbase makes the body fail merkle validation.
							bad := wire.NewMsgBlock(&block.Header)
							_ = bad.AddTransaction(block.Transactions[0])
							_ = bad.AddTransaction(block.Transactions[0])
							out = append(out, bad)
						}
					}

					return out
				}}
			}},
		{name: "too-many-headers", decoder: true, dropped: true,
			accepted: []Divergence{{Field: "Disconnected", Reason: "go-wire refuses the payload; svp2p's transport fails the connection, legacy's read loop swallows it; SVNode would score 20 and keep the peer"}},
			script: func(chain *svp2ptest.FixtureChain) svp2ptest.Script {
				return svp2ptest.Script{OnConnect: []wire.Message{rawHeaders(chain.Headers[0], wire.MaxBlockHeadersPerMsg+1)}}
			}},
		{name: "oversized-addr", decoder: true, dropped: true,
			accepted: []Divergence{{Field: "Disconnected", Reason: "go-wire refuses the payload; svp2p's transport fails the connection, legacy's read loop swallows it; SVNode would score 20 and keep the peer"}},
			script: func(*svp2ptest.FixtureChain) svp2ptest.Script {
				return svp2ptest.Script{OnConnect: []wire.Message{rawAddr(wire.MaxAddrPerMsg + 1)}}
			}},
	}
}

// TestParity_MisbehaviourScores — watch-list scenario 4. Each row runs both
// implementations against one peer that commits exactly one offence.
func TestParity_MisbehaviourScores(t *testing.T) {
	for _, row := range scoreRows() {
		row := row

		t.Run(row.name, func(t *testing.T) {
			obs, _ := RunParity(t, Scenario{
				Name:  "scores/" + row.name,
				Chain: 5,
				Peers: func(t *testing.T, chain *svp2ptest.FixtureChain, net wire.BitcoinNet) []*svp2ptest.ScriptedPeer {
					return []*svp2ptest.ScriptedPeer{svp2ptest.NewScriptedPeer(t, chain, net, row.script(chain), true)}
				},
				Drive: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) {
					sampler := n.sampleScores()

					if row.afterSync != nil {
						n.WaitForHeight(t, 5, 30*time.Second)

						for _, m := range row.afterSync(peers[0].Chain) {
							peers[0].Send(m)
						}
					}

					// Enough for the handshake, the offence and any reaction; a
					// dropped peer ends the wait early.
					n.WaitFor(t, func() bool { return peers[0].Transcript.ClosedBy() == "node" }, 6*time.Second, "")

					n.scores = sampler.Result()
				},
				Observe: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) Observation {
					o := ObserveDefault(t, n, peers)
					o.Scores = map[string]int{}
					o.Notes = map[string]string{"disconnect": disconnectReason(n), "misbehaving": firstMatching(n, "Misbehaving", "misbehavior", "ingest failed", "invalid")}

					for addr, v := range n.scores {
						if addr == peers[0].Addr {
							o.Scores["peer0"] = v
						}
					}

					return o
				},
				Accepted: append([]Divergence{
					{Field: "Requests", Reason: "how far each side got before the offence landed is timing"},
					{Field: "Served", Reason: "as Requests"},
					{Field: "BlocksAccepted", Reason: "as Requests"},
				}, row.accepted...),
			})

			s := obs[Svp2p]

			if row.decoder {
				require.Equal(t, "node", s.Disconnected["peer0"], "go-wire refuses to decode this message; svp2p fails the connection")
				require.Empty(t, obs[Legacy].Disconnected, "legacy's read loop swallows the decode error and keeps the peer")

				return
			}

			if row.legacyDrops {
				require.Equal(t, "node", obs[Legacy].Disconnected["peer0"], "legacy drops the peer for %s", row.name)
			} else {
				require.Empty(t, obs[Legacy].Disconnected, "legacy keeps the peer for %s", row.name)
			}

			require.Equal(t, row.svp2pScore, s.Scores["peer0"], "svp2p total for %s", row.name)

			if row.dropped {
				require.Equal(t, "node", s.Disconnected["peer0"])
			} else {
				require.Empty(t, s.Disconnected, "a sub-threshold offence keeps the connection")
			}

			t.Logf("%s: legacy scores=%v disconnected=%v | svp2p scores=%v disconnected=%v",
				row.name, obs[Legacy].Scores, obs[Legacy].Disconnected, s.Scores, s.Disconnected)
		})
	}
}

var _ = settings.Settings{}

func firstMatching(n *nodeUnderTest, needles ...string) string {
	for _, needle := range needles {
		if lines := n.Logger.Matching(needle); len(lines) > 0 {
			return lines[0]
		}
	}

	return ""
}
