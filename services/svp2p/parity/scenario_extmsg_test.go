package parity

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/svp2p/svp2ptest"
	"github.com/bsv-blockchain/teranode/services/svp2p/transport"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// extendedBlockLength is one byte past the basic message envelope: a uint32
// length field tops out at transport.MaxBlockFrameBytes + 1, so this is the
// smallest block that cannot be declared without the extended header.
const extendedBlockLength = uint64(4<<30) + 1

// extendedBlockFetchTimeout bounds the 4 GiB transfer. Every other scenario in
// this package moves kilobytes and lives inside a 30 second wait; this one
// moves four gibibytes over loopback twice (the node reads the body from the
// asset stub and writes it to the peer), so it gets its own, larger bound.
const extendedBlockFetchTimeout = 5 * time.Minute

// extendedBlockSettleTimeout is how long a leg is given to answer AFTER the
// asset stub has finished writing the body. A leg that answers with silence is
// only distinguishable from one still working once the fetch it is working on
// has completed.
const extendedBlockSettleTimeout = 2 * time.Minute

// fatBlockLegacyEnv opts the legacy leg into this scenario. It is OFF by
// default, and the reason is measured, not cautious: legacy materialises the
// whole payload twice over — NewRawBlockMessage reads the asset body into one
// []byte (services/legacy/raw_block_message.go:28, io.ReadAll) and go-wire's
// WriteMessageWithEncodingN encodes that into a bytes.Buffer
// (message.go:351-353).
//
// Measured on 2026-08-28, both legs, WITHOUT the race detector: 94.8 GB
// maximum resident set size. With -race the first attempt never finished — the
// test binary was killed by the OS at 62.8 GB maximum resident set size, 762 s
// in, still inside the legacy leg.
//
// The row is a verdict on svp2p, so the default leg set is svp2p alone and the
// package stays inside `make test`. Legacy's own answer is recorded from a
// separate run of exactly this scenario with the variable set and WITHOUT the
// race detector; see the scenario 9 verdict in parity-watchlist.md.
const fatBlockLegacyEnv = "PARITY_FAT_BLOCK_LEGACY"

// extendedBlockLegs is svp2p alone unless fatBlockLegacyEnv is set, in which
// case both legs run and RunParity compares them.
func extendedBlockLegs() []Impl {
	if os.Getenv(fatBlockLegacyEnv) == "1" {
		return nil
	}

	return []Impl{Svp2p}
}

// TestParity_ExtendedBlockServing — watch-list scenario 9, the one scenario the
// 2026-08-26 pass could not adjudicate in process ("needs a fat-block rig").
//
// A peer that negotiated version 70016 asks for a block of 4 GiB + 1 bytes.
// svp2p must frame it with SVNode's extended header (protocol.cpp:220-237,
// transport.extBlockFrameHeader) and stream every declared byte; before Phase 4
// it answered notfound (getdata.go OPEN QUESTION 5). The legacy leg's answer is
// recorded, not judged — the row is a verdict on svp2p.
//
// The fat block is a fixture, not a mined block: the node's stored record for
// its own tip is rewritten to declare 4 GiB + 1 bytes, and the asset stub
// answers that hash with a body of exactly that length (the real 80 byte header
// plus filler). Those two numbers are the only inputs the serving path reads —
// bridge.FetchBlock takes the length from BlockHeaderMeta.SizeInBytes and the
// body from block_legacy?wire=1 — so the path under test is the real one end to
// end. The peer counts the payload without decoding it (svp2ptest.ReadFrameHeader).
func TestParity_ExtendedBlockServing(t *testing.T) {
	stub := startAssetStub(t)

	obs, _ := RunParity(t, Scenario{
		Name:  "extended-block-serving",
		Chain: 3,
		Only:  extendedBlockLegs(),
		Tweaks: []func(Impl, *svp2ptest.FixtureChain, *settings.Settings){
			func(_ Impl, chain *svp2ptest.FixtureChain, s *settings.Settings) {
				s.Asset.HTTPAddress = stub.URL()

				// Not what gates this path — both services hand go-wire a
				// fixed 4e9 at startup (svp2p Server.go:235, legacy
				// Server.go:255) and neither reads this setting on the serve
				// side — but raised so no leg can refuse the block on a
				// policy size limit, which would make the row a verdict on
				// the wrong rule.
				s.Policy.ExcessiveBlockSize = 5 << 30

				tip := chain.Tip()
				stub.Register(tip, headerBytes(t, chain.Headers[len(chain.Headers)-1]), extendedBlockLength)
			},
		},
		Peers: honestPeers(1),
		Drive: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) {
			n.WaitForHeight(t, 3, 60*time.Second)

			peer := peers[0]
			tip := peer.Chain.Tip()

			declareBlockSize(t, n, tip, extendedBlockLength)

			conns := peer.Conns()
			require.NotEmpty(t, conns, "the node never connected to the peer")

			getData := wire.NewMsgGetData()
			require.NoError(t, getData.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &tip)))
			require.NoError(t, peer.Write(conns[0], getData))

			answered := func() bool {
				return len(peer.ExtendedFrames()) > 0 || peer.Transcript.Count(svp2ptest.In, wire.CmdNotFound) > 0
			}

			// The body leaving the asset stub is the one event a leg that
			// serves the block produces: svp2p streams it on to the peer,
			// legacy buffers it. A leg that refuses the block never opens the
			// body at all, so an answer already on the wire ends this wait
			// instead.
			n.WaitFor(t, func() bool { return answered() || stub.Completed(tip) > 0 }, extendedBlockFetchTimeout, "")

			// Silence is only distinguishable from "still working" once the
			// fetch has finished, which is why this wait follows the one above.
			n.WaitFor(t, answered, extendedBlockSettleTimeout, "")

			var declared, received uint64

			frames := peer.ExtendedFrames()
			for _, f := range frames {
				if f.Command == wire.CmdBlock {
					declared, received = f.Declared, f.Received
				}
			}

			n.notes = map[string]string{
				"asset-body-fetches": fmt.Sprint(stub.Completed(tip)),
				"extended-frames":    fmt.Sprint(len(frames)),
				"declared-length":    fmt.Sprint(declared),
				"received-bytes":     fmt.Sprint(received),
				"block-messages-in":  fmt.Sprint(peer.Transcript.Count(svp2ptest.In, wire.CmdBlock)),
				"notfound-in":        fmt.Sprint(peer.Transcript.Count(svp2ptest.In, wire.CmdNotFound)),
				"still-connected":    fmt.Sprint(n.ConnectedCount(t)),
			}
		},
		Observe: func(t *testing.T, n *nodeUnderTest, peers []*svp2ptest.ScriptedPeer) Observation {
			o := ObserveDefault(t, n, peers)
			o.Notes = n.notes

			return o
		},
		// Only reached when the legacy leg is opted in; with one leg RunParity
		// makes no comparison at all. The disconnect IS the finding, not noise:
		// legacy read the whole 4 GiB body, sent the peer nothing, and then
		// dropped it, while svp2p served the block and kept the connection.
		Accepted: []Divergence{{
			Field:  "Disconnected",
			Reason: "legacy drops the peer after a getdata it cannot frame; svp2p serves the block and keeps the connection (scenario 9)",
		}},
	})

	svp := obs[Svp2p].Notes

	require.Equal(t, "1", svp["extended-frames"],
		"svp2p must frame a block above transport.MaxBlockFrameBytes with the extended header for a peer at %d", transport.ExtendedPayloadVersion)
	require.Equal(t, fmt.Sprint(extendedBlockLength), svp["declared-length"],
		"the extension header must declare the block's full length")
	require.Equal(t, fmt.Sprint(extendedBlockLength), svp["received-bytes"],
		"the peer must receive every declared payload byte, or the connection is left mid-message")
	require.Equal(t, "1", svp["block-messages-in"], "the frame must be a block")
	require.Equal(t, "0", svp["notfound-in"], "the pre-Phase-4 notfound answer must be gone")
	require.Equal(t, "1", svp["still-connected"], "the connection must survive the transfer")

	t.Logf("extended block serving: svp2p declared %s bytes, delivered %s, answered %s notfound, %s connection(s) left",
		svp["declared-length"], svp["received-bytes"], svp["notfound-in"], svp["still-connected"])

	if legacy := obs[Legacy].Notes; legacy != nil {
		t.Logf("legacy leg: %s block(s), %s notfound, %s completed asset fetch(es), %s connection(s) left",
			legacy["block-messages-in"], legacy["notfound-in"], legacy["asset-body-fetches"], legacy["still-connected"])
	}
}

// headerBytes serializes a fixture header, which is what the first 80 bytes of
// a block_legacy?wire=1 body are.
func headerBytes(t *testing.T, header *wire.BlockHeader) []byte {
	t.Helper()

	var buf bytes.Buffer

	require.NoError(t, header.Serialize(&buf))
	require.Equal(t, 80, buf.Len())

	return buf.Bytes()
}

// declareBlockSize rewrites one stored block's size_in_bytes, which is the
// number bridge.FetchBlock reads (BlockHeaderMeta.SizeInBytes) and writes into
// the wire frame it builds. Mining a real 4 GiB block is not possible in a
// test; declaring one is, and the declared length is the only property of the
// block the serving path uses.
//
// SetBlockProcessedAt is called purely for its side effect: it ends with the
// SQL store's own ResetResponseCache (stores/blockchain/sql/SetBlockProcessedAt.go:16),
// and without it the two minute response cache would keep serving the old size.
// The read-back proves the new number is what the serving path will see.
func declareBlockSize(t *testing.T, n *nodeUnderTest, hash chainhash.Hash, size uint64) {
	t.Helper()

	ctx := context.Background()

	res, err := n.blockchainStore.GetDB().ExecContext(ctx, "UPDATE blocks SET size_in_bytes = $1 WHERE hash = $2", size, hash[:])
	require.NoError(t, err)

	rows, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows, "the node must hold the block whose size is being declared")

	require.NoError(t, n.blockchainStore.SetBlockProcessedAt(ctx, &hash))

	_, meta, err := n.blockchainStore.GetBlockHeader(ctx, &hash)
	require.NoError(t, err)
	require.Equal(t, size, meta.SizeInBytes, "the serving path must read the declared size, not a cached one")
}
