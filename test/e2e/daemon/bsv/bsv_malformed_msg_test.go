package bsv

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

// requireStillServing proves the node survived whatever was just thrown at it,
// by making a fresh peer ask for headers and getting them.
//
// Both upstream scripts here assert only a log line, and then rely on the
// framework's teardown to catch a node that fell over. A port has no log sink to
// read, so this is the substance that assertion stands for: the node is still
// accepting connections and still answering from the blockchain store. getheaders
// is the right probe because the node serves it directly, with no Kafka or
// announcement machinery in the way (see TestWirePeerGetHeadersRoundTrip).
//
// It needs a chain longer than genesis to mean anything: the locator below names
// genesis, and a node whose tip IS genesis answers correctly by sending nothing,
// which is indistinguishable from a node that has stopped answering. Both callers
// mine first, as both upstream scripts do.
func requireStillServing(t *testing.T, td *daemon.TestDaemon) {
	t.Helper()

	probe := wirepeer.Connect(t, td)
	defer probe.Close()

	msg := wire.NewMsgGetHeaders()
	msg.ProtocolVersion = wire.ProtocolVersion
	msg.BlockLocatorHashes = append(msg.BlockLocatorHashes, td.Settings.ChainCfgParams.GenesisHash)
	msg.HashStop = chainhash.Hash{}

	probe.Send(t, msg)

	headers := probe.WaitForHeaders(t, 30*time.Second)
	require.NotEmpty(t, headers.Headers, "the node should still be serving headers")
}

// TestBSVEmptyMsgCmd is the Teranode port of bitcoin-sv's bsv-empty-msg-cmd.py.
//
// Upstream completes a handshake, sends a well-framed message whose 12-byte
// command field is all NULs, and waits for the node to log
// `Unknown command "" from peer=0`. The log line stands for a deliberate design
// decision in the reference implementation, stated in the comment above it in
// net/net_processing.cpp: "Ignore unknown commands for extensibility". The node
// does not answer, does not disconnect, and carries on.
//
// Teranode reaches the same outcome here, but for a much narrower reason, and
// that difference is the point of this port. go-wire's makeEmptyMessage has no
// case for an empty command, so decoding fails with a *wire.MessageError, and
// peer.inHandler's first branch is isAllowedReadError - a btcd-inherited test
// affordance that tolerates a malformed message only on RegTestNet, and only
// from 127.0.0.1. Both conditions happen to hold for every wirepeer test, which
// is why the connection survives below. On any other network the same frame is
// answered with a "malformed" reject and a disconnect. Teranode does not ignore
// unknown commands for extensibility; it ignores them in the regression-test
// configuration. See the unknown-command-disconnects-off-regtest gap, which this
// port is what turned up.
//
// The Net assertion below is therefore load-bearing rather than decorative: it
// pins the precondition the tolerance rests on, so that if the harness ever
// stops running on regtest this test says why it broke.
//
// Reproduced from upstream:
//   - the node does not act on the unrecognised command: no reject comes back
//   - the connection survives the frame
//   - the node is still serving afterwards
func TestBSVEmptyMsgCmd(t *testing.T) {
	td := wirepeer.NewLegacyDaemon(t)
	defer td.Stop(t)

	require.Equal(t, wire.RegTestNet, td.Settings.ChainCfgParams.Net,
		"the unknown-command tolerance this port observes is regtest-only; see peer.isAllowedReadError")

	// Upstream's node.generate(1), which is what gives the liveness probe at the
	// end of the test something to answer with.
	td.MineBlocks(t, 1)

	p := wirepeer.Connect(t, td)
	defer p.Close()

	// Upstream's msg_emptycmdmsg: command b'\x00', payload struct.pack("<I", 1).
	// An empty command string lands in the wire header as the same 12 NUL bytes.
	p.SendRawFrame(t, "", []byte{1, 0, 0, 0})

	p.AssertStillConnected(t, 3*time.Second, "an unrecognised command should not cost the peer its connection")
	require.Empty(t, p.Received(wire.CmdReject),
		"the node should ignore the unrecognised command rather than answer it")

	requireStillServing(t, td)
}

// TestBSVEmptyPayload is the Teranode port of bitcoin-sv's bsv-empty-payload.py.
//
// Upstream completes a handshake, sends a `reject` message with an empty
// payload, and waits for the node to log "Unparseable reject message received".
// As with bsv-empty-msg-cmd.py the log stands for the behaviour: bitcoin-sv
// catches the deserialisation failure and swallows it, specifically so that a
// bad reject cannot provoke a reject in return. The comment above the catch says
// so - "Avoid feedback loops by preventing reject messages from triggering a new
// reject message". The connection survives.
//
// Teranode disconnects instead, and the reason is worth stating exactly, because
// it is not a policy choice. The frame is internally consistent: declared length
// 0, zero payload bytes, correct checksum. go-wire reads the header, hands
// MsgReject.Bsvdecode an empty reader, and the first field read returns a bare
// io.EOF - measured, not inferred. peer.inHandler cannot tell that from the
// socket dying: isAllowedReadError wants a *wire.MessageError and this is not
// one, then shouldHandleReadError sees io.EOF and reports "Remote peer has
// disconnected (EOF)". The peer had done nothing of the sort. So the node drops a
// healthy peer, logs it at debug level as the peer's own doing, and sends no
// reject explaining why. See the short-payload-read-as-peer-eof gap.
//
// This test therefore asserts the disconnect - the behaviour Teranode actually
// exhibits - as a tripwire rather than an endorsement. If it starts failing,
// Teranode has learned to tell a truncated payload from a dead socket, and this
// port should assert upstream's assertion instead.
//
// Reproduced from upstream:
//   - the node is still serving afterwards
func TestBSVEmptyPayload(t *testing.T) {
	td := wirepeer.NewLegacyDaemon(t)
	defer td.Stop(t)

	// Upstream's node.generate(1). See requireStillServing for why it matters.
	td.MineBlocks(t, 1)

	p := wirepeer.Connect(t, td)
	defer p.Close()

	// Upstream's msg_emptypayload: command b"reject", serialize() returns b"".
	p.SendRawFrame(t, wire.CmdReject, nil)

	t.Run("truncated_payload_tripwire", func(t *testing.T) {
		p.AssertDisconnected(t, 10*time.Second,
			"Teranode currently reads a reject message too short to decode as a bare io.EOF and "+
				"concludes the remote peer hung up. If this now fails, Teranode has gained the "+
				"ability to tell a truncated payload from a closed socket, and this port should "+
				"assert upstream's assertion - that the connection survives - instead")

		require.Empty(t, p.Received(wire.CmdReject),
			"upstream's reason for swallowing this is to avoid reject feedback loops; "+
				"Teranode should likewise not answer a reject with a reject")
	})

	requireStillServing(t, td)
}
