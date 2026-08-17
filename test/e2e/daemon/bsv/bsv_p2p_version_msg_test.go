package bsv

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

// TestBSVP2PVersionMsg ports bsv-p2p-version_msg.py, whose docstring is "Test P2P
// version message error handling".
//
// Upstream opens two connections. One handshakes normally and waits for the
// node's protoconf, which is both a liveness check and the synchronisation point
// for what follows. The other suppresses its automatic version and instead sends
// one by hand with eight raw bytes appended where a serialised association ID
// should be - struct.pack("<Q", 0x00000000111111FE). bitcoin-sv answers that with
// a reject, closes the connection, and logs "Failed to process version: (Badly
// formatted association ID".
//
// Teranode reaches the same end state by a different route, and the difference is
// the substance of this port. The two nodes fail at different layers on identical
// input:
//
//   - bitcoin-sv decodes the trailing field as a string, then finds it is not a
//     well-formed association ID. That is a semantic check, and it has somewhere
//     to report from, so it rejects.
//   - Teranode never gets that far. go-wire reads the trailing bytes as VarBytes
//     (msg_version.go Bsvdecode), so the leading 0xFE is a varint prefix
//     announcing a 4-byte length, and the 0x00111111 that follows declares
//     1,118,481 bytes against a MaxAssociationIDLen of 129. Measured error:
//     "ReadVarBytes: AssociationID is larger than the max allowed size
//     [count 1118481, max 129]". The message never becomes a *wire.MsgVersion, so
//     no association handling runs at all - and there would be no equivalent check
//     to reach if it did, because peer.NewAssociation takes the ID bytes as given
//     and validates nothing.
//
// The consequence is what the port asserts: negotiateInboundProtocol
// (services/legacy/peer/peer.go:2675) returns the decode error without writing
// anything back, so the peer gets a silent hangup where upstream gets a reject.
// The same function does send a reject when the first message is merely the wrong
// type - "a version message must precede all others" - so the machinery is right
// there and unused on this path. See the no-reject-for-undecodable-version gap.
func TestBSVP2PVersionMsg(t *testing.T) {
	td := wirepeer.NewLegacyDaemon(t)
	defer td.Stop(t)

	// Upstream is setup_clean_chain, but the liveness probe at the end needs a
	// chain longer than genesis to distinguish "answered with nothing" from
	// "stopped answering" - see requireStillServing.
	td.MineBlocks(t, 1)

	// Upstream's badConn: created before anything is sent, and deliberately silent.
	// A RawConn rather than a Peer because the malformed bytes *are* the version,
	// so there is no negotiated connection to send them over.
	bad := wirepeer.DialRaw(t, td)
	defer bad.Close()

	// Upstream's dummyConn, which handshakes normally. Its protoconf is upstream's
	// dummyCB.wait_for_protoconf() and serves the same two purposes here: it shows
	// the node's handshake path working, and it is the synchronisation point that
	// makes the silence assertion below meaningful rather than merely quick.
	dummy := wirepeer.Connect(t, td)
	defer dummy.Close()

	require.Eventually(t, func() bool { return dummy.Count(wire.CmdProtoconf) > 0 },
		10*time.Second, 100*time.Millisecond,
		"the node should send protoconf to a normally-handshaked peer, got: %s", dummy.Summary())

	// Upstream: assert_equal(len(badConnCB.message_count), 0). The node does not
	// speak first - a connection that has sent no version is sent nothing. The
	// dummy handshake above has already proved the node is awake and serving this
	// listener, so silence here is a decision rather than a race.
	require.Empty(t, bad.ReadSome(t, 4096, time.Second),
		"the node should not send anything to a connection that has not sent a version")

	payload := badVersionPayload(t)

	// Control, with no upstream counterpart and load-bearing without one. Upstream's
	// dummy connection proves the node's handshake works, but it goes through
	// wirepeer.Connect - it says nothing about whether the frame built below is
	// well-formed. Without this, a port that got the magic, the checksum or the
	// field order wrong would watch the node hang up and record a silent disconnect
	// as a finding. Sending the same bytes minus the eight trailing ones isolates
	// them as the cause: measured, the node answers this with its own version.
	control := wirepeer.DialRaw(t, td)
	defer control.Close()

	control.WriteFrame(t, wire.CmdVersion, payload[:len(payload)-8])

	require.NotEmpty(t, control.ReadSome(t, 4096, 10*time.Second),
		"the same version message without the trailing bytes should be answered; if it is not, this "+
			"port is measuring its own frame construction rather than the node")

	// Upstream's msg_version_bad.serialize(): every standard version field,
	// followed by struct.pack("<Q", 0x00000000111111FE) in place of a properly
	// serialised association ID.
	bad.WriteFrame(t, wire.CmdVersion, payload)

	// Upstream: wait_until(badConnCB.last_reject is not None). Teranode sends
	// nothing at all. Read with a real timeout first, so that a reject would be
	// caught here rather than swallowed by the closure check below.
	require.Empty(t, bad.ReadSome(t, 4096, 3*time.Second),
		"TRIPWIRE: the node now answers an undecodable version message. bitcoin-sv sends a reject "+
			"here; if Teranode has learned to as well, revisit the no-reject-for-undecodable-version "+
			"gap and assert upstream's reject instead of this silence")

	// Upstream: wait_until(badConn.state == "closed"). This half does port.
	bad.ExpectClosed(t, 10*time.Second)

	// Upstream's framework fails the test if the node died; these two say so
	// directly. The dummy connection is the sharper of the pair: a node that tore
	// down the wrong peer, or all of them, would still pass a fresh-connection
	// probe.
	dummy.AssertStillConnected(t, time.Second,
		"an unrelated peer should not lose its connection because another sent a bad version")

	requireStillServing(t, td)
}

// badVersionPayload builds upstream's msg_version_bad: a well-formed version
// message with eight raw bytes appended where a serialised association ID should
// be.
//
// It is assembled by encoding a valid message and appending, rather than by
// hand-rolling every field, so that the port stays correct if go-wire's version
// encoding changes - the eight trailing bytes are the whole point and everything
// before them is incidental. BsvEncode omits the association ID when it is empty,
// which is what leaves the trailing bytes sitting exactly where upstream puts
// them.
func badVersionPayload(t *testing.T) []byte {
	t.Helper()

	// Addresses and nonce are arbitrary; the node's self-connection check keys on
	// nonces it has itself sent, and this one is generated here.
	me := wire.NewNetAddress(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}, wire.SFNodeNetwork)
	you := wire.NewNetAddress(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 2}, wire.SFNodeNetwork)

	msg := wire.NewMsgVersion(me, you, 0x1234567890, 0)

	// A BSV user agent, or the node would ban this peer for the agent rather than
	// for the malformed field, and the port would be measuring the wrong rule.
	// See the bsv-ban-useragents.py port.
	msg.UserAgent = ""
	require.NoError(t, msg.AddUserAgent("Bitcoin SV", "1.2.2", "teranode-wirepeer"),
		"set the version message user agent")

	var buf bytes.Buffer
	require.NoError(t, msg.BsvEncode(&buf, wire.ProtocolVersion, wire.BaseEncoding),
		"encode the valid part of the version message")

	// struct.pack("<Q", 0x00000000111111FE), little-endian.
	return append(buf.Bytes(), 0xFE, 0x11, 0x11, 0x11, 0x00, 0x00, 0x00, 0x00)
}
