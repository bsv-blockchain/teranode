package bsv

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

// TestBSVProtoconfVersionsCompatibility ports
// bsv-protoconf-versions-compatibility.py, which checks how a node reacts to
// protoconf messages that a future or a broken peer might send: too few fields,
// an implausibly small declared limit, an unknown extra field, and payloads at and
// over the legacy message-size ceiling.
//
// One of the five ports as a match, one as a genuine reproduction, and three as
// tripwires, because Teranode validates nothing about a peer's protoconf.
// serverPeer.OnProtoconf (services/legacy/peer_server.go:695) stores what arrives
// and acts on it only insofar as it looks for a stream policy; no value is
// range-checked and no shape is rejected. See the protoconf-not-validated gap.
//
// Worth stating because it was worth checking: go-wire's protoconf encoding is NOT
// wrong. The bespoke helper classes in the upstream script serialise
// numberOfFields as a fixed 4-byte int, which made go-wire's varint look
// incompatible - but that is the point of those helpers, which exist to send
// malformed messages. bitcoin-sv's real format is READWRITECOMPACTSIZE
// (src/protocol.h:615), a compact size, which is what go-wire reads. The two
// agree.
//
// Upstream opens a fresh connection per case, via its run_connection context
// manager, and so does this - each subtest gets its own peer and closes it, which
// also keeps the number of concurrent peers well under MaxPeersPerIP.
func TestBSVProtoconfVersionsCompatibility(t *testing.T) {
	td := wirepeer.NewLegacyDaemonWithP2P(t)
	defer td.Stop(t)

	// Upstream's self.nodes[0].generate(1), so the liveness probes have something
	// to answer with.
	td.MineBlocks(t, 1)

	// Upstream test 1: msg_protoconf(CProtoconfWithZeroFields()). bitcoin-sv throws
	// at deserialisation - "Number of fields specified in protoconf is equal to 0"
	// (src/protocol.h:622) - and disconnects without banning.
	t.Run("a protoconf declaring zero fields is tolerated", func(t *testing.T) {
		p := sendProtoconfPayload(t, td, zeroFieldsProtoconf())
		defer p.Close()

		p.AssertStillConnected(t, 2*time.Second,
			"TRIPWIRE: the node now rejects a protoconf declaring zero fields, as bitcoin-sv does. "+
				"Revisit the protoconf-not-validated gap and assert upstream's disconnect instead")

		requireNotPunished(t, td, p)
	})

	// Upstream test 2: msg_protoconf(CProtoconf(1, 1)). bitcoin-sv disconnects
	// because the smallest permitted maxRecvPayloadLength is 1 MiB.
	t.Run("a protoconf declaring a one-byte receive limit is tolerated", func(t *testing.T) {
		p := sendProtoconfPayload(t, td, tinyLimitProtoconf())
		defer p.Close()

		p.AssertStillConnected(t, 2*time.Second,
			"TRIPWIRE: the node now rejects a protoconf declaring a receive limit below 1 MiB, as "+
				"bitcoin-sv does. Revisit the protoconf-not-validated gap")

		requireNotPunished(t, td, p)
	})

	// Upstream test 3: msg_protoconf(CProtoconfWithNewField(2, ..., 5)) - a valid
	// two-field protoconf with an unknown field appended, standing in for a future
	// protocol version. This is the one thing in the script Teranode gets right, so
	// it is a reproduction rather than a tripwire.
	//
	// Upstream proves the extra field did not confuse the parse by checking that
	// the node then splits its getdata at the declared limit. That half is not
	// reproducible and is waived: the declared value is never read, so correct
	// parsing has no observable consequence (see the
	// protoconf-payload-limit-not-honoured gap). What is observable, and is what
	// forward compatibility actually means, is that the unknown field costs the
	// peer nothing and the node keeps working.
	t.Run("an unknown trailing field does not break the node", func(t *testing.T) {
		p := sendProtoconfPayload(t, td, protoconfWithUnknownField())
		defer p.Close()

		p.AssertStillConnected(t, 2*time.Second,
			"a protoconf carrying a field this version does not know must be tolerated, or no future "+
				"protocol extension could ever be deployed")

		requireNotPunished(t, td, p)
		requireStillServing(t, td)
	})

	// Upstream test 4: a protoconf whose payload is exactly
	// LEGACY_MAX_PROTOCOL_PAYLOAD_LENGTH of filler. bitcoin-sv accepts it and does
	// not ban, and so does Teranode - the only case in this script where the two
	// agree outright.
	//
	// The filler decodes to nonsense rather than failing: 'a' is 0x61, so
	// numberOfFields reads as 97 and maxRecvPayloadLength as 0x61616161, about
	// 1.5 GB. Teranode stores both without comment. That is harmless only for as
	// long as nothing reads them - see protoconf-not-validated.
	t.Run("a protoconf of exactly the legacy maximum is accepted", func(t *testing.T) {
		p := sendProtoconfPayload(t, td, filledProtoconf(legacyMaxProtocolPayloadLength))
		defer p.Close()

		p.AssertStillConnected(t, 2*time.Second,
			"a protoconf at exactly the legacy maximum payload length must be accepted")

		requireNotPunished(t, td, p)
	})

	// Upstream test 5: one byte over. bitcoin-sv disconnects AND bans - the only
	// ban in the script. Teranode does neither.
	//
	// The boundary is the same on both sides by coincidence worth noting: go-wire's
	// MaxProtoconfPayload is 1 MiB, exactly upstream's
	// LEGACY_MAX_PROTOCOL_PAYLOAD_LENGTH, so this frame is over-long for both
	// implementations. Only the response differs.
	t.Run("a protoconf over the legacy maximum is tolerated and leaves the stream usable", func(t *testing.T) {
		p := sendProtoconfPayload(t, td, filledProtoconf(legacyMaxProtocolPayloadLength+1))
		defer p.Close()

		p.AssertStillConnected(t, 2*time.Second,
			"TRIPWIRE: the node now disconnects on a protoconf over the legacy maximum, as bitcoin-sv "+
				"does. Revisit the protoconf-not-validated gap - note bitcoin-sv also bans, which is a "+
				"separate assertion")

		requireNotPunished(t, td, p)

		// Checked deliberately rather than assumed: refusing an over-long frame
		// without consuming its payload would leave 1 MiB of filler in the socket to
		// be read as the next message header, desynchronising the connection for
		// good. Measured, it does not - this peer is still served afterwards.
		requireServedOn(t, td, p)
	})
}

// sendProtoconfPayload opens a peer, waits for the node's own protoconf so the
// handshake is demonstrably complete, then sends payload as a raw protoconf frame.
//
// Raw framing throughout: every payload here is one go-wire would refuse to
// encode, which is the point.
func sendProtoconfPayload(t *testing.T, td *daemon.TestDaemon, payload []byte) *wirepeer.Peer {
	t.Helper()

	p := wirepeer.Connect(t, td)

	require.Eventually(t, func() bool { return p.Count(wire.CmdProtoconf) > 0 },
		10*time.Second, 100*time.Millisecond,
		"the node should send its protoconf before the test sends one, received: %s", p.Summary())

	p.SendRawFrame(t, wire.CmdProtoconf, payload)

	return p
}

// requireNotPunished asserts the peer drew neither a reject nor a ban. Upstream
// checks listbanned in all five cases; the reject check has no upstream
// counterpart and is here because a reject would be the other way Teranode could
// signal disapproval.
func requireNotPunished(t *testing.T, td *daemon.TestDaemon, p *wirepeer.Peer) {
	t.Helper()

	require.Empty(t, p.Received(wire.CmdReject), "the protoconf drew a reject")
	require.Empty(t, wirepeer.TryListBanned(td), "the protoconf got the peer banned")
}

// requireServedOn proves this particular connection still works, by asking for
// headers on it. requireStillServing makes the weaker claim, using a fresh peer -
// which cannot distinguish a node that is fine from a node that has quietly
// broken the connection under test.
func requireServedOn(t *testing.T, td *daemon.TestDaemon, p *wirepeer.Peer) {
	t.Helper()

	before := p.Count(wire.CmdHeaders)

	msg := wire.NewMsgGetHeaders()
	msg.ProtocolVersion = wire.ProtocolVersion
	msg.BlockLocatorHashes = append(msg.BlockLocatorHashes, td.Settings.ChainCfgParams.GenesisHash)

	p.Send(t, msg)

	require.Eventually(t, func() bool { return p.Count(wire.CmdHeaders) > before },
		15*time.Second, 100*time.Millisecond,
		"the connection should still be usable after the malformed protoconf, received: %s", p.Summary())
}

// zeroFieldsProtoconf is upstream's CProtoconfWithZeroFields: struct.pack("<i", 0).
// Read as a compact size that gives numberOfFields = 0, with three bytes of
// trailing slack go-wire ignores.
func zeroFieldsProtoconf() []byte {
	return []byte{0x00, 0x00, 0x00, 0x00}
}

// tinyLimitProtoconf is upstream's CProtoconf(1, 1): one field, and a declared
// maximum receive payload length of a single byte.
func tinyLimitProtoconf() []byte {
	var buf bytes.Buffer

	buf.WriteByte(0x01) // compact_size(numberOfFields = 1)
	_ = binary.Write(&buf, binary.LittleEndian, uint32(1))

	return buf.Bytes()
}

// protoconfWithUnknownField is upstream's CProtoconfWithNewField(2, 1 MiB + 36, 5):
// a well-formed two-field protoconf with an extra 4-byte field appended, as a
// later protocol version might add.
//
// The declared limit is upstream's MESSAGE_LENGTH_1MiB_PLUS_1_ELEMENT, chosen there
// so that a node taking the value seriously could be distinguished from one falling
// back to the 1 MiB default. Kept identical even though Teranode reads neither, so
// the two files still line up.
func protoconfWithUnknownField() []byte {
	var buf bytes.Buffer

	buf.WriteByte(0x02) // compact_size(numberOfFields = 2)
	_ = binary.Write(&buf, binary.LittleEndian, uint32(legacyMaxProtocolPayloadLength+4+32))

	policies := []byte(wire.DefaultStreamPolicy)
	buf.WriteByte(byte(len(policies)))
	buf.Write(policies)

	_ = binary.Write(&buf, binary.LittleEndian, uint32(5)) // the field this version does not know

	return buf.Bytes()
}

// filledProtoconf is upstream's msg_protoconf_largest(size): size bytes of 'a',
// sent as the whole payload.
func filledProtoconf(size int) []byte {
	return bytes.Repeat([]byte{'a'}, size)
}
