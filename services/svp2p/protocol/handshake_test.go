package protocol

import (
	"testing"

	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

func remoteVersion(nonce uint64) *wire.MsgVersion {
	msg := wire.NewMsgVersion(
		wire.NewNetAddressIPPort(nil, 8333, 0),
		wire.NewNetAddressIPPort(nil, 8333, 0),
		nonce, 850000,
	)
	msg.UserAgent = "/sv:1.1.0/"

	return msg
}

func outboundConfig() HandshakeConfig {
	return HandshakeConfig{
		Inbound: false, Nonce: 7777, UserAgent: "/teranode-svp2p:0.1.0/",
		StartingHeight: 900000, MaxRecvPayloadLength: wire.DefaultMaxRecvPayloadLength,
		AllowBlockPriority: true,
		LocalAddr:          wire.NewNetAddressIPPort(nil, 8333, 0),
		RemoteAddr:         wire.NewNetAddressIPPort(nil, 8333, 0),
	}
}

func TestOutboundHandshakeHappyPath(t *testing.T) {
	h := NewHandshake(outboundConfig())

	initial := h.Initial()
	require.Len(t, initial, 1)
	require.IsType(t, &wire.MsgVersion{}, initial[0])

	// Their version arrives: we owe verack + protoconf
	// (net_processing.cpp ProcessVersionMessage: PushMessage(VERACK) then
	// PushProtoconf immediately after).
	reply, err := h.OnMessage(remoteVersion(1234))
	require.NoError(t, err)
	require.Len(t, reply, 2)
	require.IsType(t, &wire.MsgVerAck{}, reply[0])
	require.IsType(t, &wire.MsgProtoconf{}, reply[1])
	require.False(t, h.Established())

	// Their verack arrives: established; we send sendheaders
	// (net_processing.cpp ProcessVerAckMessage).
	reply, err = h.OnMessage(wire.NewMsgVerAck())
	require.NoError(t, err)
	require.True(t, h.Established())
	require.Len(t, reply, 1)
	require.IsType(t, &wire.MsgSendHeaders{}, reply[0])

	info := h.PeerInfo()
	require.Equal(t, "/sv:1.1.0/", info.UserAgent)
	require.Equal(t, int32(850000), info.StartingHeight)
	require.Equal(t, wire.ProtocolVersion, info.NegotiatedVersion)
}

func TestInboundHandshakeSendsVersionReply(t *testing.T) {
	cfg := outboundConfig()
	cfg.Inbound = true
	h := NewHandshake(cfg)

	require.Empty(t, h.Initial()) // net_processing.cpp: "Be shy and don't send version until we hear"

	reply, err := h.OnMessage(remoteVersion(1234))
	require.NoError(t, err)
	require.Len(t, reply, 3)
	require.IsType(t, &wire.MsgVersion{}, reply[0]) // PushNodeVersion on inbound VERSION
	require.IsType(t, &wire.MsgVerAck{}, reply[1])
	require.IsType(t, &wire.MsgProtoconf{}, reply[2])
}

func TestSelfConnectionDetected(t *testing.T) {
	h := NewHandshake(outboundConfig())
	_ = h.Initial()

	_, err := h.OnMessage(remoteVersion(7777)) // our own nonce echoed back
	require.ErrorIs(t, err, ErrSelfConnection)
}

func TestSelfConnectionViaNonceRegistry(t *testing.T) {
	// net.cpp CConnman::CheckIncomingNonce: a real self-connect carries a
	// different nonce per connection, so the per-connection compare above
	// never fires — this is the registry check that covers it.
	tests := []struct {
		name       string
		inRegistry bool
	}{
		{name: "nonce hits the registry", inRegistry: true},
		{name: "nonce absent from registry", inRegistry: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := outboundConfig()
			cfg.CheckIncomingNonce = func(uint64) bool { return tt.inRegistry }

			h := NewHandshake(cfg)
			_ = h.Initial()

			reply, err := h.OnMessage(remoteVersion(9999))

			if tt.inRegistry {
				require.ErrorIs(t, err, ErrSelfConnection)
				require.Empty(t, reply)
			} else {
				require.NoError(t, err)
				require.NotEmpty(t, reply)
			}
		})
	}
}

func TestObsoleteVersionRejected(t *testing.T) {
	h := NewHandshake(outboundConfig())
	_ = h.Initial()

	old := remoteVersion(1)
	old.ProtocolVersion = 300

	_, err := h.OnMessage(old)
	require.ErrorIs(t, err, ErrObsoleteVersion)
}

func TestDuplicateVersionScoresMisbehavior(t *testing.T) {
	h := NewHandshake(outboundConfig())
	_ = h.Initial()

	_, err := h.OnMessage(remoteVersion(1))
	require.NoError(t, err)

	// net_processing.cpp: Misbehaving(pfrom, 1, "multiple-version")
	reply, err := h.OnMessage(remoteVersion(2))
	require.NoError(t, err)
	require.Empty(t, reply)
	require.Equal(t, 1, h.MisbehaviorScore())
}

func TestMessageBeforeVersionScoresMisbehavior(t *testing.T) {
	h := NewHandshake(outboundConfig())
	_ = h.Initial()

	// net_processing.cpp: Misbehaving(pfrom, 1, "missing-version")
	_, err := h.OnMessage(wire.NewMsgPing(1))
	require.NoError(t, err)
	require.Equal(t, 1, h.MisbehaviorScore())

	_, err = h.OnMessage(wire.NewMsgVerAck())
	require.NoError(t, err)
	require.Equal(t, 2, h.MisbehaviorScore())
}

func TestPingAfterEstablishedGetsPong(t *testing.T) {
	h := NewHandshake(outboundConfig())
	_ = h.Initial()
	_, _ = h.OnMessage(remoteVersion(1))
	_, _ = h.OnMessage(wire.NewMsgVerAck())

	reply, err := h.OnMessage(wire.NewMsgPing(99))
	require.NoError(t, err)
	require.Len(t, reply, 1)
	pong, ok := reply[0].(*wire.MsgPong)
	require.True(t, ok)
	require.Equal(t, uint64(99), pong.Nonce)
}

func TestProtoconfRecorded(t *testing.T) {
	h := NewHandshake(outboundConfig())
	_ = h.Initial()
	_, _ = h.OnMessage(remoteVersion(1))
	_, _ = h.OnMessage(wire.NewMsgVerAck())

	_, err := h.OnMessage(wire.NewMsgProtoconf(1<<21, true))
	require.NoError(t, err)
	require.Equal(t, uint32(1<<21), h.PeerInfo().TheirMaxRecvPayloadLength)
}

func TestDuplicateProtoconfDisconnects(t *testing.T) {
	h := NewHandshake(outboundConfig())
	_ = h.Initial()
	_, _ = h.OnMessage(remoteVersion(1))
	_, _ = h.OnMessage(wire.NewMsgVerAck())
	_, _ = h.OnMessage(wire.NewMsgProtoconf(1<<21, true))

	// net_processing.cpp ProcessProtoconfMessage: protoconfReceived → fDisconnect
	_, err := h.OnMessage(wire.NewMsgProtoconf(1<<21, true))
	require.ErrorIs(t, err, ErrProtocolViolation)
}

func TestUndersizedProtoconfDisconnects(t *testing.T) {
	h := NewHandshake(outboundConfig())
	_ = h.Initial()
	_, _ = h.OnMessage(remoteVersion(1))
	_, _ = h.OnMessage(wire.NewMsgVerAck())

	// net_processing.cpp ProcessProtoconfMessage: maxRecvPayloadLength below
	// LEGACY_MAX_PROTOCOL_PAYLOAD_LENGTH (1 MiB) → fDisconnect
	small := &wire.MsgProtoconf{NumberOfFields: 1, MaxRecvPayloadLength: 512 * 1024}

	_, err := h.OnMessage(small)
	require.ErrorIs(t, err, ErrProtocolViolation)
}

func TestSendHeadersAndFeeFilterRecorded(t *testing.T) {
	h := NewHandshake(outboundConfig())
	_ = h.Initial()
	_, _ = h.OnMessage(remoteVersion(1))
	_, _ = h.OnMessage(wire.NewMsgVerAck())

	_, err := h.OnMessage(wire.NewMsgSendHeaders())
	require.NoError(t, err)
	require.True(t, h.PeerInfo().WantsHeaders)

	_, err = h.OnMessage(wire.NewMsgFeeFilter(250))
	require.NoError(t, err)
	require.Equal(t, int64(250), h.PeerInfo().FeeFilter)
}

func TestNoSendHeadersForOldPeer(t *testing.T) {
	h := NewHandshake(outboundConfig())
	_ = h.Initial()

	old := remoteVersion(1)
	old.ProtocolVersion = 70011 // below SENDHEADERS_VERSION

	_, err := h.OnMessage(old)
	require.NoError(t, err)

	// net_processing.cpp ProcessVerAckMessage: sendheaders only when
	// pfrom->nVersion >= SENDHEADERS_VERSION.
	reply, err := h.OnMessage(wire.NewMsgVerAck())
	require.NoError(t, err)
	require.Empty(t, reply)
	require.True(t, h.Established())
}

// net_processing.cpp PushNodeVersion: the association ID rides in version
// when multistreams are on. Outbound only — inbound answers with the peer's.
func TestHandshake_OutboundVersionCarriesOurAssociationID(t *testing.T) {
	id := testAssociationID(1)
	cfg := outboundConfig()
	cfg.AssociationID = id
	h := NewHandshake(cfg)

	msgs := h.Initial()
	require.Len(t, msgs, 1)
	v, ok := msgs[0].(*wire.MsgVersion)
	require.True(t, ok)
	require.Equal(t, id, v.AssociationID)
}

// establishedInboundHandshake drives an inbound Handshake through
// version+verack so it is fully established, for tests that only care about
// post-handshake behaviour.
func establishedInboundHandshake(t *testing.T) *Handshake {
	t.Helper()

	cfg := outboundConfig()
	cfg.Inbound = true
	h := NewHandshake(cfg)

	_, err := h.OnMessage(remoteVersion(1234))
	require.NoError(t, err)

	_, err = h.OnMessage(wire.NewMsgVerAck())
	require.NoError(t, err)
	require.True(t, h.Established())

	return h
}

// net_processing.cpp:1521-1528 / :1598-1604: createstream or streamack on a
// connection that already saw version → reject REJECT_NONSTANDARD, disconnect.
func TestHandshake_StreamMessagesAfterVersionAreRejected(t *testing.T) {
	for _, msg := range []wire.Message{&wire.MsgCreateStream{}, &wire.MsgStreamAck{}} {
		h := establishedInboundHandshake(t)

		replies, err := h.OnMessage(msg)
		require.ErrorIs(t, err, ErrStreamMessageAfterVersion)
		require.Len(t, replies, 1)
		rej, ok := replies[0].(*wire.MsgReject)
		require.True(t, ok)
		require.Equal(t, wire.RejectNonstandard, rej.Code)
	}
}

// association_id.h:34: 17 bytes, byte 0 = IDType::UUID (0x00), then 16
// random bytes.
func TestGenerateAssociationID(t *testing.T) {
	id, err := generateAssociationID()
	require.NoError(t, err)
	require.Len(t, id, 17)
	require.Equal(t, byte(0x00), id[0])

	other, err := generateAssociationID()
	require.NoError(t, err)
	require.NotEqual(t, id, other)
}

// versionIn picks the version message out of a handshake reply.
func versionIn(t *testing.T, msgs []wire.Message) *wire.MsgVersion {
	t.Helper()

	for _, msg := range msgs {
		if v, ok := msg.(*wire.MsgVersion); ok {
			return v
		}
	}

	require.Fail(t, "the reply carries no version message")

	return nil
}

// net_processing.cpp:1758-1776 ProcessVersionMessage stores the dialer's
// association ID on the accepting node (SetAssociationID at :1775), and
// net_processing.cpp:1816-1818 then answers with PushNodeVersion, which reads
// that same ID back off the node (net_processing.cpp:142-146). The accepting
// side therefore echoes the ID the dialer named.
func TestHandshake_InboundVersionEchoesTheirAssociationID(t *testing.T) {
	cfg := outboundConfig()
	cfg.Inbound = true
	h := NewHandshake(cfg)

	id := testAssociationID(2)
	their := remoteVersion(1234)
	their.AssociationID = id

	reply, err := h.OnMessage(their)
	require.NoError(t, err)
	require.Equal(t, id, versionIn(t, reply).AssociationID)
}

// net_processing.cpp:1760: the received ID is only stored when
// config.GetMultistreamsEnabled() is true, so a node with multistreams off
// echoes nothing and never gets a second stream.
func TestHandshake_InboundEchoesNothingWithoutBlockPriority(t *testing.T) {
	cfg := outboundConfig()
	cfg.Inbound = true
	cfg.AllowBlockPriority = false
	h := NewHandshake(cfg)

	their := remoteVersion(1234)
	their.AssociationID = testAssociationID(2)

	reply, err := h.OnMessage(their)
	require.NoError(t, err)
	require.Empty(t, versionIn(t, reply).AssociationID)
}

// net_processing.cpp:142-146: PushNodeVersion sends an empty ID when the node
// has none, which for an inbound peer means the dialer named none either.
func TestHandshake_InboundNamesNoAssociationWhenTheyNameNone(t *testing.T) {
	cfg := outboundConfig()
	cfg.Inbound = true
	h := NewHandshake(cfg)

	reply, err := h.OnMessage(remoteVersion(1234))
	require.NoError(t, err)
	require.Empty(t, versionIn(t, reply).AssociationID)
}

// testAssociationID is a well-formed 17-byte UUID association ID
// (association_id.h:34): the IDType::UUID tag, then 16 body bytes. tag makes
// one test's ID distinguishable from another's.
func testAssociationID(tag byte) []byte {
	id := make([]byte, 17)
	for i := 1; i < len(id); i++ {
		id[i] = tag
	}

	return id
}

// association_id.cpp:10-32 AssociationID::Make throws on any type byte other
// than IDType::UUID, and association_id.cpp:43-53 UUIDAssociationID throws
// unless the body is exactly 16 bytes. Either throw reaches the catch at
// net_processing.cpp:1796-1801, which rejects and disconnects.
func TestHandshake_MalformedAssociationIDDisconnects(t *testing.T) {
	tests := []struct {
		name string
		id   []byte
	}{
		{name: "unsupported type byte", id: append([]byte{0x01}, make([]byte, 16)...)},
		{name: "body one byte short", id: append([]byte{0x00}, make([]byte, 15)...)},
		{name: "body one byte long", id: append([]byte{0x00}, make([]byte, 17)...)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := outboundConfig()
			cfg.Inbound = true
			h := NewHandshake(cfg)

			their := remoteVersion(1234)
			their.AssociationID = tt.id

			_, err := h.OnMessage(their)
			require.ErrorIs(t, err, ErrMalformedAssociationID)
			require.ErrorIs(t, err, ErrProtocolViolation)
		})
	}
}

// association_id.cpp:12 and :31: Make returns a null pointer, not a throw, for
// fewer than two bytes, and net_processing.cpp:1778-1781 answers a null ID
// with ClearAssociationID. Such a peer stays connected and gets no echo.
func TestHandshake_NullAssociationIDIsNotAViolation(t *testing.T) {
	for _, id := range [][]byte{{}, {0x00}} {
		cfg := outboundConfig()
		cfg.Inbound = true
		h := NewHandshake(cfg)

		their := remoteVersion(1234)
		their.AssociationID = id

		reply, err := h.OnMessage(their)
		require.NoError(t, err)
		require.Empty(t, versionIn(t, reply).AssociationID)
	}
}

// net_processing.cpp:1760: the whole decode, Make included, is gated on
// GetMultistreamsEnabled, so a node with multistreams off never inspects the
// ID and never disconnects over one.
func TestHandshake_MalformedAssociationIDIgnoredWithoutBlockPriority(t *testing.T) {
	cfg := outboundConfig()
	cfg.Inbound = true
	cfg.AllowBlockPriority = false
	h := NewHandshake(cfg)

	their := remoteVersion(1234)
	their.AssociationID = []byte{0x07, 1, 2, 3}

	reply, err := h.OnMessage(their)
	require.NoError(t, err)
	require.Empty(t, versionIn(t, reply).AssociationID)
}
