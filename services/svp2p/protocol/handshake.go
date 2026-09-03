// Package protocol is the net_processing.cpp port: protocol decisions and
// per-peer state machines. It performs no I/O; the caller feeds it messages
// and sends what it returns.
package protocol

import (
	"crypto/rand"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
)

// MinPeerProtoVersion mirrors SVNode MIN_PEER_PROTO_VERSION
// (version.h: GETHEADERS_VERSION = 31800).
const MinPeerProtoVersion uint32 = 31800

// LegacyMaxProtocolPayloadLength mirrors SVNode
// LEGACY_MAX_PROTOCOL_PAYLOAD_LENGTH (protocol.h: 1 MiB). A peer whose
// protoconf advertises less violates the protocol.
const LegacyMaxProtocolPayloadLength uint32 = 1 * 1024 * 1024

// Misbehavior scores, verified against net_processing.cpp on 2026-08-18.
const (
	scoreMultipleVersion = 1 // Misbehaving(pfrom, 1, "multiple-version")
	scoreMissingVersion  = 1 // Misbehaving(pfrom, 1, "missing-version")
)

// Sentinel errors. The teranode errors package matches errors.Is by CODE,
// so each sentinel that callers must tell apart carries its own code.
var (
	ErrSelfConnection    = errors.New(errors.ERR_NETWORK_CONNECTION_REFUSED, "svp2p: connected to self")
	ErrObsoleteVersion   = errors.New(errors.ERR_NETWORK_INVALID_RESPONSE, "svp2p: peer protocol version obsolete")
	ErrProtocolViolation = errors.New(errors.ERR_NETWORK_PEER_MALICIOUS, "svp2p: protocol violation")

	// ErrStreamMessageAfterVersion mirrors net_processing.cpp
	// ProcessCreateStreamMessage / ProcessStreamAckMessage: createstream or
	// streamack arriving on a connection that already has a version is
	// invalid — REJECT_NONSTANDARD, then disconnect.
	ErrStreamMessageAfterVersion = errors.New(errors.ERR_NETWORK_PEER_MALICIOUS, "svp2p: stream setup message after version", ErrProtocolViolation)

	// ErrMalformedAssociationID mirrors the "Badly formatted association ID"
	// throw in net_processing.cpp:1784-1789, which the catch at
	// net_processing.cpp:1796-1801 turns into a reject and a disconnect.
	ErrMalformedAssociationID = errors.New(errors.ERR_NETWORK_PEER_MALICIOUS, "svp2p: malformed association id", ErrProtocolViolation)
)

// associationIDTypeUUID is IDType::UUID (association_id.h:34): byte 0 of an
// association ID identifies its format; UUID is 16 random bytes after it.
const associationIDTypeUUID = 0x00

// associationIDLength is the whole of a UUID association ID on the wire: the
// type byte plus the 16 bytes of the UUID itself.
const associationIDLength = 17

// validAssociationID reports whether AssociationID::Make would accept these
// bytes. association_id.cpp:16-28 throws on any type byte other than
// IDType::UUID, and association_id.cpp:43-53 UUIDAssociationID throws unless
// the body is exactly the 16 bytes of a UUID.
func validAssociationID(id []byte) bool {
	return len(id) == associationIDLength && id[0] == associationIDTypeUUID
}

// nullAssociationID reports the case association_id.cpp:12 and :31 answer with
// a null pointer rather than a throw: fewer than two bytes. The peer supports
// streams but has turned them off, and net_processing.cpp:1778-1781 clears the
// ID instead of disconnecting.
func nullAssociationID(id []byte) bool {
	return len(id) < 2
}

// generateAssociationID builds a fresh 17-byte association ID: a 1-byte
// IDType::UUID tag followed by 16 random bytes (association_id.h:34,
// legacy peer/association.go:190). A crypto/rand read error is returned to
// the caller, who logs it and runs the connection without multistreams.
func generateAssociationID() ([]byte, error) {
	id := make([]byte, 17)
	id[0] = associationIDTypeUUID

	if _, err := rand.Read(id[1:]); err != nil {
		return nil, errors.New(errors.ERR_UNKNOWN, "svp2p: failed to generate association id", err)
	}

	return id, nil
}

type HandshakeConfig struct {
	Inbound              bool
	Nonce                uint64
	UserAgent            string
	StartingHeight       int32
	MaxRecvPayloadLength uint32
	AllowBlockPriority   bool
	LocalAddr            *wire.NetAddress
	RemoteAddr           *wire.NetAddress

	// AssociationID rides in our version message when non-nil
	// (net_processing.cpp PushNodeVersion). Outbound only: the manager sets
	// it only for outbound peers, and the inbound side echoes the dialer's ID
	// instead (see associationIDForVersion).
	AssociationID []byte

	// CheckIncomingNonce mirrors net.cpp CConnman::CheckIncomingNonce: true
	// if this node itself sent the given nonce on one of its own
	// connections, meaning the connection is a self-connect. Supplied by
	// PeerManager as a closure over its node-global nonce registry; nil in
	// tests that don't exercise self-connection detection.
	CheckIncomingNonce func(uint64) bool
}

type PeerInfo struct {
	NegotiatedVersion         uint32
	AdvertisedVersion         uint32
	Services                  wire.ServiceFlag
	UserAgent                 string
	StartingHeight            int32
	DisableRelayTx            bool
	AssociationID             []byte // stored for Phase 4 (multistreams)
	TheirMaxRecvPayloadLength uint32
	TheirStreamPolicies       []string
	// ProtoconfReceived is true once the peer's protoconf has been processed.
	// The stream policies above are only meaningful from that point: a peer
	// that allows no policy at all and one that has not spoken yet both leave
	// TheirStreamPolicies empty (net_processing.cpp:4402-4405 sets them only
	// when the message carries two fields).
	ProtoconfReceived bool
	WantsHeaders      bool // they sent sendheaders
	FeeFilter         int64
}

// Handshake is the per-peer handshake state machine. It is not safe for
// concurrent use; the owning Peer serializes access.
type Handshake struct {
	cfg            HandshakeConfig
	versionRecvd   bool
	verackRecvd    bool
	versionSent    bool
	protoconfRecvd bool
	established    bool
	misbehavior    int
	info           PeerInfo
}

func NewHandshake(cfg HandshakeConfig) *Handshake {
	return &Handshake{cfg: cfg}
}

// Initial returns what we send unprompted at connect time.
// net_processing.cpp: outbound pushes version at connect (PushNodeVersion
// from InitializeNode); inbound is "shy" and waits for the peer's version.
func (h *Handshake) Initial() []wire.Message {
	if h.cfg.Inbound {
		return nil
	}

	h.versionSent = true

	return []wire.Message{h.ourVersion()}
}

func (h *Handshake) ourVersion() *wire.MsgVersion {
	msg := wire.NewMsgVersion(h.cfg.LocalAddr, h.cfg.RemoteAddr, h.cfg.Nonce, h.cfg.StartingHeight)
	msg.UserAgent = h.cfg.UserAgent
	msg.AssociationID = h.associationIDForVersion()

	return msg
}

// associationIDForVersion is the association ID our version message carries.
// net_processing.cpp:142-146 PushNodeVersion reads it off the node itself, so
// which ID that is depends on how the connection started:
//
//   - Outbound: net_processing.cpp:209-211 creates the ID before the version
//     goes out, and the manager hands it over as cfg.AssociationID.
//   - Inbound: net_processing.cpp:1775 SetAssociationID stores the DIALER's ID
//     on the node while the version is being processed, and the reply at
//     net_processing.cpp:1816-1818 then carries that same ID back. An inbound
//     peer that gets no echo clears its own ID and never sends createstream.
//
// Both sides are gated on config.GetMultistreamsEnabled()
// (net_processing.cpp:209 and :1760), which is AllowBlockPriority here.
// onVersion has already refused any ID Make would have thrown on, so what is
// stored here is either well formed or empty.
func (h *Handshake) associationIDForVersion() []byte {
	if !h.cfg.Inbound {
		return h.cfg.AssociationID
	}

	if !h.cfg.AllowBlockPriority {
		return nil
	}

	return h.info.AssociationID
}

func (h *Handshake) OnMessage(msg wire.Message) ([]wire.Message, error) {
	if m, ok := msg.(*wire.MsgVersion); ok {
		return h.onVersion(m)
	}

	// net_processing.cpp ProcessCreateStreamMessage:1521-1528 /
	// ProcessStreamAckMessage:1598-1604: a connection that already has a
	// version can't also set up a stream — REJECT_NONSTANDARD, disconnect.
	switch m := msg.(type) {
	case *wire.MsgCreateStream:
		if h.versionRecvd {
			return []wire.Message{wire.NewMsgReject(m.Command(), wire.RejectNonstandard, "Invalid createstream scenario")}, ErrStreamMessageAfterVersion
		}
	case *wire.MsgStreamAck:
		if h.versionRecvd {
			return []wire.Message{wire.NewMsgReject(m.Command(), wire.RejectNonstandard, "Invalid streamack")}, ErrStreamMessageAfterVersion
		}
	}

	// net_processing.cpp: "Must have a version or createstream message
	// before anything else" → Misbehaving(1, "missing-version").
	// createstream/streamack arrive in Phase 4 (associations).
	if !h.versionRecvd {
		h.misbehavior += scoreMissingVersion
		return nil, nil
	}

	switch m := msg.(type) {
	case *wire.MsgVerAck:
		return h.onVerack()
	case *wire.MsgProtoconf:
		return h.onProtoconf(m)
	case *wire.MsgPing:
		return h.onPing(m)
	case *wire.MsgSendHeaders:
		h.info.WantsHeaders = true // net_processing.cpp SENDHEADERS
		return nil, nil
	case *wire.MsgFeeFilter:
		h.info.FeeFilter = m.MinFee // net_processing.cpp FEEFILTER
		return nil, nil
	default:
		return nil, nil // unhandled in Phase 1; later phases dispatch here
	}
}

func (h *Handshake) onVersion(m *wire.MsgVersion) ([]wire.Message, error) {
	// net_processing.cpp ProcessVersionMessage: "Each connection can only
	// send one version message" → REJECT + Misbehaving(1, "multiple-version").
	if h.versionRecvd {
		h.misbehavior += scoreMultipleVersion
		return nil, nil
	}

	// net_processing.cpp ProcessVersionMessage: "connected to self at %s,
	// disconnecting". Per-connection compare: catches the case where this
	// same connection's two ends coincidentally negotiated matching nonces.
	if m.Nonce == h.cfg.Nonce {
		return nil, ErrSelfConnection
	}

	// net.cpp CConnman::CheckIncomingNonce: a real self-connect dials a
	// separate socket back to this node, so the two ends carry different
	// per-connection nonces and the compare above never fires. Instead,
	// check the incoming nonce against every nonce this node has itself
	// sent on any of its own connections.
	if h.cfg.CheckIncomingNonce != nil && h.cfg.CheckIncomingNonce(m.Nonce) {
		return nil, ErrSelfConnection
	}

	// net_processing.cpp: "using obsolete version %i; disconnecting".
	if uint32(m.ProtocolVersion) < MinPeerProtoVersion {
		return nil, ErrObsoleteVersion
	}

	// net_processing.cpp:1758-1776: the peer's association ID is decoded, and
	// AssociationID::Make throws on anything malformed. The whole block sits
	// under config.GetMultistreamsEnabled() (net_processing.cpp:1760), so a
	// node with multistreams off never inspects the ID at all.
	associationID := m.AssociationID

	if h.cfg.AllowBlockPriority {
		switch {
		case nullAssociationID(associationID):
			// net_processing.cpp:1778-1781 ClearAssociationID.
			associationID = nil
		case !validAssociationID(associationID):
			return nil, ErrMalformedAssociationID
		}
	}

	h.versionRecvd = true
	h.info.Services = m.Services
	h.info.UserAgent = m.UserAgent
	h.info.StartingHeight = m.LastBlock
	h.info.DisableRelayTx = m.DisableRelayTx
	h.info.AssociationID = associationID
	h.info.AdvertisedVersion = uint32(m.ProtocolVersion) //nolint:gosec // checked >= MinPeerProtoVersion above
	h.info.NegotiatedVersion = min(wire.ProtocolVersion, h.info.AdvertisedVersion)

	var reply []wire.Message

	// net_processing.cpp: "Be shy and don't send version until we hear" —
	// the inbound side answers VERSION with PushNodeVersion.
	if h.cfg.Inbound && !h.versionSent {
		h.versionSent = true
		reply = append(reply, h.ourVersion())
	}

	// net_processing.cpp ProcessVersionMessage: PushMessage(VERACK), then
	// "Announce our protocol configuration immediately after we send VERACK"
	// (PushProtoconf).
	reply = append(reply,
		wire.NewMsgVerAck(),
		wire.NewMsgProtoconf(h.cfg.MaxRecvPayloadLength, h.cfg.AllowBlockPriority),
	)

	return reply, nil
}

func (h *Handshake) onVerack() ([]wire.Message, error) {
	// SVNode has no duplicate-verack guard; we ignore duplicates without
	// re-sending so the handshake stays idempotent.
	if h.verackRecvd {
		return nil, nil
	}

	h.verackRecvd = true
	h.established = true // net_processing.cpp: fSuccessfullyConnected = true

	// Deliberate Phase 1 non-carries from ProcessVerAckMessage: AUTHCH
	// (miner-ID auth, optional on the wire) and SENDCMPCT (we cannot serve
	// compact blocks until Phase 5, so we must not advertise them).

	// net_processing.cpp ProcessVerAckMessage: sendheaders when the peer's
	// advertised version is at least SENDHEADERS_VERSION.
	if h.info.AdvertisedVersion >= wire.SendHeadersVersion {
		return []wire.Message{wire.NewMsgSendHeaders()}, nil
	}

	return nil, nil
}

func (h *Handshake) onProtoconf(m *wire.MsgProtoconf) ([]wire.Message, error) {
	// net_processing.cpp ProcessProtoconfMessage: a second protoconf sets
	// fDisconnect.
	if h.protoconfRecvd {
		return nil, errors.New(errors.ERR_NETWORK_PEER_MALICIOUS, "svp2p: duplicate protoconf", ErrProtocolViolation)
	}

	h.protoconfRecvd = true

	// net_processing.cpp ProcessProtoconfMessage: advertising less than
	// LEGACY_MAX_PROTOCOL_PAYLOAD_LENGTH is a protocol violation.
	if m.NumberOfFields >= 1 && m.MaxRecvPayloadLength < LegacyMaxProtocolPayloadLength {
		return nil, errors.New(errors.ERR_NETWORK_PEER_MALICIOUS, "svp2p: protoconf payload length too low", ErrProtocolViolation)
	}

	h.info.TheirMaxRecvPayloadLength = m.MaxRecvPayloadLength

	// net_processing.cpp:4402-4405: the policies are read only when the
	// message declares the field.
	if m.NumberOfFields >= 2 {
		h.info.TheirStreamPolicies = m.StreamPolicies
	}

	h.info.ProtoconfReceived = true

	return nil, nil
}

func (h *Handshake) onPing(m *wire.MsgPing) ([]wire.Message, error) {
	// net_processing.cpp PING: pong echoes the nonce (pver > BIP0031Version).
	if h.info.NegotiatedVersion > wire.BIP0031Version {
		return []wire.Message{wire.NewMsgPong(m.Nonce)}, nil
	}

	return nil, nil
}

func (h *Handshake) Established() bool { return h.established }

func (h *Handshake) PeerInfo() PeerInfo { return h.info }

func (h *Handshake) MisbehaviorScore() int { return h.misbehavior }

// AddMisbehavior applies a score a machine outside the handshake decided on
// (the headers and inv handlers), so net_processing.cpp's one Misbehaving
// counter per peer stays one counter here too. The owning Peer serializes it
// with the rest of the handshake state.
func (h *Handshake) AddMisbehavior(delta int) {
	if delta > 0 {
		h.misbehavior += delta
	}
}
