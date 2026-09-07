package protocol

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"io"
	"net"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/svp2p/transport"
)

// rejectStreamSetup is net_types.h:26 REJECT_STREAM_SETUP, the reject code
// every stream-setup failure carries (net_processing.cpp:1580). go-wire has no
// constant for it.
const rejectStreamSetup = wire.RejectCode(0x60)

// defaultFirstMessageTimeout bounds how long a fresh inbound connection may
// stay silent before its first message. It is SVNode's handshake bound, not
// its inactivity bound: net.h:88 DEFAULT_P2P_HANDSHAKE_TIMEOUT_INTERVAL is
// 1 * 60, and net.cpp:1058-1064 CNode::ServiceSockets disconnects a connection
// that has still received nothing (nLastRecv == 0) once that window passes.
//
// This is a DIFFERENT constant from net.h:86
// DEFAULT_P2P_TIMEOUT_INTERVAL (20 * 60), the idle window cited by
// effectivePingInterval further down manager.go. A connection that has said
// nothing at all gets 60 s; one that is talking gets 20 min of quiet.
const defaultFirstMessageTimeout = 60 * time.Second

// extLengthMarker is protocol.cpp:220-237's extended-header length marker. A
// first message is never a block, so an extended frame here is never legal.
const extLengthMarker = uint32(0xffffffff)

// misbehavingBanDuration is the bantime SVNode gives BanReasonNodeMisbehaving
// (net.cpp:3225 Ban(fromAddr, BanReasonNodeMisbehaving), DEFAULT_MISBEHAVING_BANTIME).
const misbehavingBanDuration = 24 * time.Hour

var (
	// ErrFirstMessageExtended reports an extended message header as the first
	// message on a connection.
	ErrFirstMessageExtended = errors.New(errors.ERR_NETWORK_PEER_MALICIOUS, "svp2p: extended header as the first message")

	// ErrFirstMessageTooLong reports a first message that declares more
	// payload than its own command can ever carry.
	ErrFirstMessageTooLong = errors.New(errors.ERR_NETWORK_PEER_MALICIOUS, "svp2p: first message payload too long")

	// ErrFirstMessageCommand reports a first message whose command is not one
	// of the few legal on a connection that has said nothing yet.
	ErrFirstMessageCommand = errors.New(errors.ERR_NETWORK_PEER_MALICIOUS, "svp2p: illegal first message command")
)

// maxRejectFirstMessageBytes bounds a reject read off a socket that carries no
// Conn yet. go-wire's own (*MsgReject).MaxPayloadLength is maxMessagePayload(),
// which it derives from the excessive block size global, so it is no bound at
// all here: it would let a 24 byte header ask this node for tens of MiB.
//
// A reject is small by construction. SVNode truncates the reason to
// validation.h:155 MAX_REJECT_MESSAGE_LENGTH (111) before sending, and reads it
// back under the same limit (net_processing.cpp:1486 LIMITED_STRING). The whole
// message is then the command varstr (1 + 12), the reject code (1), the reason
// varstr (1 + 111) and an optional 32 byte hash: 158 bytes. 256 leaves room for
// a peer that frames it less tightly and is still nothing to allocate.
const maxRejectFirstMessageBytes = uint64(256)

// recvCapped holds a per-message maximum at or below what this node advertises
// it will ever receive (net_processing.cpp:3306 maxRecvPayloadLength). go-wire
// derives several of its maxima from mutable globals, so without this cap a
// wire.SetLimits call elsewhere in the node could silently widen what an
// unauthenticated connection may make this node allocate.
func recvCapped(n uint64) uint64 {
	if n > uint64(wire.DefaultMaxRecvPayloadLength) {
		return uint64(wire.DefaultMaxRecvPayloadLength)
	}

	return n
}

// inboundFirstLimits bounds the payload of every command
// net_processing.cpp:4708-4715 accepts before a version, each by its OWN
// maximum rather than by the 2 MiB receive ceiling. The command is read out of
// the header before anything allocates, so one unauthenticated connection can
// never make this node reserve more than the largest of these.
//
// Built on each call, never frozen at init: go-wire's maxima read globals that
// this node sets at startup, so a value captured once could be stale.
func inboundFirstLimits() map[string]uint64 {
	return map[string]uint64{
		wire.CmdVersion:      recvCapped((&wire.MsgVersion{}).MaxPayloadLength(wire.ProtocolVersion)),
		wire.CmdCreateStream: recvCapped((&wire.MsgCreateStream{}).MaxPayloadLength(wire.ProtocolVersion)),
		wire.CmdStreamAck:    recvCapped((&wire.MsgStreamAck{}).MaxPayloadLength(wire.ProtocolVersion)),
	}
}

// streamAckLimits bounds the answer to a createstream this node sent: the
// streamack that confirms it, or the reject that refuses it
// (net_processing.cpp:1577-1584).
func streamAckLimits() map[string]uint64 {
	return map[string]uint64{
		wire.CmdStreamAck: recvCapped((&wire.MsgStreamAck{}).MaxPayloadLength(wire.ProtocolVersion)),
		wire.CmdReject:    maxRejectFirstMessageBytes,
	}
}

// The reject reason a stream-setup failure puts ON THE WIRE. SVNode sends
// what its own exception carried (net_processing.cpp:1580 e.what()), so the
// first four are its strings verbatim. The last three name cases SVNode has no
// string for: its own message for the address mismatch (net.cpp:3227-3229)
// names both endpoints, which would hand an attacker the victim's IP.
//
// Every one of them is a fixed constant well inside
// MAX_REJECT_MESSAGE_LENGTH (validation.h:155, 111 bytes), and none carries
// this node's internal error text. The detail belongs in the log line.
const (
	reasonStreamTypeRange = "StreamType out of range"
	reasonNoSuchNode      = "No node found with association ID"
	reasonStreamExists    = "Attempt to overwrite existing stream in move"
	reasonBadlyFormatted  = "Badly formatted message"
	reasonMultistreamsOff = "Multistreams disabled"
	reasonUnknownPolicy   = "Unknown stream policy"
	reasonOtherAddress    = "Stream setup from a different address"
)

// streamSetupError splits what a stream-setup failure says on the wire from
// what it says in the log. Reason is the plain constant the reject carries;
// Error is the detail — IPs, IDs, codes — which stays local.
type streamSetupError struct {
	reason string
	err    error
}

func (e *streamSetupError) Error() string { return e.err.Error() }

func (e *streamSetupError) Unwrap() error { return e.err }

func (e *streamSetupError) Reason() string { return e.reason }

func streamSetupFailure(reason string, err error) error {
	return &streamSetupError{reason: reason, err: err}
}

// rejectReason is the wire string for a stream-setup failure. Anything that
// did not name its own reason is reported as a malformed message rather than
// leaking the text of an error this node did not intend to publish.
func rejectReason(err error) string {
	var setup *streamSetupError

	if errors.As(err, &setup) {
		return setup.Reason()
	}

	return reasonBadlyFormatted
}

// classifyInbound decides what a fresh inbound connection is from its FIRST
// message. net_processing.cpp:4708-4715 takes version, createstream and
// streamack before anything else, and treats every other command on a
// connection with no version as "missing-version".
//
// It owns nc until it hands it on: a version starts a Peer over it, a
// successful createstream moves it into an existing association as a further
// stream, and every other outcome closes it.
func (m *PeerManager) classifyInbound(ctx context.Context, nc net.Conn) {
	magic := m.tSettings.ChainCfgParams.Net

	// The read below is the only thing standing between a silent connection
	// and Stop's wg.Wait, so shutdown has to be able to break it.
	read := make(chan struct{})

	go func() {
		select {
		case <-read:
		case <-m.quit:
			_ = nc.SetReadDeadline(time.Now())
		case <-ctx.Done():
			_ = nc.SetReadDeadline(time.Now())
		}
	}()

	msg, raw, err := readFirstMessage(nc, magic, m.firstMessageTimeout, inboundFirstLimits())

	close(read)

	if err != nil {
		m.logger.Debugf("[svp2p] inbound %s sent no usable first message: %v", nc.RemoteAddr(), err)

		_ = nc.Close()

		return
	}

	switch first := msg.(type) {
	case *wire.MsgVersion:
		_ = m.runPeer(ctx, nc, true, raw)

	case *wire.MsgCreateStream:
		if moveErr := m.moveStream(nc, first); moveErr != nil {
			m.logger.Infof("[svp2p] peer %s failed to setup new stream (%v); disconnecting", nc.RemoteAddr(), moveErr)

			m.writeAndClose(nc, wire.NewMsgReject(wire.CmdCreateStream, rejectStreamSetup, rejectReason(moveErr)))
		}

	case *wire.MsgStreamAck:
		// net_processing.cpp:1598-1604: a streamack answers a createstream WE
		// sent, and this connection has none outstanding.
		m.logger.Infof("[svp2p] peer %s sent an unsolicited streamack; disconnecting", nc.RemoteAddr())

		m.writeAndClose(nc, wire.NewMsgReject(wire.CmdStreamAck, wire.RejectNonstandard, "Invalid streamack"))

	default:
		// net_processing.cpp:4708-4711 "Must have a version or createstream
		// message before anything else".
		m.logger.Infof("[svp2p] peer %s sent %s before version; disconnecting", nc.RemoteAddr(), msg.Command())

		_ = nc.Close()
	}
}

// writeAndClose sends one last message on a raw socket that has no
// transport.Conn behind it, then ends the connection.
func (m *PeerManager) writeAndClose(nc net.Conn, msg wire.Message) {
	_ = nc.SetWriteDeadline(time.Now().Add(writeTimeout))

	if err := wire.WriteMessage(nc, msg, wire.ProtocolVersion, m.tSettings.ChainCfgParams.Net); err != nil {
		m.logger.Debugf("[svp2p] could not send %s to %s: %v", msg.Command(), nc.RemoteAddr(), err)
	}

	_ = nc.Close()
}

// readFirstMessage takes exactly one message off the socket and returns it
// together with the raw header and payload bytes, so a connection that turns
// out to be a peer can replay them into its transport.
//
// limits names every command legal here and the largest payload each may
// declare. Both are checked on the 24 byte header, BEFORE the payload buffer
// is allocated: the caller holds a connection that has authenticated nothing,
// so the declared length must never decide the size of an allocation.
func readFirstMessage(nc net.Conn, magic wire.BitcoinNet, timeout time.Duration, limits map[string]uint64) (wire.Message, []byte, error) {
	if err := nc.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, nil, errors.New(errors.ERR_NETWORK_ERROR, "svp2p: cannot set first message deadline", err)
	}

	raw := make([]byte, wire.MessageHeaderSize)

	if _, err := io.ReadFull(nc, raw); err != nil {
		return nil, nil, errors.New(errors.ERR_NETWORK_ERROR, "svp2p: cannot read first message header", err)
	}

	length := binary.LittleEndian.Uint32(raw[16:20])

	if length == extLengthMarker {
		return nil, nil, ErrFirstMessageExtended
	}

	command := string(bytes.TrimRight(raw[4:4+wire.CommandSize], "\x00"))

	limit, legal := limits[command]
	if !legal {
		return nil, nil, ErrFirstMessageCommand
	}

	if uint64(length) > limit {
		return nil, nil, ErrFirstMessageTooLong
	}

	if length > 0 {
		payload := make([]byte, length)

		if _, err := io.ReadFull(nc, payload); err != nil {
			return nil, nil, errors.New(errors.ERR_NETWORK_ERROR, "svp2p: cannot read first message payload", err)
		}

		raw = append(raw, payload...)
	}

	if err := nc.SetReadDeadline(time.Time{}); err != nil {
		return nil, nil, errors.New(errors.ERR_NETWORK_ERROR, "svp2p: cannot clear first message deadline", err)
	}

	_, msg, _, err := wire.ReadMessageWithEncodingN(bytes.NewReader(raw), wire.ProtocolVersion, magic, wire.BaseEncoding)
	if err != nil {
		return nil, nil, errors.New(errors.ERR_NETWORK_INVALID_RESPONSE, "svp2p: cannot decode first message", err)
	}

	return msg, raw, nil
}

// moveStream is net.cpp:3188-3240 CConnman::MoveStream, driven by
// net_processing.cpp:1514-1590 ProcessCreateStreamMessage. On success nc stops
// being a connection of its own and becomes a further stream of the named
// association; the caller must not close it.
func (m *PeerManager) moveStream(nc net.Conn, cs *wire.MsgCreateStream) error {
	if !m.tSettings.Legacy.AllowBlockPriority {
		return streamSetupFailure(reasonMultistreamsOff,
			errors.New(errors.ERR_INVALID_ARGUMENT, "svp2p: multistreams disabled"))
	}

	// net_processing.cpp:1547-1551: a raw stream type at or above
	// MAX_STREAM_TYPE is out of range. UNKNOWN names no stream either.
	if cs.StreamType == wire.StreamTypeUnknown || cs.StreamType > wire.StreamTypeData4 {
		return streamSetupFailure(reasonStreamTypeRange,
			errors.New(errors.ERR_INVALID_ARGUMENT, "svp2p: stream type %d out of range", cs.StreamType))
	}

	// This port carries GENERAL plus DATA1 only (association.cpp:43); GENERAL
	// is the stream the association was built on and can never be moved in.
	if cs.StreamType != wire.StreamTypeData1 {
		return streamSetupFailure(reasonStreamTypeRange,
			errors.New(errors.ERR_INVALID_ARGUMENT, "svp2p: unsupported stream type %d", cs.StreamType))
	}

	a := m.associationByID(cs.AssociationID)
	if a == nil {
		return streamSetupFailure(reasonNoSuchNode,
			errors.New(errors.ERR_NOT_FOUND, "svp2p: no node found with association id %x", cs.AssociationID))
	}

	// net.cpp:3220-3230: moving a stream between two different endpoints is a
	// DoS attempt, so the source is banned as well as refused.
	from := hostOf(nc.RemoteAddr())

	if !sameHost(nc.RemoteAddr(), a.RemoteAddr()) {
		to := hostOf(a.RemoteAddr())

		if err := m.banList.Add(from, time.Now().Add(misbehavingBanDuration)); err != nil {
			m.logger.Errorf("[svp2p] cannot ban %s: %v", from, err)
		}

		return streamSetupFailure(reasonOtherAddress,
			errors.New(errors.ERR_NETWORK_PEER_MALICIOUS, "svp2p: attempt to move stream between peers with different IPs: %s != %s", from, to))
	}

	// net.cpp:3233-3236: an empty policy name leaves the association's
	// current policy alone; a named one must exist. SVNode reaches
	// ReplaceStreamPolicy only here, AFTER the lookup at net.cpp:3192-3208 and
	// after the different-IP ban at net.cpp:3218-3232, so a createstream that
	// is wrong on both counts is banned rather than merely refused.
	var policy transport.StreamPolicy

	if cs.StreamPolicyName != "" {
		found, ok := transport.PolicyForName(cs.StreamPolicyName)
		if !ok {
			return streamSetupFailure(reasonUnknownPolicy,
				errors.New(errors.ERR_INVALID_ARGUMENT, "svp2p: unknown stream policy %q", cs.StreamPolicyName))
		}

		policy = found
	}

	// association.cpp:150-153, checked before the socket becomes a stream:
	// Attach closes the Conn it refuses, which would take the socket the
	// reject below still has to travel over.
	if a.HasStream(cs.StreamType) {
		return streamSetupFailure(reasonStreamExists,
			errors.New(errors.ERR_NETWORK_PEER_MALICIOUS, "svp2p: stream type %d already attached to association %x", cs.StreamType, cs.AssociationID))
	}

	stream := transport.New(nc, transport.Config{
		Net:             m.tSettings.ChainCfgParams.Net,
		ProtocolVersion: wire.ProtocolVersion,
		SendBudgetBytes: sendBudgetBytes,
		RecvQueueLen:    recvQueueLen,
		WriteTimeout:    writeTimeout,
		MaxBlockPayload: m.maxBlockPayload,
		Logger:          m.logger,
		StreamType:      cs.StreamType,
	})

	// Attach BEFORE SetPolicy: a refused Attach closes the stream it refused,
	// and the association must not be left carrying a policy for a stream that
	// never joined it.
	if err := a.Attach(stream); err != nil {
		return streamSetupFailure(attachReason(err), err)
	}

	if policy != nil {
		a.SetPolicy(policy)
	}

	m.logger.Infof("[svp2p] stream %d for association %x moved from %s into its association", cs.StreamType, cs.AssociationID, nc.RemoteAddr())

	// net_processing.cpp:1569-1571: the ack goes out ON THE NEW STREAM, not
	// on the association's general stream. The socket now belongs to that
	// stream, so a failure here is the stream's to report and must NOT come
	// back as a setup failure — the reject would be written over a socket this
	// function no longer owns.
	if err := stream.SendPriority(wire.NewMsgStreamAck(cs.AssociationID, cs.StreamType)); err != nil {
		m.logger.Warnf("[svp2p] cannot ack stream %d for association %x: %v", cs.StreamType, cs.AssociationID, err)
	}

	return nil
}

// attachReason maps an Attach refusal to its wire reason. Attach has taken the
// socket by the time it refuses, so these travel over a connection that is
// already closing.
func attachReason(err error) string {
	if errors.Is(err, transport.ErrStreamExists) {
		return reasonStreamExists
	}

	return reasonNoSuchNode
}

// sameHost is net.cpp:3218-3222's fromAddr != toAddr: a CNetAddr compare, so
// it is the IP alone and not the port. It compares the parsed addresses rather
// than their text, so the same host reached once over IPv4 and once as an
// IPv4-mapped IPv6 address still counts as one host.
func sameHost(a, b net.Addr) bool {
	hostA, hostB := hostOf(a), hostOf(b)

	ipA, ipB := net.ParseIP(hostA), net.ParseIP(hostB)
	if ipA == nil || ipB == nil {
		return hostA == hostB
	}

	return ipA.Equal(ipB)
}

// hostOf is the IP half of an address.
func hostOf(addr net.Addr) string {
	if addr == nil {
		return ""
	}

	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}

	return host
}

// registerAssociation makes an established peer's association reachable by ID,
// which is what a later createstream looks it up in (net.cpp:3192-3202 walks
// vNodes for the same reason).
func (m *PeerManager) registerAssociation(a *transport.Association) {
	id := a.ID()
	if len(id) == 0 {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// net.cpp:3192-3202 breaks at the FIRST node carrying the ID, so a later
	// peer that claims an ID already in use does not take it over.
	if held, taken := m.associations[string(id)]; taken && held != a {
		m.logger.Warnf("[svp2p] peer %s claims association ID %x, which is already in use", a.RemoteAddr(), id)

		return
	}

	m.associations[string(id)] = a
}

// unregisterAssociation removes every key holding a, which is the whole of a
// association's registry presence however it was keyed.
func (m *PeerManager) unregisterAssociation(a *transport.Association) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key, held := range m.associations {
		if held == a {
			delete(m.associations, key)
		}
	}
}

// AssociationStreams reports the stream types every registered association
// holds, keyed by the hex of its association ID. It is a read of the registry
// net.cpp:3192-3202 walks, and it is what tells a multi-stream peer apart from
// a single-stream one from outside this package.
func (m *PeerManager) AssociationStreams() map[string][]wire.StreamType {
	m.mu.Lock()

	held := make(map[string]*transport.Association, len(m.associations))
	for key, a := range m.associations {
		held[key] = a
	}

	m.mu.Unlock()

	out := make(map[string][]wire.StreamType, len(held))
	for key, a := range held {
		out[hex.EncodeToString([]byte(key))] = a.Streams()
	}

	return out
}

func (m *PeerManager) associationByID(id []byte) *transport.Association {
	if len(id) == 0 {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	return m.associations[string(id)]
}

// ---------------------------------------------------------------------------
// Outbound: choosing the stream policy and opening DATA1.
// ---------------------------------------------------------------------------

// newStreamQueueLen bounds CConnman::mPendingStreams (net.h:357). SVNode's own
// deque is unbounded; a bound is what keeps a queue nothing is draining from
// growing without limit, and an overflowed entry costs one missing DATA1
// stream, never a disconnect.
const newStreamQueueLen = 64

// defaultProtoconfWait bounds the wait for the peer's protoconf before the
// policy choice gives up. SVNode has no such timer: it makes the choice inside
// ProcessProtoconfMessage, so the message has arrived by construction
// (net_processing.cpp:4413-4423). This port watches the peer from another
// goroutine and needs an upper bound instead.
const defaultProtoconfWait = 30 * time.Second

// protoconfPollInterval is how often that wait re-reads the peer. A protoconf
// follows its verack immediately, so the wait is over within a poll or two in
// every honest case, and a coarse interval costs nothing.
const protoconfPollInterval = 100 * time.Millisecond

// streamAckTimeout bounds the wait for the streamack answering our
// createstream. SVNode has no separate constant for it: its
// ThreadOpenNewStreamConnections (net.cpp:2112-2142) hands the socket to the
// same poll loop every other connection runs on, so a silent peer costs it
// nothing. This port drains the queue on ONE goroutine, so the bound is ours
// and it has to be short: a peer that accepts the connection and then says
// nothing must not hold the DATA1 stream of every peer behind it.
const streamAckTimeout = 5 * time.Second

var (
	// ErrNoProtoconf reports a peer that established and then never announced
	// its protocol configuration, so no stream policy could be chosen.
	ErrNoProtoconf = errors.New(errors.ERR_NETWORK_TIMEOUT, "svp2p: peer sent no protoconf")

	// ErrAssociationGone reports an association that ended before its policy
	// was chosen.
	ErrAssociationGone = errors.New(errors.ERR_ERROR, "svp2p: association closed before a stream policy was chosen")

	// ErrManagerStopping reports the node shutting down mid-choice.
	ErrManagerStopping = errors.New(errors.ERR_SERVICE_ERROR, "svp2p: node is stopping")
)

// pendingStream is one entry of CConnman::mPendingStreams (net.cpp:2105
// QueueNewStream): the peer to dial again, the association the new connection
// must join, the policy that asked for it, and the version that association
// negotiated.
type pendingStream struct {
	addr   string
	assoc  *transport.Association
	policy transport.StreamPolicy
	pver   uint32
}

// ourStreamPolicies is the prioritised list of policies this node is willing
// to use, which is also exactly what its own protoconf advertises
// (handshake.go ourVersion's reply, wire.NewMsgProtoconf). net.cpp:904-921
// SetSupportedStreamPolicies intersects the peer's list with ours, so a node
// that does not allow block priority must not offer it here either.
func (m *PeerManager) ourStreamPolicies() []string {
	if m.tSettings.Legacy.AllowBlockPriority {
		return transport.OurPolicyPriority
	}

	return []string{wire.DefaultStreamPolicy}
}

// setupStreams is association.cpp:111-129 OpenRequiredStreams driving
// stream_policy.cpp:132-137 BlockPriorityStreamPolicy::SetupStreams. Only the
// side that dialled opens further streams; the side that accepted waits to see
// what its peer wants to do.
func (m *PeerManager) setupStreams(ctx context.Context, peer *Peer, assoc *transport.Association, addr string) {
	info, err := m.awaitStreamPolicies(ctx, peer, assoc)
	if err != nil {
		m.logger.Debugf("[svp2p] no stream policy chosen for %s, keeping the default: %v", addr, err)

		return
	}

	// net.cpp:948-965 GetPreferredStreamPolicyName: the first of our
	// prioritised policies the peer also supports.
	policy := transport.PreferredPolicy(m.ourStreamPolicies(), info.TheirStreamPolicies)

	// association.cpp:124: the policy is fixed on the association before any
	// stream is asked for.
	assoc.SetPolicy(policy)

	if !policy.RequiresData1() {
		return
	}

	id := assoc.ID()
	if len(id) == 0 {
		// association.cpp:128-129: no association ID, no further streams.
		return
	}

	m.queueNewStream(pendingStream{addr: addr, assoc: assoc, policy: policy, pver: info.ProtocolVersion})
}

// awaitStreamPolicies waits for the peer's protoconf, which is the message
// that names its stream policies. SVNode chooses the policy at the END of
// ProcessProtoconfMessage (net_processing.cpp:4413-4423), not at the verack
// that establishes the peer, and protoconf follows verack on the wire. The
// watcher this runs in wakes on the verack, so it must wait for the protoconf
// here or it would read an empty policy list and always fall back to Default.
func (m *PeerManager) awaitStreamPolicies(ctx context.Context, peer *Peer, assoc *transport.Association) (PeerSnapshot, error) {
	deadline := time.NewTimer(m.protoconfWait)
	defer deadline.Stop()

	tick := time.NewTicker(protoconfPollInterval)
	defer tick.Stop()

	for {
		if info := peer.Info(); info.ProtoconfReceived {
			return info, nil
		}

		select {
		case <-tick.C:
		case <-deadline.C:
			return PeerSnapshot{}, ErrNoProtoconf
		case <-assoc.Done():
			return PeerSnapshot{}, ErrAssociationGone
		case <-m.quit:
			return PeerSnapshot{}, ErrManagerStopping
		case <-ctx.Done():
			return PeerSnapshot{}, ErrManagerStopping
		}
	}
}

// queueNewStream is net.cpp:2105-2110 CConnman::QueueNewStream. It must never
// block: SVNode appends under a lock it holds for that append alone, and this
// runs on the goroutine watching a peer establish.
func (m *PeerManager) queueNewStream(ps pendingStream) {
	select {
	case m.newStreams <- ps:
	default:
		m.logger.Warnf("[svp2p] new stream queue is full, dropping the DATA1 request for %s", ps.addr)
	}
}

// openNewStreamsLoop is net.cpp:2112-2142 ThreadOpenNewStreamConnections. A
// dial that fails is logged and nothing more (net.cpp:2129-2132).
func (m *PeerManager) openNewStreamsLoop(ctx context.Context) {
	for {
		select {
		case ps := <-m.newStreams:
			if err := m.openNewStreamConnection(ctx, ps); err != nil {
				m.logger.Infof("[svp2p] failed to open DATA1 stream to %s: %v", ps.addr, err)
			}

		case <-m.quit:
			return

		case <-ctx.Done():
			return
		}
	}
}

// openNewStreamConnection is net.cpp:2143 OpenNetworkConnection for a
// fNewStream entry: dial the peer again, send createstream as the FIRST
// message on the new socket (net_processing.cpp:204-206 PushCreateStream),
// wait for the streamack that confirms it, and only then make the socket a
// stream of the association.
//
// Every failure closes the new socket and returns. The peer's existing
// connection is never touched: net.cpp:2131 logs "Failed to open new stream
// connection" and carries on.
func (m *PeerManager) openNewStreamConnection(ctx context.Context, ps pendingStream) error {
	id := ps.assoc.ID()
	if len(id) == 0 {
		return errors.New(errors.ERR_INVALID_ARGUMENT, "svp2p: association has no id")
	}

	// Context-aware, unlike the dial the peer loops make: this one runs on the
	// single goroutine draining the queue, so a dial that cannot be abandoned
	// would hold Stop and every DATA1 request behind it.
	nc, err := m.dialTCPContext(ctx, ps.addr)
	if err != nil {
		return errors.New(errors.ERR_NETWORK_ERROR, "svp2p: cannot dial %s for a new stream", ps.addr, err)
	}

	ack, err := m.requestStream(ctx, nc, id, ps.policy)
	if err != nil {
		_ = nc.Close()

		return err
	}

	stream := transport.New(nc, transport.Config{
		Net:             m.tSettings.ChainCfgParams.Net,
		ProtocolVersion: ps.pver,
		SendBudgetBytes: sendBudgetBytes,
		RecvQueueLen:    recvQueueLen,
		WriteTimeout:    writeTimeout,
		MaxBlockPayload: m.maxBlockPayload,
		Logger:          m.logger,
		StreamType:      ack.StreamType,
	})

	// The association is already running, so Attach starts the stream. Attach
	// closes the Conn it refuses, which closes nc with it.
	if err := ps.assoc.Attach(stream); err != nil {
		return errors.New(errors.ERR_NETWORK_ERROR, "svp2p: cannot attach the new stream to association %x", id, err)
	}

	m.logger.Infof("[svp2p] stream %d for association %x opened to %s under policy %s", ack.StreamType, id, ps.addr, ps.policy.Name())

	return nil
}

// requestStream performs the createstream/streamack exchange on a socket that
// has no transport.Conn behind it yet. The reply has to be read here, with a
// deadline, because a Conn started before the ack would consume it into its
// own inbound queue where nothing is waiting for it.
func (m *PeerManager) requestStream(ctx context.Context, nc net.Conn, id []byte, policy transport.StreamPolicy) (*wire.MsgStreamAck, error) {
	magic := m.tSettings.ChainCfgParams.Net

	if err := nc.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return nil, errors.New(errors.ERR_NETWORK_ERROR, "svp2p: cannot set the createstream deadline", err)
	}

	// net_processing.cpp:180-192 PushCreateStream: createstream is the first
	// thing on the wire, before any version.
	cs := wire.NewMsgCreateStream(id, wire.StreamTypeData1, policy.Name())

	if err := wire.WriteMessage(nc, cs, wire.ProtocolVersion, magic); err != nil {
		return nil, errors.New(errors.ERR_NETWORK_ERROR, "svp2p: cannot send createstream", err)
	}

	if err := nc.SetWriteDeadline(time.Time{}); err != nil {
		return nil, errors.New(errors.ERR_NETWORK_ERROR, "svp2p: cannot clear the createstream deadline", err)
	}

	// The read below would otherwise hold Stop for the whole first message
	// timeout, exactly as the inbound classifier's does.
	read := make(chan struct{})

	go func() {
		select {
		case <-read:
		case <-m.quit:
			_ = nc.SetReadDeadline(time.Now())
		case <-ctx.Done():
			_ = nc.SetReadDeadline(time.Now())
		}
	}()

	msg, _, err := readFirstMessage(nc, magic, streamAckTimeout, streamAckLimits())

	close(read)

	if err != nil {
		return nil, err
	}

	// net_processing.cpp:1593-1610 ProcessStreamAckMessage: anything other
	// than a streamack naming the association and stream we asked for ends the
	// attempt. A reject arrives here too, which is what a refused setup looks
	// like from this side.
	ack, ok := msg.(*wire.MsgStreamAck)
	if !ok {
		return nil, errors.New(errors.ERR_NETWORK_INVALID_RESPONSE, "svp2p: %s answered createstream, not streamack", msg.Command())
	}

	if !bytes.Equal(ack.AssociationID, id) {
		return nil, errors.New(errors.ERR_NETWORK_INVALID_RESPONSE, "svp2p: streamack names association %x, not %x", ack.AssociationID, id)
	}

	if ack.StreamType != cs.StreamType {
		return nil, errors.New(errors.ERR_NETWORK_INVALID_RESPONSE, "svp2p: streamack names stream type %d, not %d", ack.StreamType, cs.StreamType)
	}

	return ack, nil
}
