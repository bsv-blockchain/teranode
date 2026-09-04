package protocol

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/svp2p/svp2ptest"
	"github.com/bsv-blockchain/teranode/services/svp2p/transport"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// startedManager builds a manager listening on a dual-stack ephemeral port and
// starts it. The listener is dual-stack so a test can reach the same node over
// two different remote IPs (127.0.0.1 and ::1) without an interface alias.
func startedManager(t *testing.T) *PeerManager {
	t.Helper()

	return startedManagerWith(t, nil, nil)
}

func startedManagerWith(t *testing.T, tweakSettings func(*settings.Settings), tweakManager func(*PeerManager)) *PeerManager {
	t.Helper()

	tSettings := managerSettings()
	if tweakSettings != nil {
		tweakSettings(tSettings)
	}

	banList, err := NewBanList("")
	require.NoError(t, err)

	m := NewPeerManager(ulogger.TestLogger{}, tSettings, banList)

	if tweakManager != nil {
		tweakManager(m)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	require.NoError(t, m.Start(ctx, []string{":0"}))
	t.Cleanup(func() { require.NoError(t, m.Stop()) })

	return m
}

// nodeAddr rebuilds the manager's listen address with an explicit host, so a
// test can choose which local address it reaches the node from.
func nodeAddr(t *testing.T, m *PeerManager, host string) string {
	t.Helper()

	addrs := m.ListenAddrs()
	require.Len(t, addrs, 1)

	_, port, err := net.SplitHostPort(addrs[0])
	require.NoError(t, err)

	return net.JoinHostPort(host, port)
}

func dialRaw(t *testing.T, addr string) net.Conn {
	t.Helper()

	nc, err := net.Dial("tcp", addr)
	require.NoError(t, err)

	t.Cleanup(func() { _ = nc.Close() })

	return nc
}

func writeMsg(t *testing.T, nc net.Conn, msg wire.Message) {
	t.Helper()

	require.NoError(t, nc.SetWriteDeadline(time.Now().Add(5*time.Second)))

	_, err := wire.WriteMessageWithEncodingN(nc, msg, wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
	require.NoError(t, err)
}

func readMsg(t *testing.T, nc net.Conn, timeout time.Duration) wire.Message {
	t.Helper()

	require.NoError(t, nc.SetReadDeadline(time.Now().Add(timeout)))

	_, msg, _, err := wire.ReadMessageWithEncodingN(nc, wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
	require.NoError(t, err)

	return msg
}

// requireEOF asserts the node ended the connection.
func requireEOF(t *testing.T, nc net.Conn, timeout time.Duration) {
	t.Helper()

	require.NoError(t, nc.SetReadDeadline(time.Now().Add(timeout)))

	buf := make([]byte, 1)
	_, err := nc.Read(buf)
	require.Error(t, err)
	require.False(t, isTimeout(err), "the node did not close the connection: %v", err)
}

func isTimeout(err error) bool {
	var ne net.Error

	return errors.As(err, &ne) && ne.Timeout()
}

// completeOutboundHandshakeWithAssociationID is completeOutboundHandshake with
// an association ID in the peer's version message, which is what an inbound
// peer uses to name the association its later streams join
// (net_processing.cpp:1775 SetAssociationID).
func (s *scriptedPeer) completeOutboundHandshakeWithAssociationID(t *testing.T, id []byte) {
	t.Helper()

	version := remoteVersion(4321)
	version.AssociationID = id

	s.completeOutboundHandshakeAs(t, version)
}

// establishAssociation connects a scripted peer that names id, and waits for
// the manager to register the association under it.
func establishAssociation(t *testing.T, m *PeerManager, id []byte) *scriptedPeer {
	t.Helper()

	far := dialScripted(t, nodeAddr(t, m, "127.0.0.1"))

	// The cleanup closure is also what keeps far reachable for the whole
	// test: nothing else holds the connection, and the runtime finalizer on
	// an unreachable socket would close it and end the association early.
	t.Cleanup(func() { _ = far.nc.Close() })

	far.completeOutboundHandshakeWithAssociationID(t, id)

	require.Eventually(t, func() bool { return m.associationByID(id) != nil }, 5*time.Second, 20*time.Millisecond)

	return far
}

// net_processing.cpp:1514-1590 ProcessCreateStreamMessage + net.cpp:3188
// MoveStream: a createstream naming a live association from the SAME IP
// attaches and is acked ON THE NEW STREAM; the husk runs no Peer.
func TestInbound_CreateStreamAttachesAndAcksOnNewStream(t *testing.T) {
	m := startedManager(t)

	id := testAssociationID(9)
	_ = establishAssociation(t, m, id)

	raw := dialRaw(t, nodeAddr(t, m, "127.0.0.1"))
	writeMsg(t, raw, &wire.MsgCreateStream{
		AssociationID:    id,
		StreamType:       wire.StreamTypeData1,
		StreamPolicyName: wire.BlockPriorityStreamPolicy,
	})

	ack, ok := readMsg(t, raw, 5*time.Second).(*wire.MsgStreamAck)
	require.True(t, ok, "the node must answer createstream with streamack")
	require.Equal(t, id, ack.AssociationID)
	require.Equal(t, wire.StreamTypeData1, ack.StreamType)

	a := m.associationByID(id)
	require.NotNil(t, a)
	require.True(t, a.HasStream(wire.StreamTypeData1))
	require.Equal(t, wire.BlockPriorityStreamPolicy, a.Policy().Name())
	require.Equal(t, 1, len(m.peerHandles()), "the husk connection must not become a Peer")
}

// net.cpp:3220-3230: a createstream from a different IP is a DoS attempt →
// ban + disconnect.
func TestInbound_CreateStreamFromOtherIPIsBanned(t *testing.T) {
	m := startedManager(t)

	id := testAssociationID(8)
	_ = establishAssociation(t, m, id)

	otherHost, raw := dialFromOtherIP(t, m)

	writeMsg(t, raw, &wire.MsgCreateStream{
		AssociationID:    id,
		StreamType:       wire.StreamTypeData1,
		StreamPolicyName: wire.BlockPriorityStreamPolicy,
	})

	rej, ok := readMsg(t, raw, 5*time.Second).(*wire.MsgReject)
	require.True(t, ok, "a refused createstream must be rejected before the disconnect")
	require.Equal(t, rejectStreamSetup, rej.Code)

	// net.cpp:3227-3229 names both endpoints in its own message; ours must
	// not, or the attacker learns the victim's address.
	require.Equal(t, reasonOtherAddress, rej.Reason)
	require.False(t, strings.Contains(rej.Reason, "127.0.0.1"), "the reject must not echo the association's address: %q", rej.Reason)

	requireEOF(t, raw, 5*time.Second)

	require.True(t, m.banList.IsBanned(otherHost), "the misbehaving IP must be banned")
	require.False(t, m.associationByID(id).HasStream(wire.StreamTypeData1))
}

// dialFromOtherIP reaches the node from a local address that is not the
// 127.0.0.1 the association was established from. It prefers the brief's
// 127.0.0.2 source address and falls back to the ::1 side of the dual-stack
// listener when the OS refuses to bind it (macOS without an lo0 alias).
func dialFromOtherIP(t *testing.T, m *PeerManager) (string, net.Conn) {
	t.Helper()

	dialer := net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.2")}}

	nc, err := dialer.Dial("tcp", nodeAddr(t, m, "127.0.0.1"))
	if err == nil {
		t.Cleanup(func() { _ = nc.Close() })

		return "127.0.0.2", nc
	}

	t.Logf("127.0.0.2 source address unavailable, falling back to ::1: %v", err)

	nc, err = net.Dial("tcp", nodeAddr(t, m, "::1"))
	if err != nil {
		t.Skipf("no second local address available for the different-IP test: %v", err)
	}

	t.Cleanup(func() { _ = nc.Close() })

	return "::1", nc
}

// Unknown ID, duplicate type, unknown policy and an out-of-range stream type
// all fail stream setup: REJECT_STREAM_SETUP (0x60) then disconnect
// (net_processing.cpp:1577-1584).
func TestInbound_CreateStreamSetupFailures(t *testing.T) {
	m := startedManager(t)

	id := testAssociationID(7)
	_ = establishAssociation(t, m, id)

	// The duplicate case needs a DATA1 stream already in place, so attach one
	// first and keep it open for the rest of the test.
	good := dialRaw(t, nodeAddr(t, m, "127.0.0.1"))
	writeMsg(t, good, &wire.MsgCreateStream{
		AssociationID:    id,
		StreamType:       wire.StreamTypeData1,
		StreamPolicyName: wire.BlockPriorityStreamPolicy,
	})
	require.IsType(t, &wire.MsgStreamAck{}, readMsg(t, good, 5*time.Second))

	tests := []struct {
		name   string
		msg    *wire.MsgCreateStream
		reason string
	}{
		{
			name:   "unknown association id",
			msg:    &wire.MsgCreateStream{AssociationID: testAssociationID(11), StreamType: wire.StreamTypeData1, StreamPolicyName: wire.BlockPriorityStreamPolicy},
			reason: reasonNoSuchNode,
		},
		{
			name:   "duplicate data1",
			msg:    &wire.MsgCreateStream{AssociationID: id, StreamType: wire.StreamTypeData1, StreamPolicyName: wire.BlockPriorityStreamPolicy},
			reason: reasonStreamExists,
		},
		{
			name:   "unknown policy",
			msg:    &wire.MsgCreateStream{AssociationID: id, StreamType: wire.StreamTypeData1, StreamPolicyName: "Nope"},
			reason: reasonUnknownPolicy,
		},
		{
			name:   "unknown stream type",
			msg:    &wire.MsgCreateStream{AssociationID: id, StreamType: wire.StreamTypeUnknown, StreamPolicyName: wire.BlockPriorityStreamPolicy},
			reason: reasonStreamTypeRange,
		},
		{
			name:   "stream type out of range",
			msg:    &wire.MsgCreateStream{AssociationID: id, StreamType: wire.StreamType(9), StreamPolicyName: wire.BlockPriorityStreamPolicy},
			reason: reasonStreamTypeRange,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw := dialRaw(t, nodeAddr(t, m, "127.0.0.1"))
			writeMsg(t, raw, test.msg)

			rej, ok := readMsg(t, raw, 5*time.Second).(*wire.MsgReject)
			require.True(t, ok, "stream setup failure must answer with reject")
			require.Equal(t, rejectStreamSetup, rej.Code)
			require.Equal(t, wire.CmdCreateStream, rej.Cmd)
			// The reason is a plain SVNode string: no teranode error code, no
			// package prefix, and inside MAX_REJECT_MESSAGE_LENGTH
			// (validation.h:155).
			require.Equal(t, test.reason, rej.Reason)
			require.LessOrEqual(t, len(rej.Reason), 111)

			requireEOF(t, raw, 5*time.Second)
		})
	}

	require.Equal(t, 1, len(m.peerHandles()), "no husk may become a Peer")
}

// net_processing.cpp:1598-1604: streamack with nothing pending → reject +
// disconnect.
func TestInbound_UnsolicitedStreamAckIsRejected(t *testing.T) {
	m := startedManager(t)

	raw := dialRaw(t, nodeAddr(t, m, "127.0.0.1"))
	writeMsg(t, raw, &wire.MsgStreamAck{AssociationID: testAssociationID(5), StreamType: wire.StreamTypeData1})

	rej, ok := readMsg(t, raw, 5*time.Second).(*wire.MsgReject)
	require.True(t, ok, "an unsolicited streamack must be rejected")
	require.Equal(t, wire.RejectNonstandard, rej.Code)
	require.Equal(t, wire.CmdStreamAck, rej.Cmd)

	requireEOF(t, raw, 5*time.Second)
	require.Equal(t, 0, len(m.peerHandles()))
}

// net_processing.cpp:4709: the first message must be version or createstream;
// anything else disconnects.
func TestInbound_OtherFirstMessageDisconnects(t *testing.T) {
	m := startedManager(t)

	raw := dialRaw(t, nodeAddr(t, m, "127.0.0.1"))
	writeMsg(t, raw, wire.NewMsgPing(42))

	requireEOF(t, raw, 5*time.Second)
	require.Equal(t, 0, len(m.peerHandles()))
}

// With multistreams off a createstream is refused with REJECT_STREAM_SETUP.
// The peer establishes with an association ID and is NOT registered under it
// (net_processing.cpp:209-211), so the refusal has to come from the setting
// itself rather than from the missing association.
func TestInbound_CreateStreamRefusedWhenBlockPriorityOff(t *testing.T) {
	m := startedManagerWith(t, func(s *settings.Settings) { s.Legacy.AllowBlockPriority = false }, nil)

	id := testAssociationID(6)

	far := dialScripted(t, nodeAddr(t, m, "127.0.0.1"))
	t.Cleanup(func() { _ = far.nc.Close() })

	far.completeOutboundHandshakeWithAssociationID(t, id)

	require.Eventually(t, func() bool { return establishedCount(m) == 1 }, 10*time.Second, 50*time.Millisecond)

	raw := dialRaw(t, nodeAddr(t, m, "127.0.0.1"))
	writeMsg(t, raw, &wire.MsgCreateStream{
		AssociationID:    id,
		StreamType:       wire.StreamTypeData1,
		StreamPolicyName: wire.BlockPriorityStreamPolicy,
	})

	rej, ok := readMsg(t, raw, 5*time.Second).(*wire.MsgReject)
	require.True(t, ok, "createstream must be rejected when multistreams are off")
	require.Equal(t, rejectStreamSetup, rej.Code)
	require.Equal(t, reasonMultistreamsOff, rej.Reason)

	requireEOF(t, raw, 5*time.Second)
	require.Equal(t, 0, associationCount(m))
}

// A husk that sends nothing is dropped at the first-message timeout.
func TestInbound_SilentConnectionTimesOut(t *testing.T) {
	m := startedManagerWith(t, nil, func(m *PeerManager) { m.firstMessageTimeout = 200 * time.Millisecond })

	raw := dialRaw(t, nodeAddr(t, m, "127.0.0.1"))

	requireEOF(t, raw, 5*time.Second)
	require.Equal(t, 0, len(m.peerHandles()))
}

// An established peer's association leaves the registry when it disconnects,
// so a later createstream naming it cannot attach to a dead association.
func TestInbound_AssociationUnregistersOnDisconnect(t *testing.T) {
	m := startedManager(t)

	id := testAssociationID(4)
	far := establishAssociation(t, m, id)

	require.NoError(t, far.nc.Close())

	require.Eventually(t, func() bool { return m.associationByID(id) == nil }, 5*time.Second, 20*time.Millisecond)
}

// associationCount reads the registry under the lock that guards it, so a test
// assertion never races the manager's own goroutines.
func associationCount(m *PeerManager) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.associations)
}

// rawHeader hand-frames a 24-byte message header with no payload behind it, so
// a test can present framing the typed messages cannot build.
func rawHeader(command string, length uint32) []byte {
	hdr := make([]byte, wire.MessageHeaderSize)
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(wire.MainNet))
	copy(hdr[4:4+wire.CommandSize], command)
	binary.LittleEndian.PutUint32(hdr[16:20], length)

	return hdr
}

func writeRawHeader(t *testing.T, nc net.Conn, command string, length uint32) {
	t.Helper()

	require.NoError(t, nc.SetWriteDeadline(time.Now().Add(5*time.Second)))

	_, err := nc.Write(rawHeader(command, length))
	require.NoError(t, err)
}

// protocol.cpp:220-237 only frames a payload with the extended header once it
// exceeds uint32 max, which no first message ever does. An extmsg header as
// the first message is refused before a single payload byte is read.
func TestInbound_ExtendedFirstMessageDisconnects(t *testing.T) {
	m := startedManager(t)

	raw := dialRaw(t, nodeAddr(t, m, "127.0.0.1"))
	writeRawHeader(t, raw, wire.CmdExtMsg, 0xffffffff)

	requireEOF(t, raw, 5*time.Second)
	require.Equal(t, 0, len(m.peerHandles()))
	require.Equal(t, 0, associationCount(m))
}

// A first message that declares more payload than we ever advertise we will
// receive (net_processing.cpp:3306 maxRecvPayloadLength) is refused on the
// header alone, so nothing allocates the payload it claims.
func TestInbound_OverLongFirstMessageDisconnects(t *testing.T) {
	m := startedManager(t)

	raw := dialRaw(t, nodeAddr(t, m, "127.0.0.1"))
	writeRawHeader(t, raw, wire.CmdVersion, wire.DefaultMaxRecvPayloadLength+1)

	requireEOF(t, raw, 5*time.Second)
	require.Equal(t, 0, len(m.peerHandles()))
	require.Equal(t, 0, associationCount(m))
}

// ---------------------------------------------------------------------------
// Outbound: choosing the stream policy and opening DATA1.
// ---------------------------------------------------------------------------

// fixtureChainSettings carries the regtest parameters BuildFixtureChain mines
// against. The manager under test speaks MainNet magic (managerSettings), and
// the scripted peer is given that magic separately, so the chain's own
// parameters only ever decide its proof of work.
func fixtureChainSettings() *settings.Settings {
	return &settings.Settings{ChainCfgParams: &chaincfg.RegressionNetParams}
}

// outboundManager starts a manager whose only configured peer is addr, so the
// legacy_connect_peers dialer makes it an OUTBOUND connection.
func outboundManager(t *testing.T, addr string, tweak func(*settings.Settings)) *PeerManager {
	t.Helper()

	return startedManagerWith(t, func(s *settings.Settings) {
		s.Legacy.ConnectPeers = []string{addr}

		if tweak != nil {
			tweak(s)
		}
	}, nil)
}

func scriptedListener(t *testing.T, script svp2ptest.Script) *svp2ptest.ScriptedPeer {
	t.Helper()

	chain := svp2ptest.BuildFixtureChain(t, fixtureChainSettings(), 2)

	peer := svp2ptest.NewScriptedPeer(t, chain, managerSettings().ChainCfgParams.Net, script, false)
	peer.Listen()

	return peer
}

// firstIn is the first message of the given command the peer received.
func firstIn(t *testing.T, tr *svp2ptest.Transcript, cmd string) svp2ptest.Entry {
	t.Helper()

	entry, ok := tr.FirstOn(svp2ptest.In, cmd)
	require.True(t, ok, "the peer never received a %s", cmd)

	return entry
}

// association.cpp:111-129 OpenRequiredStreams + stream_policy.cpp:132-137
// BlockPriorityStreamPolicy::SetupStreams: after an OUTBOUND handshake whose
// protoconf lists BlockPriority, the node dials the peer a second time, sends
// createstream FIRST, and attaches the connection as DATA1 on the streamack.
func TestOutbound_OpensData1AfterBlockPriorityHandshake(t *testing.T) {
	peer := scriptedListener(t, svp2ptest.Script{})

	// The node's own ping is what proves the policy is in force on the wire.
	// effectivePingInterval halves an idle timeout below 2*pingInterval, so a
	// 6 s idle window gives a 3 s ping cadence; the scripted peer answers
	// every ping with a pong, which keeps the idle timer from firing.
	m := outboundManager(t, peer.Addr, func(s *settings.Settings) {
		s.Legacy.PeerIdleTimeout = 6 * time.Second
	})

	require.Eventually(t, func() bool { return peer.Connections() == 2 }, 10*time.Second, 50*time.Millisecond,
		"the node must dial the peer a second time for DATA1")

	require.Equal(t, 1, peer.Transcript.Count(svp2ptest.In, wire.CmdCreateStream))

	entry := firstIn(t, peer.Transcript, wire.CmdCreateStream)

	cs, ok := entry.Msg.(*wire.MsgCreateStream)
	require.True(t, ok)
	require.Equal(t, wire.StreamTypeData1, cs.StreamType)
	require.Equal(t, wire.BlockPriorityStreamPolicy, cs.StreamPolicyName)

	// net_processing.cpp:180-192 PushCreateStream: createstream is the FIRST
	// message on the new connection, before any version.
	require.Equal(t, 0, peer.Transcript.CountOn(entry.Conn, svp2ptest.In, wire.CmdVersion))

	require.Eventually(t, func() bool {
		a := m.associationByID(cs.AssociationID)

		return a != nil && a.HasStream(wire.StreamTypeData1)
	}, 5*time.Second, 20*time.Millisecond)

	a := m.associationByID(cs.AssociationID)
	require.NotNil(t, a)
	require.Equal(t, wire.BlockPriorityStreamPolicy, a.Policy().Name())

	data1 := entry.Conn

	general := generalConn(t, peer, data1)

	// stream_policy.cpp:187-195: a ping is a high priority message, so every
	// ping the node sends AFTER the attach travels on DATA1. Pings sent
	// before it had only GENERAL to travel on, which is why the GENERAL count
	// is compared against its value at the attach rather than against zero.
	baseline := peer.Transcript.CountOn(general, svp2ptest.In, wire.CmdPing)

	require.Eventually(t, func() bool { return peer.Transcript.CountOn(data1, svp2ptest.In, wire.CmdPing) > 0 },
		15*time.Second, 50*time.Millisecond, "a ping must travel on DATA1 once it is attached")

	require.Equal(t, baseline, peer.Transcript.CountOn(general, svp2ptest.In, wire.CmdPing),
		"no ping may travel on GENERAL once DATA1 is attached")
}

// generalConn is the peer's connection that is NOT the DATA1 one.
func generalConn(t *testing.T, peer *svp2ptest.ScriptedPeer, data1 net.Conn) net.Conn {
	t.Helper()

	for _, c := range peer.Conns() {
		if c != data1 {
			return c
		}
	}

	require.FailNow(t, "the peer holds no GENERAL connection")

	return nil
}

// net.cpp:948-965 GetPreferredStreamPolicyName: a peer that offers only
// Default leaves Default as the common policy, and stream_policy.h:104
// DefaultStreamPolicy::SetupStreams opens nothing.
func TestOutbound_NoData1WithoutBlockPriority(t *testing.T) {
	peer := scriptedListener(t, svp2ptest.Script{StreamPolicies: []string{wire.DefaultStreamPolicy}})

	m := outboundManager(t, peer.Addr, nil)

	require.Eventually(t, func() bool { return establishedCount(m) == 1 }, 10*time.Second, 50*time.Millisecond)

	require.Never(t, func() bool { return peer.Connections() != 1 }, 3*time.Second, 100*time.Millisecond,
		"a Default-only peer must not be dialed a second time")

	require.Equal(t, 0, peer.Transcript.Count(svp2ptest.In, wire.CmdCreateStream))
}

// net_processing.cpp:209-211: the association ID is created only when
// multistreams are enabled, so with legacy_allowBlockPriority off the peer
// never learns one and no second stream is ever possible.
func TestOutbound_NoStreamsWhenBlockPriorityOff(t *testing.T) {
	peer := scriptedListener(t, svp2ptest.Script{})

	m := outboundManager(t, peer.Addr, func(s *settings.Settings) {
		s.Legacy.AllowBlockPriority = false
	})

	require.Eventually(t, func() bool { return establishedCount(m) == 1 }, 10*time.Second, 50*time.Millisecond)

	version, ok := firstIn(t, peer.Transcript, wire.CmdVersion).Msg.(*wire.MsgVersion)
	require.True(t, ok)
	require.Empty(t, version.AssociationID, "a node with multistreams off must not name an association")

	require.Never(t, func() bool { return peer.Connections() != 1 }, 3*time.Second, 100*time.Millisecond)

	require.Equal(t, 0, peer.Transcript.Count(svp2ptest.In, wire.CmdCreateStream))
	require.Equal(t, 0, associationCount(m))
}

// net.cpp:2129-2132: a second dial that fails is logged and nothing more. The
// peer stays connected on its single GENERAL stream.
func TestOutbound_Data1DialFailureIsNotFatal(t *testing.T) {
	peer := scriptedListener(t, svp2ptest.Script{
		OnCreateStream: func(_ *svp2ptest.ScriptedPeer, conn net.Conn, _ *wire.MsgCreateStream) []wire.Message {
			_ = conn.Close()

			return nil
		},
	})

	m := outboundManager(t, peer.Addr, nil)

	require.Eventually(t, func() bool { return peer.Transcript.Count(svp2ptest.In, wire.CmdCreateStream) == 1 },
		10*time.Second, 50*time.Millisecond)

	cs, ok := firstIn(t, peer.Transcript, wire.CmdCreateStream).Msg.(*wire.MsgCreateStream)
	require.True(t, ok)

	require.Never(t, func() bool { return establishedCount(m) != 1 }, 2*time.Second, 100*time.Millisecond,
		"a refused DATA1 dial must not disconnect the peer")

	a := m.associationByID(cs.AssociationID)
	require.NotNil(t, a)
	require.False(t, a.HasStream(wire.StreamTypeData1))
}

// net.cpp:2112-2142 ThreadOpenNewStreamConnections drains its queue on ONE
// thread, so a peer that accepts the second connection and then never acks
// must be given up on quickly (streamAckTimeout). Otherwise every DATA1
// request queued behind it waits out that peer's silence.
func TestOutbound_SilentStreamAckDoesNotStallTheQueue(t *testing.T) {
	silent := scriptedListener(t, svp2ptest.Script{
		// Accept the connection, answer nothing, and keep it open.
		OnCreateStream: func(_ *svp2ptest.ScriptedPeer, _ net.Conn, _ *wire.MsgCreateStream) []wire.Message {
			return nil
		},
	})

	// The honest peer holds its protoconf back, which is what puts the silent
	// peer in the queue first: the request is made when protoconf names the
	// policy. Without this the queue order would be a race.
	honest := scriptedListener(t, svp2ptest.Script{
		WriteDelay: func(msg wire.Message, _ int) time.Duration {
			if msg.Command() == wire.CmdProtoconf {
				return 1500 * time.Millisecond
			}

			return 0
		},
	})

	start := time.Now()

	m := startedManagerWith(t, func(s *settings.Settings) {
		s.Legacy.ConnectPeers = []string{silent.Addr, honest.Addr}
	}, nil)

	require.Eventually(t, func() bool { return silent.Transcript.Count(svp2ptest.In, wire.CmdCreateStream) == 1 },
		5*time.Second, 50*time.Millisecond, "the silent peer was never asked for DATA1")

	require.Eventually(t, func() bool { return hasData1(m, honest) }, 10*time.Second, 50*time.Millisecond,
		"a peer that never acks must not stall the DATA1 request behind it")

	require.Less(t, time.Since(start), 12*time.Second,
		"the honest peer waited out the silent peer instead of the streamack bound")

	// The silent peer keeps its single GENERAL stream: net.cpp:2129-2132 logs
	// the failure and nothing else.
	require.False(t, hasData1(m, silent))
	require.Equal(t, 2, establishedCount(m))
}

// hasData1 reports whether the association the peer was asked to join now
// holds a DATA1 stream.
func hasData1(m *PeerManager, peer *svp2ptest.ScriptedPeer) bool {
	entry, ok := peer.Transcript.FirstOn(svp2ptest.In, wire.CmdCreateStream)
	if !ok {
		return false
	}

	cs, ok := entry.Msg.(*wire.MsgCreateStream)
	if !ok {
		return false
	}

	a := m.associationByID(cs.AssociationID)

	return a != nil && a.HasStream(wire.StreamTypeData1)
}

// The stream dialer drains its queue on one goroutine and Stop waits on that
// goroutine (net.cpp:2112-2142 runs the same loop, but on a socket set its own
// shutdown breaks). A dial this port cannot abandon would hold Stop for the
// whole dial timeout.
func TestOutbound_StreamDialIsAbandonedOnStop(t *testing.T) {
	entered := make(chan struct{}, 1)
	held := make(chan struct{})

	m := startedManagerWith(t, nil, func(m *PeerManager) {
		m.dialTCP = func(_ string) (net.Conn, error) {
			entered <- struct{}{}
			<-held

			return nil, errors.New(errors.ERR_NETWORK_ERROR, "svp2p: test dial released")
		}
	})

	defer close(held)

	dialed := make(chan error, 1)

	go func() {
		_, err := m.dialTCPContext(context.Background(), "203.0.113.1:8333")
		dialed <- err
	}()

	<-entered

	stopped := make(chan struct{})

	go func() {
		_ = m.Stop()

		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "Stop was held by a dial still in flight")
	}

	select {
	case err := <-dialed:
		require.Error(t, err, "an abandoned dial must report why it gave up")
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the abandoned dial never returned")
	}
}

// net_processing.cpp:1758-1776 then :1816-1818: a peer that dials this node
// and names an association must get that same name back in the node's version
// reply, or it never sends createstream and the association never gets a
// DATA1 stream.
func TestInbound_VersionReplyEchoesTheDialersAssociationID(t *testing.T) {
	m := startedManager(t)

	far := dialScripted(t, nodeAddr(t, m, "127.0.0.1"))
	t.Cleanup(func() { _ = far.nc.Close() })

	id := testAssociationID(7)
	version := remoteVersion(4321)
	version.AssociationID = id

	far.write(t, version)

	var ours *wire.MsgVersion

	for ours == nil {
		if v, ok := far.read(t).(*wire.MsgVersion); ok {
			ours = v
		}
	}

	require.Equal(t, id, ours.AssociationID, "the accepting side must echo the dialer's association ID")
}

// net_processing.cpp:1760: with multistreams off the accepting side stores no
// ID, so its version reply names no association.
func TestInbound_NoEchoWhenBlockPriorityOff(t *testing.T) {
	m := startedManagerWith(t, func(s *settings.Settings) { s.Legacy.AllowBlockPriority = false }, nil)

	far := dialScripted(t, nodeAddr(t, m, "127.0.0.1"))
	t.Cleanup(func() { _ = far.nc.Close() })

	version := remoteVersion(4321)
	version.AssociationID = testAssociationID(6)

	far.write(t, version)

	var ours *wire.MsgVersion

	for ours == nil {
		if v, ok := far.read(t).(*wire.MsgVersion); ok {
			ours = v
		}
	}

	require.Empty(t, ours.AssociationID, "a node with multistreams off must not name an association")
}

// An accepted connection must be logged. The live regtest run found svp2p
// silent about inbound peers, which left an operator no way to tell an
// unreachable listener from a peer that never dialled.
func TestManager_LogsEveryAcceptedConnection(t *testing.T) {
	logger := &captureLogger{}

	banList, err := NewBanList("")
	require.NoError(t, err)

	m := NewPeerManager(logger, managerSettings(), banList)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	require.NoError(t, m.Start(ctx, []string{"127.0.0.1:0"}))
	t.Cleanup(func() { require.NoError(t, m.Stop()) })

	far := dialScripted(t, m.ListenAddrs()[0])
	t.Cleanup(func() { _ = far.nc.Close() })

	far.completeOutboundHandshake(t)

	require.Eventually(t, func() bool {
		return logger.contains("inbound") && logger.contains(far.nc.LocalAddr().String())
	}, 5*time.Second, 20*time.Millisecond, "an accepted peer must be logged with its address and direction")
}

// go-wire's per-message MaxPayloadLength, not the 2 MiB receive ceiling, is
// what bounds the first message. A header that declares more than its own
// command can ever carry is refused before anything allocates the payload it
// promises: the test writes the header alone, so a node that read the payload
// would sit on the socket until the first-message timeout instead of closing.
func TestInbound_FirstMessageIsBoundedByItsCommand(t *testing.T) {
	m := startedManager(t)

	raw := dialRaw(t, nodeAddr(t, m, "127.0.0.1"))
	writeRawHeader(t, raw, wire.CmdVersion, 1<<20)

	requireEOF(t, raw, 5*time.Second)
	require.Equal(t, 0, len(m.peerHandles()))
	require.Equal(t, 0, associationCount(m))
}

// net_processing.cpp:4708-4715 takes version, createstream and streamack
// before anything else, so every other command is refused on the header alone
// however small the payload it declares.
func TestInbound_UnknownFirstCommandRefusedOnItsHeader(t *testing.T) {
	m := startedManager(t)

	raw := dialRaw(t, nodeAddr(t, m, "127.0.0.1"))
	writeRawHeader(t, raw, wire.CmdBlock, 64)

	requireEOF(t, raw, 5*time.Second)
	require.Equal(t, 0, len(m.peerHandles()))
}

// net.cpp:3218-3232 reaches the different-IP ban BEFORE net.cpp:3233-3236
// looks the stream policy up, so a createstream that is wrong on both counts
// is banned rather than merely refused.
func TestInbound_CreateStreamFromOtherIPWithBogusPolicyIsBanned(t *testing.T) {
	m := startedManager(t)

	id := testAssociationID(3)
	_ = establishAssociation(t, m, id)

	otherHost, raw := dialFromOtherIP(t, m)

	writeMsg(t, raw, &wire.MsgCreateStream{
		AssociationID:    id,
		StreamType:       wire.StreamTypeData1,
		StreamPolicyName: "Nope",
	})

	rej, ok := readMsg(t, raw, 5*time.Second).(*wire.MsgReject)
	require.True(t, ok, "a refused createstream must be rejected before the disconnect")
	require.Equal(t, rejectStreamSetup, rej.Code)
	require.Equal(t, reasonOtherAddress, rej.Reason)

	requireEOF(t, raw, 5*time.Second)

	require.True(t, m.banList.IsBanned(otherHost), "the different IP outranks the bogus policy")
}

// net_processing.cpp:209-211 creates an association ID only when multistreams
// are enabled. With them off the ID an inbound peer echoes is unvalidated peer
// bytes, so it must never become a registry key.
func TestInbound_AssociationNotRegisteredWhenBlockPriorityOff(t *testing.T) {
	m := startedManagerWith(t, func(s *settings.Settings) { s.Legacy.AllowBlockPriority = false }, nil)

	far := dialScripted(t, nodeAddr(t, m, "127.0.0.1"))
	t.Cleanup(func() { _ = far.nc.Close() })

	far.completeOutboundHandshakeWithAssociationID(t, testAssociationID(2))

	require.Eventually(t, func() bool { return establishedCount(m) == 1 }, 10*time.Second, 50*time.Millisecond)

	require.Never(t, func() bool { return associationCount(m) != 0 }, 2*time.Second, 100*time.Millisecond,
		"an unvalidated association ID must not be registered with multistreams off")
}

// go-wire's (*MsgReject).MaxPayloadLength is the whole message payload
// maximum, which it derives from the excessive block size global, so it is no
// bound at all on the answer to a createstream this node sent. A reject header
// declaring 1 MiB is refused on the header alone.
func TestOutbound_RejectAnsweringCreateStreamIsBoundedByAFixedSize(t *testing.T) {
	m := startedManager(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	t.Cleanup(func() { _ = ln.Close() })

	// What the far side sees after it sends the header ALONE. io.EOF means the
	// node decided on the header and closed, so it never asked for the 1 MiB
	// payload that header promised.
	farRead := make(chan error, 1)

	go func() {
		far, acceptErr := ln.Accept()
		if acceptErr != nil {
			farRead <- acceptErr

			return
		}

		defer func() { _ = far.Close() }()

		if _, writeErr := far.Write(rawHeader(wire.CmdReject, 1<<20)); writeErr != nil {
			farRead <- writeErr

			return
		}

		_ = far.SetReadDeadline(time.Now().Add(10 * time.Second))

		// Drain what the node sent (its createstream) until the socket ends. A
		// clean end says the node gave up on the reject header; a timeout
		// would say it was still waiting for the payload that header declared.
		_, copyErr := io.Copy(io.Discard, far)
		farRead <- copyErr
	}()

	nc, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)

	policy, ok := transport.PolicyForName(wire.BlockPriorityStreamPolicy)
	require.True(t, ok)

	start := time.Now()

	_, err = m.requestStream(context.Background(), nc, testAssociationID(1), policy)

	// openNewStreamConnection closes the socket on every failure; do the same
	// here, so the far side reaches the EOF it is waiting on.
	require.NoError(t, nc.Close())

	require.ErrorIs(t, err, ErrFirstMessageTooLong)
	require.Less(t, time.Since(start), streamAckTimeout, "the header alone must decide it, with no wait on a payload")

	require.NoError(t, <-farRead, "the node must close without asking for the payload it was promised")
}
