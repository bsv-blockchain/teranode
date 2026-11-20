package protocol

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/svp2p/svp2ptest"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// compactBlocksManager starts a manager with legacy_compactBlocks on and a
// TxIndex wired (Task 6 addendum gate: m.tSettings.Legacy.CompactBlocks &&
// m.txIndex() != nil), so the established hook is free to send sendcmpct.
func compactBlocksManager(t *testing.T, tweak func(*settings.Settings)) *PeerManager {
	t.Helper()

	return startedManagerWith(t, func(s *settings.Settings) {
		s.Legacy.CompactBlocks = true

		if tweak != nil {
			tweak(s)
		}
	}, func(m *PeerManager) {
		m.SetTxIndex(&fakeTxIndex{})
	})
}

// TestSendCmpct_SentOnceToInboundPeerWhenEnabled is SVNode's
// ProcessVerAckMessage (net_processing.cpp:1942-1953): once the handshake
// completes, and compact blocks are enabled, the node sends
// sendcmpct(announce=false, version=1) — here to a peer that dialled IN.
func TestSendCmpct_SentOnceToInboundPeerWhenEnabled(t *testing.T) {
	m := compactBlocksManager(t, nil)

	far := dialScripted(t, nodeAddr(t, m, "127.0.0.1"))
	far.completeOutboundHandshake(t)

	msg, ok := far.readUntil(t, wire.CmdSendcmpct).(*wire.MsgSendcmpct)
	require.True(t, ok)
	require.False(t, msg.SendCmpct)
	require.Equal(t, uint64(1), msg.Version)

	// Exactly one: read whatever else the node sends for a short while and
	// confirm no second sendcmpct ever arrives.
	deadline := time.Now().Add(300 * time.Millisecond)

	for time.Now().Before(deadline) {
		require.NoError(t, far.nc.SetReadDeadline(time.Now().Add(50*time.Millisecond)))

		_, m, _, err := wire.ReadMessageWithEncodingN(far.nc, wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
		if err != nil {
			continue
		}

		require.NotEqual(t, wire.CmdSendcmpct, m.Command(), "sendcmpct must be sent exactly once")
	}
}

// TestSendCmpct_SentOnceToOutboundPeerWhenEnabled proves the same send fires
// on a connection this node itself dialled, since SVNode's ProcessVerAckMessage
// sends it to every peer regardless of direction.
func TestSendCmpct_SentOnceToOutboundPeerWhenEnabled(t *testing.T) {
	peer := svp2ptest.NewScriptedPeer(t, svp2ptest.BuildFixtureChain(t, fixtureChainSettings(), 2), managerSettings().ChainCfgParams.Net, svp2ptest.Script{}, false)
	peer.Listen()

	m := compactBlocksManager(t, func(s *settings.Settings) {
		s.Legacy.ConnectPeers = []string{peer.Addr}
	})

	require.Eventually(t, func() bool {
		_, ok := peer.Transcript.FirstOn(svp2ptest.In, wire.CmdSendcmpct)
		return ok
	}, 5*time.Second, 20*time.Millisecond)

	require.Equal(t, 1, peer.Transcript.Count(svp2ptest.In, wire.CmdSendcmpct))

	entry, ok := peer.Transcript.FirstOn(svp2ptest.In, wire.CmdSendcmpct)
	require.True(t, ok)

	msg, ok := entry.Msg.(*wire.MsgSendcmpct)
	require.True(t, ok)
	require.False(t, msg.SendCmpct)
	require.Equal(t, uint64(1), msg.Version)

	_ = m
}

// TestSendCmpct_NeverSentWhenDisabled is the flag-off half of spec §8: no
// negotiation at all, today's getdata-only behaviour.
func TestSendCmpct_NeverSentWhenDisabled(t *testing.T) {
	m := startedManagerWith(t, nil, nil)

	far := dialScripted(t, nodeAddr(t, m, "127.0.0.1"))
	far.completeOutboundHandshake(t)

	deadline := time.Now().Add(300 * time.Millisecond)

	for time.Now().Before(deadline) {
		require.NoError(t, far.nc.SetReadDeadline(time.Now().Add(50*time.Millisecond)))

		_, msg, _, err := wire.ReadMessageWithEncodingN(far.nc, wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
		if err != nil {
			continue
		}

		require.NotEqual(t, wire.CmdSendcmpct, msg.Command(), "sendcmpct must never be sent with the flag off")
	}
}

// TestSendCmpct_NeverSentWithoutTxIndex is the other half of the gate: the
// flag alone is not enough, matching SetTxIndex's own doc comment ("a nil
// TxIndex leaves compact blocks off regardless of legacy_compactBlocks").
func TestSendCmpct_NeverSentWithoutTxIndex(t *testing.T) {
	m := startedManagerWith(t, func(s *settings.Settings) {
		s.Legacy.CompactBlocks = true
	}, nil)

	far := dialScripted(t, nodeAddr(t, m, "127.0.0.1"))
	far.completeOutboundHandshake(t)

	deadline := time.Now().Add(300 * time.Millisecond)

	for time.Now().Before(deadline) {
		require.NoError(t, far.nc.SetReadDeadline(time.Now().Add(50*time.Millisecond)))

		_, msg, _, err := wire.ReadMessageWithEncodingN(far.nc, wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
		if err != nil {
			continue
		}

		require.NotEqual(t, wire.CmdSendcmpct, msg.Command(), "sendcmpct must never be sent without a TxIndex")
	}
}

// TestSendCmpct_InboundVersion1RecordsFlags is ProcessSendCompactMessage
// (net_processing.cpp:2390-2411): a version-1 sendcmpct locks in
// fProvidesHeaderAndIDs and records fPreferHeaderAndIDs as the announce bit.
func TestSendCmpct_InboundVersion1RecordsFlags(t *testing.T) {
	m := startedManagerWith(t, nil, nil)

	far := dialScripted(t, nodeAddr(t, m, "127.0.0.1"))
	far.completeOutboundHandshake(t)

	far.write(t, &wire.MsgSendcmpct{SendCmpct: true, Version: 1})

	require.Eventually(t, func() bool {
		snaps := m.Snapshots()
		return len(snaps) == 1
	}, 5*time.Second, 20*time.Millisecond)

	sp := onlySyncPeerState(t, m)

	require.Eventually(t, func() bool {
		m.syncMu.Lock()
		defer m.syncMu.Unlock()

		return sp.fProvidesHeaderAndIDs
	}, 5*time.Second, 20*time.Millisecond)

	m.syncMu.Lock()
	require.True(t, sp.fProvidesHeaderAndIDs)
	require.True(t, sp.fPreferHeaderAndIDs)
	require.True(t, sp.fSupportsDesiredCmpctVersion)
	m.syncMu.Unlock()
}

// TestSendCmpct_VersionOtherThanOneIsIgnored is the version gate: SVNode's
// ProcessSendCompactMessage only acts `if(nCMPCTBLOCKVersion == 1)` — a
// version-2 message changes nothing and scores nothing.
func TestSendCmpct_VersionOtherThanOneIsIgnored(t *testing.T) {
	m := startedManagerWith(t, nil, nil)

	far := dialScripted(t, nodeAddr(t, m, "127.0.0.1"))
	far.completeOutboundHandshake(t)

	far.write(t, &wire.MsgSendcmpct{SendCmpct: false, Version: 2})

	require.Eventually(t, func() bool {
		snaps := m.Snapshots()
		return len(snaps) == 1
	}, 5*time.Second, 20*time.Millisecond)

	sp := onlySyncPeerState(t, m)

	// Give the message a moment to be processed, then assert nothing moved.
	time.Sleep(200 * time.Millisecond)

	m.syncMu.Lock()
	require.False(t, sp.fProvidesHeaderAndIDs)
	require.False(t, sp.fPreferHeaderAndIDs)
	require.False(t, sp.fSupportsDesiredCmpctVersion)
	m.syncMu.Unlock()

	require.Equal(t, 0, m.Snapshots()[0].MisbehaviorScore)
}

// TestGetBlockTxn_InboundIsIgnored proves the receive-only stance (spec §2):
// we never announce, so an inbound getblocktxn earns no reply and no
// disconnect, only a debug log.
func TestGetBlockTxn_InboundIsIgnored(t *testing.T) {
	m := startedManagerWith(t, nil, nil)

	far := dialScripted(t, nodeAddr(t, m, "127.0.0.1"))
	far.completeOutboundHandshake(t)

	far.write(t, &wire.MsgGetBlockTxn{BlockHash: chainhash.Hash{0x01}, Indexes: []uint32{0}})

	// The connection must stay open and no reply of any kind for
	// getblocktxn must be sent back.
	deadline := time.Now().Add(300 * time.Millisecond)

	for time.Now().Before(deadline) {
		require.NoError(t, far.nc.SetReadDeadline(time.Now().Add(50*time.Millisecond)))

		_, msg, _, err := wire.ReadMessageWithEncodingN(far.nc, wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
		if err != nil {
			continue
		}

		require.NotEqual(t, wire.CmdBlockTxn, msg.Command())
		require.NotEqual(t, wire.CmdReject, msg.Command())
	}

	require.Equal(t, int32(1), m.ConnectedCount())
}

// TestSendCmpct_BeforeVersionScoresMissingVersion confirms Task 6 adds no new
// path around the handshake's existing rule: net_processing.cpp
// ProcessMessage's "Must have a version message before anything else" already
// scores anything, sendcmpct included, that arrives first
// (handshake.go scoreMissingVersion). Driven directly against the Handshake
// machine, the same way TestMessageBeforeVersionScoresMisbehavior does for
// MsgPing, because a real socket's own first-message legality check
// (streams.go ErrFirstMessageCommand) never lets a bare sendcmpct be the
// literal first bytes on a connection — this rule only bites once a
// legitimate first message (createstream/streamack) has been accepted and a
// version still has not arrived.
func TestSendCmpct_BeforeVersionScoresMissingVersion(t *testing.T) {
	h := NewHandshake(outboundConfig())
	_ = h.Initial()

	_, err := h.OnMessage(wire.NewMsgSendcmpct(false))
	require.NoError(t, err)
	require.Equal(t, 1, h.MisbehaviorScore())
}

// onlySyncPeerState returns the sole connected peer's peerSyncState.
func onlySyncPeerState(t *testing.T, m *PeerManager) *peerSyncState {
	t.Helper()

	m.mu.Lock()
	defer m.mu.Unlock()

	require.Len(t, m.peers, 1)

	for _, sp := range m.peers {
		return sp.State
	}

	return nil
}
