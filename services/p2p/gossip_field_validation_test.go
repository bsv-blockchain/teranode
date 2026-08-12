package p2p

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

const testBlockHashHex = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"

func TestCheckGossipString(t *testing.T) {
	require.NoError(t, checkGossipString("f", "Teranode v1.0", 64))
	require.NoError(t, checkGossipString("f", "", 64))
	// Printable non-ASCII UTF-8 is legitimate (coinbase miner tags).
	require.NoError(t, checkGossipString("f", "矿池/ViaBTC/", 64))

	require.Error(t, checkGossipString("f", strings.Repeat("x", 65), 64), "over-long value must be rejected")
	require.Error(t, checkGossipString("f", "line1\nline2", 64), "newline must be rejected (log injection)")
	require.Error(t, checkGossipString("f", "tab\there", 64), "tab must be rejected")
	require.Error(t, checkGossipString("f", "esc\x1b[31m", 64), "ANSI escape must be rejected")
	require.Error(t, checkGossipString("f", "bad\xff\xfeutf8", 64), "invalid UTF-8 must be rejected")
	require.Error(t, checkGossipString("f", "abc\u202edef", 64), "bidi override must be rejected (display spoofing)")
	require.Error(t, checkGossipString("f", "abc\u2028def", 64), "line separator must be rejected")

	// The error must not echo the value, so a padded field cannot inflate logs.
	err := checkGossipString("f", strings.Repeat("A", 100), 64)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "AAAA")
}

func TestCheckGossipHex(t *testing.T) {
	require.NoError(t, checkGossipHex("f", "", 64))
	require.NoError(t, checkGossipHex("f", testBlockHashHex, 64))
	require.NoError(t, checkGossipHex("f", "DEADbeef", 64))

	require.Error(t, checkGossipHex("f", strings.Repeat("a", 65), 64))
	require.Error(t, checkGossipHex("f", "xyz", 64))
	require.Error(t, checkGossipHex("f", "aa bb", 64))
}

func TestSanitizeGossipString(t *testing.T) {
	require.Equal(t, "abcdef", sanitizeGossipString("abc\n\r\tdef", 64))
	require.Equal(t, "short", sanitizeGossipString("short", 64))
	require.Equal(t, strings.Repeat("x", 8), sanitizeGossipString(strings.Repeat("x", 20), 8))
	// Truncation must not split a multi-byte rune ("矿" is 3 bytes).
	require.Equal(t, "矿", sanitizeGossipString("矿池", 4))
	require.Empty(t, sanitizeGossipString("anything", 0))

	// Sanitized output must always pass the corresponding check, including for
	// invalid UTF-8 input (the raw-miner-tag case: arbitrary coinbase bytes).
	hostile := []string{
		"evil\x00name" + strings.Repeat("p", 500),
		"raw\xff\xfe\x01miner\x7ftag",
		"bidi\u202eand\u2028seps",
		strings.Repeat("\x1b[31mx", 100),
	}
	for _, in := range hostile {
		out := sanitizeGossipString(in, maxGossipClientNameLen)
		require.NoError(t, checkGossipString("f", out, maxGossipClientNameLen), "sanitize(%q) must pass validation", in)
	}
}

// newGossipFieldTestServer extends the size-limit harness with a context so the
// applyBanScore path (which uses s.gCtx) can run.
func newGossipFieldTestServer(t *testing.T) (*Server, peer.ID, *blockchain.CentralizedPeerRegistry, func() int32) {
	t.Helper()

	server, remotePeerID, reg, _ := newSizeLimitTestServer(t)
	server.gCtx = context.Background()

	banScore := func() int32 {
		info, ok := reg.Get(remotePeerID.String())
		require.True(t, ok)
		return info.BanScore
	}

	return server, remotePeerID, reg, banScore
}

func requireNoNotification(t *testing.T, server *Server, msg string) {
	t.Helper()
	select {
	case n := <-server.notificationCh:
		t.Fatalf("%s (got %s notification)", msg, n.Type)
	default:
	}
}

// A display-only field (client_name) breaching its bound is sanitized in
// place, not treated as a protocol violation: the notification still flows,
// but bounded — the broadcast amplification is closed without risking bans of
// honest peers over telemetry text.
func TestHandleNodeStatusTopic_OverlongClientNameTruncated(t *testing.T) {
	server, remotePeerID, _, banScore := newGossipFieldTestServer(t)

	// Within the whole-message cap, but the padded field breaches its bound.
	msgBytes, err := json.Marshal(NodeStatusMessage{
		PeerID:     remotePeerID.String(),
		ClientName: strings.Repeat("x", 8*1024),
	})
	require.NoError(t, err)
	require.Less(t, len(msgBytes), maxNodeStatusMessageSize)

	server.handleNodeStatusTopic(context.Background(), msgBytes, remotePeerID.String())

	select {
	case n := <-server.notificationCh:
		require.Len(t, n.ClientName, maxGossipClientNameLen, "client_name must be truncated to its bound")
		data, err := json.Marshal(n)
		require.NoError(t, err)
		require.Less(t, len(data), 4096, "one 8KB inbound field must not fan out oversized")
	default:
		t.Fatal("sanitized node_status must still be forwarded")
	}

	require.Zero(t, banScore(), "display-text violation must not be scored")
}

func TestHandleNodeStatusTopic_ControlCharactersStripped(t *testing.T) {
	server, remotePeerID, _, banScore := newGossipFieldTestServer(t)

	msgBytes, err := json.Marshal(NodeStatusMessage{
		PeerID:     remotePeerID.String(),
		ClientName: "evil\r\n[FORGED] client\x1b[2J",
	})
	require.NoError(t, err)

	server.handleNodeStatusTopic(context.Background(), msgBytes, remotePeerID.String())

	select {
	case n := <-server.notificationCh:
		require.Equal(t, "evil[FORGED] client[2J", n.ClientName, "control characters must be stripped before fan-out")
	default:
		t.Fatal("sanitized node_status must still be forwarded")
	}

	require.Zero(t, banScore())
}

// Protocol-format fields have a well-defined shape; a violation drops the
// message and scores the sender.
func TestHandleNodeStatusTopic_NonHexChainWorkRejected(t *testing.T) {
	server, remotePeerID, reg, banScore := newGossipFieldTestServer(t)
	info, _ := reg.Get(remotePeerID.String())
	baseline := info.LastMessageTime

	msgBytes, err := json.Marshal(NodeStatusMessage{
		PeerID:    remotePeerID.String(),
		ChainWork: "not-hex-at-all",
	})
	require.NoError(t, err)

	server.handleNodeStatusTopic(context.Background(), msgBytes, remotePeerID.String())

	requireNoNotification(t, server, "node_status with non-hex chain_work must not reach WebSocket clients")
	assertNoMessageTimeAdvance(t, reg, remotePeerID.String(), baseline, "invalid node_status must not advance LastMessageTime")
	require.Positive(t, banScore(), "non-hex chain_work must be scored as a protocol violation")
}

func TestHandleNodeStatusTopic_OverlongBaseURLRejected(t *testing.T) {
	server, remotePeerID, reg, banScore := newGossipFieldTestServer(t)
	info, _ := reg.Get(remotePeerID.String())
	baseline := info.LastMessageTime

	msgBytes, err := json.Marshal(NodeStatusMessage{
		PeerID:  remotePeerID.String(),
		BaseURL: "http://example.com/" + strings.Repeat("p", maxGossipURLLen),
	})
	require.NoError(t, err)
	require.Less(t, len(msgBytes), maxNodeStatusMessageSize)

	server.handleNodeStatusTopic(context.Background(), msgBytes, remotePeerID.String())

	requireNoNotification(t, server, "node_status with over-long base_url must not reach WebSocket clients")
	assertNoMessageTimeAdvance(t, reg, remotePeerID.String(), baseline, "invalid node_status must not advance LastMessageTime")
	require.Positive(t, banScore(), "over-long base_url must be scored as a protocol violation")
}

func TestHandleNodeStatusTopic_ValidMessageBroadcastStaysBounded(t *testing.T) {
	server, remotePeerID, _, banScore := newGossipFieldTestServer(t)

	msgBytes, err := json.Marshal(NodeStatusMessage{
		PeerID:        remotePeerID.String(),
		Type:          "node_status",
		ClientName:    "teranode-test",
		MinerName:     "/TAAL/",
		Version:       "v1.2.3",
		CommitHash:    "9de8693c2ffb1b6f0c79b8f3a1d2e4c5a6b7c8d9",
		BestBlockHash: testBlockHashHex,
		FSMState:      "RUNNING",
		ListenMode:    "full",
		ChainWork:     "0000000000000000000000000000000000000000000000000000000100010001",
		Storage:       "full",
	})
	require.NoError(t, err)

	server.handleNodeStatusTopic(context.Background(), msgBytes, remotePeerID.String())

	select {
	case n := <-server.notificationCh:
		data, err := json.Marshal(n)
		require.NoError(t, err)
		require.Less(t, len(data), 4096, "marshalled notification for a valid node_status must stay small")
		require.Equal(t, "teranode-test", n.ClientName)
	default:
		t.Fatal("valid node_status must be forwarded to WebSocket clients")
	}

	require.Zero(t, banScore(), "valid node_status must not be penalised")
}

func TestHandleNodeStatusTopic_OwnInvalidMessageDroppedWithoutSelfBan(t *testing.T) {
	server, _, reg, _ := newGossipFieldTestServer(t)
	selfID := server.P2PClient.GetID()
	reg.Register(&blockchain.PeerInfo{ID: selfID})

	msgBytes, err := json.Marshal(NodeStatusMessage{
		PeerID:    selfID,
		ChainWork: "not-hex-at-all",
	})
	require.NoError(t, err)

	server.handleNodeStatusTopic(context.Background(), msgBytes, selfID)

	requireNoNotification(t, server, "own message with an invalid protocol field must still be dropped")

	info, ok := reg.Get(selfID)
	require.True(t, ok)
	require.Zero(t, info.BanScore, "a node must not score itself for its own message")
}

func TestHandleBlockTopic_OverlongClientNameTruncated(t *testing.T) {
	server, remotePeerID, _, banScore := newGossipFieldTestServer(t)

	msgBytes, err := json.Marshal(BlockMessage{
		PeerID:     remotePeerID.String(),
		Hash:       testBlockHashHex,
		DataHubURL: "http://example.com:8090",
		ClientName: strings.Repeat("x", 8*1024),
	})
	require.NoError(t, err)
	require.Less(t, len(msgBytes), maxBlockMessageSize)

	server.handleBlockTopic(context.Background(), msgBytes, remotePeerID.String())

	select {
	case n := <-server.notificationCh:
		require.Len(t, n.ClientName, maxGossipClientNameLen, "client_name must be truncated to its bound")
	default:
		t.Fatal("block announcement with sanitized display text must still be forwarded (it triggers catchup)")
	}

	require.Zero(t, banScore(), "display-text violation must not suppress block announcements or score the peer")
}

func TestHandleBlockTopic_OverlongDataHubURLRejected(t *testing.T) {
	server, remotePeerID, reg, banScore := newGossipFieldTestServer(t)
	info, _ := reg.Get(remotePeerID.String())
	baseline := info.LastMessageTime

	msgBytes, err := json.Marshal(BlockMessage{
		PeerID:     remotePeerID.String(),
		Hash:       testBlockHashHex,
		DataHubURL: "http://example.com/" + strings.Repeat("p", maxGossipURLLen),
	})
	require.NoError(t, err)
	require.Less(t, len(msgBytes), maxBlockMessageSize)

	server.handleBlockTopic(context.Background(), msgBytes, remotePeerID.String())

	requireNoNotification(t, server, "block message with over-long DataHubURL must not reach WebSocket clients")
	assertNoMessageTimeAdvance(t, reg, remotePeerID.String(), baseline, "invalid block message must not advance LastMessageTime")
	require.Positive(t, banScore(), "over-long DataHubURL must be scored as a protocol violation")
}

func TestHandleSubtreeTopic_OverlongClientNameTruncated(t *testing.T) {
	server, remotePeerID, _, banScore := newGossipFieldTestServer(t)

	msgBytes, err := json.Marshal(SubtreeMessage{
		PeerID:     remotePeerID.String(),
		Hash:       testBlockHashHex,
		DataHubURL: "http://example.com:8090",
		ClientName: strings.Repeat("x", 4*1024),
	})
	require.NoError(t, err)
	require.Less(t, len(msgBytes), maxSubtreeMessageSize)

	server.handleSubtreeTopic(context.Background(), msgBytes, remotePeerID.String())

	select {
	case n := <-server.notificationCh:
		require.Len(t, n.ClientName, maxGossipClientNameLen, "client_name must be truncated to its bound")
	default:
		t.Fatal("subtree announcement with sanitized display text must still be forwarded")
	}

	require.Zero(t, banScore())
}

func TestHandleRejectedTxTopic_OverlongReasonTruncatedAndNonHexTxIDRejected(t *testing.T) {
	server, remotePeerID, reg, banScore := newGossipFieldTestServer(t)

	// Over-long reason: display text, sanitized, message processed normally.
	msgBytes, err := json.Marshal(RejectedTxMessage{
		PeerID: remotePeerID.String(),
		TxID:   testBlockHashHex,
		Reason: strings.Repeat("x", maxGossipReasonLen+512),
	})
	require.NoError(t, err)
	require.Less(t, len(msgBytes), maxRejectedTxMessageSize)

	server.handleRejectedTxTopic(context.Background(), msgBytes, remotePeerID.String())
	require.Zero(t, banScore(), "over-long reason must be truncated, not scored")

	// Non-hex tx_id: protocol field, dropped and scored.
	info, _ := reg.Get(remotePeerID.String())
	baseline := info.LastMessageTime
	msgBytes, err = json.Marshal(RejectedTxMessage{
		PeerID: remotePeerID.String(),
		TxID:   "not-a-tx-id",
	})
	require.NoError(t, err)

	server.handleRejectedTxTopic(context.Background(), msgBytes, remotePeerID.String())

	assertNoMessageTimeAdvance(t, reg, remotePeerID.String(), baseline, "invalid rejected_tx must not advance LastMessageTime")
	require.Positive(t, banScore(), "non-hex tx_id must be scored as a protocol violation")
}

// Egress round-trip: whatever hostile free text ends up in local settings, the
// node's own published node_status must pass the ingress validation of remote
// peers running the same bounds.
func TestGetNodeStatusMessage_EgressAlwaysPassesIngressValidation(t *testing.T) {
	server, _, _, _ := newGossipFieldTestServer(t)
	server.settings = &settings.Settings{
		ClientName: "evil\x00client" + strings.Repeat("x", 10*1024),
		Version:    "v1.2.3-" + strings.Repeat("y", 200),
		P2P:        settings.P2PSettings{ListenMode: settings.ListenModeFull},
	}
	server.logger = ulogger.TestLogger{}

	msg := server.getNodeStatusMessage(context.Background())
	require.NotNil(t, msg)
	require.NoError(t, checkGossipString("client_name", msg.ClientName, maxGossipClientNameLen))

	// The published NodeStatusMessage is built from this notification and then
	// sanitized+validated in handleNodeStatusNotification; mirror that here.
	published := NodeStatusMessage{
		PeerID:     msg.PeerID,
		ClientName: msg.ClientName,
		MinerName:  msg.MinerName,
		Version:    msg.Version,
		CommitHash: msg.CommitHash,
		FSMState:   msg.FSMState,
		ListenMode: msg.ListenMode,
		ChainWork:  msg.ChainWork,
		Storage:    msg.Storage,
	}
	published.sanitizeFields()
	require.NoError(t, published.validateFields())
}

// banRemotePeer pushes the remote peer over the ban threshold via a reason
// that is not in the configured ReasonPoints map, so the default points apply.
func banRemotePeer(t *testing.T, reg *blockchain.CentralizedPeerRegistry, id string) {
	t.Helper()
	_, banned := reg.AddBanScore(id, "test-ban", 100)
	require.True(t, banned, "test setup: peer must be banned")
}

// Regression tests: a banned remote peer must not dodge the banned-peer skip
// by claiming the local node's peer ID in the message. The message must be
// dropped by the skip before the spoof check can trigger an AddBanScore
// registry call, so the ban score stays exactly where it was.

func TestHandleBlockTopic_BannedPeerClaimingOwnIDStillSkipped(t *testing.T) {
	server, remotePeerID, reg, banScore := newGossipFieldTestServer(t)
	banRemotePeer(t, reg, remotePeerID.String())
	before := banScore()

	msgBytes, err := json.Marshal(BlockMessage{
		PeerID:     server.P2PClient.GetID(),
		Hash:       testBlockHashHex,
		DataHubURL: "http://example.com:8090",
	})
	require.NoError(t, err)

	server.handleBlockTopic(context.Background(), msgBytes, remotePeerID.String())

	requireNoNotification(t, server, "banned peer's block message must be dropped")
	require.Equal(t, before, banScore(), "banned peer must be dropped before any scoring runs")
}

func TestHandleSubtreeTopic_BannedPeerClaimingOwnIDStillSkipped(t *testing.T) {
	server, remotePeerID, reg, banScore := newGossipFieldTestServer(t)
	banRemotePeer(t, reg, remotePeerID.String())
	before := banScore()

	msgBytes, err := json.Marshal(SubtreeMessage{
		PeerID:     server.P2PClient.GetID(),
		Hash:       testBlockHashHex,
		DataHubURL: "http://example.com:8090",
	})
	require.NoError(t, err)

	server.handleSubtreeTopic(context.Background(), msgBytes, remotePeerID.String())

	requireNoNotification(t, server, "banned peer's subtree message must be dropped")
	require.Equal(t, before, banScore(), "banned peer must be dropped before any scoring runs")
}

func TestHandleRejectedTxTopic_BannedPeerClaimingOwnIDStillSkipped(t *testing.T) {
	server, remotePeerID, reg, banScore := newGossipFieldTestServer(t)
	banRemotePeer(t, reg, remotePeerID.String())
	before := banScore()

	msgBytes, err := json.Marshal(RejectedTxMessage{
		PeerID: server.P2PClient.GetID(),
		TxID:   testBlockHashHex,
		Reason: "some reason",
	})
	require.NoError(t, err)

	server.handleRejectedTxTopic(context.Background(), msgBytes, remotePeerID.String())

	require.Equal(t, before, banScore(), "banned peer must be dropped before any scoring runs")
}
