package p2p

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/bsv-blockchain/teranode/services/blockchain"
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
	// Sanitized output always passes the corresponding check.
	require.NoError(t, checkGossipString("f", sanitizeGossipString("evil\x00name"+strings.Repeat("p", 500), maxGossipClientNameLen), maxGossipClientNameLen))
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

func TestHandleNodeStatusTopic_OverlongClientNameRejected(t *testing.T) {
	server, remotePeerID, reg, banScore := newGossipFieldTestServer(t)
	info, _ := reg.Get(remotePeerID.String())
	baseline := info.LastMessageTime

	// Within the whole-message cap, but the padded field breaches its bound.
	msgBytes, err := json.Marshal(NodeStatusMessage{
		PeerID:     remotePeerID.String(),
		ClientName: strings.Repeat("x", 8*1024),
	})
	require.NoError(t, err)
	require.Less(t, len(msgBytes), maxNodeStatusMessageSize)

	server.handleNodeStatusTopic(context.Background(), msgBytes, remotePeerID.String())

	requireNoNotification(t, server, "padded node_status must not reach WebSocket clients")
	assertNoMessageTimeAdvance(t, reg, remotePeerID.String(), baseline, "padded node_status must not advance LastMessageTime")
	require.Positive(t, banScore(), "field violation must be scored as a protocol violation")
}

func TestHandleNodeStatusTopic_ControlCharactersRejected(t *testing.T) {
	server, remotePeerID, _, banScore := newGossipFieldTestServer(t)

	msgBytes, err := json.Marshal(NodeStatusMessage{
		PeerID:     remotePeerID.String(),
		ClientName: "evil\nclient",
	})
	require.NoError(t, err)

	server.handleNodeStatusTopic(context.Background(), msgBytes, remotePeerID.String())

	requireNoNotification(t, server, "node_status with control characters must not reach WebSocket clients")
	require.Positive(t, banScore(), "control characters must be scored as a protocol violation")
}

func TestHandleNodeStatusTopic_NonHexChainWorkRejected(t *testing.T) {
	server, remotePeerID, _, banScore := newGossipFieldTestServer(t)

	msgBytes, err := json.Marshal(NodeStatusMessage{
		PeerID:    remotePeerID.String(),
		ChainWork: "not-hex-at-all",
	})
	require.NoError(t, err)

	server.handleNodeStatusTopic(context.Background(), msgBytes, remotePeerID.String())

	requireNoNotification(t, server, "node_status with non-hex chain_work must not reach WebSocket clients")
	require.Positive(t, banScore(), "non-hex chain_work must be scored as a protocol violation")
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

	msgBytes, err := json.Marshal(NodeStatusMessage{
		PeerID:     selfID,
		ClientName: strings.Repeat("x", 8*1024),
	})
	require.NoError(t, err)

	server.handleNodeStatusTopic(context.Background(), msgBytes, selfID)

	requireNoNotification(t, server, "own message with an invalid field must still be dropped")
	if info, ok := reg.Get(selfID); ok {
		require.Zero(t, info.BanScore, "a node must not score itself for its own message")
	}
}

func TestHandleBlockTopic_OverlongClientNameRejected(t *testing.T) {
	server, remotePeerID, reg, banScore := newGossipFieldTestServer(t)
	info, _ := reg.Get(remotePeerID.String())
	baseline := info.LastMessageTime

	msgBytes, err := json.Marshal(BlockMessage{
		PeerID:     remotePeerID.String(),
		Hash:       testBlockHashHex,
		DataHubURL: "http://example.com:8090",
		ClientName: strings.Repeat("x", 8*1024),
	})
	require.NoError(t, err)
	require.Less(t, len(msgBytes), maxBlockMessageSize)

	server.handleBlockTopic(context.Background(), msgBytes, remotePeerID.String())

	requireNoNotification(t, server, "padded block message must not reach WebSocket clients")
	assertNoMessageTimeAdvance(t, reg, remotePeerID.String(), baseline, "padded block message must not advance LastMessageTime")
	require.Positive(t, banScore(), "field violation must be scored as a protocol violation")
}

func TestHandleSubtreeTopic_OverlongClientNameRejected(t *testing.T) {
	server, remotePeerID, reg, banScore := newGossipFieldTestServer(t)
	info, _ := reg.Get(remotePeerID.String())
	baseline := info.LastMessageTime

	msgBytes, err := json.Marshal(SubtreeMessage{
		PeerID:     remotePeerID.String(),
		Hash:       testBlockHashHex,
		DataHubURL: "http://example.com:8090",
		ClientName: strings.Repeat("x", 4*1024),
	})
	require.NoError(t, err)
	require.Less(t, len(msgBytes), maxSubtreeMessageSize)

	server.handleSubtreeTopic(context.Background(), msgBytes, remotePeerID.String())

	requireNoNotification(t, server, "padded subtree message must not reach WebSocket clients")
	assertNoMessageTimeAdvance(t, reg, remotePeerID.String(), baseline, "padded subtree message must not advance LastMessageTime")
	require.Positive(t, banScore(), "field violation must be scored as a protocol violation")
}

func TestHandleRejectedTxTopic_OverlongReasonRejected(t *testing.T) {
	server, remotePeerID, reg, banScore := newGossipFieldTestServer(t)
	info, _ := reg.Get(remotePeerID.String())
	baseline := info.LastMessageTime

	msgBytes, err := json.Marshal(RejectedTxMessage{
		PeerID: remotePeerID.String(),
		TxID:   testBlockHashHex,
		Reason: strings.Repeat("x", maxGossipReasonLen+1),
	})
	require.NoError(t, err)
	require.Less(t, len(msgBytes), maxRejectedTxMessageSize)

	server.handleRejectedTxTopic(context.Background(), msgBytes, remotePeerID.String())

	assertNoMessageTimeAdvance(t, reg, remotePeerID.String(), baseline, "padded rejected_tx must not advance LastMessageTime")
	require.Positive(t, banScore(), "field violation must be scored as a protocol violation")
}
