package p2p

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
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

// The egress/ingress round-trip guarantee: whatever sanitizePeerDisplayString
// (sanitize.go) produces must always pass checkGossipString, so a patched
// node's own published display text can never read as a protocol violation to
// a patched receiver - including for invalid UTF-8 input (the raw-miner-tag
// case: arbitrary coinbase bytes).
func TestSanitizePeerDisplayStringPassesGossipCheck(t *testing.T) {
	hostile := []string{
		"evil\x00name" + strings.Repeat("p", 500),
		"raw\xff\xfe\x01miner\x7ftag",
		"bidi\u202eand\u2028seps",
		strings.Repeat("\x1b[31mx", 100),
	}
	for _, in := range hostile {
		out := sanitizePeerDisplayString(in, maxPeerDisplayStringLen)
		require.NoError(t, checkGossipString("f", out, maxPeerDisplayStringLen), "sanitize(%q) must pass validation", in)
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
		require.Len(t, n.ClientName, maxPeerDisplayStringLen, "client_name must be truncated to its bound")
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

// Hex telemetry fields are all-or-nothing per sanitize.go: a non-hex value is
// blanked while the rest of the telemetry keeps flowing, and the peer is not
// scored.
func TestHandleNodeStatusTopic_NonHexChainWorkBlanked(t *testing.T) {
	server, remotePeerID, _, banScore := newGossipFieldTestServer(t)

	msgBytes, err := json.Marshal(NodeStatusMessage{
		PeerID:    remotePeerID.String(),
		ChainWork: "not-hex-at-all",
	})
	require.NoError(t, err)

	server.handleNodeStatusTopic(context.Background(), msgBytes, remotePeerID.String())

	select {
	case n := <-server.notificationCh:
		require.Empty(t, n.ChainWork, "non-hex chain_work must be blanked before fan-out")
	default:
		t.Fatal("node_status with blanked chain_work must still be forwarded")
	}

	require.Zero(t, banScore(), "blanked telemetry must not be scored")
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
		PeerID:  selfID,
		BaseURL: "http://example.com/" + strings.Repeat("p", maxGossipURLLen),
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
		require.Len(t, n.ClientName, maxPeerDisplayStringLen, "client_name must be truncated to its bound")
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
		require.Len(t, n.ClientName, maxPeerDisplayStringLen, "client_name must be truncated to its bound")
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
// peers running the same bounds — and the sanitization must demonstrably have
// happened, not just trivially validated empty fields.
func TestGetNodeStatusMessage_EgressAlwaysPassesIngressValidation(t *testing.T) {
	server, _, _, _ := newGossipFieldTestServer(t)
	server.settings = &settings.Settings{
		ClientName: "evil\x00client" + strings.Repeat("x", 10*1024),
		Version:    "v1.2.3-" + strings.Repeat("y", 200),
		Commit:     "abc<script>" + strings.Repeat("z", 300),
		P2P:        settings.P2PSettings{ListenMode: settings.ListenModeFull},
	}
	server.logger = ulogger.TestLogger{}

	msg := server.getNodeStatusMessage(context.Background())
	require.NotNil(t, msg)
	require.Len(t, msg.ClientName, maxPeerDisplayStringLen, "hostile client name must be truncated, not just validated")

	// The published NodeStatusMessage is built from this notification and then
	// sanitized+validated in handleNodeStatusNotification; mirror that here,
	// seeding an over-long URL so validateFields provably inspects something.
	published := NodeStatusMessage{
		PeerID:     msg.PeerID,
		BaseURL:    "http://example.com/" + strings.Repeat("p", maxGossipURLLen),
		ClientName: msg.ClientName,
		MinerName:  msg.MinerName,
		Version:    msg.Version,
		CommitHash: msg.CommitHash,
		FSMState:   msg.FSMState,
		ListenMode: msg.ListenMode,
		ChainWork:  msg.ChainWork,
		Storage:    msg.Storage,
	}
	sanitizeNodeStatusMessage(&published)

	require.Len(t, published.Version, maxPeerDisplayStringLen, "hostile version must be truncated")
	require.Len(t, published.CommitHash, maxPeerDisplayStringLen, "hostile commit must be truncated")
	require.NotContains(t, published.CommitHash, "<", "HTML-meaningful characters must be stripped")

	require.Error(t, published.validateFields(), "the over-long URL must be caught — proves the validator is not vacuous")

	published.BaseURL = ""
	require.NoError(t, published.validateFields(), "with URLs in bounds, sanitized egress must pass ingress validation")
}

// TestHandleNodeStatusNotification_BlanksInvalidURLAndPublishesUnderCap drives
// the real egress path: a pathological operator URL must be blanked (loud log,
// message still published) rather than taking the node off gossip, and the
// marshalled payload must fit under the topic cap peers enforce on ingress.
func TestHandleNodeStatusNotification_BlanksInvalidURLAndPublishesUnderCap(t *testing.T) {
	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(nil, nil, assert.AnError).Maybe()
	mockBlockchain.On("GetFSMCurrentState", mock.Anything).Return(nil, assert.AnError).Maybe()
	mockBlockchain.On("GetState", mock.Anything, mock.Anything).Return(nil, assert.AnError).Maybe()

	var captured []byte
	mockP2P := &MockServerP2PClient{peerID: mustNewPeerID(t)}
	mockP2P.On("Publish", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		captured = args.Get(2).([]byte)
	}).Return(nil)

	s := &Server{
		logger: &ulogger.TestLogger{},
		settings: &settings.Settings{
			ClientName: "bad\x00name" + strings.Repeat("c", 4*1024),
			P2P:        settings.P2PSettings{ListenMode: settings.ListenModeFull},
		},
		blockchainClient:    mockBlockchain,
		P2PClient:           mockP2P,
		notificationCh:      make(chan *notificationMsg, 1),
		nodeStatusTopicName: "test-node-status",
		AssetHTTPAddressURL: "http://example.com/" + strings.Repeat("p", maxGossipURLLen),
		PropagationURL:      "http://example.com/" + strings.Repeat("q", maxGossipURLLen),
	}

	require.NoError(t, s.handleNodeStatusNotification(context.Background()), "an invalid optional URL must not abort the publish")
	require.NotNil(t, captured, "the message must still be published")
	require.LessOrEqual(t, len(captured), maxNodeStatusMessageSize, "published payload must fit under the cap peers enforce")

	var published NodeStatusMessage
	require.NoError(t, json.Unmarshal(captured, &published))
	require.Empty(t, published.BaseURL, "the invalid BaseURL must be blanked, not published")
	require.Empty(t, published.PropagationURL, "the invalid PropagationURL must be blanked, not published")
	require.LessOrEqual(t, len(published.ClientName), maxPeerDisplayStringLen)

	// What we published must survive our own ingress validation unscathed.
	sanitizeNodeStatusMessage(&published)
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

// Exact boundary behaviour of the protocol-format bounds.
func TestGossipFieldBoundaries(t *testing.T) {
	require.NoError(t, checkGossipString("peer_id", strings.Repeat("a", maxGossipPeerIDLen), maxGossipPeerIDLen))
	require.Error(t, checkGossipString("peer_id", strings.Repeat("a", maxGossipPeerIDLen+1), maxGossipPeerIDLen))

	require.NoError(t, checkGossipHex("hash", strings.Repeat("0", maxGossipHashLen), maxGossipHashLen))
	require.Error(t, checkGossipHex("hash", strings.Repeat("0", maxGossipHashLen+1), maxGossipHashLen))

	require.NoError(t, checkGossipHex("header", strings.Repeat("f", maxGossipHeaderLen), maxGossipHeaderLen))
	require.Error(t, checkGossipHex("header", strings.Repeat("f", maxGossipHeaderLen+1), maxGossipHeaderLen))

	require.NoError(t, checkGossipString("url", strings.Repeat("u", maxGossipURLLen), maxGossipURLLen))
	require.Error(t, checkGossipString("url", strings.Repeat("u", maxGossipURLLen+1), maxGossipURLLen))
}

// Empty optional protocol-format values must never be scored: a node with no
// best block legitimately sends "", and older peers omit fields entirely. This
// pins the empty-allowed rule against any future "make the bounds exact"
// refactor, which would otherwise ban the whole network.
func TestHandleNodeStatusTopic_EmptyOptionalFieldsNotScored(t *testing.T) {
	server, remotePeerID, _, banScore := newGossipFieldTestServer(t)

	msgBytes, err := json.Marshal(NodeStatusMessage{PeerID: remotePeerID.String()})
	require.NoError(t, err)

	server.handleNodeStatusTopic(context.Background(), msgBytes, remotePeerID.String())

	select {
	case n := <-server.notificationCh:
		require.Equal(t, "node_status", n.Type)
	default:
		t.Fatal("node_status with empty optional fields must be forwarded")
	}

	require.Zero(t, banScore(), "empty optional fields must never be scored")
}

// The unconsumed Coinbase field must be ignored, not scored: no Teranode
// version populates it, and another implementation encoding it differently
// (e.g. base64) must not be banned over a field nothing reads.
func TestHandleBlockTopic_NonHexCoinbaseIgnored(t *testing.T) {
	server, remotePeerID, _, banScore := newGossipFieldTestServer(t)

	msgBytes, err := json.Marshal(BlockMessage{
		PeerID:     remotePeerID.String(),
		Hash:       testBlockHashHex,
		DataHubURL: "http://example.com:8090",
		Coinbase:   "bm90LWhleC1idXQtaGFybWxlc3M=",
	})
	require.NoError(t, err)

	server.handleBlockTopic(context.Background(), msgBytes, remotePeerID.String())

	select {
	case n := <-server.notificationCh:
		require.Equal(t, "block", n.Type)
	default:
		t.Fatal("block announcement with an odd coinbase encoding must still be processed")
	}

	require.Zero(t, banScore(), "the unconsumed coinbase field must not be scored")
}

// sanitizeFields must actually truncate the reason, independent of scoring.
func TestRejectedTxSanitizeFields_TruncatesReason(t *testing.T) {
	msg := RejectedTxMessage{Reason: strings.Repeat("x", maxGossipReasonLen+512)}
	msg.sanitizeFields()
	require.Len(t, msg.Reason, maxGossipReasonLen)
}

// The self-exemption from scoring must hold in every helper handler, not just
// node_status: our own loopback message with an invalid protocol field is
// dropped without the node scoring itself.
func TestHandleBlockTopic_OwnInvalidMessageDroppedWithoutSelfBan(t *testing.T) {
	server, _, reg, _ := newGossipFieldTestServer(t)
	selfID := server.P2PClient.GetID()
	reg.Register(&blockchain.PeerInfo{ID: selfID})

	msgBytes, err := json.Marshal(BlockMessage{
		PeerID:     selfID,
		Hash:       testBlockHashHex,
		DataHubURL: "http://example.com/" + strings.Repeat("p", maxGossipURLLen),
	})
	require.NoError(t, err)

	server.handleBlockTopic(context.Background(), msgBytes, selfID)

	requireNoNotification(t, server, "own block message with an invalid field must be dropped")
	info, ok := reg.Get(selfID)
	require.True(t, ok)
	require.Zero(t, info.BanScore, "a node must not score itself for its own block message")
}

func TestHandleSubtreeTopic_OwnInvalidMessageDroppedWithoutSelfBan(t *testing.T) {
	server, _, reg, _ := newGossipFieldTestServer(t)
	selfID := server.P2PClient.GetID()
	reg.Register(&blockchain.PeerInfo{ID: selfID})

	msgBytes, err := json.Marshal(SubtreeMessage{
		PeerID:     selfID,
		Hash:       testBlockHashHex,
		DataHubURL: "http://example.com/" + strings.Repeat("p", maxGossipURLLen),
	})
	require.NoError(t, err)

	server.handleSubtreeTopic(context.Background(), msgBytes, selfID)

	requireNoNotification(t, server, "own subtree message with an invalid field must be dropped")
	info, ok := reg.Get(selfID)
	require.True(t, ok)
	require.Zero(t, info.BanScore, "a node must not score itself for its own subtree message")
}

func TestHandleRejectedTxTopic_OwnInvalidMessageDroppedWithoutSelfBan(t *testing.T) {
	server, _, reg, _ := newGossipFieldTestServer(t)
	selfID := server.P2PClient.GetID()
	reg.Register(&blockchain.PeerInfo{ID: selfID})

	msgBytes, err := json.Marshal(RejectedTxMessage{
		PeerID: selfID,
		TxID:   "not-a-tx-id",
	})
	require.NoError(t, err)

	server.handleRejectedTxTopic(context.Background(), msgBytes, selfID)

	info, ok := reg.Get(selfID)
	require.True(t, ok)
	require.Zero(t, info.BanScore, "a node must not score itself for its own rejected_tx message")
}

// The property the tightened caps rest on: a legitimate message with every
// string field populated to exactly its bound still marshals under the topic
// cap. Coinbase stays empty — no Teranode version populates it, and the block
// cap's extra headroom exists precisely for it.
func TestFullyPopulatedMessagesFitUnderCaps(t *testing.T) {
	display := strings.Repeat("d", maxPeerDisplayStringLen)
	hexHash := strings.Repeat("0", maxGossipHashLen)
	url := "http://example.com/" + strings.Repeat("u", maxGossipURLLen-19)
	pid := strings.Repeat("p", maxGossipPeerIDLen)

	nodeStatus, err := json.Marshal(NodeStatusMessage{
		PeerID: pid, ClientName: display, Type: "node_status", BaseURL: url,
		PropagationURL: url, Version: display, CommitHash: display,
		BestBlockHash: hexHash, BestHeight: ^uint32(0), TxCount: ^uint64(0),
		SubtreeCount: ^uint32(0), FSMState: display, StartTime: 1<<63 - 1,
		Uptime: 1e300, MinerName: display, ListenMode: "listen_only",
		ChainWork: hexHash, SyncPeerID: pid, SyncPeerHeight: ^uint32(0),
		SyncPeerBlockHash: hexHash, SyncConnectedAt: 1<<63 - 1,
		ConnectedPeersCount: 1 << 31, Storage: "pruned",
	})
	require.NoError(t, err)
	require.Less(t, len(nodeStatus), maxNodeStatusMessageSize, "fully populated node_status must fit under its cap")

	block, err := json.Marshal(BlockMessage{
		PeerID: pid, ClientName: display, DataHubURL: url, Hash: hexHash,
		Height: ^uint32(0), Header: strings.Repeat("0", maxGossipHeaderLen),
	})
	require.NoError(t, err)
	require.Less(t, len(block), maxBlockMessageSize, "fully populated block message must fit under its cap")

	subtree, err := json.Marshal(SubtreeMessage{
		PeerID: pid, ClientName: display, DataHubURL: url, Hash: hexHash,
	})
	require.NoError(t, err)
	require.Less(t, len(subtree), maxSubtreeMessageSize, "fully populated subtree message must fit under its cap")

	rejected, err := json.Marshal(RejectedTxMessage{
		PeerID: pid, ClientName: display, TxID: hexHash,
		Reason: strings.Repeat("r", maxGossipReasonLen),
	})
	require.NoError(t, err)
	require.Less(t, len(rejected), maxRejectedTxMessageSize, "fully populated rejected_tx message must fit under its cap")
}

// A claimed PeerID that does not match the authenticated gossip sender is a
// protocol violation in the helper handlers too, not just node_status.
func TestHandleBlockTopic_SpoofedPeerIDScored(t *testing.T) {
	server, remotePeerID, _, banScore := newGossipFieldTestServer(t)

	msgBytes, err := json.Marshal(BlockMessage{
		PeerID:     mustNewPeerID(t).String(),
		Hash:       testBlockHashHex,
		DataHubURL: "http://example.com:8090",
	})
	require.NoError(t, err)

	server.handleBlockTopic(context.Background(), msgBytes, remotePeerID.String())

	requireNoNotification(t, server, "spoofed block message must be dropped")
	require.Positive(t, banScore(), "peer ID spoofing must be scored")
}

func TestHandleSubtreeTopic_SpoofedPeerIDScored(t *testing.T) {
	server, remotePeerID, _, banScore := newGossipFieldTestServer(t)

	msgBytes, err := json.Marshal(SubtreeMessage{
		PeerID:     mustNewPeerID(t).String(),
		Hash:       testBlockHashHex,
		DataHubURL: "http://example.com:8090",
	})
	require.NoError(t, err)

	server.handleSubtreeTopic(context.Background(), msgBytes, remotePeerID.String())

	requireNoNotification(t, server, "spoofed subtree message must be dropped")
	require.Positive(t, banScore(), "peer ID spoofing must be scored")
}

// capturePublishServer builds a Server whose Publish calls are recorded per
// topic, for exercising the egress publish paths end to end.
func capturePublishServer(t *testing.T) (*Server, map[string][]byte) {
	t.Helper()

	published := make(map[string][]byte)
	mockP2P := &MockServerP2PClient{peerID: mustNewPeerID(t)}
	mockP2P.On("Publish", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		published[args.Get(1).(string)] = args.Get(2).([]byte)
	}).Return(nil)

	s := &Server{
		logger: &ulogger.TestLogger{},
		settings: &settings.Settings{
			ClientName: "bad\x00name" + strings.Repeat("c", 4*1024),
			P2P:        settings.P2PSettings{ListenMode: settings.ListenModeFull},
		},
		P2PClient:           mockP2P,
		notificationCh:      make(chan *notificationMsg, 4),
		blockTopicName:      "test-block",
		subtreeTopicName:    "test-subtree",
		rejectedTxTopicName: "test-rejected",
		nodeStatusTopicName: "test-node-status",
		AssetHTTPAddressURL: "http://example.com:8090",
	}

	return s, published
}

// The block egress path must publish a message that is sanitized, under the
// topic cap, and clean under our own ingress validation.
func TestHandleBlockNotification_PublishesSanitizedValidatedMessage(t *testing.T) {
	s, published := capturePublishServer(t)

	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).Return(model.GenesisBlockHeader, &model.BlockHeaderMeta{Height: 42}, nil)
	mockBlockchain.On("GetFSMCurrentState", mock.Anything).Return(nil, assert.AnError).Maybe()
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(nil, nil, assert.AnError).Maybe()
	mockBlockchain.On("GetState", mock.Anything, mock.Anything).Return(nil, assert.AnError).Maybe()
	s.blockchainClient = mockBlockchain

	hash := model.GenesisBlockHeader.Hash()
	require.NoError(t, s.handleBlockNotification(context.Background(), hash))

	captured := published["test-block"]
	require.NotNil(t, captured, "block announcement must be published")
	require.LessOrEqual(t, len(captured), maxBlockMessageSize)

	var msg BlockMessage
	require.NoError(t, json.Unmarshal(captured, &msg))
	require.LessOrEqual(t, len(msg.ClientName), maxPeerDisplayStringLen, "hostile local client name must be sanitized on egress")
	require.Equal(t, hash.String(), msg.Hash)
	require.NoError(t, msg.validateFields(), "published block message must pass our own ingress validation")
}

// The subtree egress path must publish a message that is sanitized, under the
// topic cap, and clean under our own ingress validation.
func TestHandleSubtreeNotification_PublishesSanitizedValidatedMessage(t *testing.T) {
	s, published := capturePublishServer(t)

	hash := model.GenesisBlockHeader.Hash()
	require.NoError(t, s.handleSubtreeNotification(context.Background(), hash))

	captured := published["test-subtree"]
	require.NotNil(t, captured, "subtree announcement must be published")
	require.LessOrEqual(t, len(captured), maxSubtreeMessageSize)

	var msg SubtreeMessage
	require.NoError(t, json.Unmarshal(captured, &msg))
	require.LessOrEqual(t, len(msg.ClientName), maxPeerDisplayStringLen, "hostile local client name must be sanitized on egress")
	require.NoError(t, msg.validateFields(), "published subtree message must pass our own ingress validation")
}

// An internal rejection (empty peer_id) is re-broadcast with the validator's
// reason truncated to its bound, under the topic cap.
func TestRejectedTxHandler_InternalRejectionPublishesTruncatedReason(t *testing.T) {
	s, published := capturePublishServer(t)

	value, err := proto.Marshal(&kafkamessage.KafkaRejectedTxTopicMessage{
		TxHash: testBlockHashHex,
		PeerId: "",
		Reason: strings.Repeat("r", maxGossipReasonLen+500),
	})
	require.NoError(t, err)

	handler := s.rejectedTxHandler(context.Background())
	require.NoError(t, handler(&kafka.KafkaMessage{Value: value}))

	captured := published["test-rejected"]
	require.NotNil(t, captured, "internal rejection must be re-broadcast")
	require.LessOrEqual(t, len(captured), maxRejectedTxMessageSize)

	var msg RejectedTxMessage
	require.NoError(t, json.Unmarshal(captured, &msg))
	require.Len(t, msg.Reason, maxGossipReasonLen, "validator reason must be truncated on egress")
	require.NoError(t, msg.validateFields(), "published rejected_tx message must pass our own ingress validation")
}
