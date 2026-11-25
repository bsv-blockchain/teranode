package p2p

import (
	"context"
	"encoding/json"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newSizeLimitTestServer wires a minimal P2P Server whose pubsub-topic handlers
// can be exercised in isolation. The remote peer is pre-registered; the caller
// captures the post-register baseline time and asserts the handler did not
// advance it — that means the size guard short-circuited before any
// peer-state mutation.
func newSizeLimitTestServer(t *testing.T) (*Server, peer.ID, *blockchain.CentralizedPeerRegistry, time.Time) {
	t.Helper()

	priv1, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 0)
	require.NoError(t, err)
	selfPeerID, err := peer.IDFromPrivateKey(priv1)
	require.NoError(t, err)

	priv2, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 0)
	require.NoError(t, err)
	remotePeerID, err := peer.IDFromPrivateKey(priv2)
	require.NoError(t, err)

	mockP2P := new(MockServerP2PClient)
	mockP2P.peerID = selfPeerID

	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	client := blockchain.NewLocalPeerRegistryClient(reg)
	reg.Register(&blockchain.PeerInfo{ID: remotePeerID.String()})

	info, ok := reg.Get(remotePeerID.String())
	require.True(t, ok)
	baseline := info.LastMessageTime

	server := &Server{
		logger:       ulogger.TestLogger{},
		P2PClient:    mockP2P,
		peerRegistry: client,
		settings: &settings.Settings{
			P2P: settings.P2PSettings{
				ListenMode: settings.ListenModeFull,
			},
		},
		notificationCh: make(chan *notificationMsg, 10),
	}

	return server, remotePeerID, reg, baseline
}

func assertNoMessageTimeAdvance(t *testing.T, reg *blockchain.CentralizedPeerRegistry, id string, baseline time.Time, msg string) {
	t.Helper()
	info, ok := reg.Get(id)
	require.True(t, ok)
	assert.True(t, info.LastMessageTime.Equal(baseline), msg)
}

func TestHandleRejectedTxTopic_OversizedDropped(t *testing.T) {
	server, remotePeerID, reg, baseline := newSizeLimitTestServer(t)

	padding := make([]byte, maxRejectedTxMessageSize+1)
	for i := range padding {
		padding[i] = 'x'
	}
	msgBytes, err := json.Marshal(RejectedTxMessage{
		PeerID: remotePeerID.String(),
		TxID:   "abc",
		Reason: string(padding),
	})
	require.NoError(t, err)
	require.Greater(t, len(msgBytes), maxRejectedTxMessageSize)

	server.handleRejectedTxTopic(context.Background(), msgBytes, remotePeerID.String())

	assertNoMessageTimeAdvance(t, reg, remotePeerID.String(), baseline, "oversized rejected_tx must not advance LastMessageTime")
}

func TestHandleNodeStatusTopic_OversizedDropped(t *testing.T) {
	server, remotePeerID, reg, baseline := newSizeLimitTestServer(t)

	padding := make([]byte, maxNodeStatusMessageSize+1)
	for i := range padding {
		padding[i] = 'x'
	}
	msgBytes, err := json.Marshal(NodeStatusMessage{
		PeerID:     remotePeerID.String(),
		ClientName: string(padding),
	})
	require.NoError(t, err)
	require.Greater(t, len(msgBytes), maxNodeStatusMessageSize)

	server.handleNodeStatusTopic(context.Background(), msgBytes, remotePeerID.String())

	assertNoMessageTimeAdvance(t, reg, remotePeerID.String(), baseline, "oversized node_status must not advance LastMessageTime")
}

func TestHandleBlockTopic_OversizedDropped(t *testing.T) {
	server, remotePeerID, reg, baseline := newSizeLimitTestServer(t)

	padding := make([]byte, maxBlockMessageSize+1)
	for i := range padding {
		padding[i] = 'x'
	}
	msgBytes, err := json.Marshal(BlockMessage{
		PeerID:     remotePeerID.String(),
		Hash:       "deadbeef",
		ClientName: string(padding),
	})
	require.NoError(t, err)
	require.Greater(t, len(msgBytes), maxBlockMessageSize)

	server.handleBlockTopic(context.Background(), msgBytes, remotePeerID.String())

	assertNoMessageTimeAdvance(t, reg, remotePeerID.String(), baseline, "oversized block must not advance LastMessageTime")
}

func TestHandleSubtreeTopic_OversizedDropped(t *testing.T) {
	server, remotePeerID, reg, baseline := newSizeLimitTestServer(t)

	padding := make([]byte, maxSubtreeMessageSize+1)
	for i := range padding {
		padding[i] = 'x'
	}
	msgBytes, err := json.Marshal(SubtreeMessage{
		PeerID:     remotePeerID.String(),
		Hash:       "deadbeef",
		ClientName: string(padding),
	})
	require.NoError(t, err)
	require.Greater(t, len(msgBytes), maxSubtreeMessageSize)

	server.handleSubtreeTopic(context.Background(), msgBytes, remotePeerID.String())

	assertNoMessageTimeAdvance(t, reg, remotePeerID.String(), baseline, "oversized subtree must not advance LastMessageTime")
}

// TestTopicKindCaps_WithinGossipCeiling guards the invariant documented on
// maxGossipMessageSize: every per-topic ingress cap (and therefore every
// egress cap, since publishToNetwork uses the same table) stays at or under
// the 10KB ceiling. Teranode gossip payloads are ~1KB announcements; a cap
// drifting back up towards the libp2p 1MiB default would silently widen the
// pre-parse abuse surface on that topic.
func TestTopicKindCaps_WithinGossipCeiling(t *testing.T) {
	for _, kind := range []topicKind{topicKindBlock, topicKindSubtree, topicKindRejectedTx, topicKindNodeStatus} {
		require.Contains(t, topicKindCaps, kind, "every subscribed topic must have a cap")
	}

	for kind, capBytes := range topicKindCaps {
		require.LessOrEqual(t, capBytes, maxGossipMessageSize, "topic %s cap exceeds the gossip ceiling", kind)
		require.Positive(t, capBytes, "topic %s cap must be positive", kind)
	}
}

// TestGossipTopicValidators_CapEveryTopicBeforeRelay pins that every gossip
// topic gets a pre-relay validator carrying the same cap the handlers apply,
// keyed by the full wire topic name. A topic left out would go back to being
// relayed up to libp2p's 1MiB default before any Teranode check runs.
func TestGossipTopicValidators_CapEveryTopicBeforeRelay(t *testing.T) {
	names := map[topicKind]string{
		topicKindBlock:      "test-block",
		topicKindSubtree:    "test-subtree",
		topicKindRejectedTx: "test-rejected_tx",
		topicKindNodeStatus: "test-node_status",
	}

	validators := gossipTopicValidators(names)

	require.Len(t, validators, len(topicKindCaps))

	for kind, name := range names {
		require.Contains(t, validators, name)
		require.Equal(t, topicKindCaps[kind], validators[name].MaxPayloadSize, "topic %s", kind)
	}
}

// TestNewServerInstallsGossipTopicValidators drives the real constructor and
// publishes on each topic it built: a payload at the cap is accepted and one
// byte over is refused by the node's own pre-relay validator.
func TestNewServerInstallsGossipTopicValidators(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())

	tSettings := settings.NewSettings()
	tSettings.P2P.Port = port
	tSettings.P2P.ListenAddresses = []string{"0.0.0.0"}
	tSettings.P2P.PeerCacheDir = t.TempDir()
	tSettings.P2P.DHTMode = "off"
	tSettings.P2P.BootstrapPeers = nil
	tSettings.P2P.StaticPeers = nil
	tSettings.BlockChain.StoreURL, err = url.Parse("sqlitememory://")
	require.NoError(t, err)

	ResetBanListSingleton()
	t.Cleanup(ResetBanListSingleton)

	s, err := NewServer(t.Context(), ulogger.TestLogger{}, tSettings, nil, nil, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.P2PClient.Close() })

	topics := map[topicKind]string{
		topicKindBlock:      s.blockTopicName,
		topicKindSubtree:    s.subtreeTopicName,
		topicKindRejectedTx: s.rejectedTxTopicName,
		topicKindNodeStatus: s.nodeStatusTopicName,
	}

	for kind, name := range topics {
		capBytes := topicKindCaps[kind]
		require.NoError(t, s.P2PClient.Publish(t.Context(), name, make([]byte, capBytes)), "topic %s at cap", kind)
		require.Error(t, s.P2PClient.Publish(t.Context(), name, make([]byte, capBytes+1)), "topic %s over cap", kind)
	}
}
