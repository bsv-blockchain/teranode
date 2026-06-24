package p2p

import (
	"context"
	"encoding/json"
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
