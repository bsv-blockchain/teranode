package p2p

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

func newServerWithLocalRegistry(t *testing.T) (*Server, *blockchain.CentralizedPeerRegistry) {
	t.Helper()

	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	return &Server{
		peerRegistry: blockchain.NewLocalPeerRegistryClient(reg),
		logger:       ulogger.TestLogger{},
		gCtx:         context.Background(),
		settings: &settings.Settings{
			P2P: settings.P2PSettings{
				AllowPrunedNodeFallback:            true,
				MaxUnvalidatedAdvertisedHeightLead: 10_000,
			},
		},
	}, reg
}

func setServerLocalHeight(t *testing.T, s *Server, height uint32) {
	t.Helper()

	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(
		model.GenesisBlockHeader,
		&model.BlockHeaderMeta{Height: height},
		nil,
	).Maybe()
	s.blockchainClient = mockBlockchain
}

func mustNewPeerID(t *testing.T) peer.ID {
	t.Helper()
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 0)
	require.NoError(t, err)
	pid, err := peer.IDFromPrivateKey(priv)
	require.NoError(t, err)
	return pid
}

func TestServerHelpers_AddPeer_Registers(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)

	s.addPeer(pid, "client/1.0", 100, nil, "http://peer.example")

	got, ok := reg.Get(pid.String())
	require.True(t, ok)
	require.Equal(t, "client/1.0", got.ClientName)
	require.Equal(t, uint32(100), got.Height)
	require.False(t, got.IsConnected, "addPeer leaves IsConnected=false")
}

func TestServerHelpers_AddConnectedPeer_FlipsConnected(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)

	s.addConnectedPeer(pid, "", 50, nil, "")

	got, ok := reg.Get(pid.String())
	require.True(t, ok)
	require.True(t, got.IsConnected, "addConnectedPeer must flip the flag")
}

func TestServerHelpers_RemovePeer_DropsEntry(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)
	s.addConnectedPeer(pid, "", 1, nil, "")
	require.Equal(t, 1, reg.Count())

	s.removePeer(pid)

	require.Equal(t, 0, reg.Count())
}

func TestServerHelpers_GetPeer_FoundAndNotFound(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)

	_, found := s.getPeer(pid)
	require.False(t, found)

	s.addPeer(pid, "", 1, nil, "")
	got, found := s.getPeer(pid)
	require.True(t, found)
	require.Equal(t, pid.String(), got.ID)
}

func TestServerHelpers_UpdateStorage_PersistsMode(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)
	s.addPeer(pid, "", 1, nil, "")

	s.updateStorage(pid, "pruned")
	got, _ := reg.Get(pid.String())
	require.Equal(t, "pruned", got.Storage)

	// Empty mode must be a no-op (existing setting preserved).
	s.updateStorage(pid, "")
	got, _ = reg.Get(pid.String())
	require.Equal(t, "pruned", got.Storage)
}

func TestServerHelpers_InjectPeerForTesting_MarksFull(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)

	s.InjectPeerForTesting(pid, "test-client", "http://peer.example", 99, nil)

	got, ok := reg.Get(pid.String())
	require.True(t, ok)
	require.Equal(t, "full", got.Storage, "InjectPeerForTesting always sets storage=full")
	require.Equal(t, uint32(99), got.Height)
}

// TestGetNodeStatusMessage_CountsOnlyConnectedPeers is a regression test:
// ConnectedPeersCount in the node_status message must count only directly
// connected peers, not every peer known to the registry (gossiped peers stay
// registered with IsConnected=false).
func TestGetNodeStatusMessage_CountsOnlyConnectedPeers(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)

	s.addConnectedPeer(mustNewPeerID(t), "client/1.0", 10, nil, "")
	s.addConnectedPeer(mustNewPeerID(t), "client/1.0", 20, nil, "")
	s.addPeer(mustNewPeerID(t), "client/1.0", 30, nil, "") // gossiped, not connected

	msg := s.getNodeStatusMessage(context.Background())
	require.NotNil(t, msg)
	require.Equal(t, 2, msg.ConnectedPeersCount, "gossiped/disconnected registry peers must not be counted")
}

// TestGetNodeStatusMessage_SyncConnectionTimesBounded is a regression test:
// syncConnectionTimes must not grow unbounded — entries for previous sync peers
// are pruned on rotation, and the map is cleared when there is no sync peer.
func TestGetNodeStatusMessage_SyncConnectionTimesBounded(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	s.P2PClient = &MockServerP2PClient{peerID: mustNewPeerID(t)}
	s.syncCoordinator = &SyncCoordinator{}

	pidA := mustNewPeerID(t)
	pidB := mustNewPeerID(t)

	syncMapLen := func() int {
		n := 0
		s.syncConnectionTimes.Range(func(_, _ any) bool { n++; return true })
		return n
	}

	// Sync peer A selected: its connection time is recorded.
	s.syncCoordinator.currentSyncPeer = pidA.String()
	msg := s.getNodeStatusMessage(context.Background())
	require.NotNil(t, msg)
	_, ok := s.syncConnectionTimes.Load(pidA.String())
	require.True(t, ok, "current sync peer must be tracked")
	require.Equal(t, 1, syncMapLen())

	// Rotate to sync peer B: A's stale entry is pruned.
	s.syncCoordinator.currentSyncPeer = pidB.String()
	s.getNodeStatusMessage(context.Background())
	_, ok = s.syncConnectionTimes.Load(pidA.String())
	require.False(t, ok, "previous sync peer entry must be pruned on rotation")
	_, ok = s.syncConnectionTimes.Load(pidB.String())
	require.True(t, ok, "current sync peer must be tracked after rotation")
	require.Equal(t, 1, syncMapLen())

	// No sync peer: the map is cleared.
	s.syncCoordinator.currentSyncPeer = ""
	s.getNodeStatusMessage(context.Background())
	require.Zero(t, syncMapLen(), "map must be cleared when there is no sync peer")
}

func TestServerHelpers_GetSyncPeer_NoCoordinator(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	require.Empty(t, s.getSyncPeer())
}

func TestServerHelpers_GetPeerIDFromDataHubURL(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)

	require.Empty(t, s.getPeerIDFromDataHubURL("http://anywhere"))

	s.addPeer(pid, "", 1, nil, "http://datahub.example/api/v1")

	require.Equal(t, pid.String(), s.getPeerIDFromDataHubURL("http://datahub.example/api/v1"))
	require.Empty(t, s.getPeerIDFromDataHubURL("http://other.example"))
}

func TestServerHelpers_ShouldSkipBannedPeer_FlagsBanned(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)

	require.False(t, s.shouldSkipBannedPeer(pid.String(), "test"), "no ban → don't skip")

	// A purely registry-side ban is masked by the cached negative lookup until
	// the entry expires (reputationCacheTTL).
	reg.AddBanScore(pid.String(), "spam", 0)
	reg.AddBanScore(pid.String(), "spam", 0)
	require.False(t, s.shouldSkipBannedPeer(pid.String(), "test"), "cached negative lookup masks the ban briefly")

	// Once the cache entry expires (simulated by dropping it), the ban is honored.
	s.banStatusCache.Delete(pid.String())
	require.True(t, s.shouldSkipBannedPeer(pid.String(), "test"), "score-banned peer is skipped after cache expiry")
}

// TestServerHelpers_ShouldSkipBannedPeer_LocalBanImmediate verifies that a ban
// applied through the local transition path (applyBanScore → onPeerBanned)
// takes effect immediately, without waiting for the cached negative
// IsPeerBanned lookup to expire.
func TestServerHelpers_ShouldSkipBannedPeer_LocalBanImmediate(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	s.banList = noopBanList{}
	pid := mustNewPeerID(t)

	require.False(t, s.shouldSkipBannedPeer(pid.String(), "test"), "no ban → don't skip (caches negative)")

	// protocol_violation = 20 points; 6 hits cross the default 100 threshold
	// and trigger onPeerBanned, which overwrites the cached negative entry.
	for i := 0; i < 6; i++ {
		s.applyBanScore(pid.String(), ReasonProtocolViolation)
	}

	require.True(t, s.shouldSkipBannedPeer(pid.String(), "test"), "locally banned peer must be skipped immediately")
}

// TestGetLocalHeight_ErrorCachedWithShorterTTL: failed blockchain reads are
// cached (no per-message RPC storm during an outage) but with a shorter TTL
// than successful reads, so recovery is picked up quickly.
func TestGetLocalHeight_ErrorCachedWithShorterTTL(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)

	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(nil, nil, errors.NewServiceError("blockchain down")).Once()
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(model.GenesisBlockHeader, &model.BlockHeaderMeta{Height: 42}, nil)
	s.blockchainClient = mockBlockchain

	require.Equal(t, uint32(0), s.getLocalHeight(context.Background()), "failed read returns 0")
	require.Equal(t, uint32(0), s.getLocalHeight(context.Background()), "failure must be served from cache within the error TTL")
	mockBlockchain.AssertNumberOfCalls(t, "GetBestBlockHeader", 1)

	time.Sleep(localHeightErrorCacheTTL + 50*time.Millisecond)
	require.Equal(t, uint32(42), s.getLocalHeight(context.Background()), "recovery must be picked up after the error TTL")
}

func TestServerHelpers_ShouldSkipUnhealthyPeer(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)

	// Unknown peer ID (not in registry) — must not skip; that's a relay path.
	require.False(t, s.shouldSkipUnhealthyPeer(pid.String(), "test"))

	// Register and drive reputation below threshold.
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})
	reg.UpdateMetrics(pid.String(), 0, 0, 0, false, false, true, 0)
	require.True(t, s.shouldSkipUnhealthyPeer(pid.String(), "test"))

	// Non-decodable IDs (hostname-like) are not skipped.
	require.False(t, s.shouldSkipUnhealthyPeer("not-an-id", "test"))
}

// TestServerHelpers_HandleBlockTopic_LowReputationPeerStillForwarded is a
// regression test: block announcements must NOT be filtered by peer reputation.
// A node that is behind may only have low-reputation peers available, and these
// announcements are what trigger catchup — dropping them would stop catchup from
// ever starting. Block validation is the gatekeeper for bad blocks. If a
// shouldSkipUnhealthyPeer filter were (re)introduced into handleBlockTopic, the
// Kafka publish asserted below would never happen.
func TestServerHelpers_HandleBlockTopic_LowReputationPeerStillForwarded(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)

	self := mustNewPeerID(t)
	mockP2P := new(MockServerP2PClient)
	mockP2P.peerID = self
	s.P2PClient = mockP2P
	s.notificationCh = make(chan *notificationMsg, 1)

	producer := kafka.NewKafkaAsyncProducerMock()
	s.blocksKafkaProducerClient = producer

	// Register the originating peer and pin its reputation to 5.0 (well below the
	// 20.0 unhealthy threshold) via a malicious-interaction metric, which does not
	// ban the peer.
	lowRep := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: lowRep.String()})
	reg.UpdateMetrics(lowRep.String(), 0, 0, 0, false, false, true, 0)

	require.True(t, s.shouldSkipUnhealthyPeer(lowRep.String(), "precondition"),
		"precondition: peer must be below the unhealthy threshold")
	require.False(t, s.shouldSkipBannedPeer(lowRep.String(), "precondition"),
		"precondition: peer must be unhealthy but not banned")

	const blockHash = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
	msg := fmt.Sprintf(`{"Hash":"%s","Height":1,"DataHubURL":"http://example.com","PeerID":"%s"}`,
		blockHash, lowRep.String())

	s.handleBlockTopic(context.Background(), []byte(msg), lowRep.String())

	select {
	case published := <-producer.PublishChannel():
		require.Equal(t, blockHash, string(published.Key),
			"low-reputation peer's block must be forwarded to Kafka")
	default:
		t.Fatal("block from low-reputation peer was not published to Kafka (filtered?)")
	}
}

func TestHandleNodeStatusTopic_BoundsInflatedAdvertisedHeight(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	setServerLocalHeight(t, s, 100)

	self := mustNewPeerID(t)
	mockP2P := new(MockServerP2PClient)
	mockP2P.peerID = self
	s.P2PClient = mockP2P
	s.notificationCh = make(chan *notificationMsg, 1)

	remote := mustNewPeerID(t)
	const blockHash = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
	msgBytes, err := json.Marshal(NodeStatusMessage{
		PeerID:        remote.String(),
		ClientName:    "client/1.0",
		BaseURL:       "http://peer.example",
		BestHeight:    1_000_000,
		BestBlockHash: blockHash,
	})
	require.NoError(t, err)

	s.handleNodeStatusTopic(context.Background(), msgBytes, remote.String())

	expectedHeight := uint32(10_100)
	got, ok := reg.Get(remote.String())
	require.True(t, ok)
	require.Equal(t, expectedHeight, got.Height)
	require.NotNil(t, got.BlockHash)
	require.Equal(t, blockHash, got.BlockHash.String())

	select {
	case notification := <-s.notificationCh:
		require.Equal(t, expectedHeight, notification.BestHeight)
		require.Equal(t, blockHash, notification.BestBlockHash)
	default:
		t.Fatal("expected node_status notification")
	}
}

func TestHandleBlockTopic_BoundsInflatedAdvertisedHeight(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	setServerLocalHeight(t, s, 100)

	self := mustNewPeerID(t)
	mockP2P := new(MockServerP2PClient)
	mockP2P.peerID = self
	s.P2PClient = mockP2P
	s.notificationCh = make(chan *notificationMsg, 1)

	remote := mustNewPeerID(t)
	blockHash := chainhash.HashH([]byte("inflated advertised block")).String()
	msgBytes, err := json.Marshal(BlockMessage{
		PeerID:     remote.String(),
		ClientName: "client/1.0",
		DataHubURL: "http://peer.example",
		Hash:       blockHash,
		Height:     1_000_000,
	})
	require.NoError(t, err)

	s.handleBlockTopic(context.Background(), msgBytes, remote.String())

	expectedHeight := uint32(10_100)
	got, ok := reg.Get(remote.String())
	require.True(t, ok)
	require.Equal(t, expectedHeight, got.Height)
	require.NotNil(t, got.BlockHash)
	require.Equal(t, blockHash, got.BlockHash.String())

	select {
	case notification := <-s.notificationCh:
		require.Equal(t, expectedHeight, notification.Height)
		require.Equal(t, blockHash, notification.Hash)
	default:
		t.Fatal("expected block notification")
	}
}

func TestHandleBlockTopic_RejectsMalformedAdvertisedHash(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)

	self := mustNewPeerID(t)
	mockP2P := new(MockServerP2PClient)
	mockP2P.peerID = self
	s.P2PClient = mockP2P
	s.notificationCh = make(chan *notificationMsg, 1)

	producer := kafka.NewKafkaAsyncProducerMock()
	s.blocksKafkaProducerClient = producer

	remote := mustNewPeerID(t)
	msgBytes, err := json.Marshal(BlockMessage{
		PeerID:     remote.String(),
		ClientName: "client/1.0",
		DataHubURL: "http://peer.example",
		Hash:       "not-a-block-hash",
		Height:     1_000_000,
	})
	require.NoError(t, err)

	s.handleBlockTopic(context.Background(), msgBytes, remote.String())

	_, ok := reg.Get(remote.String())
	require.False(t, ok)

	select {
	case notification := <-s.notificationCh:
		t.Fatalf("unexpected block notification for malformed hash: %+v", notification)
	default:
	}

	select {
	case published := <-producer.PublishChannel():
		t.Fatalf("unexpected Kafka publish for malformed hash: %+v", published)
	default:
	}

	entries := 0
	s.blockPeerMap.Range(func(_, _ any) bool { entries++; return true })
	require.Zero(t, entries, "malformed hash must not create a blockPeerMap entry")
}

// chainhash.NewHashFromStr accepts non-canonical hex forms (uppercase,
// truncated), while the ban lookups in ReportInvalidBlock and
// processInvalidBlockMessage use the canonical hash.String() from block
// validation. The blockPeerMap must therefore be keyed by the canonical form,
// or a peer could evade the invalid-block ban by announcing the block with a
// non-canonical hash string.
func TestHandleBlockTopic_PeerMapKeyedByCanonicalHash(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	setServerLocalHeight(t, s, 100)

	self := mustNewPeerID(t)
	mockP2P := new(MockServerP2PClient)
	mockP2P.peerID = self
	s.P2PClient = mockP2P
	s.notificationCh = make(chan *notificationMsg, 1)

	remote := mustNewPeerID(t)
	canonical := chainhash.HashH([]byte("canonical block key")).String()
	msgBytes, err := json.Marshal(BlockMessage{
		PeerID:     remote.String(),
		ClientName: "client/1.0",
		DataHubURL: "http://peer.example",
		Hash:       strings.ToUpper(canonical),
		Height:     101,
	})
	require.NoError(t, err)

	s.handleBlockTopic(context.Background(), msgBytes, remote.String())

	peerID, err := s.getPeerFromMap(&s.blockPeerMap, canonical, "block")
	require.NoError(t, err, "blockPeerMap must be keyed by the canonical hash string")
	require.Equal(t, remote.String(), peerID)

	_, err = s.getPeerFromMap(&s.blockPeerMap, strings.ToUpper(canonical), "block")
	require.Error(t, err, "raw non-canonical announce string must not be a map key")
}

// Same canonical-key requirement as the block map: ReportInvalidSubtree looks
// up subtreePeerMap with the canonical hash.String() from subtree validation.
func TestHandleSubtreeTopic_PeerMapKeyedByCanonicalHash(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)

	self := mustNewPeerID(t)
	mockP2P := new(MockServerP2PClient)
	mockP2P.peerID = self
	s.P2PClient = mockP2P
	s.notificationCh = make(chan *notificationMsg, 1)

	remote := mustNewPeerID(t)
	canonical := chainhash.HashH([]byte("canonical subtree key")).String()
	msgBytes, err := json.Marshal(SubtreeMessage{
		PeerID:     remote.String(),
		ClientName: "client/1.0",
		DataHubURL: "http://peer.example",
		Hash:       strings.ToUpper(canonical),
	})
	require.NoError(t, err)

	s.handleSubtreeTopic(context.Background(), msgBytes, remote.String())

	peerID, err := s.getPeerFromMap(&s.subtreePeerMap, canonical, "subtree")
	require.NoError(t, err, "subtreePeerMap must be keyed by the canonical hash string")
	require.Equal(t, remote.String(), peerID)

	_, err = s.getPeerFromMap(&s.subtreePeerMap, strings.ToUpper(canonical), "subtree")
	require.Error(t, err, "raw non-canonical announce string must not be a map key")

	select {
	case notification := <-s.notificationCh:
		require.Equal(t, canonical, notification.Hash,
			"subtree notification must carry the canonical hash")
	default:
		t.Fatal("expected subtree notification")
	}
}

// A malformed subtree hash must be rejected before any use: no WebSocket
// notification, no peerMapEntry, and no peer-activity credit.
func TestHandleSubtreeTopic_RejectsMalformedHash(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)

	self := mustNewPeerID(t)
	mockP2P := new(MockServerP2PClient)
	mockP2P.peerID = self
	s.P2PClient = mockP2P
	s.notificationCh = make(chan *notificationMsg, 1)

	remote := mustNewPeerID(t)
	msgBytes, err := json.Marshal(SubtreeMessage{
		PeerID:     remote.String(),
		ClientName: "client/1.0",
		DataHubURL: "http://peer.example",
		Hash:       "not-a-subtree-hash",
	})
	require.NoError(t, err)

	s.handleSubtreeTopic(context.Background(), msgBytes, remote.String())

	select {
	case notification := <-s.notificationCh:
		t.Fatalf("unexpected subtree notification for malformed hash: %+v", notification)
	default:
	}

	entries := 0
	s.subtreePeerMap.Range(func(_, _ any) bool { entries++; return true })
	require.Zero(t, entries, "malformed hash must not create a subtreePeerMap entry")

	_, ok := reg.Get(remote.String())
	require.False(t, ok, "malformed hash must not count as peer activity")
}

// End-to-end guard for the canonical-key fix: a peer that announces an
// invalid block using a non-canonical hex form (uppercase) must still be
// banned when block validation reports the block by its canonical hash.
func TestHandleBlockTopic_NonCanonicalAnnouncerStillBanned(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	setServerLocalHeight(t, s, 100)

	self := mustNewPeerID(t)
	mockP2P := new(MockServerP2PClient)
	mockP2P.peerID = self
	s.P2PClient = mockP2P
	s.notificationCh = make(chan *notificationMsg, 1)

	remote := mustNewPeerID(t)
	canonical := chainhash.HashH([]byte("invalid block announced uppercase")).String()
	msgBytes, err := json.Marshal(BlockMessage{
		PeerID:     remote.String(),
		ClientName: "client/1.0",
		DataHubURL: "http://peer.example",
		Hash:       strings.ToUpper(canonical),
		Height:     101,
	})
	require.NoError(t, err)

	s.handleBlockTopic(context.Background(), msgBytes, remote.String())

	// Block validation reports the invalid block by its canonical hash.
	invalidMsg := mustMarshalInvalidBlockMsg(t, canonical, "invalid block")
	require.NoError(t, s.processInvalidBlockMessage(&kafka.KafkaMessage{Value: invalidMsg}))

	info, ok := reg.Get(remote.String())
	require.True(t, ok)
	require.Positive(t, info.BanScore, "announcer of the invalid block must be banned")

	_, stillThere := s.blockPeerMap.Load(canonical)
	require.False(t, stillThere, "entry must be removed after the ban")
}

func TestHandleNodeStatusTopic_RejectsMalformedAdvertisedHash(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	setServerLocalHeight(t, s, 100)

	self := mustNewPeerID(t)
	mockP2P := new(MockServerP2PClient)
	mockP2P.peerID = self
	s.P2PClient = mockP2P
	s.notificationCh = make(chan *notificationMsg, 1)

	remote := mustNewPeerID(t)
	msgBytes, err := json.Marshal(NodeStatusMessage{
		PeerID:        remote.String(),
		ClientName:    "client/1.0",
		BaseURL:       "http://peer.example",
		BestHeight:    1_000_000,
		BestBlockHash: "not-a-block-hash",
	})
	require.NoError(t, err)

	s.handleNodeStatusTopic(context.Background(), msgBytes, remote.String())

	select {
	case notification := <-s.notificationCh:
		require.Equal(t, uint32(0), notification.BestHeight)
		require.Empty(t, notification.BestBlockHash)
	default:
		t.Fatal("expected node_status notification")
	}

	got, ok := reg.Get(remote.String())
	require.True(t, ok)
	require.Equal(t, uint32(0), got.Height)
	require.Nil(t, got.BlockHash)
	require.Empty(t, got.ClientName)
	require.Empty(t, got.DataHubURL)
}

func TestServerHelpers_AddProtocolViolation_AccumulatesScore(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	s.banList = noopBanList{}
	pid := mustNewPeerID(t)

	for i := 0; i < 6; i++ {
		s.addProtocolViolation(pid.String())
	}

	// onPeerBanned removes the peer entry once the threshold is crossed, so
	// check the ban via the registry's IsBannedPeer rather than reading IsBanned
	// off PeerInfo (which is gone).
	require.True(t, reg.IsBannedPeer(pid.String()),
		"protocol_violation = 20; 6 hits = 120 should ban")
}

func TestServerHelpers_ApplyBanScore_NilRegistryNoPanic(t *testing.T) {
	s := &Server{logger: ulogger.TestLogger{}, gCtx: context.Background()}
	require.NotPanics(t, func() { s.applyBanScore("anything", "spam") })
}

func TestServerHelpers_OnPeerBanned_InvalidIDReturnsCleanly(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	require.NotPanics(t, func() { s.onPeerBanned("not-a-peer-id", "spam") })
}

func TestServerHelpers_OnPeerBanned_NoP2PClientStillRemovesPeer(t *testing.T) {
	// onPeerBanned now reads s.settings.P2P.BanDuration; with a custom value
	// set, the helper must still finish the libp2p-side cleanup without
	// panicking when P2PClient is nil (matches the ban-list-only deployment).
	s, reg := newServerWithLocalRegistry(t)
	s.settings.P2P.BanDuration = 7 * time.Minute
	pid := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	require.NotPanics(t, func() { s.onPeerBanned(pid.String(), "spam") })

	_, ok := reg.Get(pid.String())
	require.False(t, ok, "removePeer must run even with no P2PClient")
}

func TestServer_UpdateBytesReceived_SenderDelta(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	s.updateBytesReceived(pid.String(), "", 1024)
	s.updateBytesReceived(pid.String(), "", 256)

	got, _ := reg.Get(pid.String())
	require.Equal(t, uint64(1280), got.BytesReceived, "delta path must accumulate without read-modify-write")
}

func TestServer_UpdateBytesReceived_GossipUpdatesBoth(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	sender := mustNewPeerID(t)
	originator := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: sender.String()})
	reg.Register(&blockchain.PeerInfo{ID: originator.String()})

	s.updateBytesReceived(sender.String(), originator.String(), 500)

	gotSender, _ := reg.Get(sender.String())
	gotOriginator, _ := reg.Get(originator.String())
	require.Equal(t, uint64(500), gotSender.BytesReceived)
	require.Equal(t, uint64(500), gotOriginator.BytesReceived)
}

func TestServer_UpdateBytesReceived_SameSenderAndOriginatorOnlyOnce(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	// When the originator equals the sender we should NOT double-count.
	s.updateBytesReceived(pid.String(), pid.String(), 100)

	got, _ := reg.Get(pid.String())
	require.Equal(t, uint64(100), got.BytesReceived)
}

func TestServer_UpdateBytesReceived_BadIDIsLoggedNotPanicked(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	require.NotPanics(t, func() { s.updateBytesReceived("not-a-peer-id", "", 10) })
}

func TestServer_UpdateBytesReceived_NilRegistryNoOp(t *testing.T) {
	s := &Server{logger: ulogger.TestLogger{}, gCtx: context.Background()}
	require.NotPanics(t, func() { s.updateBytesReceived("any", "", 10) })
}

func TestIsUnsafeIP(t *testing.T) {
	tests := []struct {
		name     string
		ip       string
		expected string
	}{
		// Safe public IPs
		{"public_ipv4", "8.8.8.8", ""},
		{"public_ipv4_2", "1.1.1.1", ""},
		{"public_ipv6", "2607:f8b0:4004:800::200e", ""},

		// Loopback addresses
		{"loopback_ipv4", "127.0.0.1", "loopback address"},
		{"loopback_ipv4_other", "127.0.0.2", "loopback address"},
		{"loopback_ipv6", "::1", "loopback address"},

		// Private addresses
		{"private_10", "10.0.0.1", "private address"},
		{"private_172", "172.16.0.1", "private address"},
		{"private_192", "192.168.1.1", "private address"},
		{"private_ipv6", "fd00::1", "private address"},

		// Link-local addresses
		{"linklocal_ipv4", "169.254.1.1", "link-local address"},
		{"linklocal_ipv6", "fe80::1", "link-local address"},

		// Unspecified addresses
		{"unspecified_ipv4", "0.0.0.0", "unspecified address"},
		{"unspecified_ipv6", "::", "unspecified address"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip := net.ParseIP(tt.ip)
			require.NotNil(t, ip, "Failed to parse IP: %s", tt.ip)
			result := isUnsafeIP(ip)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsLocalhostHostname(t *testing.T) {
	tests := []struct {
		hostname string
		expected bool
	}{
		{"localhost", true},
		{"sub.localhost", true},
		{"deep.sub.localhost", true},
		{"example.com", false},
		{"localhosted.com", false},
		{"notlocalhost", false},
		{"localhost.example.com", false},
	}

	for _, tt := range tests {
		t.Run(tt.hostname, func(t *testing.T) {
			result := isLocalhostHostname(tt.hostname)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValidateDataHubURL(t *testing.T) {
	server := &Server{
		logger: ulogger.New("test"),
	}

	tests := []struct {
		name        string
		url         string
		expectError bool
		errorMsg    string
	}{
		// Valid URLs
		{"valid_http", "http://example.com/api/v1", false, ""},
		{"valid_https", "https://example.com/api/v1", false, ""},
		{"valid_with_port", "http://example.com:8080/api", false, ""},
		{"valid_public_ip", "http://8.8.8.8/api", false, ""},
		{"valid_public_ipv6", "http://[2607:f8b0:4004:800::200e]/api", false, ""},

		// Empty URL
		{"empty_url", "", true, "empty"},

		// Invalid scheme
		{"ftp_scheme", "ftp://example.com/file", true, "invalid scheme"},
		{"file_scheme", "file:///etc/passwd", true, "invalid scheme"},
		{"no_scheme", "example.com/api", true, "invalid scheme"},

		// No hostname
		{"no_hostname", "http:///path", true, "no hostname"},

		// Loopback addresses
		{"loopback_127", "http://127.0.0.1/api", true, "loopback"},
		{"loopback_127_other", "http://127.0.0.2:8080/api", true, "loopback"},
		{"loopback_ipv6", "http://[::1]/api", true, "loopback"},

		// Private addresses
		{"private_10", "http://10.0.0.1/api", true, "private"},
		{"private_172", "http://172.16.0.1/api", true, "private"},
		{"private_192", "http://192.168.1.1/api", true, "private"},

		// Link-local addresses
		{"linklocal_169", "http://169.254.1.1/api", true, "link-local"},
		{"linklocal_ipv6", "http://[fe80::1]/api", true, "link-local"},

		// Unspecified addresses
		{"unspecified_0000", "http://0.0.0.0/api", true, "unspecified"},

		// Localhost hostname
		{"localhost", "http://localhost/api", true, "localhost"},
		{"localhost_port", "http://localhost:8080/api", true, "localhost"},
		{"sub_localhost", "http://sub.localhost/api", true, "localhost"},

		// Rooted FQDN (trailing dots, even multiple) must not bypass the checks
		{"localhost_trailing_dot", "http://localhost./api", true, "localhost"},
		{"loopback_trailing_dot", "http://127.0.0.1./api", true, "loopback"},
		{"localhost_double_trailing_dot", "http://localhost../api", true, "localhost"},
		{"loopback_double_trailing_dot", "http://127.0.0.1../api", true, "loopback"},

		// Hostname case must not bypass the checks (DNS is case-insensitive)
		{"localhost_uppercase", "http://LOCALHOST/api", true, "localhost"},
		{"localhost_mixed_case_trailing_dot", "http://LocalHost./api", true, "localhost"},
		{"sub_localhost_uppercase", "http://sub.LOCALHOST/api", true, "localhost"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := server.validateDataHubURL(tt.url)
			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestCleanupPeerMaps_EvictsExpiredReputationEntries confirms that the
// reputationCache populated by shouldSkipUnhealthyPeer does not grow without
// bound — cleanupPeerMaps must sweep entries whose expiresAt has passed.
func TestCleanupPeerMaps_EvictsExpiredReputationEntries(t *testing.T) {
	s := &Server{
		logger:         ulogger.TestLogger{},
		peerMapTTL:     time.Minute,
		peerMapMaxSize: 100,
	}

	now := time.Now()
	s.reputationCache.Store("expired-peer", reputationCacheEntry{
		score:     75.0,
		expiresAt: now.Add(-time.Second),
	})
	s.reputationCache.Store("fresh-peer", reputationCacheEntry{
		score:     75.0,
		expiresAt: now.Add(reputationCacheTTL),
	})

	s.cleanupPeerMaps()

	_, expiredStillThere := s.reputationCache.Load("expired-peer")
	assert.False(t, expiredStillThere, "expired reputationCache entry must be evicted")
	_, freshStillThere := s.reputationCache.Load("fresh-peer")
	assert.True(t, freshStillThere, "fresh reputationCache entry must survive cleanup")
}

func TestServerHelpers_SanitizeAdvertisedTip_ClampsAndOverflow(t *testing.T) {
	const validHash = "0000000000000000000000000000000000000000000000000000000000000001"
	pid := mustNewPeerID(t).String()

	t.Run("within lead is returned unchanged", func(t *testing.T) {
		s, _ := newServerWithLocalRegistry(t)
		s.settings.P2P.MaxUnvalidatedAdvertisedHeightLead = 50
		height, hash, ok := s.sanitizeAdvertisedTip(pid, 140, validHash, 100)
		require.True(t, ok)
		require.NotNil(t, hash)
		require.Equal(t, uint32(140), height)
	})

	t.Run("beyond lead is capped to local height plus lead", func(t *testing.T) {
		s, _ := newServerWithLocalRegistry(t)
		s.settings.P2P.MaxUnvalidatedAdvertisedHeightLead = 10
		height, hash, ok := s.sanitizeAdvertisedTip(pid, 1000, validHash, 100)
		require.True(t, ok)
		require.NotNil(t, hash)
		require.Equal(t, uint32(110), height, "capped to localHeight+maxLead")
	})

	t.Run("uint32 overflow clamps the ceiling instead of wrapping", func(t *testing.T) {
		s, _ := newServerWithLocalRegistry(t)
		s.settings.P2P.MaxUnvalidatedAdvertisedHeightLead = 100
		localHeight := ^uint32(0) - 5 // localHeight+maxLead overflows uint32
		height, hash, ok := s.sanitizeAdvertisedTip(pid, ^uint32(0), validHash, localHeight)
		require.True(t, ok)
		require.NotNil(t, hash)
		require.Equal(t, ^uint32(0), height,
			"ceiling clamps to max uint32 so a near-max advertisement is accepted, not wrapped")
	})

	t.Run("invalid hash is rejected", func(t *testing.T) {
		s, _ := newServerWithLocalRegistry(t)
		height, hash, ok := s.sanitizeAdvertisedTip(pid, 100, "not-a-hash", 100)
		require.False(t, ok)
		require.Nil(t, hash)
		require.Equal(t, uint32(0), height)
	})
}

// TestHandleBlockTopic_RejectsBlacklistedDataHubURL guards the fix for block
// announcements bypassing the DataHub URL blacklist that subtree announcements
// enforce: a blacklisted host must be dropped before any WebSocket forwarding,
// peer registration, or Kafka publish (which would otherwise trigger catchup
// from the blacklisted host).
func TestHandleBlockTopic_RejectsBlacklistedDataHubURL(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	s.settings.SubtreeValidation.BlacklistedBaseURLs = map[string]struct{}{"http://evil.example": {}}

	self := mustNewPeerID(t)
	mockP2P := new(MockServerP2PClient)
	mockP2P.peerID = self
	s.P2PClient = mockP2P
	s.notificationCh = make(chan *notificationMsg, 1)

	producer := kafka.NewKafkaAsyncProducerMock()
	s.blocksKafkaProducerClient = producer

	for _, dataHubURL := range []string{
		"http://evil.example:8080/path", // same host as blacklist entry, different port/path
		"http://evil.example./path",     // rooted FQDN (trailing dot) must not bypass the blacklist
	} {
		remote := mustNewPeerID(t)
		blockHash := chainhash.HashH([]byte("blacklisted block " + dataHubURL)).String()
		msgBytes, err := json.Marshal(BlockMessage{
			PeerID:     remote.String(),
			ClientName: "client/1.0",
			DataHubURL: dataHubURL,
			Hash:       blockHash,
			Height:     1,
		})
		require.NoError(t, err)

		s.handleBlockTopic(context.Background(), msgBytes, remote.String())

		select {
		case notification := <-s.notificationCh:
			t.Fatalf("unexpected block notification for blacklisted DataHubURL %s: %+v", dataHubURL, notification)
		default:
		}

		_, ok := reg.Get(remote.String())
		require.False(t, ok, "peer with blacklisted DataHubURL %s must not be registered", dataHubURL)

		select {
		case published := <-producer.PublishChannel():
			t.Fatalf("unexpected Kafka publish for blacklisted DataHubURL %s: %+v", dataHubURL, published)
		default:
		}

		require.False(t, s.shouldSkipBannedPeer(remote.String(), "test"),
			"blacklist match is an operator choice, not a peer protocol violation")
	}
}

// TestHandleSubtreeTopic_RejectsBlacklistedDataHubURL is a regression test:
// the subtree handler enforced the blacklist before the checks were factored
// into checkDataHubURL and must keep doing so.
func TestHandleSubtreeTopic_RejectsBlacklistedDataHubURL(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	s.settings.SubtreeValidation.BlacklistedBaseURLs = map[string]struct{}{"http://evil.example": {}}

	self := mustNewPeerID(t)
	mockP2P := new(MockServerP2PClient)
	mockP2P.peerID = self
	s.P2PClient = mockP2P
	s.notificationCh = make(chan *notificationMsg, 1)

	producer := kafka.NewKafkaAsyncProducerMock()
	s.subtreeKafkaProducerClient = producer

	for _, dataHubURL := range []string{
		"http://evil.example",    // exact match
		"http://evil.example./x", // rooted FQDN (trailing dot) must not bypass the blacklist
	} {
		remote := mustNewPeerID(t)
		subtreeHash := chainhash.HashH([]byte("blacklisted subtree " + dataHubURL)).String()
		msgBytes, err := json.Marshal(SubtreeMessage{
			PeerID:     remote.String(),
			ClientName: "client/1.0",
			DataHubURL: dataHubURL,
			Hash:       subtreeHash,
		})
		require.NoError(t, err)

		s.handleSubtreeTopic(context.Background(), msgBytes, remote.String())

		select {
		case notification := <-s.notificationCh:
			t.Fatalf("unexpected subtree notification for blacklisted DataHubURL %s: %+v", dataHubURL, notification)
		default:
		}

		select {
		case published := <-producer.PublishChannel():
			t.Fatalf("unexpected Kafka publish for blacklisted DataHubURL %s: %+v", dataHubURL, published)
		default:
		}
	}
}

// TestHandleNodeStatusTopic_StripsBlacklistedBaseURL: node_status stores the
// announced BaseURL in the peer registry as the peer's DataHub URL (used by
// catchup), so a blacklisted BaseURL must never be persisted. Unlike the
// block/subtree handlers the message itself is telemetry and is kept: it is
// still forwarded to WebSocket clients, just with the URL removed.
func TestHandleNodeStatusTopic_StripsBlacklistedBaseURL(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	s.settings.SubtreeValidation.BlacklistedBaseURLs = map[string]struct{}{"http://evil.example": {}}
	setServerLocalHeight(t, s, 100)

	self := mustNewPeerID(t)
	mockP2P := new(MockServerP2PClient)
	mockP2P.peerID = self
	s.P2PClient = mockP2P
	s.notificationCh = make(chan *notificationMsg, 1)

	remote := mustNewPeerID(t)
	blockHash := chainhash.HashH([]byte("blacklisted node status")).String()
	msgBytes, err := json.Marshal(NodeStatusMessage{
		PeerID:        remote.String(),
		ClientName:    "client/1.0",
		BaseURL:       "http://evil.example./api", // rooted FQDN (trailing dot) must not bypass the blacklist
		BestHeight:    101,
		BestBlockHash: blockHash,
	})
	require.NoError(t, err)

	s.handleNodeStatusTopic(context.Background(), msgBytes, remote.String())

	select {
	case notification := <-s.notificationCh:
		require.Empty(t, notification.BaseURL, "blacklisted BaseURL must be stripped from the WebSocket notification")
		require.Equal(t, remote.String(), notification.PeerID)
	default:
		t.Fatal("node_status telemetry must still be forwarded to WebSocket clients")
	}

	got, ok := reg.Get(remote.String())
	require.True(t, ok, "peer telemetry must still be processed")
	require.Empty(t, got.DataHubURL, "blacklisted BaseURL must not be stored in the peer registry")

	require.False(t, s.shouldSkipBannedPeer(remote.String(), "test"),
		"blacklist match is an operator choice, not a peer protocol violation")
}

// TestBlacklistedDataHubURL_StoredBeforeBlacklist_NotUsedForCatchup is the
// regression for the primary operational case: a peer registers its DataHub
// URL while the host is not yet blacklisted, the operator then blacklists it.
// The node_status strip cannot evict the already-stored URL (the registry
// ignores empty-URL updates), so the blacklist must be enforced at the point
// of use - GetPeersForCatchup must stop surfacing the peer.
func TestBlacklistedDataHubURL_StoredBeforeBlacklist_NotUsedForCatchup(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	setServerLocalHeight(t, s, 100)

	self := mustNewPeerID(t)
	mockP2P := new(MockServerP2PClient)
	mockP2P.peerID = self
	s.P2PClient = mockP2P
	s.notificationCh = make(chan *notificationMsg, 4)

	// Register the peer's URL BEFORE the host is blacklisted.
	remote := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: remote.String(), DataHubURL: "http://evil.example", Storage: "full", Height: 100})

	resp, err := s.GetPeersForCatchup(context.Background(), &p2p_api.GetPeersForCatchupRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Peers, 1, "precondition: peer must be a catchup candidate before the blacklist entry")

	// Operator blacklists the host.
	s.settings.SubtreeValidation.BlacklistedBaseURLs = map[string]struct{}{"http://evil.example": {}}

	// A subsequent node_status strips the URL but cannot clear the stored one.
	blockHash := chainhash.HashH([]byte("stored before blacklist")).String()
	msgBytes, err := json.Marshal(NodeStatusMessage{
		PeerID:        remote.String(),
		ClientName:    "client/1.0",
		BaseURL:       "http://evil.example",
		BestHeight:    102,
		BestBlockHash: blockHash,
	})
	require.NoError(t, err)
	s.handleNodeStatusTopic(context.Background(), msgBytes, remote.String())

	got, ok := reg.Get(remote.String())
	require.True(t, ok)
	require.Equal(t, "http://evil.example", got.DataHubURL,
		"documented limitation: the stored URL survives the strip, which is why point-of-use filtering exists")

	// Point of use: the peer must no longer be handed to catchup.
	resp, err = s.GetPeersForCatchup(context.Background(), &p2p_api.GetPeersForCatchupRequest{})
	require.NoError(t, err)
	require.Empty(t, resp.Peers, "peer whose stored DataHubURL is now blacklisted must not be a catchup candidate")
}

// TestIsBaseURLBlacklisted covers the matcher directly, in particular the two
// bypasses fixed after review: scheme-less blacklist entries ("evil.example"
// parses as a URL path, so the entry silently never matched a real full-URL
// announcement) and hostnames with multiple trailing dots.
func TestIsBaseURLBlacklisted(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		entry   string
		want    bool
	}{
		{"full_url_entry_exact", "http://evil.example", "http://evil.example", true},
		{"full_url_entry_other_port_path", "http://evil.example:8080/path", "http://evil.example", true},
		{"scheme_less_entry_full_url_announcement", "http://evil.example:8080/path", "evil.example", true},
		{"scheme_less_entry_with_port", "https://evil.example/x", "evil.example:9000", true},
		{"trailing_dot_announcement", "http://evil.example./x", "http://evil.example", true},
		{"double_trailing_dot_announcement", "http://evil.example../x", "http://evil.example", true},
		{"case_insensitive_host", "http://EVIL.example/x", "evil.example", true},
		{"different_host_not_matched", "http://good.example/x", "evil.example", false},
		{"unparseable_entry_exact_match_fallback", "http://[bad", "http://[bad", true},
		{"unparseable_entry_no_match", "http://good.example", "http://[bad", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blacklist := map[string]struct{}{tt.entry: {}}
			require.Equal(t, tt.want, isBaseURLBlacklisted(tt.baseURL, blacklist),
				"baseURL %q vs blacklist entry %q", tt.baseURL, tt.entry)
		})
	}
}

func TestServerGetPeersReturnsConnectedPeersWithHeight(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)

	connectedPID := mustNewPeerID(t)
	disconnectedPID := mustNewPeerID(t)

	s.addConnectedPeer(connectedPID, "client/1.0", 123, nil, "")
	s.addPeer(disconnectedPID, "client/1.0", 99, nil, "")

	resp, err := s.GetPeers(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Len(t, resp.Peers, 1, "disconnected peers must be excluded")
	require.Equal(t, connectedPID.String(), resp.Peers[0].Id)
	require.Equal(t, uint32(123), resp.Peers[0].CurrentHeight)
}
