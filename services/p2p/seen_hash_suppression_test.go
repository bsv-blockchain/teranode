package p2p

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	p2pMessageBus "github.com/bsv-blockchain/go-p2p-message-bus"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func newSeenHashTestServer(t *testing.T) (*Server, *blockchain.CentralizedPeerRegistry, *kafka.KafkaAsyncProducerMock) {
	t.Helper()

	s, reg := newServerWithLocalRegistry(t)
	setServerLocalHeight(t, s, 100)

	mockP2P := new(MockServerP2PClient)
	mockP2P.peerID = mustNewPeerID(t)
	s.P2PClient = mockP2P
	s.notificationCh = make(chan *notificationMsg, 64)

	producer := kafka.NewKafkaAsyncProducerMock()
	s.blocksKafkaProducerClient = producer
	s.subtreeKafkaProducerClient = producer

	return s, reg, producer
}

func announceBlock(t *testing.T, s *Server, from peer.ID, blockHash string) {
	t.Helper()

	msgBytes, err := json.Marshal(BlockMessage{
		PeerID:     from.String(),
		ClientName: "client/1.0",
		DataHubURL: "http://peer.example",
		Hash:       blockHash,
		Height:     101,
	})
	require.NoError(t, err)

	s.handleBlockTopic(context.Background(), msgBytes, from.String())
}

func announceSubtree(t *testing.T, s *Server, from peer.ID, subtreeHash string) {
	t.Helper()

	msgBytes, err := json.Marshal(SubtreeMessage{
		PeerID:     from.String(),
		ClientName: "client/1.0",
		DataHubURL: "http://peer.example",
		Hash:       subtreeHash,
	})
	require.NoError(t, err)

	s.handleSubtreeTopic(context.Background(), msgBytes, from.String())
}

func drainPublished(producer *kafka.KafkaAsyncProducerMock) []*kafka.Message {
	var out []*kafka.Message
	for {
		select {
		case msg := <-producer.PublishChannel():
			out = append(out, msg)
		default:
			return out
		}
	}
}

// The first seenHashMaxPublishersPerHash distinct announcers of a hash reach
// Kafka (block validation collects the later ones as alternative fetch
// sources); everything past the budget, and every same-peer replay, is
// suppressed. Per-peer bookkeeping still runs for suppressed announcements.
func TestHandleBlockTopic_DuplicateAnnouncementSuppressed(t *testing.T) {
	s, reg, producer := newSeenHashTestServer(t)

	blockHash := chainhash.HashH([]byte("seen-hash dedup block")).String()

	peers := make([]peer.ID, seenHashMaxPublishersPerHash+1)
	for i := range peers {
		peers[i] = mustNewPeerID(t)
	}

	// Distinct announcers up to the budget publish; the next one is suppressed.
	for _, p := range peers {
		announceBlock(t, s, p, blockHash)
	}

	published := drainPublished(producer)
	require.Len(t, published, seenHashMaxPublishersPerHash,
		"exactly the publisher budget of distinct announcers may reach Kafka")
	for _, msg := range published {
		require.Equal(t, blockHash, string(msg.Key))
	}

	// Replays from any announcer are suppressed.
	announceBlock(t, s, peers[0], blockHash)
	announceBlock(t, s, peers[len(peers)-1], blockHash)
	require.Empty(t, drainPublished(producer), "replays must never reach Kafka")

	// The suppressed announcer's registry entry must still have been updated.
	info, ok := reg.Get(peers[len(peers)-1].String())
	require.True(t, ok, "suppressed announcements must still register the peer")
	require.NotNil(t, info.BlockHash)
	require.Equal(t, blockHash, info.BlockHash.String())

	// A different hash still goes through.
	otherHash := chainhash.HashH([]byte("another block")).String()
	announceBlock(t, s, peers[0], otherHash)

	published = drainPublished(producer)
	require.Len(t, published, 1)
	require.Equal(t, otherHash, string(published[0].Key))
}

// Suppression must not hide announcements from the WebSocket/dashboard
// notification path: every valid announcement, duplicate or not, still
// notifies.
func TestHandleBlockTopic_DuplicatesStillNotify(t *testing.T) {
	s, _, _ := newSeenHashTestServer(t)

	blockHash := chainhash.HashH([]byte("notify block")).String()
	p := mustNewPeerID(t)

	announceBlock(t, s, p, blockHash)
	announceBlock(t, s, p, blockHash)

	require.Len(t, s.notificationCh, 2, "duplicate announcements must still reach the notification channel")
}

// A peer re-announcing the same hash past the tolerance must accumulate spam
// ban score and eventually be banned; peers within the tolerance, and peers
// merely duplicating a seen hash, must not be scored.
func TestHandleBlockTopic_RepeatAnnouncerScoredAsSpam(t *testing.T) {
	s, reg, _ := newSeenHashTestServer(t)

	blockHash := chainhash.HashH([]byte("spam replay block")).String()
	tolerated := mustNewPeerID(t)
	spammer := mustNewPeerID(t)

	// Announcing 1 + tolerance times means every repeat is within the
	// tolerance: no score, no ban. This pins the reorg allowance.
	for i := 0; i <= seenHashSpamRepeatTolerance; i++ {
		announceBlock(t, s, tolerated, blockHash)
	}
	require.False(t, reg.IsBannedPeer(tolerated.String()),
		"repeats within the tolerance must not be scored")

	// Two repeats past the tolerance score 50 points each against the default
	// 100-point threshold: banned.
	for i := 0; i <= seenHashSpamRepeatTolerance+2; i++ {
		announceBlock(t, s, spammer, blockHash)
	}
	require.True(t, reg.IsBannedPeer(spammer.String()),
		"repeat announcer past the tolerance must cross the ban threshold")
	require.False(t, reg.IsBannedPeer(tolerated.String()),
		"other announcers of the same hash must not be penalised")
}

// When the Kafka producer is backlogged the announcement is dropped instead of
// blocking the gossip worker, and the publish grant is returned so a later
// announcement - even a repeat by the same peer - can retry.
func TestHandleBlockTopic_ProducerBackpressureDropsAndAllowsRetry(t *testing.T) {
	s, _, producer := newSeenHashTestServer(t)

	// Fill the mock producer's buffer so TryPublish fails.
	for i := 0; i < cap(producer.PublishChannel()); i++ {
		producer.PublishChannel() <- &kafka.Message{}
	}

	blockHash := chainhash.HashH([]byte("backpressure block")).String()
	peerA := mustNewPeerID(t)

	announceBlock(t, s, peerA, blockHash)

	// Broker recovers: the buffer drains.
	drainPublished(producer)

	announceBlock(t, s, peerA, blockHash)

	published := drainPublished(producer)
	require.Len(t, published, 1, "a dropped publish must not permanently suppress the hash")
	require.Equal(t, blockHash, string(published[0].Key))
}

func TestHandleSubtreeTopic_DuplicateAnnouncementSuppressed(t *testing.T) {
	s, _, producer := newSeenHashTestServer(t)

	subtreeHash := chainhash.HashH([]byte("seen-hash dedup subtree")).String()

	for i := 0; i < seenHashMaxPublishersPerHash+2; i++ {
		announceSubtree(t, s, mustNewPeerID(t), subtreeHash)
	}

	published := drainPublished(producer)
	require.Len(t, published, seenHashMaxPublishersPerHash,
		"only the publisher budget of distinct announcers may reach Kafka")
	require.Equal(t, subtreeHash, string(published[0].Key))
}

// An announcement dropped by the unhealthy-peer gate must not mark the hash as
// seen: later announcements of the same subtree from healthy peers must keep
// their full publish budget, or low-reputation peers could shadow subtrees off
// the network.
func TestHandleSubtreeTopic_UnhealthyPeerDropDoesNotMarkSeen(t *testing.T) {
	s, reg, producer := newSeenHashTestServer(t)

	lowRep := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: lowRep.String()})
	reg.UpdateMetrics(lowRep.String(), 0, 0, 0, false, false, true, 0)
	require.True(t, s.shouldSkipUnhealthyPeer(lowRep.String(), "precondition"),
		"precondition: peer must be below the unhealthy threshold")

	subtreeHash := chainhash.HashH([]byte("shadowed subtree")).String()

	announceSubtree(t, s, lowRep, subtreeHash)
	require.Empty(t, drainPublished(producer), "unhealthy peer's announcement must be dropped")

	healthy := mustNewPeerID(t)
	announceSubtree(t, s, healthy, subtreeHash)

	published := drainPublished(producer)
	require.Len(t, published, 1, "healthy peer's announcement of the same subtree must be published")
	require.Equal(t, subtreeHash, string(published[0].Key))
}

// A blockchain-subscription reconnect replays the current tip notification, so
// the sender must suppress consecutive re-announcements of the same hash or a
// node with a flapping blockchain stream would read as a spammer to its peers.
// A reorg away and back changes the hash in between and must still announce.
func TestHandleBlockNotification_SuppressesConsecutiveDuplicateTip(t *testing.T) {
	fsmState := blockchain_api.FSMStateType_RUNNING
	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil).Maybe()
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(model.GenesisBlockHeader, &model.BlockHeaderMeta{Height: 100}, nil).Maybe()
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).
		Return(model.GenesisBlockHeader, &model.BlockHeaderMeta{Height: 100}, nil).Maybe()
	mockBlockchain.On("GetState", mock.Anything, mock.Anything).
		Return([]byte(nil), errors.NewNotFoundError("not set")).Maybe()

	s, p2pClient := newGateTestServer(t, mockBlockchain)
	p2pClient.peerID = mustNewPeerID(t)
	p2pClient.peers = []p2pMessageBus.PeerInfo{}
	s.notificationCh = make(chan *notificationMsg, 64)
	s.AssetHTTPAddressURL = "http://asset.example"

	countBlockPublishes := func() int {
		n := 0
		for _, call := range p2pClient.Calls {
			if call.Method == "Publish" && call.Arguments.String(1) == s.blockTopicName {
				n++
			}
		}
		return n
	}

	tipA := chainhash.HashH([]byte("tip A"))
	tipB := chainhash.HashH([]byte("tip B"))

	require.NoError(t, s.handleBlockNotification(context.Background(), &tipA))
	require.NoError(t, s.handleBlockNotification(context.Background(), &tipA), "replayed tip notification")
	require.Equal(t, 1, countBlockPublishes(), "a replayed tip notification must not re-announce")

	require.NoError(t, s.handleBlockNotification(context.Background(), &tipB))
	require.NoError(t, s.handleBlockNotification(context.Background(), &tipA), "reorg back to the previous tip")
	require.Equal(t, 3, countBlockPublishes(), "a reorg back to a recent hash must still announce")
}
