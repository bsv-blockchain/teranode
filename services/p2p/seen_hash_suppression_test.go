package p2p

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func newSeenHashTestServer(t *testing.T) (*Server, *blockchain.CentralizedPeerRegistry, *kafka.KafkaAsyncProducerMock) {
	t.Helper()

	s, reg := newServerWithLocalRegistry(t)
	setServerLocalHeight(t, s, 100)

	mockP2P := new(MockServerP2PClient)
	mockP2P.peerID = mustNewPeerID(t)
	s.P2PClient = mockP2P
	s.notificationCh = make(chan *notificationMsg, 32)

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

// A hash already announced within the seen-hash TTL must not be re-published
// to Kafka, while per-peer bookkeeping (registry state) still runs for the
// duplicate announcer.
func TestHandleBlockTopic_DuplicateAnnouncementSuppressed(t *testing.T) {
	s, reg, producer := newSeenHashTestServer(t)

	blockHash := chainhash.HashH([]byte("seen-hash dedup block")).String()
	peerA := mustNewPeerID(t)
	peerB := mustNewPeerID(t)

	announceBlock(t, s, peerA, blockHash)
	announceBlock(t, s, peerB, blockHash)
	announceBlock(t, s, peerA, blockHash)

	published := drainPublished(producer)
	require.Len(t, published, 1, "only the first announcement of a hash may reach Kafka")
	require.Equal(t, blockHash, string(published[0].Key))

	// The duplicate announcer's registry entry must still have been updated.
	info, ok := reg.Get(peerB.String())
	require.True(t, ok, "duplicate announcements must still register the peer")
	require.NotNil(t, info.BlockHash)
	require.Equal(t, blockHash, info.BlockHash.String())

	// A different hash still goes through.
	otherHash := chainhash.HashH([]byte("another block")).String()
	announceBlock(t, s, peerA, otherHash)

	published = drainPublished(producer)
	require.Len(t, published, 1)
	require.Equal(t, otherHash, string(published[0].Key))
}

// A peer re-announcing the same hash past the reorg tolerance must accumulate
// spam ban score and eventually be banned; announcements from other peers must
// not be scored for merely duplicating a seen hash.
func TestHandleBlockTopic_RepeatAnnouncerScoredAsSpam(t *testing.T) {
	s, reg, producer := newSeenHashTestServer(t)

	blockHash := chainhash.HashH([]byte("spam replay block")).String()
	spammer := mustNewPeerID(t)
	honest := mustNewPeerID(t)

	// Honest peer announces once; the spammer replays the same hash. Repeats
	// 1-2 are tolerated (reorg flapping); repeats 3 and 4 score 50 spam points
	// each against the default 100 threshold.
	announceBlock(t, s, honest, blockHash)
	for i := 0; i < 5; i++ {
		announceBlock(t, s, spammer, blockHash)
	}

	require.True(t, reg.IsBannedPeer(spammer.String()), "repeat announcer must cross the ban threshold")
	require.False(t, reg.IsBannedPeer(honest.String()), "first announcer must not be penalised")

	require.Len(t, drainPublished(producer), 1, "replays must never reach Kafka")
}

// When the Kafka producer is backlogged the announcement is dropped instead of
// blocking the gossip worker, and the hash is forgotten so a later
// announcement can retry the publish.
func TestHandleBlockTopic_ProducerBackpressureDropsAndAllowsRetry(t *testing.T) {
	s, _, producer := newSeenHashTestServer(t)

	// Fill the mock producer's buffer so TryPublish fails.
	for i := 0; i < cap(producer.PublishChannel()); i++ {
		producer.PublishChannel() <- &kafka.Message{}
	}

	blockHash := chainhash.HashH([]byte("backpressure block")).String()
	peerA := mustNewPeerID(t)
	peerB := mustNewPeerID(t)

	announceBlock(t, s, peerA, blockHash)

	// Broker recovers: the buffer drains.
	drainPublished(producer)

	announceBlock(t, s, peerB, blockHash)

	published := drainPublished(producer)
	require.Len(t, published, 1, "a dropped publish must not permanently suppress the hash")
	require.Equal(t, blockHash, string(published[0].Key))
}

func TestHandleSubtreeTopic_DuplicateAnnouncementSuppressed(t *testing.T) {
	s, _, producer := newSeenHashTestServer(t)

	subtreeHash := chainhash.HashH([]byte("seen-hash dedup subtree")).String()
	peerA := mustNewPeerID(t)
	peerB := mustNewPeerID(t)

	announceSubtree(t, s, peerA, subtreeHash)
	announceSubtree(t, s, peerB, subtreeHash)
	announceSubtree(t, s, peerA, subtreeHash)

	published := drainPublished(producer)
	require.Len(t, published, 1, "only the first announcement of a subtree may reach Kafka")
	require.Equal(t, subtreeHash, string(published[0].Key))
}

// An announcement dropped by the unhealthy-peer gate must not mark the hash as
// seen: a later announcement of the same subtree from a healthy peer has to be
// published, or a low-reputation peer could shadow subtrees off the network.
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
