package p2p

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	inmemorykafka "github.com/bsv-blockchain/teranode/util/kafka/in_memory_kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func mustMarshalInvalidBlockMsg(t *testing.T, blockHash, reason string) []byte {
	t.Helper()

	msgBytes, err := proto.Marshal(&kafkamessage.KafkaInvalidBlockTopicMessage{
		BlockHash: blockHash,
		Reason:    reason,
	})
	require.NoError(t, err)
	return msgBytes
}

// TestStartInvalidBlocksConsumer_BansPeer guards the wiring in Start(): the
// injected invalid-blocks consumer is the only consumer and runs
// processInvalidBlockMessage, which bans the peer that sent the block.
func TestStartInvalidBlocksConsumer_BansPeer(t *testing.T) {
	ctx := t.Context()

	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	const topic = "invalid-blocks-wiring-test"

	kafkaURL, err := url.Parse("memory://localhost:9092/" + topic)
	require.NoError(t, err)

	consumer, err := kafka.NewKafkaConsumerGroup(kafka.KafkaConsumerConfig{
		Logger:            ulogger.TestLogger{},
		URL:               kafkaURL,
		Topic:             topic,
		ConsumerGroupID:   "p2p." + topic,
		AutoCommitEnabled: true,
	})
	require.NoError(t, err)

	s.invalidBlocksKafkaConsumerClient = consumer

	defer func() {
		require.NoError(t, s.invalidBlocksKafkaConsumerClient.Close())
		inmemorykafka.GetSharedBroker().DropTopic(topic)
	}()

	blockHash := "1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b"
	s.storePeerMapEntry(&s.blockPeerMap, blockHash, pid.String(), time.Now())

	// The same call Server.Start makes to wire the consumer.
	s.startInvalidBlocksConsumer(ctx)

	require.Eventually(t, func() bool {
		return inmemorykafka.GetSharedBroker().HasConsumer(topic)
	}, 5*time.Second, 10*time.Millisecond, "in-memory consumer never subscribed")

	msgBytes := mustMarshalInvalidBlockMsg(t, blockHash, "invalid block for wiring test")
	require.NoError(t, inmemorykafka.GetSharedBroker().Produce(ctx, topic, nil, msgBytes))

	require.Eventually(t, func() bool {
		if _, stillThere := s.blockPeerMap.Load(blockHash); stillThere {
			return false
		}
		info, ok := reg.Get(pid.String())
		return ok && info.BanScore > 0
	}, 5*time.Second, 10*time.Millisecond, "message was not processed by the real invalid-block handler")
}

func TestStartInvalidBlocksConsumer_NilConsumerIsNoOp(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	require.Nil(t, s.invalidBlocksKafkaConsumerClient)

	// Must not panic when no consumer was injected (InvalidBlocksConfig unset).
	s.startInvalidBlocksConsumer(t.Context())
}

func TestProcessInvalidBlockMessage_UnknownBlockIsNotAnError(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	msgBytes := mustMarshalInvalidBlockMsg(t, "0000000000000000000000000000000000000000000000000000000000000000", "no peer mapped")

	// No blockPeerMap entry: there is no peer to ban, which is not an error.
	err := s.processInvalidBlockMessage(&kafka.KafkaMessage{Value: msgBytes})
	require.NoError(t, err)

	info, ok := reg.Get(pid.String())
	require.True(t, ok)
	require.Zero(t, info.BanScore, "no ban score must be added when no peer sent the block")
}

func TestProcessInvalidBlockMessage_MalformedMessageIsSkipped(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)

	// A malformed message can never succeed on retry: it must be logged and
	// skipped, not returned as an error the consumer would retry.
	err := s.processInvalidBlockMessage(&kafka.KafkaMessage{Value: []byte("not a protobuf message")})
	require.NoError(t, err)
}

type failingBanScoreRegistry struct {
	blockchain.PeerRegistryClientI
	err error
}

func (f *failingBanScoreRegistry) AddBanScore(_ context.Context, _ string, _ string, _ int32) (int32, bool, error) {
	return 0, false, f.err
}

func TestProcessInvalidBlockMessage_AddBanScoreErrorIsReturned(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)
	s.peerRegistry = &failingBanScoreRegistry{err: errors.NewServiceError("registry down")}

	blockHash := "2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c"
	s.storePeerMapEntry(&s.blockPeerMap, blockHash, pid.String(), time.Now())

	msgBytes := mustMarshalInvalidBlockMsg(t, blockHash, "registry failure")

	err := s.processInvalidBlockMessage(&kafka.KafkaMessage{Value: msgBytes})
	require.Error(t, err)

	_, stillThere := s.blockPeerMap.Load(blockHash)
	require.True(t, stillThere, "entry must be kept so the consumer's in-process retries can still ban the peer")
}

// TestProcessInvalidBlockMessage_PeerIdSurvivesMapWashout pins the fix for the
// attribution-washout exploit (issue 1433): the invalid-block message now
// carries the announcer's peer ID end-to-end, so a peer that floods the
// bounded peer map between serving an invalid block and its verdict — erasing
// its own map entry — is banned anyway.
func TestProcessInvalidBlockMessage_PeerIdSurvivesMapWashout(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	// No blockPeerMap entry at all: the flood already evicted it.
	msgBytes, err := proto.Marshal(&kafkamessage.KafkaInvalidBlockTopicMessage{
		BlockHash: "3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d",
		Reason:    "washed-out map entry",
		PeerId:    pid.String(),
	})
	require.NoError(t, err)

	require.NoError(t, s.processInvalidBlockMessage(&kafka.KafkaMessage{Value: msgBytes}))

	info, ok := reg.Get(pid.String())
	require.True(t, ok)
	require.Positive(t, info.BanScore, "the message's own peer ID must ban the peer without a map entry")
}

// TestProcessInvalidBlockMessage_PeerIdBeatsMapEntry pins precedence: the
// message's peer ID is block validation's record of whose announcement was
// validated, so it wins over a map entry that a later announcer may have
// overwritten (attribution in the map is last-writer-wins).
func TestProcessInvalidBlockMessage_PeerIdBeatsMapEntry(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	announcer := mustNewPeerID(t)
	overwriter := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: announcer.String()})
	reg.Register(&blockchain.PeerInfo{ID: overwriter.String()})

	blockHash := "4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e"
	s.storePeerMapEntry(&s.blockPeerMap, blockHash, overwriter.String(), time.Now())

	msgBytes, err := proto.Marshal(&kafkamessage.KafkaInvalidBlockTopicMessage{
		BlockHash: blockHash,
		Reason:    "provenance beats map",
		PeerId:    announcer.String(),
	})
	require.NoError(t, err)

	require.NoError(t, s.processInvalidBlockMessage(&kafka.KafkaMessage{Value: msgBytes}))

	info, ok := reg.Get(announcer.String())
	require.True(t, ok)
	require.Positive(t, info.BanScore, "the peer named by block validation must be the one banned")

	other, ok := reg.Get(overwriter.String())
	require.True(t, ok)
	require.Zero(t, other.BanScore, "the map entry must not override the message's provenance")
}

// TestProcessInvalidBlockMessage_PeerUrlFallback pins the DataHub-URL fallback:
// with no peer ID in the message and no map entry, the URL the block was
// fetched from resolves to the peer serving it, mirroring the subtree path.
func TestProcessInvalidBlockMessage_PeerUrlFallback(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String(), DataHubURL: "http://203.0.113.9:8090"})

	msgBytes, err := proto.Marshal(&kafkamessage.KafkaInvalidBlockTopicMessage{
		BlockHash: "5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f",
		Reason:    "url fallback",
		PeerUrl:   "http://203.0.113.9:8090",
	})
	require.NoError(t, err)

	require.NoError(t, s.processInvalidBlockMessage(&kafka.KafkaMessage{Value: msgBytes}))

	info, ok := reg.Get(pid.String())
	require.True(t, ok)
	require.Positive(t, info.BanScore, "the DataHub URL must resolve to the peer when nothing else does")
}

// TestReportInvalidBlock_PeerURLFallback pins the same fallback on the direct
// report path: a peer-map miss must not silently void the ban when the caller
// knows which DataHub URL served the block.
func TestReportInvalidBlock_PeerURLFallback(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String(), DataHubURL: "http://203.0.113.10:8090"})

	blockHash := "6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a"
	err := s.ReportInvalidBlock(t.Context(), blockHash, "http://203.0.113.10:8090", "url fallback")
	require.NoError(t, err)

	info, ok := reg.Get(pid.String())
	require.True(t, ok)
	require.Positive(t, info.BanScore, "the DataHub URL must resolve the peer on a map miss")

	// Without a URL, a map miss still reports back to the caller.
	err = s.ReportInvalidBlock(t.Context(), blockHash, "", "no attribution at all")
	require.Error(t, err)
}

// TestProcessInvalidBlockMessage_DuplicateDeliveryScoresOnce pins the
// idempotence the map-gated path used to provide for free: Kafka is
// at-least-once and revalidation can republish the same block, so one invalid
// block must add ban score once, not once per delivery.
func TestProcessInvalidBlockMessage_DuplicateDeliveryScoresOnce(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	msgBytes, err := proto.Marshal(&kafkamessage.KafkaInvalidBlockTopicMessage{
		BlockHash: "7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b",
		Reason:    "duplicate delivery",
		PeerId:    pid.String(),
	})
	require.NoError(t, err)

	require.NoError(t, s.processInvalidBlockMessage(&kafka.KafkaMessage{Value: msgBytes}))

	info, ok := reg.Get(pid.String())
	require.True(t, ok)
	scoreAfterFirst := info.BanScore
	require.Positive(t, scoreAfterFirst)

	require.NoError(t, s.processInvalidBlockMessage(&kafka.KafkaMessage{Value: msgBytes}))

	info, ok = reg.Get(pid.String())
	require.True(t, ok)
	require.Equal(t, scoreAfterFirst, info.BanScore, "a redelivered invalid-block message must not score the peer again")
}
