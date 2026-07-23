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
