package p2p

import (
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	inmemorykafka "github.com/bsv-blockchain/teranode/util/kafka/in_memory_kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestInvalidBlocksConsumer_BansPeerViaInjectedConsumer guards the wiring in
// Start(): the injected invalid-blocks consumer is the only consumer and runs
// processInvalidBlockMessage, which bans the peer that sent the block.
func TestInvalidBlocksConsumer_BansPeerViaInjectedConsumer(t *testing.T) {
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

	// Same wiring as Server.Start: the injected consumer runs the real handler.
	s.invalidBlocksKafkaConsumerClient.Start(ctx, s.processInvalidBlockMessage, kafka.WithLogErrorAndMoveOn())

	require.Eventually(t, func() bool {
		return inmemorykafka.GetSharedBroker().HasConsumer(topic)
	}, 5*time.Second, 10*time.Millisecond, "in-memory consumer never subscribed")

	msgBytes, err := proto.Marshal(&kafkamessage.KafkaInvalidBlockTopicMessage{
		BlockHash: blockHash,
		Reason:    "invalid block for wiring test",
	})
	require.NoError(t, err)
	require.NoError(t, inmemorykafka.GetSharedBroker().Produce(ctx, topic, nil, msgBytes))

	require.Eventually(t, func() bool {
		if _, stillThere := s.blockPeerMap.Load(blockHash); stillThere {
			return false
		}
		info, ok := reg.Get(pid.String())
		return ok && info.BanScore > 0
	}, 5*time.Second, 10*time.Millisecond, "message was not processed by the real invalid-block handler")
}
