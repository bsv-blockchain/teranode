package blockvalidation

import (
	"context"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	inmemorykafka "github.com/bsv-blockchain/teranode/util/kafka/in_memory_kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// gap003Chain records every GetBlockExists call made by the Kafka block handler and fails the
// FIRST call for failHash with a recoverable ERR_SERVICE_ERROR (a transient storage/service
// fault). Every other call answers "not found".
type gap003Chain struct {
	blockchain.ClientI

	failHash *chainhash.Hash

	mu    sync.Mutex
	seen  []chainhash.Hash
	fails int
}

func (c *gap003Chain) GetBlockExists(_ context.Context, h *chainhash.Hash) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.seen = append(c.seen, *h)

	if *h == *c.failHash && c.fails == 0 {
		c.fails++
		return false, errors.NewServiceError("injected transient blockchain failure")
	}

	return false, nil
}

func (c *gap003Chain) count(h chainhash.Hash) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	n := 0

	for _, s := range c.seen {
		if s == h {
			n++
		}
	}

	return n
}

// TestGapBV003_RecoverableKafkaErrorIsRetried proves GAP-BV-003.
//
// Spec: BV-INGRESS-009 (AC-BV-INGRESS-009.1): a Kafka block message whose processing fails for a
// local or transient reason (service, storage, ...) must be processed again, not lost.
//
// The real service wiring is used: a real memory:// KafkaConsumerGroup (util/kafka, in-memory
// broker) is started exactly as Server.Start does,
// `Start(ctx, u.consumerMessageHandler(ctx), kafka.WithLogErrorAndMoveOn())`. Message 1's first
// handling fails with ERR_SERVICE_ERROR; message 2 on the same partition succeeds.
//
// The consumer commits the highest marked offset per partition, so returning the error to Kafka
// cannot redeliver message 1 once message 2 succeeds. The handler instead commits message 1 and
// retries it in-process (kafka_block_retry.go). That retry lives in memory, so a restart during
// the retry window drops it; catchup and P2P sync recover the block then.
//
// Asserted: message 1 is handled again (the handler's existence check sees hash1 a second time)
// after message 2 has been handled.
func TestGapBV003_RecoverableKafkaErrorIsRetried(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _, block := newProcessBlockFoundHarness(ctx, t)

	hash1 := *block.Hash()
	hash2 := chainhash.HashH([]byte("gap-bv003-second-message"))

	chain := &gap003Chain{ClientI: s.blockchainClient, failHash: &hash1}
	s.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, s.settings, chain, blobmemory.New(), s.txStore, s.utxoStore, nil, nil)

	topic := "gap-bv003-blocks-" + time.Now().Format("150405.000000000")

	topicURL, err := url.Parse("memory://localhost/" + topic)
	require.NoError(t, err)

	consumer, err := kafka.NewKafkaConsumerGroup(kafka.KafkaConsumerConfig{
		Logger:          ulogger.TestLogger{},
		URL:             topicURL,
		Topic:           topic,
		ConsumerGroupID: "gap-bv003-group",
	})
	require.NoError(t, err)

	defer func() { _ = consumer.Close() }()

	consumer.Start(ctx, s.consumerMessageHandler(ctx), kafka.WithLogErrorAndMoveOn())

	broker := inmemorykafka.GetSharedBroker()

	deadline := time.Now().Add(10 * time.Second)
	for !broker.HasConsumer(topic) {
		require.True(t, time.Now().Before(deadline), "setup: in-memory consumer never registered")
		time.Sleep(10 * time.Millisecond)
	}

	produce := func(h chainhash.Hash) {
		value, marshalErr := proto.Marshal(&kafkamessage.KafkaBlockTopicMessage{
			Hash:   h.String(),
			URL:    "http://test-peer",
			PeerId: "peerA",
		})
		require.NoError(t, marshalErr)
		require.NoError(t, broker.Produce(ctx, topic, []byte("k"), value))
	}

	produce(hash1)
	produce(hash2)

	// Message 2 handled => the consumer has moved past message 1.
	deadline = time.Now().Add(10 * time.Second)
	for chain.count(hash2) == 0 {
		require.True(t, time.Now().Before(deadline), "setup: message 2 was never handled (hash1 seen %d times)", chain.count(hash1))
		time.Sleep(10 * time.Millisecond)
	}

	// Bounded window for the retry of message 1.
	deadline = time.Now().Add(3 * time.Second)
	for chain.count(hash1) < 2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	require.GreaterOrEqual(t, chain.count(hash1), 2,
		"BV-INGRESS-009: message 1 failed with a recoverable ERR_SERVICE_ERROR and was never processed again (handled %d time(s)) while message 2 on the same partition was processed",
		chain.count(hash1))
}
