package subtreevalidation

import (
	"context"
	"fmt"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/kafka/kafkatest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestPolicyRejectedTxEndToEnd verifies the full Kafka roundtrip:
// producer publishes a policy-rejected tx → consumer reads it → cache stores it → lookup succeeds.
//
// Run with: go test -v -run TestPolicyRejectedTxEndToEnd -timeout 3m ./services/subtreevalidation/
func TestPolicyRejectedTxEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	logger := ulogger.New("policy-rejected-tx-e2e")
	env := kafkatest.MustStartEnv(t, ctx)

	topic := fmt.Sprintf("policy-rejected-e2e-%d", time.Now().UnixNano()%10000)
	kafkaURL, err := url.Parse(env.TopicURL(topic,
		"partitions=1&replication=1&retention=60000&flush_frequency=10ms&replay=1"))
	require.NoError(t, err)

	// --- Producer side (simulates what the validator does) ---
	producer, err := kafka.NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL, nil)
	require.NoError(t, err)
	producer.Start(ctx, make(chan *kafka.Message, 100))

	// Build a sample tx
	txHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
	tx, err := bt.NewTxFromString(txHex)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()

	m := &kafkamessage.KafkaTxPolicyRejectedTopicMessage{
		TxHash: txHash.CloneBytes(),
		RawTx:  tx.SerializeBytes(),
		Reason: "insufficient fee",
	}
	value, err := proto.Marshal(m)
	require.NoError(t, err)

	producer.Publish(&kafka.Message{
		Key:   txHash.CloneBytes(),
		Value: value,
	})

	// --- Consumer side (simulates what subtree validation does) ---
	consumer, err := kafka.NewKafkaConsumerGroupFromURL(logger, kafkaURL,
		fmt.Sprintf("policy-rejected-cg-%d", time.Now().UnixNano()%10000), true, nil)
	require.NoError(t, err)

	cache := newTxPolicyRejectedCache(64 * 1024 * 1024)
	server := &Server{
		policyRejectedTxCache: cache,
		logger:                logger,
	}
	handler := server.policyRejectedTxMessageHandler(ctx)

	var received atomic.Int64
	done := make(chan struct{})

	consumer.Start(ctx, func(msg *kafka.KafkaMessage) error {
		err := handler(msg)
		if received.Add(1) >= 1 {
			select {
			case <-done:
			default:
				close(done)
			}
		}
		return err
	})

	// Wait for consumer to join before producing (Redpanda needs a moment)
	time.Sleep(1 * time.Second)

	// Flush producer
	require.NoError(t, producer.Stop())

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("timed out waiting for consumer to receive message (received: %d)", received.Load())
	}

	require.NoError(t, consumer.Close())

	// --- Verify the cache was populated ---
	cachedTx, found := cache.Get(*txHash)
	require.True(t, found, "tx should be in the policy-rejected cache after Kafka roundtrip")
	assert.Equal(t, tx.TxID(), cachedTx.TxID())

	// --- Verify lookup works ---
	toCheck := []missingTxHash{
		{hash: *txHash, idx: 0},
		{hash: chainhash.Hash{0xFF}, idx: 1}, // missing tx
	}
	foundTxs, stillMissing := server.lookupPolicyRejectedTxs(toCheck)

	require.Len(t, foundTxs, 1, "cached tx should be found via lookup")
	assert.Equal(t, tx.TxID(), foundTxs[0].tx.TxID())
	assert.Equal(t, 0, foundTxs[0].idx)

	require.Len(t, stillMissing, 1, "unknown tx should remain missing")
	assert.Equal(t, chainhash.Hash{0xFF}, stillMissing[0].hash)

	t.Logf("end-to-end: published tx %s → consumed from Kafka → cache hit confirmed", txHash.String())
}

// TestPolicyRejectedTxMultipleMessages verifies the cache handles a batch of
// policy-rejected transactions published to a real Kafka topic.
func TestPolicyRejectedTxMultipleMessages(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	logger := ulogger.New("policy-rejected-batch-e2e")
	env := kafkatest.MustStartEnv(t, ctx)

	topic := fmt.Sprintf("policy-rejected-batch-%d", time.Now().UnixNano()%10000)
	kafkaURL, err := url.Parse(env.TopicURL(topic,
		"partitions=1&replication=1&retention=60000&flush_frequency=10ms&replay=1"))
	require.NoError(t, err)

	producer, err := kafka.NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL, nil)
	require.NoError(t, err)
	producer.Start(ctx, make(chan *kafka.Message, 1000))

	baseTxHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0704ffff001d0104ffffffff0100f2052a0100000043410496b538e853519c726a2c91e61ec11600ae1390813a627c66fb8be7947be63c52da7589379515d4e0a604f8141781e62294721166bf621e73a82cbf2342c858eeac00000000"
	baseTx, err := bt.NewTxFromString(baseTxHex)
	require.NoError(t, err)

	messageCount := 50
	txHashes := make([]chainhash.Hash, messageCount)

	for i := 0; i < messageCount; i++ {
		// Build a genuinely distinct transaction per message by varying the locktime,
		// so each message's claimed hash matches its raw bytes. The handler rejects
		// messages whose claimed hash differs from the parsed tx (cache-poisoning
		// defense), so reusing one tx under fake hashes would be dropped, not cached.
		tx := baseTx.Clone()
		tx.LockTime = uint32(i + 1)

		hash := *tx.TxIDChainHash()
		txHashes[i] = hash

		m := &kafkamessage.KafkaTxPolicyRejectedTopicMessage{
			TxHash: hash[:],
			RawTx:  tx.SerializeBytes(),
			Reason: fmt.Sprintf("policy-violation-%d", i),
		}
		value, err := proto.Marshal(m)
		require.NoError(t, err)

		producer.Publish(&kafka.Message{
			Key:   hash[:],
			Value: value,
		})
	}

	cache := newTxPolicyRejectedCache(64 * 1024 * 1024)
	server := &Server{
		policyRejectedTxCache: cache,
		logger:                logger,
	}
	handler := server.policyRejectedTxMessageHandler(ctx)

	var received atomic.Int64
	done := make(chan struct{})

	consumer, err := kafka.NewKafkaConsumerGroupFromURL(logger, kafkaURL,
		fmt.Sprintf("policy-batch-cg-%d", time.Now().UnixNano()%10000), true, nil)
	require.NoError(t, err)

	consumer.Start(ctx, func(msg *kafka.KafkaMessage) error {
		err := handler(msg)
		if received.Add(1) >= int64(messageCount) {
			select {
			case <-done:
			default:
				close(done)
			}
		}
		return err
	})

	time.Sleep(1 * time.Second)
	require.NoError(t, producer.Stop())

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("timed out: received %d/%d messages", received.Load(), messageCount)
	}

	require.NoError(t, consumer.Close())

	assert.Equal(t, messageCount, cache.Len(), "all published txs should be in the cache")

	for i, hash := range txHashes {
		_, ok := cache.Get(hash)
		assert.True(t, ok, "tx %d should be in cache", i)
	}

	t.Logf("batch end-to-end: %d messages published → consumed → all cached", messageCount)
}
