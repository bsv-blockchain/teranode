package kafka

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	redpandaImage   = "redpandadata/redpanda"
	redpandaVersion = "v24.3.1"
)

// startRedpanda starts a Redpanda container and returns the broker address and cleanup function.
func startRedpanda(t *testing.T, ctx context.Context) (brokerAddr string, cleanup func()) {
	t.Helper()

	port, err := getFreePort()
	require.NoError(t, err)

	req := testcontainers.ContainerRequest{
		Image:        fmt.Sprintf("%s:%s", redpandaImage, redpandaVersion),
		ExposedPorts: []string{fmt.Sprintf("%d:%d/tcp", port, port)},
		Cmd: []string{
			"redpanda", "start",
			"--overprovisioned",
			"--smp=1",
			"--kafka-addr", fmt.Sprintf("PLAINTEXT://0.0.0.0:%d", port),
			"--advertise-kafka-addr", fmt.Sprintf("PLAINTEXT://localhost:%d", port),
		},
		WaitingFor: wait.ForLog("Successfully started Redpanda!"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	return fmt.Sprintf("localhost:%d", port), func() {
		_ = container.Terminate(context.Background())
	}
}

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// publishAndExpectDelivery publishes a message and waits for the consumer to receive it.
// Returns an error description if the message was not delivered.
func publishAndExpectDelivery(t *testing.T, ctx context.Context, logger ulogger.Logger, brokerAddr, topic, rawQuery string, payloadSize int) error {
	t.Helper()

	kafkaURL := &url.URL{
		Scheme:   "kafka",
		Host:     brokerAddr,
		Path:     topic,
		RawQuery: rawQuery,
	}

	producer, err := NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL, nil)
	if err != nil {
		return errors.NewProcessingError("failed to create producer", err)
	}
	producer.Start(ctx, make(chan *Message, 100))
	defer producer.Stop() //nolint:errcheck

	consumer, err := NewKafkaConsumerGroupFromURL(logger, kafkaURL, "consumer-"+topic, true, nil)
	if err != nil {
		return errors.NewProcessingError("failed to create consumer", err)
	}

	var received sync.WaitGroup
	received.Add(1)
	var receivedSize int

	consumer.Start(ctx, func(msg *KafkaMessage) error {
		receivedSize = len(msg.Value)
		received.Done()
		return nil
	})

	payload := make([]byte, payloadSize)
	_, _ = rand.Read(payload)

	producer.Publish(&Message{
		Key:   []byte("test-key"),
		Value: payload,
	})

	done := make(chan struct{})
	go func() {
		received.Wait()
		close(done)
	}()

	select {
	case <-done:
		if receivedSize != payloadSize {
			return errors.NewProcessingError("size mismatch: sent %d, received %d", payloadSize, receivedSize)
		}
		return nil
	case <-time.After(10 * time.Second):
		return errors.NewProcessingError("message not delivered within 10s (likely MESSAGE_TOO_LARGE)")
	}
}

// TestFlushBytesRegression_MessageTooLarge proves that the franz-go migration breaks
// message delivery for topics configured with small flush_bytes values (e.g. 64).
//
// The Sarama library treated flush_bytes as a flush threshold ("flush after N bytes").
// The franz-go migration reinterprets it as ProducerBatchMaxBytes (a hard batch size
// limit). With flush_bytes=64, this gets clamped to 512 bytes, causing Redpanda to
// reject messages larger than ~1KB with MESSAGE_TOO_LARGE.
//
// This reproduces the production incident on teranode-mainnet-eu-1 (2026-04-02)
// where a blocks-final Kafka message was rejected, causing block 942978 to never
// propagate to SVNodes.
func TestFlushBytesRegression_MessageTooLarge(t *testing.T) {
	logger := ulogger.TestLogger{}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	brokerAddr, cleanup := startRedpanda(t, ctx)
	defer cleanup()

	// Message sizes that mirror production payloads:
	// - 100 B:   small INV message
	// - 1 KB:    typical blocks-final (header + coinbase + subtree hashes)
	// - 10 KB:   blocks-final with large coinbase
	// - 100 KB:  large txmeta batch
	// - 500 KB:  upper bound blocks-final
	messageSizes := []int{100, 1_024, 10_240, 102_400, 512_000}

	for _, size := range messageSizes {
		t.Run(fmt.Sprintf("flush_bytes_64_%d_bytes", size), func(t *testing.T) {
			topic := fmt.Sprintf("regr-flush64-%d-%d", size, time.Now().UnixNano()%10000)

			// Production-equivalent config: flush_bytes=64
			// This is the exact setting from kafka_blocksFinalConfig on mainnet
			query := "partitions=1&replication=1&retention=60000&flush_bytes=64&flush_frequency=1s"

			err := publishAndExpectDelivery(t, ctx, logger, brokerAddr, topic, query, size)
			require.NoError(t, err, "%d-byte message with flush_bytes=64 should be delivered", size)
		})
	}
}

// TestFlushBytesRegression_DefaultBatchMax is the control test — same messages with
// default flush_bytes (1MB) should always be delivered.
func TestFlushBytesRegression_DefaultBatchMax(t *testing.T) {
	logger := ulogger.TestLogger{}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	brokerAddr, cleanup := startRedpanda(t, ctx)
	defer cleanup()

	messageSizes := []int{100, 1_024, 10_240, 102_400, 512_000}

	for _, size := range messageSizes {
		t.Run(fmt.Sprintf("default_%d_bytes", size), func(t *testing.T) {
			topic := fmt.Sprintf("regr-default-%d-%d", size, time.Now().UnixNano()%10000)

			// No flush_bytes — uses default (1MB)
			query := "partitions=1&replication=1&retention=60000&flush_frequency=1s"

			err := publishAndExpectDelivery(t, ctx, logger, brokerAddr, topic, query, size)
			require.NoError(t, err, "%d-byte message with default flush_bytes should be delivered", size)
		})
	}
}

// TestClampBatchMaxBytes_FloorShouldNotRestrictLargeMessages verifies that the
// clampBatchMaxBytes function does not set ProducerBatchMaxBytes to a value that
// would restrict normal message delivery.
func TestClampBatchMaxBytes_FloorShouldNotRestrictLargeMessages(t *testing.T) {
	tests := []struct {
		flushBytes int
		name       string
	}{
		{64, "production_blocks_final"},
		{1024, "production_legacy_inv"},
		{0, "zero"},
		{-1, "negative"},
		{1024 * 1024, "one_megabyte"},
	}

	for _, tt := range tests {
		t.Run(tt.name+"_flush_bytes_"+strconv.Itoa(tt.flushBytes), func(t *testing.T) {
			result := clampBatchMaxBytes(tt.flushBytes)

			// The clamped value must be at least 1MB to avoid MESSAGE_TOO_LARGE on
			// production payloads. The current implementation clamps to 512, which
			// is the root cause of the regression.
			require.GreaterOrEqual(t, result, int32(1024*1024),
				"ProducerBatchMaxBytes must be >= 1MB to avoid MESSAGE_TOO_LARGE on Redpanda; "+
					"flush_bytes=%d should not restrict batch size below 1MB", tt.flushBytes)
		})
	}
}
