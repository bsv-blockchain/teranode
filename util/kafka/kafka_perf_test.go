//go:build perf

package kafka

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/url"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka/kafkatest"
	"github.com/stretchr/testify/require"
)

// Performance tests that exercise the real Kafka (Redpanda) path via testcontainers.
// Run with:  go test -tags perf -v -run TestPerf -timeout 15m ./util/kafka/
//
// These are NOT unit tests — they spin up a Docker container and measure real
// throughput, latency, and resource behaviour of the producer/consumer stack.
// Excluded from `make test` via the `perf` build tag because of Docker pull
// overhead and per-runner CPU variance under -race.

func TestPerfSyncProducerThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in -short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	env := kafkatest.MustStartEnv(t, ctx)

	messageSizes := []int{256, 1024, 10 * 1024, 100 * 1024}
	messageCount := 5000
	var results []kafkatest.Result

	for _, size := range messageSizes {
		topic := fmt.Sprintf("perf-sync-%d-%d", size, time.Now().UnixNano()%10000)
		kafkaURL, err := url.Parse(env.TopicURL(topic))
		require.NoError(t, err)

		producer, err := NewKafkaProducerWithContext(ctx, kafkaURL, nil)
		require.NoError(t, err)

		payload := make([]byte, size)
		_, _ = rand.Read(payload)
		key := make([]byte, 4)
		_, _ = rand.Read(key)

		start := time.Now()
		for i := 0; i < messageCount; i++ {
			require.NoError(t, producer.Send(key, payload))
		}
		elapsed := time.Since(start)

		require.NoError(t, producer.Close())

		r := kafkatest.Result{
			Name:         fmt.Sprintf("sync-producer/%dB", size),
			MessageCount: messageCount,
			MessageSize:  size,
			Elapsed:      elapsed,
		}
		r.ComputeThroughput()
		results = append(results, r)

		t.Logf("sync-producer/%dB: %d msgs in %v (%.2f MB/s, %.0f msgs/s)",
			size, messageCount, elapsed, r.ThroughputMBps, r.ThroughputMsgs)
	}

	t.Log(kafkatest.FormatResults(results))
}

func TestPerfAsyncProducerThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in -short mode")
	}

	logger := ulogger.TestLogger{}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	env := kafkatest.MustStartEnv(t, ctx)

	messageSizes := []int{256, 1024, 10 * 1024, 100 * 1024}
	messageCount := 10000
	var results []kafkatest.Result

	for _, size := range messageSizes {
		topic := fmt.Sprintf("perf-async-%d-%d", size, time.Now().UnixNano()%10000)
		kafkaURL, err := url.Parse(env.TopicURL(topic))
		require.NoError(t, err)

		producer, err := NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL, nil)
		require.NoError(t, err)

		ch := make(chan *Message, 10000)
		producer.Start(ctx, ch)

		payload := make([]byte, size)
		_, _ = rand.Read(payload)

		start := time.Now()
		for i := 0; i < messageCount; i++ {
			producer.Publish(&Message{
				Key:   []byte(fmt.Sprintf("k%d", i)),
				Value: payload,
			})
		}
		// Flush by stopping — this waits for in-flight records.
		require.NoError(t, producer.Stop())
		elapsed := time.Since(start)

		r := kafkatest.Result{
			Name:         fmt.Sprintf("async-producer/%dB", size),
			MessageCount: messageCount,
			MessageSize:  size,
			Elapsed:      elapsed,
		}
		r.ComputeThroughput()
		results = append(results, r)

		t.Logf("async-producer/%dB: %d msgs in %v (%.2f MB/s, %.0f msgs/s)",
			size, messageCount, elapsed, r.ThroughputMBps, r.ThroughputMsgs)
	}

	t.Log(kafkatest.FormatResults(results))
}

func TestPerfEndToEndLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in -short mode")
	}

	logger := ulogger.TestLogger{}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	env := kafkatest.MustStartEnv(t, ctx)

	messageSizes := []int{256, 1024, 10 * 1024}
	messageCount := 1000
	var results []kafkatest.Result

	for _, size := range messageSizes {
		topic := fmt.Sprintf("perf-e2e-%d-%d", size, time.Now().UnixNano()%10000)
		kafkaURL, err := url.Parse(env.TopicURL(topic,
			"partitions=1&replication=1&retention=60000&flush_frequency=10ms&replay=1"))
		require.NoError(t, err)

		producer, err := NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL, nil)
		require.NoError(t, err)
		ch := make(chan *Message, messageCount)
		producer.Start(ctx, ch)

		consumer, err := NewKafkaConsumerGroupFromURL(logger, kafkaURL,
			fmt.Sprintf("perf-e2e-cg-%d", time.Now().UnixNano()%10000), true, nil)
		require.NoError(t, err)

		var (
			latencies    = make([]time.Duration, 0, messageCount)
			latenciesMu  sync.Mutex
			receivedDone = make(chan struct{})
			received     atomic.Int64
		)

		// sendTimes tracks when each message key was published.
		sendTimes := &sync.Map{}

		consumer.Start(ctx, func(msg *KafkaMessage) error {
			key := string(msg.Key)
			if v, ok := sendTimes.Load(key); ok {
				lat := time.Since(v.(time.Time))
				latenciesMu.Lock()
				latencies = append(latencies, lat)
				latenciesMu.Unlock()
			}
			if received.Add(1) >= int64(messageCount) {
				close(receivedDone)
			}
			return nil
		})

		// Let consumer join the group before producing.
		time.Sleep(500 * time.Millisecond)

		payload := make([]byte, size)
		_, _ = rand.Read(payload)

		start := time.Now()
		for i := 0; i < messageCount; i++ {
			key := fmt.Sprintf("e2e-%d", i)
			sendTimes.Store(key, time.Now())
			producer.Publish(&Message{
				Key:   []byte(key),
				Value: payload,
			})
		}

		select {
		case <-receivedDone:
		case <-time.After(60 * time.Second):
			t.Fatalf("e2e/%dB: timed out waiting for %d messages (received %d)", size, messageCount, received.Load())
		}
		elapsed := time.Since(start)

		require.NoError(t, producer.Stop())
		require.NoError(t, consumer.Close())

		latenciesMu.Lock()
		r := kafkatest.Result{
			Name:         fmt.Sprintf("e2e-latency/%dB", size),
			MessageCount: messageCount,
			MessageSize:  size,
			Elapsed:      elapsed,
		}
		r.ComputeThroughput()
		r.ComputeLatencyPercentiles(latencies)
		latenciesMu.Unlock()

		results = append(results, r)

		t.Logf("e2e/%dB: %d msgs in %v — P50=%v P90=%v P99=%v (%.0f msgs/s)",
			size, messageCount, elapsed, r.P50Latency, r.P90Latency, r.P99Latency, r.ThroughputMsgs)
	}

	t.Log(kafkatest.FormatResults(results))
}

func TestPerfConsumerThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in -short mode")
	}

	logger := ulogger.TestLogger{}
	// This test pre-populates topics with 10k sync sends per size, which can take
	// several minutes under -race in CI. Keep the parent context comfortably larger
	// than total test runtime so consumer loops do not inherit an already-expired ctx.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	env := kafkatest.MustStartEnv(t, ctx)

	messageSizes := []int{256, 1024, 10 * 1024}
	messageCount := 1000
	var results []kafkatest.Result

	for _, size := range messageSizes {
		topic := fmt.Sprintf("perf-consume-%d-%d", size, time.Now().UnixNano()%10000)
		kafkaURL, err := url.Parse(env.TopicURL(topic,
			"partitions=1&replication=1&retention=60000&flush_frequency=10ms&replay=1"))
		require.NoError(t, err)

		// Pre-populate the topic with messages using the sync producer.
		producer, err := NewKafkaProducerWithContext(ctx, kafkaURL, nil)
		require.NoError(t, err)

		payload := make([]byte, size)
		_, _ = rand.Read(payload)
		key := make([]byte, 4)
		_, _ = rand.Read(key)

		for i := 0; i < messageCount; i++ {
			require.NoError(t, producer.Send(key, payload))
		}
		require.NoError(t, producer.Close())

		// Now measure consumer read throughput.
		consumer, err := NewKafkaConsumerGroupFromURL(logger, kafkaURL,
			fmt.Sprintf("perf-consume-cg-%d", time.Now().UnixNano()%10000), true, nil)
		require.NoError(t, err)

		var (
			receivedDone = make(chan struct{})
			received     atomic.Int64
		)

		start := time.Now()
		consumer.Start(ctx, func(msg *KafkaMessage) error {
			if received.Add(1) >= int64(messageCount) {
				close(receivedDone)
			}
			return nil
		})

		select {
		case <-receivedDone:
		case <-time.After(60 * time.Second):
			t.Fatalf("consumer/%dB: timed out (received %d/%d)", size, received.Load(), messageCount)
		}
		elapsed := time.Since(start)

		require.NoError(t, consumer.Close())

		r := kafkatest.Result{
			Name:         fmt.Sprintf("consumer/%dB", size),
			MessageCount: messageCount,
			MessageSize:  size,
			Elapsed:      elapsed,
		}
		r.ComputeThroughput()
		results = append(results, r)

		t.Logf("consumer/%dB: %d msgs in %v (%.2f MB/s, %.0f msgs/s)",
			size, messageCount, elapsed, r.ThroughputMBps, r.ThroughputMsgs)
	}

	t.Log(kafkatest.FormatResults(results))
}

// TestPerfMultiPartition measures throughput when producing across multiple partitions.
func TestPerfMultiPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in -short mode")
	}

	logger := ulogger.TestLogger{}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	env := kafkatest.MustStartEnv(t, ctx)

	partitionCounts := []int{1, 4, 8}
	messageCount := 10000
	msgSize := 1024
	var results []kafkatest.Result

	for _, partitions := range partitionCounts {
		topic := fmt.Sprintf("perf-part-%d-%d", partitions, time.Now().UnixNano()%10000)
		query := fmt.Sprintf("partitions=%d&replication=1&retention=60000&flush_frequency=100ms", partitions)
		kafkaURL, err := url.Parse(env.TopicURL(topic, query))
		require.NoError(t, err)

		producer, err := NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL, nil)
		require.NoError(t, err)
		ch := make(chan *Message, messageCount)
		producer.Start(ctx, ch)

		payload := make([]byte, msgSize)
		_, _ = rand.Read(payload)

		start := time.Now()
		for i := 0; i < messageCount; i++ {
			producer.Publish(&Message{
				Key:   []byte(fmt.Sprintf("pk%d", i)),
				Value: payload,
			})
		}
		require.NoError(t, producer.Stop())
		elapsed := time.Since(start)

		r := kafkatest.Result{
			Name:         fmt.Sprintf("multi-partition/%dp", partitions),
			MessageCount: messageCount,
			MessageSize:  msgSize,
			Elapsed:      elapsed,
		}
		r.ComputeThroughput()
		results = append(results, r)

		t.Logf("partitions=%d: %d msgs in %v (%.2f MB/s, %.0f msgs/s)",
			partitions, messageCount, elapsed, r.ThroughputMBps, r.ThroughputMsgs)
	}

	t.Log(kafkatest.FormatResults(results))
}

// TestPerfVaryingFlushSettings compares different flush_frequency and flush_bytes
// configurations to measure their impact on throughput.
func TestPerfVaryingFlushSettings(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in -short mode")
	}

	logger := ulogger.TestLogger{}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	env := kafkatest.MustStartEnv(t, ctx)

	configs := []struct {
		label          string
		flushFrequency string
		flushBytes     string
	}{
		{"freq=10ms/bytes=1MB", "10ms", "1048576"},
		{"freq=100ms/bytes=1MB", "100ms", "1048576"},
		{"freq=1s/bytes=1MB", "1s", "1048576"},
		{"freq=100ms/bytes=16KB", "100ms", "16384"},
		{"freq=100ms/bytes=64", "100ms", "64"},
	}
	messageCount := 10000
	msgSize := 1024
	var results []kafkatest.Result

	for _, cfg := range configs {
		topic := fmt.Sprintf("perf-flush-%s-%d", sanitizeTopicComponent(cfg.label), time.Now().UnixNano()%10000)
		query := fmt.Sprintf("partitions=1&replication=1&retention=60000&flush_frequency=%s&flush_bytes=%s",
			cfg.flushFrequency, cfg.flushBytes)
		kafkaURL, err := url.Parse(env.TopicURL(topic, query))
		require.NoError(t, err)

		producer, err := NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL, nil)
		require.NoError(t, err)
		ch := make(chan *Message, messageCount)
		producer.Start(ctx, ch)

		payload := make([]byte, msgSize)
		_, _ = rand.Read(payload)

		start := time.Now()
		for i := 0; i < messageCount; i++ {
			producer.Publish(&Message{
				Key:   []byte(fmt.Sprintf("fk%d", i)),
				Value: payload,
			})
		}
		require.NoError(t, producer.Stop())
		elapsed := time.Since(start)

		r := kafkatest.Result{
			Name:         fmt.Sprintf("flush/%s", cfg.label),
			MessageCount: messageCount,
			MessageSize:  msgSize,
			Elapsed:      elapsed,
		}
		r.ComputeThroughput()
		results = append(results, r)

		t.Logf("%s: %d msgs in %v (%.2f MB/s, %.0f msgs/s)",
			cfg.label, messageCount, elapsed, r.ThroughputMBps, r.ThroughputMsgs)
	}

	t.Log(kafkatest.FormatResults(results))
}

var invalidKafkaTopicChars = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)

func sanitizeTopicComponent(raw string) string {
	sanitized := invalidKafkaTopicChars.ReplaceAllString(raw, "-")
	if sanitized == "" {
		return "perf"
	}
	return sanitized
}
