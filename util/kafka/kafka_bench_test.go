package kafka

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka/kafkatest"
	"github.com/stretchr/testify/require"
)

// Benchmarks that use a real Redpanda broker via testcontainers.
// Run with:  go test -bench=. -benchtime=5s -timeout 10m ./util/kafka/
//
// These benchmarks provide comparable results across code changes — ideal for
// measuring the impact of new features (transfer rate monitoring, batching
// changes, etc.) on producer/consumer throughput.

// sharedBenchEnv lazily starts a single Redpanda container shared across all
// benchmarks in the same test binary invocation. The container is terminated
// when the process exits (via the testing cleanup mechanism).
var sharedBenchEnv struct {
	env  *kafkatest.Env
	once sync.Once
}

func getBenchEnv(b *testing.B) *kafkatest.Env {
	sharedBenchEnv.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		sharedBenchEnv.env = kafkatest.MustStartEnv(b, ctx)
	})
	return sharedBenchEnv.env
}

func BenchmarkSyncProducer(b *testing.B) {
	env := getBenchEnv(b)
	sizes := []int{256, 1024, 10 * 1024, 100 * 1024}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("msg_%dB", size), func(b *testing.B) {
			ctx := context.Background()
			topic := fmt.Sprintf("bench-sync-%d-%d", size, time.Now().UnixNano()%100000)
			kafkaURL, err := url.Parse(env.TopicURL(topic))
			require.NoError(b, err)

			producer, err := NewKafkaProducerWithContext(ctx, kafkaURL, nil)
			require.NoError(b, err)
			defer producer.Close()

			payload := make([]byte, size)
			_, _ = rand.Read(payload)
			key := make([]byte, 4)
			_, _ = rand.Read(key)

			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := producer.Send(key, payload); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}

func BenchmarkAsyncProducer(b *testing.B) {
	env := getBenchEnv(b)
	logger := ulogger.TestLogger{}
	sizes := []int{256, 1024, 10 * 1024, 100 * 1024}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("msg_%dB", size), func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			topic := fmt.Sprintf("bench-async-%d-%d", size, time.Now().UnixNano()%100000)
			kafkaURL, err := url.Parse(env.TopicURL(topic))
			require.NoError(b, err)

			producer, err := NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL, nil)
			require.NoError(b, err)

			ch := make(chan *Message, 50000)
			producer.Start(ctx, ch)

			payload := make([]byte, size)
			_, _ = rand.Read(payload)

			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				producer.Publish(&Message{
					Key:   []byte(fmt.Sprintf("bk%d", i)),
					Value: payload,
				})
			}
			b.StopTimer()

			require.NoError(b, producer.Stop())
		})
	}
}

func BenchmarkEndToEnd(b *testing.B) {
	env := getBenchEnv(b)
	logger := ulogger.TestLogger{}

	sizes := []int{256, 1024, 10 * 1024}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("msg_%dB", size), func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			topic := fmt.Sprintf("bench-e2e-%d-%d", size, time.Now().UnixNano()%100000)
			kafkaURL, err := url.Parse(env.TopicURL(topic,
				"partitions=1&replication=1&retention=60000&flush_frequency=10ms&replay=1"))
			require.NoError(b, err)

			producer, err := NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL, nil)
			require.NoError(b, err)
			ch := make(chan *Message, 50000)
			producer.Start(ctx, ch)

			consumer, err := NewKafkaConsumerGroupFromURL(logger, kafkaURL,
				fmt.Sprintf("bench-e2e-cg-%d", time.Now().UnixNano()%100000), true, nil)
			require.NoError(b, err)

			var received atomic.Int64
			consumer.Start(ctx, func(msg *KafkaMessage) error {
				received.Add(1)
				return nil
			})

			time.Sleep(500 * time.Millisecond)

			payload := make([]byte, size)
			_, _ = rand.Read(payload)

			b.SetBytes(int64(size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				producer.Publish(&Message{
					Key:   []byte(fmt.Sprintf("be%d", i)),
					Value: payload,
				})
			}

			// Wait for consumer to catch up.
			deadline := time.After(60 * time.Second)
			for received.Load() < int64(b.N) {
				select {
				case <-deadline:
					b.Fatalf("timed out: received %d/%d", received.Load(), b.N)
				default:
					time.Sleep(time.Millisecond)
				}
			}
			b.StopTimer()

			require.NoError(b, producer.Stop())
			require.NoError(b, consumer.Close())
		})
	}
}
