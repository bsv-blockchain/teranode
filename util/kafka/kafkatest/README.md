# Kafka Performance Testing Framework

Performance tests for `util/kafka` that exercise the **real franz-go code path** against a Kafka-compatible broker (Redpanda) in Docker. Unlike unit tests (which use `in_memory_kafka`), these measure actual throughput, latency, and the impact of configuration changes.

## Prerequisites

- **Docker** running locally
- Pre-pull the required images (one-time):

```bash
docker pull testcontainers/ryuk:0.11.0
docker pull redpandadata/redpanda:v24.3.1
```

## Running

```bash
# All perf tests (detailed table output)
go test -v -run TestPerf -timeout 5m ./util/kafka/

# A specific test
go test -v -run TestPerfAsyncProducerThroughput -timeout 5m ./util/kafka/

# Go benchmarks (standard format, comparable across commits)
go test -bench=. -benchtime=5s -timeout 10m ./util/kafka/

# Unit tests only (skips perf tests)
go test -short ./util/kafka/
```

## Available Tests

**Performance tests** (`kafka_perf_test.go`):

| Test | Measures |
|---|---|
| `TestPerfSyncProducerThroughput` | Sync `Send()` throughput at 256B–100KB |
| `TestPerfAsyncProducerThroughput` | Async `Publish()` throughput at 256B–100KB |
| `TestPerfEndToEndLatency` | Produce-to-consume P50/P90/P99 latency |
| `TestPerfConsumerThroughput` | Consumer read speed from pre-populated topic |
| `TestPerfMultiPartition` | Partition count (1/4/8) impact on throughput |
| `TestPerfVaryingFlushSettings` | `flush_frequency` / `flush_bytes` impact |

**Benchmarks** (`kafka_bench_test.go`):

| Benchmark | Measures |
|---|---|
| `BenchmarkSyncProducer` | `Send()` ops/sec and bytes/sec |
| `BenchmarkAsyncProducer` | `Publish()` ops/sec |
| `BenchmarkEndToEnd` | Full produce+consume throughput |

## Writing a New Test

```go
func TestPerfMyFeature(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping performance test in -short mode")
    }

    ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
    defer cancel()

    env := kafkatest.MustStartEnv(t, ctx)

    kafkaURL, _ := url.Parse(env.TopicURL("my-topic"))
    producer, _ := NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL, nil)
    // ... produce, measure, collect results ...

    r := kafkatest.Result{Name: "my-feature", MessageCount: n, MessageSize: size, Elapsed: elapsed}
    r.ComputeThroughput()
    t.Log(kafkatest.FormatResults([]kafkatest.Result{r}))
}
```

## When to Use

- Before/after adding features to measure throughput/latency delta
- Tuning `flush_bytes` / `flush_frequency` settings
- Comparing sync vs async producer performance
- Validating config changes won't cause regressions
