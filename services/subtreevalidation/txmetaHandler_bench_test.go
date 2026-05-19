package subtreevalidation

import (
	"context"
	"encoding/binary"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
)

// benchCache is a minimal txMetaCacheOps implementation that counts entries
// instead of writing them, isolating the handler + worker dispatch cost from
// the real cache's bucket-locking cost.
//
// The embedded txmetacache.TxMetaCache zero value satisfies the broader
// utxo.Store surface that the Server holds; only the three methods overridden
// below are ever called on the bench path.
type benchCache struct {
	txmetacache.TxMetaCache
	adds    atomic.Uint64
	deletes atomic.Uint64
}

func (c *benchCache) Delete(_ context.Context, _ *chainhash.Hash) error {
	c.deletes.Add(1)
	return nil
}

func (c *benchCache) SetCacheFromBytes(_, _ []byte) error {
	c.adds.Add(1)
	return nil
}

func (c *benchCache) SetCacheMulti(keys, _ [][]byte) error {
	c.adds.Add(uint64(len(keys)))
	return nil
}

// buildBenchMessage encodes a synthetic Kafka batch message in the binary
// format documented on txmetaHandler. Hashes are spread across all 256
// shards (h[0] = i % 256) so dispatch hits every worker.
func buildBenchMessage(b *testing.B, entriesPerMessage int, payloadSize int) *kafka.KafkaMessage {
	b.Helper()

	entrySize := 32 + 1 + 4 + payloadSize
	total := 4 + entriesPerMessage*entrySize
	buf := make([]byte, total)
	off := 0

	binary.LittleEndian.PutUint32(buf[off:], uint32(entriesPerMessage))
	off += 4

	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i)
	}

	for i := 0; i < entriesPerMessage; i++ {
		buf[off+0] = byte(i % 256)
		buf[off+1] = byte(i / 256)
		off += 32

		buf[off] = txmetaActionADD
		off++

		binary.LittleEndian.PutUint32(buf[off:], uint32(payloadSize))
		off += 4

		copy(buf[off:], payload)
		off += payloadSize
	}

	return &kafka.KafkaMessage{Value: buf}
}

// benchmarkTxmetaHandlerThroughput drives the full Kafka-handler →
// per-shard channel → worker → cache pipeline with a counting cache and
// reports entries/sec. The handler dispatch is blocking (startup mode)
// throughout the bench window, so a slow worker would surface as the
// benchmark stalling — measured throughput is bottlenecked by whichever
// stage is actually slow.
func benchmarkTxmetaHandlerThroughput(b *testing.B, entriesPerMessage, payloadSize int) {
	cache := &benchCache{}

	server := &Server{
		logger:    ulogger.TestLogger{},
		utxoStore: cache,
	}

	msg := buildBenchMessage(b, entriesPerMessage, payloadSize)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		if server.txmetaWorkerCancel != nil {
			server.txmetaWorkerCancel()
		}
		server.txmetaWorkerWg.Wait()
	}()

	const warmIters = 4
	for i := 0; i < warmIters; i++ {
		if err := server.txmetaHandler(ctx, msg); err != nil {
			b.Fatal(err)
		}
	}
	expectedWarm := uint64(warmIters * entriesPerMessage)
	for cache.adds.Load() < expectedWarm {
		time.Sleep(time.Microsecond)
	}
	cache.adds.Store(0)

	b.ResetTimer()
	b.ReportAllocs()

	start := time.Now()
	for i := 0; i < b.N; i++ {
		if err := server.txmetaHandler(ctx, msg); err != nil {
			b.Fatal(err)
		}
	}

	expected := uint64(b.N) * uint64(entriesPerMessage)
	for cache.adds.Load() < expected {
	}
	elapsed := time.Since(start)
	b.StopTimer()

	entries := float64(expected)
	b.ReportMetric(entries/elapsed.Seconds(), "tx/s")
	b.ReportMetric(elapsed.Seconds()*1e9/entries, "ns/tx")
}

// Matrix brackets realistic Kafka batch sizes for the txmeta topic
// (low-thousands of entries per message) and payload sizes for serialised
// meta.Data (tens of bytes).

func BenchmarkTxmetaHandler_Batch1_Payload64(b *testing.B) {
	benchmarkTxmetaHandlerThroughput(b, 1, 64)
}

func BenchmarkTxmetaHandler_Batch100_Payload64(b *testing.B) {
	benchmarkTxmetaHandlerThroughput(b, 100, 64)
}

func BenchmarkTxmetaHandler_Batch1000_Payload64(b *testing.B) {
	benchmarkTxmetaHandlerThroughput(b, 1000, 64)
}

func BenchmarkTxmetaHandler_Batch10000_Payload64(b *testing.B) {
	benchmarkTxmetaHandlerThroughput(b, 10000, 64)
}

func BenchmarkTxmetaHandler_Batch1000_Payload32(b *testing.B) {
	benchmarkTxmetaHandlerThroughput(b, 1000, 32)
}

func BenchmarkTxmetaHandler_Batch1000_Payload128(b *testing.B) {
	benchmarkTxmetaHandlerThroughput(b, 1000, 128)
}
