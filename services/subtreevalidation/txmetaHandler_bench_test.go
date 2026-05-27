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
	_xxhash "github.com/cespare/xxhash/v2"
)

// benchCache is a minimal txMetaCacheOps implementation that counts entries
// instead of writing them, isolating the handler cost from the real cache's
// bucket-locking cost.
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

func (c *benchCache) SetCacheMultiSequential(keys, _ [][]byte) error {
	c.adds.Add(uint64(len(keys)))
	return nil
}

func (c *benchCache) SetCacheMultiSequentialWithHashes(keys, _ [][]byte, _ []uint64) error {
	c.adds.Add(uint64(len(keys)))
	return nil
}

// buildBenchMessage encodes a v1-format Kafka batch. Hash[0] = i%256 spreads
// entries across all 256 hash-byte shards (the old design); under the new
// partition-aligned design these go into N partitions chosen by xxhash so the
// physical distribution is similar but the routing is partition-aware.
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

// buildBenchMessageV2 encodes a v2 (partition-aware) format Kafka batch.
func buildBenchMessageV2(b *testing.B, entriesPerMessage int, payloadSize int) *kafka.KafkaMessage {
	b.Helper()

	entrySize := 8 + 32 + 1 + 4 + payloadSize
	total := 8 + entriesPerMessage*entrySize
	buf := make([]byte, total)

	buf[0] = txmetaWireV2Magic
	buf[1] = txmetaWireV2Version
	// buf[2:4] reserved
	binary.LittleEndian.PutUint32(buf[4:], uint32(entriesPerMessage))
	off := 8

	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i)
	}

	for i := 0; i < entriesPerMessage; i++ {
		binary.LittleEndian.PutUint64(buf[off:], uint64(i)) // pretend hash
		off += 8

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
// SetCacheMultiSequential pipeline with a counting cache.
func benchmarkTxmetaHandlerThroughput(b *testing.B, msg *kafka.KafkaMessage, entriesPerMessage int) {
	cache := &benchCache{}
	server := &Server{
		logger:    ulogger.TestLogger{},
		utxoStore: cache,
	}

	ctx := context.Background()

	const warmIters = 4
	for i := 0; i < warmIters; i++ {
		if err := server.txmetaHandler(ctx, msg); err != nil {
			b.Fatal(err)
		}
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
	elapsed := time.Since(start)
	b.StopTimer()

	entries := float64(uint64(b.N) * uint64(entriesPerMessage))
	b.ReportMetric(entries/elapsed.Seconds(), "tx/s")
	b.ReportMetric(elapsed.Seconds()*1e9/entries, "ns/tx")
}

// v1 wire format (legacy).

func BenchmarkTxmetaHandler_V1_Batch1_Payload64(b *testing.B) {
	benchmarkTxmetaHandlerThroughput(b, buildBenchMessage(b, 1, 64), 1)
}

func BenchmarkTxmetaHandler_V1_Batch100_Payload64(b *testing.B) {
	benchmarkTxmetaHandlerThroughput(b, buildBenchMessage(b, 100, 64), 100)
}

func BenchmarkTxmetaHandler_V1_Batch1000_Payload64(b *testing.B) {
	benchmarkTxmetaHandlerThroughput(b, buildBenchMessage(b, 1000, 64), 1000)
}

func BenchmarkTxmetaHandler_V1_Batch10000_Payload64(b *testing.B) {
	benchmarkTxmetaHandlerThroughput(b, buildBenchMessage(b, 10000, 64), 10000)
}

// v2 wire format (partition-aware; receiver supports both formats).

func BenchmarkTxmetaHandler_V2_Batch1000_Payload64(b *testing.B) {
	benchmarkTxmetaHandlerThroughput(b, buildBenchMessageV2(b, 1000, 64), 1000)
}

func BenchmarkTxmetaHandler_V2_Batch10000_Payload64(b *testing.B) {
	benchmarkTxmetaHandlerThroughput(b, buildBenchMessageV2(b, 10000, 64), 10000)
}

// realCache wraps a real ImprovedCache and implements the txMetaCacheOps
// surface that the handler exercises.
type realCache struct {
	txmetacache.TxMetaCache
	c *txmetacache.ImprovedCache
}

func (r *realCache) Delete(_ context.Context, _ *chainhash.Hash) error {
	return nil
}

func (r *realCache) SetCacheFromBytes(key, value []byte) error {
	_ = r.c.Set(key, value)
	return nil
}

func (r *realCache) SetCacheMulti(keys, values [][]byte) error {
	return r.c.SetMulti(keys, values)
}

func (r *realCache) SetCacheMultiSequential(keys, values [][]byte) error {
	return r.c.SetMultiSequential(keys, values)
}

func (r *realCache) SetCacheMultiSequentialWithHashes(keys, values [][]byte, hashes []uint64) error {
	return r.c.SetMultiSequentialWithHashes(keys, values, hashes)
}

func newRealCache(b *testing.B) *realCache {
	b.Helper()
	c, err := txmetacache.New(256*1024*1024, txmetacache.Native)
	if err != nil {
		b.Fatal(err)
	}
	return &realCache{c: c}
}

// benchmarkTxmetaHandlerRealCache exercises the full handler + real
// production cache. This is the in-pod ceiling against a single goroutine.
// Multiply by NumPartitions for the partition-aware ceiling on a many-core
// pod (no cross-partition bucket contention when partitions own disjoint
// bucket ranges).
func benchmarkTxmetaHandlerRealCache(b *testing.B, msg *kafka.KafkaMessage, entriesPerMessage int) {
	cache := newRealCache(b)
	server := &Server{
		logger:    ulogger.TestLogger{},
		utxoStore: cache,
	}

	ctx := context.Background()

	for i := 0; i < 4; i++ {
		if err := server.txmetaHandler(ctx, msg); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	start := time.Now()
	for i := 0; i < b.N; i++ {
		if err := server.txmetaHandler(ctx, msg); err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	b.StopTimer()

	entries := float64(uint64(b.N) * uint64(entriesPerMessage))
	b.ReportMetric(entries/elapsed.Seconds(), "tx/s")
	b.ReportMetric(elapsed.Seconds()*1e9/entries, "ns/tx")
}

func BenchmarkTxmetaHandlerRealCache_Batch100_Payload64(b *testing.B) {
	benchmarkTxmetaHandlerRealCache(b, buildBenchMessage(b, 100, 64), 100)
}

func BenchmarkTxmetaHandlerRealCache_Batch1000_Payload64(b *testing.B) {
	benchmarkTxmetaHandlerRealCache(b, buildBenchMessage(b, 1000, 64), 1000)
}

func BenchmarkTxmetaHandlerRealCache_Batch10000_Payload64(b *testing.B) {
	benchmarkTxmetaHandlerRealCache(b, buildBenchMessage(b, 10000, 64), 10000)
}

// Cache-level benches: side-by-side SetMulti (errgroup fan-out) vs
// SetMultiSequential (no fan-out). The gap is the handler-side parallelism
// budget we recover by moving fan-out from the cache to the Kafka partition
// layer.

func benchmarkCacheSetMulti(b *testing.B, n, payloadSize int, sequential bool) {
	c, err := txmetacache.New(256*1024*1024, txmetacache.Native)
	if err != nil {
		b.Fatal(err)
	}

	keys := make([][]byte, n)
	vals := make([][]byte, n)
	for i := 0; i < n; i++ {
		k := make([]byte, 32)
		k[0] = byte(i % 256)
		k[1] = byte(i / 256)
		k[2] = byte(i / 65536)
		keys[i] = k
		vals[i] = make([]byte, payloadSize)
	}

	if sequential {
		_ = c.SetMultiSequential(keys, vals)
	} else {
		_ = c.SetMulti(keys, vals)
	}

	b.ResetTimer()
	b.ReportAllocs()

	start := time.Now()
	for i := 0; i < b.N; i++ {
		if sequential {
			_ = c.SetMultiSequential(keys, vals)
		} else {
			_ = c.SetMulti(keys, vals)
		}
	}
	elapsed := time.Since(start)
	b.StopTimer()

	entries := float64(uint64(b.N) * uint64(n))
	b.ReportMetric(entries/elapsed.Seconds(), "tx/s")
	b.ReportMetric(elapsed.Seconds()*1e9/entries, "ns/tx")
}

func BenchmarkCacheSetMulti_Batch1000(b *testing.B) {
	benchmarkCacheSetMulti(b, 1000, 64, false)
}

func BenchmarkCacheSetMulti_Batch10000(b *testing.B) {
	benchmarkCacheSetMulti(b, 10000, 64, false)
}

func BenchmarkCacheSetMultiSequential_Batch1000(b *testing.B) {
	benchmarkCacheSetMulti(b, 1000, 64, true)
}

func BenchmarkCacheSetMultiSequential_Batch10000(b *testing.B) {
	benchmarkCacheSetMulti(b, 10000, 64, true)
}

// benchmarkCacheConcurrent simulates N partition writers calling the cache in
// parallel — the realistic production load shape, not the single-call shape
// that overstates SetMulti's win by handing it free parallelism via errgroup.
//
// Each writer gets its own (keys, values) slab so cache writes don't share
// data; bucket-lock contention is the only sync cost being measured.
func benchmarkCacheConcurrent(b *testing.B, writers, perWriter int, sequential bool) {
	c, err := txmetacache.New(256*1024*1024, txmetacache.Native)
	if err != nil {
		b.Fatal(err)
	}

	allKeys := make([][][]byte, writers)
	allVals := make([][][]byte, writers)
	for w := 0; w < writers; w++ {
		ks := make([][]byte, perWriter)
		vs := make([][]byte, perWriter)
		for i := 0; i < perWriter; i++ {
			k := make([]byte, 32)
			// Distinct keys per writer so writes go to different bucket subsets
			k[0] = byte(w)
			k[1] = byte(i % 256)
			k[2] = byte(i / 256)
			ks[i] = k
			vs[i] = make([]byte, 64)
		}
		allKeys[w] = ks
		allVals[w] = vs
	}

	// Warm
	for w := 0; w < writers; w++ {
		if sequential {
			_ = c.SetMultiSequential(allKeys[w], allVals[w])
		} else {
			_ = c.SetMulti(allKeys[w], allVals[w])
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	var wg = make(chan struct{}, writers)
	start := time.Now()
	for i := 0; i < b.N; i++ {
		for w := 0; w < writers; w++ {
			w := w
			go func() {
				if sequential {
					_ = c.SetMultiSequential(allKeys[w], allVals[w])
				} else {
					_ = c.SetMulti(allKeys[w], allVals[w])
				}
				wg <- struct{}{}
			}()
		}
		for w := 0; w < writers; w++ {
			<-wg
		}
	}
	elapsed := time.Since(start)
	b.StopTimer()

	entries := float64(uint64(b.N) * uint64(writers) * uint64(perWriter))
	b.ReportMetric(entries/elapsed.Seconds(), "tx/s")
	b.ReportMetric(elapsed.Seconds()*1e9/entries, "ns/tx")
}

// 32 writers ≈ NumPartitions; perWriter=1000 ≈ entries per Kafka message.

func BenchmarkCacheConcurrent_SetMulti_32x1000(b *testing.B) {
	benchmarkCacheConcurrent(b, 32, 1000, false)
}

func BenchmarkCacheConcurrent_SetMultiSequential_32x1000(b *testing.B) {
	benchmarkCacheConcurrent(b, 32, 1000, true)
}

func BenchmarkCacheConcurrent_SetMulti_16x1000(b *testing.B) {
	benchmarkCacheConcurrent(b, 16, 1000, false)
}

func BenchmarkCacheConcurrent_SetMultiSequential_16x1000(b *testing.B) {
	benchmarkCacheConcurrent(b, 16, 1000, true)
}

// benchmarkCacheDisjointBuckets simulates the v2 partition-aligned layout:
// each writer's keys hash to a contiguous, disjoint bucket range. Under that
// invariant there is ZERO cross-writer bucket-lock contention, which is what
// the production design targets.
//
// We approximate "all my keys land in my bucket range" by repeatedly hashing
// candidate keys until xxhash(key) % BucketsCount lands inside this writer's
// owned range. This isn't free at bench setup time but is run once per
// writer; the timed loop only sees lock-free SetMultiSequential calls.
func benchmarkCacheDisjointBuckets(b *testing.B, writers, perWriter int) {
	const bucketsCount = 8 * 1024
	bucketsPerWriter := bucketsCount / writers

	c, err := txmetacache.New(256*1024*1024, txmetacache.Native)
	if err != nil {
		b.Fatal(err)
	}

	// Generate keys per writer that land inside the writer's owned buckets.
	allKeys := make([][][]byte, writers)
	allVals := make([][][]byte, writers)
	for w := 0; w < writers; w++ {
		bucketLo := uint64(w) * uint64(bucketsPerWriter)
		bucketHi := bucketLo + uint64(bucketsPerWriter)
		ks := make([][]byte, 0, perWriter)
		vs := make([][]byte, 0, perWriter)
		var n uint64
		for len(ks) < perWriter {
			n++
			k := make([]byte, 32)
			binary.LittleEndian.PutUint64(k, n*uint64(w+1))
			bucket := xxhash64(k) % bucketsCount
			if bucket >= bucketLo && bucket < bucketHi {
				ks = append(ks, k)
				vs = append(vs, make([]byte, 64))
			}
		}
		allKeys[w] = ks
		allVals[w] = vs
	}

	// Warm
	for w := 0; w < writers; w++ {
		_ = c.SetMultiSequential(allKeys[w], allVals[w])
	}

	b.ResetTimer()
	b.ReportAllocs()

	wg := make(chan struct{}, writers)
	start := time.Now()
	for i := 0; i < b.N; i++ {
		for w := 0; w < writers; w++ {
			w := w
			go func() {
				_ = c.SetMultiSequential(allKeys[w], allVals[w])
				wg <- struct{}{}
			}()
		}
		for w := 0; w < writers; w++ {
			<-wg
		}
	}
	elapsed := time.Since(start)
	b.StopTimer()

	entries := float64(uint64(b.N) * uint64(writers) * uint64(perWriter))
	b.ReportMetric(entries/elapsed.Seconds(), "tx/s")
	b.ReportMetric(elapsed.Seconds()*1e9/entries, "ns/tx")
}

// xxhash64 is exported here only to compute "which bucket would the cache
// hash this key into" — it must match the cache's internal xxhash. We can't
// reach into the cache package's internal func so we wrap the canonical
// import inline; if the cache ever switches hash function the asserted
// invariant breaks, but at that point both ends would need adjustment.
func xxhash64(k []byte) uint64 {
	return _xxhash.Sum64(k)
}

func BenchmarkCacheDisjointBuckets_32x1000(b *testing.B) {
	benchmarkCacheDisjointBuckets(b, 32, 1000)
}

func BenchmarkCacheDisjointBuckets_16x1000(b *testing.B) {
	benchmarkCacheDisjointBuckets(b, 16, 1000)
}
