package subtreevalidation

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	inmemkafka "github.com/bsv-blockchain/teranode/util/kafka/in_memory_kafka"
	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
)

const (
	e2eBucketsCount = txmetacache.BucketsCount
)

// e2eCache is a thin txMetaCacheOps adapter over a real ImprovedCache. We
// don't use the full txmetacache.TxMetaCache wrapper here so we can verify
// payloads with raw bytes (the wrapper decodes into meta.Data and demands
// a valid serialization). Test payloads are arbitrary bytes, so we read
// them back the same way they were written.
type e2eCache struct {
	txmetacache.TxMetaCache
	c *txmetacache.ImprovedCache
}

func (e *e2eCache) Delete(_ context.Context, _ *chainhash.Hash) error { return nil }

func (e *e2eCache) SetCacheFromBytes(key, value []byte) error {
	_ = e.c.Set(key, value)
	return nil
}

func (e *e2eCache) SetCacheMulti(keys, values [][]byte) error {
	return e.c.SetMulti(keys, values)
}

func (e *e2eCache) SetCacheMultiSequential(keys, values [][]byte) error {
	return e.c.SetMultiSequential(keys, values)
}

func (e *e2eCache) SetCacheMultiSequentialWithHashes(keys, values [][]byte, hashes []uint64) error {
	return e.c.SetMultiSequentialWithHashes(keys, values, hashes)
}

func (e *e2eCache) GetRaw(key []byte) ([]byte, bool) {
	var dst []byte
	if err := e.c.Get(&dst, key); err != nil {
		return nil, false
	}
	if len(dst) == 0 {
		return nil, false
	}
	return dst, true
}

// txmetaE2EHarness wires:
//
//	[validator producer-side serializeTxMetaBatchV2] →
//	[KafkaAsyncProducer in memory:// mode] →
//	[shared in-memory broker] →
//	[KafkaConsumerGroup in memory:// mode] →
//	[Server.txmetaHandler] →
//	[real TxMetaCache backed by ImprovedCache (Native)]
//
// Everything runs in-process; no real Kafka, no goroutine fan-out other than
// what the production producer/consumer code spawns themselves. The cache is
// a real one so Get-after-Set asserts behave like production.
type txmetaE2EHarness struct {
	t        *testing.T
	ctx      context.Context
	cancel   context.CancelFunc
	logger   ulogger.Logger
	producer *kafka.KafkaAsyncProducer
	consumer *kafka.KafkaConsumerGroup
	server   *Server
	cache    *e2eCache
	topic    string

	// processed counts entries that the handler has acked through the
	// cache (ADDs counted on len(keys), DELETEs counted on each call).
	// The bench loops poll this to know when in-flight work is complete.
	processed atomic.Uint64
}

// newTxmetaE2EHarness builds a fresh end-to-end pipeline. wireFormat is "v1"
// or "v2" — the producer path is selected by validator.Validator.sendTxMetaBatch.
func newTxmetaE2EHarness(t testing.TB, wireFormat string, numPartitions int) *txmetaE2EHarness {
	t.Helper()

	logger := ulogger.TestLogger{}
	topic := "txmeta-e2e-" + wireFormat + "-" + randomTopicSuffix()

	// Memory-scheme URL the producer/consumer factories understand.
	u, err := url.Parse("memory://broker/" + topic)
	require.NoError(t, err)

	// Producer: ManualPartitioning when v2 (the daemon wires this in
	// production; the in-memory async producer ignores Partition because
	// the in-memory broker has no concept of partitions, so the flag is
	// effectively a no-op for the bench. We set it anyway to mirror prod
	// configuration exactly.)
	var opts []kafka.ProducerOption
	if wireFormat == "v2" {
		opts = append(opts, kafka.WithManualPartitioning())
	}
	ctx, cancel := context.WithCancel(context.Background())
	producer, err := kafka.NewKafkaAsyncProducerFromURL(ctx, logger, u, nil, opts...)
	require.NoError(t, err)
	producer.Start(ctx, make(chan *kafka.Message, 256))

	// Consumer: unique group id so each test is independent.
	consumer, err := kafka.NewKafkaConsumerGroupFromURL(logger, u, "subtreevalidation-e2e-"+randomTopicSuffix(), true, nil)
	require.NoError(t, err)

	settingsObj := &settings.Settings{
		Validator: settings.ValidatorSettings{
			TxMetaWireFormat:    wireFormat,
			TxMetaNumPartitions: numPartitions,
		},
	}

	// Cache size of 32 MB — see ImprovedCache.New minBucketBytes floor of
	// ChunkSize*8 (= 32 KB per bucket × 8192 buckets = 256 MB worst case),
	// but for smaller caches it pre-allocates fewer slabs. 32 MB total
	// keeps the bench's resident footprint low while leaving more than
	// enough headroom for the entriesPerBatch ≤ 1000 unique entries the
	// bench reuses across all iterations.
	rawCache, err := txmetacache.New(32*1024*1024, txmetacache.Native)
	require.NoError(t, err)
	cache := &e2eCache{c: rawCache}

	server := &Server{
		logger:    logger,
		utxoStore: cache,
		settings:  settingsObj,
	}

	h := &txmetaE2EHarness{
		t:        asT(t),
		ctx:      ctx,
		cancel:   cancel,
		logger:   logger,
		producer: producer,
		consumer: consumer,
		server:   server,
		cache:    cache,
		topic:    topic,
	}

	// Start the consumer. We wrap the handler so we can count entries
	// processed, which is what bench/test code polls instead of guessing
	// at flush+process latency.
	consumer.Start(ctx, func(msg *kafka.KafkaMessage) error {
		n := countEntries(msg.Value)
		err := h.server.txmetaHandler(ctx, msg)
		if err == nil {
			h.processed.Add(uint64(n))
		}
		return err
	})

	// The in-memory broker's Produce drops messages when a topic has zero
	// consumers OR when the consumer's channel is full (select-default).
	// Consumer.Start returns immediately while the consumer goroutine is
	// still registering. We poll the broker's per-topic consumer list to
	// confirm registration before letting callers publish. Avoids the
	// "processed 0" timeout flake.
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		if inmemkafka.GetSharedBroker().HasConsumer(topic) {
			return h
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("in-memory consumer never registered for topic %s", topic)
	return h
}

func (h *txmetaE2EHarness) close() {
	// Producer first so no more sends are in flight, then consumer (which
	// stops the inflight handler invocations), then cancel ctx.
	_ = h.producer.Stop()
	_ = h.consumer.Close()
	h.cancel()
	// Release the broker-side retained-messages buffer; otherwise the
	// shared singleton pins everything we produced for the lifetime of
	// the test process.
	inmemkafka.GetSharedBroker().DropTopic(h.topic)
}

// publishV1 emits a single v1-format Kafka message containing the provided
// batch (mirroring services/validator.serializeTxMetaBatch behaviour). Used
// by the v1 e2e tests so we don't need to construct a full Validator.
func (h *txmetaE2EHarness) publishV1(t testing.TB, hashes []chainhash.Hash, payloads [][]byte) {
	t.Helper()

	value := serializeV1ForE2E(hashes, payloads)
	h.producer.Publish(&kafka.Message{
		Key:   hashes[0][:],
		Value: value,
	})
}

// publishV2 mirrors validator.sendTxMetaBatchV2: groups items by partition
// using the documented routing rule and emits one Kafka record per non-empty
// partition. Byte layout matches validator.serializeTxMetaBatchV2 exactly;
// the validator-side unit tests assert that layout, so the e2e test only has
// to keep a single copy of the math.
func (h *txmetaE2EHarness) publishV2(t testing.TB, hashes []chainhash.Hash, payloads [][]byte) {
	t.Helper()

	numPartitions := h.server.settings.Validator.TxMetaNumPartitions
	// Cap to BucketsCount so the floor below keeps bucketsPerPartition >= 1.
	// Under the testtxmetacache build tag BucketsCount=8, so the harness's
	// default 32 partitions would otherwise divide to zero.
	if numPartitions > e2eBucketsCount {
		numPartitions = e2eBucketsCount
	}
	bucketsPerPartition := e2eBucketsCount / numPartitions
	if bucketsPerPartition < 1 {
		bucketsPerPartition = 1
	}

	type item struct {
		hash    chainhash.Hash
		payload []byte
		h       uint64
	}
	partitions := make([][]item, numPartitions)
	for i := range hashes {
		hv := xxhash.Sum64(hashes[i][:])
		bucket := int(hv % uint64(e2eBucketsCount))
		p := bucket / bucketsPerPartition
		partitions[p] = append(partitions[p], item{hash: hashes[i], payload: payloads[i], h: hv})
	}

	for p, items := range partitions {
		if len(items) == 0 {
			continue
		}

		size := 8 // header
		for _, it := range items {
			size += 8 + 32 + 1 + 4 + len(it.payload)
		}
		buf := make([]byte, size)
		buf[0] = txmetacache.WireV2Magic
		buf[1] = txmetacache.WireV2Version
		putUint32LE(buf[4:], uint32(len(items)))
		off := 8

		for _, it := range items {
			binary8LE(buf[off:], it.h)
			off += 8
			copy(buf[off:], it.hash[:])
			off += 32
			buf[off] = txmetacache.WireActionADD
			off++
			putUint32LE(buf[off:], uint32(len(it.payload)))
			off += 4
			copy(buf[off:], it.payload)
			off += len(it.payload)
		}

		h.producer.Publish(&kafka.Message{
			Partition: int32(p), //nolint:gosec // p < NumPartitions
			Value:     buf,
		})
	}
}

// waitProcessed polls the harness processed counter until at least n entries
// have been handled, or the context deadline fires.
func (h *txmetaE2EHarness) waitProcessed(t testing.TB, target uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for h.processed.Load() < target {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: processed %d / %d entries within %s", h.processed.Load(), target, timeout)
		}
		time.Sleep(time.Millisecond)
	}
}

// serializeV1ForE2E mirrors services/validator.serializeTxMetaBatch on the
// types this test file already has — replicated here to avoid a circular
// import (validator imports nothing from subtreevalidation, but the test
// file lives in subtreevalidation).
func serializeV1ForE2E(hashes []chainhash.Hash, payloads [][]byte) []byte {
	size := 4
	for i := range hashes {
		size += 32 + 1 + 4 + len(payloads[i])
	}
	buf := make([]byte, size)
	putUint32LE(buf[0:], uint32(len(hashes)))
	off := 4
	for i := range hashes {
		copy(buf[off:], hashes[i][:])
		off += 32
		buf[off] = txmetacache.WireActionADD
		off++
		putUint32LE(buf[off:], uint32(len(payloads[i])))
		off += 4
		copy(buf[off:], payloads[i])
		off += len(payloads[i])
	}
	return buf
}

func putUint32LE(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}

// countEntries inspects a raw Kafka message payload (either wire format) to
// count how many txmeta entries it carries. Used purely as a bench progress
// counter — never parses payloads or validates layout beyond the entry count.
func countEntries(data []byte) int {
	if len(data) < 4 {
		return 0
	}
	if data[0] == txmetacache.WireV2Magic {
		if len(data) < 8 {
			return 0
		}
		return int(uint32(data[4]) | uint32(data[5])<<8 | uint32(data[6])<<16 | uint32(data[7])<<24)
	}
	return int(uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24)
}

// asT returns the concrete *testing.T if available, otherwise nil. Used so
// the harness can hold a typed pointer for test-only paths while still
// accepting testing.TB at the constructor boundary (so benchmarks can drive
// it).
func asT(tb testing.TB) *testing.T {
	if t, ok := tb.(*testing.T); ok {
		return t
	}
	return nil
}

// randomTopicSuffix returns a process-unique suffix so each harness gets a
// fresh topic in the shared in-memory broker (otherwise different tests'
// messages pile up on each other).
var topicSeq atomic.Uint64

func randomTopicSuffix() string {
	n := topicSeq.Add(1)
	return formatUint(n)
}

func formatUint(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// ---------- Tests ----------

// TestTxmetaE2E_V1_RoundTrip drives a small v1 batch through the full
// validator-producer→in-mem broker→consumer→handler→cache pipeline and
// verifies entries are retrievable from the cache afterwards.
func TestTxmetaE2E_V1_RoundTrip(t *testing.T) {
	h := newTxmetaE2EHarness(t, "v1", 32)
	defer h.close()

	hashes, payloads := makeUniqueHashes(t, 16, 64)
	h.publishV1(t, hashes, payloads)
	h.waitProcessed(t, 16, 5*time.Second)

	// Verify every hash is in the cache.
	for i := range hashes {
		got, ok := h.cache.GetRaw(hashes[i][:])
		require.Truef(t, ok, "entry %d not in cache", i)
		require.Equalf(t, payloads[i], got, "entry %d value mismatch", i)
	}
}

// TestTxmetaE2E_V2_RoundTrip exercises the validator's v2 producer end to
// end. Because the in-memory broker has no partitions the per-partition
// split is still computed (validator side) and emitted as separate Kafka
// records — verifying they all parse and write into the cache.
func TestTxmetaE2E_V2_RoundTrip(t *testing.T) {
	h := newTxmetaE2EHarness(t, "v2", 32)
	defer h.close()

	hashes, payloads := makeUniqueHashes(t, 64, 64)
	h.publishV2(t, hashes, payloads)
	h.waitProcessed(t, 64, 5*time.Second)

	for i := range hashes {
		got, ok := h.cache.GetRaw(hashes[i][:])
		require.Truef(t, ok, "entry %d not in cache", i)
		require.Equalf(t, payloads[i], got, "entry %d value mismatch", i)
	}
}

func makeUniqueHashes(t testing.TB, n, payloadSize int) ([]chainhash.Hash, [][]byte) {
	t.Helper()
	hashes := make([]chainhash.Hash, n)
	payloads := make([][]byte, n)
	for i := 0; i < n; i++ {
		// Deterministic but well-distributed (xxhash of i then expand to 32B).
		seed := xxhash.Sum64([]byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)})
		for j := 0; j < 4; j++ {
			binary8LE(hashes[i][j*8:], seed+uint64(j))
		}
		payloads[i] = make([]byte, payloadSize)
		for k := range payloads[i] {
			payloads[i][k] = byte(i + k)
		}
	}
	return hashes, payloads
}

func binary8LE(b []byte, v uint64) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
	b[4] = byte(v >> 32)
	b[5] = byte(v >> 40)
	b[6] = byte(v >> 48)
	b[7] = byte(v >> 56)
}

// ---------- Benchmarks ----------
//
// The benchmarks deliberately bypass Kafka. We don't want to measure
// in-memory broker channel buffers, franz-go publish/poll loops, or
// goroutine plumbing in the kafka package — none of that is what changes
// when v2 ships. The two pieces of code that DO change are:
//
//   1. producer-side: xxhash + partition split + v2 byte serialization
//      (mirrors validator.sendTxMetaBatchV2 / serializeTxMetaBatchV2)
//   2. receiver-side: txmetaHandler v2 parse + SetCacheMultiSequential
//
// The bench wires (1) directly to (2) via in-process function calls. One
// goroutine per partition runs the handler concurrently, matching the
// production receiver layout (one per-partition Kafka consumer goroutine).

// benchV2Item is the per-partition group element. Internal to the bench;
// declared outside serializeV2Partitioned so the pool below can refer to it.
type benchV2Item struct {
	idx int
	h   uint64
}

// benchV2Scratch holds all reusable scratch for serializeV2Partitioned across
// bench iterations. The bench owns message lifetimes via the WaitGroup at the
// call site, so it's safe to recycle the byte buffers as soon as all per-
// partition handler goroutines have finished consuming them.
type benchV2Scratch struct {
	groups [][]benchV2Item
	bufs   [][]byte
	out    [][]byte
}

var benchV2ScratchPool = sync.Pool{
	New: func() any { return &benchV2Scratch{} },
}

// serializeV2Partitioned mirrors validator.sendTxMetaBatchV2: hash, partition-
// split, and serialize to N byte slices (one per non-empty partition).
//
// The returned []byte slices and the outer slice itself live in the scratch
// struct returned alongside, so the caller must invoke recycle() once it's
// done reading those bytes. This eliminates the producer-side allocations
// that previously dominated the bench's mem profile (~50% of total).
func serializeV2Partitioned(hashes []chainhash.Hash, payloads [][]byte, numPartitions int) ([][]byte, *benchV2Scratch) {
	// Same floor as validator.sendTxMetaBatchV2 — necessary under the
	// testtxmetacache build tag where BucketsCount=8 collapses to zero
	// against the bench's default 32 partitions.
	if numPartitions > e2eBucketsCount {
		numPartitions = e2eBucketsCount
	}
	bucketsPerPartition := e2eBucketsCount / numPartitions
	if bucketsPerPartition < 1 {
		bucketsPerPartition = 1
	}

	s := benchV2ScratchPool.Get().(*benchV2Scratch)

	// Grow or reslice the outer arrays for this iter's shape.
	if cap(s.groups) < numPartitions {
		s.groups = make([][]benchV2Item, numPartitions)
	} else {
		s.groups = s.groups[:numPartitions]
	}
	if cap(s.bufs) < numPartitions {
		s.bufs = make([][]byte, numPartitions)
	} else {
		s.bufs = s.bufs[:numPartitions]
	}
	if cap(s.out) < numPartitions {
		s.out = make([][]byte, 0, numPartitions)
	} else {
		s.out = s.out[:0]
	}

	// Reset per-partition group lengths but keep their backing arrays.
	for i := range s.groups {
		s.groups[i] = s.groups[i][:0]
	}

	for i := range hashes {
		hv := xxhash.Sum64(hashes[i][:])
		bucket := int(hv % uint64(e2eBucketsCount))
		p := bucket / bucketsPerPartition
		s.groups[p] = append(s.groups[p], benchV2Item{idx: i, h: hv})
	}

	for p, g := range s.groups {
		if len(g) == 0 {
			continue
		}
		size := 8
		for _, it := range g {
			size += 8 + 32 + 1 + 4 + len(payloads[it.idx])
		}
		// Reuse this partition's prior buffer if it's already big enough.
		buf := s.bufs[p]
		if cap(buf) < size {
			buf = make([]byte, size)
		} else {
			buf = buf[:size]
		}
		s.bufs[p] = buf

		buf[0] = txmetacache.WireV2Magic
		buf[1] = txmetacache.WireV2Version
		buf[2], buf[3] = 0, 0
		putUint32LE(buf[4:], uint32(len(g)))
		off := 8
		for _, it := range g {
			binary8LE(buf[off:], it.h)
			off += 8
			copy(buf[off:], hashes[it.idx][:])
			off += 32
			buf[off] = txmetacache.WireActionADD
			off++
			putUint32LE(buf[off:], uint32(len(payloads[it.idx])))
			off += 4
			copy(buf[off:], payloads[it.idx])
			off += len(payloads[it.idx])
		}
		s.out = append(s.out, buf)
	}
	return s.out, s
}

// recycle returns the scratch to the pool. Callers MUST ensure no goroutine
// is still reading any of the byte slices when this is invoked.
func (s *benchV2Scratch) recycle() {
	benchV2ScratchPool.Put(s)
}

// benchmarkTxmetaProducerReceiver_V2 measures the end-to-end producer +
// receiver cost for one Kafka batch:
//
//	[hash + partition split + v2 serialize]
//	    → N byte slices, one per non-empty partition →
//	[N goroutines, one per partition, calling txmetaHandler concurrently]
//	    → real ImprovedCache via SetCacheMultiSequential
//
// No Kafka, no broker, no in-flight buffers. The benched bytes are exactly
// what validator.sendTxMetaBatchV2 would emit, so the parser is exercised on
// production wire bytes.
//
// Receiver fan-out matches the production design: each partition writes to a
// disjoint cache bucket range, so concurrent SetCacheMultiSequential calls
// see zero cross-partition lock contention.
func benchmarkTxmetaProducerReceiver_V2(b *testing.B, entriesPerBatch, numPartitions int) {
	// Cache size chosen to avoid chunk rotation during the bench: 32 MB
	// would give exactly 4 KB / bucket = one ChunkSize-sized chunk, so any
	// re-write of the same key triggers chunk wrap → cleanLockedMap →
	// allocate-and-rebuild a fresh 8192-shard sub-map. That allocation
	// pattern dominated the first profiling run (24% CPU in
	// NewNativeSplitLockFreeMapUint64). 256 MB gives 8 chunks / bucket so
	// the bench's repeated rewrites stay within the first chunk and
	// cleanLockedMap never fires. This still represents the production hot
	// path because real deployments run with 24 GB caches (≈ 3 MB / bucket)
	// where chunk rotation is essentially never observed.
	//
	// We share ONE cache across every bench function invocation in the
	// process: go test's benchmark calibration calls the same benchmark
	// function 3-5 times with growing b.N until benchtime is exceeded, and
	// constructing a 256 MB Native cache costs ~3s of CPU (initializes
	// 8192 × 8192 sub-maps). That 15s of setup overhead was dominating the
	// CPU profile and making the hot path invisible. Reusing the cache
	// gives steady-state numbers without the setup noise.
	rc := getSharedBenchCache(b)

	server := &Server{
		logger:    ulogger.TestLogger{},
		utxoStore: rc,
		settings: &settings.Settings{
			Validator: settings.ValidatorSettings{
				TxMetaWireFormat:    "v2",
				TxMetaNumPartitions: numPartitions,
			},
		},
	}

	hashes, payloads := makeUniqueHashes(b, entriesPerBatch, 256)
	ctx := context.Background()

	// Warm-up: serialize + parse one batch.
	{
		msgs, scratch := serializeV2Partitioned(hashes, payloads, numPartitions)
		for _, m := range msgs {
			_ = server.txmetaHandler(ctx, &kafka.KafkaMessage{Value: m})
		}
		scratch.recycle()
	}

	b.ResetTimer()
	b.ReportAllocs()

	wg := make(chan struct{}, numPartitions)
	start := time.Now()
	for i := 0; i < b.N; i++ {
		msgs, scratch := serializeV2Partitioned(hashes, payloads, numPartitions)
		for _, m := range msgs {
			m := m
			go func() {
				_ = server.txmetaHandler(ctx, &kafka.KafkaMessage{Value: m})
				wg <- struct{}{}
			}()
		}
		for range msgs {
			<-wg
		}
		// All goroutines have returned, so the underlying byte buffers are
		// safe to recycle for the next iter.
		scratch.recycle()
	}
	elapsed := time.Since(start)
	b.StopTimer()

	entries := float64(uint64(b.N) * uint64(entriesPerBatch))
	b.ReportMetric(entries/elapsed.Seconds(), "tx/s")
	b.ReportMetric(elapsed.Seconds()*1e9/entries, "ns/tx")
}

// Shared bench cache — see comment inside benchmarkTxmetaProducerReceiver_V2.
var (
	sharedBenchCacheOnce sync.Once
	sharedBenchCache     *e2eCache
)

func getSharedBenchCache(b *testing.B) *e2eCache {
	sharedBenchCacheOnce.Do(func() {
		c, err := txmetacache.New(256*1024*1024, txmetacache.Native)
		if err != nil {
			b.Fatal(err)
		}
		sharedBenchCache = &e2eCache{c: c}
	})
	return sharedBenchCache
}

func BenchmarkTxmetaProducerReceiver_V2_Batch100(b *testing.B) {
	benchmarkTxmetaProducerReceiver_V2(b, 100, 32)
}

func BenchmarkTxmetaProducerReceiver_V2_Batch1000(b *testing.B) {
	benchmarkTxmetaProducerReceiver_V2(b, 1000, 32)
}
