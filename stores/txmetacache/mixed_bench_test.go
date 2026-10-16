package txmetacache

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	rtmetrics "runtime/metrics"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/cespare/xxhash/v2"
)

// The mixed benchmark drives the cache the way subtree validation does in
// production: Kafka partition writers ingesting small txmeta batches while
// subtree-validation readers resolve 1024-hash chunks, at the same time, with a
// cache small enough that the ring wraps during the run.
//
// Keys, values and xxhashes come from a pool generated once, outside the timed
// region, so the measured ingest rate is the cache's and not the generator's.
// The pool holds no pointers, so it adds nothing to GC scan work. Sequence
// number s maps to pool entry s mod mixedPoolSize, whose fee is the entry
// index: every hit is checked against it, so a torn or misattributed read shows
// up in the bad_reads metric instead of passing silently.

const (
	mixedWriterBatch = 32   // ~ entries per v2 Kafka message per partition
	mixedWriterChunk = 1024 // sequence numbers a writer claims at a time
	mixedReaderBatch = 1024 // subtreevalidation_processTxMetaUsingCache_BatchSize
	mixedReaders     = 32   // subtreevalidation_processTxMetaUsingCache_Concurrency
)

// mixedKey derives the 32-byte key for sequence number i. splitmix64 output is
// uniform, standing in for a SHA256d tx hash.
func mixedKey(i uint64) chainhash.Hash {
	var h chainhash.Hash

	x := i
	for w := 0; w < 4; w++ {
		x += 0x9e3779b97f4a7c15
		z := x
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		z ^= z >> 31
		binary.LittleEndian.PutUint64(h[w*8:], z)
	}

	return h
}

// mixedParents is the parent count for sequence number i: 1-3, weighted to the
// common single-parent case.
func mixedParents(i uint64) int {
	switch i % 8 {
	case 0, 1:
		return 2
	case 2:
		return 3
	default:
		return 1
	}
}

// appendMixedValue appends the MetaBytes encoding of sequence number i's
// metadata: fee = i, size derived from i, no flags, mixedParents(i) parents
// with one vout each.
func appendMixedValue(dst []byte, i uint64) []byte {
	n := mixedParents(i)

	dst = binary.LittleEndian.AppendUint64(dst, i)
	dst = binary.LittleEndian.AppendUint64(dst, 250+i%500)
	dst = append(dst, 0)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(n)) //nolint:gosec // 1-3

	for p := 0; p < n; p++ {
		parent := mixedKey(i ^ (uint64(p+1) << 56))
		dst = append(dst, parent[:]...)
	}

	for p := 0; p < n; p++ {
		dst = binary.LittleEndian.AppendUint32(dst, 1)
		dst = binary.LittleEndian.AppendUint32(dst, uint32(p)) //nolint:gosec // 0-2
	}

	return dst
}

// mixedPoolSize is the number of distinct keys the benchmark cycles through:
// 8M keys and ~0.7 GB of values, more than any LLC, so a large cache still
// takes DRAM and TLB misses on every lookup.
const mixedPoolSize = 1 << 23

type mixedPoolT struct {
	keys    []chainhash.Hash
	xx      []uint64
	arena   []byte
	offsets []uint32 // value i is arena[offsets[i]:offsets[i+1]]
}

var (
	mixedPoolOnce sync.Once
	mixedPoolData mixedPoolT
)

func mixedPool() *mixedPoolT {
	mixedPoolOnce.Do(func() {
		p := &mixedPoolData
		p.keys = make([]chainhash.Hash, mixedPoolSize)
		p.xx = make([]uint64, mixedPoolSize)
		p.offsets = make([]uint32, mixedPoolSize+1)
		p.arena = make([]byte, 0, mixedPoolSize*90)

		for i := uint64(0); i < mixedPoolSize; i++ {
			p.keys[i] = mixedKey(i)
			p.xx[i] = xxhash.Sum64(p.keys[i][:])
			p.arena = appendMixedValue(p.arena, i)
			p.offsets[i+1] = uint32(len(p.arena)) //nolint:gosec // < 4 GB
		}
	})

	return &mixedPoolData
}

func (p *mixedPoolT) value(i uint64) []byte {
	return p.arena[p.offsets[i]:p.offsets[i+1]:p.offsets[i+1]]
}

// mixedReadFunc resolves one reader batch. idx[j] is the pool index of
// hashes[j]; implementations count hits and hits whose content does not match.
type mixedReadFunc func(ctx context.Context, c *TxMetaCache, hashes []chainhash.Hash, idx []uint64) (hits, bad int)

// mixedReadGetMetaCached is the pre-#1957 production read loop: one
// GetMetaCached per hash.
func mixedReadGetMetaCached(ctx context.Context, c *TxMetaCache, hashes []chainhash.Hash, idx []uint64) (hits, bad int) {
	for j := range hashes {
		d, ok := c.GetMetaCached(ctx, hashes[j])
		if !ok {
			continue
		}

		hits++

		if d.Fee != idx[j] || len(d.TxInpoints.ParentTxHashes) != mixedParents(idx[j]) {
			bad++
		}
	}

	return hits, bad
}

// mixedReadMulti is the #1957 read path (GetMetaCachedMulti): the whole batch
// in one call, each bucket's read lock taken once, one *meta.Data per hit.
func mixedReadMulti(ctx context.Context, c *TxMetaCache, hashes []chainhash.Hash, idx []uint64) (hits, bad int) {
	out := make([]*meta.Data, len(hashes))
	c.GetMetaCachedMulti(ctx, hashes, nil, out)

	for j, d := range out {
		if d == nil {
			continue
		}

		hits++

		if d.Fee != idx[j] || len(d.TxInpoints.ParentTxHashes) != mixedParents(idx[j]) {
			bad++
		}
	}

	return hits, bad
}

// mixedReadInto is the zero-allocation-per-hit read path (GetMetaCachedInto).
// Like the subtree validator, the caller owns the result slices; they are
// reused per reader goroutine.
func mixedReadInto(ctx context.Context, c *TxMetaCache, hashes []chainhash.Hash, idx []uint64) (hits, bad int) {
	sc := mixedIntoPool.Get().(*mixedIntoScratch)
	defer mixedIntoPool.Put(sc)

	out, found := sc.out[:len(hashes)], sc.found[:len(hashes)]
	hits, err := c.GetMetaCachedInto(ctx, hashes, nil, out, found)
	if err != nil {
		panic(err)
	}

	for j := range out {
		if found[j] && (out[j].Fee != idx[j] || len(out[j].TxInpoints.ParentTxHashes) != mixedParents(idx[j])) {
			bad++
		}
	}

	return hits, bad
}

type mixedIntoScratch struct {
	out   []CachedMeta
	found []bool
}

var mixedIntoPool = sync.Pool{
	New: func() any {
		return &mixedIntoScratch{out: make([]CachedMeta, mixedReaderBatch), found: make([]bool, mixedReaderBatch)}
	},
}

type mixedConfig struct {
	bucket  BucketType
	sizeMB  int
	writers int
	readers int // 0: ingest-only, b.N counts ingested entries instead of reads
	read    mixedReadFunc
}

type cpuSample struct{ gc, total float64 }

func sampleCPU() cpuSample {
	s := []rtmetrics.Sample{
		{Name: "/cpu/classes/gc/total:cpu-seconds"},
		{Name: "/cpu/classes/total:cpu-seconds"},
	}
	rtmetrics.Read(s)

	return cpuSample{gc: s[0].Value.Float64(), total: s[1].Value.Float64()}
}

func runMixed(b *testing.B, cfg mixedConfig) {
	pool := mixedPool()
	c := newBenchTxMetaCache(b, cfg.sizeMB, cfg.bucket)
	ctx := context.Background()

	// About 125 B per record; readers stay inside the most recent 60% of
	// that (and inside the pool, so the keys they ask for are distinct) so
	// most reads should hit while writers keep lapping the oldest data.
	approxCapacity := uint64(cfg.sizeMB) * 1024 * 1024 / 125
	readWindow := min(approxCapacity*6/10, mixedPoolSize/2)

	// Prefill one window so readers hit from the start.
	for base := uint64(0); base < readWindow; base += 1024 {
		end := min(base+1024, readWindow)
		keys := make([][]byte, 0, end-base)
		values := make([][]byte, 0, end-base)

		for i := base; i < end; i++ {
			keys = append(keys, pool.keys[i][:])
			values = append(values, pool.value(i))
		}

		if err := c.SetCacheMultiSequential(keys, values); err != nil {
			b.Fatal(err)
		}
	}

	runtime.GC()

	var (
		stop      atomic.Bool
		reads     atomic.Int64
		hits      atomic.Int64
		bad       atomic.Int64
		ingested  atomic.Int64
		nextSeq   atomic.Uint64
		written   atomic.Uint64
		writersWG sync.WaitGroup
		readersWG sync.WaitGroup
	)

	nextSeq.Store(readWindow)
	written.Store(readWindow)

	readTarget := int64(b.N)
	ingestOnly := cfg.readers == 0

	b.ResetTimer()

	cpuStart := sampleCPU()
	start := time.Now()

	for w := 0; w < cfg.writers; w++ {
		writersWG.Add(1)

		go func() {
			defer writersWG.Done()

			// TXMETA_BENCH_WRITER_BATCH=1024 reproduces the v1 wire format
			// (the production default): one Kafka message per validator batch
			// of up to 1024 entries, written with SetCacheMultiSequential. The
			// default 32 is the v2 shape (per-partition messages, with hashes).
			batch := uint64(envInts("TXMETA_BENCH_WRITER_BATCH", []int{mixedWriterBatch})[0])
			v1 := batch != mixedWriterBatch

			keys := make([][]byte, batch)
			values := make([][]byte, batch)
			hashes := make([]uint64, batch)

			var local int64

			defer func() { ingested.Add(local) }()

			// Sequence numbers are claimed, and progress published, once per
			// chunk: per-batch shared counters bounce between dies on a large
			// machine and would cap the harness rather than the cache.
			for !stop.Load() {
				base := nextSeq.Add(mixedWriterChunk) - mixedWriterChunk
				if ingestOnly && base >= readWindow+uint64(readTarget) {
					return
				}

				for off := uint64(0); off < mixedWriterChunk; off += batch {
					for j := uint64(0); j < batch; j++ {
						i := (base + off + j) % mixedPoolSize
						keys[j] = pool.keys[i][:]
						values[j] = pool.value(i)
						hashes[j] = pool.xx[i]
					}

					var err error
					if v1 {
						err = c.SetCacheMultiSequential(keys, values)
					} else {
						err = c.SetCacheMultiSequentialWithHashes(keys, values, hashes)
					}

					if err != nil {
						b.Error(err)
						return
					}
				}

				local += mixedWriterChunk

				// Publish progress monotonically so readers only ask for
				// sequence numbers that have been written.
				for {
					cur := written.Load()
					if base+mixedWriterChunk <= cur || written.CompareAndSwap(cur, base+mixedWriterChunk) {
						break
					}
				}
			}
		}()
	}

	for r := 0; r < cfg.readers; r++ {
		readersWG.Add(1)

		go func(r int) {
			defer readersWG.Done()

			hashes := make([]chainhash.Hash, mixedReaderBatch)
			idx := make([]uint64, mixedReaderBatch)
			rng := uint64(r+1) * 0x9e3779b97f4a7c15

			for reads.Load() < readTarget {
				hi := written.Load()
				lo := hi - readWindow

				// A contiguous run of the pool, like a subtree's hash list: the
				// input is read sequentially, the cache is still hit at random
				// (consecutive keys hash to unrelated shards and buckets).
				rng ^= rng << 13
				rng ^= rng >> 7
				rng ^= rng << 17
				first := lo + rng%(readWindow-mixedReaderBatch)

				for j := range hashes {
					i := (first + uint64(j)) % mixedPoolSize
					idx[j] = i
					hashes[j] = pool.keys[i]
				}

				h, bd := cfg.read(ctx, c, hashes, idx)
				hits.Add(int64(h))
				bad.Add(int64(bd))
				reads.Add(mixedReaderBatch)
			}
		}(r)
	}

	if ingestOnly {
		writersWG.Wait()
	} else {
		readersWG.Wait()
		stop.Store(true)
		writersWG.Wait()
	}

	elapsed := time.Since(start).Seconds()
	cpuEnd := sampleCPU()

	b.StopTimer()

	totalReads := float64(reads.Load())
	b.ReportMetric(totalReads/elapsed, "reads/s")
	b.ReportMetric(float64(ingested.Load())/elapsed, "ingest/s")
	b.ReportMetric((totalReads+float64(ingested.Load()))/elapsed, "total_ops/s")

	if totalReads > 0 {
		b.ReportMetric(100*(1-float64(hits.Load())/totalReads), "miss_pct")
	}
	b.ReportMetric(float64(bad.Load()), "bad_reads")

	if d := cpuEnd.total - cpuStart.total; d > 0 {
		b.ReportMetric(100*(cpuEnd.gc-cpuStart.gc)/d, "gc_cpu_pct")
	}

	if bad.Load() != 0 {
		b.Errorf("%d reads returned data that does not belong to the requested key", bad.Load())
	}
}

// mixedSizesMB returns the cache sizes to run: TXMETA_BENCH_SIZES_MB (comma
// separated) if set, else 64 MB plus, with TXMETA_BENCH_LARGE=1, 4 GB. The
// large sizes take DRAM and TLB misses the way a 32-256 GB production cache
// does, and need several GB of RAM.
func mixedSizesMB() []int {
	def := []int{64}

	if os.Getenv("TXMETA_BENCH_LARGE") == "1" {
		def = append(def, 4096)
	}

	return envInts("TXMETA_BENCH_SIZES_MB", def)
}

// envInts parses a comma separated list of positive ints from env, or
// returns def.
func envInts(name string, def []int) []int {
	env := os.Getenv(name)
	if env == "" {
		return def
	}

	var out []int

	for _, f := range strings.Split(env, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil || n <= 0 {
			panic(fmt.Sprintf("bad %s entry %q", name, f))
		}

		out = append(out, n)
	}

	return out
}

// mixedBackends lists the rows to compare; TXMETA_BENCH_BACKENDS can restrict
// them (comma separated names).
var mixedBackends = []struct {
	name   string
	bucket BucketType
	read   mixedReadFunc
}{
	{"unallocated", Unallocated, mixedReadGetMetaCached},
	{"unallocated-multi", Unallocated, mixedReadMulti},
	{"unallocated-into", Unallocated, mixedReadInto},
	{"lockfree", Lockfree, mixedReadGetMetaCached},
	{"lockfree-into", Lockfree, mixedReadInto},
}

// BenchmarkMixedIngestRead is the throughput gate for the txmeta cache: Kafka
// writers and subtree-validation readers running together on a wrapping cache.
// ns/op is per read; reads/s, ingest/s and total_ops/s are the numbers to
// compare across backends.
func BenchmarkMixedIngestRead(b *testing.B) {
	mixedPool()

	only := os.Getenv("TXMETA_BENCH_BACKENDS")

	for _, size := range mixedSizesMB() {
		for _, writers := range envInts("TXMETA_BENCH_WRITERS", []int{8, 32}) {
			for _, readers := range envInts("TXMETA_BENCH_READERS", []int{mixedReaders}) {
				for _, be := range mixedBackends {
					if only != "" && !slices.Contains(strings.Split(only, ","), be.name) {
						continue
					}

					name := fmt.Sprintf("%s/%dMB/writers=%d/readers=%d", be.name, size, writers, readers)
					b.Run(name, func(b *testing.B) {
						runMixed(b, mixedConfig{bucket: be.bucket, sizeMB: size, writers: writers, readers: readers, read: be.read})
					})
				}
			}
		}
	}
}

// BenchmarkReadOnly measures lookups alone: 32 readers, no writers, on a
// prefilled cache. ns/op is per lookup.
func BenchmarkReadOnly(b *testing.B) {
	mixedPool()

	only := os.Getenv("TXMETA_BENCH_BACKENDS")

	for _, size := range mixedSizesMB() {
		for _, readers := range envInts("TXMETA_BENCH_READERS", []int{mixedReaders}) {
			for _, be := range mixedBackends {
				if only != "" && !slices.Contains(strings.Split(only, ","), be.name) {
					continue
				}

				b.Run(fmt.Sprintf("%s/%dMB/readers=%d", be.name, size, readers), func(b *testing.B) {
					runMixed(b, mixedConfig{bucket: be.bucket, sizeMB: size, readers: readers, read: be.read})
				})
			}
		}
	}
}

// BenchmarkIngestOnly measures Kafka-shaped ingest alone (32-entry
// SetCacheMultiSequentialWithHashes batches) on a wrapping cache. ns/op is per
// ingested entry. The read path of a backend does not matter here, so only one
// row per bucket type runs.
func BenchmarkIngestOnly(b *testing.B) {
	mixedPool()

	only := os.Getenv("TXMETA_BENCH_BACKENDS")

	for _, size := range mixedSizesMB() {
		for _, writers := range envInts("TXMETA_BENCH_WRITERS", []int{8, 32}) {
			for _, be := range mixedBackends {
				if strings.Contains(be.name, "-") || (only != "" && !slices.Contains(strings.Split(only, ","), be.name)) {
					continue
				}

				b.Run(fmt.Sprintf("%s/%dMB/writers=%d", be.name, size, writers), func(b *testing.B) {
					runMixed(b, mixedConfig{bucket: be.bucket, sizeMB: size, writers: writers})
				})
			}
		}
	}
}
