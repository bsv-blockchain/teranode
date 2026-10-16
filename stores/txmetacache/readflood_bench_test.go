package txmetacache

import (
	"crypto/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
)

// BenchmarkKafkaIngestUnderReadFlood measures the Kafka txmeta handler's insert
// cost (ns/op = wall time per 31-entry Kafka call) while subtree validation
// probes the cache, in the two shapes the probe has had:
//
//   - perkey: 6,144 goroutines (192 subtrees x 32 workers) each calling Get
//     once per key — every lookup takes its bucket's read lock.
//   - ranged: 32 workers shared by all subtrees, each probing one bucket range
//     of a subtree with GetMulti — one read lock per bucket per range.
//
// Mreads/s reports the probe throughput achieved alongside, so a faster writer
// is not bought with a starved reader.
//
//	go test -run '^$' -bench KafkaIngestUnderReadFlood -benchtime 200000x ./stores/txmetacache/
func BenchmarkKafkaIngestUnderReadFlood(b *testing.B) {
	const (
		hot          = 4_000_000 // keys already ingested, which the probes hit
		subtreeKeys  = 1 << 20
		kafkaWorkers = 128
		kafkaBatch   = 31
	)

	variants := []struct {
		name    string
		readers int
		probe   func(cache *ImprovedCache, keys [][]byte, scratch *[]byte, spans [][2]int) int
	}{
		{"probe=none", 0, nil},
		{"probe=perkey", 192 * 32, perKeyProbe},
		{"probe=ranged", 32, rangedProbe},
		// Decompositions: the shared worker limit without batching, and
		// batching with a higher limit.
		{"probe=perkey32", 32, perKeyProbe},
		{"probe=ranged192", 192, rangedProbe},
	}

	for _, v := range variants {
		b.Run(v.name, func(b *testing.B) {
			cache, err := New(2*1024*1024*1024, Native)
			require.NoError(b, err)

			hotKeys := make([][]byte, hot)
			arena := make([]byte, hot*32)
			_, err = rand.Read(arena)
			require.NoError(b, err)

			value := make([]byte, 120)

			for i := range hotKeys {
				hotKeys[i] = arena[i*32 : (i+1)*32]
				require.NoError(b, cache.Set(hotKeys[i], value))
			}

			// One subtree's keys grouped by probe range, as processTxMetaUsingCache does.
			perRange := make([][][]byte, ProbeRanges)
			for i := 0; i < subtreeKeys; i++ {
				k := hotKeys[i%hot]
				r := int(xxhash.Sum64(k) % BucketsCount / (BucketsCount / ProbeRanges))
				perRange[r] = append(perRange[r], k)
			}

			poolSize := b.N * kafkaBatch
			keys := make([][]byte, poolSize)
			values := make([][]byte, poolSize)
			hashes := make([]uint64, poolSize)
			keyArena := make([]byte, poolSize*32)
			_, err = rand.Read(keyArena)
			require.NoError(b, err)

			for i := range keys {
				keys[i] = keyArena[i*32 : (i+1)*32]
				values[i] = value
				hashes[i] = xxhash.Sum64(keys[i])
			}

			var (
				done   atomic.Bool
				reads  atomic.Int64
				rangeN atomic.Int64
				rwg    sync.WaitGroup
			)

			for r := 0; r < v.readers; r++ {
				rwg.Add(1)

				go func() {
					defer rwg.Done()

					var (
						scratch []byte
						spans   = make([][2]int, subtreeKeys)
						n       int64
					)

					for !done.Load() {
						group := perRange[int(rangeN.Add(1))%ProbeRanges]
						n += int64(v.probe(cache, group, &scratch, spans))
					}

					reads.Add(n)
				}()
			}

			var (
				cursor atomic.Int64
				wg     sync.WaitGroup
			)

			start := time.Now()

			b.ResetTimer()

			for w := 0; w < kafkaWorkers; w++ {
				wg.Add(1)

				go func() {
					defer wg.Done()

					for {
						s := int(cursor.Add(kafkaBatch)) - kafkaBatch
						if s >= poolSize {
							return
						}

						e := min(s+kafkaBatch, poolSize)
						_ = cache.SetMultiSequentialWithHashes(keys[s:e], values[s:e], hashes[s:e])
					}
				}()
			}

			wg.Wait()
			b.StopTimer()

			elapsed := time.Since(start)

			done.Store(true)
			rwg.Wait()

			b.ReportMetric(float64(reads.Load())/elapsed.Seconds()/1e6, "Mreads/s")
		})
	}
}

func perKeyProbe(cache *ImprovedCache, keys [][]byte, scratch *[]byte, _ [][2]int) int {
	for _, k := range keys {
		*scratch = (*scratch)[:0]
		_ = cache.Get(scratch, k)
	}

	return len(keys)
}

func rangedProbe(cache *ImprovedCache, keys [][]byte, scratch *[]byte, spans [][2]int) int {
	*scratch = (*scratch)[:0]
	cache.GetMulti(keys, nil, scratch, spans[:len(keys)])

	return len(keys)
}
