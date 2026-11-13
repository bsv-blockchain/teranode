package txmetacache

import (
	"crypto/rand"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
)

// BenchmarkKafkaIngestUnderWriteBack measures the Kafka txmeta handler's insert
// cost while BatchDecorate write-backs run concurrently, the shape seen on the
// subtree validator during a miss burst. Kafka workers and write-back workers
// draw from one key pool, so part of the Kafka inserts are duplicates of keys
// the write-back already stored (and the other way around).
//
// ns/op (wall time per Kafka call) is the number to compare across the
// writeback variants; kafka-ns/entry adds per-worker latency including lock waits.
//
//	go test -run '^$' -bench KafkaIngestUnderWriteBack -benchtime 200000x ./stores/txmetacache/
func BenchmarkKafkaIngestUnderWriteBack(b *testing.B) {
	const (
		kafkaWorkers   = 128
		kafkaBatch     = 31
		writeBackBatch = 1024
	)

	// 48 is one subtree's processTxMetaUsingStore concurrency; 2048 is the order
	// of concurrent write-backs seen on a collapsing subtree validator (192
	// subtrees in flight, each with its own store workers).
	for _, writeBackWorkers := range []int{48, 2048} {
		benchKafkaIngestUnderWriteBack(b, kafkaWorkers, kafkaBatch, writeBackWorkers, writeBackBatch)
	}
}

func benchKafkaIngestUnderWriteBack(b *testing.B, kafkaWorkers, kafkaBatch, writeBackWorkers, writeBackBatch int) {
	b.Helper()

	variants := []struct {
		name  string
		write func(c *ImprovedCache, keys, values [][]byte) error
	}{
		{"writeback=none", nil},
		{"writeback=fanout", (*ImprovedCache).SetMulti},
		{"writeback=sequential", (*ImprovedCache).SetMultiSequential},
		{"writeback=lanes", nil},
	}

	for _, v := range variants {
		b.Run(fmt.Sprintf("writers=%d/%s", writeBackWorkers, v.name), func(b *testing.B) {
			cache, err := New(2*1024*1024*1024, Native)
			require.NoError(b, err)

			write := v.write
			if v.name == "writeback=lanes" {
				tmc := &TxMetaCache{cache: &improvedCacheBackend{cache: cache}}
				write = func(_ *ImprovedCache, keys, values [][]byte) error { return tmc.writeBack(keys, values) }
			}

			poolSize := b.N * kafkaBatch
			keys := make([][]byte, poolSize)
			values := make([][]byte, poolSize)
			hashes := make([]uint64, poolSize)
			keyArena := make([]byte, poolSize*32)
			_, err = rand.Read(keyArena)
			require.NoError(b, err)

			value := make([]byte, 120)

			for i := range keys {
				keys[i] = keyArena[i*32 : (i+1)*32]
				values[i] = value
				hashes[i] = xxhash.Sum64(keys[i])
			}

			var (
				kafkaCursor atomic.Int64
				wbCursor    atomic.Int64
				kafkaDone   atomic.Bool
				kafkaNanos  atomic.Int64
				wg          sync.WaitGroup
				wbWg        sync.WaitGroup
			)

			b.ResetTimer()

			if write != nil {
				for w := 0; w < writeBackWorkers; w++ {
					wbWg.Add(1)

					go func() {
						defer wbWg.Done()

						for !kafkaDone.Load() {
							start := int(wbCursor.Add(int64(writeBackBatch))) - writeBackBatch
							if start >= poolSize {
								wbCursor.Store(0) // keep the pressure on for as long as Kafka runs
								continue
							}

							end := min(start+writeBackBatch, poolSize)
							_ = write(cache, keys[start:end], values[start:end])
						}
					}()
				}
			}

			for w := 0; w < kafkaWorkers; w++ {
				wg.Add(1)

				go func() {
					defer wg.Done()

					for {
						start := int(kafkaCursor.Add(int64(kafkaBatch))) - kafkaBatch
						if start >= poolSize {
							return
						}

						end := min(start+kafkaBatch, poolSize)
						t0 := time.Now()
						_ = cache.SetMultiSequentialWithHashes(keys[start:end], values[start:end], hashes[start:end])
						kafkaNanos.Add(int64(time.Since(t0)))
					}
				}()
			}

			wg.Wait()
			b.StopTimer()
			kafkaDone.Store(true)
			wbWg.Wait()

			// Average wall time a Kafka worker spends per entry, including lock waits.
			b.ReportMetric(float64(kafkaNanos.Load())/float64(poolSize), "kafka-ns/entry")
		})
	}
}
