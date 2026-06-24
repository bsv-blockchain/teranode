package subtreeprocessor

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"net/url"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"golang.org/x/sync/errgroup"
)

// These benchmarks reproduce, at reduced scale, the two phases of foreign-block
// moveForwardBlock that profiling showed plateau at ~15M map-ops/s on a
// 192-core node (~27 cores busy):
//
//   - Phase A (CreateTransactionMap): many concurrent per-subtree goroutines,
//     each spawning one goroutine per non-empty bucket, all contending on the
//     1024 bucket mutexes of a shared SplitSwissMap.
//   - Phase B (processRemainderTxHashes): NumCPU readers probing the same map
//     via Exists, each taking the bucket RWMutex read lock although the map is
//     read-only for the whole phase.
//
// Baselines are recorded before the contention fix and compared after; see the
// PR description for the numbers.

// contentionBenchBuckets matches the bucket count used by the production
// txMapPool (NewSplitSwissMap(1024, ...) in CreateTransactionMap).
const contentionBenchBuckets = 1024

// benchConcurrentSubtreeReads mirrors the blockassembly_subtreeProcessorConcurrentReads
// default that bounds the per-subtree goroutines in CreateTransactionMap.
const benchConcurrentSubtreeReads = 375

// genBenchHashes deterministically generates n pseudo-random 32-byte hashes.
// PCG is ~50x faster than hashing real data and the SplitSwissMap only cares
// about key distribution, which PCG output matches.
func genBenchHashes(n int, seed uint64) []chainhash.Hash {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	hashes := make([]chainhash.Hash, n)

	for i := range hashes {
		binary.LittleEndian.PutUint64(hashes[i][0:8], rng.Uint64())
		binary.LittleEndian.PutUint64(hashes[i][8:16], rng.Uint64())
		binary.LittleEndian.PutUint64(hashes[i][16:24], rng.Uint64())
		binary.LittleEndian.PutUint64(hashes[i][24:32], rng.Uint64())
	}

	return hashes
}

// bucketizeBySubtree splits hashes into numSubtrees groups, each pre-bucketed
// per Bytes2Uint16Buckets — the shape DeserializeHashesFromReaderIntoBuckets
// hands to the insert stage in CreateTransactionMap.
func bucketizeBySubtree(hashes []chainhash.Hash, numSubtrees int) []map[uint16][]chainhash.Hash {
	perSubtree := len(hashes) / numSubtrees
	out := make([]map[uint16][]chainhash.Hash, numSubtrees)

	for s := range out {
		buckets := make(map[uint16][]chainhash.Hash, contentionBenchBuckets)
		for _, h := range hashes[s*perSubtree : (s+1)*perSubtree] {
			bucket := txmap.Bytes2Uint16Buckets(h, contentionBenchBuckets)
			buckets[bucket] = append(buckets[bucket], h)
		}

		out[s] = buckets
	}

	return out
}

// fillMapUncontended prefills m with hashes using one goroutine per bucket
// (setup helper, not the measured path).
func fillMapUncontended(b *testing.B, m *SplitSwissMap, hashes []chainhash.Hash) {
	b.Helper()

	grouped := make(map[uint16][]chainhash.Hash, contentionBenchBuckets)
	for _, h := range hashes {
		bucket := txmap.Bytes2Uint16Buckets(h, contentionBenchBuckets)
		grouped[bucket] = append(grouped[bucket], h)
	}

	g := errgroup.Group{}
	for bucket, bucketHashes := range grouped {
		g.Go(func() error {
			return m.PutMultiBucket(bucket, bucketHashes)
		})
	}

	if err := g.Wait(); err != nil {
		b.Fatal(err)
	}
}

// benchmarkPhaseAInsert replicates the CreateTransactionMap insert stage as it
// exists today: per-subtree goroutines (bounded like SubtreeProcessorConcurrentReads)
// each spawning one PutMultiBucket goroutine per non-empty bucket of a shared map.
func benchmarkPhaseAInsert(b *testing.B, totalTx, numSubtrees int) {
	subtreeBuckets := bucketizeBySubtree(genBenchHashes(totalTx, 1), numSubtrees)
	m := NewSplitSwissMap(contentionBenchBuckets, totalTx)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m.Clear()

		g := errgroup.Group{}
		g.SetLimit(benchConcurrentSubtreeReads)

		for _, buckets := range subtreeBuckets {
			g.Go(func() error {
				bucketG := errgroup.Group{}
				for bucket, hashes := range buckets {
					bucketG.Go(func() error {
						return m.PutMultiBucket(bucket, hashes)
					})
				}

				return bucketG.Wait()
			})
		}

		if err := g.Wait(); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(totalTx)*float64(b.N)/b.Elapsed().Seconds(), "tx/s")

	if got := m.Length(); got != totalTx {
		b.Fatalf("map length %d, want %d", got, totalTx)
	}
}

// phaseAInsertCases are shared by the current-shape and owners-shape insert
// benchmarks so the two are directly comparable.
var phaseAInsertCases = []struct {
	totalTx     int
	numSubtrees int
	large       bool
}{
	{4_000_000, 64, false},
	{16_000_000, 64, true},
	{16_000_000, 256, true},
	{40_000_000, 256, true},
}

func BenchmarkPhaseAInsertCurrent(b *testing.B) {
	for _, c := range phaseAInsertCases {
		b.Run(fmt.Sprintf("tx=%dM/subtrees=%d", c.totalTx/1_000_000, c.numSubtrees), func(b *testing.B) {
			if c.large && testing.Short() {
				b.Skip("skipping large insert benchmark in short mode")
			}

			benchmarkPhaseAInsert(b, c.totalTx, c.numSubtrees)
		})
	}
}

// benchmarkPhaseAInsertOwners drives the same input through the bucketInserter:
// per-subtree submitter goroutines, one exclusive writer per bucket stripe.
func benchmarkPhaseAInsertOwners(b *testing.B, totalTx, numSubtrees int) {
	subtreeBuckets := bucketizeBySubtree(genBenchHashes(totalTx, 1), numSubtrees)
	m := NewSplitSwissMap(contentionBenchBuckets, totalTx)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m.Clear()

		ins := newBucketInserter(m, runtime.GOMAXPROCS(0))

		g := errgroup.Group{}
		g.SetLimit(benchConcurrentSubtreeReads)

		for _, buckets := range subtreeBuckets {
			g.Go(func() error {
				return ins.submit(buckets)
			})
		}

		if err := g.Wait(); err != nil {
			b.Fatal(err)
		}

		if err := ins.closeAndWait(); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(totalTx)*float64(b.N)/b.Elapsed().Seconds(), "tx/s")

	if got := m.Length(); got != totalTx {
		b.Fatalf("map length %d, want %d", got, totalTx)
	}
}

func BenchmarkPhaseAInsertOwners(b *testing.B) {
	for _, c := range phaseAInsertCases {
		b.Run(fmt.Sprintf("tx=%dM/subtrees=%d", c.totalTx/1_000_000, c.numSubtrees), func(b *testing.B) {
			if c.large && testing.Short() {
				b.Skip("skipping large insert benchmark in short mode")
			}

			benchmarkPhaseAInsertOwners(b, c.totalTx, c.numSubtrees)
		})
	}
}

// benchmarkPhaseBExists replicates the processRemainderTxHashes membership scan:
// all CPUs probing a fully-built (read-only) map, ~50% hit ratio. With frozen
// set, the map is frozen first so Exists takes the lock-free path.
func benchmarkPhaseBExists(b *testing.B, totalTx int, frozen bool) {
	hashes := genBenchHashes(totalTx, 1)
	m := NewSplitSwissMap(contentionBenchBuckets, totalTx)
	fillMapUncontended(b, m, hashes)

	if frozen {
		m.Freeze()
	}

	// Probe set: half present, half absent, deterministically shuffled.
	probes := make([]chainhash.Hash, 0, totalTx)
	probes = append(probes, hashes[:totalTx/2]...)
	probes = append(probes, genBenchHashes(totalTx/2, 2)...)

	rng := rand.New(rand.NewPCG(3, 3))
	rng.Shuffle(len(probes), func(i, j int) {
		probes[i], probes[j] = probes[j], probes[i]
	})

	var start atomic.Uint64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Spread goroutines across the probe slice so they don't all walk the
		// same cache lines in lockstep.
		local := start.Add(0x9e3779b9)
		n := uint64(len(probes))

		for pb.Next() {
			_ = m.Exists(probes[local%n])
			local++
		}
	})

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")
}

func BenchmarkPhaseBExists(b *testing.B) {
	for _, totalTx := range []int{4_000_000, 16_000_000} {
		b.Run(fmt.Sprintf("tx=%dM", totalTx/1_000_000), func(b *testing.B) {
			if totalTx > 4_000_000 && testing.Short() {
				b.Skip("skipping large Exists benchmark in short mode")
			}

			benchmarkPhaseBExists(b, totalTx, false)
		})
	}
}

func BenchmarkPhaseBExistsFrozen(b *testing.B) {
	for _, totalTx := range []int{4_000_000, 16_000_000} {
		b.Run(fmt.Sprintf("tx=%dM", totalTx/1_000_000), func(b *testing.B) {
			if totalTx > 4_000_000 && testing.Short() {
				b.Skip("skipping large Exists benchmark in short mode")
			}

			benchmarkPhaseBExists(b, totalTx, true)
		})
	}
}

// BenchmarkCreateTransactionMapE2E drives the real CreateTransactionMap
// (subtree files in a memory blob store → deserialize → insert) at CI scale,
// pooled-map reuse across iterations as in production.
func BenchmarkCreateTransactionMapE2E(b *testing.B) {
	const (
		numSubtrees   = 16
		txsPerSubtree = 16384
	)

	ctx := context.Background()

	newSubtreeChan := make(chan NewSubtreeRequest, 10)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	defer close(newSubtreeChan)

	subtreeStore := blob_memory.New()
	tSettings := createBenchmarkSettings(txsPerSubtree)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	if err != nil {
		b.Fatal(err)
	}

	utxoStore, err := sql.New(ctx, ulogger.TestLogger{}, tSettings, utxoStoreURL)
	if err != nil {
		b.Fatal(err)
	}

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, tSettings, subtreeStore, nil, utxoStore, newSubtreeChan)
	if err != nil {
		b.Fatal(err)
	}

	stp.SetCurrentItemsPerFile(txsPerSubtree)

	blockSubtreesMap := make(map[chainhash.Hash]int, numSubtrees)

	for s := 0; s < numSubtrees; s++ {
		subtree, err := subtreepkg.NewTreeByLeafCount(txsPerSubtree)
		if err != nil {
			b.Fatal(err)
		}

		if s == 0 {
			if err = subtree.AddCoinbaseNode(); err != nil {
				b.Fatal(err)
			}
		}

		for i := 0; i < txsPerSubtree-1; i++ {
			txHash := chainhash.HashH([]byte(fmt.Sprintf("tx-%d-%d", s, i)))
			if err = subtree.AddNode(txHash, uint64(s*txsPerSubtree+i+1), 100); err != nil {
				b.Fatal(err)
			}
		}

		subtreeBytes, err := subtree.Serialize()
		if err != nil {
			b.Fatal(err)
		}

		if err = subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes,
			options.WithDeleteAt(tSettings.GlobalBlockHeightRetention)); err != nil {
			b.Fatal(err)
		}

		blockSubtreesMap[*subtree.RootHash()] = s
	}

	totalTx := numSubtrees * txsPerSubtree

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		transactionMap, _, err := stp.CreateTransactionMap(ctx, blockSubtreesMap, numSubtrees, uint64(totalTx))
		if err != nil {
			b.Fatal(err)
		}

		if transactionMap == nil {
			b.Fatal("nil transaction map")
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(totalTx)*float64(b.N)/b.Elapsed().Seconds(), "tx/s")
}
