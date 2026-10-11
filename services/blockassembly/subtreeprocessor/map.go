package subtreeprocessor

import (
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/dolthub/swiss"
)

type SplitSwissMap struct {
	// m and mu are dense, indexed by bucket (0..nrOfBuckets-1). Slices, not
	// map[uint16]: with hundreds of millions of operations per block, the
	// per-op Go-map lookup (hash + probe of the bucket key) showed up at ~5%
	// of insert CPU in profiles; a slice index is free.
	m           []*swiss.Map[chainhash.Hash, struct{}]
	mu          []*sync.RWMutex
	nrOfBuckets uint16

	// frozen marks the map read-only: Exists skips the per-bucket RLock and
	// the write methods fail. The caller must join all writers before calling
	// Freeze — that join is the happens-before edge that makes bucket contents
	// safe for lock-free readers. Clear un-freezes for pool reuse.
	frozen atomic.Bool
}

// swissBucketHeadroomNumerator / swissBucketHeadroomDenominator give the
// per-bucket sizeHint multiplier (1.3x) we pass to swiss.NewMap.
//
// dolthub/swiss has a 7/8 = 0.875 load factor: it pre-allocates capacity for
// `sizeHint` entries and triggers a full bucket rehash once that capacity is
// exceeded. With xxhash-routed keys, per-bucket fill is not perfectly uniform —
// the heaviest bucket typically runs ~10-15 % above the mean. Without the
// headroom, that heavy bucket trips the resize threshold mid-fill and each
// resize is O(bucket_size) memcpy of the entire bucket's metadata + entry
// arrays. Profiling a 664 M-tx block movement showed swiss.Map.Put +
// runtime.memmove / memclrNoHeapPointers together consuming ~30 % of
// CreateTransactionMap's CPU — almost entirely resize work.
//
// 1.3x covers both the 0.875 load factor (≈ 1.143x worst-case fill) and ~14 %
// distribution skew on top, eliminating mid-fill resizes in steady state.
// Cost: ~30 % more peak memory for the transaction map.
const swissBucketHeadroomNumerator = 13
const swissBucketHeadroomDenominator = 10

// NewSplitSwissMap creates a new SplitSwissMap with the specified number of buckets and total length.
// The length is divided equally among the buckets.
// This map is safe for concurrent writes, but does not lock when reading.
//
// Parameters:
//   - nrOfBuckets: The number of buckets to split the map into.
//   - length: The total expected length of the map.
//
// Returns:
//   - A pointer to the newly created SplitSwissMap.
func NewSplitSwissMap(nrOfBuckets uint16, length int) *SplitSwissMap {
	m := make([]*swiss.Map[chainhash.Hash, struct{}], nrOfBuckets)
	mu := make([]*sync.RWMutex, nrOfBuckets)

	perBucketSizeHint := length / int(nrOfBuckets)
	if perBucketSizeHint > 0 {
		perBucketSizeHint = (perBucketSizeHint*swissBucketHeadroomNumerator +
			swissBucketHeadroomDenominator - 1) / swissBucketHeadroomDenominator
	}

	for i := uint16(0); i < nrOfBuckets; i++ {
		m[i] = swiss.NewMap[chainhash.Hash, struct{}](uint32(perBucketSizeHint))
		mu[i] = &sync.RWMutex{}
	}

	return &SplitSwissMap{
		m:           m,
		mu:          mu,
		nrOfBuckets: nrOfBuckets,
	}
}

// Exists reports whether hash is present in its bucket. Holds the per-bucket
// RLock for the duration of the lookup.
//
// The dolthub/swiss.Map underlying each bucket uses open-addressing with
// in-place control-byte probing — concurrent read while another goroutine is
// writing (Put or Delete) can land the reader in an inconsistent probe state,
// in the worst case spinning forever on a corrupted control sequence. Put
// and PutMultiBucket acquire the per-bucket sync.RWMutex as a write lock;
// Exists now matches with a read lock. Readers across distinct buckets do
// not serialise on each other; readers and writers within one bucket do.
func (s *SplitSwissMap) Exists(hash chainhash.Hash) bool {
	bucket := txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)

	// Frozen maps have no writers (Put/PutMultiBucket are rejected), so the
	// RLock — whose readerCount atomics dominate profiles at ~190 concurrent
	// readers — can be skipped entirely.
	if s.frozen.Load() {
		_, ok := s.m[bucket].Get(hash)
		return ok
	}

	s.mu[bucket].RLock()
	_, ok := s.m[bucket].Get(hash)
	s.mu[bucket].RUnlock()
	return ok
}

// Freeze marks the map read-only. All writers must have finished (joined)
// before this call; afterwards Exists reads lock-free and Put/PutMultiBucket
// return an error. Clear lifts the freeze for pooled reuse.
func (s *SplitSwissMap) Freeze() {
	s.frozen.Store(true)
}

func (s *SplitSwissMap) Length() int {
	var length int

	for bucket := uint16(0); bucket < s.nrOfBuckets; bucket++ {
		s.mu[bucket].Lock()
		length += s.m[bucket].Count()
		s.mu[bucket].Unlock()
	}

	return length
}

func (s *SplitSwissMap) Buckets() uint16 {
	return s.nrOfBuckets
}

func (s *SplitSwissMap) Put(hash chainhash.Hash) error {
	if s.frozen.Load() {
		return errors.NewProcessingError("SplitSwissMap is frozen, Put rejected for %s", hash.String())
	}

	bucket := txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)

	s.mu[bucket].Lock()
	defer s.mu[bucket].Unlock()

	s.m[bucket].Put(hash, struct{}{})

	return nil
}

func (s *SplitSwissMap) PutMultiBucket(bucket uint16, hashes []chainhash.Hash) error {
	if s.frozen.Load() {
		return errors.NewProcessingError("SplitSwissMap is frozen, PutMultiBucket rejected for bucket %d", bucket)
	}

	s.mu[bucket].Lock()
	defer s.mu[bucket].Unlock()

	for _, hash := range hashes {
		s.m[bucket].Put(hash, struct{}{})
	}

	return nil
}

func (s *SplitSwissMap) Iter(f func(hash chainhash.Hash, v struct{}) bool) {
	for _, swissMap := range s.m {
		swissMap.Iter(f)
	}
}

// Clear empties every bucket in place without reallocating bucket arrays or
// the per-bucket swiss.Map. The underlying swiss.Map.Clear walks its existing
// control + group arrays and zeroes them, so capacity is retained for reuse.
// Used to recycle a pooled SplitSwissMap across blocks.
//
// Each per-bucket Clear is independent (the swiss.Map and its RWMutex are
// per-bucket), so we run them in parallel: clearing a ~600 K-entry swiss.Map
// is memory-bandwidth-bound on its key/value/ctrl arrays, and with 1024
// buckets the work is naturally embarrassingly parallel.
func (s *SplitSwissMap) Clear() {
	// Lift any freeze before clearing: the pooled map is recycled across
	// blocks (fill → Freeze → read → Clear → fill ...), and the next block's
	// inserts must succeed.
	s.frozen.Store(false)

	// GOMAXPROCS(0), not NumCPU(): this parallel clear is bandwidth-bound and
	// should track runnable parallelism, matching ParallelBulkSetIfNotExists
	// below and the inserter sizing — under a cgroup CPU quota NumCPU() reports
	// host cores and oversubscribes.
	ncpu := runtime.GOMAXPROCS(0)
	if ncpu > int(s.nrOfBuckets) {
		ncpu = int(s.nrOfBuckets)
	}

	if ncpu < 1 {
		ncpu = 1
	}

	// numWorkers fits in uint16 because it is clamped to nrOfBuckets.
	numWorkers := uint16(ncpu)

	if numWorkers == 1 {
		for bucket := uint16(0); bucket < s.nrOfBuckets; bucket++ {
			s.mu[bucket].Lock()
			s.m[bucket].Clear()
			s.mu[bucket].Unlock()
		}

		return
	}

	var wg sync.WaitGroup

	wg.Add(int(numWorkers))

	for w := uint16(0); w < numWorkers; w++ {
		start := w

		go func() {
			defer wg.Done()

			for bucket := start; bucket < s.nrOfBuckets; bucket += numWorkers {
				s.mu[bucket].Lock()
				s.m[bucket].Clear()
				s.mu[bucket].Unlock()
			}
		}()
	}

	wg.Wait()
}
