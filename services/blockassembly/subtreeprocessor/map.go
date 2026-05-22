package subtreeprocessor

import (
	"runtime"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/dolthub/swiss"
)

type SplitSwissMap struct {
	m           map[uint16]*swiss.Map[chainhash.Hash, struct{}]
	mu          map[uint16]*sync.RWMutex
	nrOfBuckets uint16
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
	m := make(map[uint16]*swiss.Map[chainhash.Hash, struct{}], nrOfBuckets)
	mu := make(map[uint16]*sync.RWMutex, nrOfBuckets)

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
	s.mu[bucket].RLock()
	_, ok := s.m[bucket].Get(hash)
	s.mu[bucket].RUnlock()
	return ok
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
	bucket := txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)

	s.mu[bucket].Lock()
	defer s.mu[bucket].Unlock()

	s.m[bucket].Put(hash, struct{}{})

	return nil
}

func (s *SplitSwissMap) PutMultiBucket(bucket uint16, hashes []chainhash.Hash) error {
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
	ncpu := runtime.NumCPU()
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

type SplitTxInpointsMap struct {
	m           map[uint16]*txmap.SyncedMap[chainhash.Hash, *subtreepkg.TxInpoints]
	nrOfBuckets uint16
}

func NewSplitTxInpointsMap(nrOfBuckets uint16) *SplitTxInpointsMap {
	m := make(map[uint16]*txmap.SyncedMap[chainhash.Hash, *subtreepkg.TxInpoints], nrOfBuckets)
	for i := uint16(0); i < nrOfBuckets; i++ {
		m[i] = txmap.NewSyncedMap[chainhash.Hash, *subtreepkg.TxInpoints]()
	}

	return &SplitTxInpointsMap{
		m:           m,
		nrOfBuckets: nrOfBuckets,
	}
}

func (s *SplitTxInpointsMap) Delete(hash chainhash.Hash) bool {
	return s.m[txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)].Delete(hash)
}

func (s *SplitTxInpointsMap) Exists(hash chainhash.Hash) bool {
	return s.m[txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)].Exists(hash)
}

func (s *SplitTxInpointsMap) Get(hash chainhash.Hash) (*subtreepkg.TxInpoints, bool) {
	return s.m[txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)].Get(hash)
}

func (s *SplitTxInpointsMap) Length() int {
	length := 0

	for _, syncedMap := range s.m {
		length += syncedMap.Length()
	}

	return length
}

func (s *SplitTxInpointsMap) Set(hash chainhash.Hash, inpoints *subtreepkg.TxInpoints) {
	s.m[txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)].Set(hash, inpoints)
}

func (s *SplitTxInpointsMap) SetIfNotExists(hash chainhash.Hash, inpoints *subtreepkg.TxInpoints) (*subtreepkg.TxInpoints, bool) {
	return s.m[txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)].SetIfNotExists(hash, inpoints)
}

func (s *SplitTxInpointsMap) Clear() {
	for _, syncedMap := range s.m {
		syncedMap.Clear()
	}
}

// Buckets returns the configured bucket count.
func (s *SplitTxInpointsMap) Buckets() uint16 {
	return s.nrOfBuckets
}

// BucketFor returns the destination bucket index for a given hash, matching the
// scheme used by Get/Set/SetIfNotExists.
func (s *SplitTxInpointsMap) BucketFor(hash chainhash.Hash) uint16 {
	return txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)
}

// PutMultiBucketTxInpoints inserts a batch of (hash, inpoints) pairs that all
// belong to the same bucket. Delegates to the underlying SyncedMap's
// SetIfNotExistsMulti, which takes the per-bucket lock exactly once for the
// whole batch instead of once per entry — eliminating the per-call
// Lock/Unlock overhead that profiling showed at ~17 % of every
// SetIfNotExists call in the hot path.
//
// All entries MUST belong to the named bucket; callers are expected to have
// partitioned by BucketFor beforehand. keys and values are walked in
// parallel up to min(len(keys), len(values)); the returned slice has that
// length. wasInserted[i] is true if keys[i] was newly added, false if it
// already existed.
//
// Designed to be called from bucket-affinity worker pools where each worker
// owns one or more buckets exclusively, so the per-bucket RWMutex is
// uncontended across worker calls.
func (s *SplitTxInpointsMap) PutMultiBucketTxInpoints(bucket uint16, keys []chainhash.Hash, values []*subtreepkg.TxInpoints) []bool {
	return s.m[bucket].SetIfNotExistsMulti(keys, values)
}
