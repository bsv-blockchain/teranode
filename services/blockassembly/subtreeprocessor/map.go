package subtreeprocessor

import (
	"runtime"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/dolthub/swiss"
)

type txInpointsBucket struct {
	mu sync.Mutex
	m  *swiss.Map[chainhash.Hash, *subtreepkg.TxInpoints]
}

type SplitTxInpointsMap struct {
	buckets     []txInpointsBucket
	nrOfBuckets uint16
}

func NewSplitTxInpointsMap(nrOfBuckets uint16) *SplitTxInpointsMap {
	buckets := make([]txInpointsBucket, nrOfBuckets)
	for i := uint16(0); i < nrOfBuckets; i++ {
		buckets[i].m = swiss.NewMap[chainhash.Hash, *subtreepkg.TxInpoints](64)
	}

	return &SplitTxInpointsMap{
		buckets:     buckets,
		nrOfBuckets: nrOfBuckets,
	}
}

func (s *SplitTxInpointsMap) Delete(hash chainhash.Hash) bool {
	b := &s.buckets[txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)]
	b.mu.Lock()
	ok := b.m.Has(hash)
	if ok {
		b.m.Delete(hash)
	}
	b.mu.Unlock()
	return ok
}

func (s *SplitTxInpointsMap) Exists(hash chainhash.Hash) bool {
	b := &s.buckets[txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)]
	b.mu.Lock()
	ok := b.m.Has(hash)
	b.mu.Unlock()
	return ok
}

func (s *SplitTxInpointsMap) Get(hash chainhash.Hash) (*subtreepkg.TxInpoints, bool) {
	b := &s.buckets[txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)]
	b.mu.Lock()
	v, ok := b.m.Get(hash)
	b.mu.Unlock()
	return v, ok
}

func (s *SplitTxInpointsMap) Length() int {
	length := 0
	for i := uint16(0); i < s.nrOfBuckets; i++ {
		b := &s.buckets[i]
		b.mu.Lock()
		length += b.m.Count()
		b.mu.Unlock()
	}
	return length
}

func (s *SplitTxInpointsMap) Set(hash chainhash.Hash, inpoints *subtreepkg.TxInpoints) {
	b := &s.buckets[txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)]
	b.mu.Lock()
	b.m.Put(hash, inpoints)
	b.mu.Unlock()
}

func (s *SplitTxInpointsMap) SetIfNotExists(hash chainhash.Hash, inpoints *subtreepkg.TxInpoints) (*subtreepkg.TxInpoints, bool) {
	b := &s.buckets[txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)]
	b.mu.Lock()
	if existing, ok := b.m.Get(hash); ok {
		b.mu.Unlock()
		return existing, false
	}
	b.m.Put(hash, inpoints)
	b.mu.Unlock()
	return inpoints, true
}

// Clear empties every bucket in place via swiss.Map.Clear, which zeroes the
// existing control/group arrays and retains capacity, rather than discarding
// each bucket map. This is the pool-reuse approach
// and avoids per-Clear reallocation + rehashing on the next refill, which
// matters because SplitTxInpointsMap.Clear is on the currentTxMap double-buffer
// reuse path.
func (s *SplitTxInpointsMap) Clear() {
	for i := uint16(0); i < s.nrOfBuckets; i++ {
		b := &s.buckets[i]
		b.mu.Lock()
		b.m.Clear()
		b.mu.Unlock()
	}
}

// ParallelBulkSetIfNotExists inserts multiple entries in parallel, grouped by bucket.
// Each bucket is processed by a separate goroutine with a single lock acquisition.
// wasSet[i] is set to true if hashes[i] was newly inserted (not already present).
func (s *SplitTxInpointsMap) ParallelBulkSetIfNotExists(
	hashes []chainhash.Hash,
	inpoints []*subtreepkg.TxInpoints,
	wasSet []bool,
) {
	n := len(hashes)
	if n == 0 {
		return
	}

	if len(inpoints) != n {
		panic("SplitTxInpointsMap.ParallelBulkSetIfNotExists: len(inpoints) must equal len(hashes)")
	}
	if wasSet == nil || len(wasSet) != n {
		panic("SplitTxInpointsMap.ParallelBulkSetIfNotExists: len(wasSet) must equal len(hashes)")
	}

	// Phase 1: Group indices by bucket (O(N), no locks)
	bucketIndices := make([][]int, s.nrOfBuckets)
	for i := 0; i < n; i++ {
		bucket := txmap.Bytes2Uint16Buckets(hashes[i], s.nrOfBuckets)
		bucketIndices[bucket] = append(bucketIndices[bucket], i)
	}

	// Phase 2: Process buckets with a bounded pool of stride workers. Spawning
	// one goroutine per non-empty bucket would create up to nrOfBuckets (default
	// 16*1024) goroutines for a large bulk insert, where scheduler churn and
	// memory spikes can dominate. Cap fan-out at GOMAXPROCS (matching
	// bulkBuildSubtrees); each worker strides over buckets.
	numWorkers := runtime.GOMAXPROCS(0)
	if numWorkers > int(s.nrOfBuckets) {
		numWorkers = int(s.nrOfBuckets)
	}
	if numWorkers < 1 {
		numWorkers = 1
	}

	processBucket := func(bIdx uint16) {
		indices := bucketIndices[bIdx]
		if len(indices) == 0 {
			return
		}
		b := &s.buckets[bIdx]
		b.mu.Lock()
		for _, idx := range indices {
			if !b.m.Has(hashes[idx]) {
				b.m.Put(hashes[idx], inpoints[idx])
				wasSet[idx] = true
			} else {
				// Explicitly record the existing-key result so the contract
				// (wasSet[i] reflects the outcome for hashes[i]) holds even if
				// the caller reuses a slice with stale true values.
				wasSet[idx] = false
			}
		}
		b.mu.Unlock()
	}

	if numWorkers == 1 {
		for bIdx := uint16(0); bIdx < s.nrOfBuckets; bIdx++ {
			processBucket(bIdx)
		}
		return
	}

	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for w := 0; w < numWorkers; w++ {
		start := uint16(w)
		go func() {
			defer wg.Done()
			for bIdx := start; bIdx < s.nrOfBuckets; bIdx += uint16(numWorkers) {
				processBucket(bIdx)
			}
		}()
	}
	wg.Wait()
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
// belong to the same bucket. Takes the per-bucket lock exactly once for the
// whole batch instead of once per entry.
//
// All entries MUST belong to the named bucket; callers are expected to have
// partitioned by BucketFor beforehand. The returned slice has length
// min(len(keys), len(values)); wasInserted[i] is true if keys[i] was newly
// added, false if it already existed.
func (s *SplitTxInpointsMap) PutMultiBucketTxInpoints(bucket uint16, keys []chainhash.Hash, values []*subtreepkg.TxInpoints) []bool {
	n := len(keys)
	if len(values) < n {
		n = len(values)
	}
	wasInserted := make([]bool, n)
	if n == 0 {
		return wasInserted
	}
	b := &s.buckets[bucket]
	b.mu.Lock()
	for i := 0; i < n; i++ {
		if !b.m.Has(keys[i]) {
			b.m.Put(keys[i], values[i])
			wasInserted[i] = true
		}
	}
	b.mu.Unlock()
	return wasInserted
}
