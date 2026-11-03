package txmetacache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
)

// A second Set for a key the native bucket already maps is dropped by the map
// (NativeSplitLockFreeMapUint64 cannot overwrite), so it must not spend ring
// space or count as a new entry either. Under a miss burst nearly every Kafka
// txmeta insert is such a duplicate of the Aerospike write-back.
func TestBucketNative_DuplicateSetDoesNotConsumeRing(t *testing.T) {
	cache, err := New(128*1024*1024, Native)
	require.NoError(t, err)

	key := []byte("0123456789abcdef0123456789abcdef")
	h := xxhash.Sum64(key)
	b := cache.buckets[h%BucketsCount].(*bucketNative)

	require.NoError(t, cache.Set(key, []byte("first")))

	idxAfterFirst := b.idx
	countAfterFirst := b.currentGenCount

	require.NoError(t, cache.Set(key, []byte("second-value")))

	require.Equal(t, idxAfterFirst, b.idx, "duplicate Set must not append to the ring")
	require.Equal(t, countAfterFirst, b.currentGenCount, "duplicate Set must not count as a new entry")

	var dst []byte
	require.NoError(t, cache.Get(&dst, key))
	require.Equal(t, []byte("first"), dst, "the first value stays visible, as before")
}

// recordingBackend records which batch insert BatchDecorate's write-back uses.
type recordingBackend struct {
	cacheBackend
	fanOutCalls     int
	sequentialCalls int
}

func (r *recordingBackend) SetMultiFromBytes(keys, values [][]byte) error {
	r.fanOutCalls++
	return r.cacheBackend.SetMultiFromBytes(keys, values)
}

func (r *recordingBackend) SetMultiSequential(keys, values [][]byte) error {
	r.sequentialCalls++
	return r.cacheBackend.SetMultiSequential(keys, values)
}

// The Aerospike write-back runs on the subtree-validation worker that already
// holds the batch. ImprovedCache.SetMulti's per-bucket goroutine fan-out put
// ~960 goroutines per batch into the scheduler and onto the bucket locks the
// Kafka txmeta handler needs, starving ingestion.
func TestTxMetaCache_BatchDecorateWriteBackDoesNotFanOut(t *testing.T) {
	c := newPerHashCache(t, Native)
	rec := &recordingBackend{cacheBackend: c.cache}
	c.cache = rec

	const n = 1024 // well above smallSetMultiBatchThreshold, so SetMulti would fan out

	items := make([]*utxo.UnresolvedMetaData, n)
	for i := range items {
		items[i] = &utxo.UnresolvedMetaData{Hash: hashFor(1, i), Idx: i}
	}

	require.NoError(t, c.BatchDecorate(context.Background(), items))

	require.Equal(t, 0, rec.fanOutCalls, "write-back must not use the goroutine fan-out path")
	require.Positive(t, rec.sequentialCalls)

	for i := 0; i < n; i++ {
		h := hashFor(1, i)
		d, ok := c.GetMetaCached(context.Background(), h)
		require.True(t, ok, "entry %d must be cached", i)
		require.Equal(t, feeFor(h), d.Fee)
		require.Equal(t, sizeFor(h), d.SizeInBytes)
	}

}

// laneBackend tracks, per write-back lane, how many writes are inside the cache
// at once.
type laneBackend struct {
	cacheBackend
	mu       sync.Mutex
	inFlight map[int]int
	peak     map[int]int
	calls    atomic.Int32
}

func (c *laneBackend) SetMultiSequential(keys, values [][]byte) error {
	lane := writeBackLaneOf(keys[0])
	for _, k := range keys {
		if writeBackLaneOf(k) != lane {
			panic("a write-back batch must stay inside one lane")
		}
	}

	c.calls.Add(1)

	c.mu.Lock()
	c.inFlight[lane]++
	c.peak[lane] = max(c.peak[lane], c.inFlight[lane])
	c.mu.Unlock()

	time.Sleep(time.Millisecond) // widen the window so overlapping writers would show

	err := c.cacheBackend.SetMultiSequential(keys, values)

	c.mu.Lock()
	c.inFlight[lane]--
	c.mu.Unlock()

	return err
}

// Like the Kafka txmeta partitions, each write-back lane owns a contiguous
// bucket range and has at most one writer at a time, which merges whatever the
// concurrent BatchDecorate callers queued for that lane. Thousands of
// concurrent write-backs therefore never become thousands of concurrent
// writers on the bucket locks the Kafka handler needs.
func TestTxMetaCache_BatchDecorateWriteBackOneWriterPerLane(t *testing.T) {
	c := newPerHashCache(t, Native)
	rec := &laneBackend{cacheBackend: c.cache, inFlight: map[int]int{}, peak: map[int]int{}}
	c.cache = rec

	const callers, perCaller = 64, 300

	// Without merging, every (caller, non-empty lane) pair would be its own write.
	unmerged := 0

	for g := 0; g < callers; g++ {
		lanes := map[int]struct{}{}

		for i := 0; i < perCaller; i++ {
			h := hashFor(g+10, i)
			lanes[writeBackLaneOf(h[:])] = struct{}{}
		}

		unmerged += len(lanes)
	}

	errs := make(chan error, callers)

	var wg sync.WaitGroup

	for g := 0; g < callers; g++ {
		wg.Add(1)

		go func(g int) {
			defer wg.Done()

			items := make([]*utxo.UnresolvedMetaData, perCaller)
			for i := range items {
				items[i] = &utxo.UnresolvedMetaData{Hash: hashFor(g+10, i), Idx: i}
			}

			errs <- c.BatchDecorate(context.Background(), items)

			// Populated by the time BatchDecorate returns, whoever wrote it.
			for i := range items {
				if _, ok := c.GetMetaCached(context.Background(), hashFor(g+10, i)); !ok {
					errs <- errors.NewProcessingError("entry not cached when BatchDecorate returned")
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	require.NotEmpty(t, rec.peak)

	for lane, p := range rec.peak {
		require.Equal(t, 1, p, "lane %d had concurrent writers", lane)
	}

	// Concurrent callers' entries are merged into shared per-lane writes.
	require.Less(t, int(rec.calls.Load()), unmerged)
}

// gatedBackend blocks each of the first two writes until released.
type gatedBackend struct {
	cacheBackend
	entered [2]chan struct{}
	release [2]chan struct{}
	calls   atomic.Int32
}

func newGatedBackend(inner cacheBackend) *gatedBackend {
	g := &gatedBackend{cacheBackend: inner}
	for i := range g.entered {
		g.entered[i] = make(chan struct{})
		g.release[i] = make(chan struct{})
	}

	return g
}

func (g *gatedBackend) SetMultiSequential(keys, values [][]byte) error {
	if n := int(g.calls.Add(1)); n <= len(g.entered) {
		close(g.entered[n-1])
		<-g.release[n-1]
	}

	return g.cacheBackend.SetMultiSequential(keys, values)
}

// A writer writes one merged batch and hands the lane over: it must not keep
// writing other callers' entries while they keep arriving, or one subtree
// worker stalls for as long as the burst lasts.
func TestTxMetaCache_WriteBackWriterHandsOffAfterOneBatch(t *testing.T) {
	c := newPerHashCache(t, Native)
	gate := newGatedBackend(c.cache)
	c.cache = gate

	// Keys that all fall into lane 0.
	var laneKeys [][]byte

	for i := 0; len(laneKeys) < 4; i++ {
		h := hashFor(99, i)
		if writeBackLaneOf(h[:]) == 0 {
			laneKeys = append(laneKeys, h.CloneBytes())
		}
	}

	value := []byte("value")

	firstDone := make(chan error, 1)

	go func() { firstDone <- c.writeBack(laneKeys[:1], [][]byte{value}) }()

	<-gate.entered[0] // the first caller is writing, alone

	othersDone := make(chan error, 3)

	for _, k := range laneKeys[1:] {
		go func(k []byte) { othersDone <- c.writeBack([][]byte{k}, [][]byte{value}) }(k)
	}

	require.Eventually(t, func() bool {
		l := &c.lanes[0]
		l.mu.Lock()
		defer l.mu.Unlock()

		return len(l.pending) == 3
	}, 5*time.Second, time.Millisecond)

	close(gate.release[0])
	<-gate.entered[1] // the next batch, the three queued meanwhile, is being written

	// ...but not by the first caller, which returns while it is still blocked.
	select {
	case err := <-firstDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("first writer kept writing other callers' batch instead of handing the lane over")
	}

	close(gate.release[1])

	for range 3 {
		require.NoError(t, <-othersDone)
	}

	require.Equal(t, int32(2), gate.calls.Load())
}

// panicOnceBackend panics on its first write.
type panicOnceBackend struct {
	cacheBackend
	calls atomic.Int32
}

func (p *panicOnceBackend) SetMultiSequential(keys, values [][]byte) error {
	if p.calls.Add(1) == 1 {
		panic("boom")
	}

	return p.cacheBackend.SetMultiSequential(keys, values)
}

// A panic while writing must not leave the lane marked as writing: every later
// caller hashing into it would block forever.
func TestTxMetaCache_WriteBackPanicReleasesLane(t *testing.T) {
	c := newPerHashCache(t, Native)
	c.cache = &panicOnceBackend{cacheBackend: c.cache}

	key := hashFor(77, 0)
	value := []byte("value")

	require.Error(t, c.writeBack([][]byte{key.CloneBytes()}, [][]byte{value}))

	done := make(chan error, 1)
	go func() { done <- c.writeBack([][]byte{key.CloneBytes()}, [][]byte{value}) }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("lane stayed wedged after a panicking write")
	}
}
