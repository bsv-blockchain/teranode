package txmetacache

import (
	"fmt"
	"sync"
	"unsafe"

	"github.com/cespare/xxhash"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

// 1024 buckets, 100 in a batch, 1000 in a batch, 10000 in a batch. does it make sense?
// only for bigger batches?

const maxValueSizeKB = 2
const maxValueSizeLog = 11 // 10 + log2(maxValueSizeKB)

const chunksPerAlloc = 1024

const bucketsCount = 512

const chunkSize = maxValueSizeKB * 1024

const bucketSizeBits = 40

const genSizeBits = 64 - bucketSizeBits

const maxGen = 1<<genSizeBits - 1

const maxBucketSize uint64 = 1 << bucketSizeBits

// Stats represents cache stats.
//
// Use Cache.UpdateStats for obtaining fresh stats from the cache.
type Stats struct {
	// EntriesCount is the current number of entries in the cache.
	EntriesCount uint64
}

// Reset resets s, so it may be re-used again in Cache.UpdateStats.
func (s *Stats) Reset() {
	*s = Stats{}
}

// ImprovedCache is a fast thread-safe inmemory cache optimized for big number
// of entries.
//
// It has much lower impact on GC comparing to a simple `map[string][]byte`.
//
// Use New or LoadFromFile* for creating new cache instance.
// Concurrent goroutines may call any Cache methods on the same cache instance.
//
// Call Reset when the cache is no longer needed. This reclaims the allocated
// memory.
type ImprovedCache struct {
	buckets [bucketsCount]bucket
}

// New returns new cache with the given maxBytes capacity in bytes.
//
// maxBytes must be smaller than the available RAM size for the app,
// since the cache holds data in memory.
//
// If maxBytes is less than 32MB, then the minimum cache capacity is 32MB.
func NewImprovedCache(maxBytes int) *ImprovedCache {
	if maxBytes <= 0 {
		panic(fmt.Errorf("maxBytes must be greater than 0; got %d", maxBytes))
	}
	var c ImprovedCache
	maxBucketBytes := uint64((maxBytes + bucketsCount - 1) / bucketsCount)
	for i := range c.buckets[:] {
		c.buckets[i].Init(maxBucketBytes)
	}
	return &c
}

// Set stores (k, v) in the cache.
//
// Get must be used for reading the stored entry.
//
// The stored entry may be evicted at any time either due to cache
// overflow or due to unlikely hash collision.
// Pass higher maxBytes value to New if the added items disappear
// frequently.
//
// (k, v) entries with summary size exceeding maxValueSizeKB aren't stored in the cache.
// SetBig can be used for storing entries exceeding maxValueSizeKB.
//
// k and v contents may be modified after returning from Set.
func (c *ImprovedCache) Set(k, v []byte) {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	c.buckets[idx].Set(k, v, h)
}

// lots of key value pairs at once
// decides which bucket, for lots of items
// [0..31 -b0, 32..63 -b1, 64..95 -b2, 96..127 -b3, 128..159 -b4, 160..191 -b5, 192..223 -b6, 224..255 -b7, 256..287 -b0, 288..319 -b1, 320..351 -b2, 352..383 -b3, 384..415 -b4, 416..447 -b5, 448..479 -b6, 480..511 -b7]
// Value is a byte slice per key. 4 bytes for each block ID, can have more than one block ID per key.
// a single block id is sent for all keys
func (c *ImprovedCache) SetMulti(keys []byte, value []byte, keySize int) error {
	if len(keys)%keySize != 0 {
		return fmt.Errorf("keys length must be a multiple of keySize; got %d; want %d", len(keys), keySize)
	}

	batchedKeys := make([][][]byte, bucketsCount)
	hashes := make([][]uint64, bucketsCount)

	var key []byte
	var bucketIdx uint64
	var h uint64

	// divide keys blob into buckets
	for i := 0; i < len(keys); i += keySize {
		key = keys[i : i+keySize]
		h = xxhash.Sum64(key)
		bucketIdx = h % bucketsCount
		batchedKeys[bucketIdx] = append(batchedKeys[bucketIdx], key)
		hashes[bucketIdx] = append(hashes[bucketIdx], h)
	}

	g := errgroup.Group{}

	for bucketIdx := range batchedKeys {
		if len(batchedKeys[bucketIdx]) == 0 { // there is no key for this bucket
			continue
		}
		bucketIdx := bucketIdx
		g.Go(func() error {
			return c.buckets[bucketIdx].SetMulti(batchedKeys[bucketIdx], value, hashes[bucketIdx])
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

// Get appends value by the key k to the given dst.
//
// Get allocates new byte slice for the returned value if dst is nil.
//
// Get returns only values stored in c via Set.
//
// k contents may be modified after returning from Get.
func (c *ImprovedCache) Get(dst *[]byte, k []byte) error {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount

	if !c.buckets[idx].Get(dst, k, h, true) {
		return fmt.Errorf("key %s not found in cache", k)
	}
	return nil
}

// // HasGet works identically to Get, but also returns whether the given key
// // exists in the cache. This method makes it possible to differentiate between a
// // stored nil/empty value versus and non-existing value.
func (c *ImprovedCache) HasGet(dst, k []byte) ([]byte, bool) {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	return dst, c.buckets[idx].Get(&dst, k, h, true)
}

// // Has returns true if entry for the given key k exists in the cache.
func (c *ImprovedCache) Has(k []byte) bool {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	return c.buckets[idx].Get(nil, k, h, false)
}

// Del deletes value for the given k from the cache.
//
// k contents may be modified after returning from Del.
func (c *ImprovedCache) Del(k []byte) {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	c.buckets[idx].Del(h)
}

// Reset removes all the items from the cache.
func (c *ImprovedCache) Reset() {
	for i := range c.buckets[:] {
		c.buckets[i].Reset()
	}
}

// UpdateStats adds cache stats to s.
//
// Call s.Reset before calling UpdateStats if s is re-used.
func (c *ImprovedCache) UpdateStats(s *Stats) {
	for i := range c.buckets[:] {
		c.buckets[i].UpdateStats(s)
	}
}

type bucket struct {
	mu sync.RWMutex

	// chunks is a ring buffer with encoded (k, v) pairs.
	// It consists of maxValueSizeKB chunks.
	chunks [][]byte

	// m maps hash(k) to idx of (k, v) pair in chunks.
	m map[uint64]uint64

	// idx points to chunks for writing the next (k, v) pair.
	idx uint64

	// gen is the generation of chunks.
	gen uint64
}

func (b *bucket) Init(maxBytes uint64) {
	if maxBytes == 0 {
		panic(fmt.Errorf("maxBytes cannot be zero"))
	}
	if maxBytes >= maxBucketSize {
		panic(fmt.Errorf("too big maxBytes=%d; should be smaller than %d", maxBytes, maxBucketSize))
	}
	maxChunks := (maxBytes + chunkSize - 1) / chunkSize
	b.chunks = make([][]byte, maxChunks)
	b.m = make(map[uint64]uint64)
	b.Reset()
}

func (b *bucket) Reset() {
	b.mu.Lock()
	chunks := b.chunks
	for i := range chunks {
		putChunk(chunks[i])
		chunks[i] = nil
	}
	b.m = make(map[uint64]uint64)
	b.idx = 0
	b.gen = 1
	b.mu.Unlock()
}

func (b *bucket) cleanLocked() {
	bGen := b.gen & ((1 << genSizeBits) - 1)
	bIdx := b.idx
	bm := b.m
	newItems := 0
	for _, v := range bm {
		gen := v >> bucketSizeBits
		idx := v & ((1 << bucketSizeBits) - 1)
		if (gen+1 == bGen || gen == maxGen && bGen == 1) && idx >= bIdx || gen == bGen && idx < bIdx {
			newItems++
		}
	}
	if newItems < len(bm) {
		// Re-create b.m with valid items, which weren't expired yet instead of deleting expired items from b.m.
		// This should reduce memory fragmentation and the number Go objects behind b.m.
		// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/5379
		bmNew := make(map[uint64]uint64, newItems)
		for k, v := range bm {
			gen := v >> bucketSizeBits
			idx := v & ((1 << bucketSizeBits) - 1)
			if (gen+1 == bGen || gen == maxGen && bGen == 1) && idx >= bIdx || gen == bGen && idx < bIdx {
				bmNew[k] = v
			}
		}
		b.m = bmNew
	}
}

func (b *bucket) UpdateStats(s *Stats) {
	b.mu.RLock()
	s.EntriesCount += uint64(len(b.m))
	// bytesSize := uint64(0)
	// for _, chunk := range b.chunks {
	// 	bytesSize += uint64(cap(chunk))
	// }
	//s.BytesSize += bytesSize
	//s.MaxBytesSize += uint64(len(b.chunks)) * chunkSize
	b.mu.RUnlock()
}

func (b *bucket) SetMulti(keys [][]byte, value []byte, hashes []uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var prevValue []byte

	for idx, key := range keys {
		prevValue = value
		b.Get(&prevValue, key, hashes[idx], false, true)
		b.Set(key, prevValue, hashes[idx], true)
	}

	return nil
}

func (b *bucket) Set(k, v []byte, h uint64, skipLocking ...bool) {
	if len(k) >= (1<<maxValueSizeLog) || len(v) >= (1<<maxValueSizeLog) {
		// Too big key or value - its length cannot be encoded
		// with 2 bytes (see below). Skip the entry.
		return
	}
	var kvLenBuf [4]byte
	kvLenBuf[0] = byte(uint16(len(k)) >> 8) // higher order 8 bits of key's length
	kvLenBuf[1] = byte(len(k))              // lower order 8 bits of key's length
	kvLenBuf[2] = byte(uint16(len(v)) >> 8) // higher order 8 bits of value's length
	kvLenBuf[3] = byte(len(v))              // lower order 8 bits of value's length
	kvLen := uint64(len(kvLenBuf) + len(k) + len(v))
	if kvLen >= chunkSize {
		// Do not store too big keys and values, since they do not
		// fit a chunk.
		return
	}

	chunks := b.chunks
	needClean := false

	if len(skipLocking) == 0 || !skipLocking[0] {
		b.mu.Lock()
		defer b.mu.Unlock()
	}

	idx := b.idx
	idxNew := idx + kvLen
	chunkIdx := idx / chunkSize
	chunkIdxNew := idxNew / chunkSize
	if chunkIdxNew > chunkIdx {
		if chunkIdxNew >= uint64(len(chunks)) {
			idx = 0
			idxNew = kvLen
			chunkIdx = 0
			b.gen++
			if b.gen&((1<<genSizeBits)-1) == 0 {
				b.gen++
			}
			needClean = true
		} else {
			idx = chunkIdxNew * chunkSize
			idxNew = idx + kvLen
			chunkIdx = chunkIdxNew
		}
		chunks[chunkIdx] = chunks[chunkIdx][:0]
	}
	chunk := chunks[chunkIdx]
	if chunk == nil {
		chunk = getChunk()
		chunk = chunk[:0]
	}
	chunk = append(chunk, kvLenBuf[:]...)
	chunk = append(chunk, k...)
	chunk = append(chunk, v...)
	chunks[chunkIdx] = chunk
	b.m[h] = idx | (b.gen << bucketSizeBits)
	b.idx = idxNew
	if needClean {
		b.cleanLocked()
	}
}

func (b *bucket) Get(dst *[]byte, k []byte, h uint64, returnDst bool, skipLocking ...bool) bool {
	found := false
	chunks := b.chunks
	if len(skipLocking) == 0 || !skipLocking[0] {
		b.mu.RLock()
		defer b.mu.RUnlock()
	}

	v := b.m[h]
	bGen := b.gen & ((1 << genSizeBits) - 1)
	if v > 0 {
		gen := v >> bucketSizeBits
		idx := v & ((1 << bucketSizeBits) - 1)
		if gen == bGen && idx < b.idx || gen+1 == bGen && idx >= b.idx || gen == maxGen && bGen == 1 && idx >= b.idx {
			chunkIdx := idx / chunkSize
			if chunkIdx >= uint64(len(chunks)) {
				// Corrupted data during the load from file. Just skip it.
				goto end
			}
			chunk := chunks[chunkIdx]
			idx %= chunkSize
			if idx+4 >= chunkSize {
				// Corrupted data during the load from file. Just skip it.
				goto end
			}
			kvLenBuf := chunk[idx : idx+4]
			keyLen := (uint64(kvLenBuf[0]) << 8) | uint64(kvLenBuf[1])
			valLen := (uint64(kvLenBuf[2]) << 8) | uint64(kvLenBuf[3])
			idx += 4
			if idx+keyLen+valLen >= chunkSize {
				// Corrupted data during the load from file. Just skip it.
				goto end
			}
			if string(k) == string(chunk[idx:idx+keyLen]) {
				idx += keyLen
				if returnDst {
					*dst = append(*dst, chunk[idx:idx+valLen]...)
				}
				found = true
			}
		}
	}
end:
	return found
}

func (b *bucket) Del(h uint64) {
	b.mu.Lock()
	delete(b.m, h)
	b.mu.Unlock()
}

var (
	freeChunks     []*[chunkSize]byte
	freeChunksLock sync.Mutex
)

func getChunk() []byte {
	freeChunksLock.Lock()
	if len(freeChunks) == 0 {
		// Allocate offheap memory, so GOGC won't take into account cache size.
		// This should reduce free memory waste.
		data, err := unix.Mmap(-1, 0, chunkSize*chunksPerAlloc, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
		if err != nil {
			panic(fmt.Errorf("cannot allocate %d bytes via mmap: %s", chunkSize*chunksPerAlloc, err))
		}
		for len(data) > 0 {
			p := (*[chunkSize]byte)(unsafe.Pointer(&data[0]))
			freeChunks = append(freeChunks, p)
			data = data[chunkSize:]
		}
	}
	n := len(freeChunks) - 1
	p := freeChunks[n]
	freeChunks[n] = nil
	freeChunks = freeChunks[:n]
	freeChunksLock.Unlock()
	return p[:]
}

func putChunk(chunk []byte) {
	if chunk == nil {
		return
	}
	chunk = chunk[:chunkSize]
	p := (*[chunkSize]byte)(unsafe.Pointer(&chunk[0]))

	freeChunksLock.Lock()
	freeChunks = append(freeChunks, p)
	freeChunksLock.Unlock()
}
