package txmetacache

import (
	"fmt"
	"math"
	"sync"
	"unsafe"

	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/types"
	"github.com/cespare/xxhash"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

// Example calculation for the improved cache:
// cache size : 2 * 2 *  1024 -> 4 KB
// number of buckets: 4
// bucket size: 4 KB / 4 = 1 KB
// chunk size: 2 * 128 = 256 bytes
// number of total chunks: 4 KB / 256 bytes = 16 chunks
// number of chunks per bucket: 16 / 4 = 4 chunks

// 600 Million keys per block * 5 blocks = 3 Billion keys
// 3 billion keys
// 16 * 1024 buckets = 16,384 buckets, 256 GB / 16,384 buckets = 16 MB per bucket
// 1024 chunks per bucket = 16 MB / 1024 chunks = 16 KB per chunk
// (16 * 1024) bucket * 1024 chunks per bucket = 16,777,216 chunks
// 3 billion / 16,777,216 = 178 keys per chunk
// 178 * 68 bytes = 194,156 bytes per chunk -> 194 KB
// For 128 GB, 8 * 1024 buckets, 1.5 Billion keys.

const maxValueSizeKB = 2 // 2KB

const maxValueSizeLog = 11 // 10 + log2(maxValueSizeKB)

const bucketsCount = 8 * 1024

const chunkSize = maxValueSizeKB * 2 * 1024 // 4 KB

const bucketSizeBits = 40

const genSizeBits = 64 - bucketSizeBits

const maxGen = 1<<genSizeBits - 1

const maxBucketSize uint64 = 1 << bucketSizeBits

const chunksPerAlloc = 1024

// Stats represents cache stats.
//
// Use Cache.UpdateStats for obtaining fresh stats from the cache.
type Stats struct {
	// EntriesCount is the current number of entries in the cache.
	EntriesCount       uint64
	TrimCount          uint64
	TotalMapSize       uint64
	TotalElementsAdded uint64
}

// Reset resets s, so it may be re-used again in Cache.UpdateStats.
func (s *Stats) Reset() {
	*s = Stats{}
}

// create a bucket interface, which will be implemented by the bucket and bucketUnallcated
type bucketInterface interface {
	Init(maxBytes uint64, trimRatio int)
	Reset()
	Set(k, v []byte, h uint64, skipLocking ...bool) error
	SetMultiKeysSingleValue(keys [][]byte, value []byte)
	SetMulti(keys [][]byte, values [][]byte)
	Get(*[]byte, []byte, uint64, bool, ...bool) bool
	Del(k uint64)
	UpdateStats(s *Stats)
	listChunks()
	getMapSize() uint64
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
	buckets   [bucketsCount]bucketInterface
	trimRatio int
}

// NewImprovedCache returns new cache with the given maxBytes capacity in bytes.
// trimRatioSetting is the percentage of the chunks to be removed when the chunks are full, default is 25%.
func NewImprovedCache(maxBytes int, bucketType types.BucketType) *ImprovedCache {
	if maxBytes <= 0 {
		panic(fmt.Errorf("maxBytes must be greater than 0; got %d", maxBytes))
	}
	var c ImprovedCache

	maxBucketBytes := uint64(maxBytes / bucketsCount)

	if maxBucketBytes < chunkSize {
		maxBucketBytes = chunkSize * 8
	}

	trimRatio, _ := gocore.Config().GetInt("txMetaCacheTrimRatio", 2)

	// if the cache is unallocated cache, unallocatedCache is false, minedBlockStore
	if bucketType == types.Unallocated {
		for i := 0; i < bucketsCount; i++ {
			c.buckets[i] = &bucketUnallocated{}
			c.buckets[i].Init(maxBucketBytes, 0)
		}
	} else if bucketType == types.Preallocated {
		c.trimRatio = trimRatio

		for i := 0; i < bucketsCount; i++ {
			c.buckets[i] = &bucketPreallocated{}
			c.buckets[i].Init(maxBucketBytes, c.trimRatio)
		}
	} else { // trimmed cache
		for i := 0; i < bucketsCount; i++ {
			c.buckets[i] = &bucketTrimmed{}
			c.buckets[i].Init(maxBucketBytes, 0)
		}
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
func (c *ImprovedCache) Set(k, v []byte) error {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	return c.buckets[idx].Set(k, v, h)
}

// SetMultiKeysSingleValueAppended stores multiple (k, v) entries in the cache, for the same v. New v is appended to the existing v, doesn't overwrite.
//
// Logic: decides which bucket, for lots of items. All keys are distributed to buckets. All buckets are populated via goroutines.
// Keys: a single byte slice containing many appended keys.
// - key to bucket: [0..31 -b0, 32..63 -b1, 64..95 -b2, 96..127 -b3, 128..159 -b4, 160..191 -b5, 192..223 -b6, 224..255 -b7, 256..287 -b0, 288..319 -b1, 320..351 -b2, 352..383 -b3, 384..415 -b4, 416..447 -b5, 448..479 -b6, 480..511 -b7]
// Value: single block id is sent for all keys. 4 bytes for each block ID, there can be more than one block ID per key.
// Value bytes are appended to the end of the previous value bytes.
func (c *ImprovedCache) SetMultiKeysSingleValueAppended(keys []byte, value []byte, keySize int) error {
	if len(keys)%keySize != 0 {
		return fmt.Errorf("keys length must be a multiple of keySize; got %d; want %d", len(keys), keySize)
	}

	batchedKeys := make([][][]byte, bucketsCount)

	var key []byte
	var bucketIdx uint64
	var h uint64

	// divide keys blob into buckets
	for i := 0; i < len(keys); i += keySize {
		key = keys[i : i+keySize]
		h = xxhash.Sum64(key)
		bucketIdx = h % bucketsCount
		batchedKeys[bucketIdx] = append(batchedKeys[bucketIdx], key)
	}

	g := errgroup.Group{}

	for bucketIdx := range batchedKeys {
		// if there is no key for this bucket
		if len(batchedKeys[bucketIdx]) == 0 {
			continue
		}
		bucketIdx := bucketIdx
		g.Go(func() error {
			c.buckets[bucketIdx].SetMultiKeysSingleValue(batchedKeys[bucketIdx], value) //, hashes[bucketIdx])
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

// SetMulti stores multiple (k, v) entries in the cache, for different values.
func (c *ImprovedCache) SetMulti(keys [][]byte, values [][]byte) error {
	batchedKeys := make([][][]byte, bucketsCount)
	batchedValues := make([][][]byte, bucketsCount)

	var bucketIdx uint64
	var h uint64

	// divide keys into buckets
	for i, key := range keys {
		h = xxhash.Sum64(key)
		bucketIdx = h % bucketsCount
		batchedKeys[bucketIdx] = append(batchedKeys[bucketIdx], key)
		batchedValues[bucketIdx] = append(batchedValues[bucketIdx], values[i])
	}

	g := errgroup.Group{}

	// for every bucket run a goroutine to populate it
	for bucketIdx := range batchedKeys {

		// if there is no key for this bucket
		if len(batchedKeys[bucketIdx]) == 0 {
			continue
		}
		bucketIdx := bucketIdx
		g.Go(func() error {
			c.buckets[bucketIdx].SetMulti(batchedKeys[bucketIdx], batchedValues[bucketIdx])
			return nil
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
		return fmt.Errorf("key %v not found in cache", k)
	}
	return nil
}

// Has returns true if entry for the given key k exists in the cache.
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

type bucketTrimmed struct {
	mu sync.RWMutex

	// chunks is a ring buffer with encoded (k, v) pairs.
	// It consists of maxValueSizeKB chunks.
	chunks [][]byte

	// m maps hash(k) to idx of (k, v) pair in chunks.
	// m map[uint64]uint64
	m *util.SplitSwissMapKVUint64
	// pass txId directly. How is memory?
	// m map[[32]byte]uint64

	// idx points to chunks for writing the next (k, v) pair.
	idx uint64

	// gen is the generation of chunks.
	gen uint64

	// free chunks per bucket.
	freeChunks []*[chunkSize]byte

	elementsAdded int

	// number of items in the bucket.
	numberOfItems int

	overWriting bool
}

func (b *bucketTrimmed) Init(maxBytes uint64, _ int) {
	if maxBytes == 0 {
		panic(fmt.Errorf("maxBytes cannot be zero"))
	}
	if maxBytes >= maxBucketSize {
		panic(fmt.Errorf("too big maxBytes=%d; should be smaller than %d", maxBytes, maxBucketSize))
	}
	maxChunks := (maxBytes + chunkSize - 1) / chunkSize
	b.chunks = make([][]byte, maxChunks)
	b.m = util.NewSplitSwissMapKVUint64(1024)
	b.overWriting = false
	b.Reset()
}

func (b *bucketTrimmed) Reset() {
	b.mu.Lock()
	chunks := b.chunks
	for i := range chunks {
		b.putChunk(chunks[i])
		chunks[i] = nil
	}
	b.m = util.NewSplitSwissMapKVUint64(1024)
	b.idx = 0
	b.gen = 1
	b.overWriting = false
	b.mu.Unlock()
}

// cleanLockedMap removes expired k-v pairs from bucket map.
func (b *bucketTrimmed) cleanLockedMap() {
	bGen := b.gen & ((1 << genSizeBits) - 1)
	bIdx := b.idx
	bm := b.m
	newItems := 0

	for _, maps := range bm.Map() {
		maps.Map().Iter(func(k uint64, v uint64) (stop bool) {
			gen := v >> bucketSizeBits
			idx := v & ((1 << bucketSizeBits) - 1)
			if (gen+1 == bGen || (gen == maxGen && bGen == 1) && idx >= bIdx) || (gen == bGen && idx < bIdx) {
				newItems++
			}
			return false // Continue iteration over all elements
		})
	}
	if newItems < bm.Length() {
		// Re-create b.m with valid items, which weren't expired yet instead of deleting expired items from b.m.
		// This should reduce memory fragmentation and the number Go objects behind b.m.
		// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/5379
		bmNew := util.NewSplitSwissMapKVUint64(1024)
		for _, maps := range bm.Map() {
			maps.Map().Iter(func(k uint64, v uint64) (stop bool) {
				gen := v >> bucketSizeBits
				idx := v & ((1 << bucketSizeBits) - 1)
				if (gen+1 == bGen || gen == maxGen && bGen == 1) && idx >= bIdx || gen == bGen && idx < bIdx {
					// TODO: consider handling error
					_ = bmNew.Put(k, v)
				}
				return false
			})
		}
		b.m = bmNew
	}
}

func (b *bucketTrimmed) UpdateStats(s *Stats) {
	b.mu.RLock()
	s.EntriesCount += uint64(b.numberOfItems)
	s.TotalElementsAdded += uint64(b.elementsAdded)
	s.TotalMapSize += b.getMapSize()
	b.mu.RUnlock()
}

func (b *bucketTrimmed) listChunks() {
	fmt.Println("chunks: ", b.chunks)
}

func (b *bucketTrimmed) SetMulti(keys [][]byte, values [][]byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var hash uint64
	var prevValue []byte

	for i, key := range keys {
		prevValue = values[i]
		hash = xxhash.Sum64(key)
		//b.Get(&prevValue, key, hash, true, true)
		// TODO: consider logging if set is not successful. But this should only happen when the key-value size is too big.
		_ = b.Set(key, prevValue, hash, true)
	}
}

// Set skips locking if skipLocking is set to true. Locking should be only skipped when the caller holds the lock, i.e. when called from SetMulti.
func (b *bucketTrimmed) Set(k, v []byte, h uint64, skipLocking ...bool) error {
	if len(k) >= (1<<maxValueSizeLog) || len(v) >= (1<<maxValueSizeLog) {
		// Too big key or value - its length cannot be encoded
		// with 2 bytes (see below). Skip the entry.
		return fmt.Errorf("too big key or value")
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
		return fmt.Errorf("key, value, and k-v length bytes doesn't fit to a chunk")
	}
	chunks := b.chunks
	needClean := false

	if len(skipLocking) == 0 || !skipLocking[0] {
		b.mu.Lock()
		defer b.mu.Unlock()
	}

	// calculate the idx of the k-v pair to be added
	// adjust idxNew, calculate where the new k-v pair will end
	// the new k-v pair must be in the same chunk.
	idx := b.idx
	idxNew := idx + kvLen
	chunkIdx := idx / chunkSize
	chunkIdxNew := idxNew / chunkSize

	// check if we are crossing the chunk boundary, we need to allocate a new chunk
	if chunkIdxNew > chunkIdx {
		// if there are no more chunks to allocate, we need to reset the bucket
		if chunkIdxNew >= uint64(len(chunks)) {
			// writing needs to start over from the beginning.
			idx = 0
			idxNew = kvLen
			// the chunk index is set to 0
			chunkIdx = 0
			// the generation of the bucket is incremented
			b.gen++
			if b.gen&((1<<genSizeBits)-1) == 0 {
				b.gen++
			}
			b.overWriting = true
			needClean = true
		} else { // if the new item doesn't overflow the chunks, we need to allocate a new chunk
			// calculate the index as byte offset
			idx = chunkIdxNew * chunkSize
			idxNew = idx + kvLen
			chunkIdx = chunkIdxNew
		}
		chunks[chunkIdx] = chunks[chunkIdx][:0]
	}
	chunk := chunks[chunkIdx]

	if chunk == nil {
		chunk = b.getChunk()
		chunk = chunk[:0]
	}

	chunk = append(chunk, kvLenBuf[:]...)
	chunk = append(chunk, k...)
	chunk = append(chunk, v...)
	chunks[chunkIdx] = chunk

	// TODO: consider handling error
	_ = b.m.Put(h, idx|(b.gen<<bucketSizeBits))
	b.idx = idxNew
	if needClean {
		b.cleanLockedMap()
	}

	b.elementsAdded++
	if !b.overWriting {
		b.numberOfItems++
	}
	return nil
}

// Get skips locking if skipLocking is set to true. Locking should be only skipped when the caller holds the lock, i.e. when called from SetMulti.
func (b *bucketTrimmed) Get(dst *[]byte, k []byte, h uint64, returnDst bool, skipLocking ...bool) bool {
	found := false
	chunks := b.chunks
	if len(skipLocking) == 0 || !skipLocking[0] {
		b.mu.RLock()
		defer b.mu.RUnlock()
	}

	v, foundInMap := b.m.Get(h)
	if !foundInMap {
		return found
	}

	bGen := b.gen & ((1 << genSizeBits) - 1)
	if v > 0 {
		// shift the v to the right by 40 bits, to get the generation
		gen := v >> bucketSizeBits
		// mask lower 40 bits 00000..40bitsSetTo1...., extract the index
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

// SetMultiKeysSingleValue stores multiple (k, v) entries for the same bucket for a single v. Appends v to the exsiting v value, doesn't overwrite.
func (b *bucketTrimmed) SetMultiKeysSingleValue(keys [][]byte, value []byte) { //, hashes []uint64) { //error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var prevValue []byte
	var hash uint64

	for _, key := range keys {
		prevValue = value
		hash = xxhash.Sum64(key)
		b.Get(&prevValue, key, hash, true, true)
		// TODO: consider logging if set is not successful. But this should only happen when the key-value size is too big.
		_ = b.Set(key, prevValue, hash, true)
	}
}

func (b *bucketTrimmed) getChunk() []byte {
	if len(b.freeChunks) == 0 {
		// Allocate offheap memory, so GOGC won't take into account cache size.
		// This should reduce free memory waste.
		data, err := unix.Mmap(-1, 0, chunkSize*chunksPerAlloc, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
		if err != nil {
			panic(fmt.Errorf("cannot allocate %d bytes via mmap: %s", chunkSize*chunksPerAlloc, err))
		}
		for len(data) > 0 {
			p := (*[chunkSize]byte)(unsafe.Pointer(&data[0]))
			b.freeChunks = append(b.freeChunks, p)
			data = data[chunkSize:]
		}
	}
	n := len(b.freeChunks) - 1
	p := b.freeChunks[n]
	b.freeChunks[n] = nil
	b.freeChunks = b.freeChunks[:n]
	return p[:]
}

func (b *bucketTrimmed) putChunk(chunk []byte) {
	if chunk == nil {
		return
	}
	chunk = chunk[:chunkSize]
	p := (*[chunkSize]byte)(unsafe.Pointer(&chunk[0]))

	b.freeChunks = append(b.freeChunks, p)
}

func (b *bucketTrimmed) Del(h uint64) {
	b.mu.Lock()
	delete(b.m.Map(), h)
	b.mu.Unlock()
}

func (b *bucketTrimmed) getMapSize() uint64 {
	return uint64(b.m.Length())
}

// bucketPreallocated is a bucket with preallocated memory for chunks.
// it allocates memory at the beginning, and then uses it for the entire lifetime of the bucket.
// it als operforms trimming
type bucketPreallocated struct {
	mu sync.RWMutex

	// chunks is a ring buffer with encoded (k, v) pairs.
	// It consists of maxValueSizeKB chunks.
	chunks [][]byte

	// m maps hash(k) to idx of (k, v) pair in chunks.
	m map[uint64]uint64
	// TODO: maybe pass txId directly. Check how does it affect memory.
	// m map[[32]byte]uint64

	// idx points to chunks for writing the next (k, v) pair.
	idx uint64

	// gen is the generation of chunks.
	gen uint64

	trimRatio int

	trimCount uint64
}

func (b *bucketPreallocated) Init(maxBytes uint64, trimRatio int) {
	if maxBytes == 0 {
		panic(fmt.Errorf("maxBytes cannot be zero"))
	}
	if maxBytes >= maxBucketSize {
		panic(fmt.Errorf("too big maxBytes=%d; should be smaller than %d", maxBytes, maxBucketSize))
	}

	// allocate memory for all chunks of the bucket
	data, err := unix.Mmap(-1, 0, int(maxBytes), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
	if err != nil {
		panic(fmt.Errorf("cannot allocate %d bytes via mmap: %s", maxBytes, err))
	}
	for len(data) > 0 {
		p := (*[chunkSize]byte)(unsafe.Pointer(&data[0]))
		b.chunks = append(b.chunks, p[:])
		data = data[chunkSize:]
	}

	b.m = make(map[uint64]uint64)
	b.trimRatio = trimRatio
	b.Reset()
}

func (b *bucketPreallocated) Reset() {
	b.mu.Lock()
	b.m = make(map[uint64]uint64)
	b.idx = 0
	b.gen = 1
	b.mu.Unlock()
}

func (b *bucketPreallocated) getMapSize() uint64 {
	return uint64(len(b.m))
}

// cleanLockedMap removes expired k-v pairs from bucket map.
func (b *bucketPreallocated) cleanLockedMap(startingOffset int) {
	bmSize := len(b.m)
	bmNew := make(map[uint64]uint64, bmSize)

	for k, v := range b.m {
		idx := v & ((1 << bucketSizeBits) - 1)

		// adjust the idx for each item, since we removed the first half of the chunks/
		// we only take items in the second half of the chunks, i.e. after the byteOffsetRemoved
		if int(idx) >= startingOffset {
			// calcualte the adjusted index. We move old indexes of the items to the left by byteOffsetRemoved
			adjustedIdx := idx - uint64(startingOffset)
			bmNew[k] = adjustedIdx | (b.gen << bucketSizeBits)
		}
	}
	b.m = bmNew
	b.trimCount++
}

func (b *bucketPreallocated) UpdateStats(s *Stats) {
	b.mu.RLock()
	s.EntriesCount += uint64(len(b.m))
	s.TrimCount = b.trimCount
	s.TotalMapSize += b.getMapSize()
	b.mu.RUnlock()
}

func (b *bucketPreallocated) listChunks() {
	fmt.Println("chunks: ", b.chunks)
}

// SetMulti stores multiple (k, v) entries with 1-1 mapping for the same bucket. OWERWRITES.
func (b *bucketPreallocated) SetMulti(keys [][]byte, values [][]byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var hash uint64

	for i, key := range keys {
		hash = xxhash.Sum64(key)
		// TODO: consider logging if set is not successful. But this should only happen when the key-value size is too big.
		_ = b.Set(key, values[i], hash, true)
	}
}

func (b *bucketPreallocated) SetMultiKeysSingleValue(keys [][]byte, value []byte) { //, hashes []uint64) { //error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var hash uint64

	for _, key := range keys {
		hash = xxhash.Sum64(key)
		// TODO: consider logging if set is not successful. But this should only happen when the key-value size is too big.
		_ = b.Set(key, value, hash, true)
	}
}

// SetNew skips locking if skipLocking is set to true. Locking should be only skipped when the caller holds the lock, i.e. when called from SetMulti.
// removes only half of the each chunk when the chunk is full
func (b *bucketPreallocated) Set(k, v []byte, h uint64, skipLocking ...bool) error {
	if len(k) >= (1<<maxValueSizeLog) || len(v) >= (1<<maxValueSizeLog) {
		// Too big key or value - its length cannot be encoded
		// with 2 bytes (see below). Skip the entry.
		return fmt.Errorf("too big key or value")
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
		return fmt.Errorf("key, value, and k-v length bytes doesn't fit to a chunk")
	}
	chunks := b.chunks

	if len(skipLocking) == 0 || !skipLocking[0] {
		b.mu.Lock()
		defer b.mu.Unlock()
	}

	// calculate the idx of the k-v pair to be added
	// adjust idxNew, calcualte where the new k-v pair will end
	// the new k-v pair must be in the same chunk.
	idx := b.idx
	idxNew := idx + kvLen
	chunkIdx := idx / chunkSize
	chunkIdxNew := idxNew / chunkSize

	// check if we are crossing the chunk boundary, we need to allocate a new chunk
	if chunkIdxNew > chunkIdx {
		// if there are no more chunks to allocate, we need to reset the bucket
		if chunkIdxNew >= uint64(len(chunks)) {
			numOfChunksToRemove := int(math.Ceil(float64(len(chunks)*b.trimRatio) / float64(100)))
			numOfChunksToKeep := len(chunks) - numOfChunksToRemove

			// Shift the more recent half of the chunks to the start of the array
			for i := 0; i < numOfChunksToKeep; i++ {
				chunks[i] = chunks[i+numOfChunksToRemove]
			}

			// Clear the rest of the chunks
			for i := numOfChunksToKeep; i < len(chunks); i++ {
				chunks[i] = make([]byte, chunkSize)
			}

			// writing needs to start form the end of the kept chunks.
			idx = chunkSize * uint64(numOfChunksToKeep)
			idxNew = idx + kvLen
			// calculate the where the next write should occur based on new index
			chunkIdx = idx / chunkSize

			b.cleanLockedMap(numOfChunksToRemove * chunkSize)

		} else { // if the new item doesn't overflow the chunks, we need to allocate a new chunk
			// calculate the index as byte offset
			idx = chunkIdxNew * chunkSize
			idxNew = idx + kvLen
			chunkIdx = chunkIdxNew
		}
	}

	chunk := chunks[chunkIdx]
	if len(chunk) == 0 {
		panic(fmt.Errorf("SHOULD NEVER ENTER HERE, chunk is nil or empty"))
	}
	data := append(append(kvLenBuf[:], k...), v...)
	copy(chunk[idx%chunkSize:], data)

	chunks[chunkIdx] = chunk
	b.m[h] = idx | (b.gen << bucketSizeBits)
	b.idx = idxNew
	return nil
}

// TODO: Simplfy by removing references to generations, generations are not relevant for bucket.
// Get skips locking if skipLocking is set to true. Locking should be only skipped when the caller holds the lock, i.e. when called from SetMulti.
func (b *bucketPreallocated) Get(dst *[]byte, k []byte, h uint64, returnDst bool, skipLocking ...bool) bool {
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

func (b *bucketPreallocated) Del(h uint64) {
	b.mu.Lock()
	delete(b.m, h)
	b.mu.Unlock()
}

type bucketUnallocated struct {
	mu sync.RWMutex

	// chunks is a ring buffer with encoded (k, v) pairs.
	// It consists of maxValueSizeKB chunks.
	chunks [][]byte

	// m maps hash(k) to idx of (k, v) pair in chunks.
	m map[uint64]uint64
	// pass txId directly. How is memory?
	// m map[[32]byte]uint64

	// idx points to chunks for writing the next (k, v) pair.
	idx uint64

	// gen is the generation of chunks.
	gen uint64

	// free chunks per bucket
	freeChunks []*[chunkSize]byte
}

func (b *bucketUnallocated) Init(maxBytes uint64, _ int) {
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

func (b *bucketUnallocated) Reset() {
	b.mu.Lock()
	chunks := b.chunks
	for i := range chunks {
		b.putChunk(chunks[i])
		chunks[i] = nil
	}
	b.m = make(map[uint64]uint64)
	b.idx = 0
	b.gen = 1
	b.mu.Unlock()
}

// cleanLockedMap removes expired k-v pairs from bucket map.
func (b *bucketUnallocated) cleanLockedMap() {

	bGen := b.gen & ((1 << genSizeBits) - 1)
	bIdx := b.idx
	bm := b.m
	//bmSize := len(b.m)
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

func (b *bucketUnallocated) UpdateStats(s *Stats) {
	b.mu.RLock()
	s.EntriesCount += uint64(len(b.m))
	s.TotalMapSize += b.getMapSize()
	b.mu.RUnlock()
}

func (b *bucketUnallocated) listChunks() {
	fmt.Println("chunks: ", b.chunks)
}

func (b *bucketUnallocated) SetMulti(keys [][]byte, values [][]byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var hash uint64
	var prevValue []byte

	for i, key := range keys {
		prevValue = values[i]
		hash = xxhash.Sum64(key)
		b.Get(&prevValue, key, hash, true, true)
		// TODO: consider logging if set is not successful. But this should only happen when the key-value size is too big.
		_ = b.Set(key, prevValue, hash, true)
	}
}

// Set skips locking if skipLocking is set to true. Locking should be only skipped when the caller holds the lock, i.e. when called from SetMulti.
func (b *bucketUnallocated) Set(k, v []byte, h uint64, skipLocking ...bool) error {
	if len(k) >= (1<<maxValueSizeLog) || len(v) >= (1<<maxValueSizeLog) {
		// Too big key or value - its length cannot be encoded
		// with 2 bytes (see below). Skip the entry.
		return fmt.Errorf("too big key or value")
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
		return fmt.Errorf("key, value, and k-v length bytes doesn't fit to a chunk")
	}

	chunks := b.chunks
	needClean := false

	if len(skipLocking) == 0 || !skipLocking[0] {
		b.mu.Lock()
		defer b.mu.Unlock()
	}

	// calculate the idx of the k-v pair to be added
	// adjust idxNew, calcualte where the new k-v pair will end
	// the new k-v pair must be in the same chunk.
	idx := b.idx
	idxNew := idx + kvLen
	chunkIdx := idx / chunkSize
	chunkIdxNew := idxNew / chunkSize
	// check if we are crossing the chunk boundary, we need to allocate a new chunk
	if chunkIdxNew > chunkIdx {
		// if there are no more chunks to allocate, we need to reset the bucket
		if chunkIdxNew >= uint64(len(chunks)) {
			// writing needs to start over from the beginning.
			idx = 0
			idxNew = kvLen
			// the chunk index is set to 0
			chunkIdx = 0
			// the generation of the bucket is incremented
			b.gen++
			if b.gen&((1<<genSizeBits)-1) == 0 {
				b.gen++
			}
			needClean = true
		} else { // if the new item doesn't overflow the chunks, we need to allocate a new chunk
			// calculate the index as byte offset
			idx = chunkIdxNew * chunkSize
			idxNew = idx + kvLen
			chunkIdx = chunkIdxNew
		}
		chunks[chunkIdx] = chunks[chunkIdx][:0]
	}
	chunk := chunks[chunkIdx]
	if chunk == nil {
		chunk = b.getChunk()
		chunk = chunk[:0]
	}
	chunk = append(chunk, kvLenBuf[:]...)
	chunk = append(chunk, k...)
	chunk = append(chunk, v...)
	chunks[chunkIdx] = chunk
	b.m[h] = idx | (b.gen << bucketSizeBits)
	b.idx = idxNew
	if needClean {
		b.cleanLockedMap()
	}
	return nil
}

// Get skips locking if skipLocking is set to true. Locking should be only skipped when the caller holds the lock, i.e. when called from SetMulti.
func (b *bucketUnallocated) Get(dst *[]byte, k []byte, h uint64, returnDst bool, skipLocking ...bool) bool {
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

// SetMultiKeysSingleValue stores multiple (k, v) entries for the same bucket for a single v. Appends v to the exsiting v value, doesn't overwrite.
func (b *bucketUnallocated) SetMultiKeysSingleValue(keys [][]byte, value []byte) { //, hashes []uint64) { //error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var prevValue []byte
	var hash uint64

	for _, key := range keys {
		prevValue = value
		hash = xxhash.Sum64(key)
		b.Get(&prevValue, key, hash, true, true)
		// TODO: consider logging if set is not successful. But this should only happen when the key-value size is too big.
		_ = b.Set(key, prevValue, hash, true)
	}
}

func (b *bucketUnallocated) getChunk() []byte {
	if len(b.freeChunks) == 0 {
		// Allocate offheap memory, so GOGC won't take into account cache size.
		// This should reduce free memory waste.
		data, err := unix.Mmap(-1, 0, chunkSize*chunksPerAlloc, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
		if err != nil {
			panic(fmt.Errorf("cannot allocate %d bytes via mmap: %s", chunkSize*chunksPerAlloc, err))
		}
		for len(data) > 0 {
			p := (*[chunkSize]byte)(unsafe.Pointer(&data[0]))
			b.freeChunks = append(b.freeChunks, p)
			data = data[chunkSize:]
		}
	}
	n := len(b.freeChunks) - 1
	p := b.freeChunks[n]
	b.freeChunks[n] = nil
	b.freeChunks = b.freeChunks[:n]
	return p[:]
}

func (b *bucketUnallocated) putChunk(chunk []byte) {
	if chunk == nil {
		return
	}
	chunk = chunk[:chunkSize]
	p := (*[chunkSize]byte)(unsafe.Pointer(&chunk[0]))

	b.freeChunks = append(b.freeChunks, p)
}

func (b *bucketUnallocated) Del(h uint64) {
	b.mu.Lock()
	delete(b.m, h)
	b.mu.Unlock()
}

func (b *bucketUnallocated) getMapSize() uint64 {
	return uint64(len(b.m))
}
