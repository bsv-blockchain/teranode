// Package txmetacache provides a high-performance caching layer for transaction metadata.
// This file implements the ImprovedCache - a memory-efficient, highly concurrent cache
// specifically optimized for blockchain transaction metadata storage with minimal GC pressure.
package txmetacache

import (
	"fmt"
	"math"
	"sync"
	"unsafe"

	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/cespare/xxhash"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

// ImprovedCache Design Calculations and Memory Model:
//
// The cache is designed to efficiently handle billions of transaction entries with
// minimal memory overhead and garbage collection impact. The design is based on the
// following principles:
//
// 1. Divide the cache into multiple buckets to reduce lock contention
// 2. Use fixed-size memory chunks to minimize GC pressure
// 3. Apply hash-based distribution of entries across buckets
// 4. Implement different allocation strategies based on use cases
//
// Memory usage calculations for various configurations:
// ----------------------------------------------------
// Example for small cache (test environment):
// - Cache size: 2 * 2 * 1024 = 4 KB
// - Number of buckets: 4
// - Bucket size: 4 KB / 4 = 1 KB
// - Chunk size: 2 * 128 = 256 bytes
// - Total chunks: 4 KB / 256 bytes = 16 chunks
// - Chunks per bucket: 16 / 4 = 4 chunks
//
// Example for production cache with 3 billion keys:
// - Keys: 600 Million keys per block * 5 blocks = 3 Billion keys
// - Buckets: 16 * 1024 = 16,384 buckets
// - Bucket size: 256 GB / 16,384 buckets = 16 MB per bucket
// - Chunks per bucket: 1024 chunks = 16 MB / 1024 chunks = 16 KB per chunk
// - Total chunks: (16 * 1024) buckets * 1024 chunks per bucket = 16,777,216 chunks
// - Keys per chunk: 3 billion / 16,777,216 = 178 keys per chunk
// - Memory per chunk: 178 * 68 bytes = 194,156 bytes (~194 KB)
//
// For 128 GB configuration: 8 * 1024 buckets, ~1.5 Billion keys capacity
// Cache configuration constants and memory management parameters

// maxValueSizeKB defines the maximum size of a cached value in kilobytes
const maxValueSizeKB = 2 // 2KB

// maxValueSizeLog is the log2 representation of maxValueSizeKB (with offset)
// Used for faster size calculations with bitwise operations
const maxValueSizeLog = 11 // 10 + log2(maxValueSizeKB)

// bucketSizeBits defines how many bits are used for bucket size representation
// This affects the maximum possible size of a bucket
const bucketSizeBits = 40

// genSizeBits defines how many bits are used for generation tracking
// Generation tracking helps manage entry eviction policies
const genSizeBits = 64 - bucketSizeBits

// maxGen is the maximum generation value before wrapping around
const maxGen = 1<<genSizeBits - 1

// maxBucketSize defines the maximum number of entries a bucket can hold
const maxBucketSize uint64 = 1 << bucketSizeBits

// chunksPerAlloc is a hard upper bound on the number of chunks to allocate in a single
// mmap slab for on-demand bucket types.
//
// The *effective* per-bucket slab cap is computed dynamically from the configured cache
// size (txMetaCacheMaxMB -> maxBytes -> maxBucketBytes) in order to:
// - avoid wasting off-heap memory in small-cache deployments
// - avoid excessive mmap/syscall overhead in large-cache deployments
//
// See calcMaxSlabChunks() below.
const chunksPerAlloc = 1024

const (
	minSlabChunks       = 8                         // 8 * 4KB = 32KB minimum slab
	maxSlabBytesPerMmap = 4 * 1024 * 1024           // 4MB target maximum slab size
	minSlabBytesPerMmap = minSlabChunks * ChunkSize // keep in sync with minSlabChunks
)

func calcMaxSlabChunks(maxBucketBytes uint64, maxChunks uint64) uint64 {
	// Target slab size:
	// - at most 4MB to avoid huge single mmaps per bucket
	// - about 1/16 of the bucket size, so small buckets don't keep large idle freeChunks
	// - but at least 32KB to avoid too many tiny mmaps when filling quickly. for example when txmetaCacheMaxMB is 1 GB.
	targetBytes := maxBucketBytes / 16
	if targetBytes < minSlabBytesPerMmap {
		targetBytes = minSlabBytesPerMmap
	}

	if targetBytes > maxSlabBytesPerMmap {
		targetBytes = maxSlabBytesPerMmap
	}

	// Convert to chunk count (floor), then clamp.
	ch := targetBytes / ChunkSize
	if ch < minSlabChunks {
		ch = minSlabChunks
	}

	if ch > uint64(chunksPerAlloc) {
		ch = uint64(chunksPerAlloc)
	}

	if maxChunks > 0 && ch > maxChunks {
		ch = maxChunks
	}

	return ch
}

// calcInitialAllocChunks determines the optimal initial allocation size for a bucket
// based on its maximum slab size and total capacity. For small buckets, it allocates
// the full size upfront to minimize syscall overhead. For large buckets, it starts at
// 25% capacity to enable exponential growth and memory efficiency for lightly-used buckets.
func calcInitialAllocChunks(maxSlabChunks uint64, maxChunks uint64) uint64 {
	// If the bucket's total capacity is small (≤128KB), allocate everything upfront
	// regardless of maxSlabChunks limit. This avoids multiple mmaps for tiny buckets.
	if maxChunks <= 32 {
		// Small bucket: allocate full capacity upfront.
		// For a 1GB cache with 8192 buckets, each bucket gets 32 chunks (128KB).
		// Better to do 1 mmap of 32 chunks than 4 mmaps of 8 chunks each.
		return maxChunks
	}

	// For larger buckets, respect the per-mmap slab limit but start smart.
	if maxSlabChunks <= 32 {
		// If the slab limit itself is small, allocate full slab upfront.
		return maxSlabChunks
	}

	// Large buckets: start at 25% of slab size or minimum 8 chunks.
	// This enables exponential growth (8→16→32→...) for memory efficiency
	// while ensuring reasonable initial allocation.
	initial := maxSlabChunks / 4
	if initial < minSlabChunks {
		initial = minSlabChunks
	}

	return initial
}

// -------------------------------------------------------------------
// Cache size configuration constants
//
// The cache uses different configurations based on build tags, allowing for
// flexibility in deployment across various environments with different memory
// requirements. At build time, only one of these configurations will be used:
//
// 1. Large cache (production): See improved_cache_const_large.go
// 2. Small cache (development): See improved_cache_const_small.go
// 3. Test cache: See improved_cache_const_test.go
// -------------------------------------------------------------------
// Configuration options for different cache sizes (only one set will be used)
const bucketCountLarge = 8 * 1024                //nolint:unused
const bucketCountSmall = 32                      //nolint:unused
const bucketCountTest = 8                        //nolint:unused
const chunkSizeLarge = maxValueSizeKB * 2 * 1024 //nolint:unused
const chunkSizeSmall = maxValueSizeKB * 512      //nolint:unused
const chunkSizeTest = maxValueSizeKB * 2 * 1024  //nolint:unused
// -------------------------------------------------------------------

// Stats represents cache statistics for monitoring and diagnostics.
//
// This structure provides visibility into the cache's current state,
// including entry count, eviction metrics, and memory utilization.
// Use ImprovedCache.UpdateStats method to obtain the most current statistics.
type Stats struct {
	// EntriesCount is the current number of entries in the cache.
	EntriesCount       uint64 // Current number of entries stored in the cache
	TrimCount          uint64 // Number of trim operations performed on the cache
	TotalMapSize       uint64 // Total size of all hash maps used by the cache buckets
	TotalElementsAdded uint64 // Cumulative count of all elements ever added to the cache
}

// Reset clears all statistics in the Stats object.
// This allows the Stats instance to be reused for subsequent UpdateStats calls
// without allocating a new instance, reducing memory allocations.
func (s *Stats) Reset() {
	*s = Stats{}
}

// bucketInterface defines the contract for cache bucket implementations.
// The cache uses multiple buckets to distribute load and reduce lock contention.
// Different bucket implementations provide varying memory allocation strategies
// and performance characteristics for different use cases.
//
// There are three implementations of this interface:
// 1. bucketPreallocated: Preallocates all memory upfront
// 2. bucketUnallocated: Allocates memory on demand
// 3. bucketTrimmed: Similar to unallocated but with trimming capabilities
type bucketInterface interface {
	Init(maxBytes uint64, trimRatio int) error
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

// ImprovedCache is a high-performance, thread-safe in-memory cache optimized for
// storing billions of transaction metadata entries with minimal GC pressure.
//
// Key features:
// - Sharded architecture with multiple buckets to reduce lock contention
// - Efficient memory management with pre-allocated chunks to minimize GC impact
// - Support for different memory allocation strategies via bucket implementations
// - Fast hash-based lookup and insertion operations
// - Support for batch operations to improve throughput
// - Built-in metrics for monitoring cache performance
//
// The cache significantly outperforms a simple map[string][]byte for large datasets,
// as it manages memory in fixed-size chunks and uses specialized data structures.
//
// Thread-safety: All methods are safe for concurrent use by multiple goroutines.
//
// Memory management: Call Reset when the cache is no longer needed to reclaim memory.
// Note: Call Reset when the cache is no longer needed to reclaim allocated memory.
type ImprovedCache struct {
	// buckets is an array of bucket interfaces that store the actual data
	// Sharding data across multiple buckets reduces lock contention
	buckets [BucketsCount]bucketInterface

	// trimRatio controls how aggressively the cache evicts entries when full
	// Higher values result in more aggressive trimming
	trimRatio int
}

// New creates a new cache instance with the specified memory allocation strategy.
//
// Parameters:
//   - maxBytes: Maximum memory capacity of the cache in bytes. This is divided evenly
//     among all buckets.
//   - bucketType: The memory allocation strategy to use (Unallocated, Preallocated, or Trimmed)
//     which affects performance characteristics and memory usage patterns:
//   - Unallocated: Allocates memory on demand, suitable for caches with unpredictable usage patterns
//   - Preallocated: Allocates all memory upfront, optimizing for predictable high-throughput scenarios
//   - Trimmed: Uses a trimming strategy to manage memory, balancing between the other approaches
//
// Returns:
// - A new ImprovedCache instance configured with the specified parameters
// - Error if initialization fails due to invalid parameters or memory allocation issues
//
// The cache distributes data across multiple buckets to reduce lock contention,
// with each bucket initialized according to the specified allocation strategy.
func New(maxBytes int, bucketType BucketType) (*ImprovedCache, error) {
	LogCacheSize() // log whether we are using small or large cache

	if maxBytes <= 0 {
		return nil, errors.NewServiceError("maxBytes must be greater than 0; got %d", maxBytes)
	}

	var c ImprovedCache

	maxBucketBytes, err := safeconversion.IntToUint64(maxBytes / BucketsCount)
	if err != nil {
		return nil, errors.NewProcessingError("failed to convert maxBytes", err)
	}

	if maxBucketBytes < ChunkSize {
		maxBucketBytes = ChunkSize * 8
	}

	trimRatio, _ := gocore.Config().GetInt("txMetaCacheTrimRatio", 2)

	switch bucketType {
	// if the cache is unallocated cache, unallocatedCache is false, minedBlockStore
	case Unallocated:
		for i := 0; i < BucketsCount; i++ {
			c.buckets[i] = &bucketUnallocated{}
			if err := c.buckets[i].Init(maxBucketBytes, 0); err != nil {
				return nil, errors.NewProcessingError("error creating unallocated cache", err)
			}
		}
	case Preallocated:
		c.trimRatio = trimRatio

		for i := range BucketsCount {
			c.buckets[i] = &bucketPreallocated{}
			if err := c.buckets[i].Init(maxBucketBytes, c.trimRatio); err != nil {
				return nil, errors.NewProcessingError("error creating preallocated cache", err)
			}
		}
	default: // trimmed cache
		for i := 0; i < BucketsCount; i++ {
			c.buckets[i] = &bucketTrimmed{}
			if err := c.buckets[i].Init(maxBucketBytes, 0); err != nil {
				return nil, errors.NewProcessingError("error creating trimmed cache", err)
			}
		}
	}

	return &c, nil
}

// Set adds or updates a single key-value pair in the cache.
// This is the fundamental method for storing data in the cache and is used by higher-level
// operations like SetMulti and SetMultiKeysSingleValue.

// Parameters:
// - k: Key as byte slice, typically a transaction hash
// - v: Value as byte slice, typically transaction metadata
//
// Returns:
// - Error if the storage operation fails, such as when the value exceeds maximum size
//
// Implementation details:
// - Uses xxhash for fast hashing and bucket selection
// - Delegates actual storage to the appropriate bucket implementation
// - Thread-safe for concurrent access from multiple goroutines
//
// Important notes:
// - The key and value contents may be modified after returning from Set
// - Values exceeding maxValueSizeKB (2KB) won't be stored in the cache
// - Entries may be evicted at any time due to cache overflow or hash collisions
// - Use Get method to retrieve stored values
func (c *ImprovedCache) Set(k, v []byte) error {
	h := xxhash.Sum64(k)
	idx := h % BucketsCount

	return c.buckets[idx].Set(k, v, h)
}

// SetMultiKeysSingleValue efficiently stores multiple keys with the same value in the cache.
//
// This method is optimized for scenarios where multiple transaction keys (hashes) need
// to be associated with the same metadata (such as a block ID). It is particularly useful
// for batch processing of transactions that share common metadata.
//
// Parameters:
// - keys: Array of byte slices containing the keys (transaction hashes)
// - value: A single byte slice value to be associated with all keys
// - keySize: Number of keys per batch for processing
//
// Returns:
// - Error if the operation fails, particularly if keys length is not a multiple of keySize
//
// Implementation details:
// - Distributes keys across appropriate buckets based on hash value
// - Uses concurrent processing with goroutines for better performance
// - Appends new value bytes to existing values rather than overwriting
// - Optimized for scenarios like associating multiple transactions with a block ID
func (c *ImprovedCache) SetMultiKeysSingleValue(keys [][]byte, value []byte, keySize int) error {
	if len(keys)%keySize != 0 {
		return errors.NewProcessingError("keys length must be a multiple of keySize; got %d; want %d", len(keys), keySize)
	}

	batchedKeys := make([][][]byte, BucketsCount)

	var key []byte

	var bucketIdx uint64

	var h uint64

	// divide keys blob into buckets
	for i := 0; i < len(keys); i += keySize {
		key = keys[i]
		h = xxhash.Sum64(key)
		bucketIdx = h % BucketsCount
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
			c.buckets[bucketIdx].SetMultiKeysSingleValue(batchedKeys[bucketIdx], value)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
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
		return errors.NewProcessingError("keys length must be a multiple of keySize; got %d; want %d", len(keys), keySize)
	}

	batchedKeys := make([][][]byte, BucketsCount)

	var key []byte

	var bucketIdx uint64

	var h uint64

	// divide keys blob into buckets
	for i := 0; i < len(keys); i += keySize {
		key = keys[i : i+keySize]
		h = xxhash.Sum64(key)
		bucketIdx = h % BucketsCount
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
			c.buckets[bucketIdx].SetMultiKeysSingleValue(batchedKeys[bucketIdx], value) // , hashes[bucketIdx])
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
	batchedKeys := make([][][]byte, BucketsCount)
	batchedValues := make([][][]byte, BucketsCount)

	var bucketIdx uint64

	var h uint64

	// divide keys into buckets
	for i, key := range keys {
		h = xxhash.Sum64(key)
		bucketIdx = h % BucketsCount
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
// Get retrieves a value from the cache for the given key.
//
// This method efficiently retrieves cached transaction metadata using a fast hash-based
// lookup strategy. It handles the destination buffer allocation and copying of the value.
//
// Parameters:
//   - dst: Pointer to a byte slice where the retrieved value will be stored
//     The method may resize this slice as needed to accommodate the value
//   - k: Key to look up (typically a transaction hash)
//
// Returns:
// - nil if the key was found in the cache and the value was successfully retrieved
// - ErrNotFound if the key doesn't exist in the cache
//
// Implementation details:
// - Uses xxhash for fast hashing and bucket selection
// - Thread-safe for concurrent access
// - The key contents may be modified after returning from this method
// - Uses a bucket-specific implementation to handle the actual retrieval
func (c *ImprovedCache) Get(dst *[]byte, k []byte) error {
	h := xxhash.Sum64(k)
	idx := h % BucketsCount

	if !c.buckets[idx].Get(dst, k, h, true) {
		return errors.NewNotFoundError("key %v not found in cache", k, errors.ErrNotFound)
	}

	return nil
}

// Has checks if an entry exists in the cache for the given key.
//
// This method performs a lightweight existence check without retrieving the actual value,
// making it more efficient than Get when only checking for presence.
//
// Parameters:
// - k: Key to check (typically a transaction hash)
//
// Returns:
// - true if the key exists in the cache
// - false if the key does not exist in the cache
//
// Implementation details:
// - Uses xxhash for fast hashing and bucket selection
// - Thread-safe for concurrent access
func (c *ImprovedCache) Has(k []byte) bool {
	h := xxhash.Sum64(k)
	idx := h % BucketsCount

	return c.buckets[idx].Get(nil, k, h, false)
}

// Del removes an entry from the cache for the given key.
//
// This method efficiently removes transaction metadata from the cache
// based on the key's hash value.
//
// Parameters:
// - k: Key to delete (typically a transaction hash)
//
// Implementation details:
// - Uses xxhash for fast hashing and bucket selection
// - Thread-safe for concurrent access
// - The key contents may be modified after returning from this method
// - No-op if the key doesn't exist in the cache (doesn't return an error)
func (c *ImprovedCache) Del(k []byte) {
	h := xxhash.Sum64(k)
	idx := h % BucketsCount
	c.buckets[idx].Del(h)
}

// Reset clears all entries from the cache and reclaims allocated memory.
//
// This method is important for proper memory management when the cache
// is no longer needed or when a complete refresh is required. It iterates
// through all buckets and resets each one.
//
// Implementation details:
// - Thread-safe for concurrent access
// - Calls Reset on each bucket implementation
// - Does not destroy the cache structure itself, just clears its contents
// - Useful when repurposing an existing cache or during graceful shutdown
func (c *ImprovedCache) Reset() {
	for i := range c.buckets[:] {
		c.buckets[i].Reset()
	}
}

// UpdateStats populates the provided Stats structure with current cache statistics.
//
// This method collects performance metrics from all cache buckets, providing
// insight into the cache's current state and utilization.
//
// Parameters:
// - s: Pointer to a Stats structure that will be populated with cache statistics
//
// Implementation details:
//   - Aggregates statistics from all bucket implementations
//   - Thread-safe for concurrent access
//   - Call s.Reset before calling UpdateStats if s is being reused to avoid
//     mixing statistics from previous calls
func (c *ImprovedCache) UpdateStats(s *Stats) {
	for i := range c.buckets[:] {
		c.buckets[i].UpdateStats(s)
	}
}

// bucketTrimmed implements a cache bucket with on-demand memory allocation and automatic trimming.
//
// This bucket type allocates memory chunks as needed and automatically trims expired entries
// when the cache wraps around. It uses a generation-based approach to track entry validity
// and employs a lock-free map for high-performance concurrent access.
//
// Key characteristics:
// - Memory allocated on-demand using mmap for off-heap storage
// - Automatic cleanup of expired entries when cache wraps
// - Generation tracking to determine entry validity
// - Lock-free map implementation for improved concurrent performance
// - Ring buffer design for efficient memory reuse
type bucketTrimmed struct {
	mu sync.RWMutex

	// chunks is a ring buffer with encoded (k, v) pairs.
	// It consists of maxValueSizeKB chunks.
	chunks [][]byte

	// m maps hash(k) to idx of (k, v) pair in chunks.
	// m map[uint64]uint64
	m *txmap.SplitSwissLockFreeMapUint64
	// pass txId directly. How is memory?
	// m map[[32]byte]uint64

	// idx points to chunks for writing the next (k, v) pair.
	idx uint64

	// gen is the generation of chunks.
	gen uint64

	// free chunks per bucket.
	freeChunks []*[ChunkSize]byte

	// allocatedChunks is the total number of chunks allocated for the bucket via mmap.
	// This is capped by uint64(len(chunks)).
	allocatedChunks uint64

	// nextAllocChunks is the next slab size (in chunks) to mmap when freeChunks is empty.
	// It grows exponentially from 1 up to chunksPerAlloc to minimize unused slack.
	nextAllocChunks uint64

	// maxSlabChunks is the per-bucket cap for mmap slab size (in chunks).
	// It is computed from the configured bucket maxBytes.
	maxSlabChunks uint64

	elementsAdded int

	// number of items in the bucket.
	numberOfItems int

	overWriting bool
}

func (b *bucketTrimmed) Init(maxBytes uint64, _ int) error {
	if maxBytes == 0 {
		return errors.NewInvalidArgumentError("maxBytes cannot be zero")
	}

	if maxBytes >= maxBucketSize {
		return errors.NewProcessingError("too big maxBytes=%d; should be smaller than %d", maxBytes, maxBucketSize)
	}

	// IMPORTANT: Use floor division so this bucket can never mmap more than maxBytes
	// worth of chunk storage. Any remainder < ChunkSize is intentionally left unused.
	maxChunks := maxBytes / ChunkSize
	if maxChunks == 0 {
		maxChunks = 1
	}
	b.maxSlabChunks = calcMaxSlabChunks(maxBytes, maxChunks)
	maxChunksInt, err := safeconversion.Uint64ToInt(maxChunks)
	if err != nil {
		return errors.NewProcessingError("failed converting maxChunks", err)
	}
	b.chunks = make([][]byte, maxChunksInt)
	b.m = txmap.NewSplitSwissLockFreeMapUint64(1024)
	b.overWriting = false
	b.Reset()

	return nil
}

func (b *bucketTrimmed) Reset() {
	b.mu.Lock()

	chunks := b.chunks
	for i := range chunks {
		b.putChunk(chunks[i])
		chunks[i] = nil
	}

	b.m = txmap.NewSplitSwissLockFreeMapUint64(1024)
	b.idx = 0
	b.gen = 1
	b.overWriting = false
	// Keep allocatedChunks (we keep the mmap'd chunks in freeChunks for reuse),
	// but reset growth with smart initial sizing based on bucket capacity.
	b.nextAllocChunks = calcInitialAllocChunks(b.maxSlabChunks, uint64(len(b.chunks)))
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
		bmNew := txmap.NewSplitSwissLockFreeMapUint64(1024)

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
	s.EntriesCount += uint64(b.numberOfItems)       // nolint:gosec
	s.TotalElementsAdded += uint64(b.elementsAdded) // nolint:gosec
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

	for i, key := range keys {
		hash = xxhash.Sum64(key)
		// TODO: consider logging if set is not successful. But this should only happen when the key-value size is too big.
		_ = b.Set(key, values[i], hash, true)
	}
}

// Set skips locking if skipLocking is set to true. Locking should be only skipped when the caller holds the lock, i.e. when called from SetMulti.
func (b *bucketTrimmed) Set(k, v []byte, h uint64, skipLocking ...bool) error {
	if len(k) >= (1<<maxValueSizeLog) || len(v) >= (1<<maxValueSizeLog) {
		// Too big key or value - its length cannot be encoded
		// with 2 bytes (see below). Skip the entry.
		return errors.NewProcessingError("[bucketTrimmed.Set]too big key or value (key %d, value %d) max %d", len(k), len(v), 1<<maxValueSizeLog)
	}

	var kvLenBuf [4]byte

	kvLenBuf[0] = byte(uint16(len(k)) >> 8) // nolint:gosec // higher order 8 bits of key's length
	kvLenBuf[1] = byte(len(k))              // lower order 8 bits of key's length
	kvLenBuf[2] = byte(uint16(len(v)) >> 8) // nolint:gosec // higher order 8 bits of value's length
	kvLenBuf[3] = byte(len(v))              // lower order 8 bits of value's length

	kvLen := uint64(len(kvLenBuf) + len(k) + len(v)) // nolint:gosec
	if kvLen >= ChunkSize {
		// Do not store too big keys and values, since they do not
		// fit a chunk.
		return errors.NewProcessingError("key, value, and k-v length %d bytes doesn't fit to a chunk %d bytes", kvLen, ChunkSize)
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
	chunkIdx := idx / ChunkSize
	chunkIdxNew := idxNew / ChunkSize

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
			idx = chunkIdxNew * ChunkSize
			idxNew = idx + kvLen
			chunkIdx = chunkIdxNew
		}

		chunks[chunkIdx] = chunks[chunkIdx][:0]
	}

	chunk := chunks[chunkIdx]

	if chunk == nil {
		var err error
		chunk, err = b.getChunk()

		if err != nil {
			return errors.NewProcessingError("cannot allocate chunk", err)
		}

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
			chunkIdx := idx / ChunkSize
			if chunkIdx >= uint64(len(chunks)) {
				// Corrupted data during the load from file. Just skip it.
				goto end
			}

			chunk := chunks[chunkIdx]
			idx %= ChunkSize

			if idx+4 >= ChunkSize {
				// Corrupted data during the load from file. Just skip it.
				goto end
			}

			kvLenBuf := chunk[idx : idx+4]
			keyLen := (uint64(kvLenBuf[0]) << 8) | uint64(kvLenBuf[1])
			valLen := (uint64(kvLenBuf[2]) << 8) | uint64(kvLenBuf[3])
			idx += 4

			if idx+keyLen+valLen >= ChunkSize {
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

// SetMultiKeysSingleValue stores multiple (k, v) entries for the same bucket for a single v. Appends v to the existing v value, doesn't overwrite.
func (b *bucketTrimmed) SetMultiKeysSingleValue(keys [][]byte, value []byte) { // , hashes []uint64) { //error {
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

func (b *bucketTrimmed) getChunk() ([]byte, error) {
	if len(b.freeChunks) == 0 {
		maxChunks := uint64(len(b.chunks))
		remaining := maxChunks - b.allocatedChunks
		if remaining == 0 {
			// This should be unreachable: once the bucket is full it overwrites and
			// reuses already-allocated chunk slots. Do not mmap beyond configured capacity.
			return nil, errors.NewProcessingError("cannot allocate more chunks; bucket is at maxChunks=%d", maxChunks)
		}

		// Dynamic slab size with smart initial sizing, then exponential growth
		// up to per-bucket maxSlabChunks, capped by remaining capacity.
		slabChunks := b.nextAllocChunks
		if slabChunks == 0 {
			slabChunks = calcInitialAllocChunks(b.maxSlabChunks, maxChunks)
		}
		maxSlab := b.maxSlabChunks
		if maxSlab == 0 {
			maxSlab = minSlabChunks
		}
		if slabChunks > maxSlab {
			slabChunks = maxSlab
		}
		if slabChunks > remaining {
			slabChunks = remaining
		}

		slabBytes := int(slabChunks) * ChunkSize
		if slabBytes <= 0 {
			return nil, errors.NewProcessingError("invalid slabBytes=%d (slabChunks=%d)", slabBytes, slabChunks)
		}

		// Allocate offheap memory, so GOGC won't take into account cache size.
		// This should reduce free memory waste.
		data, err := allocateNamedMmap(slabBytes)
		if err != nil {
			return nil, err
		}

		b.allocatedChunks += slabChunks
		if b.nextAllocChunks == 0 {
			b.nextAllocChunks = calcInitialAllocChunks(b.maxSlabChunks, maxChunks)
		}
		b.nextAllocChunks <<= 1
		if b.nextAllocChunks > maxSlab {
			b.nextAllocChunks = maxSlab
		}

		for len(data) > 0 {
			p := (*[ChunkSize]byte)(unsafe.Pointer(&data[0]))
			b.freeChunks = append(b.freeChunks, p)
			data = data[ChunkSize:]
		}
	}

	n := len(b.freeChunks) - 1
	p := b.freeChunks[n]
	b.freeChunks[n] = nil
	b.freeChunks = b.freeChunks[:n]

	return p[:], nil
}

func (b *bucketTrimmed) putChunk(chunk []byte) {
	if chunk == nil {
		return
	}

	chunk = chunk[:ChunkSize]
	p := (*[ChunkSize]byte)(unsafe.Pointer(&chunk[0]))

	b.freeChunks = append(b.freeChunks, p)
}

func (b *bucketTrimmed) Del(h uint64) {
	b.mu.Lock()
	delete(b.m.Map(), h)
	b.mu.Unlock()
}

func (b *bucketTrimmed) getMapSize() uint64 {
	return uint64(b.m.Length()) // nolint:gosec
}

// bucketPreallocated implements a cache bucket with fully preallocated memory and intelligent trimming.
//
// This bucket type allocates all required memory upfront using mmap for off-heap storage,
// providing predictable memory usage and optimal performance for high-throughput scenarios.
// When the cache fills up, it employs a configurable trimming strategy that removes a
// percentage of the oldest entries to make room for new data.
//
// Key characteristics:
// - All memory allocated upfront for predictable resource usage
// - Configurable trim ratio for memory management when cache fills
// - Off-heap storage using mmap to reduce GC pressure
// - Optimized for high-throughput, predictable workloads
// - Simple map-based indexing for fast lookups
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

func (b *bucketPreallocated) Init(maxBytes uint64, trimRatio int) error {
	if maxBytes == 0 {
		return errors.NewProcessingError("maxBytes cannot be zero")
	}

	if maxBytes >= maxBucketSize {
		return errors.NewProcessingError("too big maxBytes=%d; should be smaller than %d", maxBytes, maxBucketSize)
	}

	// allocate memory for all chunks of the bucket
	data, err := allocateNamedMmap(int(maxBytes))
	if err != nil {
		return err
	}

	for len(data) > 0 {
		p := (*[ChunkSize]byte)(unsafe.Pointer(&data[0]))
		b.chunks = append(b.chunks, p[:])
		data = data[ChunkSize:]
	}

	b.m = make(map[uint64]uint64)
	b.trimRatio = trimRatio
	b.Reset()

	return nil
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
		if int(idx) >= startingOffset { // nolint:gosec
			// calculate the adjusted index. We move old indexes of the items to the left by byteOffsetRemoved
			adjustedIdx := idx - uint64(startingOffset) // nolint:gosec
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

func (b *bucketPreallocated) SetMultiKeysSingleValue(keys [][]byte, value []byte) { // , hashes []uint64) { //error {
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
		return errors.NewProcessingError("[bucketPreallocated.Set]too big key or value (key %d, value %d) max %d", len(k), len(v), 1<<maxValueSizeLog)
	}

	var kvLenBuf [4]byte

	kvLenBuf[0] = byte(uint16(len(k)) >> 8) // nolint: gosec // higher order 8 bits of key's length
	kvLenBuf[1] = byte(len(k))              // lower order 8 bits of key's length
	kvLenBuf[2] = byte(uint16(len(v)) >> 8) // nolint: gosec // higher order 8 bits of value's length
	kvLenBuf[3] = byte(len(v))              // lower order 8 bits of value's length

	kvLen := uint64(len(kvLenBuf) + len(k) + len(v)) // nolint: gosec
	if kvLen >= ChunkSize {
		// Do not store too big keys and values, since they do not
		// fit a chunk.
		return errors.NewProcessingError("key, value, and k-v length %d bytes doesn't fit to a chunk %d bytes", kvLen, ChunkSize)
	}

	chunks := b.chunks

	if len(skipLocking) == 0 || !skipLocking[0] {
		b.mu.Lock()
		defer b.mu.Unlock()
	}

	// calculate the idx of the k-v pair to be added
	// adjust idxNew, calculate where the new k-v pair will end
	// the new k-v pair must be in the same chunk.
	idx := b.idx
	idxNew := idx + kvLen
	chunkIdx := idx / ChunkSize
	chunkIdxNew := idxNew / ChunkSize

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
				chunks[i] = make([]byte, ChunkSize)
			}

			// writing needs to start form the end of the kept chunks.
			idx = ChunkSize * uint64(numOfChunksToKeep) // nolint: gosec
			idxNew = idx + kvLen
			// calculate the where the next write should occur based on new index
			chunkIdx = idx / ChunkSize

			b.cleanLockedMap(numOfChunksToRemove * ChunkSize)
		} else {
			// if the new item doesn't overflow the chunks, we need to allocate a new chunk
			// calculate the index as byte offset
			idx = chunkIdxNew * ChunkSize
			idxNew = idx + kvLen
			chunkIdx = chunkIdxNew
		}
	}

	chunk := chunks[chunkIdx]

	if len(chunk) == 0 {
		return errors.NewProcessingError("SHOULD NEVER ENTER HERE, chunk is nil or empty")
	}

	data := append(append(kvLenBuf[:], k...), v...)
	copy(chunk[idx%ChunkSize:], data)

	chunks[chunkIdx] = chunk
	b.m[h] = idx | (b.gen << bucketSizeBits)
	b.idx = idxNew

	return nil
}

// TODO: Simplify by removing references to generations, generations are not relevant for bucket.
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
			chunkIdx := idx / ChunkSize
			if chunkIdx >= uint64(len(chunks)) {
				// Corrupted data during the load from file. Just skip it.
				goto end
			}

			chunk := chunks[chunkIdx]
			idx %= ChunkSize

			if idx+4 >= ChunkSize {
				// Corrupted data during the load from file. Just skip it.
				goto end
			}

			kvLenBuf := chunk[idx : idx+4]
			keyLen := (uint64(kvLenBuf[0]) << 8) | uint64(kvLenBuf[1])
			valLen := (uint64(kvLenBuf[2]) << 8) | uint64(kvLenBuf[3])
			idx += 4

			if idx+keyLen+valLen >= ChunkSize {
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

// bucketUnallocated implements a cache bucket with on-demand memory allocation.
// When full, it wraps around and overwrites oldest entries without trimming.
// Suitable for unpredictable workloads where trimming overhead should be avoided.
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
	freeChunks []*[ChunkSize]byte

	// allocatedChunks is the total number of chunks allocated for the bucket
	// via mmap. This is capped by maxChunks.
	allocatedChunks uint64

	// nextAllocChunks is the next slab size (in chunks) to mmap when freeChunks is empty.
	// It grows exponentially from min size up to maxSlabChunks to minimize unused slack for
	// lightly-used buckets, while still keeping mmap/syscall overhead bounded.
	nextAllocChunks uint64

	// maxSlabChunks is the per-bucket cap for mmap slab size (in chunks).
	// It is computed from the configured bucket maxBytes.
	maxSlabChunks uint64
}

func (b *bucketUnallocated) Init(maxBytes uint64, _ int) error {
	if maxBytes == 0 {
		return errors.NewProcessingError("maxBytes cannot be zero")
	}

	if maxBytes >= maxBucketSize {
		return errors.NewProcessingError("too big maxBytes=%d; should be smaller than %d", maxBytes, maxBucketSize)
	}

	// IMPORTANT: Use floor division so this bucket can never mmap more than maxBytes
	// worth of chunk storage. Any remainder < ChunkSize is intentionally left unused.
	maxChunks := maxBytes / ChunkSize
	if maxChunks == 0 {
		maxChunks = 1
	}

	// calculate max slub chunks before beginning
	b.maxSlabChunks = calcMaxSlabChunks(maxBytes, maxChunks)
	maxChunksInt, err := safeconversion.Uint64ToInt(maxChunks)
	if err != nil {
		return errors.NewProcessingError("failed converting maxChunks", err)
	}

	b.chunks = make([][]byte, maxChunksInt)
	b.m = make(map[uint64]uint64)

	b.Reset()

	return nil
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

	// Keep allocatedChunks -> we keep the mmap'd chunks in freeChunks for reuse,
	// but reset growth with smart initial sizing based on bucket capacity.
	b.nextAllocChunks = calcInitialAllocChunks(b.maxSlabChunks, uint64(len(b.chunks)))

	b.mu.Unlock()
}

// cleanLockedMap removes expired k-v pairs from bucket map.
func (b *bucketUnallocated) cleanLockedMap() {
	bGen := b.gen & ((1 << genSizeBits) - 1)
	bIdx := b.idx
	bm := b.m
	// bmSize := len(b.m)
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

	for i, key := range keys {
		hash = xxhash.Sum64(key)
		// TODO: consider logging if set is not successful. But this should only happen when the key-value size is too big.
		_ = b.Set(key, values[i], hash, true)
	}
}

// Set skips locking if skipLocking is set to true. Locking should be only skipped when the caller holds the lock, i.e. when called from SetMulti.
func (b *bucketUnallocated) Set(k, v []byte, h uint64, skipLocking ...bool) error {
	if len(k) >= (1<<maxValueSizeLog) || len(v) >= (1<<maxValueSizeLog) {
		// Too big key or value - its length cannot be encoded
		// with 2 bytes (see below). Skip the entry.
		return errors.NewProcessingError("[bucketUnallocated.Set] too big key or value (key %d, value %d) max %d", len(k), len(v), 1<<maxValueSizeLog)
	}

	var kvLenBuf [4]byte

	kvLenBuf[0] = byte(uint16(len(k)) >> 8) // nolint: gosec // higher order 8 bits of key's length
	kvLenBuf[1] = byte(len(k))              // lower order 8 bits of key's length
	kvLenBuf[2] = byte(uint16(len(v)) >> 8) // nolint: gosec // higher order 8 bits of value's length
	kvLenBuf[3] = byte(len(v))              // lower order 8 bits of value's length

	kvLen := uint64(len(kvLenBuf) + len(k) + len(v)) // nolint: gosec
	if kvLen >= ChunkSize {
		// Do not store too big keys and values, since they do not
		// fit a chunk.
		return errors.NewProcessingError("key, value, and k-v length %d bytes doesn't fit to a chunk %d bytes", kvLen, ChunkSize)
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
	chunkIdx := idx / ChunkSize
	chunkIdxNew := idxNew / ChunkSize
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
			idx = chunkIdxNew * ChunkSize
			idxNew = idx + kvLen
			chunkIdx = chunkIdxNew
		}

		chunks[chunkIdx] = chunks[chunkIdx][:0]
	}

	chunk := chunks[chunkIdx]
	if chunk == nil {
		var err error
		chunk, err = b.getChunk()

		if err != nil {
			return errors.NewProcessingError("cannot allocate chunk", err)
		}

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
			chunkIdx := idx / ChunkSize
			if chunkIdx >= uint64(len(chunks)) {
				// Corrupted data during the load from file. Just skip it.
				goto end
			}

			chunk := chunks[chunkIdx]
			idx %= ChunkSize

			if idx+4 >= ChunkSize {
				// Corrupted data during the load from file. Just skip it.
				goto end
			}

			kvLenBuf := chunk[idx : idx+4]
			keyLen := (uint64(kvLenBuf[0]) << 8) | uint64(kvLenBuf[1])
			valLen := (uint64(kvLenBuf[2]) << 8) | uint64(kvLenBuf[3])

			idx += 4

			if idx+keyLen+valLen >= ChunkSize {
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

// SetMultiKeysSingleValue stores multiple (k, v) entries for the same bucket for a single v. Appends v to the existing v value, doesn't overwrite.
func (b *bucketUnallocated) SetMultiKeysSingleValue(keys [][]byte, value []byte) { // , hashes []uint64) { //error {
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

func (b *bucketUnallocated) getChunk() ([]byte, error) {
	if len(b.freeChunks) == 0 {
		maxChunks := uint64(len(b.chunks))

		remaining := maxChunks - b.allocatedChunks
		if remaining == 0 {
			// This should be unreachable: once the bucket is full it overwrites and
			// reuses already-allocated chunk slots. Do not mmap beyond configured capacity.
			return nil, errors.NewProcessingError("cannot allocate more chunks; bucket is at maxChunks=%d", maxChunks)
		}

		slabChunks := b.nextAllocChunks
		if slabChunks == 0 {
			slabChunks = calcInitialAllocChunks(b.maxSlabChunks, maxChunks)
		}

		maxSlab := b.maxSlabChunks
		if maxSlab == 0 {
			maxSlab = minSlabChunks
		}

		if slabChunks > maxSlab {
			slabChunks = maxSlab
		}

		// we are at most creating remaining nubmer of available chunks.
		if slabChunks > remaining {
			slabChunks = remaining
		}

		slabBytes := int(slabChunks) * ChunkSize
		if slabBytes <= 0 {
			return nil, errors.NewProcessingError("invalid slabBytes=%d (slabChunks=%d)", slabBytes, slabChunks)
		}

		// Allocate offheap memory, so GOGC won't take into account cache size.
		// This should reduce free memory waste.
		data, err := allocateNamedMmap(slabBytes)
		if err != nil {
			return nil, err
		}

		// increase number of already allocated chunks.
		b.allocatedChunks += slabChunks

		// Exponential growth for the next allocation.
		if b.nextAllocChunks == 0 {
			b.nextAllocChunks = calcInitialAllocChunks(b.maxSlabChunks, maxChunks)
		}

		// move to next power of two.
		b.nextAllocChunks <<= 1
		if b.nextAllocChunks > maxSlab {
			b.nextAllocChunks = maxSlab
		}

		for len(data) > 0 {
			p := (*[ChunkSize]byte)(unsafe.Pointer(&data[0]))
			b.freeChunks = append(b.freeChunks, p)
			data = data[ChunkSize:]
		}
	}

	n := len(b.freeChunks) - 1
	p := b.freeChunks[n]
	b.freeChunks[n] = nil
	b.freeChunks = b.freeChunks[:n]

	return p[:], nil
}

func (b *bucketUnallocated) putChunk(chunk []byte) {
	if chunk == nil {
		return
	}

	chunk = chunk[:ChunkSize]
	p := (*[ChunkSize]byte)(unsafe.Pointer(&chunk[0]))
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
