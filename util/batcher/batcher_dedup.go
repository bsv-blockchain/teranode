package batcher

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/teranode/util"
)

// TimePartitionedMap is a time-partitioned map for efficient expiration of old entries.
// It divides time into buckets (e.g., minute buckets) and stores items in the appropriate bucket.
// This allows for efficient cleanup by simply dropping entire buckets when they expire.
type TimePartitionedMap[K comparable, V any] struct {
	buckets         *util.SyncedMap[int64, *util.SyncedMap[K, V]] // Map of timestamp buckets to key-value maps
	bucketSize      time.Duration                                 // Size of each time bucket (e.g., 1 minute)
	maxBuckets      int                                           // Maximum number of buckets to keep
	oldestBucket    atomic.Int64                                  // Timestamp of the oldest bucket
	newestBucket    atomic.Int64                                  // Timestamp of the newest bucket
	itemCount       atomic.Int64                                  // Total number of items across all buckets
	currentBucketID atomic.Int64                                  // Current bucket ID, updated periodically
	zero            V                                             // Zero value for V
	bucketsMu       sync.Mutex                                    // Mutex for buckets
}

// NewTimePartitionedMap creates a new time-partitioned map with the specified bucket size and max buckets.
func NewTimePartitionedMap[K comparable, V any](bucketSize time.Duration, maxBuckets int) *TimePartitionedMap[K, V] {
	m := &TimePartitionedMap[K, V]{
		buckets:         util.NewSyncedMap[int64, *util.SyncedMap[K, V]](),
		bucketSize:      bucketSize,
		maxBuckets:      maxBuckets,
		oldestBucket:    atomic.Int64{},
		newestBucket:    atomic.Int64{},
		itemCount:       atomic.Int64{},
		currentBucketID: atomic.Int64{},
	}

	// Initialize the current bucket ID
	initialBucketID := time.Now().UnixNano() / int64(m.bucketSize)
	m.currentBucketID.Store(initialBucketID)

	// Start a goroutine to update the current bucket ID periodically
	// Use a ticker with a period that's a fraction of the bucket size to ensure accuracy
	updateInterval := m.bucketSize / 10
	if updateInterval < time.Millisecond*100 {
		updateInterval = time.Millisecond * 100 // Minimum update interval of 100ms
	}

	go func() {
		ticker := time.NewTicker(updateInterval)
		defer ticker.Stop()

		for range ticker.C {
			m.currentBucketID.Store(time.Now().UnixNano() / int64(m.bucketSize))
		}
	}()

	go func() {
		ticker := time.NewTicker(m.bucketSize / 2)
		defer ticker.Stop()

		for range ticker.C {
			m.cleanupOldBuckets()
		}
	}()

	return m
}

// Get retrieves a value from the map
func (m *TimePartitionedMap[K, V]) Get(key K) (V, bool) {
	for bucketID := range m.buckets.Range() {
		if bucket, exists := m.buckets.Get(bucketID); exists {
			if value, found := bucket.Get(key); found {
				return value, true
			}
		}
	}

	return m.zero, false
}

// Set adds or updates a key-value pair in the map
func (m *TimePartitionedMap[K, V]) Set(key K, value V) bool {
	// Global deduplication check: if key already exists in any bucket, do not proceed.
	// This check iterates over all buckets and can be resource-intensive.
	if _, exists := m.Get(key); exists {
		return false
	}

	var (
		bucket *util.SyncedMap[K, V]
		exists bool
	)

	bucketID := m.currentBucketID.Load()

	m.bucketsMu.Lock() // Lock before accessing or modifying m.buckets structure

	// Initialize bucket if it doesn't exist for the current bucketID
	if bucket, exists = m.buckets.Get(bucketID); !exists {
		bucket = util.NewSyncedMap[K, V]()
		m.buckets.Set(bucketID, bucket)

		// Update newest/oldest bucket trackers since a new bucket was added
		if m.newestBucket.Load() < bucketID {
			m.newestBucket.Store(bucketID)
		}

		if m.oldestBucket.Load() == 0 || m.oldestBucket.Load() > bucketID {
			m.oldestBucket.Store(bucketID)
		}
	}

	// Add item to the determined bucket and update total item count.
	// This is now done under the m.bucketsMu lock to prevent a race condition
	// where the bucket might be removed by cleanupOldBuckets between being
	// retrieved/created and the item being added to it.
	bucket.Set(key, value)
	m.itemCount.Add(1)

	m.bucketsMu.Unlock() // Unlock after all modifications to m.buckets and the specific bucket's content.

	return true
}

// Delete removes a key from the map
func (m *TimePartitionedMap[K, V]) Delete(key K) bool {
	deleted := false

	// Check all buckets
	bucketMap := m.buckets.Range()
	for bucketID, bucket := range bucketMap {
		if _, found := bucket.Get(key); found {
			bucket.Delete(key)

			deleted = true

			m.itemCount.Add(-1)

			// If bucket is empty, remove it
			if bucket.Length() == 0 {
				m.buckets.Delete(bucketID)
				// Recalculate oldest bucket if needed
				if bucketID == m.oldestBucket.Load() {
					m.recalculateOldestBucket()
				}
			}

			break
		}
	}

	return deleted
}

// cleanupOldBuckets removes buckets that are older than the maximum age
func (m *TimePartitionedMap[K, V]) cleanupOldBuckets() {
	m.bucketsMu.Lock()
	defer m.bucketsMu.Unlock()

	// Clean up by time - remove buckets older than our window
	// Use the cached bucket ID by passing a zero time
	maxAgeBucketID := time.Now().Add(-m.bucketSize*time.Duration(m.maxBuckets)).UnixNano() / int64(m.bucketSize)

	for bucketID, bucket := range m.buckets.Range() {
		if bucketID <= maxAgeBucketID {
			m.itemCount.Add(int64(-1 * bucket.Length()))
			m.buckets.Delete(bucketID)
		}
	}

	// Update oldest bucket
	if m.oldestBucket.Load() <= maxAgeBucketID {
		m.oldestBucket.Store(maxAgeBucketID)
		m.recalculateOldestBucket()
	}
}

// recalculateOldestBucket finds the oldest bucket after deletions
func (m *TimePartitionedMap[K, V]) recalculateOldestBucket() {
	if m.buckets.Length() == 0 {
		m.oldestBucket.Store(0)
		m.newestBucket.Store(0)

		return
	}

	oldest := int64(1<<63 - 1) // Max int64
	newest := int64(0)

	for bucketID := range m.buckets.Range() {
		if bucketID < oldest {
			oldest = bucketID
		}

		if bucketID > newest {
			newest = bucketID
		}
	}

	m.oldestBucket.Store(oldest)
	m.newestBucket.Store(newest)
}

// Count returns the total number of items in the map
func (m *TimePartitionedMap[K, V]) Count() int {
	return int(m.itemCount.Load())
}

// BatcherWithDedup is a utility that batches items together with deduplication support.
// It extends the basic Batcher2 functionality by adding deduplication of items.
type BatcherWithDedup[T comparable] struct {
	Batcher2[T]

	// Deduplication related fields
	deduplicationWindow time.Duration
	deduplicationMap    *TimePartitionedMap[T, struct{}]
}

// NewWithDeduplication creates a new Batcher with deduplication support.
// The deduplication window is set to 1 minute by default.
// This requires T to be comparable.
func NewWithDeduplication[T comparable](size int, timeout time.Duration, fn func(batch []*T), background bool) *BatcherWithDedup[T] {
	deduplicationWindow := time.Minute // 1-minute deduplication window

	b := &BatcherWithDedup[T]{
		Batcher2: Batcher2[T]{
			fn:         fn,
			size:       size,
			timeout:    timeout,
			batch:      make([]*T, 0, size),
			ch:         make(chan *T, size*64),
			triggerCh:  make(chan struct{}),
			background: background,
		},
		deduplicationWindow: deduplicationWindow,
		// Create a time-partitioned map with 1-second buckets and enough buckets to cover the deduplication window
		deduplicationMap: NewTimePartitionedMap[T, struct{}](time.Second, int(deduplicationWindow.Seconds())+1),
	}

	go b.worker()

	return b
}

// Put adds an item to the batch with deduplication. If the item is a duplicate
// within the deduplication window, it will be ignored.
// This method overrides the Put method from Batcher2.
func (b *BatcherWithDedup[T]) Put(item *T) {
	if item == nil {
		return
	}

	// Set returns TRUE if the item was added, FALSE if it was a duplicate
	if b.deduplicationMap.Set(*item, struct{}{}) {
		// Add the item to the batch
		b.ch <- item
	}
}
