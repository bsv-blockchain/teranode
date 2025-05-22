// Package memory provides an in-memory implementation of the blob.Store interface.
// This implementation is designed for temporary storage, testing, and development purposes.
// It stores all blobs in memory, making it fast but volatile (data is lost on process restart).
// The implementation supports all blob.Store features including DAH-based expiration.
package memory

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
)

// Memory implements the blob.Store interface using in-memory data structures.
// It provides a fast, non-persistent storage solution primarily for testing and development.
// The implementation uses maps to store blob data, headers, footers, and Delete-At-Height values.
type Memory struct {
	// mu protects concurrent access to all maps and shared data
	mu                 sync.RWMutex
	// headers stores blob header data by key
	headers            map[string][]byte
	// footers stores blob footer data by key
	footers            map[string][]byte
	// keys tracks known blob keys
	keys               map[string][]byte
	// blobs stores the actual blob content by key
	blobs              map[string][]byte
	// dahs stores Delete-At-Height values by key
	dahs               map[string]uint32
	// options contains default options for blob operations
	options            *options.Options
	// Counters tracks operation metrics (exported for testing)
	Counters           map[string]int
	// countersMu protects access to Counters map
	countersMu         sync.Mutex
	// currentBlockHeight tracks current blockchain height for DAH processing
	currentBlockHeight uint32
}

// New creates a new in-memory blob store with the specified options.
// It initializes all internal maps and starts a background goroutine to periodically
// clean expired blobs based on their Delete-At-Height (DAH) values.
//
// Parameters:
//   - opts: Optional store configuration options that affect default behavior
//
// Returns:
//   - *Memory: A configured in-memory blob store instance
func New(opts ...options.StoreOption) *Memory {
	m := &Memory{
		keys:     make(map[string][]byte),
		blobs:    make(map[string][]byte),
		dahs:     make(map[string]uint32),
		headers:  make(map[string][]byte),
		footers:  make(map[string][]byte),
		options:  options.NewStoreOptions(opts...),
		Counters: make(map[string]int),
	}

	go m.ttlCleaner(context.Background(), 1*time.Minute)

	return m
}

// SetBlockHeight updates the current block height used for DAH-based cleanup.
// When the current block height exceeds a blob's DAH value, the blob becomes
// eligible for automatic deletion during the next cleanup cycle.
//
// Parameters:
//   - blockHeight: The current blockchain height
func (m *Memory) SetBlockHeight(blockHeight uint32) {
	m.currentBlockHeight = blockHeight
}

// ttlCleaner runs a periodic cleanup process to remove expired blobs.
// It runs as a background goroutine and checks for expired blobs based on their
// Delete-At-Height (DAH) values compared to the current block height.
//
// Parameters:
//   - ctx: Context for controlling the cleaner lifecycle
//   - interval: Duration between cleanup operations
func (m *Memory) ttlCleaner(ctx context.Context, interval time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			cleanExpiredFiles(m, m.currentBlockHeight)
		}
	}
}

// cleanExpiredFiles removes blobs that have reached their Delete-At-Height (DAH).
// When a blob's DAH is less than or equal to the current block height, the blob
// and all its associated data (headers, footers, etc.) are removed from memory.
//
// Parameters:
//   - m: The Memory store instance
//   - blockHeight: Current blockchain height to compare against DAH values
func cleanExpiredFiles(m *Memory, blockHeight uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key, dah := range m.dahs {
		if dah == 0 {
			continue
		}

		if dah <= blockHeight {
			delete(m.blobs, key)
			delete(m.dahs, key)
			delete(m.headers, key)
			delete(m.footers, key)
		}
	}
}

// Health checks the operational status of the memory store.
// For the memory implementation, this always succeeds unless the context is canceled.
// The function increments an internal counter for monitoring/debugging purposes.
//
// Parameters:
//   - ctx: Context for the operation (can be used to cancel the check)
//   - checkLiveness: Whether to perform a more thorough health check (ignored for memory store)
//
// Returns:
//   - int: HTTP status code (always http.StatusOK for memory store)
//   - string: Status message
//   - error: Any error that occurred (always nil for memory store)
func (m *Memory) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	m.countersMu.Lock()
	m.Counters["health"]++
	m.countersMu.Unlock()

	return http.StatusOK, "Memory Store", nil
}

// Close releases resources associated with the memory store.
// For the memory implementation, this is a no-op operation since there are no
// external resources to release, but it does increment an internal counter.
// The method acquires a lock to ensure thread safety with concurrent operations.
//
// Parameters:
//   - ctx: Context for the operation (unused in memory implementation)
//
// Returns:
//   - error: Any error that occurred (always nil for memory store)
func (m *Memory) Close(_ context.Context) error {
	// The lock is to ensure that no other operations are happening while we close the store
	m.mu.Lock()
	defer m.mu.Unlock()

	m.countersMu.Lock()
	m.Counters["close"]++
	m.countersMu.Unlock()

	// noop
	return nil
}

func (m *Memory) SetFromReader(ctx context.Context, key []byte, reader io.ReadCloser, opts ...options.FileOption) error {
	defer reader.Close()

	merged := options.MergeOptions(m.options, opts)

	if !merged.AllowOverwrite {
		// for consistency with other stores, check if the blob already exists and throw BlobAlreadyExistsError if it does
		if exists, err := m.Exists(ctx, key); err != nil {
			return err
		} else if exists {
			return errors.NewBlobAlreadyExistsError("blob already exists")
		}
	}

	b, err := io.ReadAll(reader)
	if err != nil {
		return errors.NewStorageError("failed to read data from reader", err)
	}

	return m.Set(ctx, key, b, opts...)
}

func (m *Memory) Set(ctx context.Context, hash []byte, value []byte, opts ...options.FileOption) error {
	merged := options.MergeOptions(m.options, opts)

	storeKey := hashKey(hash, merged)

	if !merged.AllowOverwrite {
		// for consistency with other stores, check if the blob already exists and throw BlobAlreadyExistsError if it does
		if exists, err := m.Exists(ctx, hash); err != nil {
			return err
		} else if exists {
			return errors.NewBlobAlreadyExistsError("blob already exists")
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.countersMu.Lock()
	m.Counters["set"]++
	m.countersMu.Unlock()

	m.keys[string(hash)] = []byte{}
	m.blobs[storeKey] = value

	dah := merged.DAH
	if dah == 0 && merged.BlockHeightRetention > 0 {
		dah = m.currentBlockHeight + merged.BlockHeightRetention
	}

	m.dahs[storeKey] = dah

	if merged.Header != nil {
		m.headers[storeKey] = merged.Header
	}

	if merged.Footer != nil {
		footer, err := merged.Footer.GetFooter()
		if err != nil {
			return err
		}

		m.footers[storeKey] = footer
	}

	return nil
}

func (m *Memory) SetDAH(_ context.Context, hash []byte, newDAH uint32, opts ...options.FileOption) error {
	merged := options.MergeOptions(m.options, opts)

	storeKey := hashKey(hash, merged)

	m.mu.Lock()
	defer m.mu.Unlock()

	if newDAH > 0 {
		m.dahs[storeKey] = newDAH
	} else {
		delete(m.dahs, storeKey)
	}

	return nil
}

func (m *Memory) GetDAH(_ context.Context, hash []byte, opts ...options.FileOption) (uint32, error) {
	merged := options.MergeOptions(m.options, opts)

	storeKey := hashKey(hash, merged)

	m.mu.RLock()
	defer m.mu.RUnlock()

	dah, ok := m.dahs[storeKey]
	if !ok {
		return 0, nil
	}

	return dah, nil
}

func (m *Memory) GetIoReader(ctx context.Context, key []byte, opts ...options.FileOption) (io.ReadCloser, error) {
	setOptions := options.NewFileOptions(opts...)

	storeKey := key
	if setOptions.Extension != "" {
		storeKey = append(storeKey, []byte(setOptions.Extension)...)
	}

	b, err := m.Get(ctx, storeKey)
	if err != nil {
		return nil, err
	}

	return io.NopCloser(bytes.NewBuffer(b)), nil
}

func (m *Memory) Get(_ context.Context, hash []byte, opts ...options.FileOption) ([]byte, error) {
	merged := options.MergeOptions(m.options, opts)

	storeKey := hashKey(hash, merged)

	m.mu.RLock()
	defer m.mu.RUnlock()

	m.countersMu.Lock()
	m.Counters["get"]++
	m.countersMu.Unlock()

	bytes, ok := m.blobs[storeKey]
	if !ok {
		return nil, errors.ErrNotFound
	}

	return bytes, nil
}

func (m *Memory) GetHead(_ context.Context, hash []byte, nrOfBytes int, opts ...options.FileOption) ([]byte, error) {
	b, err := m.Get(context.Background(), hash, opts...)
	if err != nil {
		return nil, err
	}

	if nrOfBytes > len(b) {
		return b, nil
	}

	m.countersMu.Lock()
	m.Counters["getHead"]++
	m.countersMu.Unlock()

	return b[:nrOfBytes], nil
}

func (m *Memory) Exists(_ context.Context, hash []byte, opts ...options.FileOption) (bool, error) {
	merged := options.MergeOptions(m.options, opts)

	storeKey := hashKey(hash, merged)

	m.mu.RLock()
	defer m.mu.RUnlock()

	m.countersMu.Lock()
	m.Counters["exists"]++
	m.countersMu.Unlock()

	_, ok := m.blobs[storeKey]

	return ok, nil
}

func (m *Memory) Del(_ context.Context, hash []byte, opts ...options.FileOption) error {
	merged := options.MergeOptions(m.options, opts)

	storeKey := hashKey(hash, merged)

	m.mu.Lock()
	defer m.mu.Unlock()

	m.countersMu.Lock()
	m.Counters["del"]++
	m.countersMu.Unlock()

	delete(m.blobs, storeKey)
	delete(m.keys, string(hash))
	delete(m.dahs, storeKey)

	return nil
}

func hashKey(key []byte, options *options.Options) string {
	var storeKey []byte

	if len(options.Filename) > 0 {
		storeKey = []byte(options.Filename)
	} else {
		storeKey = key
	}

	if len(options.Extension) > 0 {
		storeKey = append(storeKey, []byte(options.Extension)...)
	}

	return string(storeKey)
}

func (m *Memory) GetHeader(ctx context.Context, hash []byte, opts ...options.FileOption) ([]byte, error) {
	merged := options.MergeOptions(m.options, opts)

	storeKey := hashKey(hash, merged)

	return m.headers[storeKey], nil
}

func (m *Memory) GetFooterMetaData(ctx context.Context, hash []byte, opts ...options.FileOption) ([]byte, error) {
	merged := options.MergeOptions(m.options, opts)

	if merged.Footer == nil {
		return nil, nil
	}

	storeKey := hashKey(hash, merged)

	return merged.Footer.GetFooterMetaData(m.footers[storeKey]), nil
}

func (m *Memory) ListKeys() [][]byte {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys := make([][]byte, 0, len(m.keys))
	for k := range m.keys {
		keys = append(keys, []byte(k))
	}

	return keys
}
