// Package memory provides an in-memory implementation of the blob.Store interface.
// This implementation is designed for temporary storage, testing, and development purposes.
// It stores all blobs in memory, making it fast but volatile (data is lost on process restart).
// The implementation supports all blob.Store features including DAH-based expiration.
//
// The memory implementation offers several advantages for development and testing:
// - Zero configuration required - works out of the box
// - Extremely fast access with no I/O overhead
// - Automatic cleanup of expired blobs via background goroutine
// - Thread-safe operations with internal synchronization
// - Support for all blob.Store interface features
//
// This implementation is not recommended for production use with large datasets
// as it will consume system memory proportional to the stored data size and
// does not persist data across process restarts.
package memory

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
)

// blobData holds the complete blob data (header + payload + footer) and its DAH
type blobData struct {
	data []byte
	dah  uint32
}

// Memory implements the blob.Store interface using in-memory data structures.
// It provides a fast, non-persistent storage solution primarily for testing and development.
// The implementation uses a map to store blob data as a single byte slice (header + payload + footer).
type Memory struct {
	mu                 sync.RWMutex
	blobs              map[string]*blobData
	keys               map[string][]byte
	options            *options.Options
	Counters           map[string]int
	countersMu         sync.Mutex
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
		blobs:    make(map[string]*blobData),
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

	for key, bd := range m.blobs {
		if bd.dah == 0 {
			continue
		}

		if bd.dah <= blockHeight {
			delete(m.blobs, key)
			delete(m.keys, key)
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

func (m *Memory) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	defer reader.Close()

	merged := options.MergeOptions(m.options, opts)

	if !merged.AllowOverwrite {
		// for consistency with other stores, check if the blob already exists and throw BlobAlreadyExistsError if it does
		if exists, err := m.Exists(ctx, key, fileType, opts...); err != nil {
			return err
		} else if exists {
			return errors.NewBlobAlreadyExistsError("blob already exists")
		}
	}

	b, err := io.ReadAll(reader)
	if err != nil {
		return errors.NewStorageError("failed to read data from reader", err)
	}

	return m.Set(ctx, key, fileType, b, opts...)
}

func (m *Memory) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	merged := options.MergeOptions(m.options, opts)
	storeKey := hashKey(key, fileType, merged)

	if !merged.AllowOverwrite {
		if exists, err := m.Exists(ctx, key, fileType, opts...); err != nil {
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
	m.keys[storeKey] = key

	dah := merged.DAH

	if dah == 0 && merged.BlockHeightRetention > 0 {
		dah = m.currentBlockHeight + merged.BlockHeightRetention
	}

	var data []byte

	header := fileformat.NewHeader(fileType)
	data = append(data, header.Bytes()...)

	data = append(data, value...)

	m.blobs[storeKey] = &blobData{
		data: data,
		dah:  dah,
	}

	return nil
}

func (m *Memory) SetDAH(_ context.Context, key []byte, fileType fileformat.FileType, newDAH uint32, opts ...options.FileOption) error {
	merged := options.MergeOptions(m.options, opts)
	storeKey := hashKey(key, fileType, merged)

	m.mu.Lock()
	defer m.mu.Unlock()

	if bd, ok := m.blobs[storeKey]; ok {
		bd.dah = newDAH
	}

	return nil
}

func (m *Memory) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	b, err := m.Get(ctx, key, fileType, opts...)
	if err != nil {
		return nil, err
	}

	return io.NopCloser(bytes.NewBuffer(b)), nil
}

func (m *Memory) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	merged := options.MergeOptions(m.options, opts)
	storeKey := hashKey(key, fileType, merged)

	m.mu.RLock()
	defer m.mu.RUnlock()

	m.countersMu.Lock()
	m.Counters["get"]++
	m.countersMu.Unlock()

	bd, ok := m.blobs[storeKey]
	if !ok {
		return nil, errors.ErrNotFound
	}

	header := fileformat.NewHeader(fileType)

	return bd.data[header.Size():], nil
}

func (m *Memory) Exists(_ context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
	merged := options.MergeOptions(m.options, opts)
	storeKey := hashKey(key, fileType, merged)

	m.mu.RLock()
	defer m.mu.RUnlock()

	m.countersMu.Lock()
	m.Counters["exists"]++
	m.countersMu.Unlock()

	_, ok := m.blobs[storeKey]

	return ok, nil
}

func (m *Memory) Del(_ context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error {
	merged := options.MergeOptions(m.options, opts)
	storeKey := hashKey(key, fileType, merged)

	m.mu.Lock()
	defer m.mu.Unlock()

	m.countersMu.Lock()
	m.Counters["del"]++
	m.countersMu.Unlock()

	delete(m.blobs, storeKey)
	delete(m.keys, storeKey)

	return nil
}

func hashKey(key []byte, fileType fileformat.FileType, options *options.Options) string {
	var storeKey string

	if len(options.Filename) > 0 {
		storeKey = options.Filename
	} else {
		storeKey = hex.EncodeToString(key)
	}

	storeKey += "_" + fileType.String()

	return storeKey
}

func (m *Memory) ListKeys() [][]byte {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys := make([][]byte, 0, len(m.keys))
	for _, k := range m.keys {
		keys = append(keys, k)
	}

	return keys
}

func (m *Memory) SetCurrentBlockHeight(height uint32) {
	m.currentBlockHeight = height
}
