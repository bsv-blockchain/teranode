// Package blob provides blob storage functionality with various storage backend implementations.
package blob

import (
	"context"
	"io"
	"sync"

	"github.com/bitcoin-sv/teranode/errors"
	blob_options "github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/libsv/go-bt/v2/chainhash"
)

// ConcurrentBlob provides thread-safe access to blob storage operations.
type ConcurrentBlob[K chainhash.Hash] struct {
	// blobStore is the underlying blob storage implementation
	blobStore Store
	// options contains the file options to use when storing blobs
	options []blob_options.FileOption
	// mu provides mutual exclusion for concurrent operations
	mu sync.RWMutex
	// wg tracks ongoing blob operations by key
	wg map[K]*sync.WaitGroup
}

// NewConcurrentBlob creates a new ConcurrentBlob instance
// blobStore is the blob store to use for caching the blobs. Set a default TTL on the store to have the blobs expire.
// options is a list of file options to use when storing the blobs.
func NewConcurrentBlob[K chainhash.Hash](blobStore Store, options ...blob_options.FileOption) *ConcurrentBlob[K] {
	return &ConcurrentBlob[K]{
		blobStore: blobStore,
		options:   options,
		wg:        make(map[K]*sync.WaitGroup),
	}
}

// Get retrieves a blob from the store. If the blob doesn't exist, it will be fetched
// using the provided getBlobReader function and stored in the blob store before being returned.
// The operation is thread-safe and ensures only one fetch operation occurs at a time for each key.
//
// Parameters:
//   - ctx: The context for the operation
//   - key: The key identifying the blob
//   - getBlobReader: A function that returns an io.ReadCloser for fetching the blob
//
// Returns:
//   - io.ReadCloser: A reader for the blob data
//   - error: Any error that occurred during the operation
func (c *ConcurrentBlob[K]) Get(ctx context.Context, key K, getBlobReader func() (io.ReadCloser, error)) (io.ReadCloser, error) {
	var (
		found bool
		err   error
		wg    *sync.WaitGroup
	)

	// Start by acquiring a read lock
	c.mu.RLock()

	// Check if the value is already in the cache
	if found, err = c.blobStore.Exists(ctx, key[:], c.options...); found && err == nil {
		c.mu.RUnlock()
		return c.blobStore.GetIoReader(ctx, key[:], c.options...)
	}

	// Upgrade to a write lock if the value is not found
	c.mu.RUnlock()
	c.mu.Lock()

	// Check again to avoid race conditions
	if found, err = c.blobStore.Exists(ctx, key[:], c.options...); found && err == nil {
		c.mu.Unlock()
		return c.blobStore.GetIoReader(ctx, key[:], c.options...)
	}

	// If not, check if there is an ongoing request
	if wg, found = c.wg[key]; found {
		c.mu.Unlock()
		wg.Wait() // Wait for the other goroutine to finish

		if found, err = c.blobStore.Exists(ctx, key[:], c.options...); found && err == nil {
			return c.blobStore.GetIoReader(ctx, key[:], c.options...)
		}

		return nil, errors.NewProcessingError("failed to get blob while waiting for another goroutine to finish")
	}

	// Create a new WaitGroup for the key
	wg = &sync.WaitGroup{}
	wg.Add(1)
	c.wg[key] = wg

	// Release the global lock, for others to wait on the wait group
	c.mu.Unlock()

	// Perform the fetch, with a lock on the cache
	c.mu.Lock()

	defer func() {
		wg.Done()         // Mark the wait group as done
		delete(c.wg, key) // Remove it from the map

		c.mu.Unlock()
	}()

	// Perform the fetch
	resultReader, err := getBlobReader()
	if err != nil {
		return nil, err
	}

	// Cache the result
	err = c.blobStore.SetFromReader(ctx, key[:], resultReader, c.options...)
	if err != nil {
		return nil, err
	}

	_ = resultReader.Close()

	// Return a reader to the cached blob
	return c.blobStore.GetIoReader(ctx, key[:], c.options...)
}
