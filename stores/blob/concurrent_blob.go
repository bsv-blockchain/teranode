// Package blob provides a comprehensive blob storage system with multiple backend implementations.
// The blob package is designed to store, retrieve, and manage arbitrary binary data (blobs) with
// features such as customizable storage backends, Delete-At-Height (DAH) functionality for automatic
// data expiration, and a standardized HTTP API.
//
// This file implements the ConcurrentBlob wrapper, which provides thread-safe access to blob storage
// with optimized concurrency patterns. It uses a double-checked locking pattern to ensure that only
// one fetch operation occurs at a time for each unique key, while allowing concurrent operations on
// different keys. This significantly improves performance in high-concurrency environments where
// the same blob might be requested multiple times simultaneously.
package blob

import (
	"context"
	"io"
	"sync"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	blob_options "github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/libsv/go-bt/v2/chainhash"
)

// ConcurrentBlob provides thread-safe access to blob storage operations with optimized concurrent access patterns.
// It implements a double-checked locking pattern to ensure that only one fetch operation occurs at a time
// for each unique key, while allowing concurrent operations on different keys. This pattern
// is particularly useful in high-concurrency environments where the same blob might be requested
// multiple times simultaneously, avoiding duplicate network or disk operations.
// ConcurrentBlob is a generic type parametrized by a key type K that must satisfy chainhash.Hash constraints.
// This allows type-safe handling of different hash types while maintaining the concurrent access pattern.
type ConcurrentBlob[K chainhash.Hash] struct {
	// blobStore is the underlying blob storage implementation that handles the actual data storage
	blobStore Store
	// options contains the file options to use when storing blobs (e.g., DAH settings)
	options []blob_options.FileOption
	// mu provides mutual exclusion for concurrent operations, using a read-write lock for efficiency
	mu sync.RWMutex
	// wg tracks ongoing blob operations by key, allowing other goroutines to wait for completion
	// rather than duplicating work
	wg map[K]*sync.WaitGroup
}

// NewConcurrentBlob creates a new ConcurrentBlob instance with the specified blob store and options.
//
// Parameters:
//   - blobStore: The underlying blob store implementation to use for data storage. For automatic
//     expiration of blobs, use a store with Default-At-Height (DAH) functionality configured.
//   - options: Optional file options to apply when storing blobs, such as custom metadata or
//     specific DAH values. These options will be used for all operations performed by this instance.
//
// Returns:
//   - *ConcurrentBlob[K]: A new instance ready for concurrent blob operations
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
func (c *ConcurrentBlob[K]) Get(ctx context.Context, key K, fileType fileformat.FileType, getBlobReader func() (io.ReadCloser, error)) (io.ReadCloser, error) {
	var (
		found bool
		err   error
		wg    *sync.WaitGroup
	)

	// Start by acquiring a read lock
	c.mu.RLock()

	// Check if the value is already in the cache
	if found, err = c.blobStore.Exists(ctx, key[:], fileType, c.options...); found && err == nil {
		c.mu.RUnlock()
		return c.blobStore.GetIoReader(ctx, key[:], fileType, c.options...)
	}

	// Upgrade to a write lock if the value is not found
	c.mu.RUnlock()
	c.mu.Lock()

	// Check again to avoid race conditions
	if found, err = c.blobStore.Exists(ctx, key[:], fileType, c.options...); found && err == nil {
		c.mu.Unlock()
		return c.blobStore.GetIoReader(ctx, key[:], fileType, c.options...)
	}

	// If not, check if there is an ongoing request
	if wg, found = c.wg[key]; found {
		c.mu.Unlock()
		wg.Wait() // Wait for the other goroutine to finish

		if found, err = c.blobStore.Exists(ctx, key[:], fileType, c.options...); found && err == nil {
			return c.blobStore.GetIoReader(ctx, key[:], fileType, c.options...)
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
	err = c.blobStore.SetFromReader(ctx, key[:], fileType, resultReader, c.options...)
	if err != nil {
		return nil, err
	}

	_ = resultReader.Close()

	// Return a reader to the cached blob
	return c.blobStore.GetIoReader(ctx, key[:], fileType, c.options...)
}
