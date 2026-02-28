// Package batcher provides a batching wrapper implementation for the blob.Store interface.
// It improves performance by aggregating multiple small blob operations into larger batches,
// which reduces overhead for storage backends with high per-operation costs (like network or disk I/O).
//
// The batcher works by collecting individual blob operations in memory until either:
// - The accumulated data reaches a configured size threshold
// - A background process flushes the batch after a timeout
//
// This implementation is particularly useful for high-throughput scenarios where many small
// blobs are being stored in rapid succession. By batching these operations, it significantly
// reduces the number of actual storage operations performed on the underlying store.
//
// Note that the batcher only supports write operations (Set). Read operations (Get, Exists)
// and metadata operations (SetDAH) are passed through to the underlying store.
package batcher

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"io"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	lockfreequeue "github.com/bsv-blockchain/go-lockfree-queue"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"golang.org/x/sync/errgroup"
)

// Batcher implements the blob.Store interface with batching capabilities.
// It aggregates multiple small blob operations into larger batches to improve performance
// when working with storage backends that have high per-operation costs.
type Batcher struct {
	// logger provides structured logging for batcher operations and errors
	logger ulogger.Logger
	// blobStore is the underlying blob storage implementation where batches are ultimately stored
	blobStore blobStoreSetter
	// sizeInBytes defines the maximum size of a batch in bytes before it's flushed
	sizeInBytes int
	// writeKeys determines whether to store a separate index of keys for each batch
	writeKeys bool
	// queue is a lock-free queue for storing batch items to be processed asynchronously
	queue *lockfreequeue.LockFreeQ[BatchItem]
	// done is the channel for signaling the background batch processing goroutine to stop
	done chan struct{}
	// notifyCh is used to notify the worker goroutine when new items are enqueued
	notifyCh chan struct{}
	// wg is used to wait for the background worker goroutine to complete during shutdown
	wg sync.WaitGroup
	// currentBatch holds the accumulated blob data for the current batch
	currentBatch []byte
	// currentBatchKeys holds the accumulated key data for the current batch (if writeKeys is true)
	currentBatchKeys []byte
}

// BatchItem represents a single blob operation to be included in a batch.
// It contains all the necessary information to store a blob in the underlying store.
type BatchItem struct {
	// hash is the unique identifier for the blob, typically a transaction ID or similar hash
	hash chainhash.Hash
	// fileType indicates the type of file being stored (e.g., transaction, block, etc.)
	fileType fileformat.FileType
	// value contains the actual blob data to be stored
	value []byte
	// next  atomic.Pointer[BatchItem]
}

// blobStoreSetter defines the minimal interface required for the underlying blob store.
// The batcher only needs to interact with a subset of the full blob.Store interface,
// specifically the Health check and Set operations.
type blobStoreSetter interface {
	// Health checks the health status of the underlying blob store
	Health(ctx context.Context, checkLiveness bool) (int, string, error)
	// Set stores a blob in the underlying store
	Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error
}

// New creates a new Batcher instance that wraps the provided blob store.
//
// The batcher improves performance by aggregating multiple small blob operations into larger batches,
// which reduces overhead for storage backends with high per-operation costs. It starts a background
// goroutine that processes queued items and flushes batches when they reach the configured size.
//
// Parameters:
//   - logger: Logger instance for batcher operations and error reporting
//   - blobStore: The underlying blob store where batches will be stored
//   - sizeInBytes: Maximum size of a batch in bytes before it's automatically flushed
//   - writeKeys: Whether to store a separate index of keys for each batch, enabling later retrieval by key
//
// Returns:
//   - *Batcher: A configured batcher instance ready to accept blob operations
func New(logger ulogger.Logger, blobStore blobStoreSetter, sizeInBytes int, writeKeys bool) *Batcher {
	b := &Batcher{
		logger:           logger,
		blobStore:        blobStore,
		sizeInBytes:      sizeInBytes,
		writeKeys:        writeKeys,
		queue:            lockfreequeue.NewLockFreeQ[BatchItem](),
		done:             make(chan struct{}),
		notifyCh:         make(chan struct{}, 1),
		currentBatch:     make([]byte, 0, sizeInBytes),
		currentBatchKeys: make([]byte, 0, sizeInBytes),
	}

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()

		var (
			batchItem *BatchItem
			err       error
		)

		for {
			// Try immediate dequeue (optimistic fast path)
			batchItem = b.queue.Dequeue()
			if batchItem != nil {
				err = b.processBatchItem(batchItem)
				if err != nil {
					b.logger.Errorf("error processing batch item: %v", err)
				}
				continue
			}

			// Queue is empty - wait for notification or shutdown
			select {
			case <-b.done:
				// Process remaining items before exiting
				for {
					batchItem = b.queue.Dequeue()
					if batchItem == nil {
						break
					}

					if err = b.processBatchItem(batchItem); err != nil {
						b.logger.Errorf("error processing batch item during shutdown: %v", err)
					}
				}
				// Write final batch if needed
				if len(b.currentBatch) > 0 {
					if err = b.writeBatch(b.currentBatch, b.currentBatchKeys); err != nil {
						b.logger.Errorf("error writing final batch during shutdown: %v", err)
					}
				}
				return

			case <-b.notifyCh:
				// Item available, loop back to dequeue
				continue
			}
		}
	}()

	return b
}

// processBatchItem handles a single batch item, adding it to the current batch.
// If adding the item would exceed the configured batch size limit, the current batch
// is first flushed to the underlying store. This method is called by the background
// processing goroutine for each item in the queue.
//
// Parameters:
//   - batchItem: The batch item to process, containing the blob data and metadata
//
// Returns:
//   - error: Any error that occurred during processing, particularly during batch flushing
func (b *Batcher) processBatchItem(batchItem *BatchItem) error {
	// check whether our batch would overflow the size limit, or is zero, which means we have 1 big transaction
	currentPos := len(b.currentBatch)
	dataSize := len(batchItem.value)

	if currentPos+dataSize > b.sizeInBytes && currentPos > 0 {
		if err := b.writeBatch(b.currentBatch, b.currentBatchKeys); err != nil {
			return errors.NewStorageError("error writing batch", err)
		}

		b.currentBatch = make([]byte, 0, b.sizeInBytes)
		b.currentBatchKeys = make([]byte, 0, b.sizeInBytes)
	}

	// add to batch
	b.currentBatch = append(b.currentBatch, batchItem.value...)

	if b.writeKeys {
		// keys are written as a separate batch, with the position and size as the first 8 bytes
		// followed by the key and a carriage return
		hashLength := len(batchItem.hash)
		key := make([]byte, hashLength+8)

		copy(key[:hashLength], batchItem.hash[:])

		currentPosUint32, err := safeconversion.IntToUint32(currentPos)
		if err != nil {
			return err
		}

		dataSizeUint32, err := safeconversion.IntToUint32(dataSize)
		if err != nil {
			return err
		}

		binary.BigEndian.PutUint32(key[hashLength:hashLength+4], currentPosUint32)
		binary.BigEndian.PutUint32(key[hashLength+4:hashLength+8], dataSizeUint32)

		hexKey := hex.EncodeToString(key)
		hexKey += "\n"

		// append the bytes of the key
		b.currentBatchKeys = append(b.currentBatchKeys, []byte(hexKey)...)
	}

	return nil
}

// writeBatch flushes the current batch to the underlying blob store.
// It generates a unique key for the batch based on the current time and random bytes,
// then writes both the batch data and (optionally) the batch keys to the underlying store.
// The batch keys provide an index that maps each original blob key to its position within
// the batch data, enabling potential future retrieval by key.
//
// Parameters:
//   - currentBatch: The accumulated blob data to write as a single batch
//   - batchKeys: The accumulated key data to write (if writeKeys is enabled)
//
// Returns:
//   - error: Any error that occurred during the write operation
func (b *Batcher) writeBatch(currentBatch []byte, batchKeys []byte) error {
	batchKey := make([]byte, 4)

	timeUint32, err := safeconversion.IntToUint32(int(time.Now().Unix()))
	if err != nil {
		return err
	}

	// add the current time as the first bytes
	binary.BigEndian.PutUint32(batchKey, timeUint32)
	// add a random string as the next bytes, to prevent conflicting filenames from other pods
	randBytes := make([]byte, 4)
	if _, err := rand.Read(randBytes); err != nil {
		return errors.NewStorageError("failed to generate random bytes for batch key", err)
	}
	batchKey = append(batchKey, randBytes...)

	g, gCtx := errgroup.WithContext(context.Background())

	// flush current batch
	g.Go(func() error {
		b.logger.Debugf("flushing batch of %d bytes", len(currentBatch))
		// we need to reverse the bytes of the key, since this is not a transaction ID
		if err := b.blobStore.Set(gCtx, util.ReverseSlice(batchKey), fileformat.FileTypeBatchData, currentBatch); err != nil {
			return errors.NewStorageError("error putting batch", err)
		}

		return nil
	})

	if b.writeKeys {
		// flush current batch keys
		g.Go(func() error {
			// we need to reverse the bytes of the key, since this is not a transaction ID, but a batch ID
			if err := b.blobStore.Set(gCtx, util.ReverseSlice(batchKey), fileformat.FileTypeBatchKeys, batchKeys); err != nil {
				return errors.NewStorageError("error putting batch keys", err)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

// Health checks the health status of the batcher and its underlying blob store.
// This method simply delegates to the underlying blob store's Health method,
// as the batcher's health is directly dependent on the health of the store it wraps.
//
// Parameters:
//   - ctx: Context for the health check operation
//   - checkLiveness: Whether to perform a more thorough liveness check
//
// Returns:
//   - int: HTTP status code indicating health status
//   - string: Description of the health status
//   - error: Any error that occurred during the health check
func (b *Batcher) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	// just pass the health of the underlying blob store
	return b.blobStore.Health(ctx, checkLiveness)
}

// Close shuts down the batcher, stopping the background processing goroutine
// and flushing any remaining items in the queue. This ensures that all pending
// operations are completed before the batcher is terminated.
//
// Parameters:
//   - ctx: Context for the close operation (unused in this implementation)
//
// Returns:
//   - error: Any error that occurred during shutdown
func (b *Batcher) Close(_ context.Context) error {
	// Signal the background goroutine to stop
	close(b.done)

	// Wake up the worker if it's blocked on notifyCh
	select {
	case b.notifyCh <- struct{}{}:
	default:
	}

	// Wait for the background goroutine to finish processing all remaining items
	b.wg.Wait()

	return nil
}

// SetFromReader reads data from an io.ReadCloser and queues it for batch processing.
// This method is useful for streaming large blobs directly from a source (like an HTTP request)
// without having to load the entire blob into memory first.
//
// Parameters:
//   - ctx: Context for the operation (unused in this implementation as processing is asynchronous)
//   - key: The key identifying the blob
//   - fileType: The type of the file
//   - reader: Reader providing the blob data
//   - opts: Optional file options (ignored in this implementation)
//
// Returns:
//   - error: Any error that occurred during reading or queueing
func (b *Batcher) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	defer reader.Close()

	bb, err := io.ReadAll(reader)
	if err != nil {
		return errors.NewStorageError("failed to read data from reader", err)
	}

	return b.Set(ctx, key, fileType, bb, opts...)
}

// Set queues a blob for batch processing. The blob is not immediately stored in the
// underlying blob store but is instead added to a queue for asynchronous processing.
// This allows the caller to continue execution without waiting for the actual storage
// operation to complete.
//
// Parameters:
//   - ctx: Context for the operation (unused as processing is asynchronous)
//   - hash: The key identifying the blob
//   - fileType: The type of the file
//   - value: The blob data to store
//   - opts: Optional file options (ignored in this implementation)
//
// Returns:
//   - error: Any error that occurred during queueing
func (b *Batcher) Set(_ context.Context, hash []byte, fileType fileformat.FileType, value []byte, _ ...options.FileOption) error {
	b.queue.Enqueue(BatchItem{
		hash:     chainhash.Hash(hash),
		fileType: fileType,
		value:    value,
	})

	// Notify worker that new item is available (non-blocking)
	select {
	case b.notifyCh <- struct{}{}:
	default: // Already notified, don't block
	}

	return nil
}

// SetDAH is not supported by the batcher implementation.
// The batcher is designed primarily for efficient write operations and does not
// support metadata operations like setting Delete-At-Height values.
//
// Parameters:
//   - ctx: Context for the operation (unused)
//   - key: The key identifying the blob (unused)
//   - fileType: The type of the file (unused)
//   - dah: The delete at height value (unused)
//   - opts: Optional file options (unused)
//
// Returns:
//   - error: Always returns go-errors.NewStorageError with an unsupported operation message
func (b *Batcher) SetDAH(_ context.Context, _ []byte, _ fileformat.FileType, _ uint32, _ ...options.FileOption) error {
	return errors.NewStorageError("SetDAH not supported by batcher")
}

// GetIoReader is not supported by the batcher implementation.
// The batcher is designed primarily for efficient write operations and does not
// support read operations like retrieving blob data.
//
// Parameters:
//   - ctx: Context for the operation (unused)
//   - key: The key identifying the blob (unused)
//   - fileType: The type of the file (unused)
//   - opts: Optional file options (unused)
//
// Returns:
//   - io.ReadCloser: Always returns nil
//   - error: Always returns go-errors.NewStorageError with an unsupported operation message
func (b *Batcher) GetIoReader(_ context.Context, _ []byte, _ fileformat.FileType, _ ...options.FileOption) (io.ReadCloser, error) {
	return nil, errors.NewStorageError("GetIoReader not supported by batcher")
}

// Get is not supported by the batcher implementation.
// The batcher is designed primarily for efficient write operations and does not
// support read operations like retrieving blob data.
//
// Parameters:
//   - ctx: Context for the operation (unused)
//   - key: The key identifying the blob (unused)
//   - fileType: The type of the file (unused)
//   - opts: Optional file options (unused)
//
// Returns:
//   - []byte: Always returns nil
//   - error: Always returns go-errors.NewStorageError with an unsupported operation message
func (b *Batcher) Get(_ context.Context, _ []byte, _ fileformat.FileType, _ ...options.FileOption) ([]byte, error) {
	return nil, errors.NewStorageError("Get not supported by batcher")
}

// Exists is not supported by the batcher implementation.
// The batcher is designed primarily for efficient write operations and does not
// support query operations like checking for blob existence.
//
// Parameters:
//   - ctx: Context for the operation (unused)
//   - key: The key identifying the blob (unused)
//   - fileType: The type of the file (unused)
//   - opts: Optional file options (unused)
//
// Returns:
//   - bool: Always returns false
//   - error: Always returns go-errors.NewStorageError with an unsupported operation message
func (b *Batcher) Exists(_ context.Context, _ []byte, _ fileformat.FileType, _ ...options.FileOption) (bool, error) {
	return false, errors.NewStorageError("Exists not supported by batcher")
}

// Del is not supported by the batcher implementation.
// The batcher is designed primarily for efficient write operations and does not
// support deletion operations.
//
// Parameters:
//   - ctx: Context for the operation (unused)
//   - key: The key identifying the blob (unused)
//   - fileType: The type of the file (unused)
//   - opts: Optional file options (unused)
//
// Returns:
//   - error: Always returns go-errors.NewStorageError with an unsupported operation message
func (b *Batcher) Del(_ context.Context, _ []byte, _ fileformat.FileType, _ ...options.FileOption) error {
	return errors.NewStorageError("Del not supported by batcher")
}

// SetCurrentBlockHeight is a no-op in the batcher implementation.
// The batcher does not implement Delete-At-Height functionality, so it ignores
// block height updates.
//
// Parameters:
//   - height: The current block height (ignored)
func (b *Batcher) SetCurrentBlockHeight(_ uint32) {
	// noop
}
