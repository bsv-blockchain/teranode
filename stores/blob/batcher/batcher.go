// Package batcher provides a batching wrapper implementation for the blob.Store interface.
// It improves performance by aggregating multiple small blob operations into larger batches,
// which reduces overhead for storage backends with high per-operation costs (like network or disk I/O).
//
// The batcher works by collecting individual blob operations in memory and flushing the
// accumulated batch to the underlying store only when either:
// - Adding the next item would overflow the configured size threshold,
// - Adding the next item would push its offset past the uint32 a key record addresses it with (writeKeys only), or
// - The batcher is closed (a final flush of whatever remains).
//
// There is no background timer or ticker: under a low write rate, data queued via Set sits
// in memory, unflushed, until either the size threshold is reached or Close is called.
//
// This implementation is particularly useful for high-throughput scenarios where many small
// blobs are being stored in rapid succession. By batching these operations, it significantly
// reduces the number of actual storage operations performed on the underlying store.
//
// Note that the batcher only supports write operations (Set, SetFromReader). Read and query
// operations (Get, GetIoReader, Exists), deletion (Del) and metadata operations (SetDAH) are
// NOT passed through to the underlying store: they return an unsupported-operation error.
// SetCurrentBlockHeight is a no-op, so a wrapped store never sees block-height updates and
// its Delete-At-Height bookkeeping stops advancing. Nothing in this repository reads back the
// batch-data / batch-keys files this package writes, so a batched store is write-only in the
// strongest sense: the data goes in and there is no supported way to get it out again.
//
// The caller's file type is also discarded. Every flush is written as FileTypeBatchData, and a
// key record carries only the hash, offset and size, so a batch that mixes (say) subtree and
// transaction blobs cannot be told apart afterwards even in principle.
package batcher

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"io"
	"math"
	"sync"
	"sync/atomic"
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
	// currentBatch holds the accumulated blob data for the current batch. Only the
	// background worker goroutine ever reads or writes it.
	currentBatch []byte
	// currentBatchKeys holds the accumulated key data for the current batch (if writeKeys is true)
	currentBatchKeys []byte
	// currentBatchLen mirrors len(currentBatch), updated by the worker goroutine after every
	// append. It is deliberately not updated when an overflow flush empties the buffer, so a Close landing
	// between a successful flush and the following append reports the pre-flush length; the value
	// is approximate by nature and is only ever used for a log line and an error message.
	// Close reads it from the caller's goroutine while the worker may still be running,
	// so it must not read currentBatch directly (that would race); this atomic mirror is the
	// safe way to report an approximate unflushed-byte count in that situation.
	currentBatchLen atomic.Int64
	// closeOnce guards the shutdown sequence so it runs exactly once. Close is fallible, which
	// invites a caller that sees an error to retry it; without this, the second call would panic
	// on close of an already-closed done channel.
	closeOnce sync.Once
	// closeErr memoises the outcome of that single shutdown sequence, so repeat callers get the
	// same answer the first caller got. sync.Once establishes the happens-before that makes
	// reading it from another goroutine safe.
	closeErr error
	// closed is set as soon as shutdown begins, so Set can reject writes that would otherwise be
	// enqueued behind a worker that is already draining or has exited.
	closed atomic.Bool
	// shutdownFlushErr carries the final flush's error from the worker goroutine back to Close.
	// It is a pointer so that "flushed fine" and "has not run yet" remain distinguishable.
	shutdownFlushErr atomic.Pointer[error]
}

// BatchItem represents a single blob operation to be included in a batch.
// It contains all the necessary information to store a blob in the underlying store.
type BatchItem struct {
	// hash is the unique identifier for the blob, typically a transaction ID or similar hash
	hash chainhash.Hash
	// fileType indicates the type of file being stored (e.g., transaction, block, etc.).
	// It is recorded here for completeness but never persisted: batches are written as
	// FileTypeBatchData and key records carry no discriminator (see the package comment).
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
//   - sizeInBytes: Maximum size of a batch in bytes before it's automatically flushed.
//     Must be greater than 0. The buffers are preallocated with this capacity, so a
//     negative value panics in make(); zero does not panic but degenerates the batcher
//     into flushing after every single item, which defeats the point of wrapping the
//     store at all. createBatchedStore rejects any non-positive value before it reaches
//     here, so any other caller has to do the same.
//   - writeKeys: Whether to store a separate index of keys and their offsets for each batch.
//     This only helps an external consumer that parses the batch-keys format; nothing in this
//     repository reads it back (see the package comment).
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
						// Hand the failure to Close rather than only logging it: the caller is
						// being told its store shut down cleanly, and these bytes are gone.
						// Copy first, so the stored pointer does not alias the loop's err.
						finalErr := err
						b.shutdownFlushErr.Store(&finalErr)
					} else {
						b.currentBatchLen.Store(0)
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

// exceedsKeyRecordSize reports whether a value is too large for a key record to describe.
// A key record stores its item's size as a uint32, so a larger value could never be indexed
// no matter how the batch is flushed. Set refuses such a value synchronously rather than
// accepting it and discarding it in the worker, by which point its caller has long since
// been told the write was queued.
//
// Without writeKeys no key records are produced and no such limit applies. This is split out
// from Set so the arithmetic can be tested directly: reaching the limit through Set would
// need a 4 GiB value. On a platform where int is 32 bits the comparison is simply never
// true, since no slice can be that long.
//
// Parameters:
//   - valueLen: Length of the value being written
//   - writeKeys: Whether key records, and therefore uint32 sizes, are being written
//
// Returns:
//   - bool: Whether the value is too large to be addressed by a key record
func exceedsKeyRecordSize(valueLen int, writeKeys bool) bool {
	return writeKeys && int64(valueLen) > math.MaxUint32
}

// shouldFlushBefore reports whether the accumulated batch has to be written out before an
// item of dataSize bytes can be appended to it.
//
// Two things force a flush. The obvious one is the configured size limit. The second is the
// uint32 offset that each key record uses to address its item: a batch is retained across
// flush failures, so it can grow past that ceiling, and the conversion in processBatchItem
// would then fail with the item already dequeued and its caller long since told the write
// was queued. Flushing first resets the offset to zero, so the item is appended normally
// instead of being dropped. The ceiling only binds when key records are actually written.
//
// An empty batch is never flushed: there is nothing to write, and an item larger than the
// whole batch size is appended on its own and flushed by the next Set or by Close.
//
// Parameters:
//   - currentPos: Current length of the accumulated batch, and the offset the item would take
//   - dataSize: Length of the item about to be appended
//   - sizeInBytes: Configured maximum batch size
//   - writeKeys: Whether key records, and therefore uint32 offsets, are being written
//
// Returns:
//   - bool: Whether the batch must be flushed before appending the item
func shouldFlushBefore(currentPos, dataSize, sizeInBytes int, writeKeys bool) bool {
	if currentPos == 0 {
		return false
	}

	if currentPos+dataSize > sizeInBytes {
		return true
	}

	return writeKeys && int64(currentPos)+int64(dataSize) > math.MaxUint32
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

	var flushErr error

	if shouldFlushBefore(currentPos, dataSize, b.sizeInBytes, b.writeKeys) {
		flushErr = b.writeBatch(b.currentBatch, b.currentBatchKeys)

		// Only reset the batch (and the position the item is about to be keyed against) if the
		// flush actually succeeded. If it failed, the item below is appended to the batch that
		// just failed to flush rather than being dropped, and its key is recorded at its real
		// position within that still-unflushed batch. This deliberately lets the in-memory batch
		// grow across repeated flush failures against a dead backend, rather than silently
		// discarding the bytes the caller was told were queued: an unbounded-growth risk that
		// would need a new setting to cap is preferable to data loss, and is out of scope here.
		if flushErr == nil {
			b.currentBatch = make([]byte, 0, b.sizeInBytes)
			b.currentBatchKeys = make([]byte, 0, b.sizeInBytes)
			currentPos = 0
		}
	}

	// The key record is built before the value is appended, not after. Both currentPos and
	// dataSize are already known here, and building it first means a failed conversion cannot
	// return with the value already sitting in the batch data and no index entry addressing
	// it.
	//
	// shouldFlushBefore above keeps currentPos under the uint32 ceiling whenever a flush can
	// succeed, and Set rejects a value too large to be addressed by one, so the conversions
	// below only fail in one remaining case: the backend is dead, the retained batch has
	// already grown past 4 GiB, and the flush that would have reset it failed too. That batch
	// is unwritable either way — Close reports it as lost — so dropping one further item
	// changes nothing that was still recoverable.
	var keyRecord []byte

	if b.writeKeys {
		// keys are written as a separate batch, with the position and size as the first 8 bytes
		// followed by the key and a carriage return
		hashLength := len(batchItem.hash)
		key := make([]byte, hashLength+8)

		copy(key[:hashLength], batchItem.hash[:])

		currentPosUint32, err := safeconversion.IntToUint32(currentPos)
		if err != nil {
			// Carry flushErr when there is one. This path is only reached after a failed flush,
			// so returning the conversion error alone would lose the "error writing batch"
			// signal that tells the worker why the backend is unhealthy in the first place.
			if flushErr != nil {
				return errors.NewStorageError("error writing batch, and item at offset %d cannot be addressed in a key record", currentPos, flushErr)
			}

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

		keyRecord = []byte(hexKey)
	}

	// add to batch, regardless of whether the flush above succeeded or failed
	b.currentBatch = append(b.currentBatch, batchItem.value...)
	// mirror the new length for Close, which reads it from a different goroutine
	// and must not touch currentBatch itself (see the field comment)
	b.currentBatchLen.Store(int64(len(b.currentBatch)))

	if keyRecord != nil {
		b.currentBatchKeys = append(b.currentBatchKeys, keyRecord...)
	}

	if flushErr != nil {
		return errors.NewStorageError("error writing batch", flushErr)
	}

	return nil
}

// writeBatch flushes the current batch to the underlying blob store.
// It generates a unique key for the batch based on the current time and random bytes,
// then writes both the batch data and (optionally) the batch keys to the underlying store.
// The batch keys provide an index that maps each original blob key to its position within
// the batch data, enabling potential future retrieval by key.
//
// The two writes are independent Set calls issued concurrently, so a partial failure is
// possible: one lands, the other does not, and writeBatch reports an error. Since a failed
// flush now retains the batch, the next successful flush writes those same bytes again under
// a freshly generated batchKey. The store is then left holding whichever half of the first
// attempt succeeded, addressed by a key nothing else refers to, plus a duplicate of its
// contents. Nothing reads either file type back today, so this costs space rather than
// correctness; making it atomic would need a two-phase write.
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
// The wait for the shutdown flush is bounded by ctx: if ctx is cancelled or its
// deadline expires first, Close abandons the wait and returns an error rather than
// blocking forever. This matters because daemon.closeStores gives every store a
// shared budget (e.g. 30s) and closes them SEQUENTIALLY, so one store hung here
// would otherwise also block the blockchain state store's orderly close later in
// that same list. Abandoning is the right trade-off, not just an expedient one:
// the unflushed data is memory-only, held against a backend that is already stuck,
// so waiting longer does not make it any safer, while every second spent waiting
// is a second stolen from the stores that close after this one. Note that
// daemon.closeStores currently discards the error this method returns, so the log
// line below is the only operator-visible signal that a flush was abandoned.
//
// Close is idempotent and safe to call more than once. Because it can fail, a caller
// that sees an error is quite likely to retry it; the shutdown sequence therefore runs
// under a sync.Once and later calls return the first call's result. Note that this
// result is a snapshot: if the first call abandoned a hung flush that later completed,
// a second call still reports the abandonment.
//
// Two consequences of abandoning are worth stating rather than leaving to be discovered.
// The worker goroutine and the goroutine waiting on it are not killed, so both leak for
// the remaining life of the process, and the worker may call the wrapped store's Set well
// after the daemon considers this store closed. Both are acceptable on a shutdown path
// that is about to exit anyway, and neither is fixable without a way to interrupt a Set
// that is already in flight.
//
// Close also does not close the store it wraps: blobStoreSetter exposes only Health and
// Set. Since createBatchedStore returns the batcher in place of the real store, nothing
// closes the wrapped store at all when batch=true, so any cleanup it does on Close is
// skipped.
//
// Pass a context that still has budget left. A select among ready cases picks at random, so
// an already-expired ctx can take the cancellation branch on the very first poll and abandon
// the final batch without the worker getting a chance to flush it at all. daemon.closeStores
// builds a fresh budget for exactly this reason; a caller that instead reuses a shutdown
// context already drained by earlier work would silently discard whatever is still batched.
//
// Parameters:
//   - ctx: Context bounding how long Close will wait for the shutdown flush
//
// Returns:
//   - error: NewContextCanceledError if ctx expires before the shutdown flush completes,
//     or NewStorageError if the shutdown flush ran but failed to write
func (b *Batcher) Close(ctx context.Context) error {
	b.closeOnce.Do(func() {
		b.closeErr = b.shutdown(ctx)
	})

	return b.closeErr
}

// shutdown performs the one-shot teardown behind Close's sync.Once.
func (b *Batcher) shutdown(ctx context.Context) error {
	// Reject any further writes before draining, so a Set racing this teardown is told
	// the store is closed instead of parking an item in a queue nobody will read again.
	b.closed.Store(true)

	// Signal the background goroutine to stop
	close(b.done)

	// Wake up the worker if it's blocked on notifyCh
	select {
	case b.notifyCh <- struct{}{}:
	default:
	}

	// Wait for the background goroutine to finish processing all remaining items, but don't
	// wait past ctx: run the wait on a goroutine so we can select it against ctx.Done().
	waitDone := make(chan struct{})

	go func() {
		b.wg.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		// The worker has returned, so wg.Wait() orders its writes before these reads.
		// A final flush that errored has to surface here: returning nil would tell the
		// caller the store closed cleanly while the batch it was holding was discarded,
		// which is the same defect on the shutdown path that this type fixes on the
		// overflow path.
		if flushErr := b.shutdownFlushErr.Load(); flushErr != nil {
			unflushed := b.currentBatchLen.Load()
			return errors.NewStorageError("batcher shutdown flush failed, %d bytes not written", unflushed, *flushErr)
		}

		return nil
	case <-ctx.Done():
		// currentBatch itself is only safe to read from the worker goroutine, which may still
		// be running at this point; currentBatchLen is the atomic mirror kept for this purpose.
		unflushed := b.currentBatchLen.Load()
		b.logger.Errorf("batcher close abandoned shutdown flush of %d unflushed bytes: %v", unflushed, ctx.Err())

		return errors.NewContextCanceledError("batcher close abandoned shutdown flush of %d unflushed bytes", unflushed, ctx.Err())
	}
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
// Set does not copy value. It enqueues the caller's slice and the worker appends it to the
// batch later, so the batcher takes ownership of those bytes until the batch is flushed:
// mutating or reusing the buffer after Set returns corrupts the batch. Every other blob
// store is synchronous and copies before returning, so this is the one backend where that
// is unsafe. Copying here instead would add a full copy per item to the only path this type
// exists to make cheap.
//
// After Close, Set returns an error rather than accepting a write it cannot honour: the
// worker goroutine has stopped, so an item enqueued then would sit in the queue forever
// while its caller was told the write had been accepted. A Set running concurrently with
// Close can still slip through this check and be dropped — closing that window would mean
// serialising the write path, which would cost more than this dormant path is worth. Do
// not call Set concurrently with Close and rely on the outcome. Such an item is worse than
// dropped: the queue is unbounded and the worker has already returned, so nothing will ever
// dequeue it and its bytes stay resident for the remaining life of the process.
//
// Returns:
//   - error: Any error that occurred during queueing, or if the batcher has been closed
func (b *Batcher) Set(_ context.Context, hash []byte, fileType fileformat.FileType, value []byte, _ ...options.FileOption) error {
	if len(hash) != chainhash.HashSize {
		return errors.NewInvalidArgumentError("hash must be %d bytes, got %d", chainhash.HashSize, len(hash))
	}

	if b.closed.Load() {
		return errors.NewStorageError("batcher is closed, write not accepted")
	}

	if exceedsKeyRecordSize(len(value), b.writeKeys) {
		return errors.NewInvalidArgumentError("value of %d bytes cannot be indexed in a batch key record, limit is %d", len(value), int64(math.MaxUint32))
	}

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
