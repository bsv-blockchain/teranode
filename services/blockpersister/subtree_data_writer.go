// Package blockpersister provides comprehensive functionality for persisting blockchain blocks and their associated data.
// It handles block persistence, transaction processing, and UTXO set management across multiple storage backends.
package blockpersister

import (
	"context"
	"sync"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/utxopersister/filestorer"
)

// SubtreeDataWriter handles streaming writes of subtree transaction data with ordered batch processing.
// It implements a fan-in pattern where multiple batches can be processed concurrently,
// but writes occur in strict sequential order (batch 0, then 1, then 2, etc.).
//
// This ensures that transaction data is written to disk in the exact order required
// by the subtreeData format while allowing parallel batch fetching and processing.
//
// A batch that arrives before the batches ahead of it is parked in pending until they have been
// written. The window bounds how far ahead that can be: WriteBatch admits batches nextIndex up to
// nextIndex+window-1, and a batch beyond that waits until the batches before it are written. So
// pending never holds more than window batches. That bounds pending only: a waiting batch still
// holds its txBytes on its own goroutine, so the bytes held at once also count the waiters (see
// NewSubtreeDataWriter). Without the bound, one slow batch lets every later batch park, and
// pending grows to the whole subtree.
type SubtreeDataWriter struct {
	// storer is the underlying file storer for writing to blob storage
	storer *filestorer.FileStorer

	// window is how many batches, counting from nextIndex, WriteBatch admits
	window int

	// stopCancelWake unregisters the context.AfterFunc that wakes waiting batches when the
	// writer's context is cancelled
	stopCancelWake func() bool

	// mu protects all fields below
	mu sync.Mutex

	// nextIndex is the next batch index we're waiting to write
	nextIndex int

	// pending stores batch data that has arrived but can't be written yet
	// because earlier batches haven't arrived
	pending map[int][][]byte

	// closed indicates if the writer has been closed
	closed bool

	// cond wakes the batches waiting for the window to reach them. It is broadcast when nextIndex
	// advances and when the writer stops: a write fails, the writer is closed or aborted, or its
	// context is cancelled.
	cond *sync.Cond

	// writeErr stores any error that occurred during writing
	writeErr error

	// cancelErr is the writer's context error once the context is cancelled. Only a batch
	// waiting on the window reads it: an errgroup cancels its context when Wait returns, on
	// success too, so it says nothing about a writer whose batches have all arrived.
	cancelErr error
}

// NewSubtreeDataWriter creates a new SubtreeDataWriter that wraps a FileStorer and admits window
// batches ahead of the next one to write. A window below 1 is treated as 1.
//
// A batch beyond the window waits in WriteBatch, holding whatever its caller holds, so the caller
// must make sure batch nextIndex is always on its way. CreateSubtreeDataFileStreaming does, with
// any window and any errgroup limit, 1 included. It dispatches batches in index order from one
// loop, and the errgroup's Go blocks only until a slot is free. Take nextIndex = n. If any batch
// above n has started, batch n started before it. If none has, the only batches that can hold a
// slot are those below n, which have been written, so their calls have returned or are about to,
// and the dispatch of batch n gets a slot. Batch n itself never waits, since n < n+window. It
// either reaches WriteBatch, which writes it, advances nextIndex and wakes the waiters, or fails.
//
// A batch that fails never reaches WriteBatch, so nextIndex stops at it, and only ctx can wake
// the batches waiting behind it. ctx must therefore be cancelled when any batch fails, as an
// errgroup's context is. Without that, they wait forever and the errgroup's Wait never returns.
// A waiting batch woken by ctx returns a context-canceled error; the errgroup reports the first
// error, the failed batch's own.
//
// A waiting batch keeps its errgroup slot. CreateSubtreeDataFileStreaming passes its errgroup
// limit as the window. With any smaller window, batches that finish slightly out of order would
// wait while batch nextIndex is still running, and fetching would stall. With window equal to the
// limit, every running batch is inside the window as long as pending is empty, so a call waits
// only when batches complete out of order, and pending then holds at most limit-1 batches.
// pending is not all that is held, though: each batch waiting on the window or still fetching
// holds its own txBytes, so the serialized bytes held at once come to about 2*limit-1 batches.
func NewSubtreeDataWriter(ctx context.Context, storer *filestorer.FileStorer, window int) *SubtreeDataWriter {
	w := &SubtreeDataWriter{
		storer:    storer,
		window:    max(window, 1),
		nextIndex: 0,
		pending:   make(map[int][][]byte),
	}
	w.cond = sync.NewCond(&w.mu)

	// The broadcast is made under the lock. A waiter checks cancelErr and enters cond.Wait under
	// the same lock, so a cancellation can never fall between the two and go unseen.
	w.stopCancelWake = context.AfterFunc(ctx, func() {
		w.mu.Lock()
		w.cancelErr = ctx.Err()
		w.cond.Broadcast()
		w.mu.Unlock()
	})

	return w
}

// WriteBatch writes a batch of serialized transaction bytes with its batch index.
// Batches can arrive out of order, but they will be written in sequential order.
// A batch window or more ahead of the next one to write waits until the batches before it
// are written (see NewSubtreeDataWriter).
// Returns an error if writing fails, if the writer has been closed, or if the writer's context
// is cancelled while the batch waits.
func (w *SubtreeDataWriter) WriteBatch(batchIndex int, txBytes [][]byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Every wake-up re-checks why the writer may have stopped before checking the window again.
	for {
		if w.closed {
			return errors.NewProcessingError("writer is closed")
		}

		if w.writeErr != nil {
			return w.writeErr
		}

		if batchIndex < w.nextIndex+w.window {
			break
		}

		if w.cancelErr != nil {
			return errors.NewContextCanceledError("[SubtreeDataWriter] batch %d cancelled while waiting for batch %d", batchIndex, w.nextIndex, w.cancelErr)
		}

		w.cond.Wait()
	}

	// Store this batch in pending
	w.pending[batchIndex] = txBytes

	// Try to write all sequential batches starting from nextIndex
	for {
		batch, exists := w.pending[w.nextIndex]
		if !exists {
			// Next batch hasn't arrived yet, stop writing
			break
		}

		// Write all transactions in this batch
		for _, txData := range batch {
			if _, err := w.storer.Write(txData); err != nil {
				w.writeErr = errors.NewStorageError("error writing transaction data", err)
				w.cond.Broadcast()
				return w.writeErr
			}
		}

		// Remove from pending and move to next batch
		delete(w.pending, w.nextIndex)
		w.nextIndex++
	}

	// Signal that we processed some batches
	w.cond.Broadcast()

	return nil
}

// Abort signals that the write operation should be abandoned and the temp file should not be finalized.
// It marks the writer as closed and aborts the underlying FileStorer.
// This should be called instead of Close() when an error occurs during batch processing.
// Batches waiting on the window return an error.
func (w *SubtreeDataWriter) Abort(err error) {
	w.stopCancelWake()

	w.mu.Lock()
	w.closed = true
	if err != nil && w.writeErr == nil {
		w.writeErr = err
	}
	w.cond.Broadcast()
	w.mu.Unlock()

	// Abort the underlying storer to prevent incomplete file from being finalized
	w.storer.Abort(err)
}

// Close finalizes the writer, ensuring all pending batches have been written
// and closing the underlying FileStorer.
// If there are pending batches or write errors, Close will abort the underlying storer
// to prevent incomplete files from being finalized.
// It never consults the writer's context, which the errgroup cancels as soon as its Wait returns.
func (w *SubtreeDataWriter) Close(ctx context.Context) error {
	w.stopCancelWake()

	w.mu.Lock()

	if w.closed {
		w.mu.Unlock()
		return errors.NewProcessingError("writer already closed")
	}

	w.closed = true
	w.cond.Broadcast()

	// Check if there are any pending batches that shouldn't be there
	if len(w.pending) > 0 {
		w.mu.Unlock()
		// Abort the storer to prevent incomplete file from being finalized
		w.storer.Abort(errors.NewProcessingError("writer closed with %d pending batches", len(w.pending)))
		return errors.NewProcessingError("writer closed with %d pending batches (expected batch %d)", len(w.pending), w.nextIndex)
	}

	if w.writeErr != nil {
		err := w.writeErr
		w.mu.Unlock()
		// Abort the storer to prevent incomplete file from being finalized
		w.storer.Abort(err)
		return err
	}

	w.mu.Unlock()

	// Close the underlying storer (this will flush buffers)
	return w.storer.Close(ctx)
}
