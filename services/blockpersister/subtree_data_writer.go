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
type SubtreeDataWriter struct {
	// storer is the underlying file storer for writing to blob storage
	storer *filestorer.FileStorer

	// mu protects all fields below
	mu sync.Mutex

	// nextIndex is the next batch index we're waiting to write
	nextIndex int

	// pending stores batch data that has arrived but can't be written yet
	// because earlier batches haven't arrived
	pending map[int][][]byte

	// closed indicates if the writer has been closed
	closed bool

	// cond is used to signal when new batches arrive
	cond *sync.Cond

	// writeErr stores any error that occurred during writing
	writeErr error
}

// NewSubtreeDataWriter creates a new SubtreeDataWriter that wraps a FileStorer.
func NewSubtreeDataWriter(storer *filestorer.FileStorer) *SubtreeDataWriter {
	w := &SubtreeDataWriter{
		storer:    storer,
		nextIndex: 0,
		pending:   make(map[int][][]byte),
	}
	w.cond = sync.NewCond(&w.mu)
	return w
}

// WriteBatch writes a batch of serialized transaction bytes with its batch index.
// Batches can arrive out of order, but they will be written in sequential order.
// Returns an error if writing fails or if the writer has been closed.
func (w *SubtreeDataWriter) WriteBatch(batchIndex int, txBytes [][]byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.NewProcessingError("writer is closed")
	}

	if w.writeErr != nil {
		return w.writeErr
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
func (w *SubtreeDataWriter) Abort(err error) {
	w.mu.Lock()
	w.closed = true
	if err != nil && w.writeErr == nil {
		w.writeErr = err
	}
	w.mu.Unlock()

	// Abort the underlying storer to prevent incomplete file from being finalized
	w.storer.Abort(err)
}

// Close finalizes the writer, ensuring all pending batches have been written
// and closing the underlying FileStorer.
// If there are pending batches or write errors, Close will abort the underlying storer
// to prevent incomplete files from being finalized.
func (w *SubtreeDataWriter) Close(ctx context.Context) error {
	w.mu.Lock()

	if w.closed {
		w.mu.Unlock()
		return errors.NewProcessingError("writer already closed")
	}

	w.closed = true

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
