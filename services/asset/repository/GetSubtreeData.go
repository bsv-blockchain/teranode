// Package repository provides access to blockchain data storage and retrieval operations.
// It implements the necessary interfaces to interact with various data stores and
// blockchain clients.
package repository

import (
	"context"
	"io"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// semaphoreReadCloser wraps an io.ReadCloser and releases a semaphore permit when closed.
type semaphoreReadCloser struct {
	io.ReadCloser
	sem  *semaphore.Weighted
	once sync.Once
}

func (sr *semaphoreReadCloser) Close() error {
	err := sr.ReadCloser.Close()
	sr.once.Do(func() {
		releaseSemaphorePermit(sr.sem)
	})
	return err
}

// GetSubtreeDataReader retrieves the subtree data associated with the given subtree hash.
// It returns a PipeReader that can be used to read the subtree data as it is being streamed.
// The data is either retrieved from the block store or the subtree store, depending on availability.
//
// Parameters:
// - ctx: The context for managing cancellation and timeouts.
// - subtreeHash: The hash of the subtree to retrieve.
//
// Returns:
// - *io.PipeReader: A PipeReader that can be used to read the subtree data.
// - error: An error if the retrieval fails, or nil if successful.
func (repo *Repository) GetSubtreeDataReader(ctx context.Context, subtreeHash *chainhash.Hash) (io.ReadCloser, error) {
	if err := acquireSemaphorePermit(ctx, repo.semGetSubtreeDataReader, "GetSubtreeDataReader"); err != nil {
		return nil, err
	}
	// Note: semaphore will be released when the returned reader is closed

	subtreeDataExists, err := repo.SubtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
	if err == nil && subtreeDataExists {
		reader, err := repo.SubtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
		if err != nil {
			releaseSemaphorePermit(repo.semGetSubtreeDataReader)
			return nil, err
		}
		// Wrap reader to release semaphore when closed
		return &semaphoreReadCloser{
			ReadCloser: reader,
			sem:        repo.semGetSubtreeDataReader,
		}, nil
	}

	r, w := io.Pipe()
	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		// Release semaphore when goroutine completes (after all Aerospike reads are done)
		defer releaseSemaphorePermit(repo.semGetSubtreeDataReader)

		// write all transactions of the subtree to the pipe writer in streaming chunks to minimize memory usage
		if err := repo.writeTransactionsViaSubtreeStoreStreaming(gCtx, w, nil, subtreeHash); err != nil {
			_ = w.CloseWithError(io.ErrClosedPipe)
			_ = r.CloseWithError(err)

			return err
		}

		// close the writer after all subtrees have been streamed
		_ = w.CloseWithError(io.ErrClosedPipe)

		return nil
	})

	return r, nil
}
