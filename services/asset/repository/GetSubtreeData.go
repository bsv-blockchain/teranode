// Package repository provides access to blockchain data storage and retrieval operations.
// It implements the necessary interfaces to interact with various data stores and
// blockchain clients.
package repository

import (
	"context"
	"io"

	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"golang.org/x/sync/errgroup"
)

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
	subtreeDataExists, err := repo.SubtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
	if err == nil && subtreeDataExists {
		return repo.SubtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
	}

	r, w := io.Pipe()
	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		if err := repo.writeTransactionsViaSubtreeStore(gCtx, w, nil, subtreeHash); err != nil {
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
