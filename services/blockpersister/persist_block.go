// Package blockpersister provides comprehensive functionality for persisting blockchain blocks and their associated data.
// It ensures reliable storage of blocks, transactions, and UTXO set changes to maintain blockchain data integrity.
package blockpersister

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/services/utxopersister/filestorer"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

// persistBlock stores a block and its associated data to persistent storage.
//
// This is a core function of the blockpersister service that handles the complete persistence
// workflow for a single block. It ensures all components of a block (header, transactions,
// and UTXO changes) are properly stored in a consistent and recoverable manner.
//
// The function implements a multi-stage persistence process:
//  1. Convert raw block bytes into a structured block model
//  2. Create a new UTXO difference set for tracking changes
//  3. Process the coinbase transaction if no subtrees are present
//  4. For blocks with subtrees, process each subtree concurrently according to configured limits
//  5. Close and finalize the UTXO difference set once all transactions are processed
//  6. Write the complete block to persistent storage
//
// Concurrency is managed through errgroup with configurable parallel processing limits
// to optimize performance while avoiding resource exhaustion.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and tracing
//   - hash: Hash identifier of the block to persist
//   - blockBytes: Raw serialized bytes of the complete block
//
// Returns an error if any part of the persistence process fails. The error will be wrapped
// with appropriate context to identify the specific failure point.
//
// Note: Block persistence is atomic - if any part fails, the entire operation is considered
// failed and should be retried after resolving the underlying issue.
func (u *Server) persistBlock(ctx context.Context, hash *chainhash.Hash, blockBytes []byte) error {
	ctx, _, deferFn := tracing.Tracer("blockpersister").Start(ctx, "persistBlock",
		tracing.WithHistogram(prometheusBlockPersisterPersistBlock),
		tracing.WithLogMessage(u.logger, "[persistBlock] called for block %s", hash.String()),
	)
	defer deferFn()

	block, err := model.NewBlockFromBytes(blockBytes)
	if err != nil {
		return errors.NewProcessingError("error creating block from bytes", err)
	}

	u.logger.Infof("[BlockPersister] Processing block %s (%d subtrees)...", block.Header.Hash().String(), len(block.Subtrees))

	concurrency := u.settings.Block.BlockPersisterConcurrency

	// In all-in-one mode, reduce concurrency to avoid resource starvation across multiple services
	if u.settings.IsAllInOneMode {
		concurrency = concurrency / 2
		if concurrency < 1 {
			concurrency = 1 // Ensure at least 1
		}
		u.logger.Infof("[BlockPersister] All-in-one mode detected: reducing concurrency to %d", concurrency)
	}

	u.logger.Infof("[BlockPersister] Processing subtrees with concurrency %d", concurrency)

	// Only process UTXO files if enabled
	var utxoDiff *utxopersister.UTXOSet
	if u.settings.Block.BlockPersisterProcessUTXOFiles {
		// Create a new UTXO diff
		var err error
		utxoDiff, err = utxopersister.NewUTXOSet(ctx, u.logger, u.settings, u.blockStore, block.Header.Hash(), block.Height)
		if err != nil {
			return errors.NewProcessingError("error creating utxo diff", err)
		}

		defer func() {
			if closeErr := utxoDiff.Close(); closeErr != nil {
				u.logger.Warnf("[persistBlock] error closing utxoDiff during error cleanup: %v", closeErr)
			}
		}()
	} else {
		u.logger.Infof("[BlockPersister] UTXO file processing disabled - skipping utxo-additions and utxo-deletions files")
	}

	if len(block.Subtrees) == 0 {
		// No subtrees to process, just write the coinbase UTXO to the diff and continue
		if utxoDiff != nil {
			if err := utxoDiff.ProcessTx(block.CoinbaseTx); err != nil {
				return errors.NewProcessingError("error processing coinbase tx", err)
			}
		}
	} else {
		g, gCtx := errgroup.WithContext(ctx)
		util.SafeSetLimit(g, concurrency)

		for i, subtreeHash := range block.Subtrees {
			subtreeHash := subtreeHash
			i := i

			g.Go(func() error {
				u.logger.Infof("[BlockPersister] processing subtree %d / %d [%s]", i+1, len(block.Subtrees), subtreeHash.String())

				return u.ProcessSubtree(gCtx, *subtreeHash, block.CoinbaseTx, utxoDiff)
			})
		}

		u.logger.Infof("[BlockPersister] writing UTXODiff for block %s", block.Header.Hash().String())

		if err = g.Wait(); err != nil {
			// Don't wrap the error again, ProcessSubtree should return the error in correct format
			return err
		}
	}

	// Now, write the block file
	u.logger.Infof("[BlockPersister] Writing block %s to disk", block.Header.Hash().String())

	storer, err := filestorer.NewFileStorer(ctx, u.logger, u.settings, u.blockStore, hash[:], fileformat.FileTypeBlock)
	if err != nil {
		return errors.NewStorageError("error creating block file", err)
	}

	defer func() {
		if closeErr := storer.Close(ctx); closeErr != nil {
			u.logger.Warnf("[persistBlock] error closing storer: %v", closeErr)
		}
	}()

	if _, err = storer.Write(blockBytes); err != nil {
		return errors.NewStorageError("error writing block to disk", err)
	}

	return nil
}
