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

	u.logger.Infof("[persistBlock][%s] Processing block (%d subtrees)...", block.String(), len(block.Subtrees))

	concurrency := u.settings.BlockPersister.Concurrency

	// In all-in-one mode, reduce concurrency to avoid resource starvation across multiple services
	if u.settings.IsAllInOneMode {
		concurrency = concurrency / 2
		if concurrency < 1 {
			concurrency = 1 // Ensure at least 1
		}
		u.logger.Infof("[persistBlock][%s] All-in-one mode detected: reducing concurrency to %d", block.String(), concurrency)
	}

	u.logger.Infof("[persistBlock][%s] Processing subtrees with concurrency %d", block.String(), concurrency)

	// Only process UTXO files if enabled
	var utxoDiff *utxopersister.UTXOSet
	if u.settings.BlockPersister.ProcessUTXOFiles {
		// Create a new UTXO diff
		var err error
		utxoDiff, err = utxopersister.NewUTXOSet(ctx, u.logger, u.settings, u.blockStore, block.Header.Hash(), block.Height)
		if err != nil {
			return errors.NewProcessingError("error creating utxo diff", err)
		}

		defer func() {
			if closeErr := utxoDiff.Close(); closeErr != nil {
				u.logger.Warnf("[persistBlock][%s] error closing utxoDiff during error cleanup: %v", block.String(), closeErr)
			}
		}()
	} else {
		u.logger.Infof("[persistBlock][%s] UTXO file processing disabled - skipping utxo-additions and utxo-deletions files", block.String())
	}

	if len(block.Subtrees) == 0 {
		// No subtrees to process, just write the coinbase UTXO to the diff and continue
		// If starting from a seed, blocks may not contain a CoinbaseTx; if so skip
		if utxoDiff != nil && block.CoinbaseTx != nil {
			if err := utxoDiff.ProcessTx(block.CoinbaseTx); err != nil {
				return errors.NewProcessingError("[persistBlock][%s] error processing coinbase tx", block.String(), err)
			}
		}
	} else {
		// Phase 1: Create all subtreeData files in parallel
		u.logger.Infof("[persistBlock][%s] Phase 1: Creating %d subtreeData files", block.String(), len(block.Subtrees))

		g1, gCtx1 := errgroup.WithContext(ctx)
		util.SafeSetLimit(g1, concurrency)

		for i, subtreeHash := range block.Subtrees {
			subtreeHash := subtreeHash
			i := i

			g1.Go(func() error {
				return u.CreateSubtreeDataFileStreaming(gCtx1, *subtreeHash, block, i+1)
			})
		}

		if err = g1.Wait(); err != nil {
			return err
		}

		u.logger.Infof("[persistBlock][%s] Phase 1 complete: All subtreeData files created", block.String())

		// Phase 2: Process UTXO diff sequentially through files
		if utxoDiff != nil {
			u.logger.Infof("[persistBlock][%s] Phase 2: Processing UTXO diff for %d subtrees", block.String(), len(block.Subtrees))

			for i, subtreeHash := range block.Subtrees {
				u.logger.Debugf("[persistBlock][%s] processing UTXO for subtree %d / %d [%s]", block.String(), i+1, len(block.Subtrees), subtreeHash.String())

				if err := u.ProcessSubtreeUTXOStreaming(ctx, *subtreeHash, utxoDiff); err != nil {
					return err
				}
			}

			u.logger.Infof("[persistBlock][%s] Phase 2 complete: UTXO diff processed", block.String())
		}
	}

	// Now, write the block file
	u.logger.Infof("[persistBlock][%s] Writing block to disk", block.String())

	storer, err := filestorer.NewFileStorer(ctx, u.logger, u.settings, u.blockStore, hash[:], fileformat.FileTypeBlock)
	if err != nil {
		return errors.NewStorageError("[persistBlock][%s] error creating block file", block.String(), err)
	}

	// Track whether write succeeded to determine whether to close or abort
	var writeSucceeded bool
	defer func() {
		if writeSucceeded {
			if closeErr := storer.Close(ctx); closeErr != nil {
				u.logger.Warnf("[persistBlock][%s] error closing storer: %v", block.String(), closeErr)
			}
		} else {
			// Abort to prevent incomplete file from being finalized
			storer.Abort(errors.NewProcessingError("[persistBlock][%s] block write failed", block.String()))
		}
	}()

	if _, err = storer.Write(blockBytes); err != nil {
		return errors.NewStorageError("[persistBlock][%s] error writing block to disk", block.String(), err)
	}

	writeSucceeded = true

	return nil
}
