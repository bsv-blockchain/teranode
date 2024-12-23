package blockpersister

import (
	"context"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/utxopersister"
	"github.com/bitcoin-sv/teranode/services/utxopersister/filestorer"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/libsv/go-bt/v2/chainhash"
	"golang.org/x/sync/errgroup"
)

func (u *Server) persistBlock(ctx context.Context, hash *chainhash.Hash, blockBytes []byte) error {
	ctx, _, deferFn := tracing.StartTracing(ctx, "persistBlock",
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
	u.logger.Infof("[BlockPersister] Processing subtrees with concurrency %d", concurrency)

	// Create a new UTXO diff
	utxoDiff, err := utxopersister.NewUTXOSet(ctx, u.logger, u.settings, u.blockStore, block.Header.Hash(), block.Height)
	if err != nil {
		return errors.NewProcessingError("error creating utxo diff", err)
	}

	if len(block.Subtrees) == 0 {
		// No subtrees to process, just write the coinbase UTXO to the diff and continue
		if err := utxoDiff.ProcessTx(block.CoinbaseTx); err != nil {
			return errors.NewProcessingError("error processing coinbase tx", err)
		}
	} else {
		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(concurrency)

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

	// At this point, we have a complete UTXODiff for this block.
	if err = utxoDiff.Close(); err != nil {
		return errors.NewStorageError("error closing utxo diff", err)
	}

	// Now, write the block file
	u.logger.Infof("[BlockPersister] Writing block %s to disk", block.Header.Hash().String())

	storer := filestorer.NewFileStorer(ctx, u.logger, u.settings, u.blockStore, hash[:], "block")

	if _, err = storer.Write(blockBytes); err != nil {
		return errors.NewStorageError("error writing block to disk", err)
	}

	if err = storer.Close(ctx); err != nil {
		return errors.NewStorageError("error closing block file", err)
	}

	return nil
}
