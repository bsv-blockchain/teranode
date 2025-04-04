// Package subtreevalidation provides functionality for validating subtrees in a blockchain context.
// It handles the validation of transaction subtrees, manages transaction metadata caching,
// and interfaces with blockchain and validation services.
package subtreevalidation

import (
	"context"
	"sync/atomic"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/txmetacache"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"golang.org/x/sync/errgroup"
)

// processTxMetaUsingCache attempts to retrieve transaction metadata from the cache for a batch
// of transactions. It processes transactions in parallel batches for efficiency.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - txHashes: Slice of transaction hashes to process
//   - txMetaSlice: Pre-allocated slice to store retrieved metadata
//   - failFast: If true, fails quickly when missing transaction threshold is exceeded
//
// Returns:
//   - int: Number of transactions missing from cache
//   - error: Any error encountered during processing
//
// The function will return a ThresholdExceededError if failFast is true and
// the number of missing transactions exceeds the configured threshold.
func (u *Server) processTxMetaUsingCache(ctx context.Context, txHashes []chainhash.Hash, txMetaSlice []*meta.Data, failFast bool) (int, error) {
	if len(txHashes) != len(txMetaSlice) {
		return 0, errors.NewProcessingError("txHashes and txMetaSlice must be the same length")
	}

	ctx, _, deferFn := tracing.StartTracing(ctx, "processTxMetaUsingCache")
	defer deferFn()

	batchSize := u.settings.SubtreeValidation.ProcessTxMetaUsingCacheBatchSize
	validateSubtreeInternalConcurrency := u.settings.SubtreeValidation.ProcessTxMetaUsingCacheConcurrency
	missingTxThreshold := u.settings.SubtreeValidation.ProcessTxMetaUsingCacheMissingTxThreshold

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(validateSubtreeInternalConcurrency)

	cache, ok := u.utxoStore.(*txmetacache.TxMetaCache)
	if !ok {
		u.logger.Warnf("[processTxMetaUsingCache] txMetaStore is not a cached implementation")
		return len(txHashes), nil // As there was no cache, we "missed" all the txHashes
	}

	missed := atomic.Int32{}

	// cycle through batches of 1024 txHashes at a time
	for i := 0; i < len(txHashes); i += batchSize {
		i := i

		g.Go(func() error {
			var txMeta *meta.Data

			// cycle through the batch size, making sure not to go over the length of the txHashes
			for j := 0; j < util.Min(batchSize, len(txHashes)-i); j++ {
				// check whether the txMetaSlice has already been populated
				if txMetaSlice[i+j] != nil {
					continue
				}

				select {
				case <-gCtx.Done(): // Listen for cancellation signal
					// Return the error that caused the cancellation
					return errors.NewContextCanceledError("[processTxMetaUsingCache context cancelled]", gCtx.Err())

				default:
					txHash := txHashes[i+j]

					if txHash.Equal(*util.CoinbasePlaceholderHash) {
						// coinbase placeholder is not in the store
						continue
					}

					txMeta = cache.GetMetaCached(gCtx, &txHash)
					if txMeta != nil {
						txMetaSlice[i+j] = txMeta
						continue
					}

					newMissed := missed.Add(1)
					if failFast && missingTxThreshold > 0 && newMissed > int32(missingTxThreshold) {
						return errors.NewThresholdExceededError("threshold exceeded for missing txs: %d > %d", newMissed, missingTxThreshold)
					}
				}
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return int(missed.Load()), errors.NewProcessingError("error processing txMeta using cache", err)
	}

	return int(missed.Load()), nil
}
