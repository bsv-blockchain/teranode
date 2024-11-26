package subtreevalidation

import (
	"context"
	"sync/atomic"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/txmetacache"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"golang.org/x/sync/errgroup"
)

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
		u.logger.Errorf("[processTxMetaUsingCache] txMetaStore is not a cached implementation")
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
		return int(missed.Load()), errors.NewProcessingError("error processing txMeta using cache: %v", err)
	}

	return int(missed.Load()), nil
}
