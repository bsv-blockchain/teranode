package blockpersister

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/stores/txmetacache"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

func (u *Server) processTxMetaUsingCache(ctx context.Context, txHashes []chainhash.Hash, txMetaSlice []*txmeta.Data) (int, error) {
	if len(txHashes) != len(txMetaSlice) {
		return 0, fmt.Errorf("txHashes and txMetaSlice must be the same length")
	}

	start, stat, ctx := util.StartStatFromContext(ctx, "processTxMetaUsingCache")
	defer stat.AddTime(start)

	batchSize, _ := gocore.Config().GetInt("blockvalidation_processTxMetaUsingCache_BatchSize", 1024)
	validateSubtreeInternalConcurrency, _ := gocore.Config().GetInt("blockvalidation_processTxMetaUsingCache_Concurrency", util.Max(4, runtime.NumCPU()/2))

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(validateSubtreeInternalConcurrency)

	cache, ok := u.txMetaStore.(*txmetacache.TxMetaCache)
	if !ok {
		u.logger.Errorf("[processTxMetaUsingCache] txMetaStore is not a cached implementation")
		return len(txHashes), nil // As there was no cache, we "missed" all the txHashes
	}

	missed := atomic.Int32{}

	// cycle through batches of 1024 txHashes at a time
	for i := 0; i < len(txHashes); i += batchSize {
		i := i

		g.Go(func() error {
			var txMeta *txmeta.Data

			// cycle through the batch size, making sure not to go over the length of the txHashes
			for j := 0; j < util.Min(batchSize, len(txHashes)-i); j++ {
				// check whether the txMetaSlice has already been populated
				if txMetaSlice[i+j] != nil {
					continue
				}

				select {
				case <-gCtx.Done(): // Listen for cancellation signal
					return gCtx.Err() // Return the error that caused the cancellation

				default:
					txHash := txHashes[i+j]

					if txHash.Equal(*model.CoinbasePlaceholderHash) {
						// coinbase placeholder is not in the store
						continue
					}

					txMeta = cache.GetMetaCached(gCtx, &txHash)
					if txMeta != nil {
						txMetaSlice[i+j] = txMeta
						continue
					}

					missed.Add(1)
				}
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return int(missed.Load()), err
	}

	return int(missed.Load()), nil
}
