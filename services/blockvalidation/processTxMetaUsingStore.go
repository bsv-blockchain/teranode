package blockvalidation

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

func (u *BlockValidation) processTxMetaUsingStore(ctx context.Context, txHashes []chainhash.Hash, txMetaSlice []*txmeta.Data, previouslyMissed int, batched bool, failFast bool) (int, error) {
	start, stat, ctx := util.StartStatFromContext(ctx, "processTxMetaUsingStore")
	defer stat.AddTime(start)

	batchSize, _ := gocore.Config().GetInt("blockvalidation_validateSubtreeBatchSize", 1024)
	validateSubtreeInternalConcurrency, _ := gocore.Config().GetInt("blockvalidation_validateSubtreeInternal", util.Max(4, runtime.NumCPU()/2))
	missingTxThreshold, _ := gocore.Config().GetInt("blockvalidation_subtree_validation_store_miss_threshold", 1000)

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(validateSubtreeInternalConcurrency)

	var missed atomic.Int32

	if batched {
		// TODO: implement batched processing
		return len(txHashes), nil

	} else {

		for i := 0; i < len(txHashes); i += batchSize {
			i := i

			g.Go(func() error {
				// cycle through the batch size, making sure not to go over the length of the txHashes
				for j := 0; j < util.Min(batchSize, len(txHashes)-i); j++ {

					select {
					case <-gCtx.Done(): // Listen for cancellation signal
						return gCtx.Err() // Return the error that caused the cancellation

					default:
						txHash := txHashes[i+j]

						if txHash.Equal(*model.CoinbasePlaceholderHash) {
							// coinbase placeholder is not in the store
							continue
						}

						txMeta, err := u.txMetaStore.GetMeta(gCtx, &txHash)
						if err != nil {
							return err
						}

						if txMeta != nil {
							txMetaSlice[i+j] = txMeta
							continue
						}

						newMissed := missed.Add(1)
						if failFast && newMissed > int32(missingTxThreshold) {
							return fmt.Errorf("missed threshold reached")
						}
					}
				}

				return nil
			})
		}

		return int(missed.Load()), g.Wait()
	}
}
