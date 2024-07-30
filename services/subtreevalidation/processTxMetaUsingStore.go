package subtreevalidation

import (
	"context"
	"runtime"
	"sync/atomic"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

func (u *Server) processTxMetaUsingStore(ctx context.Context, txHashes []chainhash.Hash, txMetaSlice []*meta.Data, batched bool, failFast bool) (int, error) {
	if len(txHashes) != len(txMetaSlice) {
		return 0, errors.NewInvalidArgumentError("txHashes and txMetaSlice must be the same length")
	}

	start, stat, ctx := tracing.StartStatFromContext(ctx, "processTxMetaUsingStore")
	defer stat.AddTime(start)

	batchSize, _ := gocore.Config().GetInt("blockvalidation_processTxMetaUsingStore_BatchSize", 1024)
	validateSubtreeInternalConcurrency, _ := gocore.Config().GetInt("blockvalidation_processTxMetaUsingStor_Concurrency", util.Max(4, runtime.NumCPU()/2))
	missingTxThreshold, _ := gocore.Config().GetInt("blockvalidation_processTxMetaUsingStore_MissingTxThreshold", 0)

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(validateSubtreeInternalConcurrency)

	var missed atomic.Int32

	if batched {
		for i := 0; i < len(txHashes); i += batchSize {
			i := i // capture range variable for goroutine

			g.Go(func() error {
				end := util.Min(i+batchSize, len(txHashes))

				missingTxHashesCompacted := make([]*utxo.UnresolvedMetaData, 0, end-i)

				for j := 0; j < util.Min(batchSize, len(txHashes)-i); j++ {
					select {
					case <-gCtx.Done(): // Listen for cancellation signal
						return gCtx.Err() // Return the error that caused the cancellation

					default:

						if txHashes[i+j].Equal(*model.CoinbasePlaceholderHash) {
							// coinbase placeholder is not in the store
							continue
						}

						if txMetaSlice[i+j] == nil {
							missingTxHashesCompacted = append(missingTxHashesCompacted, &utxo.UnresolvedMetaData{
								Hash: txHashes[i+j],
								Idx:  i + j,
							})
						}
					}
				}

				if err := u.utxoStore.BatchDecorate(gCtx, missingTxHashesCompacted, "fee", "sizeInBytes", "parentTxHashes", "blockIDs"); err != nil {
					return err
				}

				select {
				case <-gCtx.Done(): // Listen for cancellation signal
					return gCtx.Err() // Return the error that caused the cancellation

				default:
					for _, data := range missingTxHashesCompacted {
						if data.Data == nil || data.Err != nil {
							newMissed := missed.Add(1)
							if failFast && missingTxThreshold > 0 && newMissed > int32(missingTxThreshold) {
								return errors.ErrThresholdExceeded
							}
							continue
						}
						txMetaSlice[data.Idx] = data.Data
					}

					return nil
				}
			})
		}

		if err := g.Wait(); err != nil {
			return int(missed.Load()), err
		}

		return int(missed.Load()), nil

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

						if txMetaSlice[i+j] == nil {
							txMeta, err := u.utxoStore.GetMeta(gCtx, &txHash)
							if err != nil {
								return err
							}

							if txMeta != nil {
								txMetaSlice[i+j] = txMeta
								continue
							}
						}

						newMissed := missed.Add(1)
						if failFast && missingTxThreshold > 0 && newMissed > int32(missingTxThreshold) {
							return errors.ErrThresholdExceeded
						}
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
}
