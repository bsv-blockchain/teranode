// Package blockpersister provides comprehensive functionality for persisting blockchain blocks and their associated data.
// It coordinates the storage of blocks, transactions, and UTXO set changes with high efficiency and data integrity.
package blockpersister

import (
	"context"
	"runtime"
	"sync/atomic"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/libsv/go-bt/v2/chainhash"
	"golang.org/x/sync/errgroup"
)

// processTxMetaUsingStore retrieves transaction metadata from storage for a batch of transactions.
//
// This method efficiently retrieves transaction metadata from the UTXO store, supporting both
// batch retrieval and individual lookups based on configuration. It's a critical component in
// the transaction processing pipeline, ensuring all necessary transaction data is available
// for validation and persistence.
//
// The function implements two distinct retrieval strategies:
//  1. Batched mode: Collects missing transaction hashes into batches and retrieves them
//     in bulk operations, significantly reducing the number of store requests for high
//     transaction volumes
//  2. Individual mode: Retrieves transaction metadata one by one, which may be more
//     efficient for small numbers of transactions or when batch retrieval is not supported
//
// The method employs concurrency through goroutines to parallelize retrieval operations,
// with configurable batch sizes and concurrency limits to optimize performance based on
// system capabilities and load characteristics.
//
// Parameters:
//   - ctx: Context for coordinating cancellation and tracing
//   - txHashes: Slice of transaction hashes to retrieve metadata for
//   - txMetaSlice: Pre-allocated slice where retrieved transaction metadata will be stored,
//     with indices corresponding to txHashes
//   - batched: Whether to use batch processing mode (true) or individual retrieval (false)
//
// Returns:
//   - int: Number of transactions that couldn't be found or had retrieval errors
//   - error: Any error encountered during the retrieval process
//
// The function handles special cases like coinbase transactions, which are placeholders not
// present in the store. It also accounts for context cancellation to support clean shutdowns.
// Concurrent access to shared state is protected using atomic operations to ensure thread safety.
func (u *Server) processTxMetaUsingStore(ctx context.Context, txHashes []chainhash.Hash, txMetaSlice []*meta.Data, batched bool) (int, error) {
	if len(txHashes) != len(txMetaSlice) {
		return 0, errors.NewInvalidArgumentError("txHashes and txMetaSlice must be the same length")
	}

	ctx, _, deferFn := tracing.StartTracing(ctx, "processTxMetaUsingStore")
	defer deferFn()

	batchSize := u.settings.Block.ProcessTxMetaUsingStoreBatchSize
	validateSubtreeInternalConcurrency := util.Max(4, runtime.NumCPU()/2)

	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, validateSubtreeInternalConcurrency)

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
						if txHashes[i+j].Equal(*util.CoinbasePlaceholderHash) {
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

				if err := u.utxoStore.BatchDecorate(gCtx, missingTxHashesCompacted, "tx"); err != nil {
					return err
				}

				select {
				case <-gCtx.Done(): // Listen for cancellation signal
					return gCtx.Err() // Return the error that caused the cancellation

				default:
					for _, data := range missingTxHashesCompacted {
						if data.Data == nil || data.Err != nil {
							missed.Add(1)
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

						if txHash.Equal(*util.CoinbasePlaceholderHash) {
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
}
