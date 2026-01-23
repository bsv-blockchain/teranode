// Package blockpersister provides comprehensive functionality for persisting blockchain blocks and their associated data.
// It coordinates the storage of blocks, transactions, and UTXO set changes with high efficiency and data integrity.
package blockpersister

import (
	"context"
	"runtime"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
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
func (u *Server) processTxMetaUsingStore(ctx context.Context, subtree *subtreepkg.Subtree, subtreeData *subtreepkg.Data) error {
	ctx, _, deferFn := tracing.Tracer("blockpersister").Start(ctx, "processTxMetaUsingStore")
	defer deferFn()

	batchSize := u.settings.Block.ProcessTxMetaUsingStoreBatchSize
	validateSubtreeInternalConcurrency := subtreepkg.Max(4, runtime.NumCPU()*2)

	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, validateSubtreeInternalConcurrency)

	var (
		subtreeLen = subtree.Length()
	)

	if u.settings.BlockPersister.BatchMissingTransactions {
		for i := 0; i < subtreeLen; i += batchSize {
			i := i // capture range variable for goroutine

			g.Go(func() error {
				end := subtreepkg.Min(i+batchSize, subtreeLen)

				missingTxHashesCompacted := make([]*utxo.UnresolvedMetaData, 0, end-i)

				for j := 0; j < subtreepkg.Min(batchSize, subtreeLen-i); j++ {
					if subtree.Nodes[i+j].Hash.Equal(*subtreepkg.CoinbasePlaceholderHash) {
						// coinbase placeholder is not in the store
						continue
					}

					if subtreeData.Txs[i+j] == nil {
						missingTxHashesCompacted = append(missingTxHashesCompacted, &utxo.UnresolvedMetaData{
							Hash: subtree.Nodes[i+j].Hash,
							Idx:  i + j,
						})
					}
				}

				// get the missing transactions in a batch from the store
				if err := u.utxoStore.BatchDecorate(gCtx, missingTxHashesCompacted, fields.Tx); err != nil {
					return err
				}

				for _, data := range missingTxHashesCompacted {
					if data.Data == nil || data.Err != nil {
						return errors.NewProcessingError("[processTxMetaUsingStore] failed to retrieve transaction metadata for hash %s", data.Hash, data.Err)
					}

					if err := subtreeData.AddTx(data.Data.Tx, data.Idx); err != nil {
						return errors.NewProcessingError("[processTxMetaUsingStore] failed to add transaction metadata for hash %s: %v", data.Hash, err)
					}
				}

				return nil
			})
		}

		if err := g.Wait(); err != nil {
			return err
		}

		return nil
	} else {
		for i := 0; i < subtreeLen; i += batchSize {
			i := i

			g.Go(func() error {
				// cycle through the batch size, making sure not to go over the length of the txHashes
				for j := 0; j < subtreepkg.Min(batchSize, subtreeLen-i); j++ {
					txHash := subtree.Nodes[i+j].Hash

					if txHash.Equal(*subtreepkg.CoinbasePlaceholderHash) {
						// coinbase placeholder is not in the store
						continue
					}

					if subtreeData.Txs[i+j] == nil {
						txMeta, err := u.utxoStore.Get(gCtx, &txHash, fields.Tx)
						if err != nil {
							return err
						}

						if txMeta == nil || txMeta.Tx == nil {
							return errors.NewProcessingError("[processTxMetaUsingStore] failed to retrieve transaction metadata for hash %s", txHash)
						}

						if err = subtreeData.AddTx(txMeta.Tx, i+j); err != nil {
							return errors.NewProcessingError("[processTxMetaUsingStore] failed to add transaction metadata for hash %s: %v", txHash, err)
						}
					}
				}

				return nil
			})
		}

		if err := g.Wait(); err != nil {
			return err
		}

		return nil
	}
}
