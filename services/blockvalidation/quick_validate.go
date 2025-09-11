package blockvalidation

import (
	"bufio"
	"context"
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/services/validator"
	bloboptions "github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/blockchain/options"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/fields"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"golang.org/x/sync/errgroup"
)

// quickValidateBlock performs optimized validation for blocks below checkpoints.
// This follows the legacy sync approach: create all UTXOs first, then validate later.
// This is safe because checkpoints guarantee these blocks are valid.
// NOTE: Since BlockValidation doesn't have direct access to the validator,
// we focus on UTXO creation which is the main optimization.
//
// Parameters:
//   - ctx: Context for cancellation
//   - block: Block to validate
//
// Returns:
//   - error: If validation fails
func (u *BlockValidation) quickValidateBlock(ctx context.Context, block *model.Block, baseURL string) error {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "quickValidateBlock",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[quickValidateBlock][%s] performing quick validation for checkpointed block at height %d", block.Hash().String(), block.Height),
	)
	defer deferFn()

	// get the next block ID from the blockchain store
	// we will use this to set the txs as mined and for the block itself
	id, err := u.blockchainClient.GetNextBlockID(ctx)
	if err != nil {
		return errors.NewProcessingError("[quickValidateBlock][%s] failed to get next block ID", block.Hash().String(), err)
	}

	// why is the id uint64 in the database, but uint32 in the model?
	block.ID = uint32(id) // nolint:gosec

	if len(block.Subtrees) > 0 {
		// Get transaction data from subtrees
		txWrappers, err := u.getBlockTransactions(ctx, block)
		if err != nil {
			return errors.NewProcessingError("[createAllUTXOs][%s] failed to get block transactions", block.Hash().String(), err)
		}

		// For checkpointed blocks, we can skip full validation and just create UTXOs
		// This is the main optimization - we trust the checkpoint and don't need to
		// validate every transaction's scripts and signatures
		if err = u.createAllUTXOs(ctx, block, txWrappers); err != nil {
			return errors.NewProcessingError("[quickValidateBlock][%s] failed to create UTXOs", block.Hash().String(), err)
		}

		// validate all transactions in the block
		if err = u.spendAllTransactions(ctx, block, txWrappers); err != nil {
			return errors.NewProcessingError("[quickValidateBlock][%s] failed to validate transactions", block.Hash().String(), err)
		}
	}

	// add block directly to blockchain
	if err = u.blockchainClient.AddBlock(ctx,
		block,
		baseURL,
		options.WithSubtreesSet(true),
		options.WithMinedSet(true),
		options.WithID(id),
	); err != nil {
		return errors.NewProcessingError("[quickValidateBlock][%s] failed to add block to blockchain", block.Hash().String(), err)
	}

	// Mark block as existing in cache
	if err = u.SetBlockExists(block.Hash()); err != nil {
		u.logger.Errorf("[ValidateBlock][%s] failed to set block exists cache: %s", block.Hash().String(), err)
	}

	return nil
}

// createAllUTXOs creates all UTXOs for transactions in the block before validation.
// This is the first phase of quick validation for checkpointed blocks.
//
// Parameters:
//   - ctx: Context for cancellation
//   - block: Block containing transactions
//
// Returns:
//   - error: If UTXO creation fails
func (u *BlockValidation) createAllUTXOs(ctx context.Context, block *model.Block, txs []txWrapper) error {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "createAllUTXOs",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[createAllUTXOs][%s] creating UTXOs for %d transactions", block.Hash().String(), block.TransactionCount),
	)
	defer deferFn()

	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, u.settings.UtxoStore.StoreBatcherSize*2)

	// Create UTXOs in parallel
	for idx, txW := range txs {
		tx := txW.tx // Capture for goroutine

		if tx == nil {
			return errors.NewProcessingError("[createAllUTXOs][%s] invalid nil transaction at index %d", block.Hash().String(), idx)
		}

		g.Go(func() error {
			// create the UTXO
			if _, err := u.utxoStore.Create(gCtx, txW.tx, block.Height, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
				BlockID:     block.ID,
				BlockHeight: block.Height,
				SubtreeIdx:  txW.subtreeIdx,
			})); err != nil {
				if !errors.Is(err, errors.ErrTxExists) {
					return errors.NewProcessingError("[createAllUTXOs][%s] failed to create UTXO for tx %s", block.Hash().String(), tx.TxIDChainHash().String(), err)
				}

				u.logger.Debugf("[createAllUTXOs][%s] UTXO already exists for tx %s, deleting and recreating", block.Hash().String(), tx.TxIDChainHash().String())

				// the utxo already exists, this can happen in recovery scenarios
				// we should delete the utxo and try again to ensure it has the correct block ID and height
				if err = u.utxoStore.Delete(gCtx, tx.TxIDChainHash()); err != nil {
					return errors.NewProcessingError("[createAllUTXOs][%s] failed to delete existing UTXO for tx %s", block.Hash().String(), tx.TxIDChainHash().String(), err)
				}

				if _, err = u.utxoStore.Create(gCtx, txW.tx, block.Height, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
					BlockID:     block.ID,
					BlockHeight: block.Height,
					SubtreeIdx:  txW.subtreeIdx,
				})); err != nil {
					// this should not have failed
					return errors.NewProcessingError("[createAllUTXOs][%s] failed to recreate UTXO for tx %s", block.Hash().String(), tx.TxIDChainHash().String(), err)
				}
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return errors.NewProcessingError("[createAllUTXOs][%s] failed to create UTXOs", block.Hash().String(), err)
	}

	return nil
}

// spendAllTransactions performs full validation on all transactions in the block.
// This is the second phase of quick validation for checkpointed blocks.
//
// Parameters:
//   - ctx: Context for cancellation
//   - block: Block containing transactions
//   - txs: List of transactions to validate
//
// Returns:
//   - error: If validation fails
func (u *BlockValidation) spendAllTransactions(ctx context.Context, block *model.Block, txs []txWrapper) error {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "validateAllTransactions",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[validateAllTransactions][%s] validating %d transactions", block.Hash().String(), block.TransactionCount),
	)
	defer deferFn()

	spendBatcherSize := u.settings.UtxoStore.SpendBatcherSize
	spendBatcherConcurrency := u.settings.UtxoStore.SpendBatcherConcurrency

	// validate all the transactions in parallel
	g, gCtx := errgroup.WithContext(ctx)                           // we don't want the tracing to be linked to these calls
	util.SafeSetLimit(g, spendBatcherSize*spendBatcherConcurrency) // we limit the number of concurrent requests, to not overload Aerospike

	validationOptions := validator.NewDefaultOptions()
	// For checkpointed blocks, we skip UTXO creation, they have already been created
	validationOptions.SkipUtxoCreation = true
	// We don't add transactions to block assembly as this is a mined block
	validationOptions.AddTXToBlockAssembly = false
	// We skip policy checks as the txs are assumed valid for a mined block
	validationOptions.SkipPolicyChecks = true

	for idx, txW := range txs {
		tx := txW.tx // Capture for goroutine

		if tx == nil {
			return errors.NewProcessingError("[spendAllTransactions][%s] invalid nil transaction at index %d", block.Hash().String(), idx)
		}

		g.Go(func() error {
			if _, err := u.utxoStore.Spend(gCtx, tx); err != nil {
				return errors.NewProcessingError("failed to spend tx %s", tx.TxIDChainHash().String(), err)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return errors.NewProcessingError("[validateAllTransactions][%s] failed to validate transactions", block.Hash().String(), err)
	}

	return nil
}

type txWrapper struct {
	tx         *bt.Tx
	subtreeIdx int
	Idx        int
}

// getBlockTransactions retrieves all transactions from a block's subtrees.
// This helper function fetches and deserializes transaction data.
//
// Parameters:
//   - ctx: Context for cancellation
//   - block: Block to get transactions from
//
// Returns:
//   - []*bt.Tx: List of transactions
//   - error: If fetching fails
func (u *BlockValidation) getBlockTransactions(ctx context.Context, block *model.Block) ([]txWrapper, error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "getBlockTransactions",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[getBlockTransactions][%s] retrieving transactions from %d subtrees", block.Hash().String(), len(block.Subtrees)),
	)
	defer deferFn()

	if len(block.Subtrees) == 0 {
		return nil, errors.NewProcessingError("[getBlockTransactions][%s] block has no subtrees", block.Hash().String())
	}

	block.SubtreeSlices = make([]*subtreepkg.Subtree, len(block.Subtrees))
	txs := make([]txWrapper, 0, block.TransactionCount)
	txsIndex := make(map[chainhash.Hash]int, block.TransactionCount)
	txsPerSubtree := make([][]txWrapper, len(block.Subtrees))
	txsMu := sync.RWMutex{}

	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, u.settings.BlockValidation.GetBlockTransactionsConcurrency) // Limit concurrency to avoid overwhelming I/O

	for subtreeIdx, subtreeHash := range block.Subtrees {
		subtreeHash := subtreeHash // Capture for goroutine
		subtreeIdx := subtreeIdx   // Capture for goroutine

		txsPerSubtree[subtreeIdx] = make([]txWrapper, 0, 1024) // sensible default

		g.Go(func() error {
			// get the subtree from disk, should be in .subtreeToCheck
			subtreeReader, err := u.subtreeStore.GetIoReader(gCtx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
			if err != nil {
				return errors.NewNotFoundError("[getBlockTransactions][%s] failed to get subtree %s", block.Hash().String(), subtreeHash.String(), err)
			}
			defer subtreeReader.Close()

			// create a buffered reader to read the subtree
			bufferedReader := bufio.NewReaderSize(subtreeReader, 1*1024*1024) // 1MB buffer size

			// subtree only contains the tx hashes (nodes) of the subtree. It is missing the fee and sizeInBytes
			subtree, err := subtreepkg.NewSubtreeFromReader(bufferedReader)
			if err != nil {
				return errors.NewProcessingError("[getBlockTransactions][%s] failed to deserialize subtree %s", block.Hash().String(), subtreeHash.String(), err)
			}

			block.SubtreeSlices[subtreeIdx] = subtree

			// get the subtree data from disk
			subtreeDataReader, err := u.subtreeStore.GetIoReader(gCtx, subtreeHash[:], fileformat.FileTypeSubtreeData)
			if err != nil {
				return errors.NewNotFoundError("[getBlockTransactions][%s] failed to get subtree data %s", block.Hash().String(), subtreeHash.String(), err)
			}

			// create a buffered reader to read the subtree data
			bufferedReader = bufio.NewReaderSize(subtreeDataReader, 1*1024*1024) // 1MB buffer size

			// the subtree data reader will make sure the data matches the transaction ids from the subtree
			subtreeData, err := subtreepkg.NewSubtreeDataFromReader(subtree, bufferedReader)
			if err != nil {
				// Use the standard subtree invalid error with the subtree hash in the message
				return errors.NewProcessingError("[getBlockTransactions][%s] failed to deserialize subtree data %s: %v",
					block.Hash().String(), subtreeHash.String(), err)
			}

			// check that all the transactions are valid
			for idx, tx := range subtreeData.Txs {
				// first tx in first subtree must be coinbase
				if subtreeIdx == 0 && idx == 0 {
					// check for coinbase tx
					if tx != nil && !tx.IsCoinbase() {
						return errors.NewProcessingError("[getBlockTransactions][%s] invalid coinbase tx at index %d in subtree %s", block.Hash().String(), idx, subtreeHash.String())
					}

					// set to nil to indicate coinbase
					subtreeData.Txs[idx] = nil
				} else {
					if tx == nil {
						return errors.NewProcessingError("[getBlockTransactions][%s] missing tx at index %d in subtree %s", block.Hash().String(), idx, subtreeHash.String())
					}
				}
			}

			// add the transactions to the txs slice at the correct position, thread-safe
			txsMu.Lock()
			for idx, tx := range subtreeData.Txs {
				if tx == nil {
					// coinbase tx is the first tx in the first subtree
					if subtreeIdx != 0 && idx != 0 {
						return errors.NewProcessingError("[getBlockTransactions][%s] unexpected nil tx at index %d in subtree %s", block.Hash().String(), idx, subtreeHash.String())
					}

					// skip missing coinbase tx, it will be nil in the txs slice
					continue
				}

				// set the tx hash for caching purposes, it is not changed
				tx.SetTxHash(tx.TxIDChainHash())

				// add the tx to the subtree txs list
				txsPerSubtree[subtreeIdx] = append(txsPerSubtree[subtreeIdx], txWrapper{tx: tx, subtreeIdx: subtreeIdx, Idx: idx})
			}
			txsMu.Unlock()

			fullSubtreeExists, err := u.subtreeStore.Exists(gCtx, subtreeHash[:], fileformat.FileTypeSubtree)
			if err != nil {
				return errors.NewProcessingError("[getBlockTransactions][%s] failed to check existence of full subtree %s", block.Hash().String(), subtreeHash.String(), err)
			}

			if !fullSubtreeExists {
				// write the subtree file with full info to disk and store as .subtree
				fullSubtree, err := subtreepkg.NewIncompleteTreeByLeafCount(subtree.Size())
				if err != nil {
					return errors.NewProcessingError("[getBlockTransactions][%s] failed to create full subtree %s", block.Hash().String(), subtreeHash.String(), err)
				}

				for idx, tx := range subtreeData.Txs {
					// check for coinbase tx
					if subtreeIdx == 0 && idx == 0 {
						if err = fullSubtree.AddCoinbaseNode(); err != nil {
							return errors.NewProcessingError("[getBlockTransactions][%s] failed to add coinbase node to full subtree %s", block.Hash().String(), subtreeHash.String(), err)
						}
					} else {
						txMeta, err := util.TxMetaDataFromTx(tx)
						if err != nil {
							return errors.NewProcessingError("[getBlockTransactions][%s] failed to get tx metadata for tx %s in subtree %s", block.Hash().String(), tx.TxIDChainHash().String(), subtreeHash.String(), err)
						}

						if err = fullSubtree.AddNode(*tx.TxIDChainHash(), txMeta.Fee, txMeta.SizeInBytes); err != nil {
							return errors.NewProcessingError("[getBlockTransactions][%s] failed to add tx node %s to full subtree %s", block.Hash().String(), tx.TxIDChainHash().String(), subtreeHash.String(), err)
						}
					}
				}

				fullSubtreeBytes, err := fullSubtree.Serialize()
				if err != nil {
					return errors.NewProcessingError("[getBlockTransactions][%s] failed to serialize full subtree %s", block.Hash().String(), subtreeHash.String(), err)
				}

				if err = u.subtreeStore.Set(gCtx,
					subtreeHash[:],
					fileformat.FileTypeSubtree,
					fullSubtreeBytes,
					bloboptions.WithAllowOverwrite(true),
					bloboptions.WithDeleteAt(0), // do not delete
				); err != nil {
					return errors.NewProcessingError("[getBlockTransactions][%s] failed to store full subtree %s", block.Hash().String(), subtreeHash.String(), err)
				}
			} else {
				// make sure the subtree is not marked for deletion
				if err = u.subtreeStore.SetDAH(gCtx, subtreeHash[:], fileformat.FileTypeSubtree, 0); err != nil {
					return errors.NewProcessingError("[getBlockTransactions][%s] failed to unset DAH for full subtree %s", block.Hash().String(), subtreeHash.String(), err)
				}
			}

			subtreeMetaExists, err := u.subtreeStore.Exists(gCtx, subtreeHash[:], fileformat.FileTypeSubtreeMeta)
			if err != nil {
				return errors.NewProcessingError("[getBlockTransactions][%s] failed to check existence of subtree meta %s", block.Hash().String(), subtreeHash.String(), err)
			}

			if !subtreeMetaExists {
				// write the subtree meta file to disk and store as .subtreeMeta
				subtreeMetaData := subtreepkg.NewSubtreeMeta(subtree)

				for idx, tx := range subtreeData.Txs {
					if tx == nil {
						// skip missing txs, they will be nil in the txs slice
						continue
					}

					if err = subtreeMetaData.SetTxInpointsFromTx(tx); err != nil {
						return errors.NewTxError("[getBlockTransactions][%s] failed to set tx inpoints for tx %s in subtree meta %s:%d", block.Hash().String(), tx.TxIDChainHash().String(), subtreeHash.String(), idx, err)
					}
				}

				subtreeMetaBytes, err := subtreeMetaData.Serialize()
				if err != nil {
					return errors.NewProcessingError("[getBlockTransactions][%s] failed to serialize subtree meta %s", block.Hash().String(), subtreeHash.String(), err)
				}

				if err = u.subtreeStore.Set(gCtx,
					subtreeHash[:],
					fileformat.FileTypeSubtreeMeta,
					subtreeMetaBytes,
					bloboptions.WithAllowOverwrite(true),
				); err != nil {
					return errors.NewProcessingError("[getBlockTransactions][%s] failed to store subtree meta %s", block.Hash().String(), subtreeHash.String(), err)
				}
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, errors.NewProcessingError("[getBlockTransactions][%s] failed to retrieve transactions", block.Hash().String(), err)
	}

	// combine all the subtree txs into the main txs slice
	for _, subtreeTxs := range txsPerSubtree {
		txs = append(txs, subtreeTxs...)
	}

	// reset to nil to free memory
	txsPerSubtree = nil
	subtreeSize := 0

	var (
		// once ensures the quorum is initialized only once
		once sync.Once
	)

	// extend all the transactions that are not in extended form
	for _, txW := range txs {
		tx := txW.tx

		if !tx.IsExtended() {
			once.Do(func() {
				u.logger.Warnf("[getBlockTransactions][%s] extending transactions in block, not all txs were extended", block.Hash().String())

				// create the lookup map for txs, for extending transactions if needed
				for idx, txW := range txs {
					txsIndex[*txW.tx.TxIDChainHash()] = idx
				}
			})

			// kick-off extending the transaction in the background
			// we do this in parallel to populate the utxo batcher for retrieval
			g.Go(func() error {
				if err := u.ExtendTransaction(ctx, tx, txsIndex, txs); err != nil {
					return errors.NewProcessingError("[getBlockTransactions][%s] failed to extend tx %s", block.Hash().String(), tx.TxIDChainHash().String(), err)
				}

				return nil
			})
		}
	}

	if err := g.Wait(); err != nil {
		return nil, errors.NewProcessingError("[getBlockTransactions][%s] failed to extend transactions", block.Hash().String(), err)
	}

	// check that all the subtrees, except the last are the same size
	for i := 0; i < len(block.SubtreeSlices)-1; i++ {
		if i == 0 {
			subtreeSize = block.SubtreeSlices[i].Length()
		} else if block.SubtreeSlices[i].Length() != subtreeSize && i != len(block.SubtreeSlices)-1 {
			return nil, errors.NewProcessingError("[getBlockTransactions][%s] subtree %d size %d does not match previous subtree size %d", block.Hash().String(), i, block.SubtreeSlices[i].Size(), subtreeSize)
		}
	}

	// check the merkle root of the block, this is a quick check to ensure we have the correct transactions
	if err := block.CheckMerkleRoot(ctx); err != nil {
		return nil, errors.NewProcessingError("[getBlockTransactions][%s] merkle root mismatch", block.Hash().String(), err)
	}

	return txs, nil
}

func (u *BlockValidation) ExtendTransaction(ctx context.Context, tx *bt.Tx, txMap map[chainhash.Hash]int, txs []txWrapper) error {
	inputLen := len(tx.Inputs)
	populatedInputs := 0

	g := errgroup.Group{}
	inputsLock := sync.Mutex{} // to protect the inputs slice from concurrent writes

	for i, input := range tx.Inputs {
		i := i         // capture the loop variable
		input := input // capture the input variable
		prevTxHash := *input.PreviousTxIDChainHash()

		if previousTxIndex, found := txMap[prevTxHash]; found {
			if prevTxWrapper := txs[previousTxIndex]; prevTxWrapper.tx != nil {
				g.Go(func() error {
					// we do have a parent, but since everything is happening in parallel, we need to check if the parent has
					// already been extended
					timeOut := time.After(u.settings.BlockValidation.ExtendTransactionTimeout)

				WaitForParent:
					for {
						select {
						case <-timeOut:
							return errors.NewProcessingError("timed out waiting for parent transaction %s to be extended", prevTxHash.String())
						default:
							if prevTxWrapper.tx.IsExtended() {
								break WaitForParent
							}

							time.Sleep(10 * time.Millisecond) // wait for the parent transaction to be extended
						}
					}

					// lock the inputs slice to prevent concurrent writes
					inputsLock.Lock()

					tx.Inputs[i].PreviousTxSatoshis = prevTxWrapper.tx.Outputs[input.PreviousTxOutIndex].Satoshis
					tx.Inputs[i].PreviousTxScript = bscript.NewFromBytes(*prevTxWrapper.tx.Outputs[input.PreviousTxOutIndex].LockingScript)

					populatedInputs++

					inputsLock.Unlock() // unlock the inputs slice

					return nil
				})
			}
		}
	}

	if err := g.Wait(); err != nil {
		return errors.NewProcessingError("failed to extend transaction %s", tx.TxIDChainHash(), err)
	}

	if populatedInputs == inputLen {
		// all inputs were populated, we can return early
		return nil
	}

	if err := u.utxoStore.PreviousOutputsDecorate(ctx, tx); err != nil {
		if errors.Is(err, errors.ErrProcessing) || errors.Is(err, errors.ErrTxNotFound) {
			// we could not decorate the transaction. This could be because the parent transaction has been DAH'd, which
			// can only happen if this transaction has been processed. In that case, we can try getting the transaction
			// itself.
			txMeta, err := u.utxoStore.Get(ctx, tx.TxIDChainHash(), fields.Tx)
			if err == nil && txMeta != nil {
				if txMeta.Tx != nil {
					for i, input := range txMeta.Tx.Inputs {
						tx.Inputs[i].PreviousTxSatoshis = input.PreviousTxSatoshis
						tx.Inputs[i].PreviousTxScript = input.PreviousTxScript
					}

					return nil
				}
			}
		}

		return errors.NewProcessingError("failed to decorate previous outputs for tx %s", tx.TxIDChainHash(), err)
	}

	return nil
}
