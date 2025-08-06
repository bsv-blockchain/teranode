package subtreevalidation

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bitcoin-sv/teranode/services/validator"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"golang.org/x/sync/errgroup"
)

func (u *Server) CheckBlockSubtrees(ctx context.Context, request *subtreevalidation_api.CheckBlockSubtreesRequest) (*subtreevalidation_api.CheckBlockSubtreesResponse, error) {
	block, err := model.NewBlockFromBytes(request.Block, nil)
	if err != nil {
		return nil, errors.NewProcessingError("[CheckBlockSubtrees] Failed to get block from blockchain client", err)
	}

	// stop processing subtrees until this is done
	u.pauseSubtreeProcessing.Store(true)

	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "CheckBlockSubtrees",
		tracing.WithParentStat(u.stats),
		tracing.WithHistogram(prometheusSubtreeValidationCheckSubtree),
		tracing.WithLogMessage(u.logger, "[CheckBlockSubtrees] called for block %s at height %d", block.Hash().String(), block.Height),
	)
	defer func() {
		u.pauseSubtreeProcessing.Store(false)
		deferFn()
	}()

	// validate all the subtrees in the block
	missingSubtrees := make([]chainhash.Hash, 0, len(block.Subtrees))
	for _, subtreeHash := range block.Subtrees {
		subtreeExists, err := u.subtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
		if err != nil {
			return nil, errors.NewProcessingError("[CheckBlockSubtrees] Failed to check if subtree exists in store", err)
		}

		if !subtreeExists {
			missingSubtrees = append(missingSubtrees, *subtreeHash)
		}
	}

	if len(missingSubtrees) == 0 {
		return &subtreevalidation_api.CheckBlockSubtreesResponse{
			Blessed: true,
		}, nil
	}

	// Shared collection for all transactions across subtrees
	var (
		allTransactionsMutex sync.Mutex
		subtree              *subtreepkg.Subtree
	)

	allTransactions := make([]*bt.Tx, 0, block.TransactionCount)

	// get all the subtrees that are missing from the peer in parallel
	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, 32)

	for _, subtreeHash := range missingSubtrees {
		subtreeHash := subtreeHash

		g.Go(func() (err error) {
			subtreeDataExists, err := u.subtreeStore.Exists(gCtx, subtreeHash[:], fileformat.FileTypeSubtreeData)
			if err != nil {
				return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to check if subtree data exists in store: %v", subtreeHash.String(), err)
			}

			if !subtreeDataExists {
				// get the subtree data from the peer and process it directly
				url := fmt.Sprintf("%s/subtree_data/%s", request.BaseUrl, subtreeHash.String())

				body, subtreeDataErr := util.DoHTTPRequestBodyReader(gCtx, url)
				if subtreeDataErr != nil {
					return errors.NewServiceError("[CheckBlockSubtrees][%s] failed to get subtree data from %s: %v", subtreeHash.String(), url, subtreeDataErr)
				}

				// Process transactions directly from the stream while storing to disk
				err = u.processSubtreeDataStream(gCtx, subtreeHash, body, &allTransactionsMutex, &allTransactions)
				_ = body.Close()

				if err != nil {
					return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to process subtree data stream: %v", subtreeHash.String(), err)
				}
			} else {
				// SubtreeData exists, extract transactions from stored file
				err = u.extractAndCollectTransactions(gCtx, subtreeHash, &allTransactionsMutex, &allTransactions)
				if err != nil {
					return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to extract transactions: %v", subtreeHash.String(), err)
				}
			}

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return nil, errors.NewProcessingError("[CheckBlockSubtreesRequest] Failed to get subtree tx hashes", err)
	}

	// get the previous block headers on this chain and pass into the validation
	blockHeaderIDs, err := u.blockchainClient.GetBlockHeaderIDs(ctx, block.Header.HashPrevBlock, uint64(u.settings.GetUtxoStoreBlockHeightRetention()))
	if err != nil {
		return nil, errors.NewProcessingError("[CheckSubtree] Failed to get block headers from blockchain client", err)
	}

	blockIds := make(map[uint32]bool, len(blockHeaderIDs))

	for _, blockID := range blockHeaderIDs {
		blockIds[blockID] = true
	}

	// Process all transactions using block-wide level-based validation
	if len(allTransactions) == 0 {
		u.logger.Infof("[CheckBlockSubtrees] No transactions to validate")
	} else {
		u.logger.Infof("[CheckBlockSubtrees] Processing %d transactions from %d subtrees using level-based validation",
			len(allTransactions), len(missingSubtrees))

		err = u.processTransactionsInLevels(ctx, allTransactions, block.Height, blockIds)
		if err != nil {
			return nil, errors.NewProcessingError("[CheckBlockSubtreesRequest] Failed to process transactions in levels: %v", err)
		}

		g, gCtx = errgroup.WithContext(ctx)
		util.SafeSetLimit(g, 32) // TODO: make this configurable

		var revalidateSubtreesMutex sync.Mutex
		revalidateSubtrees := make([]chainhash.Hash, 0, len(missingSubtrees))

		// validate all the subtrees in parallel, since we already validated all transactions
		for _, subtreeHash := range missingSubtrees {
			subtreeHash := subtreeHash

			g.Go(func() (err error) {
				// This line is only reached when the base URL is not "legacy"
				v := ValidateSubtree{
					SubtreeHash:   subtreeHash,
					BaseURL:       request.BaseUrl,
					AllowFailFast: false,
				}

				if subtree, err = u.ValidateSubtreeInternal(
					ctx,
					v,
					block.Height,
					blockIds,
					validator.WithSkipPolicyChecks(true),
					validator.WithCreateConflicting(true),
					validator.WithIgnoreLocked(true),
				); err != nil {
					u.logger.Debugf("[CheckBlockSubtreesRequest] Failed to validate subtree %s: %v", subtreeHash.String(), err)
					revalidateSubtreesMutex.Lock()
					revalidateSubtrees = append(revalidateSubtrees, subtreeHash)
					revalidateSubtreesMutex.Unlock()

					return nil
				}

				// Remove validated transactions from orphanage
				for _, node := range subtree.Nodes {
					u.orphanage.Delete(node.Hash)
				}

				return nil
			})
		}

		// Wait for all parallel validations to complete
		if err = g.Wait(); err != nil {
			return nil, errors.NewProcessingError("[CheckBlockSubtreesRequest] Failed during parallel subtree validation: %v", err)
		}

		// Now validate the subtrees, in order, which should be much faster since we already validated all transactions
		// and they should have been added to the internal cache
		for _, subtreeHash := range revalidateSubtrees {
			// This line is only reached when the base URL is not "legacy"
			v := ValidateSubtree{
				SubtreeHash:   subtreeHash,
				BaseURL:       request.BaseUrl,
				AllowFailFast: false,
			}

			if subtree, err = u.ValidateSubtreeInternal(
				ctx,
				v,
				block.Height,
				blockIds,
				validator.WithSkipPolicyChecks(true),
				validator.WithCreateConflicting(true),
				validator.WithIgnoreLocked(true),
			); err != nil {
				return nil, errors.NewProcessingError("[CheckBlockSubtreesRequest] Failed to validate subtree %s: %v", subtreeHash.String(), err)
			}

			// Remove validated transactions from orphanage
			for _, node := range subtree.Nodes {
				u.orphanage.Delete(node.Hash)
			}
		}
	}

	u.processOrphans(ctx, *block.Header.Hash(), block.Height, blockIds)

	return &subtreevalidation_api.CheckBlockSubtreesResponse{
		Blessed: true,
	}, nil
}

// extractAndCollectTransactions extracts all transactions from a subtree's data file
// and adds them to the shared collection for block-wide processing
func (u *Server) extractAndCollectTransactions(ctx context.Context, subtreeHash chainhash.Hash,
	mutex *sync.Mutex, allTransactions *[]*bt.Tx) error {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "extractAndCollectTransactions",
		tracing.WithParentStat(u.stats),
		tracing.WithDebugLogMessage(u.logger, "[extractAndCollectTransactions] called for subtree %s", subtreeHash.String()),
	)
	defer deferFn()

	// Get subtreeData reader
	subtreeDataReader, err := u.subtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
	if err != nil {
		return errors.NewStorageError("[extractAndCollectTransactions] failed to get subtreeData from store", err)
	}
	defer subtreeDataReader.Close()

	// Read transactions directly into the shared collection
	txCount, err := u.readTransactionsFromSubtreeDataStream(subtreeDataReader, mutex, allTransactions)
	if err != nil {
		return errors.NewProcessingError("[extractAndCollectTransactions] failed to read transactions from subtreeData", err)
	}

	u.logger.Debugf("[extractAndCollectTransactions] Extracted %d transactions from subtree %s",
		txCount, subtreeHash.String())

	return nil
}

// processSubtreeDataStream processes subtreeData directly from HTTP stream while storing to disk
// This avoids the inefficiency of writing to disk and immediately reading back
func (u *Server) processSubtreeDataStream(ctx context.Context, subtreeHash chainhash.Hash,
	body io.ReadCloser, mutex *sync.Mutex, allTransactions *[]*bt.Tx) error {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "processSubtreeDataStream",
		tracing.WithParentStat(u.stats),
		tracing.WithDebugLogMessage(u.logger, "[processSubtreeDataStream] called for subtree %s", subtreeHash.String()),
	)
	defer deferFn()

	// Create a buffer to capture the data for storage
	var buffer bytes.Buffer

	// Use TeeReader to read from HTTP stream while writing to buffer
	teeReader := io.TeeReader(body, &buffer)

	// Read transactions directly into the shared collection from the stream
	txCount, err := u.readTransactionsFromSubtreeDataStream(teeReader, mutex, allTransactions)
	if err != nil {
		return errors.NewProcessingError("[processSubtreeDataStream] failed to read transactions from stream", err)
	}

	// Now store the buffered data to disk
	err = u.subtreeStore.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData, buffer.Bytes())
	if err != nil {
		return errors.NewProcessingError("[processSubtreeDataStream] failed to store subtree data: %v", err)
	}

	u.logger.Debugf("[processSubtreeDataStream] Processed %d transactions from subtree %s directly from stream",
		txCount, subtreeHash.String())

	return nil
}

// readTransactionsFromSubtreeDataStream reads transactions directly from subtreeData stream
// This follows the same pattern as go-subtree's serializeFromReader but appends directly to the shared collection
func (u *Server) readTransactionsFromSubtreeDataStream(reader io.Reader, mutex *sync.Mutex, allTransactions *[]*bt.Tx) (int, error) {
	txCount := 0

	for {
		tx := &bt.Tx{}

		_, err := tx.ReadFrom(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// End of stream reached
				break
			}
			return txCount, errors.NewProcessingError("[readTransactionsFromSubtreeDataStream] error reading transaction: %v", err)
		}

		// Add transaction directly to shared collection with proper locking
		mutex.Lock()
		*allTransactions = append(*allTransactions, tx)
		mutex.Unlock()

		txCount++
	}

	return txCount, nil
}

// processTransactionsInLevels processes all transactions from all subtrees using level-based validation
// This ensures transactions are processed in dependency order while maximizing parallelism
func (u *Server) processTransactionsInLevels(ctx context.Context, allTransactions []*bt.Tx,
	blockHeight uint32, blockIds map[uint32]bool) error {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "processTransactionsInLevels",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[processTransactionsInLevels] Processing %d transactions at block height %d", len(allTransactions), blockHeight),
	)
	defer deferFn()

	if len(allTransactions) == 0 {
		return nil
	}

	u.logger.Infof("[processTransactionsInLevels] Organizing %d transactions into dependency levels", len(allTransactions))

	// Convert transactions to missingTx format for prepareTxsPerLevel
	missingTxs := make([]missingTx, len(allTransactions))
	for i, tx := range allTransactions {
		if tx == nil {
			return errors.NewProcessingError("[processTransactionsInLevels] transaction is nil at index %d", i)
		}
		missingTxs[i] = missingTx{
			tx:  tx,
			idx: 0, // Index not needed for block-wide processing
		}
	}

	// Use the existing prepareTxsPerLevel logic to organize transactions by dependency levels
	maxLevel, txsPerLevel, err := u.prepareTxsPerLevel(ctx, missingTxs)
	if err != nil {
		return errors.NewProcessingError("[processTransactionsInLevels] Failed to prepare transactions per level: %v", err)
	}

	u.logger.Infof("[processTransactionsInLevels] Processing transactions across %d levels", maxLevel+1)

	// Pre-process validation options
	processedValidatorOptions := validator.ProcessOptions(
		validator.WithSkipPolicyChecks(true),
		validator.WithCreateConflicting(true),
		validator.WithIgnoreLocked(true),
	)

	// Track validation results
	var (
		errorsFound      atomic.Uint64
		addedToOrphanage atomic.Uint64
	)

	// Process each level in series, but all transactions within a level in parallel
	for level := uint32(0); level <= maxLevel; level++ {
		levelTxs := txsPerLevel[level]
		if len(levelTxs) == 0 {
			continue
		}

		u.logger.Debugf("[processTransactionsInLevels] Processing level %d with %d transactions", level, len(levelTxs))

		// Process all transactions at this level in parallel
		g, gCtx := errgroup.WithContext(ctx)
		util.SafeSetLimit(g, u.settings.SubtreeValidation.SpendBatcherSize*2)

		for _, mTx := range levelTxs {
			tx := mTx.tx
			if tx == nil {
				return errors.NewProcessingError("[processTransactionsInLevels] transaction is nil at level %d", level)
			}

			g.Go(func() error {
				// Use existing blessMissingTransaction logic for validation
				txMeta, err := u.blessMissingTransaction(gCtx, chainhash.Hash{}, tx, blockHeight, blockIds, processedValidatorOptions)
				if err != nil {
					u.logger.Debugf("[processTransactionsInLevels] Failed to validate transaction %s: %v",
						tx.TxIDChainHash().String(), err)
					errorsFound.Add(1)

					// Handle missing parent transactions by adding to orphanage
					if errors.Is(err, errors.ErrTxMissingParent) {
						isRunning, runningErr := u.blockchainClient.IsFSMCurrentState(gCtx, blockchain.FSMStateRUNNING)
						if runningErr == nil && isRunning {
							u.logger.Debugf("[processTransactionsInLevels] Transaction %s missing parent, adding to orphanage",
								tx.TxIDChainHash().String())
							u.orphanage.Set(*tx.TxIDChainHash(), tx)
							addedToOrphanage.Add(1)
						}
					} else if errors.Is(err, errors.ErrTxInvalid) && !errors.Is(err, errors.ErrTxPolicy) {
						// Log truly invalid transactions
						u.logger.Warnf("[processTransactionsInLevels] Invalid transaction detected: %s: %v",
							tx.TxIDChainHash().String(), err)
					} else {
						u.logger.Errorf("[processTransactionsInLevels] Processing error for transaction %s: %v",
							tx.TxIDChainHash().String(), err)
					}

					return nil // Don't fail the entire level
				}

				if txMeta == nil {
					u.logger.Debugf("[processTransactionsInLevels] Transaction metadata is nil for %s",
						tx.TxIDChainHash().String())
				} else {
					u.logger.Debugf("[processTransactionsInLevels] Successfully validated transaction %s",
						tx.TxIDChainHash().String())
				}

				return nil
			})
		}

		// Wait for all transactions in this level to complete before proceeding to next level
		if err := g.Wait(); err != nil {
			return errors.NewProcessingError("[processTransactionsInLevels] Failed to process level %d: %v", level, err)
		}

		u.logger.Debugf("[processTransactionsInLevels] Completed level %d", level)
	}

	if errorsFound.Load() > 0 {
		u.logger.Warnf("[processTransactionsInLevels] Completed processing with %d errors, %d transactions added to orphanage",
			errorsFound.Load(), addedToOrphanage.Load())
	} else {
		u.logger.Infof("[processTransactionsInLevels] Successfully processed all %d transactions", len(allTransactions))
	}

	return nil
}
