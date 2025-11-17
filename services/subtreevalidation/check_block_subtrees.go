package subtreevalidation

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

// bufioReaderPool reduces GC pressure by reusing bufio.Reader instances.
// With 14,496 subtrees per block, using 32KB buffers provides excellent I/O performance
// while dramatically reducing memory pressure and GC overhead (16x reduction from previous 512KB).
var bufioReaderPool = sync.Pool{
	New: func() interface{} {
		return bufio.NewReaderSize(nil, 32*1024) // 32KB buffer - optimized for sequential I/O
	},
}

// countingReadCloser wraps an io.ReadCloser and counts bytes read
type countingReadCloser struct {
	reader    io.ReadCloser
	bytesRead *uint64 // Pointer to allow external access to count
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.reader.Read(p)
	atomic.AddUint64(c.bytesRead, uint64(n))
	return n, err
}

func (c *countingReadCloser) Close() error {
	return c.reader.Close()
}

// CheckBlockSubtrees validates that all subtrees referenced in a block exist in storage.
//
// Pauses subtree processing during validation to avoid conflicts and returns missing
// subtree information for blocks that reference unavailable subtrees.
func (u *Server) CheckBlockSubtrees(ctx context.Context, request *subtreevalidation_api.CheckBlockSubtreesRequest) (*subtreevalidation_api.CheckBlockSubtreesResponse, error) {
	block, err := model.NewBlockFromBytes(request.Block)
	if err != nil {
		return nil, errors.NewProcessingError("[CheckBlockSubtrees] Failed to get block from blockchain client", err)
	}

	// Extract PeerID from request for tracking
	peerID := request.PeerId

	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "CheckBlockSubtrees",
		tracing.WithParentStat(u.stats),
		tracing.WithHistogram(prometheusSubtreeValidationCheckSubtree),
		tracing.WithLogMessage(u.logger, "[CheckBlockSubtrees] called for block %s at height %d", block.Hash().String(), block.Height),
	)
	defer deferFn()

	// Panic recovery to ensure pause lock is always released even on crashes
	defer func() {
		if r := recover(); r != nil {
			u.logger.Errorf("[CheckBlockSubtrees] PANIC recovered for block %s: %v", block.Hash().String(), r)
			// Panic is re-raised after this defer completes, ensuring all defers execute
			panic(r)
		}
	}()

	// Check if the block is on our chain or will become part of our chain
	// Only pause subtree processing if this block is on our chain or extending our chain
	shouldPauseProcessing := false

	bestBlockHeader, _, err := u.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, errors.NewProcessingError("[CheckBlockSubtrees] Failed to get best block header", err)
	}

	if bestBlockHeader.Hash().IsEqual(block.Header.HashPrevBlock) {
		// If the block's parent is the best block, we can safely assume this block
		// is extending our chain and should pause subtree processing
		u.logger.Infof("[CheckBlockSubtrees] Block %s is extending our chain - pausing subtree processing", block.Hash().String())
		shouldPauseProcessing = true
	} else {
		// First check if the block's parent exists
		parentExists, err := u.blockchainClient.GetBlockExists(ctx, block.Header.HashPrevBlock)
		if err != nil {
			u.logger.Warnf("[CheckBlockSubtrees] Failed to check if parent block exists: %v", err)
			// On error, default to pausing for safety
			shouldPauseProcessing = true
		} else if parentExists {
			// If the parent exists, check if it's on our current chain
			_, parentMeta, err := u.blockchainClient.GetBlockHeader(ctx, block.Header.HashPrevBlock)
			if err != nil {
				u.logger.Warnf("[CheckBlockSubtrees] Failed to get parent block header: %v", err)
				// On error, default to pausing for safety
				shouldPauseProcessing = true
			} else if parentMeta != nil && parentMeta.ID > 0 {
				// Check if the parent is on the current chain
				isOnChain, err := u.blockchainClient.CheckBlockIsInCurrentChain(ctx, []uint32{parentMeta.ID})
				if err != nil {
					u.logger.Warnf("[CheckBlockSubtrees] Failed to check if parent is on current chain: %v", err)
					// On error, default to pausing for safety
					shouldPauseProcessing = true
				} else {
					shouldPauseProcessing = isOnChain
				}
			}
		} else {
			// Parent doesn't exist - this could be a block from a different fork
			// Don't pause processing for blocks from different forks
			u.logger.Infof("[CheckBlockSubtrees] Block %s parent %s not found - likely from different fork, not pausing subtree processing", block.Hash().String(), block.Header.HashPrevBlock.String())
			shouldPauseProcessing = false
		}
	}

	// Skip pause lock if we're catching up
	if shouldPauseProcessing {
		currentState, err := u.blockchainClient.GetFSMCurrentState(ctx)
		if err != nil {
			u.logger.Warnf("[CheckBlockSubtrees] Failed to get FSM state: %v - will pause for safety", err)
		} else if currentState != nil &&
			(*currentState == blockchain.FSMStateCATCHINGBLOCKS || *currentState == blockchain.FSMStateLEGACYSYNCING) {
			u.logger.Infof("[CheckBlockSubtrees] Skipping pause lock - FSM state is %s (catching up)", currentState.String())
			shouldPauseProcessing = false
		}
	}

	// Acquire and manage pause lock with immediate defer for guaranteed cleanup
	if shouldPauseProcessing {
		u.logger.Infof("[CheckBlockSubtrees] Block %s is on our chain or extending it - acquiring pause lock across all pods", block.Hash().String())

		releasePause, err := u.setPauseProcessing(ctx)
		// Always defer - safe to call even on error (returns noopFunc which does nothing)
		defer releasePause()

		if err != nil {
			u.logger.Warnf("[CheckBlockSubtrees] Failed to acquire distributed pause lock: %v - continuing without pause", err)
		} else {
			u.logger.Infof("[CheckBlockSubtrees] Pause lock acquired successfully for block %s", block.Hash().String())
		}
	} else {
		u.logger.Infof("[CheckBlockSubtrees] Block %s is on a different fork - not pausing subtree processing", block.Hash().String())
	}

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
		subtreeTxs      = make([][]*bt.Tx, len(missingSubtrees))
		allTransactions = make([]*bt.Tx, 0, block.TransactionCount)
	)

	// get all the subtrees that are missing from the peer in parallel
	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, u.settings.SubtreeValidation.CheckBlockSubtreesConcurrency)

	dah := u.utxoStore.GetBlockHeight() + u.settings.GetSubtreeValidationBlockHeightRetention()

	for subtreeIdx, subtreeHash := range missingSubtrees {
		subtreeHash := subtreeHash
		subtreeIdx := subtreeIdx

		subtreeTxs[subtreeIdx] = make([]*bt.Tx, 0, 1024) // Pre-allocate space for transactions in this subtree

		g.Go(func() (err error) {
			subtreeToCheckExists, err := u.subtreeStore.Exists(gCtx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
			if err != nil {
				return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to check if subtree exists in store", subtreeHash.String(), err)
			}

			var subtreeToCheck *subtreepkg.Subtree

			if subtreeToCheckExists {
				// get the subtreeToCheck from the store
				subtreeReader, err := u.subtreeStore.GetIoReader(gCtx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
				if err != nil {
					return errors.NewStorageError("[CheckBlockSubtrees][%s] failed to get subtree from store", subtreeHash.String(), err)
				}
				defer subtreeReader.Close()

				// Use pooled bufio.Reader to reduce allocations (eliminates 50% of GC pressure)
				bufferedReader := bufioReaderPool.Get().(*bufio.Reader)
				bufferedReader.Reset(subtreeReader)
				defer func() {
					bufferedReader.Reset(nil) // Clear reference before returning to pool
					bufioReaderPool.Put(bufferedReader)
				}()

				subtreeToCheck, err = subtreepkg.NewSubtreeFromReader(bufferedReader)
				if err != nil {
					return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to deserialize subtree", subtreeHash.String(), err)
				}
			} else {
				// get the subtree from the peer
				url := fmt.Sprintf("%s/subtree/%s", request.BaseUrl, subtreeHash.String())

				subtreeNodeBytes, err := util.DoHTTPRequest(gCtx, url)
				if err != nil {
					return errors.NewServiceError("[CheckBlockSubtrees][%s] failed to get subtree from %s", subtreeHash.String(), url, err)
				}

				// Track bytes downloaded from peer
				if u.p2pClient != nil && peerID != "" {
					if err := u.p2pClient.RecordBytesDownloaded(gCtx, peerID, uint64(len(subtreeNodeBytes))); err != nil {
						u.logger.Warnf("[CheckBlockSubtrees][%s] failed to record %d bytes downloaded from peer %s: %v", subtreeHash.String(), len(subtreeNodeBytes), peerID, err)
					}
				}

				subtreeToCheck, err = subtreepkg.NewIncompleteTreeByLeafCount(len(subtreeNodeBytes) / chainhash.HashSize)
				if err != nil {
					return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to create subtree structure", subtreeHash.String(), err)
				}

				var nodeHash chainhash.Hash
				for i := 0; i < len(subtreeNodeBytes)/chainhash.HashSize; i++ {
					copy(nodeHash[:], subtreeNodeBytes[i*chainhash.HashSize:(i+1)*chainhash.HashSize])

					if nodeHash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
						if err = subtreeToCheck.AddCoinbaseNode(); err != nil {
							return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to add coinbase node to subtree", subtreeHash.String(), err)
						}
					} else {
						if err = subtreeToCheck.AddNode(nodeHash, 0, 0); err != nil {
							return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to add node to subtree", subtreeHash.String(), err)
						}
					}
				}

				if !subtreeHash.Equal(*subtreeToCheck.RootHash()) {
					return errors.NewProcessingError("[CheckBlockSubtrees][%s] subtree root hash mismatch: %s", subtreeHash.String(), subtreeToCheck.RootHash().String())
				}

				subtreeBytes, err := subtreeToCheck.Serialize()
				if err != nil {
					return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to serialize subtree", subtreeHash.String(), err)
				}

				// Store the subtreeToCheck for later processing
				// we not set a DAH as this is part of a block and will be permanently stored anyway
				if err = u.subtreeStore.Set(gCtx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes, options.WithDeleteAt(dah)); err != nil {
					return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to store subtree", subtreeHash.String(), err)
				}
			}

			subtreeDataExists, err := u.subtreeStore.Exists(gCtx, subtreeHash[:], fileformat.FileTypeSubtreeData)
			if err != nil {
				return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to check if subtree data exists in store", subtreeHash.String(), err)
			}

			if !subtreeDataExists {
				// get the subtree data from the peer and process it directly
				url := fmt.Sprintf("%s/subtree_data/%s", request.BaseUrl, subtreeHash.String())

				body, subtreeDataErr := util.DoHTTPRequestBodyReader(gCtx, url)
				if subtreeDataErr != nil {
					return errors.NewServiceError("[CheckBlockSubtrees][%s] failed to get subtree data from %s", subtreeHash.String(), url, subtreeDataErr)
				}

				// Wrap with counting reader to track bytes downloaded
				var bytesRead uint64
				countingBody := &countingReadCloser{
					reader:    body,
					bytesRead: &bytesRead,
				}

				// Process transactions directly from the stream while storing to disk
				err = u.processSubtreeDataStream(gCtx, subtreeToCheck, countingBody, &subtreeTxs[subtreeIdx])
				_ = countingBody.Close()

				// Track bytes downloaded from peer after stream is consumed
				// Decouple the context to ensure tracking completes even if parent context is cancelled
				if u.p2pClient != nil && peerID != "" {
					trackCtx, _, deferFn := tracing.DecoupleTracingSpan(gCtx, "subtreevalidation", "recordBytesDownloaded")
					defer deferFn()
					if err := u.p2pClient.RecordBytesDownloaded(trackCtx, peerID, bytesRead); err != nil {
						u.logger.Warnf("[CheckBlockSubtrees][%s] failed to record %d bytes downloaded from peer %s: %v", subtreeHash.String(), bytesRead, peerID, err)
					}
				}

				if err != nil {
					return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to process subtree data stream", subtreeHash.String(), err)
				}
			} else {
				// SubtreeData exists, extract transactions from stored file
				err = u.extractAndCollectTransactions(gCtx, subtreeToCheck, &subtreeTxs[subtreeIdx])
				if err != nil {
					return errors.NewProcessingError("[CheckBlockSubtrees][%s] failed to extract transactions", subtreeHash.String(), err)
				}
			}

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return nil, errors.NewProcessingError("[CheckBlockSubtreesRequest] Failed to get subtree tx hashes", err)
	}

	// Collect all transactions from all subtrees into a single slice for processing
	for _, txs := range subtreeTxs {
		if len(txs) > 0 {
			allTransactions = append(allTransactions, txs...)
		}
	}

	subtreeTxs = nil // Clear the slice to free memory

	// get the previous block headers on this chain and pass into the validation
	blockHeaderIDs, err := u.blockchainClient.GetBlockHeaderIDs(ctx, block.Header.HashPrevBlock, uint64(u.settings.GetUtxoStoreBlockHeightRetention()*2))
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
		u.logger.Infof("[CheckBlockSubtrees] Processing %d transactions from %d subtrees using level-based validation", len(allTransactions), len(missingSubtrees))

		if err = u.processTransactionsInLevels(ctx, allTransactions, block.Height, blockIds); err != nil {
			return nil, errors.NewProcessingError("[CheckBlockSubtreesRequest] Failed to process transactions in levels", err)
		}

		g, gCtx = errgroup.WithContext(ctx)
		util.SafeSetLimit(g, u.settings.SubtreeValidation.CheckBlockSubtreesConcurrency)

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
					PeerID:        peerID,
				}

				subtree, err := u.ValidateSubtreeInternal(
					ctx,
					v,
					block.Height,
					blockIds,
					validator.WithSkipPolicyChecks(true),
					validator.WithCreateConflicting(true),
					validator.WithIgnoreLocked(true),
				)
				if err != nil {
					u.logger.Debugf("[CheckBlockSubtreesRequest] Failed to validate subtree %s", subtreeHash.String(), err)
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
			return nil, errors.WrapGRPC(errors.NewProcessingError("[CheckBlockSubtreesRequest] Failed during parallel subtree validation", err))
		}

		// Now validate the subtrees, in order, which should be much faster since we already validated all transactions
		// and they should have been added to the internal cache
		for _, subtreeHash := range revalidateSubtrees {
			// This line is only reached when the base URL is not "legacy"
			v := ValidateSubtree{
				SubtreeHash:   subtreeHash,
				BaseURL:       request.BaseUrl,
				AllowFailFast: false,
				PeerID:        peerID,
			}

			subtree, err := u.ValidateSubtreeInternal(
				ctx,
				v,
				block.Height,
				blockIds,
				validator.WithSkipPolicyChecks(true),
				validator.WithCreateConflicting(true),
				validator.WithIgnoreLocked(true),
			)
			if err != nil {
				return nil, errors.WrapGRPC(errors.NewProcessingError("[CheckBlockSubtreesRequest] Failed to validate subtree %s", subtreeHash.String(), err))
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
func (u *Server) extractAndCollectTransactions(ctx context.Context, subtree *subtreepkg.Subtree, subtreeTransactions *[]*bt.Tx) error {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "extractAndCollectTransactions",
		tracing.WithParentStat(u.stats),
		tracing.WithDebugLogMessage(u.logger, "[extractAndCollectTransactions] called for subtree %s", subtree.RootHash().String()),
	)
	defer deferFn()

	// Get subtreeData reader
	subtreeDataReader, err := u.subtreeStore.GetIoReader(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData)
	if err != nil {
		return errors.NewStorageError("[extractAndCollectTransactions] failed to get subtreeData from store", err)
	}
	defer subtreeDataReader.Close()

	// Use pooled bufio.Reader to accelerate reading and reduce allocations
	bufferedReader := bufioReaderPool.Get().(*bufio.Reader)
	bufferedReader.Reset(subtreeDataReader)
	defer func() {
		bufferedReader.Reset(nil)
		bufioReaderPool.Put(bufferedReader)
	}()

	// Read transactions directly into the shared collection
	txCount, err := u.readTransactionsFromSubtreeDataStream(subtree, bufferedReader, subtreeTransactions)
	if err != nil {
		return errors.NewProcessingError("[extractAndCollectTransactions] failed to read transactions from subtreeData", err)
	}

	if txCount != subtree.Length() {
		return errors.NewProcessingError("[extractAndCollectTransactions] transaction count mismatch: expected %d, got %d", subtree.Length(), txCount)
	}

	u.logger.Debugf("[extractAndCollectTransactions] Extracted %d transactions from subtree %s", txCount, subtree.RootHash().String())

	return nil
}

// processSubtreeDataStream processes subtreeData directly from HTTP stream while storing to disk
// This avoids the inefficiency of writing to disk and immediately reading back
func (u *Server) processSubtreeDataStream(ctx context.Context, subtree *subtreepkg.Subtree,
	body io.ReadCloser, allTransactions *[]*bt.Tx) error {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "processSubtreeDataStream",
		tracing.WithParentStat(u.stats),
		tracing.WithDebugLogMessage(u.logger, "[processSubtreeDataStream] called for subtree %s", subtree.RootHash().String()),
	)
	defer deferFn()

	// Create a buffer to capture the data for storage
	var buffer bytes.Buffer

	// Use TeeReader to read from HTTP stream while writing to buffer
	teeReader := io.TeeReader(body, &buffer)

	// Read transactions directly into the shared collection from the stream
	txCount, err := u.readTransactionsFromSubtreeDataStream(subtree, teeReader, allTransactions)
	if err != nil {
		return errors.NewProcessingError("[processSubtreeDataStream] failed to read transactions from stream", err)
	}

	// make sure the subtree transaction count matches what we read from the stream
	if txCount != subtree.Length() {
		return errors.NewProcessingError("[processSubtreeDataStream] transaction count mismatch: expected %d, got %d", subtree.Length(), txCount)
	}

	// Now store the buffered data to disk
	// we not set a DAH as this is part of a block and will be permanently stored anyway
	err = u.subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData, buffer.Bytes())
	if err != nil {
		return errors.NewProcessingError("[processSubtreeDataStream] failed to store subtree data", err)
	}

	u.logger.Debugf("[processSubtreeDataStream] Processed %d transactions from subtree %s directly from stream",
		txCount, subtree.RootHash().String())

	return nil
}

// readTransactionsFromSubtreeDataStream reads transactions directly from subtreeData stream
// This follows the same pattern as go-subtree's serializeFromReader but appends directly to the shared collection
func (u *Server) readTransactionsFromSubtreeDataStream(subtree *subtreepkg.Subtree, reader io.Reader, subtreeTransactions *[]*bt.Tx) (int, error) {
	txIndex := 0

	if len(subtree.Nodes) > 0 && subtree.Nodes[0].Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
		txIndex = 1
	}

	for {
		tx := &bt.Tx{}

		_, err := tx.ReadFrom(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// End of stream reached
				break
			}
			return txIndex, errors.NewProcessingError("[readTransactionsFromSubtreeDataStream] error reading transaction", err)
		}

		if tx.IsCoinbase() && txIndex == 1 {
			// we did get an unexpected coinbase transaction
			// reset the index to 0 to check the coinbase
			txIndex = 0
		}

		tx.SetTxHash(tx.TxIDChainHash()) // Cache the transaction hash to avoid recomputing it

		// Basic sanity check: ensure the transaction hash matches the expected hash from the subtree
		if txIndex < subtree.Length() {
			expectedHash := subtree.Nodes[txIndex].Hash
			if !expectedHash.Equal(*tx.TxIDChainHash()) {
				return txIndex, errors.NewProcessingError("[readTransactionsFromSubtreeDataStream] transaction hash mismatch at index %d: expected %s, got %s", txIndex, expectedHash.String(), tx.TxIDChainHash().String())
			}
		} else {
			return txIndex, errors.NewProcessingError("[readTransactionsFromSubtreeDataStream] more transactions than expected in subtreeData")
		}

		*subtreeTransactions = append(*subtreeTransactions, tx)
		txIndex++
	}

	return txIndex, nil
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
			idx: i,
		}
	}

	// Use the existing prepareTxsPerLevel logic to organize transactions by dependency levels
	maxLevel, txsPerLevel, err := u.prepareTxsPerLevel(ctx, missingTxs)
	if err != nil {
		return errors.NewProcessingError("[processTransactionsInLevels] Failed to prepare transactions per level", err)
	}

	u.logger.Infof("[processTransactionsInLevels] Processing transactions across %d levels", maxLevel+1)

	validatorOptions := []validator.Option{
		validator.WithSkipPolicyChecks(true),
		validator.WithCreateConflicting(true),
		validator.WithIgnoreLocked(true),
	}

	currentState, err := u.blockchainClient.GetFSMCurrentState(ctx)
	if err != nil {
		return errors.NewProcessingError("[processTransactionsInLevels] Failed to get FSM current state", err)
	}

	// During legacy syncing or catching up, disable adding transactions to block assembly
	if *currentState == blockchain.FSMStateLEGACYSYNCING || *currentState == blockchain.FSMStateCATCHINGBLOCKS {
		validatorOptions = append(validatorOptions, validator.WithAddTXToBlockAssembly(false))
	}

	// Pre-process validation options
	processedValidatorOptions := validator.ProcessOptions(validatorOptions...)

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

		u.logger.Debugf("[processTransactionsInLevels] Processing level %d/%d with %d transactions", level+1, maxLevel+1, len(levelTxs))

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
					u.logger.Debugf("[processTransactionsInLevels] Failed to validate transaction %s: %v", tx.TxIDChainHash().String(), err)

					// TX_EXISTS is not an error - transaction was already validated
					if errors.Is(err, errors.ErrTxExists) {
						u.logger.Debugf("[processTransactionsInLevels] Transaction %s already exists, skipping", tx.TxIDChainHash().String())
						return nil
					}

					// Count all other errors
					errorsFound.Add(1)

					// Handle missing parent transactions by adding to orphanage
					if errors.Is(err, errors.ErrTxMissingParent) {
						isRunning, runningErr := u.blockchainClient.IsFSMCurrentState(gCtx, blockchain.FSMStateRUNNING)
						if runningErr == nil && isRunning {
							u.logger.Debugf("[processTransactionsInLevels] Transaction %s missing parent, adding to orphanage", tx.TxIDChainHash().String())
							if u.orphanage.Set(*tx.TxIDChainHash(), tx) {
								addedToOrphanage.Add(1)
							} else {
								u.logger.Warnf("[processTransactionsInLevels] Failed to add transaction %s to orphanage - orphanage is full", tx.TxIDChainHash().String())
							}
						} else {
							u.logger.Debugf("[processTransactionsInLevels] Transaction %s missing parent, but FSM not in RUNNING state - not adding to orphanage", tx.TxIDChainHash().String())
						}
					} else if errors.Is(err, errors.ErrTxInvalid) && !errors.Is(err, errors.ErrTxPolicy) {
						// Log truly invalid transactions
						u.logger.Warnf("[processTransactionsInLevels] Invalid transaction detected: %s: %v", tx.TxIDChainHash().String(), err)

						if errors.Is(err, errors.ErrTxInvalid) {
							return err
						}
					} else {
						u.logger.Errorf("[processTransactionsInLevels] Processing error for transaction %s: %v", tx.TxIDChainHash().String(), err)
					}

					return nil // Don't fail the entire level
				}

				if txMeta == nil {
					u.logger.Debugf("[processTransactionsInLevels] Transaction metadata is nil for %s", tx.TxIDChainHash().String())
				} else {
					u.logger.Debugf("[processTransactionsInLevels] Successfully validated transaction %s", tx.TxIDChainHash().String())
				}

				return nil
			})
		}

		// Fail early if we get an actual tx error thrown
		if err = g.Wait(); err != nil {
			return errors.NewProcessingError("[processTransactionsInLevels] Failed to process level %d", level+1, err)
		}

		u.logger.Debugf("[processTransactionsInLevels] Processing level %d/%d with %d transactions DONE", level+1, maxLevel+1, len(levelTxs))
	}

	if errorsFound.Load() > 0 {
		return errors.NewProcessingError("[processTransactionsInLevels] Completed processing with %d errors, %d transactions added to orphanage", errorsFound.Load(), addedToOrphanage.Load())
	}

	u.logger.Infof("[processTransactionsInLevels] Successfully processed all %d transactions", len(allTransactions))
	return nil
}
