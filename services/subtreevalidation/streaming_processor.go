package subtreevalidation

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"runtime"
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
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

// pendingTx represents a transaction that is waiting to be validated.
// It tracks the number of remaining dependencies (parent transactions in the same batch
// that must be validated first).
type pendingTx struct {
	tx            *bt.Tx
	idx           int   // Original index in the transaction list
	remainingDeps int32 // Number of unprocessed parent dependencies (atomic)
}

// batchResult holds the results from a single batch in filterAlreadyValidated.
// Used for ordered fan-in: batches process in parallel but results are emitted in order.
type batchResult struct {
	batchIdx int
	txs      []*bt.Tx
}

// streamingProcessor implements a memory-efficient streaming pipeline for processing
// large numbers of transactions. It exploits the topological ordering of transactions
// (parents always come before children) to process transactions in a single pass.
//
// The processor integrates subtree loading with validation to minimize memory usage:
// 1. Load subtrees one by one
// 2. For each transaction, check if already validated via BatchDecorate
// 3. NIL out validated transactions to allow GC
// 4. Classify and process unvalidated transactions using bucket-based approach
type streamingProcessor struct {
	server *Server

	// Map from parent hash -> list of children waiting on that parent
	waitingOnParent map[chainhash.Hash][]*pendingTx

	// Mutex for thread-safe access to readyBatch
	readyMu    sync.Mutex
	readyBatch []*pendingTx

	// Configuration
	batchSize   int
	concurrency int

	// Context for validation
	blockHash   chainhash.Hash
	blockHeight uint32
	blockIds    map[uint32]bool

	// Pre-processed validation options
	validatorOptions *validator.Options

	// Error tracking
	errorsFound      atomic.Uint64
	addedToOrphanage atomic.Uint64

	// Stats
	totalTransactions     int
	validatedTransactions atomic.Int64
	skippedTransactions   atomic.Int64
}

// newStreamingProcessor creates a new streaming processor for validating transactions.
func newStreamingProcessor(server *Server, blockHash chainhash.Hash, blockHeight uint32, blockIds map[uint32]bool) *streamingProcessor {
	return &streamingProcessor{
		server:          server,
		waitingOnParent: make(map[chainhash.Hash][]*pendingTx),
		readyBatch:      make([]*pendingTx, 0, server.settings.SubtreeValidation.SpendBatcherSize),
		batchSize:       server.settings.SubtreeValidation.SpendBatcherSize,
		concurrency:     server.settings.SubtreeValidation.SpendBatcherSize * 2,
		blockHash:       blockHash,
		blockHeight:     blockHeight,
		blockIds:        blockIds,
	}
}

// processMissingSubtreesStreaming is the streaming replacement for processMissingSubtrees.
// It processes subtrees one by one, immediately filtering out already-validated transactions
// to minimize memory usage.
func (u *Server) processMissingSubtreesStreaming(ctx context.Context, request *subtreevalidation_api.CheckBlockSubtreesRequest, missingSubtrees []chainhash.Hash, peerID string, block *model.Block) (map[uint32]bool, error) {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "processMissingSubtreesStreaming",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[processMissingSubtreesStreaming] Processing %d missing subtrees for block %s at height %d", len(missingSubtrees), block.Hash().String(), block.Height),
	)
	defer deferFn()

	// Get block headers for validation
	blockHeaderIDs, err := u.blockchainClient.GetBlockHeaderIDs(ctx, block.Header.HashPrevBlock, uint64(u.settings.GetUtxoStoreBlockHeightRetention()*2))
	if err != nil {
		return nil, errors.NewProcessingError("[processMissingSubtreesStreaming] Failed to get block headers from blockchain client", err)
	}

	blockIds := make(map[uint32]bool, len(blockHeaderIDs))
	for _, blockID := range blockHeaderIDs {
		blockIds[blockID] = true
	}

	// Create streaming processor
	sp := newStreamingProcessor(u, *block.Hash(), block.Height, blockIds)

	// Setup validation options
	validatorOptions := []validator.Option{
		validator.WithSkipPolicyChecks(true),
		validator.WithCreateConflicting(true),
		validator.WithIgnoreLocked(true),
	}

	currentState, err := u.blockchainClient.GetFSMCurrentState(ctx)
	if err != nil {
		return nil, errors.NewProcessingError("[processMissingSubtreesStreaming] Failed to get FSM current state", err)
	}

	if *currentState == blockchain.FSMStateLEGACYSYNCING || *currentState == blockchain.FSMStateCATCHINGBLOCKS {
		validatorOptions = append(validatorOptions, validator.WithAddTXToBlockAssembly(false))
	}

	sp.validatorOptions = validator.ProcessOptions(validatorOptions...)

	dah := u.utxoStore.GetBlockHeight() + u.settings.GetSubtreeValidationBlockHeightRetention()

	// Phase 1 & 2: Stream, filter, and process transactions in pipeline
	u.logger.Infof("[processMissingSubtreesStreaming] Starting streaming pipeline for %d subtrees", len(missingSubtrees))

	unvalidatedTxChan := make(chan *bt.Tx, 100000) // Large buffer for throughput
	streamErr := make(chan error, 1)

	// Launch Phase 1: Sequential subtree streaming + filtering
	go func() {
		defer close(unvalidatedTxChan)
		err := sp.streamAndFilterSubtrees(ctx, missingSubtrees, request, peerID, dah, unvalidatedTxChan)
		if err != nil {
			streamErr <- err
		}
	}()

	// Phase 2: Stream classification and processing
	if err := sp.classifyAndProcessStreaming(ctx, unvalidatedTxChan); err != nil {
		return nil, errors.NewProcessingError("[processMissingSubtreesStreaming] Failed to process transactions", err)
	}

	// Check for streaming errors
	select {
	case err := <-streamErr:
		return nil, errors.NewProcessingError("[processMissingSubtreesStreaming] Failed to stream subtrees", err)
	default:
	}

	// Check for errors before proceeding to subtree validation
	if sp.errorsFound.Load() > 0 {
		return blockIds, errors.NewProcessingError("[processMissingSubtreesStreaming] Completed transaction processing with %d errors, %d added to orphanage",
			sp.errorsFound.Load(), sp.addedToOrphanage.Load())
	}

	u.logger.Infof("[processMissingSubtreesStreaming] Streaming pipeline complete: validated %d transactions, skipped %d with 0 errors",
		sp.validatedTransactions.Load(), sp.skippedTransactions.Load())

	// Phase 3: Validate subtrees (now that all transactions are validated)
	u.logger.Infof("[processMissingSubtreesStreaming] Phase 3: Validating %d subtrees", len(missingSubtrees))

	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, u.settings.SubtreeValidation.CheckBlockSubtreesConcurrency)

	var revalidateSubtreesMutex sync.Mutex
	revalidateSubtrees := make([]chainhash.Hash, 0)

	for _, subtreeHash := range missingSubtrees {
		subtreeHash := subtreeHash

		g.Go(func() error {
			v := ValidateSubtree{
				SubtreeHash:   subtreeHash,
				BaseURL:       request.BaseUrl,
				AllowFailFast: false,
				PeerID:        peerID,
			}

			subtree, err := u.ValidateSubtreeInternal(
				gCtx,
				v,
				block.Height,
				blockIds,
				validator.WithSkipPolicyChecks(true),
				validator.WithCreateConflicting(true),
				validator.WithIgnoreLocked(true),
			)
			if err != nil {
				u.logger.Debugf("[processMissingSubtreesStreaming] Failed to validate subtree %s: %v", subtreeHash.String(), err)
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

	if err := g.Wait(); err != nil {
		return nil, errors.WrapGRPC(errors.NewProcessingError("[processMissingSubtreesStreaming] Failed during parallel subtree validation", err))
	}

	// Revalidate any failed subtrees sequentially
	for _, subtreeHash := range revalidateSubtrees {
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
			return nil, errors.WrapGRPC(errors.NewProcessingError("[processMissingSubtreesStreaming] Failed to validate subtree %s", subtreeHash.String(), err))
		}

		for _, node := range subtree.Nodes {
			u.orphanage.Delete(node.Hash)
		}
	}

	u.logger.Infof("[processMissingSubtreesStreaming] Successfully processed all subtrees. Validated: %d, Skipped (already validated): %d",
		sp.validatedTransactions.Load(), sp.skippedTransactions.Load())

	return blockIds, nil
}

// loadSubtreeTransactions loads transactions from a single subtree.
// This is extracted from the original processMissingSubtrees logic.
func (u *Server) loadSubtreeTransactions(ctx context.Context, request *subtreevalidation_api.CheckBlockSubtreesRequest, subtreeHash chainhash.Hash, peerID string, dah uint32) ([]*bt.Tx, error) {
	// Check if subtreeToCheck exists
	subtreeToCheckExists, err := u.subtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
	if err != nil {
		return nil, errors.NewProcessingError("[loadSubtreeTransactions][%s] failed to check if subtree exists", subtreeHash.String(), err)
	}

	var subtreeToCheck *subtreepkg.Subtree

	if subtreeToCheckExists {
		// Get the subtreeToCheck from the store
		subtreeReader, err := u.subtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
		if err != nil {
			return nil, errors.NewStorageError("[loadSubtreeTransactions][%s] failed to get subtree from store", subtreeHash.String(), err)
		}
		defer subtreeReader.Close()

		bufferedReader := bufioReaderPool.Get().(*bufio.Reader)
		bufferedReader.Reset(subtreeReader)
		defer func() {
			bufferedReader.Reset(nil)
			bufioReaderPool.Put(bufferedReader)
		}()

		subtreeToCheck, err = subtreepkg.NewSubtreeFromReader(bufferedReader)
		if err != nil {
			return nil, errors.NewProcessingError("[loadSubtreeTransactions][%s] failed to deserialize subtree", subtreeHash.String(), err)
		}
	} else {
		// Get the subtree from the peer
		url := fmt.Sprintf("%s/subtree/%s", request.BaseUrl, subtreeHash.String())

		subtreeNodeBytes, err := util.DoHTTPRequest(ctx, url)
		if err != nil {
			return nil, errors.NewServiceError("[loadSubtreeTransactions][%s] failed to get subtree from %s", subtreeHash.String(), url, err)
		}

		if u.p2pClient != nil && peerID != "" {
			if err := u.p2pClient.RecordBytesDownloaded(ctx, peerID, uint64(len(subtreeNodeBytes))); err != nil {
				u.logger.Warnf("[loadSubtreeTransactions][%s] failed to record bytes downloaded: %v", subtreeHash.String(), err)
			}
		}

		subtreeToCheck, err = subtreepkg.NewIncompleteTreeByLeafCount(len(subtreeNodeBytes) / chainhash.HashSize)
		if err != nil {
			return nil, errors.NewProcessingError("[loadSubtreeTransactions][%s] failed to create subtree structure", subtreeHash.String(), err)
		}

		var nodeHash chainhash.Hash
		for i := 0; i < len(subtreeNodeBytes)/chainhash.HashSize; i++ {
			copy(nodeHash[:], subtreeNodeBytes[i*chainhash.HashSize:(i+1)*chainhash.HashSize])

			if nodeHash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
				if err = subtreeToCheck.AddCoinbaseNode(); err != nil {
					return nil, errors.NewProcessingError("[loadSubtreeTransactions][%s] failed to add coinbase node", subtreeHash.String(), err)
				}
			} else {
				if err = subtreeToCheck.AddNode(nodeHash, 0, 0); err != nil {
					return nil, errors.NewProcessingError("[loadSubtreeTransactions][%s] failed to add node", subtreeHash.String(), err)
				}
			}
		}

		if !subtreeHash.Equal(*subtreeToCheck.RootHash()) {
			return nil, errors.NewProcessingError("[loadSubtreeTransactions][%s] subtree root hash mismatch: %s", subtreeHash.String(), subtreeToCheck.RootHash().String())
		}

		subtreeBytes, err := subtreeToCheck.Serialize()
		if err != nil {
			return nil, errors.NewProcessingError("[loadSubtreeTransactions][%s] failed to serialize subtree", subtreeHash.String(), err)
		}

		if err = u.subtreeStore.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes, options.WithDeleteAt(dah), options.WithAllowOverwrite(true)); err != nil {
			return nil, errors.NewProcessingError("[loadSubtreeTransactions][%s] failed to store subtree", subtreeHash.String(), err)
		}
	}

	// Check if subtreeData exists
	subtreeDataExists, err := u.subtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
	if err != nil {
		return nil, errors.NewProcessingError("[loadSubtreeTransactions][%s] failed to check if subtree data exists", subtreeHash.String(), err)
	}

	var txs []*bt.Tx

	if !subtreeDataExists {
		// Get the subtree data from the peer
		url := fmt.Sprintf("%s/subtree_data/%s", request.BaseUrl, subtreeHash.String())

		body, err := util.DoHTTPRequestBodyReader(ctx, url)
		if err != nil {
			return nil, errors.NewServiceError("[loadSubtreeTransactions][%s] failed to get subtree data from %s", subtreeHash.String(), url, err)
		}

		var bytesRead uint64
		countingBody := &countingReadCloser{
			reader:    body,
			bytesRead: &bytesRead,
		}

		// Read transactions and store data
		txs = make([]*bt.Tx, 0, subtreeToCheck.Length())
		var buffer bytes.Buffer
		teeReader := io.TeeReader(countingBody, &buffer)

		txCount, err := u.readTransactionsFromSubtreeDataStreamToSlice(subtreeToCheck, teeReader, &txs)
		_ = countingBody.Close()

		if u.p2pClient != nil && peerID != "" {
			trackCtx, _, traceDeferFn := tracing.DecoupleTracingSpan(ctx, "subtreevalidation", "recordBytesDownloaded")
			defer traceDeferFn()
			if err := u.p2pClient.RecordBytesDownloaded(trackCtx, peerID, bytesRead); err != nil {
				u.logger.Warnf("[loadSubtreeTransactions][%s] failed to record bytes downloaded: %v", subtreeHash.String(), err)
			}
		}

		if err != nil {
			return nil, errors.NewProcessingError("[loadSubtreeTransactions][%s] failed to read transactions", subtreeHash.String(), err)
		}

		if txCount != subtreeToCheck.Length() {
			return nil, errors.NewProcessingError("[loadSubtreeTransactions][%s] transaction count mismatch: expected %d, got %d", subtreeHash.String(), subtreeToCheck.Length(), txCount)
		}

		// Store the subtree data
		if err = u.subtreeStore.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData, buffer.Bytes(), options.WithDeleteAt(dah)); err != nil {
			return nil, errors.NewProcessingError("[loadSubtreeTransactions][%s] failed to store subtree data", subtreeHash.String(), err)
		}
	} else {
		// Extract from existing file
		txs = make([]*bt.Tx, 0, subtreeToCheck.Length())
		if err := u.extractTransactionsToSlice(ctx, subtreeToCheck, &txs); err != nil {
			return nil, errors.NewProcessingError("[loadSubtreeTransactions][%s] failed to extract transactions", subtreeHash.String(), err)
		}
	}

	return txs, nil
}

// readTransactionsFromSubtreeDataStreamToSlice reads transactions from a stream into a slice.
func (u *Server) readTransactionsFromSubtreeDataStreamToSlice(subtree *subtreepkg.Subtree, reader io.Reader, txs *[]*bt.Tx) (int, error) {
	txIndex := 0

	if len(subtree.Nodes) > 0 && subtree.Nodes[0].Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
		txIndex = 1
	}

	for {
		tx := &bt.Tx{}

		_, err := tx.ReadFrom(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return txIndex, errors.NewProcessingError("[readTransactionsFromSubtreeDataStreamToSlice] error reading transaction", err)
		}

		if tx.IsCoinbase() && txIndex == 1 {
			txIndex = 0
		}

		tx.SetTxHash(tx.TxIDChainHash())

		if txIndex < subtree.Length() {
			expectedHash := subtree.Nodes[txIndex].Hash
			if !expectedHash.Equal(*tx.TxIDChainHash()) {
				return txIndex, errors.NewProcessingError("[readTransactionsFromSubtreeDataStreamToSlice] hash mismatch at index %d", txIndex)
			}
		} else {
			return txIndex, errors.NewProcessingError("[readTransactionsFromSubtreeDataStreamToSlice] more transactions than expected")
		}

		*txs = append(*txs, tx)
		txIndex++
	}

	return txIndex, nil
}

// extractTransactionsToSlice extracts transactions from stored subtree data into a slice.
func (u *Server) extractTransactionsToSlice(ctx context.Context, subtree *subtreepkg.Subtree, txs *[]*bt.Tx) error {
	subtreeDataReader, err := u.subtreeStore.GetIoReader(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData)
	if err != nil {
		return errors.NewStorageError("[extractTransactionsToSlice] failed to get subtreeData from store", err)
	}
	defer subtreeDataReader.Close()

	bufferedReader := bufioReaderPool.Get().(*bufio.Reader)
	bufferedReader.Reset(subtreeDataReader)
	defer func() {
		bufferedReader.Reset(nil)
		bufioReaderPool.Put(bufferedReader)
	}()

	txCount, err := u.readTransactionsFromSubtreeDataStreamToSlice(subtree, bufferedReader, txs)
	if err != nil {
		return err
	}

	if txCount != subtree.Length() {
		return errors.NewProcessingError("[extractTransactionsToSlice] transaction count mismatch: expected %d, got %d", subtree.Length(), txCount)
	}

	return nil
}

// filterAlreadyValidated checks which transactions are already validated using BatchDecorate
// and returns only the unvalidated ones in their original order (preserving topological order).
// Uses ordered fan-in: batches process in parallel but results are emitted in batch order.
func (sp *streamingProcessor) filterAlreadyValidated(ctx context.Context, transactions []*bt.Tx) ([]*bt.Tx, error) {
	batchSize := sp.server.settings.BlockValidation.ProcessTxMetaUsingStoreBatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	numBatches := (len(transactions) + batchSize - 1) / batchSize
	if numBatches == 0 {
		return nil, nil
	}

	resultsChan := make(chan batchResult, numBatches)

	g, gCtx := errgroup.WithContext(ctx)
	concurrency := sp.server.settings.BlockValidation.ProcessTxMetaUsingStoreConcurrency
	if concurrency <= 0 {
		concurrency = max(4, runtime.NumCPU()/2)
	}
	util.SafeSetLimit(g, concurrency)

	// Launch batches with their index for ordered fan-in
	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		batchIdx := batchIdx
		startIdx := batchIdx * batchSize
		endIdx := startIdx + batchSize
		if endIdx > len(transactions) {
			endIdx = len(transactions)
		}

		g.Go(func() error {
			// Check if context is already cancelled
			select {
			case <-gCtx.Done():
				return gCtx.Err()
			default:
			}

			batch := make([]*utxo.UnresolvedMetaData, 0, endIdx-startIdx)
			txMap := make(map[int]*bt.Tx, endIdx-startIdx)

			for j := startIdx; j < endIdx; j++ {
				tx := transactions[j]
				if tx == nil || tx.IsCoinbase() {
					continue
				}
				batch = append(batch, &utxo.UnresolvedMetaData{
					Hash: *tx.TxIDChainHash(),
					Idx:  j,
				})
				txMap[j] = tx
			}

			if len(batch) == 0 {
				resultsChan <- batchResult{batchIdx: batchIdx, txs: nil}
				return nil
			}

			// Use parent context (ctx) instead of errgroup context (gCtx) to prevent
			// BatchDecorate from being cancelled when other goroutines complete or timeout.
			// This is important for large batches where database operations may take longer.
			if err := sp.server.utxoStore.BatchDecorate(ctx, batch, TxMetaFieldsForDecorate...); err != nil {
				return errors.NewStorageError("[filterAlreadyValidated] BatchDecorate failed", err)
			}

			// Collect unvalidated txs for this batch (preserves order within batch)
			var localUnvalidated []*bt.Tx
			for _, data := range batch {
				if data.Data != nil && data.Err == nil && !data.Data.Creating {
					// Already validated - skip
					sp.skippedTransactions.Add(1)
				} else {
					// Not validated - keep it
					if tx, ok := txMap[data.Idx]; ok {
						localUnvalidated = append(localUnvalidated, tx)
					}
				}
			}

			// Send batch result with index for ordered reassembly
			resultsChan <- batchResult{batchIdx: batchIdx, txs: localUnvalidated}
			return nil
		})
	}

	// Close channel when all batches complete
	go func() {
		_ = g.Wait()
		close(resultsChan)
	}()

	// Ordered consumer: collect results and emit in batch order (0, 1, 2, ...)
	pending := make(map[int][]*bt.Tx)
	nextBatch := 0
	var unvalidated []*bt.Tx

	for result := range resultsChan {
		pending[result.batchIdx] = result.txs

		// Drain all consecutive batches starting from nextBatch
		for {
			if txs, ok := pending[nextBatch]; ok {
				if len(txs) > 0 {
					unvalidated = append(unvalidated, txs...)
				}
				delete(pending, nextBatch)
				nextBatch++
			} else {
				break
			}
		}
	}

	// Check for errgroup errors after channel is drained
	if err := g.Wait(); err != nil {
		return nil, err
	}

	return unvalidated, nil
}

// processBuckets processes transactions level by level.
// It processes all transactions in readyBatch, which may be populated by
// classifyAndProcess and by onTxProcessed during processing.
func (sp *streamingProcessor) processBuckets(ctx context.Context, maxBucket int) error {
	// Keep processing until readyBatch is empty
	// Each round may add more items to readyBatch via onTxProcessed
	// Note: maxBucket is only used for progress tracking, not as a hard limit
	round := 0
	maxRounds := sp.totalTransactions // Worst case: one transaction per round (chain)

	// If totalTransactions wasn't set (e.g., in unit tests), use a very large default
	if maxRounds == 0 {
		maxRounds = 1_000_000 // Generous default for unit tests
	}

	for round < maxRounds {
		if len(sp.readyBatch) == 0 {
			break
		}

		sp.server.logger.Debugf("[processBuckets] Round %d: Processing %d ready transactions", round+1, len(sp.readyBatch))

		// Copy and clear readyBatch before processing
		batchToProcess := make([]*pendingTx, len(sp.readyBatch))
		copy(batchToProcess, sp.readyBatch)
		sp.readyBatch = sp.readyBatch[:0]

		if err := sp.processBatch(ctx, batchToProcess); err != nil {
			return err
		}

		round++
	}

	// Sanity check: there should be no remaining items
	if len(sp.readyBatch) > 0 {
		return errors.NewProcessingError("[processBuckets] Exceeded max rounds (%d) with %d transactions still pending", maxRounds, len(sp.readyBatch))
	}

	return nil
}

// processBatch validates a batch of transactions in parallel.
func (sp *streamingProcessor) processBatch(ctx context.Context, batch []*pendingTx) error {
	if len(batch) == 0 {
		return nil
	}

	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, sp.concurrency)

	for _, ptx := range batch {
		ptx := ptx

		g.Go(func() error {
			txMeta, err := sp.server.blessMissingTransaction(gCtx, sp.blockHash, chainhash.Hash{}, ptx.tx, sp.blockHeight, sp.blockIds, sp.validatorOptions)
			if err != nil {
				sp.server.logger.Debugf("[processBatch] Failed to validate transaction %s: %v", ptx.tx.TxIDChainHash().String(), err)

				if errors.Is(err, errors.ErrTxExists) {
					sp.server.logger.Debugf("[processBatch] Transaction %s already exists", ptx.tx.TxIDChainHash().String())
					sp.onTxProcessed(ptx.tx)
					return nil
				}

				sp.errorsFound.Add(1)

				if errors.Is(err, errors.ErrTxMissingParent) {
					isRunning, runningErr := sp.server.blockchainClient.IsFSMCurrentState(gCtx, blockchain.FSMStateRUNNING)
					if runningErr == nil && isRunning {
						if sp.server.orphanage.Set(*ptx.tx.TxIDChainHash(), ptx.tx) {
							sp.addedToOrphanage.Add(1)
						}
					}
				} else if errors.Is(err, errors.ErrTxInvalid) {
					sp.server.logger.Warnf("[processBatch] Invalid transaction: %s: %v", ptx.tx.TxIDChainHash().String(), err)
				} else {
					sp.server.logger.Errorf("[processBatch] Processing error: %s: %v", ptx.tx.TxIDChainHash().String(), err)
				}

				// Don't call onTxProcessed - children of failed tx cannot be processed.
				// Return nil (not error) to continue processing other independent tx chains in the batch.
				return nil
			}

			if txMeta != nil {
				sp.validatedTransactions.Add(1)
			}

			sp.onTxProcessed(ptx.tx)
			return nil
		})
	}

	return g.Wait()
}

// onTxProcessed is called after a transaction is processed.
// It updates children waiting on this transaction and moves them to readyBatch if all dependencies are resolved.
func (sp *streamingProcessor) onTxProcessed(tx *bt.Tx) {
	txHash := *tx.TxIDChainHash()

	sp.readyMu.Lock()
	defer sp.readyMu.Unlock()

	children, exists := sp.waitingOnParent[txHash]
	if !exists {
		return
	}

	for _, child := range children {
		newDeps := atomic.AddInt32(&child.remainingDeps, -1)
		if newDeps == 0 {
			sp.readyBatch = append(sp.readyBatch, child)
		}
	}

	delete(sp.waitingOnParent, txHash)
}

// streamAndFilterSubtrees sequentially loads subtrees, filters already-validated transactions,
// and streams unvalidated transactions to the output channel.
func (sp *streamingProcessor) streamAndFilterSubtrees(
	ctx context.Context,
	missingSubtrees []chainhash.Hash,
	request *subtreevalidation_api.CheckBlockSubtreesRequest,
	peerID string,
	dah uint32,
	unvalidatedTxChan chan<- *bt.Tx,
) error {
	for i, subtreeHash := range missingSubtrees {
		// Load subtree transactions
		txs, err := sp.server.loadSubtreeTransactions(ctx, request, subtreeHash, peerID, dah)
		if err != nil {
			return err
		}

		if len(txs) == 0 {
			continue
		}

		// Filter already-validated transactions
		unvalidatedTxs, err := sp.filterAlreadyValidated(ctx, txs)
		if err != nil {
			return err
		}

		sp.server.logger.Debugf("[streamAndFilterSubtrees] Subtree %d/%d (%s): %d txs, %d unvalidated",
			i+1, len(missingSubtrees), subtreeHash.String(), len(txs), len(unvalidatedTxs))

		// Stream unvalidated transactions to channel
		for _, tx := range unvalidatedTxs {
			select {
			case unvalidatedTxChan <- tx:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	sp.server.logger.Infof("[streamAndFilterSubtrees] Completed streaming %d subtrees", len(missingSubtrees))
	return nil
}

// classifyAndProcessStreaming performs single-pass classification and processing
// leveraging topological ordering of transactions.
func (sp *streamingProcessor) classifyAndProcessStreaming(
	ctx context.Context,
	txChan <-chan *bt.Tx,
) error {
	// txLevels tracks the dependency level of each transaction
	// Level 0 = no block parents, Level N = max(parent levels) + 1
	txLevels := make(map[chainhash.Hash]int)

	maxDeps := 0
	txCount := 0
	level0Count := 0

	// Single pass: classify and batch ready transactions as we stream
	for tx := range txChan {
		if tx == nil || tx.IsCoinbase() {
			continue
		}

		txCount++
		txHash := *tx.TxIDChainHash()

		// Calculate dependency level based on parent levels
		depLevel := 0
		var parentHashes []chainhash.Hash

		for _, input := range tx.Inputs {
			parentHash := *input.PreviousTxIDChainHash()
			if parentLevel, exists := txLevels[parentHash]; exists {
				// Parent is in this block - this tx's level is parent's level + 1
				if parentLevel+1 > depLevel {
					depLevel = parentLevel + 1
				}
				parentHashes = append(parentHashes, parentHash)
			}
		}

		// Mark this transaction's level
		txLevels[txHash] = depLevel
		if depLevel == 0 {
			level0Count++
		}

		if depLevel == 0 {
			// No dependencies on block transactions - add to ready batch
			// Don't process yet - children might not be classified yet
			sp.readyBatch = append(sp.readyBatch, &pendingTx{
				tx:            tx,
				remainingDeps: 0,
			})
		} else {
			// Has dependencies - create pending entry
			// remainingDeps = number of parent transactions (will be decremented as parents complete)
			ptx := &pendingTx{
				tx:            tx,
				remainingDeps: int32(len(parentHashes)),
			}

			// Register as waiting on each parent
			for _, parentHash := range parentHashes {
				sp.waitingOnParent[parentHash] = append(sp.waitingOnParent[parentHash], ptx)
			}

			if depLevel > maxDeps {
				maxDeps = depLevel
			}
		}
	}

	// Store total for processBuckets loop limit
	sp.totalTransactions = txCount

	sp.server.logger.Infof("[classifyAndProcessStreaming] Classified %d transactions: %d at level 0, %d with dependencies (maxDeps=%d). (Level 0 = no block parents, expected for coinbase spenders)",
		txCount, level0Count, len(sp.waitingOnParent), maxDeps)

	// Now that ALL transactions are classified, process level 0
	// This will cascade through levels as onTxProcessed decrements children
	if len(sp.readyBatch) > 0 {
		sp.server.logger.Debugf("[classifyAndProcessStreaming] Processing level 0: %d transactions", len(sp.readyBatch))
		if err := sp.flushReadyBatch(ctx); err != nil {
			return err
		}
	}

	// Process remaining dependency levels via cascade
	if maxDeps > 0 {
		sp.server.logger.Infof("[classifyAndProcessStreaming] Processing remaining %d dependency levels", maxDeps)
		if err := sp.processBuckets(ctx, maxDeps); err != nil {
			return err
		}
	}

	return nil
}

// flushReadyBatch processes the current ready batch and clears it.
func (sp *streamingProcessor) flushReadyBatch(ctx context.Context) error {
	batchToProcess := make([]*pendingTx, len(sp.readyBatch))
	copy(batchToProcess, sp.readyBatch)
	sp.readyBatch = sp.readyBatch[:0]
	return sp.processBatch(ctx, batchToProcess)
}

// classifyAndProcessForTest is a test helper that adapts the old API to new streaming API.
// It simulates streaming by sending all transactions through a channel.
func (sp *streamingProcessor) classifyAndProcessForTest(ctx context.Context, transactions []*bt.Tx) error {
	txChan := make(chan *bt.Tx, len(transactions))
	for _, tx := range transactions {
		txChan <- tx
	}
	close(txChan)

	return sp.classifyAndProcessStreaming(ctx, txChan)
}
