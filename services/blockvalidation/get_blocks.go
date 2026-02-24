// This file contains block fetching utilities for catchup operations.
package blockvalidation

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

// Work item represents a block with its position for ordered delivery
type workItem struct {
	block *model.Block
	index int // Position in original sequence for ordering
}

// Result item represents completed work
type resultItem struct {
	block *model.Block
	index int
	err   error
}

// fetchBlocksConcurrently fetches blocks from a peer using a high-performance worker pool architecture.
// This function implements:
// 1. Large batch fetching (~100 blocks per HTTP request) for maximum throughput
// 2. Immediate distribution to multiple workers for parallel subtree data fetching
// 3. Strict ordered delivery to validation channel after all subtree data is ready
//
// Architecture:
//
//	[Large Batch Fetch] → [Work Queue] → [Worker Pool] → [Ordered Buffer] → [validateBlocksChan]
//
// Parameters:
//   - gCtx: Context for cancellation
//   - catchupCtx: Context containing block headers and peer info
//   - validateBlocksChan: Channel to send blocks for validation
//   - size: Atomic counter for remaining blocks
//
// Returns:
//   - error: If fetching fails
func (u *Server) fetchBlocksConcurrently(ctx context.Context, catchupCtx *CatchupContext, validateBlocksChan chan *model.Block, size *atomic.Int64) error {
	blockUpTo := catchupCtx.blockUpTo
	baseURL := catchupCtx.baseURL
	peerID := catchupCtx.peerID
	blockHeaders := catchupCtx.blockHeaders

	if len(blockHeaders) == 0 {
		close(validateBlocksChan)
		return nil
	}

	// Start tracing span for the entire operation
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "fetchBlocksConcurrently",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[catchup:fetchBlocksConcurrently][%s] starting high-performance pipeline for %d blocks from %s", blockUpTo.Hash().String(), len(blockHeaders), baseURL),
	)
	defer deferFn()

	// Configuration for high-performance pipeline
	// All values come from settings with sensible defaults:
	// - FetchLargeBatchSize (100): Blocks per HTTP request for efficiency
	// - FetchNumWorkers (16): Parallel workers for subtree fetching
	// - FetchBufferSize (50): Channel buffer size - keeps workers ~100-150 blocks ahead max
	largeBatchSize := u.settings.BlockValidation.FetchLargeBatchSize
	numWorkers := u.settings.BlockValidation.FetchNumWorkers
	bufferSize := u.settings.BlockValidation.FetchBufferSize

	// Channels for pipeline stages
	workQueue := make(chan workItem, bufferSize)
	resultQueue := make(chan resultItem, bufferSize)

	// Create local error group for better error handling and cancellation
	g, gCtx := errgroup.WithContext(ctx)

	// Start worker pool for parallel subtree data fetching
	for i := 0; i < numWorkers; i++ {
		workerID := i
		g.Go(func() error {
			return u.blockWorker(gCtx, workerID, workQueue, resultQueue, peerID, baseURL, blockUpTo)
		})
	}

	// Start ordered delivery goroutine
	g.Go(func() error {
		return u.orderedDelivery(gCtx, resultQueue, validateBlocksChan, len(blockHeaders), blockUpTo, size)
	})

	// Start batch fetching and work distribution
	g.Go(func() error {
		defer close(workQueue)

		// In production, commonAncestorMeta is always set during catchup initialization
		if catchupCtx == nil {
			return errors.NewProcessingError("[catchup:fetchBlocksConcurrently][%s] catchupCtx must not be nil", blockUpTo.Hash().String())
		}

		if catchupCtx.commonAncestorMeta == nil {
			return errors.NewProcessingError("[catchup:fetchBlocksConcurrently][%s] commonAncestorMeta must not be nil", blockUpTo.Hash().String())
		}

		// Calculate starting height from common ancestor
		startingHeight := catchupCtx.commonAncestorMeta.Height + 1

		return u.batchFetchAndDistribute(gCtx, blockHeaders, workQueue, peerID, baseURL, blockUpTo, largeBatchSize, startingHeight)
	})

	// Wait for all goroutines to complete
	// Note: resultQueue is not closed explicitly; termination is orchestrated by:
	// 1. Context cancellation propagates to all goroutines
	// 2. orderedDelivery returns when all totalBlocks are processed or on error
	// 3. Workers naturally terminate when workQueue is closed and drained
	// 4. Any error in the pipeline cancels the context, stopping all producers/workers
	return g.Wait()
}

// batchFetchAndDistribute fetches blocks in large batches and immediately distributes them to workers
func (u *Server) batchFetchAndDistribute(ctx context.Context, blockHeaders []*model.BlockHeader, workQueue chan<- workItem, peerID string, baseURL string, blockUpTo *model.Block, batchSize int, startingHeight uint32) error {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "batchFetchAndDistribute",
		tracing.WithParentStat(u.stats),
	)
	defer deferFn()

	u.logger.Debugf("[catchup:batchFetchAndDistribute][%s] fetching %d blocks in batches of %d", blockUpTo.Hash().String(), len(blockHeaders), batchSize)

	currentIndex := 0
	for i := 0; i < len(blockHeaders); i += batchSize {
		end := i + batchSize
		if end > len(blockHeaders) {
			end = len(blockHeaders)
		}

		batchHeaders := blockHeaders[i:end]
		u.logger.Debugf("[catchup:batchFetchAndDistribute][%s] fetching batch %d-%d (%d blocks)",
			blockUpTo.Hash().String(), i, end-1, len(batchHeaders))

		// Fetch entire batch in one HTTP request, from last block, since the data is returned newest-first
		blocks, err := u.fetchBlocksBatch(ctx, batchHeaders[len(batchHeaders)-1].Hash(), uint32(len(batchHeaders)), peerID, baseURL)
		if err != nil {
			return errors.NewProcessingError("[catchup:batchFetchAndDistribute][%s] failed to fetch batch starting at %s", blockUpTo.Hash().String(), batchHeaders[0].Hash().String(), err)
		}

		if len(blocks) != len(batchHeaders) {
			return errors.NewProcessingError("[catchup:batchFetchAndDistribute][%s] expected %d blocks, got %d", blockUpTo.Hash().String(), len(batchHeaders), len(blocks))
		}

		// reverse the blocks to match the order of headers
		for j, k := 0, len(blocks)-1; j < k; j, k = j+1, k-1 {
			blocks[j], blocks[k] = blocks[k], blocks[j]
		}

		// Verify each fetched block matches the expected header
		for j, block := range blocks {
			if block.Hash().String() != batchHeaders[j].Hash().String() {
				return errors.NewProcessingError("[catchup:batchFetchAndDistribute][%s] block hash mismatch at index %d: expected %s, got %s", blockUpTo.Hash().String(), j, batchHeaders[j].Hash().String(), block.Hash().String())
			}
		}

		// Immediately distribute blocks to workers
		for _, block := range blocks {
			// Set block height based on its position in the chain
			block.Height = startingHeight + uint32(currentIndex)

			select {
			case workQueue <- workItem{
				block: block,
				index: currentIndex,
			}:
				currentIndex++
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	u.logger.Debugf("[catchup:batchFetchAndDistribute][%s] completed distribution of %d blocks", blockUpTo.Hash().String(), currentIndex)
	return nil
}

// blockWorker processes blocks and fetches their subtree data in parallel
func (u *Server) blockWorker(ctx context.Context, workerID int, workQueue <-chan workItem, resultQueue chan<- resultItem,
	peerID, baseURL string, blockUpTo *model.Block) error {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "blockWorker",
		tracing.WithParentStat(u.stats),
		tracing.WithDebugLogMessage(u.logger, "[catchup:blockWorker-%d][%s] starting worker", workerID, blockUpTo.Hash().String()),
	)
	defer deferFn()

	for {
		select {
		case work, ok := <-workQueue:
			if !ok {
				u.logger.Debugf("[catchup:blockWorker-%d][%s] work queue closed, worker shutting down", workerID, blockUpTo.Hash().String())
				return nil
			}

			// Fetch subtree data for this block
			err := u.fetchSubtreeDataForBlock(ctx, work.block, peerID, baseURL)
			if err != nil {
				// Send result (even if error occurred)
				result := resultItem{
					block: work.block,
					index: work.index,
					err:   err,
				}

				select {
				case resultQueue <- result:
				case <-ctx.Done():
					return ctx.Err()
				}

				continue
			}

			// Send result
			result := resultItem{
				block: work.block,
				index: work.index,
			}

			select {
			case resultQueue <- result:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// orderedDelivery ensures blocks are delivered to validateBlocksChan in strict order
func (u *Server) orderedDelivery(gCtx context.Context, resultQueue <-chan resultItem, validateBlocksChan chan<- *model.Block, totalBlocks int, blockUpTo *model.Block, size *atomic.Int64) error {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(gCtx, "orderedDelivery",
		tracing.WithParentStat(u.stats),
		tracing.WithDebugLogMessage(u.logger, "[catchup:orderedDelivery][%s] starting ordered delivery for %d blocks", blockUpTo.Hash().String(), totalBlocks),
	)
	defer func() {
		deferFn()
		close(validateBlocksChan)
	}()

	// Buffer to hold results until they can be delivered in order
	results := make(map[int]resultItem)
	nextIndex := 0
	receivedCount := 0

	for receivedCount < totalBlocks {
		select {
		case result, ok := <-resultQueue:
			if !ok {
				return errors.NewProcessingError("[catchup:orderedDelivery][%s] result queue closed unexpectedly", blockUpTo.Hash().String())
			}

			receivedCount++

			if result.err != nil {
				return errors.NewProcessingError("[catchup:orderedDelivery][%s] worker failed for block %s", blockUpTo.Hash().String(), result.block.Hash().String(), result.err)
			}

			// Store result for ordered delivery
			results[result.index] = result

			// Deliver all consecutive blocks starting from nextIndex
			for {
				if orderedResult, exists := results[nextIndex]; exists {
					u.logger.Debugf("[catchup:orderedDelivery][%s] delivering block %s at index %d (received %d/%d)", blockUpTo.Hash().String(), orderedResult.block.Hash().String(), nextIndex, receivedCount, totalBlocks)

					select {
					case validateBlocksChan <- orderedResult.block:
						delete(results, nextIndex)
						nextIndex++
						// Note: size counter is decremented by validateBlocksOnChannel after processing
					case <-ctx.Done():
						return ctx.Err()
					}
				} else {
					u.logger.Debugf("[catchup:orderedDelivery][%s] received result for block %s at index %d, processing later (received %d/%d)", blockUpTo.Hash().String(), result.block.Hash().String(), result.index, receivedCount, totalBlocks)

					break
				}
			}

			// Check if we've delivered all blocks (not just received)
			if nextIndex == totalBlocks {
				u.logger.Debugf("[catchup:orderedDelivery][%s] completed ordered delivery of %d blocks", blockUpTo.Hash().String(), totalBlocks)
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// fetchSubtreeDataForBlock fetches subtree and subtreeData for all subtrees in a block
// and stores them in the subtreeStore for later use by block validation.
// This function fetches both the subtree (for subtreeToCheck) and raw subtree data concurrently.
// When parallel fetching is enabled, subtrees are distributed across multiple peers at max height.
func (u *Server) fetchSubtreeDataForBlock(gCtx context.Context, block *model.Block, peerID, baseURL string) error {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(gCtx, "fetchSubtreeDataForBlock",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[catchup:fetchSubtreeDataForBlock][%s] fetching subtree data for block with %d subtrees", block.Hash().String(), len(block.Subtrees)),
	)
	defer deferFn()

	if len(block.Subtrees) == 0 {
		u.logger.Debugf("[catchup:fetchSubtreeDataForBlock] Block %s has no subtrees, skipping", block.Hash().String())

		return nil
	}

	// Create error group for concurrent subtree fetching
	g, ctx := errgroup.WithContext(ctx)
	// Limit concurrency to avoid overwhelming the peer
	// This can be adjusted based on peer capabilities and network conditions
	subtreeConcurrency := 8 // Default value
	if u.settings.BlockValidation.SubtreeFetchConcurrency > 0 {
		subtreeConcurrency = u.settings.BlockValidation.SubtreeFetchConcurrency
	}
	g.SetLimit(subtreeConcurrency)

	// Get peer assignments for subtrees if parallel fetching is enabled
	var peerAssignments []*PeerForSubtreeFetch
	if u.settings.BlockValidation.CatchupParallelFetchEnabled && u.p2pClient != nil {
		var err error
		peerAssignments, err = DistributeSubtreesAcrossPeers(ctx, u.logger, u.p2pClient, peerID, baseURL, len(block.Subtrees))
		if err != nil {
			u.logger.Warnf("[catchup:fetchSubtreeDataForBlock][%s] Failed to distribute subtrees across peers: %v, using single peer", block.Hash().String(), err)
			peerAssignments = nil
		}
	}

	// Process each unique subtree concurrently
	for i, subtreeHash := range block.Subtrees {
		subtreeHashCopy := *subtreeHash // Capture for goroutine
		subtreeIndex := i

		// Determine which peer to use for this subtree
		fetchPeerID := peerID
		fetchBaseURL := baseURL
		if peerAssignments != nil && subtreeIndex < len(peerAssignments) {
			assignment := peerAssignments[subtreeIndex]
			fetchPeerID = assignment.PeerID
			fetchBaseURL = assignment.BaseURL
		}

		// Capture for goroutine
		capturedPeerID := fetchPeerID
		capturedBaseURL := fetchBaseURL

		g.Go(func() error {
			return u.fetchAndStoreSubtreeAndSubtreeData(ctx, block, &subtreeHashCopy, capturedPeerID, capturedBaseURL)
		})
	}

	// Wait for all subtree fetching to complete
	if err := g.Wait(); err != nil {
		return errors.NewServiceError("[catchup:fetchSubtreeDataForBlock] Failed to fetch subtree data for block %s", block.Hash().String(), err)
	}

	return nil
}

// fetchAndStoreSubtree fetches and stores only the subtree (for subtreeToCheck)
func (u *Server) fetchAndStoreSubtree(ctx context.Context, block *model.Block, subtreeHash *chainhash.Hash, peerID, baseURL string) (*subtreepkg.Subtree, error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "fetchAndStoreSubtree",
		tracing.WithParentStat(u.stats),
		// tracing.WithDebugLogMessage(u.logger, "[catchup:fetchAndStoreSubtree] fetching subtree for %s", subtreeHash.String()),
	)
	defer deferFn()

	dah := block.Height + u.settings.GetSubtreeValidationBlockHeightRetention()

	// Check if we already have the subtree
	subtreeExists, err := u.subtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
	if err != nil {
		return nil, errors.NewProcessingError("[catchup:fetchAndStoreSubtree] Error checking subtree existence for %s: %v", subtreeHash.String(), err)
	}

	if subtreeExists {
		u.logger.Debugf("[catchup:fetchAndStoreSubtree] Subtree already exists for %s, loading from store", subtreeHash.String())

		// Load existing subtree from store
		subtreeBytes, err := u.subtreeStore.Get(ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
		if err != nil {
			return nil, errors.NewStorageError("[catchup:fetchAndStoreSubtree] Failed to get existing subtree for %s", subtreeHash.String(), err)
		}

		subtree, err := subtreepkg.NewSubtreeFromBytes(subtreeBytes)
		if err != nil {
			return nil, errors.NewProcessingError("[catchup:fetchAndStoreSubtree] Failed to deserialize existing subtree for %s", subtreeHash.String(), err)
		}

		return subtree, nil
	}

	// Fetch subtree from peer
	subtreeNodeBytes, subtreeErr := u.fetchSubtreeFromPeer(ctx, subtreeHash, peerID, baseURL)
	if subtreeErr != nil {
		return nil, errors.NewServiceError("[catchup:fetchAndStoreSubtree] Failed to fetch subtree for %s", subtreeHash.String(), subtreeErr)
	}

	// in the subtree validation, we only use the hashes of the FileTypeSubtreeToCheck, which is what is returned from the peer
	numberOfNodes := len(subtreeNodeBytes) / chainhash.HashSize
	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(numberOfNodes)
	if err != nil {
		return nil, errors.NewProcessingError("[catchup:fetchAndStoreSubtree] Failed to create subtree with %d nodes for %s", numberOfNodes, subtreeHash.String(), err)
	}

	// Sanity check, subtrees should never be empty
	if numberOfNodes == 0 {
		return nil, errors.NewProcessingError("[catchup:fetchAndStoreSubtree] Subtree for %s has zero nodes", subtreeHash.String())
	}

	// Deserialize the subtree nodes from the bytes
	for i := 0; i < numberOfNodes; i++ {
		// Each node is a chainhash.Hash, so we read chainhash.HashSize bytes
		nodeBytes := subtreeNodeBytes[i*chainhash.HashSize : (i+1)*chainhash.HashSize]
		nodeHash, err := chainhash.NewHash(nodeBytes)
		if err != nil {
			return nil, errors.NewProcessingError("[catchup:fetchAndStoreSubtree] Failed to create hash from bytes for subtree %s at index %d", subtreeHash.String(), i, err)
		}

		if i == 0 && nodeHash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			if err = subtree.AddCoinbaseNode(); err != nil {
				return nil, errors.NewProcessingError("[catchup:fetchAndStoreSubtree] Failed to add coinbase node to subtree %s at index %d", subtreeHash.String(), i, err)
			}
			continue
		}

		// Add the node to the subtree, we do not know the fee or size yet, so we use 0
		if err = subtree.AddNode(*nodeHash, 0, 0); err != nil {
			return nil, errors.NewProcessingError("[catchup:fetchAndStoreSubtree] Failed to add node %s to subtree %s at index %d", nodeHash.String(), subtreeHash.String(), i, err)
		}
	}

	subtreeBytes, err := subtree.Serialize()
	if err != nil {
		return nil, errors.NewProcessingError("[catchup:fetchAndStoreSubtree] Failed to serialize subtree %s for %s", subtreeHash.String(), err)
	}

	// Store subtree (for subtreeToCheck) in subtreeStore
	if err = u.subtreeStore.Set(ctx,
		subtreeHash[:],
		fileformat.FileTypeSubtreeToCheck,
		subtreeBytes,
		options.WithAllowOverwrite(true),
		options.WithDeleteAt(dah),
	); err != nil {
		return nil, errors.NewStorageError("[catchup:fetchAndStoreSubtree] Failed to store subtreeToCheck for %s", subtreeHash.String(), err)
	}

	// Don't report subtree fetch during catchup - wait for full validation
	// Only report success after the entire block is validated
	// This prevents inflating reputation for peers providing invalid chains
	// if u.p2pClient != nil {
	// 	if err := u.p2pClient.ReportValidSubtree(ctx, peerID, subtreeHash.String()); err != nil {
	// 		u.logger.Warnf("[fetchAndStoreSubtree][%s] failed to report valid subtree: %v", subtreeHash.String(), err)
	// 	}
	// }

	return subtree, nil
}

// fetchAndStoreSubtreeData fetches and stores only the subtreeData
func (u *Server) fetchAndStoreSubtreeData(ctx context.Context, block *model.Block, subtreeHash *chainhash.Hash,
	subtree *subtreepkg.Subtree, peerID, baseURL string) error {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "fetchAndStoreSubtreeData",
		tracing.WithParentStat(u.stats),
		tracing.WithDebugLogMessage(u.logger, "[catchup:fetchAndStoreSubtreeData][%s] Fetching subtree data from peer %s (%s) for subtree %s", block.Hash().String(), peerID, baseURL, subtreeHash.String()),
	)
	defer deferFn()

	dah := block.Height + u.settings.GetSubtreeValidationBlockHeightRetention()

	// Check if we already have the subtreeData
	subtreeDataExists, err := u.subtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
	if err != nil {
		return errors.NewProcessingError("[catchup:fetchAndStoreSubtreeData] Error checking subtreeData existence for %s: %v", subtreeHash.String(), err)
	}

	if subtreeDataExists {
		u.logger.Debugf("[catchup:fetchAndStoreSubtreeData] SubtreeData already exists for %s, skipping fetch", subtreeHash.String())
		return nil
	}

	subtreeDataReader, err := u.fetchSubtreeDataFromPeer(ctx, subtreeHash, peerID, baseURL)
	if err != nil {
		return errors.NewProcessingError("[catchup:fetchAndStoreSubtreeData] Failed to fetch subtreeData for %s", subtreeHash.String(), err)
	}
	defer subtreeDataReader.Close()

	// Use pooled buffered reader to reduce GC pressure
	bufferedReader := bufioReaderPool.Get().(*bufio.Reader)
	bufferedReader.Reset(subtreeDataReader)
	defer func() {
		bufferedReader.Reset(nil)
		bufioReaderPool.Put(bufferedReader)
	}()
	subtreeDataBufferedReader := io.NopCloser(bufferedReader)

	// loading the subtree data like this will validate the data as it is read
	// compared to the transactions in the subtree
	subtreeData, err := subtreepkg.NewSubtreeDataFromReader(subtree, subtreeDataBufferedReader)
	if err != nil {
		return errors.NewProcessingError("[catchup:fetchAndStoreSubtreeData] Failed to create subtreeData for %s", subtreeHash.String(), err)
	}

	// Debug: Log how many transactions we actually got
	nonNilCount := 0
	for _, tx := range subtreeData.Txs {
		if tx != nil {
			nonNilCount++
		}
	}
	u.logger.Debugf("[catchup:fetchAndStoreSubtreeData] Subtree %s from %s has %d/%d non-nil transactions",
		subtreeHash.String(), baseURL, nonNilCount, len(subtreeData.Txs))

	// Try to serialize the subtreeData to validate it's complete
	subtreeDataBytes, err := subtreeData.Serialize()
	if err != nil {
		return errors.NewProcessingError("[catchup:fetchAndStoreSubtreeData] Peer %s (%s) provided incomplete subtree data for %s", peerID, baseURL, subtreeHash.String(), err)
	}

	// Store subtreeData (raw data) in subtreeStore
	if err = u.subtreeStore.Set(ctx,
		subtreeHash[:],
		fileformat.FileTypeSubtreeData,
		subtreeDataBytes,
		options.WithAllowOverwrite(true),
		options.WithDeleteAt(dah),
	); err != nil {
		return errors.NewStorageError("[catchup:fetchAndStoreSubtreeData] Failed to store subtreeData for %s", subtreeHash.String(), err)
	}

	return nil
}

// fetchAndStoreSubtreeAndSubtreeData fetches both subtree and subtreeData for a single subtree hash
// and stores them in the subtreeStore. If the primary peer fails, it will try alternative peers
// at max height before giving up.
func (u *Server) fetchAndStoreSubtreeAndSubtreeData(ctx context.Context, block *model.Block, subtreeHash *chainhash.Hash,
	peerID, baseURL string) error {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "fetchAndStoreSubtreeAndSubtreeData",
		tracing.WithParentStat(u.stats),
		// tracing.WithDebugLogMessage(u.logger, "[catchup:fetchAndStoreSubtreeAndSubtreeData] fetching subtree and data for %s", subtreeHash.String()),
	)
	defer deferFn()

	// Try primary peer first
	subtree, err := u.fetchAndStoreSubtree(ctx, block, subtreeHash, peerID, baseURL)
	if err == nil {
		// Primary peer succeeded for subtree, now try subtreeData
		if err = u.fetchAndStoreSubtreeData(ctx, block, subtreeHash, subtree, peerID, baseURL); err == nil {
			return nil // Success
		}
		u.logger.Warnf("[catchup:fetchAndStoreSubtreeAndSubtreeData] Primary peer %s failed to fetch subtreeData for %s: %v, trying alternatives", peerID, subtreeHash.String(), err)
	} else {
		u.logger.Warnf("[catchup:fetchAndStoreSubtreeAndSubtreeData] Primary peer %s failed to fetch subtree for %s: %v, trying alternatives", peerID, subtreeHash.String(), err)
	}

	// Primary peer failed, try alternative peers
	var lastErr error = err
	if u.p2pClient != nil {
		alternativePeers, getPeersErr := GetPeersAtMaxHeight(ctx, u.logger, u.p2pClient, peerID)
		if getPeersErr != nil {
			u.logger.Warnf("[catchup:fetchAndStoreSubtreeAndSubtreeData] Failed to get alternative peers: %v", getPeersErr)
		} else if len(alternativePeers) > 0 {
			u.logger.Infof("[catchup:fetchAndStoreSubtreeAndSubtreeData] Trying %d alternative peers for subtree %s", len(alternativePeers), subtreeHash.String())

			for _, altPeer := range alternativePeers {
				altPeerID := altPeer.ID.String()
				altBaseURL := altPeer.DataHubURL

				if altBaseURL == "" {
					continue
				}

				u.logger.Debugf("[catchup:fetchAndStoreSubtreeAndSubtreeData] Trying alternative peer %s for subtree %s", altPeerID, subtreeHash.String())

				// Try to fetch subtree from alternative peer
				subtree, err = u.fetchAndStoreSubtree(ctx, block, subtreeHash, altPeerID, altBaseURL)
				if err != nil {
					u.logger.Debugf("[catchup:fetchAndStoreSubtreeAndSubtreeData] Alternative peer %s failed for subtree %s: %v", altPeerID, subtreeHash.String(), err)
					lastErr = err
					continue
				}

				// Subtree succeeded, try subtreeData
				if err = u.fetchAndStoreSubtreeData(ctx, block, subtreeHash, subtree, altPeerID, altBaseURL); err != nil {
					u.logger.Debugf("[catchup:fetchAndStoreSubtreeAndSubtreeData] Alternative peer %s failed for subtreeData %s: %v", altPeerID, subtreeHash.String(), err)
					lastErr = err
					continue
				}

				// Success with alternative peer
				u.logger.Infof("[catchup:fetchAndStoreSubtreeAndSubtreeData] Successfully fetched subtree %s from alternative peer %s", subtreeHash.String(), altPeerID)
				return nil
			}
		}
	}

	// All peers failed
	return errors.NewServiceError("[catchup:fetchAndStoreSubtreeAndSubtreeData] All peers failed to fetch subtree %s, last error: %v", subtreeHash.String(), lastErr)
}

// fetchSubtreeFromPeer fetches subtree (for subtreeToCheck) from a peer via HTTP
func (u *Server) fetchSubtreeFromPeer(ctx context.Context, subtreeHash *chainhash.Hash, peerID string, baseURL string) ([]byte, error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "fetchSubtreeFromPeer",
		tracing.WithParentStat(u.stats),
	)
	defer deferFn()

	// Construct URL for subtree endpoint (for subtreeToCheck)
	url := fmt.Sprintf("%s/subtree/%s", baseURL, subtreeHash.String())

	u.logger.Debugf("[catchup:fetchSubtreeFromPeer] fetching subtree from %s", url)

	// Use the existing HTTP utility to fetch subtree
	subtreeBytes, err := util.DoHTTPRequest(ctx, url)
	if err != nil {
		return nil, errors.NewServiceError("[catchup:fetchSubtreeFromPeer] failed to fetch subtree from %s", url, err)
	}

	// Track bytes downloaded from peer
	if u.p2pClient != nil && peerID != "" {
		if err := u.p2pClient.RecordBytesDownloaded(ctx, peerID, uint64(len(subtreeBytes))); err != nil {
			u.logger.Warnf("[fetchSubtreeFromPeer][%s] failed to record %d bytes downloaded from peer %s: %v", subtreeHash.String(), len(subtreeBytes), peerID, err)
		}
	}

	if len(subtreeBytes) == 0 {
		return nil, errors.NewNotFoundError("[catchup:fetchSubtreeFromPeer] empty subtree received from %s", url)
	}

	u.logger.Debugf("[catchup:fetchSubtreeFromPeer] successfully fetched %d bytes of subtree from %s", len(subtreeBytes), url)

	return subtreeBytes, nil
}

// countingReadCloser wraps an io.ReadCloser and counts bytes read
type countingReadCloser struct {
	reader    io.ReadCloser
	bytesRead uint64
	onClose   func(uint64) // Callback when closed with total bytes read
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.reader.Read(p)
	c.bytesRead += uint64(n)
	return n, err
}

func (c *countingReadCloser) Close() error {
	if c.onClose != nil {
		c.onClose(c.bytesRead)
	}
	return c.reader.Close()
}

// fetchSubtreeDataFromPeer fetches subtree data from a peer via HTTP
func (u *Server) fetchSubtreeDataFromPeer(ctx context.Context, subtreeHash *chainhash.Hash, peerID string, baseURL string) (io.ReadCloser, error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "fetchSubtreeDataFromPeer",
		tracing.WithParentStat(u.stats),
	)
	defer deferFn()

	// Construct URL for subtree data endpoint
	// Based on user clarification, subtree data is fetched from /subtree_data/:hash
	url := fmt.Sprintf("%s/subtree_data/%s", baseURL, subtreeHash.String())

	u.logger.Debugf("[catchup:fetchSubtreeDataFromPeer] fetching subtree data from %s", url)

	// Use the existing HTTP utility to fetch subtree data
	subtreeDataReader, err := util.DoHTTPRequestBodyReader(ctx, url)
	if err != nil {
		return nil, errors.NewServiceError("[catchup:fetchSubtreeDataFromPeer] failed to fetch subtree data from %s", url, err)
	}

	// Wrap with counting reader to track bytes when stream is consumed
	countingReader := &countingReadCloser{
		reader: subtreeDataReader,
		onClose: func(bytesRead uint64) {
			// Track bytes downloaded from peer when reader is closed (after all data consumed)
			// Decouple the context to ensure tracking completes even if parent context is cancelled
			if u.p2pClient != nil && peerID != "" {
				trackCtx, _, deferFn := tracing.DecoupleTracingSpan(ctx, "blockvalidation", "recordBytesDownloaded")
				defer deferFn()
				if err := u.p2pClient.RecordBytesDownloaded(trackCtx, peerID, bytesRead); err != nil {
					u.logger.Warnf("[fetchSubtreeDataFromPeer][%s] failed to record %d bytes downloaded from peer %s: %v", subtreeHash.String(), bytesRead, peerID, err)
				}
			}
		},
	}

	return countingReader, nil
}

// fetchBlocksBatch fetches a batch of blocks from a peer starting from the specified hash.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - hash: Starting block hash
//   - n: Number of blocks to fetch
//   - baseURL: Peer URL to fetch from
//
// Returns:
//   - []*model.Block: Fetched blocks
//   - error: If request fails or blocks are invalid
func (u *Server) fetchBlocksBatch(ctx context.Context, hash *chainhash.Hash, n uint32, peerID string, baseURL string) ([]*model.Block, error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "fetchBlocksBatch",
		tracing.WithParentStat(u.stats),
	)
	defer deferFn()

	blockBytes, err := util.DoHTTPRequest(ctx, fmt.Sprintf("%s/blocks/%s?n=%d", baseURL, hash.String(), n))
	if err != nil {
		return nil, errors.NewProcessingError("[catchup:fetchBlocksBatch][%s] failed to get blocks from peer", hash.String(), err)
	}

	// Track bytes downloaded from peer
	if u.p2pClient != nil && peerID != "" {
		if err := u.p2pClient.RecordBytesDownloaded(ctx, peerID, uint64(len(blockBytes))); err != nil {
			u.logger.Warnf("[fetchBlocksBatch][%s] failed to record %d bytes downloaded from peer %s: %v", hash.String(), len(blockBytes), peerID, err)
		}
	}

	blockReader := bytes.NewReader(blockBytes)

	blocks := make([]*model.Block, 0)

	for {
		block, err := model.NewBlockFromReader(blockReader)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}

			return nil, errors.NewProcessingError("[catchup:fetchBlocksBatch][%s] failed to create block from bytes", hash.String(), err)
		}

		blocks = append(blocks, block)
	}

	return blocks, nil
}

// fetchSingleBlock fetches a single block from a peer by its hash.
//
// Parameters:
//   - ctx: Context for cancellation and tracing
//   - hash: Block hash to fetch
//   - peerID: Peer ID for reputation tracking
//   - baseURL: Peer URL to fetch from
//
// Returns:
//   - *model.Block: The fetched block
//   - error: If request fails or block is invalid
func (u *Server) fetchSingleBlock(ctx context.Context, hash *chainhash.Hash, peerID, baseURL string) (*model.Block, error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "fetchSingleBlock",
		tracing.WithParentStat(u.stats),
	)
	defer deferFn()

	blockBytes, err := util.DoHTTPRequest(ctx, fmt.Sprintf("%s/block/%s", baseURL, hash.String()))
	if err != nil {
		return nil, errors.NewProcessingError("[catchup:fetchSingleBlock][%s] failed to get block from peer", hash.String(), err)
	}

	// Track bytes downloaded from peer
	if u.p2pClient != nil && peerID != "" {
		if err := u.p2pClient.RecordBytesDownloaded(ctx, peerID, uint64(len(blockBytes))); err != nil {
			u.logger.Warnf("[fetchSingleBlock][%s] failed to record %d bytes downloaded from peer %s: %v", hash.String(), len(blockBytes), peerID, err)
		}
	}

	block, err := model.NewBlockFromBytes(blockBytes)
	if err != nil {
		return nil, errors.NewProcessingError("[catchup:fetchSingleBlock][%s] failed to create block from bytes", hash.String(), err)
	}

	if block == nil {
		return nil, errors.NewProcessingError("[catchup:fetchSingleBlock][%s] block could not be created from %d bytes received from peer",
			hash.String(), len(blockBytes))
	}

	// Don't report block fetch during catchup - wait for full validation
	// Only report success after the block is validated to prevent
	// inflating reputation for peers providing invalid chains
	// if u.p2pClient != nil && peerID != "" {
	// 	if err := u.p2pClient.ReportValidBlock(ctx, peerID, hash.String()); err != nil {
	// 		u.logger.Warnf("[fetchSingleBlock][%s] failed to report valid block: %s", hash.String(), err.Error())
	// 	}
	// }

	return block, nil
}
