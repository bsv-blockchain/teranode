// This file contains block fetching utilities for catchup operations.
package blockvalidation

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
	"github.com/bsv-blockchain/teranode/pkg/adaptivefetch"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
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
	block             *model.Block
	index             int
	err               error
	contributingPeers map[string]struct{} // peers that provided subtree data for this block
}

// blockForValidation wraps a block with metadata about which peers contributed data
type blockForValidation struct {
	block             *model.Block
	contributingPeers map[string]struct{}
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
func (u *Server) fetchBlocksConcurrently(ctx context.Context, catchupCtx *CatchupContext, validateBlocksChan chan blockForValidation, size *atomic.Int64) error {
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

		reverseBlocks(blocks)

		if err := verifyBlockHeaders(blocks, batchHeaders, blockUpTo); err != nil {
			return err
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

			// Fetch subtree data for this block — adaptive-fetch state may skip it
			// entirely when the node is receiving txs via a distributor.
			//
			// What the skip actually costs: this fetch is only a prewarm. It
			// pulls subtreeData ahead of time so the later block-validation step
			// finds everything already in the store. Skipping it does NOT skip
			// validation — when the block is validated, subtree validation still
			// runs and recovers any genuinely-missing txs from peers on demand
			// (see services/subtreevalidation getSubtreeMissingTxs). So an
			// optimistic skip that turns out to be wrong costs extra bandwidth
			// later (the txs get fetched then instead of now); it does not risk
			// accepting an unvalidated block or losing data.
			//
			// Capture the live mode (not just the boolean) so we can later
			// record the observation against the snapshot. Workers run
			// concurrently and the mode can transition between this point
			// and the Record call below; the snapshot lets the state machine
			// drop any observation whose underlying work was performed in a
			// different mode.
			modeAtSample := u.adaptiveFetch.Mode()
			optimistic := modeAtSample == adaptivefetch.ModeOptimistic

			var contributingPeers map[string]struct{}
			var err error
			if optimistic {
				contributingPeers, err = nil, nil
			} else {
				fetchFn := u.fetchSubtreeDataForBlockFn
				if fetchFn == nil {
					fetchFn = u.fetchSubtreeDataForBlock
				}
				contributingPeers, err = fetchFn(ctx, work.block, peerID, baseURL)
			}

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

			// Record a synthetic warm-up observation for the adaptive-fetch
			// state machine. The rationale (why MissingFetches is 0 today, why
			// that is safe, and the TODO to plumb real counts) lives once on
			// adaptivefetch.State.RecordSyntheticWarmup. This gate is only
			// consulted during catch-up; the State is armed on first FSM
			// RUNNING (see Server), so a cold-start IBD stays pessimistic.
			txCount := 0
			if work.block != nil {
				txCount = int(work.block.TransactionCount)
			}
			u.adaptiveFetch.RecordSyntheticWarmup(modeAtSample, txCount, 0)

			// Send result
			result := resultItem{
				block:             work.block,
				index:             work.index,
				contributingPeers: contributingPeers,
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
func (u *Server) orderedDelivery(gCtx context.Context, resultQueue <-chan resultItem, validateBlocksChan chan<- blockForValidation, totalBlocks int, blockUpTo *model.Block, size *atomic.Int64) error {
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
					case validateBlocksChan <- blockForValidation{block: orderedResult.block, contributingPeers: orderedResult.contributingPeers}:
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
// Returns a map of peer IDs that contributed subtree data for this block.
func (u *Server) fetchSubtreeDataForBlock(gCtx context.Context, block *model.Block, peerID, baseURL string) (map[string]struct{}, error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(gCtx, "fetchSubtreeDataForBlock",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[catchup:fetchSubtreeDataForBlock][%s] fetching subtree data for block with %d subtrees", block.Hash().String(), len(block.Subtrees)),
	)
	defer deferFn()

	if len(block.Subtrees) == 0 {
		u.logger.Debugf("[catchup:fetchSubtreeDataForBlock] Block %s has no subtrees, skipping", block.Hash().String())

		return nil, nil
	}

	// Track which peers contributed subtree data for this block
	var peersMu sync.Mutex
	contributingPeers := make(map[string]struct{})

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
			servingPeerID, err := u.fetchAndStoreSubtreeAndSubtreeData(ctx, block, &subtreeHashCopy, capturedPeerID, capturedBaseURL)
			if err != nil {
				return err
			}
			if servingPeerID != "" {
				peersMu.Lock()
				contributingPeers[servingPeerID] = struct{}{}
				peersMu.Unlock()
			}
			return nil
		})
	}

	// Wait for all subtree fetching to complete
	if err := g.Wait(); err != nil {
		return nil, errors.NewServiceError("[catchup:fetchSubtreeDataForBlock] Failed to fetch subtree data for block %s", block.Hash().String(), err)
	}

	return contributingPeers, nil
}

// verifySubtreeNodes checks that nodes hash to root and are not a padded form
// of a shorter list. The merkle construction hashes a missing right sibling as
// a copy of the left, so a duplicated level tail or zero padding reproduces the
// root of a shorter list (CVE-2012-2459). Other equal siblings are genuinely
// duplicate transactions, left to the block checks. This does not bind a list
// to its hash: any tree level also hashes to root, and only matching
// transactions rule that out.
func verifySubtreeNodes(nodes []subtreepkg.Node, root *chainhash.Hash) error {
	merkles, err := subtreepkg.BuildMerkleTreeStoreFromBytes(nodes)
	if err != nil {
		return errors.NewProcessingError("failed to compute subtree root", err)
	}
	if len(*merkles) == 0 || !(*merkles)[len(*merkles)-1].IsEqual(root) {
		return errors.NewProcessingError("subtree nodes do not hash to %s", root.String())
	}
	var zero chainhash.Hash
	for i := range nodes {
		if nodes[i].Hash.Equal(zero) {
			return errors.NewProcessingError("subtree %s has a zero leaf at index %d", root.String(), i)
		}
	}
	// A level with an even count of at least four whose last two entries are
	// equal hashes like the same level without its last entry. With two, the
	// shorter level is a single entry, which is its own root, so the subtree
	// hash differs. Lifting a final subtree does make [a] and [a,a] give the
	// same block merkle root, but under different subtree hashes: that is a
	// mutated block body, which the duplicate transaction check rejects.
	count := len(nodes)
	if count >= 4 && count%2 == 0 && nodes[count-1].Hash.Equal(nodes[count-2].Hash) {
		return errors.NewProcessingError("subtree %s has equal sibling leaves at its tail", root.String())
	}
	// Internal levels are stored bottom-up, each padded to a power of two.
	offset := 0
	for width := subtreepkg.NextPowerOfTwo(len(nodes)) / 2; width > 1; width /= 2 {
		count = (count + 1) / 2
		if count >= 4 && count%2 == 0 && (*merkles)[offset+count-1].Equal((*merkles)[offset+count-2]) {
			return errors.NewProcessingError("subtree %s has equal sibling nodes at a level tail", root.String())
		}
		offset += width
	}
	return nil
}

// fetchAndStoreSubtree returns the nodes to check a peer's subtree data against:
// the validated nodes if they are stored, otherwise the peer's own node list.
// A peer's list is not stored here. Any merkle level hashes to the subtree
// root, so only matching transactions bind it; fetchAndStoreSubtreeData stores
// it once they do.
func (u *Server) fetchAndStoreSubtree(ctx context.Context, block *model.Block, subtreeHash *chainhash.Hash, peerID, baseURL string) (*subtreepkg.Subtree, bool, error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "fetchAndStoreSubtree",
		tracing.WithParentStat(u.stats),
		// tracing.WithDebugLogMessage(u.logger, "[catchup:fetchAndStoreSubtree] fetching subtree for %s", subtreeHash.String()),
	)
	defer deferFn()

	store := u.catchupSubtreeStore(ctx)

	// A pending FileTypeSubtreeToCheck is deliberately not reused: it may be
	// a collapsed tree level, and checking fresh data against it could not
	// tell which of the two is wrong.
	validated, err := store.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
	if err != nil {
		return nil, false, errors.NewStorageError("[catchup:fetchAndStoreSubtree] error checking subtree existence for %s", subtreeHash.String(), err)
	}

	if validated {
		u.logger.Debugf("[catchup:fetchAndStoreSubtree] Subtree already exists for %s, loading from store", subtreeHash.String())

		subtreeBytes, err := store.Get(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
		if err != nil {
			return nil, false, errors.NewStorageError("[catchup:fetchAndStoreSubtree] Failed to get existing subtree for %s", subtreeHash.String(), err)
		}

		subtree, err := subtreeFromBytesWithMmap(subtreeBytes, u.settings.BlockValidation.SubtreeMmapDir)
		if err != nil {
			return nil, false, catchupArtifactError(ctx, *subtreeHash, fileformat.FileTypeSubtree,
				errors.NewProcessingError("[catchup:fetchAndStoreSubtree] Failed to deserialize existing subtree for %s", subtreeHash.String(), err))
		}
		// Peer data is judged against these nodes, so corrupt ones would
		// convict every honest peer.
		if err = verifySubtreeNodes(subtree.Nodes, subtreeHash); err != nil {
			_ = subtree.Close()
			return nil, false, catchupArtifactError(ctx, *subtreeHash, fileformat.FileTypeSubtree,
				errors.NewProcessingError("[catchup:fetchAndStoreSubtree] Existing subtree nodes do not match %s", subtreeHash.String(), err))
		}

		return subtree, true, nil
	}

	// Fetch subtree from peer
	subtreeNodeBytes, subtreeErr := u.fetchSubtreeFromPeer(ctx, subtreeHash, peerID, baseURL)
	if subtreeErr != nil {
		return nil, false, errors.NewServiceError("[catchup:fetchAndStoreSubtree] Failed to fetch subtree for %s", subtreeHash.String(), subtreeErr)
	}

	// A payload that is not a whole number of hashes is malformed.
	if len(subtreeNodeBytes)%chainhash.HashSize != 0 {
		u.reportCatchupError(ctx, peerID, "subtree nodes are not a whole number of hashes")
		return nil, false, errors.NewProcessingError("[catchup:fetchAndStoreSubtree] Peer %s (%s) provided %d bytes of subtree nodes for %s", peerID, baseURL, len(subtreeNodeBytes), subtreeHash.String())
	}

	// in the subtree validation, we only use the hashes of the FileTypeSubtreeToCheck, which is what is returned from the peer
	numberOfNodes := len(subtreeNodeBytes) / chainhash.HashSize
	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(numberOfNodes)
	if err != nil {
		return nil, false, errors.NewProcessingError("[catchup:fetchAndStoreSubtree] Failed to create subtree with %d nodes for %s", numberOfNodes, subtreeHash.String(), err)
	}

	// Sanity check, subtrees should never be empty
	if numberOfNodes == 0 {
		return nil, false, errors.NewProcessingError("[catchup:fetchAndStoreSubtree] Subtree for %s has zero nodes", subtreeHash.String())
	}

	// Deserialize the subtree nodes from the bytes
	for i := 0; i < numberOfNodes; i++ {
		// Each node is a chainhash.Hash, so we read chainhash.HashSize bytes
		nodeBytes := subtreeNodeBytes[i*chainhash.HashSize : (i+1)*chainhash.HashSize]
		nodeHash, err := chainhash.NewHash(nodeBytes)
		if err != nil {
			return nil, false, errors.NewProcessingError("[catchup:fetchAndStoreSubtree] Failed to create hash from bytes for subtree %s at index %d", subtreeHash.String(), i, err)
		}

		if i == 0 && nodeHash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			if err = subtree.AddCoinbaseNode(); err != nil {
				return nil, false, errors.NewProcessingError("[catchup:fetchAndStoreSubtree] Failed to add coinbase node to subtree %s at index %d", subtreeHash.String(), i, err)
			}
			continue
		}

		// Add the node to the subtree, we do not know the fee or size yet, so we use 0
		if err = subtree.AddNode(*nodeHash, 0, 0); err != nil {
			return nil, false, errors.NewProcessingError("[catchup:fetchAndStoreSubtree] Failed to add node %s to subtree %s at index %d", nodeHash.String(), subtreeHash.String(), i, err)
		}
	}

	if err = verifySubtreeNodes(subtree.Nodes, subtreeHash); err != nil {
		u.reportCatchupError(ctx, peerID, "subtree nodes do not match their hash")
		return nil, false, errors.NewProcessingError("[catchup:fetchAndStoreSubtree] Peer %s (%s) provided subtree nodes that do not match %s", peerID, baseURL, subtreeHash.String(), err)
	}

	// Reputation is credited post-validation in validateBlocksOnChannel via reportValidBlockForPeers

	return subtree, false, nil
}

// fetchAndStoreSubtreeData fetches the peer's subtree data and checks it against
// subtree. Matching transactions bind the node list to its hash, so both files
// are stored only then. A mismatch is the peer's fault: it served both files,
// or its data contradicts validated nodes.
func (u *Server) fetchAndStoreSubtreeData(ctx context.Context, block *model.Block, subtreeHash *chainhash.Hash,
	subtree *subtreepkg.Subtree, validated bool, peerID, baseURL string) error {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "fetchAndStoreSubtreeData",
		tracing.WithParentStat(u.stats),
		tracing.WithDebugLogMessage(u.logger, "[catchup:fetchAndStoreSubtreeData][%s] Fetching subtree data from peer %s (%s) for subtree %s", block.Hash().String(), peerID, baseURL, subtreeHash.String()),
	)
	defer deferFn()

	dah := block.Height + u.settings.GetSubtreeValidationBlockHeightRetention()

	// Detach from sibling cancellation: this function is called from a per-subtree
	// goroutine inside fetchSubtreeDataForBlock's errgroup. Using gCtx for the HTTP
	// fetch + parse + store means a single sibling failure cancels every in-flight
	// subtree_data download in the batch — and each cancellation closes the upstream
	// connection, causing the peer to abort its on-demand creation (storer.Abort) and
	// throw away Aerospike work that was already paid for. Detaching here lets each
	// fetch run to completion (or hit its own http_streaming_timeout) so the peer can
	// finish writing its subtreeData file.
	//
	// See companion fix in services/subtreevalidation/check_block_subtrees.go.
	ctx = context.WithoutCancel(ctx)

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
	if err == nil {
		err = checkSubtreeDataMatchesNodes(subtree, subtreeData)
	}
	if err != nil {
		if errors.Is(err, subtreepkg.ErrTxHashMismatch) || errors.Is(err, subtreepkg.ErrTxIndexOutOfBounds) {
			u.reportCatchupError(ctx, peerID, "subtree data does not match subtree nodes")
		}
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

	subtreeBytes, err := subtree.Serialize()
	if err != nil {
		return errors.NewProcessingError("[catchup:fetchAndStoreSubtreeData] Failed to serialize subtree %s", subtreeHash.String(), err)
	}
	// The data binds the peer's list unless it could be a collapsed tree level;
	// then both files are stored unbound, so no verdict can rest on them.
	bound := validated || !allMerkleNodeSized(subtree, subtreeData.Txs)
	if err = u.storeBoundSubtreeFile(ctx, subtreeHash, fileformat.FileTypeSubtreeData, subtreeDataBytes, dah, 0, bound); err != nil {
		return errors.NewStorageError("[catchup:fetchAndStoreSubtreeData] Failed to store subtreeData for %s", subtreeHash.String(), err)
	}
	if validated {
		return u.bindPendingToValidated(ctx, block, subtreeHash)
	}
	if err = u.storeBoundSubtreeFile(ctx, subtreeHash, fileformat.FileTypeSubtreeToCheck, subtreeBytes, dah, subtree.Length(), bound); err != nil {
		return errors.NewStorageError("[catchup:fetchAndStoreSubtreeData] Failed to store subtreeToCheck for %s", subtreeHash.String(), err)
	}

	return nil
}

// allMerkleNodeSized reports whether every transaction outside the coinbase slot
// is 64 bytes; see isMerkleNodeSized.
func allMerkleNodeSized(subtree *subtreepkg.Subtree, txs []*bt.Tx) bool {
	start := 0
	if len(subtree.Nodes) > 0 && subtree.Nodes[0].Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
		start = 1
	}
	if start >= len(txs) {
		return false
	}
	for _, tx := range txs[start:] {
		if tx == nil || !isMerkleNodeSized(tx) {
			return false
		}
	}
	return true
}

// bindPendingToValidated replaces a pending node list with the validated one.
// Readers prefer the pending file, which may be stale or collapsed.
func (u *Server) bindPendingToValidated(ctx context.Context, block *model.Block, subtreeHash *chainhash.Hash) error {
	store := u.catchupSubtreeStore(ctx)
	pending, err := store.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
	if err != nil || !pending {
		return err
	}
	validatedBytes, err := store.Get(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
	if err != nil {
		return errors.NewStorageError("[catchup:bindPendingToValidated] Failed to get subtree %s", subtreeHash.String(), err)
	}
	validated, err := subtreepkg.NewSubtreeFromBytes(validatedBytes)
	if err == nil {
		err = verifySubtreeNodes(validated.Nodes, subtreeHash)
	}
	if err != nil {
		return catchupArtifactError(ctx, *subtreeHash, fileformat.FileTypeSubtree,
			errors.NewProcessingError("[catchup:bindPendingToValidated] Invalid validated subtree %s", subtreeHash.String(), err))
	}
	dah := block.Height + u.settings.GetSubtreeValidationBlockHeightRetention()
	if err = u.storeBoundSubtreeFile(ctx, subtreeHash, fileformat.FileTypeSubtreeToCheck, validatedBytes, dah, validated.Length(), true); err != nil {
		return errors.NewStorageError("[catchup:bindPendingToValidated] Failed to store subtreeToCheck for %s", subtreeHash.String(), err)
	}
	return nil
}

// checkSubtreeDataMatchesNodes requires a transaction matching every node.
// go-subtree's reader stops quietly at EOF, leaving unfilled slots nil (and
// Data.Serialize then dereferences a nil slot 0), and it places a coinbase
// found in second position into slot 0 over the transaction already there.
// Only the coinbase placeholder slot may be empty or hold another hash. A
// missing transaction is reported as a length mismatch, not a hash mismatch:
// an honest server that aborts a committed stream also ends it cleanly.
func checkSubtreeDataMatchesNodes(subtree *subtreepkg.Subtree, data *subtreepkg.Data) error {
	for i, node := range subtree.Nodes {
		if i == 0 && node.Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			continue
		}
		if data.Txs[i] == nil {
			return errors.NewProcessingError("subtree data is missing transaction %d", i, subtreepkg.ErrSubtreeLengthMismatch)
		}
		if !data.Txs[i].TxIDChainHash().Equal(node.Hash) {
			return errors.NewProcessingError("subtree data transaction %d does not match its node", i, subtreepkg.ErrTxHashMismatch)
		}
	}
	return nil
}

// bindStoredSubtree reuses subtree data already on disk. Transaction data is
// self-authenticating: its txids must be the canonical leaves of the subtree
// hash, which no collapsed or padded node list can fake. The node list is then
// derived from the data, whatever node file is stored. Returns false when the
// data is missing or does not authenticate, so it is fetched again.
func (u *Server) bindStoredSubtree(ctx context.Context, block *model.Block, subtreeHash *chainhash.Hash) (bool, error) {
	store := u.catchupSubtreeStore(ctx)
	exists, err := store.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
	if err != nil {
		return false, errors.NewStorageError("[catchup:bindStoredSubtree] error checking subtreeData existence for %s", subtreeHash.String(), err)
	}
	if !exists {
		return false, nil
	}
	validated, err := store.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
	if err != nil {
		return false, errors.NewStorageError("[catchup:bindStoredSubtree] error checking subtree existence for %s", subtreeHash.String(), err)
	}
	if validated {
		// Accepted with this data by an earlier validation.
		return true, u.bindPendingToValidated(ctx, block, subtreeHash)
	}
	reader, err := store.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
	if err != nil {
		return false, errors.NewStorageError("[catchup:bindStoredSubtree] failed to read subtreeData for %s", subtreeHash.String(), err)
	}
	defer reader.Close()
	bufferedReader := bufioReaderPool.Get().(*bufio.Reader)
	bufferedReader.Reset(reader)
	defer func() {
		bufferedReader.Reset(nil)
		bufioReaderPool.Put(bufferedReader)
	}()
	coinbaseSlot := len(block.Subtrees) > 0 && block.Subtrees[0].IsEqual(subtreeHash)
	subtree, ambiguous, err := subtreeFromData(bufferedReader, coinbaseSlot)
	if err == nil {
		err = verifySubtreeNodes(subtree.Nodes, subtreeHash)
	}
	if err != nil {
		u.logger.Warnf("[catchup:bindStoredSubtree] stored subtreeData for %s does not authenticate, fetching again: %v", subtreeHash.String(), err)
		return false, nil
	}
	subtreeBytes, err := subtree.Serialize()
	if err != nil {
		return false, errors.NewProcessingError("[catchup:bindStoredSubtree] Failed to serialize subtree %s", subtreeHash.String(), err)
	}
	if ambiguous {
		// It cannot prove its own node list; a peer must supply both again.
		return false, nil
	}
	dah := block.Height + u.settings.GetSubtreeValidationBlockHeightRetention()
	if err = u.storeBoundSubtreeFile(ctx, subtreeHash, fileformat.FileTypeSubtreeToCheck, subtreeBytes, dah, subtree.Length(), true); err != nil {
		return false, errors.NewStorageError("[catchup:bindStoredSubtree] Failed to store subtreeToCheck for %s", subtreeHash.String(), err)
	}
	return true, nil
}

// subtreeFromData builds the node list implied by serialized subtree data. The
// first subtree of a block reserves its first slot for the coinbase, which the
// data may or may not include. ambiguous reports data that does not bind the
// list: see merkleLevelSized.
func subtreeFromData(r io.Reader, coinbaseSlot bool) (subtree *subtreepkg.Subtree, ambiguous bool, err error) {
	var leaves []chainhash.Hash
	ambiguous = true
	for {
		tx := &bt.Tx{}
		if _, err := tx.ReadFrom(r); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, false, errors.NewProcessingError("failed to read transaction %d", len(leaves), err)
		}
		if coinbaseSlot && len(leaves) == 0 && tx.IsCoinbase() {
			continue
		}
		ambiguous = ambiguous && isMerkleNodeSized(tx)
		leaves = append(leaves, *tx.TxIDChainHash())
	}
	count := len(leaves)
	if coinbaseSlot {
		count++
	}
	if count == 0 {
		return nil, false, errors.NewProcessingError("subtree data is empty")
	}
	ambiguous = ambiguous && len(leaves) > 0
	subtree, err = subtreepkg.NewIncompleteTreeByLeafCount(count)
	if err != nil {
		return nil, false, errors.NewProcessingError("failed to create subtree with %d nodes", count, err)
	}
	if coinbaseSlot {
		if err = subtree.AddCoinbaseNode(); err != nil {
			return nil, false, errors.NewProcessingError("failed to add coinbase node", err)
		}
	}
	for _, leaf := range leaves {
		if err = subtree.AddNode(leaf, 0, 0); err != nil {
			return nil, false, errors.NewProcessingError("failed to add node %s", leaf.String(), err)
		}
	}
	return subtree, ambiguous, nil
}

// isMerkleNodeSized reports a transaction whose serialization is exactly two
// merkle children long, so its txid may be an internal merkle node. Data binds
// a node list unless every transaction is that size: only then can the data
// be a whole collapsed tree level. The coinbase slot is excluded, as the
// placeholder leaf has no preimage.
func isMerkleNodeSized(tx *bt.Tx) bool {
	return tx.Size() == 2*chainhash.HashSize
}

// fetchAndStoreSubtreeAndSubtreeData fetches both subtree and subtreeData for a single subtree hash
// and stores them in the subtreeStore. If the primary peer fails, it will try alternative peers
// at max height before giving up.
// Returns the peer ID that actually served the data and any error.
func (u *Server) fetchAndStoreSubtreeAndSubtreeData(ctx context.Context, block *model.Block, subtreeHash *chainhash.Hash,
	peerID, baseURL string) (string, error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "fetchAndStoreSubtreeAndSubtreeData",
		tracing.WithParentStat(u.stats),
		// tracing.WithDebugLogMessage(u.logger, "[catchup:fetchAndStoreSubtreeAndSubtreeData] fetching subtree and data for %s", subtreeHash.String()),
	)
	defer deferFn()

	if bound, err := u.bindStoredSubtree(ctx, block, subtreeHash); err != nil || bound {
		return "", err
	}

	// Try primary peer first
	subtree, validated, err := u.fetchAndStoreSubtree(ctx, block, subtreeHash, peerID, baseURL)
	if err == nil {
		// Primary peer succeeded for subtree, now try subtreeData
		if err = u.fetchAndStoreSubtreeDataAndRelease(ctx, block, subtreeHash, subtree, validated, peerID, baseURL); err == nil {
			return peerID, nil // Success
		}
		// Check if error is local (not peer-related) - don't retry with other peers
		if errors.IsLocalError(err) {
			return "", errors.NewServiceError("[catchup:fetchAndStoreSubtreeAndSubtreeData] Local error fetching subtreeData for %s (not retrying with other peers)", subtreeHash.String(), err)
		}
		u.logger.Warnf("[catchup:fetchAndStoreSubtreeAndSubtreeData] Primary peer %s failed to fetch subtreeData for %s: %v, trying alternatives", peerID, subtreeHash.String(), err)
	} else {
		// Check if error is local (not peer-related) - don't retry with other peers
		if errors.IsLocalError(err) {
			return "", errors.NewServiceError("[catchup:fetchAndStoreSubtreeAndSubtreeData] Local error fetching subtree for %s (not retrying with other peers)", subtreeHash.String(), err)
		}
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
				subtree, validated, err = u.fetchAndStoreSubtree(ctx, block, subtreeHash, altPeerID, altBaseURL)
				if err != nil {
					u.logger.Debugf("[catchup:fetchAndStoreSubtreeAndSubtreeData] Alternative peer %s failed for subtree %s: %v", altPeerID, subtreeHash.String(), err)
					lastErr = err
					// Don't continue trying other peers if it's a local error
					if errors.IsLocalError(err) {
						return "", errors.NewServiceError("[catchup:fetchAndStoreSubtreeAndSubtreeData] Local error fetching subtree %s (aborting peer retry)", subtreeHash.String(), err)
					}
					continue
				}

				// Subtree succeeded, try subtreeData
				if err = u.fetchAndStoreSubtreeDataAndRelease(ctx, block, subtreeHash, subtree, validated, altPeerID, altBaseURL); err != nil {
					u.logger.Debugf("[catchup:fetchAndStoreSubtreeAndSubtreeData] Alternative peer %s failed for subtreeData %s: %v", altPeerID, subtreeHash.String(), err)
					lastErr = err
					// Don't continue trying other peers if it's a local error
					if errors.IsLocalError(err) {
						return "", errors.NewServiceError("[catchup:fetchAndStoreSubtreeAndSubtreeData] Local error fetching subtreeData %s (aborting peer retry)", subtreeHash.String(), err)
					}
					continue
				}

				// Success with alternative peer
				u.logger.Infof("[catchup:fetchAndStoreSubtreeAndSubtreeData] Successfully fetched subtree %s from alternative peer %s", subtreeHash.String(), altPeerID)
				return altPeerID, nil
			}
		}
	}

	// All peers failed. errors.NewServiceError extracts the trailing error param as
	// the wrapped error, so a "%v" placeholder for lastErr would render as
	// %!v(MISSING). The wrapped error is preserved in the chain.
	return "", errors.NewServiceError("[catchup:fetchAndStoreSubtreeAndSubtreeData] All peers failed to fetch subtree %s", subtreeHash.String(), lastErr)
}

// fetchAndStoreSubtreeDataAndRelease is fetchAndStoreSubtreeData followed by
// releasing subtree, which may be mmap-backed when loaded from the store.
func (u *Server) fetchAndStoreSubtreeDataAndRelease(ctx context.Context, block *model.Block, subtreeHash *chainhash.Hash,
	subtree *subtreepkg.Subtree, validated bool, peerID, baseURL string) error {
	defer func() { _ = subtree.Close() }()
	return u.fetchAndStoreSubtreeData(ctx, block, subtreeHash, subtree, validated, peerID, baseURL)
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

	// Bound the body at the receive-side policy cap (MaxIncomingSubtreeBytes). A peer that
	// streams more than this is malicious — fail fast rather than ReadAll into memory.
	// This must be independent of local BlockAssembly.MaximumMerkleItemsPerSubtree, which
	// only controls what *this node* assembles; peers may legitimately produce larger subtrees.
	maxSubtreeBytes := u.settings.SubtreeValidation.MaxIncomingSubtreeBytes

	// Use the existing HTTP utility to fetch subtree
	subtreeBytes, err := util.DoHTTPRequestBounded(ctx, url, maxSubtreeBytes)
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

	// Retry on 503 — peer's asset service may reject under admission control while it
	// generates the file on-demand from Aerospike. The retry loop honors the peer's
	// Retry-After header.
	subtreeDataReader, err := util.DoHTTPRequestBodyReaderWithRetry(ctx, url)
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

	// Reputation is credited post-validation in validateBlocksOnChannel via reportValidBlockForPeers

	return block, nil
}

// reverseBlocks reverses a slice of blocks in place.
func reverseBlocks(blocks []*model.Block) {
	for j, k := 0, len(blocks)-1; j < k; j, k = j+1, k-1 {
		blocks[j], blocks[k] = blocks[k], blocks[j]
	}
}

// verifyBlockHeaders checks that each fetched block's hash matches the expected header.
func verifyBlockHeaders(blocks []*model.Block, headers []*model.BlockHeader, blockUpTo *model.Block) error {
	for j, block := range blocks {
		if block.Hash().String() != headers[j].Hash().String() {
			return errors.NewProcessingError("[catchup:batchFetchAndDistribute][%s] block hash mismatch at index %d: expected %s, got %s",
				blockUpTo.Hash().String(), j, headers[j].Hash().String(), block.Hash().String())
		}
	}
	return nil
}
