// This file contains block fetching utilities for catchup operations.
package blockvalidation

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/adaptivefetch"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	lru "github.com/hashicorp/golang-lru/v2"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

// peerFetchLimiterCacheSize bounds the number of distinct per-peer (data-hub URL) rate
// limiters retained. Far above any realistic catchup peer set, so it only caps pathological
// growth from a peer churning its advertised DataHubURL on a long-running node.
const peerFetchLimiterCacheSize = 1024

// peerFetchLimiter returns the per-peer client-side rate limiter for baseURL,
// lazily creating it from settings. Returns nil when per-peer pacing is disabled
// (PerPeerFetchRate <= 0), in which case callers skip the wait. Keyed by baseURL
// (the actual HTTP target) so two peers can't share a bucket and one peer's limit
// can't throttle another.
func (u *Server) peerFetchLimiter(baseURL string) *rate.Limiter {
	r := u.settings.BlockValidation.PerPeerFetchRate
	if r <= 0 {
		return nil
	}

	u.peerFetchLimitersMu.Lock()
	defer u.peerFetchLimitersMu.Unlock()

	if u.peerFetchLimiters == nil {
		// lru.New only errors on size <= 0, which the constant guards against.
		c, _ := lru.New[string, *rate.Limiter](peerFetchLimiterCacheSize)
		u.peerFetchLimiters = c
	}

	if lim, ok := u.peerFetchLimiters.Get(baseURL); ok {
		return lim
	}

	// rate == burst: allow a short burst up to the rate, then pace to it.
	lim := rate.NewLimiter(rate.Limit(r), r)
	u.peerFetchLimiters.Add(baseURL, lim)

	return lim
}

// awaitPeerFetchSlot blocks until the per-peer rate limiter grants a token (or ctx
// is done), pacing heavy-fetch request issuance to baseURL so the catchup fan-out
// can't burst into the peer's asset heavy-route limiter. No-op when pacing is
// disabled. The wait holds nothing across the subsequent download, so it cannot
// deadlock or pin a slot for the lifetime of a slow stream.
//
// A failed wait is ALWAYS a local condition (our own pacing budget vs the context
// deadline), never the peer's fault — x/time/rate.Wait returns a plain non-context
// error ("would exceed context deadline") in that case, so we re-wrap it as a
// context-canceled error to ensure errors.IsLocalError classifies it correctly and
// the peer-reputation gate does not blame the peer for our local stall.
func (u *Server) awaitPeerFetchSlot(ctx context.Context, baseURL string) error {
	lim := u.peerFetchLimiter(baseURL)
	if lim == nil {
		return nil
	}

	if err := lim.Wait(ctx); err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return errors.NewContextCanceledError("[peerFetchLimiter] local pacing wait aborted", cerr)
		}
		// x/time/rate.Wait returns a plain "would exceed context deadline" error here
		// (ctx not yet done). NewContextCanceledError stamps ERR_CONTEXT_CANCELED, which
		// errors.IsContextError/IsLocalError detect by CODE anywhere in the chain even after
		// callers wrap it in a ProcessingError/ServiceError on the way to the reputation gate,
		// so the gate does not blame the peer for our local stall. Passing the real
		// context.DeadlineExceeded sentinel also preserves it natively (identity), not as
		// forgeable message text.
		return errors.NewContextCanceledError("[peerFetchLimiter] wait aborted for %s", util.RedactPeerURL(baseURL), context.DeadlineExceeded)
	}

	return nil
}

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

	// Track which peers contributed subtree data for this block (credited post-validation).
	// Per-peer FAILURE attribution is handled inside tryPeerForSubtree via recordCatchupPeerFailure
	// and drained at catchup release (#1371), so this path no longer keeps its own failed-peer
	// carrier.
	var peersMu sync.Mutex
	contributingPeers := make(map[string]struct{})

	// parentCtx is the catchup context BEFORE the errgroup derivation below: it is cancelled
	// by node shutdown / catchup cancel but NOT by a sibling subtree failing in this batch.
	// fetchAndStoreSubtreeData derives its detached download context from this, so an
	// in-flight subtree_data download survives sibling failures yet still aborts on shutdown.
	parentCtx := ctx

	var peerSnapshot *catchupPeerSnapshot
	if u.p2pClient != nil {
		peerSnapshot = newCatchupPeerSnapshot(
			parentCtx,
			u.logger,
			u.p2pClient,
			peerID,
			block.Hash().String(),
		)
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
	if u.settings.BlockValidation.CatchupParallelFetchEnabled && peerSnapshot != nil {
		blockAltPeers, primaryPruned, _ := peerSnapshot.get()
		// Never proactively assign subtrees to pruned peers (they 404 on archival subtree_data);
		// they stay reachable only as last-resort failover via filterMaxHeightPeers' tail.
		peerAssignments = DistributeSubtreesAcrossPeers(
			u.logger,
			peerID,
			baseURL,
			primaryPruned,
			nonPrunedPeers(blockAltPeers),
			len(block.Subtrees),
		)
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
			servingPeerID, err := u.fetchAndStoreSubtreeAndSubtreeData(ctx, parentCtx, block, &subtreeHashCopy, capturedPeerID, capturedBaseURL, peerSnapshot)
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

	// Wait for all subtree fetching to complete. Per-peer failures were already attributed at the
	// point of failure (recordCatchupPeerFailure) and are drained at catchup release; the terminal
	// ErrExternal from fetchAndStoreSubtreeAndSubtreeData carries the per-peer attempt summary.
	if err := g.Wait(); err != nil {
		return nil, errors.NewServiceError("[catchup:fetchSubtreeDataForBlock] Failed to fetch subtree data for block %s", block.Hash().String(), err)
	}

	return contributingPeers, nil
}

// fetchAndStoreSubtree fetches and stores only the subtree (for subtreeToCheck)
func (u *Server) fetchAndStoreSubtree(ctx context.Context, block *model.Block, subtreeHash *chainhash.Hash, peerID, baseURL string, bypassCache bool) (*subtreepkg.Subtree, error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "fetchAndStoreSubtree",
		tracing.WithParentStat(u.stats),
		// tracing.WithDebugLogMessage(u.logger, "[catchup:fetchAndStoreSubtree] fetching subtree for %s", subtreeHash.String()),
	)
	defer deferFn()

	dah := block.Height + u.settings.GetSubtreeValidationBlockHeightRetention()

	// Check if we already have the subtree, under either FileTypeSubtreeToCheck
	// (peer-fetched, pending validation) or FileTypeSubtree (already validated).
	// See findLocalSubtreeFile for why both must be consulted.
	localFileType, localExists, err := findLocalSubtreeFile(ctx, u.subtreeStore, *subtreeHash)
	if err != nil {
		return nil, errors.NewStorageError("[catchup:fetchAndStoreSubtree] error checking subtree existence for %s", subtreeHash.String(), err)
	}

	if localExists {
		u.logger.Debugf("[catchup:fetchAndStoreSubtree] Subtree already exists for %s, loading from store", subtreeHash.String())

		// Load existing subtree from store under whichever file type was found
		subtreeBytes, err := u.subtreeStore.Get(ctx, subtreeHash[:], localFileType)
		if err != nil {
			return nil, errors.NewStorageError("[catchup:fetchAndStoreSubtree] Failed to get existing subtree for %s", subtreeHash.String(), err)
		}

		subtree, err := subtreeFromBytesWithMmap(subtreeBytes, u.settings.BlockValidation.SubtreeMmapDir)
		if err != nil {
			return nil, errors.NewStorageError("[catchup:fetchAndStoreSubtree] Failed to deserialize existing subtree for %s", subtreeHash.String(), err)
		}

		return subtree, nil
	}

	// Fetch subtree from peer
	subtreeNodeBytes, subtreeErr := u.fetchSubtreeFromPeer(ctx, subtreeHash, peerID, baseURL, bypassCache)
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

	// Reputation is credited post-validation in validateBlocksOnChannel via reportValidBlockForPeers

	return subtree, nil
}

// classifyPeerFetchCtxErr is the single source of truth for classifying a catchup fetch/parse
// error as a local cancel vs a peer network-timeout. ctx is the fetch context; canceledMsg and
// timeoutMsg are the fully-formatted messages for the two peer-facing branches. Returns nil to
// mean "genuine peer bad-data — the caller wraps ProcessingError".
//
// Two subtleties, both load-bearing:
//   - Order: a shutdown cancel and a peer stall (per-request streaming-timeout deadline) both
//     surface here as read/parse errors; classify the cancel as LOCAL and the deadline as a
//     (non-local) peer network-timeout, so a stalling peer — the wedge this PR targets — is
//     failed over and dinged rather than silently absolved.
//   - Do NOT wrap err in the timeout branch: (*Error).Is falls back to substring matching, so a
//     chain that renders "context deadline exceeded" is infectious — no outer re-classification
//     can undo it. NewNetworkTimeoutError with no wrapped error keeps the peer-fault class clean.
func classifyPeerFetchCtxErr(ctx context.Context, err error, canceledMsg, timeoutMsg string) error {
	if errors.Is(err, errors.ErrContextCanceled) {
		return err
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return errors.NewContextCanceledError(canceledMsg, context.Canceled)
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return errors.NewNetworkTimeoutError(timeoutMsg)
	}
	return nil
}

// classifyDownloadErr classifies a subtree_data fetch/parse failure. dlCtx is the download context
// (a child of shutdownCtx). See classifyPeerFetchCtxErr for the load-bearing subtleties.
func classifyDownloadErr(dlCtx context.Context, subtreeHash *chainhash.Hash, err error) error {
	return classifyPeerFetchCtxErr(dlCtx, err,
		fmt.Sprintf("[catchup:fetchAndStoreSubtreeData] subtree data aborted (shutdown) for %s", subtreeHash.String()),
		fmt.Sprintf("[catchup:fetchAndStoreSubtreeData] subtree data timed out for %s", subtreeHash.String()))
}

// fetchAndStoreSubtreeData fetches and stores only the subtreeData. shutdownCtx is the
// catchup parent context (NOT the per-subtree errgroup child): the download+store is
// derived from it so a sibling subtree failing in the same batch can't abort this in-flight
// download (which would discard the peer's paid on-demand work), while node shutdown /
// catchup cancel still tears it down promptly.
func (u *Server) fetchAndStoreSubtreeData(ctx context.Context, shutdownCtx context.Context, block *model.Block, subtreeHash *chainhash.Hash,
	subtree *subtreepkg.Subtree, peerID, baseURL string, bypassCache bool) error {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "fetchAndStoreSubtreeData",
		tracing.WithParentStat(u.stats),
		tracing.WithDebugLogMessage(u.logger, "[catchup:fetchAndStoreSubtreeData][%s] Fetching subtree data from peer %s (%s) for subtree %s", block.Hash().String(), peerID, baseURL, subtreeHash.String()),
	)
	defer deferFn()

	dah := block.Height + u.settings.GetSubtreeValidationBlockHeightRetention()

	// Check if we already have the subtreeData
	subtreeDataExists, err := u.subtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
	if err != nil {
		// A sibling/shutdown cancel of this existence read is local, not a storage fault.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// A genuine store failure (disk full / blob backend down) must classify as a STORAGE
		// error so the loud local-storage gate halts catchup, rather than a ProcessingError
		// (neither local nor storage) that fails over across every peer for a local outage.
		return errors.NewStorageError("[catchup:fetchAndStoreSubtreeData] error checking subtreeData existence for %s", subtreeHash.String(), err)
	}

	if subtreeDataExists {
		u.logger.Debugf("[catchup:fetchAndStoreSubtreeData] SubtreeData already exists for %s, skipping fetch", subtreeHash.String())
		return nil
	}

	// Derive the download+store context from shutdownCtx (the catchup parent), NOT from the
	// per-subtree errgroup ctx. A sibling subtree failing in this batch cancels the errgroup
	// ctx; running the HTTP fetch + parse + store on that would close the upstream connection
	// and make the peer abort its on-demand creation (storer.Abort), discarding Aerospike work
	// already paid for. Deriving from shutdownCtx avoids sibling cancellation while STILL
	// honoring node shutdown / catchup cancel (the previous context.WithoutCancel detached
	// from everything, so a stuck stream blocked graceful shutdown until http_streaming_timeout).
	// The existence check above still uses the original (errgroup) ctx, so a pre-cancelled call
	// exits early. See companion fix in services/subtreevalidation/check_block_subtrees.go.
	//
	// context.WithCancel(shutdownCtx) carries shutdownCtx's values (the catchup-level trace
	// span), so re-attach this function's span afterwards — otherwise the fetch/store child
	// spans reparent to the catchup root instead of nesting under fetchAndStoreSubtreeData.
	// Observability-only; the cancellation behaviour is unchanged.
	// WithTimeout (not WithCancel): this download context lives across the fetch AND the streaming
	// read+store of the returned body and is cancelled only when this function returns, so it is the
	// correct owner of the subtree-fetch wall clock (all retry attempts plus the streaming read).
	// Without it doRequestReaderWithRetryAfter mints a fresh per-attempt streaming timeout with no
	// ceiling, so a peer stalling each attempt could hold this ~maxAttempts x http_streaming_timeout.
	spanCtx := ctx
	var dlCancel context.CancelFunc
	ctx, dlCancel = u.withCatchupSubtreeFetchTimeout(shutdownCtx)
	defer dlCancel()
	ctx = trace.ContextWithSpan(ctx, trace.SpanFromContext(spanCtx))

	// The per-attempt rate-limit pacing hook uses the attempt ctx (this dlCtx), which is
	// cancellable on shutdown — so both the pacing wait and the download abort promptly.
	subtreeDataReader, err := u.fetchSubtreeDataFromPeer(ctx, subtreeHash, peerID, baseURL,
		func(c context.Context) error { return u.awaitPeerFetchSlot(c, baseURL) }, bypassCache)
	if err != nil {
		if c := classifyDownloadErr(ctx, subtreeHash, err); c != nil {
			return c
		}
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
		if c := classifyDownloadErr(ctx, subtreeHash, err); c != nil {
			return c
		}
		return errors.NewProcessingError("[catchup:fetchAndStoreSubtreeData] Failed to create subtreeData for %s", subtreeHash.String(), err)
	}

	// Reject a response the subtree cannot be satisfied by, before Serialize turns it
	// into a generic ErrSubtreeLengthMismatch that names no peer. An empty body is the
	// issue-1368 signature: a peer's proxy cache replaying a failed or aborted
	// on-demand generation as "200 + 0 bytes".
	//
	// The predicate mirrors what subtreepkg.Data.Serialize *safely* tolerates, which is
	// not the same as its literal `i != 0` nil exemption. Serialize skips index 0 only
	// when Nodes[0] is the coinbase placeholder: it then sets txStartIndex = 1 and never
	// touches Txs[0]. For any other Nodes[0] it sets txStartIndex = 0 while still
	// guarding its own nil check with `i != 0`, so it walks straight into
	// Txs[0].SerializeBytes() on a nil *bt.Tx and panics (IsExtended is nil-safe, so it
	// falls through to Bytes -> toBytesHelper -> Size). Copying the unconditional
	// exemption here would let such a response through with missing == 0, and the panic
	// lands in a per-subtree errgroup goroutine that no recover() in this package
	// covers. That is reachable without malice: a non-first subtree has no coinbase
	// placeholder, so any block whose tx count is congruent to 1 modulo the subtree size
	// ends with a one-node subtree holding a real tx hash at index 0.
	//
	// So index 0 counts as missing unless it genuinely is the coinbase placeholder —
	// the only case Serialize actually tolerates — and nothing that used to succeed
	// starts failing here.
	missing := 0
	coinbaseAtZero := len(subtree.Nodes) > 0 && subtree.Nodes[0].Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue)

	for i, tx := range subtreeData.Txs {
		if tx != nil {
			continue
		}

		if i == 0 && coinbaseAtZero {
			continue
		}

		missing++
	}

	bytesRead := subtreeDataReader.BytesRead()

	u.logger.Debugf("[catchup:fetchAndStoreSubtreeData] Subtree %s from %s has %d/%d txs (%d bytes, %d missing)",
		subtreeHash.String(), baseURL, len(subtreeData.Txs)-missing, len(subtreeData.Txs), bytesRead, missing)

	if missing > 0 {
		return newPoisonedSubtreeDataError(peerID, baseURL, subtreeHash, missing, subtree.Length(), bytesRead)
	}

	// Try to serialize the subtreeData to validate it's complete
	subtreeDataBytes, err := subtreeData.Serialize()
	if err != nil {
		if c := classifyDownloadErr(ctx, subtreeHash, err); c != nil {
			return c
		}
		return errors.NewProcessingError("[catchup:fetchAndStoreSubtreeData] Peer %s (%s) provided incomplete subtree data for %s", peerID, util.RedactPeerURL(baseURL), subtreeHash.String(), err)
	}

	// Store subtreeData (raw data) in subtreeStore
	if err = u.subtreeStore.Set(ctx,
		subtreeHash[:],
		fileformat.FileTypeSubtreeData,
		subtreeDataBytes,
		options.WithAllowOverwrite(true),
		options.WithDeleteAt(dah),
	); err != nil {
		// A shutdown/catchup cancel mid-Set surfaces here; classify it local (like the fetch/parse
		// paths above) so it doesn't trip the loud storage-error gate on a clean shutdown.
		if c := classifyDownloadErr(ctx, subtreeHash, err); c != nil {
			return c
		}
		return errors.NewStorageError("[catchup:fetchAndStoreSubtreeData] Failed to store subtreeData for %s", subtreeHash.String(), err)
	}

	return nil
}

// maxSubtreeFailoverPeers bounds how many alternative peers a single subtree fetch tries after its
// assigned peer fails. It is deliberately NOT CatchupMaxRetries (which bounds peer retries WITHIN
// one catchup operation): failover BREADTH should track how many max-height peers might hold a
// subtree whose data is skewed to a minority, not a retry count. Each alternative fetch is itself
// bounded by a wall clock (withCatchupSubtreeFetchTimeout, default 120s, covering all retry attempts
// plus the streaming read), and the per-block attempt cap (CatchupMaxAttemptsPerBlock) bounds
// re-entry — so this only caps the per-subtree fan-out width, not total time. (Block fetches carry
// the analogous withCatchupFetchTimeout wall clock.)
const maxSubtreeFailoverPeers = 10

func alternativePeerCapacity(maxAttempts, peerCount int) int {
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	return min(maxAttempts, peerCount)
}

func selectAlternativePeers(
	peers []*p2p.PeerInfo,
	assignedPeerID string,
	assignedBaseURL string,
	maxAttempts int,
) []*p2p.PeerInfo {
	candidateCap := alternativePeerCapacity(maxAttempts, len(peers))
	selected := make([]*p2p.PeerInfo, 0, candidateCap)
	seenURLs := make(map[string]struct{}, candidateCap)
	if assignedBaseURL != "" {
		seenURLs[assignedBaseURL] = struct{}{}
	}

	for _, peer := range peers {
		if peer == nil || peer.DataHubURL == "" || peer.ID.String() == assignedPeerID {
			continue
		}
		if _, exists := seenURLs[peer.DataHubURL]; exists {
			continue
		}
		seenURLs[peer.DataHubURL] = struct{}{}
		selected = append(selected, peer)
		if len(selected) == candidateCap {
			break
		}
	}

	return selected
}

// fetchSubtreeAndDataFromPeer fetches the subtree and then its subtreeData from a
// single peer. With bypassCache set, both requests carry a cache-busting query
// parameter. shutdownCtx is threaded to fetchAndStoreSubtreeData so its detached
// download survives sibling-errgroup cancellation but still aborts on node shutdown.
func (u *Server) fetchSubtreeAndDataFromPeer(ctx context.Context, shutdownCtx context.Context, block *model.Block, subtreeHash *chainhash.Hash,
	peerID, baseURL string, bypassCache bool) error {
	subtree, err := u.fetchAndStoreSubtree(ctx, block, subtreeHash, peerID, baseURL, bypassCache)
	if err != nil {
		return err
	}

	return u.fetchAndStoreSubtreeData(ctx, shutdownCtx, block, subtreeHash, subtree, peerID, baseURL, bypassCache)
}

// tryPeerForSubtree fetches subtree + subtreeData from one peer, retrying that same
// peer exactly once with a cache-busting URL when its response looked poisoned.
// "Poisoned" is what carries the cache-bypass marker, and that is narrower than "any
// 200 with a short body": a subtree_data body that is empty or cannot satisfy the
// subtree, or a strictly empty /subtree body. A truncated-but-nonzero /subtree body
// is not covered — it fails in the subtree parser on a different, unmarked path.
//
// A peer whose proxy cache is replaying a failed generation is the issue-1368 stall:
// without the bypass no peer behind that cache can serve the subtree for the whole
// TTL, and the node cannot pass the checkpoint. The bypass only fires after a
// detected poisoning, so a healthy fleet never pays for it. The already-stored
// subtree file makes the retry's /subtree fetch a local load, so only subtree_data
// is re-requested.
func (u *Server) tryPeerForSubtree(ctx context.Context, shutdownCtx context.Context, block *model.Block, subtreeHash *chainhash.Hash,
	peerID, baseURL string) error {
	err := u.fetchSubtreeAndDataFromPeer(ctx, shutdownCtx, block, subtreeHash, peerID, baseURL, false)
	if err == nil {
		return nil
	}

	if !isCacheBypassRetryable(err) {
		// recordCatchupPeerFailure itself skips errors.IsLocalError — a local failure
		// (context cancellation, storage) is ours, not the peer's.
		u.recordCatchupPeerFailure(peerID, err)

		return err
	}

	u.logger.Warnf("[catchup:fetchAndStoreSubtreeAndSubtreeData] Peer %s served an unusable response for subtree %s, retrying with cache bypass: %v", peerID, subtreeHash.String(), err)

	bypassErr := u.fetchSubtreeAndDataFromPeer(ctx, shutdownCtx, block, subtreeHash, peerID, baseURL, true)
	if bypassErr != nil {
		u.recordCatchupPeerFailure(peerID, bypassErr)
	}

	return bypassErr
}

// fetchAndStoreSubtreeAndSubtreeData fetches both subtree and subtreeData for a single subtree hash
// and stores them in the subtreeStore. If the primary peer fails, it will try alternative peers
// at max height before giving up.
// Returns the peer ID that actually served the data and any error.
// fetchAndStoreSubtreeAndSubtreeData fetches both subtree and subtreeData for a single subtree hash
// and stores them in the subtreeStore. If the primary peer fails, it tries the block's alternative
// peers (bounded, pruned-aware, via selectAlternativePeers over the block snapshot) before giving
// up. Per-peer failures are attributed via recordCatchupPeerFailure inside tryPeerForSubtree and
// drained at catchup release. Returns the peer ID that actually served the data and any error.
func (u *Server) fetchAndStoreSubtreeAndSubtreeData(ctx context.Context, shutdownCtx context.Context, block *model.Block, subtreeHash *chainhash.Hash,
	peerID, baseURL string, peerSnapshot *catchupPeerSnapshot) (string, error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "fetchAndStoreSubtreeAndSubtreeData",
		tracing.WithParentStat(u.stats),
	)
	defer deferFn()

	// Try the primary peer first.
	err := u.tryPeerForSubtree(ctx, shutdownCtx, block, subtreeHash, peerID, baseURL)
	if err == nil {
		return peerID, nil
	}

	// A local error means our own storage or context failed — another peer cannot fix
	// that, so do not spend attempts on the rest of the fleet.
	if errors.IsLocalError(err) {
		return "", errors.NewServiceError("[catchup:fetchAndStoreSubtreeAndSubtreeData] Local error fetching subtree %s (not retrying with other peers)", subtreeHash.String(), err)
	}

	u.logger.Warnf("[catchup:fetchAndStoreSubtreeAndSubtreeData] Primary peer %s failed for subtree %s: %v, trying alternatives", peerID, subtreeHash.String(), err)

	primaryErr := err

	attempts := make([]subtreeFetchAttempt, 0, 4)
	attempts = append(attempts, subtreeFetchAttempt{peerID: peerID, baseURL: baseURL, role: "primary", err: err})

	// Alternatives come from the block-level snapshot (loaded once, pruned-aware) bounded by
	// selectAlternativePeers — not a fresh per-subtree GetPeersAtMaxHeight gRPC.
	var alternativePeers []*p2p.PeerInfo
	if peerSnapshot != nil {
		peers, _, snapshotErr := peerSnapshot.get()
		if snapshotErr == nil {
			alternativePeers = selectAlternativePeers(peers, peerID, baseURL, maxSubtreeFailoverPeers)
		}
	}

	if len(alternativePeers) > 0 {
		u.logger.Infof("[catchup:fetchAndStoreSubtreeAndSubtreeData] Trying %d alternative peers for subtree %s", len(alternativePeers), subtreeHash.String())

		for _, altPeer := range alternativePeers {
			altPeerID := altPeer.ID.String()
			altBaseURL := altPeer.DataHubURL
			if altBaseURL == "" {
				continue
			}

			altErr := u.tryPeerForSubtree(ctx, shutdownCtx, block, subtreeHash, altPeerID, altBaseURL)
			if altErr == nil {
				u.logger.Infof("[catchup:fetchAndStoreSubtreeAndSubtreeData] Successfully fetched subtree %s from alternative peer %s", subtreeHash.String(), altPeerID)
				return altPeerID, nil
			}

			u.logger.Warnf("[catchup:fetchAndStoreSubtreeAndSubtreeData] Alternative peer %s failed for subtree %s: %v", altPeerID, subtreeHash.String(), altErr)

			attempts = append(attempts, subtreeFetchAttempt{peerID: altPeerID, baseURL: altBaseURL, role: "alternative", err: altErr})

			if errors.IsLocalError(altErr) {
				return "", errors.NewServiceError("[catchup:fetchAndStoreSubtreeAndSubtreeData] Local error fetching subtree %s (aborting peer retry)", subtreeHash.String(), altErr)
			}
		}
	}

	// All peers failed. Classify as ErrExternal — every peer we tried returned bad data
	// or rejected/failed the request, but none of the local infrastructure (subtree
	// store, blockchain client, context) failed. ErrServiceError is reserved for genuine
	// local failures and the catchup top-level handler short-circuits ErrServiceError
	// into a silent "clear markers, retry" loop that hides peer-data-quality issues.
	// With ErrExternal the handler reports peer failure and lets P2P switch peers instead.
	//
	// The wrapped cause is the PRIMARY's error (the peer catchup selected); the full
	// per-peer summary rides in the message so no attempt is lost (issue 1368, Defect A).
	// markCatchupFailureReported tags this as already attributed so processCatchupChItem's
	// rotation signal is not double-counted as reputation.
	return "", markCatchupFailureReported(errors.NewExternalError("[catchup:fetchAndStoreSubtreeAndSubtreeData] all %d peer attempts failed to fetch subtree %s [%s]", len(attempts), subtreeHash.String(), formatSubtreeFetchAttempts(attempts), primaryErr))
}

// fetchSubtreeFromPeer fetches subtree (for subtreeToCheck) from a peer via HTTP
func (u *Server) fetchSubtreeFromPeer(ctx context.Context, subtreeHash *chainhash.Hash, peerID string, baseURL string, bypassCache bool) ([]byte, error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "fetchSubtreeFromPeer",
		tracing.WithParentStat(u.stats),
	)
	defer deferFn()

	// Bound the whole fetch (all retry attempts + streaming read) by one wall clock so a stalling
	// peer can't hold it for maxAttempts x http_streaming_timeout.
	ctx, cancel := u.withCatchupSubtreeFetchTimeout(ctx)
	defer cancel()

	// Construct URL for subtree endpoint (for subtreeToCheck)
	url := u.peerResourceURL(baseURL, "subtree", subtreeHash, bypassCache)

	u.logger.Debugf("[catchup:fetchSubtreeFromPeer] fetching subtree from %s", url)

	// Bound the body at the receive-side policy cap (MaxIncomingSubtreeBytes). A peer that
	// streams more than this is malicious — fail fast rather than ReadAll into memory.
	// This must be independent of local BlockAssembly.MaximumMerkleItemsPerSubtree, which
	// only controls what *this node* assembles; peers may legitimately produce larger subtrees.
	maxSubtreeBytes := u.settings.SubtreeValidation.MaxIncomingSubtreeBytes

	// WithRetry backs off on 429/503 (peer rate limiting / admission control) rather
	// than failing the whole fetch. The beforeAttempt hook paces EVERY attempt through
	// the per-peer limiter (not just the first issuance), so retries can't re-burst.
	subtreeBytes, err := util.DoHTTPRequestBoundedWithRetry(ctx, url, maxSubtreeBytes,
		func(c context.Context) error { return u.awaitPeerFetchSlot(c, baseURL) })
	if err != nil {
		return nil, errors.NewServiceError("[catchup:fetchSubtreeFromPeer] failed to fetch subtree from %s", util.RedactPeerURL(url), err)
	}

	// Track bytes downloaded from peer
	if u.p2pClient != nil && peerID != "" {
		if err := u.p2pClient.RecordBytesDownloaded(ctx, peerID, uint64(len(subtreeBytes))); err != nil {
			u.logger.Warnf("[fetchSubtreeFromPeer][%s] failed to record %d bytes downloaded from peer %s: %v", subtreeHash.String(), len(subtreeBytes), peerID, err)
		}
	}

	if len(subtreeBytes) == 0 {
		return nil, markCacheBypassRetryable(errors.NewNotFoundError("[catchup:fetchSubtreeFromPeer] empty subtree received from %s", util.RedactPeerURL(url)))
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

// BytesRead returns the number of bytes pulled from the underlying reader so far.
// Callers read it after the stream has been consumed, from the same goroutine that
// consumed it, so no synchronisation is needed.
func (c *countingReadCloser) BytesRead() uint64 {
	return c.bytesRead
}

// fetchSubtreeDataFromPeer fetches subtree data from a peer via HTTP. beforeAttempt (nil = no-op)
// runs before every retry attempt — used to pace each attempt through the per-peer rate limiter on
// a cancellable context (the ctx here may be detached). With bypassCache set, the request carries a
// cache-busting query parameter (issue-1368 poisoned-cache recovery).
func (u *Server) fetchSubtreeDataFromPeer(ctx context.Context, subtreeHash *chainhash.Hash, peerID string, baseURL string, beforeAttempt func(context.Context) error, bypassCache bool) (*countingReadCloser, error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "fetchSubtreeDataFromPeer",
		tracing.WithParentStat(u.stats),
	)
	defer deferFn()

	// NOTE: the wall-clock bound for subtree_data is applied by the caller
	// (fetchAndStoreSubtreeData's download context), NOT here — this function returns a STREAMING
	// body the caller reads after we return, so a defer-cancel here would truncate that stream.

	// peerResourceURL builds <baseURL>/subtree_data/<hash>, appending the cachebust
	// query parameter when bypassCache is set.
	url := u.peerResourceURL(baseURL, "subtree_data", subtreeHash, bypassCache)

	u.logger.Debugf("[catchup:fetchSubtreeDataFromPeer] fetching subtree data from %s", url)

	// Retry on 503/429 — peer's asset service may reject under admission control while
	// it generates the file on-demand from Aerospike, or rate-limit the heavy route.
	// The retry loop backs off (honoring Retry-After when present). beforeAttempt paces
	// each attempt through the per-peer limiter on a cancellable context (this ctx may
	// be detached via context.WithoutCancel by the caller).
	subtreeDataReader, err := util.DoHTTPRequestBodyReaderWithRetryFunc(ctx, url, beforeAttempt)
	if err != nil {
		return nil, errors.NewServiceError("[catchup:fetchSubtreeDataFromPeer] failed to fetch subtree data from %s", util.RedactPeerURL(url), err)
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

const blockBatchCapacityHint = 100

// blockStreamReadBufferMinSize keeps the catchup block-decode read-ahead at bufio's floor ON
// PURPOSE. The decode's io.LimitedReader (decodeBoundedBlock) sits INSIDE this bufio.Reader, so
// bufio's read-ahead pulls from the peer BEFORE the transport-envelope limit can gate it — a larger
// buffer would let a hostile peer's oversized-block tail be drained past the DoS bound that
// TestPeerBlockFetches_StreamAndBoundResponses guards (bytesRead <= maxTransportBytes + this floor).
// Growing it safely would require re-nesting the LimitedReader outside bufio; not worth it for a
// read-ahead micro-optimisation on a path already bounded by the per-attempt fetch pacing.
const blockStreamReadBufferMinSize = 16

type blockResponseLimits struct {
	maxTransportBytes int64
	maxDeclaredBytes  uint64
	enforceDeclared   bool
}

// withCatchupFetchTimeout bounds a single catchup block fetch — all retry attempts AND the
// streaming body read — by the configured catchup-iteration timeout (default 30s). The streaming
// retry helper mints a fresh per-attempt timeout and the catchup path is otherwise deadline-free,
// so without this a peer that stalls each attempt just under the per-attempt streaming timeout
// could hold one fetch for ~maxAttempts x http_streaming_timeout.
func (u *Server) withCatchupFetchTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := time.Duration(u.settings.BlockValidation.CatchupIterationTimeout) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return context.WithTimeout(ctx, timeout)
}

// withCatchupSubtreeFetchTimeout bounds a single catchup subtree/subtree_data fetch — all retry
// attempts AND the streaming body read — by SubtreeFetchTimeout (default 120s). Same rationale as
// withCatchupFetchTimeout: this path is otherwise deadline-free and the streaming retry helper mints
// a fresh per-attempt timeout, so a peer stalling each attempt just under it could hold one subtree
// fetch for maxAttempts x http_streaming_timeout (~30 min) before failover.
func (u *Server) withCatchupSubtreeFetchTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := u.settings.BlockValidation.SubtreeFetchTimeout
	if timeout <= 0 {
		timeout = 120 * time.Second
	}
	return context.WithTimeout(ctx, timeout)
}

func resolveBlockResponseLimits(maxTransportBytes int64, excessiveBlockSize int) (blockResponseLimits, error) {
	if maxTransportBytes <= 0 {
		configErr := errors.NewConfigurationError("blockvalidation_max_incoming_block_bytes must be positive, got %d", maxTransportBytes)
		return blockResponseLimits{}, errors.NewServiceError("invalid local peer block receive configuration", configErr)
	}
	if excessiveBlockSize < 0 {
		configErr := errors.NewConfigurationError("excessiveblocksize must be non-negative, got %d", excessiveBlockSize)
		return blockResponseLimits{}, errors.NewServiceError("invalid local peer block acceptance configuration", configErr)
	}

	limits := blockResponseLimits{maxTransportBytes: maxTransportBytes}
	if excessiveBlockSize > 0 {
		limits.maxDeclaredBytes = uint64(excessiveBlockSize)
		limits.enforceDeclared = true
	}

	return limits, nil
}

// classifyBlockStreamErr classifies a block-stream fetch/parse failure. See classifyPeerFetchCtxErr
// for the load-bearing subtleties.
func classifyBlockStreamErr(ctx context.Context, hash *chainhash.Hash, err error) error {
	return classifyPeerFetchCtxErr(ctx, err,
		fmt.Sprintf("[catchup:blockFetch][%s] block response aborted by caller", hash.String()),
		fmt.Sprintf("[catchup:blockFetch][%s] peer block response timed out", hash.String()))
}

// decodeBoundedBlock decodes one block from r, which must be layered over `limited` — the transport
// budget SHARED across a whole response. The caller owns the LimitedReader so a multi-block batch is
// bounded in aggregate (one budget for the entire response), not per block; passing a fresh limiter
// per call would give an n x cap ceiling with no bound on one batch's resident set.
func decodeBoundedBlock(r io.Reader, limited *io.LimitedReader, limits blockResponseLimits) (*model.Block, error) {
	var block *model.Block
	var err error
	if limits.enforceDeclared {
		// Transport path: bound the buffered coinbase by the response envelope. This can never
		// reject a consensus-valid block (a coinbase cannot exceed the block, which already has
		// to fit the envelope), and the trusted decode paths (NewBlockFromReader/Bytes) stay
		// unbounded so store read-back and consensus validation accept any valid coinbase.
		block, err = model.NewBlockFromReaderWithDeclaredSizeLimit(r, limits.maxDeclaredBytes, limits.maxTransportBytes)
	} else {
		// No declared-size policy configured; the transport LimitedReader (limited) still bounds
		// every read, coinbase included, to maxTransportBytes.
		block, err = model.NewBlockFromReader(r)
	}
	if err != nil {
		if limited.N == 0 {
			return nil, errors.NewExternalError("peer block response reached transport envelope limit of %d bytes", limits.maxTransportBytes)
		}
		// A TRUNCATED response (peer restart, TCP RST, LB/proxy close mid-stream) surfaces from the
		// model as a BlockInvalidError wrapping io.EOF/io.ErrUnexpectedEOF. Return a FRESH, UNWRAPPED
		// external error for it: (*Error).Is matches by code ANYWHERE in the chain, so if the
		// BlockInvalid code rode along in a wrapped error, catchup.go's validation_failure case
		// (evaluated before the ErrExternal case) would report an HONEST peer as malicious and pin its
		// reputation. Unwrapped is load-bearing — the same idiom classifyPeerFetchCtxErr uses for its
		// timeout branch. (limited.N == 0 above already peeled off the oversized-block case.)
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return nil, errors.NewExternalError("peer block response truncated (read %d of up to %d transport bytes)", limits.maxTransportBytes-limited.N, limits.maxTransportBytes)
		}
		// A decode failure on a fully-received response is the peer's bad payload: classify external
		// so catchup fails over. The model's block-layer cause (declared size / go-bt limit) is
		// preserved — a structurally invalid complete block is genuinely the peer serving bad data.
		return nil, errors.NewExternalError("peer block response failed to decode", err)
	}

	if block == nil {
		return nil, errors.NewBlockInvalidError("peer block response decoded to nil block")
	}
	if limits.enforceDeclared && block.SizeInBytes > limits.maxDeclaredBytes {
		return nil, errors.NewExternalError("peer block declared size %d exceeds excessiveblocksize %d", block.SizeInBytes, limits.maxDeclaredBytes)
	}

	return block, nil
}

func (u *Server) trackedBlockResponse(ctx context.Context, reader io.ReadCloser, hash *chainhash.Hash, peerID, operation string) io.ReadCloser {
	return &countingReadCloser{
		reader: reader,
		onClose: func(bytesRead uint64) {
			if u.p2pClient == nil || peerID == "" {
				return
			}

			trackCtx, _, deferFn := tracing.DecoupleTracingSpan(ctx, "blockvalidation", "recordBytesDownloaded")
			defer deferFn()
			if err := u.p2pClient.RecordBytesDownloaded(trackCtx, peerID, bytesRead); err != nil {
				u.logger.Warnf("[%s][%s] failed to record %d bytes downloaded from peer %s: %v", operation, hash.String(), bytesRead, peerID, err)
			}
		},
	}
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
	if n == 0 {
		return []*model.Block{}, nil
	}

	limits, err := resolveBlockResponseLimits(
		u.settings.BlockValidation.MaxIncomingBlockBytes,
		u.settings.Policy.ExcessiveBlockSize,
	)
	if err != nil {
		return nil, err
	}

	// Bound the whole fetch — all retry attempts AND the streaming read — by one catchup-iteration
	// timeout. The streaming retry helper mints a fresh per-attempt timeout, and the catchup path
	// otherwise carries no deadline, so a peer that stalls each attempt just under the per-attempt
	// timeout could pin a single fetch for ~1h. This restores the pre-streaming wall clock and
	// activates retryHTTP's deadline-based peer attribution.
	ctx, cancel := u.withCatchupFetchTimeout(ctx)
	defer cancel()

	// WithRetry backs off on 429/503 instead of failing the whole batch; the hook paces
	// every attempt through the per-peer limiter so retries don't re-burst.
	responseBody, err := util.DoHTTPRequestBodyReaderWithRetryFunc(ctx, fmt.Sprintf("%s/blocks/%s?n=%d", baseURL, hash.String(), n),
		func(c context.Context) error { return u.awaitPeerFetchSlot(c, baseURL) })
	if err != nil {
		if classified := classifyBlockStreamErr(ctx, hash, err); classified != nil {
			return nil, classified
		}
		return nil, errors.NewProcessingError("[catchup:fetchBlocksBatch][%s] failed to get blocks from peer", hash.String(), err)
	}
	trackedBody := u.trackedBlockResponse(ctx, responseBody, hash, peerID, "fetchBlocksBatch")
	defer func() { _ = trackedBody.Close() }()

	// One aggregate transport budget for the WHOLE batch response, so n blocks share a single
	// maxTransportBytes ceiling rather than n x maxTransportBytes (a fresh per-block limiter left
	// one batch's resident set unbounded).
	limited := &io.LimitedReader{R: trackedBody, N: limits.maxTransportBytes}
	blockReader := bufio.NewReaderSize(limited, blockStreamReadBufferMinSize)
	capacityHint := blockBatchCapacityHint
	if n < uint32(capacityHint) {
		capacityHint = int(n)
	}
	blocks := make([]*model.Block, 0, capacityHint)
	for count := uint32(0); count < n; count++ {
		if _, err = blockReader.Peek(1); err != nil {
			if classified := classifyBlockStreamErr(ctx, hash, err); classified != nil {
				return nil, classified
			}
			return nil, errors.NewProcessingError("[catchup:fetchBlocksBatch][%s] truncated batch: expected %d blocks, got %d", hash.String(), n, count, err)
		}
		block, decodeErr := decodeBoundedBlock(blockReader, limited, limits)
		if decodeErr != nil {
			if classified := classifyBlockStreamErr(ctx, hash, decodeErr); classified != nil {
				return nil, classified
			}
			return nil, errors.NewProcessingError("[catchup:fetchBlocksBatch][%s] failed to decode block %d of %d", hash.String(), count+1, n, decodeErr)
		}
		blocks = append(blocks, block)
	}
	if _, err = blockReader.Peek(1); err == nil {
		return nil, errors.NewExternalError("[catchup:fetchBlocksBatch][%s] peer returned more than requested %d blocks or trailing data", hash.String(), n)
	} else if !errors.Is(err, io.EOF) {
		if classified := classifyBlockStreamErr(ctx, hash, err); classified != nil {
			return nil, classified
		}
		return nil, errors.NewProcessingError("[catchup:fetchBlocksBatch][%s] failed checking batch boundary", hash.String(), err)
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
	limits, err := resolveBlockResponseLimits(
		u.settings.BlockValidation.MaxIncomingBlockBytes,
		u.settings.Policy.ExcessiveBlockSize,
	)
	if err != nil {
		return nil, err
	}

	// Bound the whole fetch (retries + streaming read) by one catchup-iteration timeout; see
	// withCatchupFetchTimeout and the note in fetchBlocksBatch.
	ctx, cancel := u.withCatchupFetchTimeout(ctx)
	defer cancel()

	// WithRetry backs off on 429/503 (peer rate limiting) instead of failing; the hook
	// paces every attempt through the per-peer limiter so retries don't re-burst.
	responseBody, err := util.DoHTTPRequestBodyReaderWithRetryFunc(ctx, fmt.Sprintf("%s/block/%s", baseURL, hash.String()),
		func(c context.Context) error { return u.awaitPeerFetchSlot(c, baseURL) })
	if err != nil {
		if classified := classifyBlockStreamErr(ctx, hash, err); classified != nil {
			return nil, classified
		}
		return nil, errors.NewProcessingError("[catchup:fetchSingleBlock][%s] failed to get block from peer", hash.String(), err)
	}
	trackedBody := u.trackedBlockResponse(ctx, responseBody, hash, peerID, "fetchSingleBlock")
	defer func() { _ = trackedBody.Close() }()

	limited := &io.LimitedReader{R: trackedBody, N: limits.maxTransportBytes}
	blockReader := bufio.NewReaderSize(limited, blockStreamReadBufferMinSize)
	block, err := decodeBoundedBlock(blockReader, limited, limits)
	if err != nil {
		if classified := classifyBlockStreamErr(ctx, hash, err); classified != nil {
			return nil, classified
		}
		return nil, errors.NewProcessingError("[catchup:fetchSingleBlock][%s] failed to create block from bytes", hash.String(), err)
	}
	if _, err = blockReader.Peek(1); err == nil {
		return nil, errors.NewExternalError("[catchup:fetchSingleBlock][%s] peer returned trailing block data", hash.String())
	} else if !errors.Is(err, io.EOF) {
		if classified := classifyBlockStreamErr(ctx, hash, err); classified != nil {
			return nil, classified
		}
		return nil, errors.NewProcessingError("[catchup:fetchSingleBlock][%s] failed checking block boundary", hash.String(), err)
	}

	// The peer chooses the response body, so a well-formed block is not necessarily the
	// block that was asked for. Callers treat the two identities interchangeably (the
	// in-flight marker is keyed on the requested hash while every later lookup keys on
	// the served block), so a substitution would leave that marker undeletable and
	// suppress every subsequent honest announcement of the requested hash. Reject the
	// substitution here instead, mirroring verifyBlockHeaders on the batch path.
	if !block.Hash().IsEqual(hash) {
		return nil, errors.NewProcessingError("[catchup:fetchSingleBlock][%s] peer served block %s for a different hash",
			hash.String(), block.Hash().String())
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
		if !block.Hash().IsEqual(headers[j].Hash()) {
			return errors.NewProcessingError("[catchup:batchFetchAndDistribute][%s] block hash mismatch at index %d: expected %s, got %s",
				blockUpTo.Hash().String(), j, headers[j].Hash().String(), block.Hash().String())
		}
	}
	return nil
}
