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
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/adaptivefetch"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

// peerBlockFetchTimeout bounds a single peer HTTP fetch of block data - whether a batch of
// blocks or one block - so a slow or hostile peer cannot hold a fetch goroutine open
// indefinitely. util.DoHTTPRequestBodyReader falls back to http_streaming_timeout when the
// context carries no deadline of its own (600 s as shipped in settings.conf, 300 s compiled-in
// default, sized for large subtree_data downloads), which is far too generous a budget for a
// block message; every block fetch call site sets this explicitly instead of relying on that
// fallback.
const peerBlockFetchTimeout = 30 * time.Second

// overSendProbeTimeout bounds fetchBlocksBatch's diagnostic read past the last requested block.
// It is diagnostic only, so it must never spend a meaningful share of peerBlockFetchTimeout.
const overSendProbeTimeout = 2 * time.Second

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
	// freshlyWritten records, per (hash, fileType), exactly which peer-supplied subtree blobs
	// THIS fetch attempt itself wrote for this block — as opposed to found already present
	// locally. removeCatchupSubtreeFiles restricts deletion to these pairs on a later corrupt
	// verdict (bitcoin-sv/teranode#4692), so a doctored body naming an already-persisted hash never
	// triggers deletion of that hash's promoted blobs.
	freshlyWritten map[chainhash.Hash]map[fileformat.FileType]struct{}
}

// blockForValidation wraps a block with metadata about which peers contributed data
type blockForValidation struct {
	block             *model.Block
	contributingPeers map[string]struct{}
	freshlyWritten    map[chainhash.Hash]map[fileformat.FileType]struct{}
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

		// Fetch entire batch in one HTTP request, from last block, since the data is returned newest-first.
		// Bound the fetch explicitly: when ctx carries no deadline, DoHTTPRequestBodyReader (used since
		// bitcoin-sv/teranode#4742) falls back to http_streaming_timeout (600 s in settings.conf), where the
		// old io.ReadAll-based DoHTTPRequest fell back to http_timeout (30 s in settings.conf).
		fetchCtx, fetchCancel := context.WithTimeout(ctx, peerBlockFetchTimeout)
		blocks, err := u.fetchBlocksBatch(fetchCtx, batchHeaders[len(batchHeaders)-1].Hash(), uint32(len(batchHeaders)), peerID, baseURL)
		fetchCancel()
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
			var freshlyWritten map[chainhash.Hash]map[fileformat.FileType]struct{}
			var err error
			if optimistic {
				contributingPeers, freshlyWritten, err = nil, nil, nil
			} else {
				// Reserve before the prewarm: fetchSubtreeDataForBlock parses every transaction of every
				// subtree of this block into memory concurrently, and the sum of a block's subtree_data
				// is ~block.SizeInBytes when the payload is in standard transaction format
				// (bsv-blockchain/teranode#1139). The declared size is a HEURISTIC, not a bound: a peer may
				// serve extended-format transactions, which the parser accepts and which carry an extra
				// PreviousTxScript per input. See boundSubtreeConcurrencyByBudget.
				//
				// The release must run on EVERY exit of the fetch, including error. It is written
				// inline rather than deferred on purpose: a bare defer here is scoped to the whole
				// for loop, not to this iteration, so it would hold every reservation until the
				// worker exits and permanently wedge catch-up.
				var weight int64

				weight, err = u.acquireCatchupPrefetch(ctx, work.block)
				if err == nil {
					fetchFn := u.fetchSubtreeDataForBlockFn
					if fetchFn == nil {
						fetchFn = u.fetchSubtreeDataForBlock
					}

					contributingPeers, freshlyWritten, err = fetchFn(ctx, work.block, peerID, baseURL)

					u.releaseCatchupPrefetch(weight)
				}
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
				freshlyWritten:    freshlyWritten,
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
					case validateBlocksChan <- blockForValidation{block: orderedResult.block, contributingPeers: orderedResult.contributingPeers, freshlyWritten: orderedResult.freshlyWritten}:
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
// Returns a map of peer IDs that contributed subtree data for this block, and the set of
// (hash, fileType) pairs this call itself freshly wrote — as opposed to found already present
// locally — for removeCatchupSubtreeFiles to later restrict deletion to on a corrupt verdict
// (bitcoin-sv/teranode#4692).
func (u *Server) fetchSubtreeDataForBlock(gCtx context.Context, block *model.Block, peerID, baseURL string) (map[string]struct{}, map[chainhash.Hash]map[fileformat.FileType]struct{}, error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(gCtx, "fetchSubtreeDataForBlock",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[catchup:fetchSubtreeDataForBlock][%s] fetching subtree data for block with %d subtrees", block.Hash().String(), len(block.Subtrees)),
	)
	defer deferFn()

	if len(block.Subtrees) == 0 {
		u.logger.Debugf("[catchup:fetchSubtreeDataForBlock] Block %s has no subtrees, skipping", block.Hash().String())

		return nil, nil, nil
	}

	// Scoped to this single fetch attempt for this block — never reused across blocks or
	// across attempts (bitcoin-sv/teranode#4692).
	freshness := newSubtreeFreshness()

	// Track which peers contributed subtree data for this block
	var peersMu sync.Mutex
	contributingPeers := make(map[string]struct{})

	// Create error group for concurrent subtree fetching
	g, ctx := errgroup.WithContext(ctx)
	// Limit concurrency to avoid overwhelming the peer
	// This can be adjusted based on peer capabilities and network conditions
	subtreeConcurrency := 8 // fail-safe when the setting is unset or non-positive; the real default is 32 (settings.go)
	if u.settings.BlockValidation.SubtreeFetchConcurrency > 0 {
		subtreeConcurrency = u.settings.BlockValidation.SubtreeFetchConcurrency
	}

	subtreeConcurrency = u.boundSubtreeConcurrencyByBudget(subtreeConcurrency, block)

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
			servingPeerID, err := u.fetchAndStoreSubtreeAndSubtreeData(ctx, block, &subtreeHashCopy, capturedPeerID, capturedBaseURL, freshness)
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
		return nil, nil, errors.NewServiceError("[catchup:fetchSubtreeDataForBlock] Failed to fetch subtree data for block %s", block.Hash().String(), err)
	}

	return contributingPeers, freshness.snapshot(), nil
}

// fetchAndStoreSubtree fetches and stores only the subtree (for subtreeToCheck)
func (u *Server) fetchAndStoreSubtree(ctx context.Context, block *model.Block, subtreeHash *chainhash.Hash, peerID, baseURL string, bypassCache bool, freshness *subtreeFreshness) (*subtreepkg.Subtree, error) {
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
			return nil, errors.NewProcessingError("[catchup:fetchAndStoreSubtree] Failed to deserialize existing subtree for %s", subtreeHash.String(), err)
		}

		return subtree, nil
	}

	// Fetch subtree from peer
	subtreeNodeBytes, subtreeErr := u.fetchSubtreeFromPeer(ctx, subtreeHash, peerID, baseURL, bypassCache)
	if subtreeErr != nil {
		return nil, errors.NewServiceError("[catchup:fetchAndStoreSubtree] Failed to fetch subtree for %s", subtreeHash.String(), subtreeErr)
	}

	// The response must be a whole number of node hashes. The integer division below silently
	// discards a trailing partial hash, which would make a TRUNCATED or length-inconsistent response
	// indistinguishable from doctored bytes at the root check further down — and that check strikes
	// the serving peer for a corrupt block body. Reject the malformed shape here instead, with no
	// strike, so the strike is reserved for a well-formed node list that hashes to the wrong root
	// (bitcoin-sv/teranode#4692). subtreevalidation guards the same case on its own fetch branch via
	// validateSubtreeLeafCount.
	//
	// Marked cache-bypass retryable: a truncated body is the issue-1368 signature — a caching layer in
	// front of the peer replaying a failed or aborted on-demand generation as a 200. Without the marker
	// no peer behind that cache can serve this subtree for the whole TTL. The marker only sets data, so
	// the ProcessingError class and the no-strike decision above are untouched.
	//
	// It does two things, not one. It buys one retry of this same peer with a cache-busting URL, and it
	// DEFERS the peer-failure charge to that retry: tryPeerForSubtree takes the retryable branch, so the
	// first attempt's recordCatchupPeerFailure is skipped and only a failing bypass records one. A peer
	// that stays broken is therefore still charged, exactly once; a peer whose cache-busted response is
	// honest is not charged at all, which is the correct attribution because the fault was the cache's.
	// The error the caller sees is the bypass attempt's, not this one.
	if len(subtreeNodeBytes)%chainhash.HashSize != 0 {
		return nil, markCacheBypassRetryable(errors.NewProcessingError("[catchup:fetchAndStoreSubtree] peer %s served %d bytes for subtree %s, not a whole number of %d-byte node hashes",
			peerID, len(subtreeNodeBytes), subtreeHash.String(), chainhash.HashSize))
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

	// The peer's node bytes must hash to the subtree we asked for. Without this the blob is stored
	// under a filename it does not match, findLocalSubtreeFile short-circuits to it on retry, and the
	// resulting block-level merkle mismatch is charged to the catch-up primary instead of to the peer
	// that served the bytes (bitcoin-sv/teranode#4692). Mirrors subtreevalidation.CheckBlockSubtrees'
	// identical check on the RUNNING fetch branch.
	//
	// Deliberately a ProcessingError and NOT a BlockCorruptError, matching that precedent's class:
	// this error travels the fetch path, and a corrupt code anywhere in the chain would hit
	// reportCatchupFailureForError's corrupt exemption and suppress the legitimate all-peers-failed
	// charge. It is also not IsLocalError, so tryPeerForSubtree's recordCatchupPeerFailure charge
	// against the serving peer still lands and fetchAndStoreSubtreeAndSubtreeData can still try an
	// honest alternative.
	// The root == nil arm is unreachable today — RootHash's contract permits nil, but all three of
	// its nil paths are excluded here: the receiver is non-nil, the zero-node guard above rules out
	// Length()==0, and its BuildMerkleTreeStoreFromBytes path cannot fail (every return in that
	// function returns a nil error). Kept anyway, because it is free and it is the dependency's
	// declared error path: if a future version starts failing there, this treats it as a mismatch
	// rather than binding unverified bytes to the requested hash.
	if root := subtree.RootHash(); root == nil || !subtreeHash.IsEqual(root) {
		computed := "<nil>"
		if root != nil {
			computed = root.String()
		}

		// Strike ONLY once the cache explanation has been eliminated (bitcoin-sv/teranode#4692). With a
		// caching layer interposed, "these bytes do not match this hash" is a claim about the cache, not
		// about the peer: a poisoned non-empty wrong-root generation replayed for the whole TTL would
		// otherwise charge every peer behind that cache. bypassCache is true only on the cache-busted
		// retry tryPeerForSubtree issues after the marker below, so the sequence is: attempt 1 mismatch,
		// no strike, marker set -> retry with cache bypass -> attempt 2 mismatch, strike. This can only
		// under-strike, never over-strike, which is the safe direction here — excluding a
		// possibly-sole-source honest peer is the self-isolation this work exists to prevent. A
		// genuinely malicious peer is still struck, one request later.
		//
		// penalizeCorruptBlockPeer already returns early for a nil p2pClient, an empty peerID and a
		// legacy-namespaced peerID, so no further guard is needed; the u.blockValidation nil check is,
		// because several get_blocks tests construct a bare Server.
		if bypassCache && u.blockValidation != nil {
			u.blockValidation.penalizeCorruptBlockPeer(ctx, peerID, block, "subtree root hash mismatch on catchup fetch")
		}

		// Marked cache-bypass retryable so the bypass above is actually reached. The marker only sets
		// data: the ProcessingError class is deliberate (a corrupt code here would hit
		// reportCatchupFailureForError's corrupt exemption) and is preserved, as is non-IsLocalError so
		// tryPeerForSubtree's peer-failure charge still lands.
		return nil, markCacheBypassRetryable(errors.NewProcessingError("[catchup:fetchAndStoreSubtree] peer %s served subtree bytes for %s that hash to %s", peerID, subtreeHash.String(), computed))
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

	// This attempt itself just wrote FileTypeSubtreeToCheck for this hash — eligible for
	// removeCatchupSubtreeFiles to delete later if this block turns out corrupt
	// (bitcoin-sv/teranode#4692). The localExists branch above never reaches here, so a
	// pre-existing (possibly permanently-promoted) blob for this hash is never marked fresh.
	freshness.markFresh(*subtreeHash, fileformat.FileTypeSubtreeToCheck)

	// Reputation is credited post-validation in validateBlocksOnChannel via reportValidBlockForPeers

	return subtree, nil
}

// minCatchupPrefetchWeight is the floor charged for one block. Without it a run of tiny
// blocks admits an unbounded number of concurrent prewarms within the byte budget; with it
// the in-flight count can never exceed budget/floor. FetchNumWorkers already caps that at 16
// today, so this is insurance against a raised worker count. Same value and reasoning as
// netsync's minInFlightBlockWeight.
const minCatchupPrefetchWeight = 64 * 1024

// acquireCatchupPrefetch reserves capacity for one block's subtree-data prewarm and returns
// the weight to hand back. A nil budget (disabled) is a no-op returning (0, nil). An
// already-cancelled context returns ctx.Err() with nothing reserved, BEFORE the fast path,
// so the cancellation contract holds whether or not capacity happens to be free. The weight
// is the block's declared size, floored at minCatchupPrefetchWeight and clamped to the whole
// budget, so an oversized block is admitted alone rather than deadlocking against a budget it
// cannot fit in. On error nothing was reserved and the caller must not release.
func (u *Server) acquireCatchupPrefetch(ctx context.Context, block *model.Block) (int64, error) {
	if u.catchupPrefetchBudget == nil {
		return 0, nil
	}

	// Before the fast path, not after: semaphore.Weighted.TryAcquire does not consult the
	// context, so a cancelled caller would otherwise walk away holding a live reservation
	// whenever capacity happened to be free.
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	var size uint64
	if block != nil {
		size = block.SizeInBytes
	}

	// block.SizeInBytes is peer-supplied. A plain cast of a hostile value can go negative,
	// and semaphore.Acquire with a negative weight is undefined, so an unrepresentable size
	// is treated as at least the whole budget and clamped below.
	weight, convErr := safeconversion.Uint64ToInt64(size)
	if convErr != nil {
		weight = u.catchupPrefetchBudgetBytes
	}

	if weight < minCatchupPrefetchWeight {
		weight = minCatchupPrefetchWeight
	}

	if weight > u.catchupPrefetchBudgetBytes {
		weight = u.catchupPrefetchBudgetBytes
	}

	// Fast path: capacity available right now.
	if u.catchupPrefetchBudget.TryAcquire(weight) {
		return weight, nil
	}

	// Warn, not debug: a parked worker stops draining the work queue, so a sustained rate here
	// is catch-up being throttled by blockvalidation_catchup_prefetch_budget_bytes and is the
	// operator's signal to size it against measured headroom.
	u.logger.Warnf("[catchup:acquireCatchupPrefetch][%s] parked waiting for %d bytes of prefetch budget (blockvalidation_catchup_prefetch_budget_bytes=%d)", catchupPrefetchBlockName(block), weight, u.catchupPrefetchBudgetBytes)

	if prometheusCatchupPrefetchBudgetParked != nil {
		prometheusCatchupPrefetchBudgetParked.Inc()
	}

	if err := u.catchupPrefetchBudget.Acquire(ctx, weight); err != nil {
		return 0, err
	}

	return weight, nil
}

// catchupPrefetchBlockName renders a block hash for the budget-park log line without
// assuming a header is present. Block.Hash() dereferences Header, and several tests drive
// this path with header-less blocks, so a log line must never be the thing that panics.
func catchupPrefetchBlockName(block *model.Block) string {
	if block == nil || block.Header == nil {
		return "unknown"
	}

	return block.Hash().String()
}

// releaseCatchupPrefetch returns a weight from acquireCatchupPrefetch. Zero weight or a nil
// budget is a no-op.
func (u *Server) releaseCatchupPrefetch(weight int64) {
	if u.catchupPrefetchBudget == nil || weight <= 0 {
		return
	}

	u.catchupPrefetchBudget.Release(weight)
}

// boundSubtreeConcurrencyByBudget drops the per-block subtree parse concurrency to 1 when a
// block's declared size exceeds the catch-up prefetch budget.
//
// The block-level reservation clamps such a block to the whole budget and admits it alone.
// That bounds how many BLOCKS parse at once but not how many BYTES one block parses at once:
// SubtreeFetchConcurrency subtrees of a multi-gigabyte block would still materialise
// together. Dropping to 1 bounds it at a single subtree_data payload.
//
// Deliberately NOT an average-based divisor (budget / (SizeInBytes/len(Subtrees))). An
// average says nothing about the worst case under skew: a 4 GiB block with 1024 subtrees
// averages 4 MiB, so an average rule would permit the full 32-way concurrency, yet if two of
// those subtrees hold ~2 GiB each both can be in flight and ~4 GiB is retained. The
// fits/does-not-fit predicate below is skew-independent for the block that does NOT fit: "1"
// depends on no size estimate at all. For a block that DOES fit, the argument is that its
// total subtree_data is ~its declared size, so no distribution of that total across its
// subtrees can exceed the reservation already held for it.
//
// That fitting-block argument is a HEURISTIC, not a bound, and three things can break it:
//   - Format expansion. subtree_data may carry EXTENDED transactions. Tx.ReadFrom auto-detects
//     the 0xEF extended marker and parses a PreviousTxScript per input, and the only check
//     serializeFromReader applies is the txid, which is computed over the STANDARD bytes and
//     so cannot tell the two apart. Teranode's own producers emit standard format (the asset
//     server's on-demand generation and the block persister both write non-extended bytes),
//     but nothing on the receive side enforces it.
//   - SizeInBytes is peer-declared and may be understated.
//   - One subtree_data payload is irreducible: it must be fully materialised before it can be
//     checked against the subtree.
//
// None of the three is detectable cheaply from the bytes alone, so this is a heuristic that
// removes the unbounded case rather than a bound (bsv-blockchain/teranode#1139).
//
// This applies to RevalidateBlock too, which calls fetchSubtreeDataForBlock directly. That is
// deliberate: unlike the semaphore, this rule never makes an operator operation WAIT on
// catch-up — it only lowers its own internal parallelism — and an oversized block revalidated
// inline has exactly the same memory shape as one in catch-up. Scoping it to catch-up would
// mean threading a flag through the fetchSubtreeDataForBlockFn seam, a wider diff than the fix
// itself.
//
// A block that declares NO size is treated the same way, because an undeclared size is the one
// value a hostile peer pays nothing to supply: it is the cheapest way to claim the configured
// 32-way fan-out while promising nothing. It costs honest work nothing in practice — a block
// taken from the blockchain store always carries a real SizeInBytes, which is what
// RevalidateBlock passes — and it leaves the catch-up receive path, where the declaration comes
// off the wire, as the only place the case arises.
//
// The asymmetry with acquireCatchupPrefetch is deliberate, not drift. That function charges a
// capacity RESERVATION, so an undeclared size is floored rather than maximised: a reservation
// must be finite and over-charging an undeclared block would park legitimate work. This function
// applies a TRUST PREDICATE, where the same undeclared size is least worth trusting. The
// residual is real: several blocks declaring 0 are still all admitted by the semaphore, but each
// then parses one subtree at a time instead of blockvalidation_subtree_fetch_concurrency
// (bsv-blockchain/teranode#1139).
func (u *Server) boundSubtreeConcurrencyByBudget(configured int, block *model.Block) int {
	if u.catchupPrefetchBudgetBytes <= 0 || block == nil {
		return configured
	}

	size, err := safeconversion.Uint64ToInt64(block.SizeInBytes)

	switch {
	case err != nil:
		// A declaration that does not fit an int64 exceeds every positive budget, so it is
		// oversized; only its diagnostic text differs from the over-budget case.
		u.warnSubtreeConcurrencyClamped(block, "declared a size too large to represent, which exceeds any budget")

		if prometheusCatchupPrefetchOversizedBlocks != nil {
			prometheusCatchupPrefetchOversizedBlocks.Inc()
		}

		return 1

	case size == 0:
		u.warnSubtreeConcurrencyClamped(block, "declared no size; an undeclared size is not trusted with the configured concurrency")

		if prometheusCatchupPrefetchUndeclaredSizeBlocks != nil {
			prometheusCatchupPrefetchUndeclaredSizeBlocks.Inc()
		}

		return 1

	case size > u.catchupPrefetchBudgetBytes:
		u.warnSubtreeConcurrencyClamped(block, fmt.Sprintf("declared %d bytes, which exceeds the catch-up prefetch budget", size))

		if prometheusCatchupPrefetchOversizedBlocks != nil {
			prometheusCatchupPrefetchOversizedBlocks.Inc()
		}

		return 1
	}

	return configured
}

// warnSubtreeConcurrencyClamped emits the single log line for a block whose subtree parse
// concurrency has been dropped to 1. It is only called from the clamping branches, so it never
// claims a change that did not happen. The logger can be nil on the bare &Server{...} several
// tests build, and a log line must never be the thing that panics.
func (u *Server) warnSubtreeConcurrencyClamped(block *model.Block, reason string) {
	if u.logger == nil {
		return
	}

	u.logger.Warnf("[catchup:boundSubtreeConcurrencyByBudget][%s] block %s; parsing its subtrees one at a time (blockvalidation_catchup_prefetch_budget_bytes=%d)",
		catchupPrefetchBlockName(block), reason, u.catchupPrefetchBudgetBytes)
}

// subtreeDataFetchTimeout resolves the bound for one detached subtree_data fetch.
//
// It fails closed: a nil settings object, or a non-positive configured value, yields the
// default rather than "no limit". The fetch this bounds is peer-controlled, so treating
// an unset or unparsed value as unbounded would silently restore the behaviour the bound
// exists to remove. Nil-tolerant because several tests construct a bare settings object.
func subtreeDataFetchTimeout(tSettings *settings.Settings) time.Duration {
	if tSettings == nil || tSettings.BlockValidation.SubtreeDataFetchTimeout <= 0 {
		return settings.DefaultSubtreeDataFetchTimeout
	}

	return tSettings.BlockValidation.SubtreeDataFetchTimeout
}

// fetchAndStoreSubtreeData fetches and stores only the subtreeData
func (u *Server) fetchAndStoreSubtreeData(ctx context.Context, block *model.Block, subtreeHash *chainhash.Hash,
	subtree *subtreepkg.Subtree, peerID, baseURL string, bypassCache bool, freshness *subtreeFreshness) (err error) {
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

	// Detach from sibling cancellation: this function is called from a per-subtree
	// goroutine inside fetchSubtreeDataForBlock's errgroup. Using gCtx for the HTTP
	// fetch + parse + store means a single sibling failure cancels every in-flight
	// subtree_data download in the batch — and each cancellation closes the upstream
	// connection, causing the peer to abort its on-demand creation (storer.Abort) and
	// throw away Aerospike work that was already paid for. Detaching here lets each
	// fetch run to completion so the peer can finish writing its subtreeData file. The
	// existence check above still respects the original ctx, so a pre-cancelled call
	// still exits early.
	//
	// The deadline is what keeps that detachment bounded, and it must be applied here
	// rather than left to the HTTP layer. context.WithoutCancel returns a context with
	// no deadline AND a nil Done channel, which has two consequences downstream in
	// DoHTTPRequestBodyReaderWithRetry: its `case <-ctx.Done()` abort can never be
	// selected, so the retry loop always runs to maxAttempts, and each attempt sees no
	// deadline and installs a *fresh* http_streaming_timeout of its own. A hostile peer
	// answering 503 and then dripping bytes therefore held one fetch for maxAttempts ×
	// http_streaming_timeout. Setting a deadline here fixes both at once: Done() fires
	// again for the retry guard, and because the per-attempt timeout is only installed
	// when the context has no deadline, every attempt now shares this single bound.
	timeout := subtreeDataFetchTimeout(u.settings)

	parentCtx := ctx

	ctx, cancelDetached := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancelDetached()

	// Keep an expiry of our own bound attributable to the peer. Every error below this
	// point would otherwise surface as a context error, and errors.IsLocalError treats
	// those as ours: fetchAndStoreSubtreeAndSubtreeData would skip alternative-peer
	// failover and recordCatchupPeerFailure would decline to charge the peer, so a peer
	// that stalls until the bound fires would stay in rotation unrecorded. Before this
	// bound existed the same peer exhausted the retry loop and surfaced as
	// ErrServiceUnavailable, which is attributable, so leaving it as a context error
	// would be a regression in exactly the case the bound exists to contain.
	//
	// Only re-attribute while the caller's context is still alive. A genuinely cancelled
	// or expired parent is our shutdown, not the peer's fault, and stays local. The
	// parent is read rather than the detached context so this does not depend on defer
	// ordering relative to cancelDetached.
	//
	// The replacement must not carry the context error, neither wrapped as a cause nor
	// rendered into the message: IsContextError matches both, so either form silently
	// restores the local classification this is undoing.
	defer func() {
		if err != nil && parentCtx.Err() == nil && errors.IsContextError(err) {
			err = errors.NewServiceUnavailableError(
				"[catchup:fetchAndStoreSubtreeData] peer %s (%s) exceeded the %s subtree_data bound for %s",
				peerID, baseURL, timeout, subtreeHash.String())
		}
	}()

	subtreeDataReader, err := u.fetchSubtreeDataFromPeer(ctx, subtreeHash, peerID, baseURL, bypassCache)
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

	// Reject a response the subtree cannot be satisfied by, before Serialize turns it
	// into a generic ErrSubtreeLengthMismatch that names no peer — or panics on a nil
	// tx at index 0. model.MissingSubtreeDataTxs owns that reasoning, shared with the
	// subtree meta regenerator's local read so the two cannot drift on it. The
	// regenerator's meta build asks a finer version of the same question, node by
	// node, and keeps its own loop; the index-0 rule is the part the three have to
	// agree on and it lives in the predicate.
	//
	// The exemption argument is true here, which is Data.Serialize's own rule
	// rather than validateSubtree's. Nothing on this path builds a meta: it stores
	// the body, and the reason to reject an unsatisfying one is that Serialize
	// walks into a nil *bt.Tx at index 0. Serialize skips index 0 whenever Nodes[0]
	// holds the placeholder, whatever the subtree's position, so a stricter rule
	// here would reject a body Serialize would have handled and charge the peer for
	// it. The regenerator passes the subtree's real position instead, because a
	// meta does have to agree with validateSubtree.
	missing := model.MissingSubtreeDataTxs(subtree, subtreeData, true)

	bytesRead := subtreeDataReader.BytesRead()

	u.logger.Debugf("[catchup:fetchAndStoreSubtreeData] Subtree %s from %s has %d/%d txs (%d bytes, %d missing)",
		subtreeHash.String(), baseURL, len(subtreeData.Txs)-missing, len(subtreeData.Txs), bytesRead, missing)

	if missing > 0 {
		return newPoisonedSubtreeDataError(peerID, baseURL, subtreeHash, missing, subtree.Length(), bytesRead)
	}

	// Stream the transactions straight into the store instead of building a second complete
	// in-memory copy with Serialize() and handing that to Set: the parsed []*bt.Tx is already
	// resident, and one more full serialized copy per in-flight subtree_data is exactly the
	// amplifier #1139 is about. WriteTransactionsToWriter(w, 0, subtree.Length()) emits the
	// same byte stream Serialize() would — both skip index 0 when Nodes[0] is the coinbase
	// placeholder and both write SerializeBytes/SerializeTo, which branch identically on
	// IsExtended() — and it is stricter: it returns ErrTransactionNil where Serialize would
	// dereference a nil tx at index 0.
	//
	// The second full copy is avoided on the FILE store only. The memory and batcher stores
	// io.ReadAll the body and S3 copies it into a bytes.Buffer before upload
	// (stores/blob/s3/s3.go:253-264), so on those the streamed reader is materialised anyway.
	// The subtree store is file-backed in production, which is the deployment this is for.
	pr, pw := io.Pipe()
	done := make(chan struct{})

	// writeErr is written by the producer goroutine below and read after <-done. close(done)
	// and the receive on it are the happens-before edge, so the post-join read is race-free
	// and needs no mutex.
	var writeErr error

	go func() {
		defer close(done)

		// bufio is mandatory, not cosmetic: bt.Tx.SerializeTo delegates to WriteTo, which
		// emits many 4-byte and varint-sized writes, and every write to an unbuffered
		// io.Pipe is a synchronous rendezvous with the reader. The buffer is pooled and
		// 64 KiB rather than a fresh 1 MiB per call, because up to
		// blockvalidation_fetch_num_workers x blockvalidation_subtree_fetch_concurrency of
		// them are live at once and collapsing the rendezvous does not need a megabyte
		// (bsv-blockchain/teranode#1139).
		bw := bufioWriterPool.Get().(*bufio.Writer)
		bw.Reset(pw)

		defer func() {
			// Mandatory: without it a pooled writer holds a live *io.PipeWriter and any
			// residual bytes for as long as it sits in the pool.
			bw.Reset(nil)
			bufioWriterPool.Put(bw)
		}()

		writeErr = subtreeData.WriteTransactionsToWriter(bw, 0, subtree.Length())
		if writeErr == nil {
			writeErr = bw.Flush()
		}

		_ = pw.CloseWithError(writeErr)
	}()

	storeErr := u.subtreeStore.SetFromReader(ctx,
		subtreeHash[:],
		fileformat.FileTypeSubtreeData,
		pr,
		options.WithAllowOverwrite(true),
		options.WithDeleteAt(dah),
	)

	// Order matters. Close the read side FIRST: if SetFromReader returned before draining the
	// pipe, the producer is parked in Write and <-done would deadlock. io.PipeReader.Close is
	// idempotent and makes the producer's next Write/Flush return io.ErrClosedPipe, so it
	// always reaches close(done). The Store.SetFromReader contract on closing its input reader
	// is inconsistent across implementations, which is why this close is ours to make.
	//
	// Joining the producer before returning is what keeps the parsed transactions it holds
	// from outliving this block's prefetch-budget reservation.
	_ = pr.Close()
	<-done

	if err = subtreeDataWriteFailure(peerID, baseURL, subtreeHash, writeErr, storeErr); err != nil {
		return err
	}

	// This attempt itself just wrote FileTypeSubtreeData for this hash — eligible for
	// removeCatchupSubtreeFiles to delete later if this block turns out corrupt
	// (bitcoin-sv/teranode#4692). The subtreeDataExists early return above never reaches here, so
	// a pre-existing (possibly permanently-promoted) blob for this hash is never marked fresh.
	freshness.markFresh(*subtreeHash, fileformat.FileTypeSubtreeData)

	return nil
}

// subtreeDataWriteFailure decides who is at fault when the streamed store write fails.
//
// Check order matters and is NOT interchangeable. Data.WriteTransactionsToWriter wraps writer
// failures with TWO %w verbs (go-subtree subtree_data.go), so an error raised because the store
// aborted and fetchAndStoreSubtreeData then closed the read side satisfies errors.Is for
// ErrTransactionWrite AND for io.ErrClosedPipe at the same time. io.ErrClosedPipe can only come
// from that function's own pr.Close(), which runs after SetFromReader has already returned — so it
// is always a store-first failure and must be tested FIRST, with errors.Is and never with
// equality. Matching the producer sentinels first would charge an innocent peer for our own
// storage failure.
//
// A genuine producer error means the body the PEER served parsed but cannot be re-serialized, and
// must stay peer-attributable: errors.IsLocalError treats ErrStorageError as ours, and a local
// error makes fetchAndStoreSubtreeAndSubtreeData skip alternative-peer failover and
// recordCatchupPeerFailure decline to charge the peer (bsv-blockchain/teranode#1139).
func subtreeDataWriteFailure(peerID, baseURL string, subtreeHash *chainhash.Hash,
	writeErr, storeErr error) error {
	if writeErr == nil && storeErr == nil {
		return nil
	}

	// The nil guard is not redundant: errors.Is normalises its argument through the gRPC
	// unwrapper, which turns a nil error into a typed-nil *Error.
	if writeErr != nil && errors.Is(writeErr, io.ErrClosedPipe) {
		if storeErr != nil {
			return errors.NewStorageError("[catchup:fetchAndStoreSubtreeData] Failed to store subtreeData for %s", subtreeHash.String(), storeErr)
		}

		// The store reported success without draining the body. Still ours, not the peer's.
		return errors.NewStorageError("[catchup:fetchAndStoreSubtreeData] Store stopped reading subtreeData for %s before the body was fully written", subtreeHash.String(), writeErr)
	}

	if writeErr != nil {
		return errors.NewProcessingError("[catchup:fetchAndStoreSubtreeData] Peer %s (%s) provided unusable subtree data for %s",
			peerID, baseURL, subtreeHash.String(), writeErr)
	}

	return errors.NewStorageError("[catchup:fetchAndStoreSubtreeData] Failed to store subtreeData for %s", subtreeHash.String(), storeErr)
}

// fetchSubtreeAndDataFromPeer fetches the subtree and then its subtreeData from a
// single peer. With bypassCache set, both requests carry a cache-busting query
// parameter.
func (u *Server) fetchSubtreeAndDataFromPeer(ctx context.Context, block *model.Block, subtreeHash *chainhash.Hash,
	peerID, baseURL string, bypassCache bool, freshness *subtreeFreshness) error {
	subtree, err := u.fetchAndStoreSubtree(ctx, block, subtreeHash, peerID, baseURL, bypassCache, freshness)
	if err != nil {
		return err
	}

	return u.fetchAndStoreSubtreeData(ctx, block, subtreeHash, subtree, peerID, baseURL, bypassCache, freshness)
}

// tryPeerForSubtree fetches subtree + subtreeData from one peer, retrying that same
// peer exactly once with a cache-busting URL when its response looked poisoned.
// "Poisoned" is what carries the cache-bypass marker: a subtree_data body that is
// empty or cannot satisfy the subtree, and on the /subtree side an empty body, a
// truncated-but-nonzero body (not a whole number of node hashes) or a well-formed
// node list that hashes to the wrong root — all three of the last group marked at
// their rejection sites in fetchAndStoreSubtree (bitcoin-sv/teranode#4692).
//
// The two resources differ in what the retry actually does. A failed subtree_data
// attempt leaves the already-stored subtree file in place, so the retry's /subtree
// fetch is a local load and only subtree_data crosses the wire. A failed /subtree
// attempt stores NOTHING — both rejection sites return before the Set, and the
// local short-circuit at the top of fetchAndStoreSubtree does not consult
// bypassCache — so the retry genuinely re-issues /subtree/<hash>?cachebust=… .
//
// A peer whose proxy cache is replaying a failed generation is the issue-1368 stall:
// without the bypass no peer behind that cache can serve the subtree for the whole
// TTL, and the node cannot pass the checkpoint. The bypass only fires after a
// detected poisoning, so a healthy fleet never pays for it. The already-stored
// subtree file makes the retry's /subtree fetch a local load, so only subtree_data
// is re-requested.
func (u *Server) tryPeerForSubtree(ctx context.Context, block *model.Block, subtreeHash *chainhash.Hash,
	peerID, baseURL string, freshness *subtreeFreshness) error {
	err := u.fetchSubtreeAndDataFromPeer(ctx, block, subtreeHash, peerID, baseURL, false, freshness)
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

	bypassErr := u.fetchSubtreeAndDataFromPeer(ctx, block, subtreeHash, peerID, baseURL, true, freshness)
	if bypassErr != nil {
		u.recordCatchupPeerFailure(peerID, bypassErr)
	}

	return bypassErr
}

// fetchAndStoreSubtreeAndSubtreeData fetches both subtree and subtreeData for a single subtree hash
// and stores them in the subtreeStore. If the primary peer fails, it will try alternative peers
// at max height before giving up.
// Returns the peer ID that actually served the data and any error.
func (u *Server) fetchAndStoreSubtreeAndSubtreeData(ctx context.Context, block *model.Block, subtreeHash *chainhash.Hash,
	peerID, baseURL string, freshness *subtreeFreshness) (string, error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "fetchAndStoreSubtreeAndSubtreeData",
		tracing.WithParentStat(u.stats),
		// tracing.WithDebugLogMessage(u.logger, "[catchup:fetchAndStoreSubtreeAndSubtreeData] fetching subtree and data for %s", subtreeHash.String()),
	)
	defer deferFn()

	// Try the primary peer first.
	err := u.tryPeerForSubtree(ctx, block, subtreeHash, peerID, baseURL, freshness)
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

				altErr := u.tryPeerForSubtree(ctx, block, subtreeHash, altPeerID, altBaseURL, freshness)
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
	}

	// All peers failed. Classify as ErrExternal — every peer we tried returned bad data
	// or rejected/failed the request, but none of the local infrastructure (subtree
	// store, blockchain client, context) failed. ErrServiceError is reserved for genuine
	// local failures and the catchup top-level handler short-circuits ErrServiceError
	// into a silent "clear markers, retry" loop that hides peer-data-quality issues.
	// With ErrExternal the handler reports peer failure and lets P2P switch peers instead.
	//
	// Note on detection: primaryErr usually carries ERR_SERVICE_ERROR (the per-peer HTTP
	// fetch wrappers), and callers wrap this error further (fetchSubtreeDataForBlock
	// adds a ServiceError, orderedDelivery a ProcessingError), so by the time it
	// reaches processCatchupChItem the ERR_EXTERNAL code sits mid-chain and
	// errors.Is(err, ErrServiceError) is also true. The handler therefore checks
	// ErrExternal before ErrServiceError — see processCatchupChItem.
	//
	// errors.NewExternalError extracts the trailing error param as the wrapped error,
	// so a "%v" placeholder for primaryErr would render as %!v(MISSING). The wrapped
	// error is preserved in the chain.
	//
	// The wrapped cause is the PRIMARY's error: it is the peer catchup selected and
	// the most relevant single reason. The full per-peer summary rides in the message
	// so no attempt is lost (issue 1368, Defect A — this function used to keep a single
	// error variable that each alternative overwrote, so the reported cause was
	// whichever alternative failed last, unrelated to the primary; primaryErr replaced
	// it precisely so the primary's error survives).
	return "", markCatchupFailureReported(errors.NewExternalError("[catchup:fetchAndStoreSubtreeAndSubtreeData] all %d peer attempts failed to fetch subtree %s [%s]", len(attempts), subtreeHash.String(), formatSubtreeFetchAttempts(attempts), primaryErr))
}

// fetchSubtreeFromPeer fetches subtree (for subtreeToCheck) from a peer via HTTP
func (u *Server) fetchSubtreeFromPeer(ctx context.Context, subtreeHash *chainhash.Hash, peerID string, baseURL string, bypassCache bool) ([]byte, error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "fetchSubtreeFromPeer",
		tracing.WithParentStat(u.stats),
	)
	defer deferFn()

	// Construct URL for subtree endpoint (for subtreeToCheck)
	url, err := u.peerResourceURL(baseURL, "subtree", subtreeHash, bypassCache)
	if err != nil {
		return nil, errors.NewServiceError("[catchup:fetchSubtreeFromPeer] invalid peer base URL for subtree %s", subtreeHash.String(), err)
	}

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
		return nil, markCacheBypassRetryable(errors.NewNotFoundError("[catchup:fetchSubtreeFromPeer] empty subtree received from %s", url))
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

// fetchSubtreeDataFromPeer fetches subtree data from a peer via HTTP
func (u *Server) fetchSubtreeDataFromPeer(ctx context.Context, subtreeHash *chainhash.Hash, peerID string, baseURL string, bypassCache bool) (*countingReadCloser, error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "fetchSubtreeDataFromPeer",
		tracing.WithParentStat(u.stats),
	)
	defer deferFn()

	// peerResourceURL builds <baseURL>/subtree_data/<hash>, appending the cachebust
	// query parameter when bypassCache is set.
	url, err := u.peerResourceURL(baseURL, "subtree_data", subtreeHash, bypassCache)
	if err != nil {
		return nil, errors.NewServiceError("[catchup:fetchSubtreeDataFromPeer] invalid peer base URL for subtree %s", subtreeHash.String(), err)
	}

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

	blocksURL, err := util.JoinPeerURL(baseURL, "blocks", hash.String())
	if err != nil {
		return nil, errors.NewProcessingError("[catchup:fetchBlocksBatch][%s] invalid peer base URL", hash.String(), err)
	}

	url := fmt.Sprintf("%s?n=%d", blocksURL, n)

	// Stream and parse incrementally rather than io.ReadAll-ing the whole response: a block
	// carries no consensus-defined maximum size, so there is no byte cap to apply to the HTTP
	// body as a whole (unlike fetchSubtreeFromPeer's DoHTTPRequestBounded). Streaming the parse
	// only halves peak memory versus io.ReadAll, though - it does not by itself bound anything,
	// since the subtree hash list length is an unvalidated wire varint (model/Block.go) that the
	// parse loop honours with no ceiling. Each block message is additionally capped below via
	// io.LimitedReader (bitcoin-sv/teranode#4742); see that comment for what the cap does not
	// cover.
	//
	// reqCtx exists so the over-send probe below can abort the body read on its own short
	// budget without spending the caller's fetch deadline.
	reqCtx, reqCancel := context.WithCancel(ctx)
	defer reqCancel()

	bodyReader, err := util.DoHTTPRequestBodyReader(reqCtx, url)
	if err != nil {
		return nil, errors.NewProcessingError("[catchup:fetchBlocksBatch][%s] failed to get blocks from peer", hash.String(), err)
	}

	countingReader := &countingReadCloser{
		reader: bodyReader,
		onClose: func(bytesRead uint64) {
			if u.p2pClient != nil && peerID != "" {
				trackCtx, _, deferFn := tracing.DecoupleTracingSpan(ctx, "blockvalidation", "recordBytesDownloaded")
				defer deferFn()

				if err := u.p2pClient.RecordBytesDownloaded(trackCtx, peerID, bytesRead); err != nil {
					u.logger.Warnf("[fetchBlocksBatch][%s] failed to record %d bytes downloaded from peer %s: %v", hash.String(), bytesRead, peerID, err)
				}
			}
		},
	}
	defer countingReader.Close()

	blocks := make([]*model.Block, 0, n)
	maxBlockMessageBytes := u.settings.BlockValidation.MaxIncomingBlockMessageBytes

	// Bounded by n, not read-until-EOF: a peer that keeps streaming well-formed blocks past
	// what was asked for would otherwise drive an allocation bounded only by how long it kept
	// sending, in *model.Block values that no byte cap could constrain.
	for uint32(len(blocks)) < n {
		// Cap what a single block message may consume off the wire. Without this, a peer could
		// still drive the subtree hash list (and thus the parsed *model.Block) unboundedly large
		// even though the response itself is streamed rather than io.ReadAll'd. Use a
		// LimitedReader (not a byte-counted DoHTTPRequestBounded-style cap on the whole
		// response) so cap-exhaustion is distinguishable per-block from a genuine short stream.
		//
		// The cap bounds bytes delivered, not bytes allocated. It does not bound the coinbase
		// read: go-bt's readArenaScript allocates a script's declared length (up to its
		// MaxArenaAlloc, 1 GiB) before reading any of it, so that allocation happens whatever
		// this cap is (bsv-blockchain/go-bt#187).
		limited := &io.LimitedReader{R: countingReader, N: maxBlockMessageBytes}

		block, err := model.NewBlockFromReader(limited)
		if err != nil {
			if limited.N == 0 {
				return nil, errors.NewExternalError("[catchup:fetchBlocksBatch][%s] block message from peer %s exceeds %d byte limit", hash.String(), peerID, maxBlockMessageBytes)
			}

			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}

			return nil, errors.NewProcessingError("[catchup:fetchBlocksBatch][%s] failed to create block from bytes", hash.String(), err)
		}

		blocks = append(blocks, block)
	}

	// Diagnostic only: the n blocks above stand regardless - they are still verified by the
	// caller's verifyBlockHeaders, and stopping at n (rather than reading to EOF) is deliberate
	// so a peer that pads correct data with extra bytes no longer aborts the whole catchup
	// cycle. This probe exists purely so the padding is not completely invisible: it records
	// the observation for the peer dashboard without charging reputation, since padding on top
	// of correct data is at least as likely to be a caching proxy or ?n= version skew as it is
	// to be malicious (see the ErrBlockPolicyDeclined exemption in peer_metrics_helpers.go for
	// the same reasoning about not punishing ambiguous peer behaviour).
	//
	// The probe reads a body the peer still controls, so it gets its own budget: a peer that
	// sends the n blocks and then never ends the response would otherwise hold this read until
	// the fetch deadline, and the batch would still return success.
	probeTimer := time.AfterFunc(overSendProbeTimeout, reqCancel)

	var probe [1]byte
	_, probeErr := io.ReadFull(countingReader, probe[:])

	probeTimer.Stop()

	if probeErr == nil {
		u.logger.Warnf("[catchup:fetchBlocksBatch][%s] peer %s streamed more than the %d blocks requested", hash.String(), peerID, n)
		u.reportCatchupError(ctx, peerID, fmt.Sprintf("peer over-sent on /blocks (requested %d)", n))
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

	blockURL, err := util.JoinPeerURL(baseURL, "block", hash.String())
	if err != nil {
		return nil, errors.NewProcessingError("[catchup:fetchSingleBlock][%s] invalid peer base URL", hash.String(), err)
	}

	// Stream and parse incrementally rather than io.ReadAll-ing the whole response - see the
	// comment on fetchBlocksBatch's DoHTTPRequestBodyReader call (bitcoin-sv/teranode#4742).
	bodyReader, err := util.DoHTTPRequestBodyReader(ctx, blockURL)
	if err != nil {
		return nil, errors.NewProcessingError("[catchup:fetchSingleBlock][%s] failed to get block from peer", hash.String(), err)
	}

	countingReader := &countingReadCloser{
		reader: bodyReader,
		onClose: func(bytesRead uint64) {
			if u.p2pClient != nil && peerID != "" {
				trackCtx, _, deferFn := tracing.DecoupleTracingSpan(ctx, "blockvalidation", "recordBytesDownloaded")
				defer deferFn()

				if err := u.p2pClient.RecordBytesDownloaded(trackCtx, peerID, bytesRead); err != nil {
					u.logger.Warnf("[fetchSingleBlock][%s] failed to record %d bytes downloaded from peer %s: %v", hash.String(), bytesRead, peerID, err)
				}
			}
		},
	}
	defer countingReader.Close()

	// Cap what the block message may consume off the wire - see fetchBlocksBatch's matching
	// io.LimitedReader comment, including what the cap does not bound (bitcoin-sv/teranode#4742,
	// bsv-blockchain/go-bt#187).
	maxBlockMessageBytes := u.settings.BlockValidation.MaxIncomingBlockMessageBytes
	limited := &io.LimitedReader{R: countingReader, N: maxBlockMessageBytes}

	block, err := model.NewBlockFromReader(limited)
	if err != nil {
		if limited.N == 0 {
			return nil, errors.NewExternalError("[catchup:fetchSingleBlock][%s] block message from peer %s exceeds %d byte limit", hash.String(), peerID, maxBlockMessageBytes)
		}

		return nil, errors.NewProcessingError("[catchup:fetchSingleBlock][%s] failed to create block from bytes", hash.String(), err)
	}

	if block == nil {
		return nil, errors.NewProcessingError("[catchup:fetchSingleBlock][%s] block could not be created from peer response", hash.String())
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
