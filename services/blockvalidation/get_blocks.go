// This file contains block fetching utilities for catchup operations.
package blockvalidation

import (
	"bufio"
	"context"
	stderrors "errors" //nolint:depguard // Inspect native transport causes without message-based classification.
	"fmt"
	"io"
	"math"
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
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/settings"
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
// deadline), never the peer's fault. Inspect the reservation delay so only a real
// queue wait receives the pacing marker; an already-ended caller is cancellation.
func (u *Server) awaitPeerFetchSlot(ctx context.Context, baseURL string) error {
	lim := u.peerFetchLimiter(baseURL)
	if lim == nil {
		return nil
	}

	if err := ctx.Err(); err != nil {
		return errors.NewContextCanceledError("[peerFetchLimiter] local pacing wait aborted", err)
	}
	now := time.Now()
	reservation := lim.ReserveN(now, 1)
	if !reservation.OK() {
		return errors.NewConfigurationError("peer fetch limiter cannot reserve a single token")
	}
	granted := false
	defer func() {
		if !granted {
			reservation.Cancel()
		}
	}()
	if err := ctx.Err(); err != nil {
		return errors.NewContextCanceledError("[peerFetchLimiter] local pacing wait aborted", err)
	}
	delay := reservation.DelayFrom(now)
	if delay == 0 {
		granted = true
		return nil
	}
	pacingExhausted := func() error {
		pacingErr := errors.NewContextCanceledError("[peerFetchLimiter] local pacing wait budget exhausted", context.DeadlineExceeded)
		pacingErr.SetData(peerFetchPacingKey, true)
		return pacingErr
	}
	if deadline, ok := ctx.Deadline(); ok && delay > deadline.Sub(now) {
		return pacingExhausted()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		granted = true
		return nil
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return pacingExhausted()
		}
		return errors.NewContextCanceledError("[peerFetchLimiter] local pacing wait aborted", ctx.Err())
	}
}

// A pacing queue belongs to one peer. It remains local for reputation, but
// another peer's independent queue can serve the same request.
const peerFetchPacingKey = "peer_fetch_pacing_exhausted"

func shouldStopPeerFailover(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return true
	}
	if !errors.IsLocalError(err) {
		return false
	}
	// Storage/configuration failures remain fatal even if an outer error carries
	// a pacing marker. Peer-controlled error messages cannot set native error data.
	if errors.Is(err, errors.ErrStorageError) || errors.Is(err, errors.ErrConfiguration) {
		return true
	}
	for depth := 0; err != nil && depth < 32; depth++ {
		var native *errors.Error
		if !errors.As(err, &native) {
			break
		}
		if paced, _ := native.GetData(peerFetchPacingKey).(bool); paced {
			return false
		}
		err = native.WrappedErr()
	}
	return true
}

// overSendProbeTimeout bounds fetchBlocksBatch's diagnostic read past the last requested block.
// It is diagnostic only, so it must never spend a meaningful share of the block fetch timeout.
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

	limits, err := resolveBlockResponseLimits(u.settings.BlockValidation.MaxIncomingBlockBytes, u.settings.Policy.ExcessiveBlockSize)
	if err != nil {
		return err
	}
	messageLimit := u.settings.BlockValidation.MaxIncomingBlockMessageBytes
	if messageLimit <= 0 {
		return errors.NewConfigurationError("blockvalidation_max_incoming_block_message_bytes must be positive")
	}
	// Every individually acceptable message must fit in the requested batch's
	// aggregate allowance. Otherwise honest peers all fail the same fixed batch.
	// When the aggregate cap is smaller, request one and let the decoder enforce it.
	batchSize = int(min(int64(batchSize), max(int64(1), limits.maxTransportBytes/messageLimit)))

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
		fetchCtx, fetchCancel := u.withCatchupFetchTimeout(ctx)
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
	subtreeConcurrency := 8 // fail-safe when the setting is unset or non-positive; the real default is 32 (settings.go)
	if u.settings.BlockValidation.SubtreeFetchConcurrency > 0 {
		subtreeConcurrency = u.settings.BlockValidation.SubtreeFetchConcurrency
	}

	subtreeConcurrency = u.boundSubtreeConcurrencyByBudget(subtreeConcurrency, block)

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
			nonPrunedPeers(peersAtOrAboveHeight(blockAltPeers, block.Height)),
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
			servingPeerID, err := u.fetchAndStoreSubtreeAndSubtreeData(ctx, parentCtx, block, &subtreeHashCopy, capturedPeerID, capturedBaseURL, peerSnapshot, freshness)
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
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.NewStorageError("[catchup:fetchAndStoreSubtree] error checking subtree existence for %s", subtreeHash.String(), err)
	}

	if localExists {
		u.logger.Debugf("[catchup:fetchAndStoreSubtree] Subtree already exists for %s, loading from store", subtreeHash.String())

		// Load existing subtree from store under whichever file type was found
		subtreeBytes, err := u.subtreeStore.Get(ctx, subtreeHash[:], localFileType)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
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
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
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
	// The decoder can wrap the HTTP reader's native cancellation in BlockInvalid
	// and External errors. A canceled caller must shed those peer-fault verdicts,
	// rather than returning the wrapper chain merely because it contains cancellation.
	// Consult the actual context; peer-supplied error text is not cancellation proof.
	if ctx.Err() == context.Canceled {
		return errors.NewContextCanceledError(canceledMsg, context.Canceled)
	}
	if errors.Is(err, errors.ErrContextCanceled) {
		// Preserve local pacing markers when the caller is still active or its
		// deadline expired while waiting for our own rate limiter.
		return err
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

	// Survive sibling errgroup cancellation, but keep shutdown/catchup cancellation.
	// One download deadline covers retries and the streaming parse. The store write
	// below switches back to shutdownCtx once the peer has delivered its data.
	// Reattach this function's span because shutdownCtx carries the parent span.
	spanCtx := ctx
	var dlCancel context.CancelFunc
	ctx, dlCancel = context.WithTimeout(shutdownCtx, subtreeDataFetchTimeout(u.settings))
	defer dlCancel()
	ctx = trace.ContextWithSpan(ctx, trace.SpanFromContext(spanCtx))

	// The per-attempt rate-limit pacing hook uses the attempt ctx (this dlCtx), which is
	// cancellable on shutdown — so both the pacing wait and the download abort promptly.
	subtreeDataReader, err := u.fetchSubtreeDataFromPeer(ctx, subtreeHash, peerID, baseURL,
		func(c context.Context) error { return u.awaitPeerFetchSlot(c, baseURL) }, bypassCache)
	if err != nil {
		// The caller (including RevalidateBlock's RPC) can have an earlier
		// deadline than the download. That expiry is local, not a peer stall.
		if parentErr := shutdownCtx.Err(); parentErr != nil {
			return errors.NewContextCanceledError("[catchup:fetchAndStoreSubtreeData] caller context ended for %s", subtreeHash.String(), parentErr)
		}
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
		if parentErr := shutdownCtx.Err(); parentErr != nil {
			return errors.NewContextCanceledError("[catchup:fetchAndStoreSubtreeData] caller context ended for %s", subtreeHash.String(), parentErr)
		}
		// Parser errors may quote peer-controlled text. Only the actual download
		// context establishes cancellation here; a quoted sentinel is not local failure.
		if c := classifyDownloadErr(ctx, subtreeHash, nil); c != nil {
			return c
		}
		return errors.NewProcessingError("[catchup:fetchAndStoreSubtreeData] Failed to create subtreeData for %s", subtreeHash.String())
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

	// The peer has finished serving data. Local persistence has its own lifecycle;
	// a slow store must not inherit the peer deadline or be blamed on that peer.
	dlCancel()
	storeCtx := trace.ContextWithSpan(shutdownCtx, trace.SpanFromContext(ctx))
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

	storeErr := u.subtreeStore.SetFromReader(storeCtx,
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

	if storeErr != nil && storeCtx.Err() != nil {
		return errors.NewContextCanceledError("[catchup:fetchAndStoreSubtreeData] local storage write aborted", storeCtx.Err())
	}

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
			peerID, util.RedactPeerURL(baseURL), subtreeHash.String())
	}

	return errors.NewStorageError("[catchup:fetchAndStoreSubtreeData] Failed to store subtreeData for %s", subtreeHash.String(), storeErr)
}

// fetchSubtreeAndDataFromPeer fetches the subtree and then its subtreeData from a
// single peer. With bypassCache set, both requests carry a cache-busting query
// parameter. shutdownCtx is threaded to fetchAndStoreSubtreeData so its detached
// download survives sibling-errgroup cancellation but still aborts on node shutdown.
func (u *Server) fetchSubtreeAndDataFromPeer(ctx context.Context, shutdownCtx context.Context, block *model.Block, subtreeHash *chainhash.Hash,
	peerID, baseURL string, bypassCache bool, freshness *subtreeFreshness) error {
	subtree, err := u.fetchAndStoreSubtree(ctx, block, subtreeHash, peerID, baseURL, bypassCache, freshness)
	if err != nil {
		return err
	}

	return u.fetchAndStoreSubtreeData(ctx, shutdownCtx, block, subtreeHash, subtree, peerID, baseURL, bypassCache, freshness)
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
func (u *Server) tryPeerForSubtree(ctx context.Context, shutdownCtx context.Context, block *model.Block, subtreeHash *chainhash.Hash,
	peerID, baseURL string, freshness *subtreeFreshness) error {
	err := u.fetchSubtreeAndDataFromPeer(ctx, shutdownCtx, block, subtreeHash, peerID, baseURL, false, freshness)
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

	bypassErr := u.fetchSubtreeAndDataFromPeer(ctx, shutdownCtx, block, subtreeHash, peerID, baseURL, true, freshness)
	if bypassErr != nil {
		u.recordCatchupPeerFailure(peerID, bypassErr)
	}

	return bypassErr
}

// fetchAndStoreSubtreeAndSubtreeData fetches both subtree and subtreeData for a single subtree hash
// and stores them in the subtreeStore. If the primary peer fails, it tries the block's alternative
// peers (bounded, pruned-aware, via selectAlternativePeers over the block snapshot) before giving
// up. Per-peer failures are attributed via recordCatchupPeerFailure inside tryPeerForSubtree and
// drained at catchup release. Returns the peer ID that actually served the data and any error.
func (u *Server) fetchAndStoreSubtreeAndSubtreeData(ctx context.Context, shutdownCtx context.Context, block *model.Block, subtreeHash *chainhash.Hash,
	peerID, baseURL string, peerSnapshot *catchupPeerSnapshot, freshness *subtreeFreshness) (string, error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "fetchAndStoreSubtreeAndSubtreeData",
		tracing.WithParentStat(u.stats),
	)
	defer deferFn()

	// Try the primary peer first.
	err := u.tryPeerForSubtree(ctx, shutdownCtx, block, subtreeHash, peerID, baseURL, freshness)
	if err == nil {
		return peerID, nil
	}

	// A local error means our own storage or context failed — another peer cannot fix
	// that, so do not spend attempts on the rest of the fleet.
	if shouldStopPeerFailover(ctx, err) {
		return "", errors.NewServiceError("[catchup:fetchAndStoreSubtreeAndSubtreeData] Local error fetching subtree %s (not retrying with other peers)", subtreeHash.String(), err)
	}

	u.logger.Warnf("[catchup:fetchAndStoreSubtreeAndSubtreeData] Primary peer %s failed for subtree %s: %v, trying alternatives", peerID, subtreeHash.String(), err)

	primaryErr := err

	attempts := make([]subtreeFetchAttempt, 0, 4)
	attempts = append(attempts, subtreeFetchAttempt{peerID: peerID, baseURL: baseURL, role: "primary", err: err})

	// Alternatives come from the block-level snapshot (successful lookups cached) bounded by
	// selectAlternativePeers — not a fresh per-subtree GetPeersAtMaxHeight gRPC.
	var alternativePeers []*p2p.PeerInfo
	if peerSnapshot != nil {
		peers, _, snapshotErr := peerSnapshot.get()
		if snapshotErr != nil {
			// The actual primary failure is already retained in failedPeers and
			// charged by releaseCatchupLock even when discovery is locally down.
			// Preserve its diagnostic without changing the local terminal cause.
			return "", errors.NewServiceUnavailableError("local peer discovery unavailable during subtree failover after [%s]", formatSubtreeFetchAttempts(attempts), snapshotErr)
		}
		alternativePeers = selectAlternativePeers(peersAtOrAboveHeight(peers, block.Height), peerID, baseURL, maxSubtreeFailoverPeers)
	}

	if len(alternativePeers) > 0 {
		u.logger.Infof("[catchup:fetchAndStoreSubtreeAndSubtreeData] Trying %d alternative peers for subtree %s", len(alternativePeers), subtreeHash.String())

		for _, altPeer := range alternativePeers {
			altPeerID := altPeer.ID.String()
			altBaseURL := altPeer.DataHubURL
			if altBaseURL == "" {
				continue
			}

			altErr := u.tryPeerForSubtree(ctx, shutdownCtx, block, subtreeHash, altPeerID, altBaseURL, freshness)
			if altErr == nil {
				u.logger.Infof("[catchup:fetchAndStoreSubtreeAndSubtreeData] Successfully fetched subtree %s from alternative peer %s", subtreeHash.String(), altPeerID)
				return altPeerID, nil
			}

			u.logger.Warnf("[catchup:fetchAndStoreSubtreeAndSubtreeData] Alternative peer %s failed for subtree %s: %v", altPeerID, subtreeHash.String(), altErr)

			attempts = append(attempts, subtreeFetchAttempt{peerID: altPeerID, baseURL: altBaseURL, role: "alternative", err: altErr})

			if shouldStopPeerFailover(ctx, altErr) {
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
	parentCtx := ctx
	ctx, cancel := u.withCatchupSubtreeFetchTimeout(ctx)
	defer cancel()

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

	// WithRetry backs off on 429/503 (peer rate limiting / admission control) rather
	// than failing the whole fetch. The beforeAttempt hook paces EVERY attempt through
	// the per-peer limiter (not just the first issuance), so retries can't re-burst.
	subtreeBytes, err := util.DoHTTPRequestBoundedWithRetry(ctx, url, maxSubtreeBytes,
		func(c context.Context) error { return u.awaitPeerFetchSlot(c, baseURL) })
	if err != nil {
		// A caller deadline (for example RevalidateBlock's RPC budget) also ends
		// the fetch, but must not charge the peer for exhausting our local budget.
		if parentErr := parentCtx.Err(); parentErr != nil {
			return nil, errors.NewContextCanceledError("[catchup:fetchSubtreeFromPeer] caller context ended for %s", subtreeHash.String(), parentErr)
		}
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
	url, err := u.peerResourceURL(baseURL, "subtree_data", subtreeHash, bypassCache)
	if err != nil {
		return nil, errors.NewServiceError("[catchup:fetchSubtreeDataFromPeer] invalid peer base URL for subtree %s", subtreeHash.String(), err)
	}

	u.logger.Debugf("[catchup:fetchSubtreeDataFromPeer] fetching subtree data from %s", url)

	// Retry on 503/429 — peer's asset service may reject under admission control while
	// it generates the file on-demand from Aerospike, or rate-limit the heavy route.
	// The retry loop backs off (honoring Retry-After when present). beforeAttempt paces
	// each attempt through the per-peer limiter on a cancellable context (this ctx may
	// derive from the catchup parent context to survive sibling cancellation).
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
				trackCtx, cancel := context.WithTimeout(context.WithoutCancel(trackCtx), catchupReputationReportTimeout)
				defer cancel()
				if err := u.p2pClient.RecordBytesDownloaded(trackCtx, peerID, bytesRead); err != nil {
					u.logger.Warnf("[fetchSubtreeDataFromPeer][%s] failed to record %d bytes downloaded from peer %s: %v", subtreeHash.String(), bytesRead, peerID, err)
				}
			}
		},
	}

	return countingReader, nil
}

const blockBatchCapacityHint = 100

// Keep read-ahead small so invalid block declarations are rejected before pulling
// unnecessary payload from a peer. The outer response limiter bounds all fills;
// decodeBoundedBlock exposes remaining bytes, including buffered bytes, to the model.
const blockStreamReadBufferMinSize = 16

type blockResponseLimits struct {
	maxTransportBytes int64
	maxMessageBytes   int64
	maxDeclaredBytes  uint64
	enforceDeclared   bool
}

// withCatchupFetchTimeout gives a block response one deadline covering pacing,
// all HTTP retries and the body stream. Header iterations have a separate budget.
func (u *Server) withCatchupFetchTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := u.settings.BlockValidation.BlockFetchTimeout
	if timeout <= 0 {
		timeout = settings.DefaultBlockFetchTimeout
	}
	return context.WithTimeout(ctx, timeout)
}

// withCatchupSubtreeFetchTimeout bounds one /subtree hash-list response, including
// pacing, retries and the full body read. Subtree data has its own larger budget.
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
// budget SHARED across a whole response. The caller owns the aggregate LimitedReader;
// the decoder additionally applies maxMessageBytes to each block. The coinbase scanner
// sees the smaller remaining allowance and rejects impossible allocations before reading.
func decodeBoundedBlock(r io.Reader, limited *io.LimitedReader, limits blockResponseLimits) (*model.Block, error) {
	// bufio may already hold bytes charged to the response limiter. Expose the
	// remaining *consumable* allowance to the model, so its coinbase scanner can
	// reject impossible lengths before reading or allocating their payloads.
	remaining := limited.N
	if buffered, ok := r.(*bufio.Reader); ok {
		remaining += int64(buffered.Buffered())
	}
	if limits.maxMessageBytes > 0 {
		remaining = min(remaining, limits.maxMessageBytes)
	}
	blockReader := &io.LimitedReader{R: r, N: remaining}
	coinbaseBudget := remaining
	declaredLimit := uint64(math.MaxUint64)
	if limits.enforceDeclared {
		declaredLimit = limits.maxDeclaredBytes
		if declaredLimit <= math.MaxInt64 {
			coinbaseBudget = min(coinbaseBudget, int64(declaredLimit))
		}
	}
	block, err := model.NewBlockFromReaderWithDeclaredSizeLimit(blockReader, declaredLimit, coinbaseBudget)
	if err != nil {
		if errors.Is(err, errors.ErrBlockPolicyDeclined) {
			return nil, err
		}
		// HTTP body readers sanitize transport failures before the model wraps them
		// in BlockInvalid. Preserve their native classification without that wrapper:
		// an interrupted transfer is not evidence of an invalid block. Inspect codes
		// structurally; IsNetworkError also matches untrusted message substrings.
		for cause := err; cause != nil; cause = stderrors.Unwrap(cause) {
			if native, ok := cause.(*errors.Error); ok {
				switch native.Code() {
				case errors.ERR_NETWORK_ERROR:
					return nil, errors.NewNetworkError("peer block response transport failure")
				case errors.ERR_NETWORK_TIMEOUT:
					return nil, errors.NewNetworkTimeoutError("peer block response timed out")
				case errors.ERR_NETWORK_CONNECTION_REFUSED:
					return nil, errors.NewNetworkConnectionRefusedError("peer block response connection refused")
				}
			}
		}
		if errors.Is(err, errors.ErrThresholdExceeded) {
			// Receive policy is ours, not a consensus verdict. Scanning can reject
			// an advertised length before exhausting the reader, so checking N or
			// EOF alone misses this case. Keep diagnostics, drop BlockInvalid codes.
			return nil, errors.NewExternalError("peer block response exceeds receive byte limits: %s", err.Error())
		}
		if blockReader.N == 0 && limits.maxMessageBytes > 0 && remaining == limits.maxMessageBytes {
			return nil, errors.NewExternalError("peer block message exceeds %d byte limit", limits.maxMessageBytes)
		}
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
		// so catchup fails over. The model's structural cause (such as a go-bt limit) is
		// preserved — a structurally invalid complete block is genuinely the peer serving bad data.
		return nil, errors.NewExternalError("peer block response failed to decode", err)
	}

	if block == nil {
		return nil, errors.NewBlockInvalidError("peer block response decoded to nil block")
	}
	if limits.enforceDeclared && block.SizeInBytes > limits.maxDeclaredBytes {
		return nil, errors.NewBlockPolicyDeclinedError("peer block declared size %d exceeds excessiveblocksize %d", block.SizeInBytes, limits.maxDeclaredBytes)
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
			trackCtx, cancel := context.WithTimeout(context.WithoutCancel(trackCtx), catchupReputationReportTimeout)
			defer cancel()
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
	limits.maxMessageBytes = u.settings.BlockValidation.MaxIncomingBlockMessageBytes
	if limits.maxMessageBytes <= 0 {
		return nil, errors.NewConfigurationError("blockvalidation_max_incoming_block_message_bytes must be positive")
	}

	blocksURL, err := util.JoinPeerURL(baseURL, "blocks", hash.String())
	if err != nil {
		return nil, errors.NewProcessingError("[catchup:fetchBlocksBatch][%s] invalid peer base URL", hash.String(), err)
	}

	// One response deadline covers pacing, retries and the streamed batch.
	ctx, cancel := u.withCatchupFetchTimeout(ctx)
	defer cancel()

	// WithRetry backs off on 429/503 instead of failing the whole batch; the hook paces
	// every attempt through the per-peer limiter so retries don't re-burst.
	reqCtx, reqCancel := context.WithCancel(ctx)
	defer reqCancel()
	responseBody, err := util.DoHTTPRequestBodyReaderWithRetryFunc(reqCtx, fmt.Sprintf("%s?n=%d", blocksURL, n),
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
	_, probeErr := io.ReadFull(blockReader, probe[:])

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
	limits, err := resolveBlockResponseLimits(
		u.settings.BlockValidation.MaxIncomingBlockBytes,
		u.settings.Policy.ExcessiveBlockSize,
	)
	if err != nil {
		return nil, err
	}
	limits.maxMessageBytes = u.settings.BlockValidation.MaxIncomingBlockMessageBytes
	if limits.maxMessageBytes <= 0 {
		return nil, errors.NewConfigurationError("blockvalidation_max_incoming_block_message_bytes must be positive")
	}

	blockURL, err := util.JoinPeerURL(baseURL, "block", hash.String())
	if err != nil {
		return nil, errors.NewProcessingError("[catchup:fetchSingleBlock][%s] invalid peer base URL", hash.String(), err)
	}

	// Bound the whole fetch (retries + streaming read) by BlockFetchTimeout; see
	// withCatchupFetchTimeout and the note in fetchBlocksBatch.
	ctx, cancel := u.withCatchupFetchTimeout(ctx)
	defer cancel()

	// WithRetry backs off on 429/503 (peer rate limiting) instead of failing; the hook
	// paces every attempt through the per-peer limiter so retries don't re-burst.
	responseBody, err := util.DoHTTPRequestBodyReaderWithRetryFunc(ctx, blockURL,
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
	// /block returns exactly one serialized block: there is no count parameter
	// or legacy batch-size negotiation to explain additional records. Keep its
	// strict framing contract. The /blocks oversend exception above is scoped
	// to batch interoperability; it does not make arbitrary proxy padding valid
	// on the single-object endpoint.
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
