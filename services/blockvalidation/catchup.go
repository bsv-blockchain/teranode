package blockvalidation

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockvalidation/catchup"
	"github.com/bitcoin-sv/teranode/stores/blockchain/options"
	"github.com/bitcoin-sv/teranode/util/blockassemblyutil"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"golang.org/x/sync/errgroup"
)

const (
	// maxBlockHeadersPerRequest is the maximum number of headers to request in a single batch
	maxBlockHeadersPerRequest = 10_000

	// maxCatchupIterations was the old iteration limit, kept for reference but no longer used
	// since we now make a single request for headers
	maxCatchupIterations = 1000
)

// CatchupContext holds all the state needed during a catchup operation
type CatchupContext struct {
	blockUpTo               *model.Block
	baseURL                 string
	peerID                  string
	startTime               time.Time
	commonAncestorHash      *chainhash.Hash
	commonAncestorMeta      *model.BlockHeaderMeta
	commonAncestorIndex     int // Index of common ancestor in peer headers
	forkDepth               uint32
	currentHeight           uint32
	blockHeaders            []*model.BlockHeader
	headersFetchResult      *catchup.Result
	useQuickValidation      bool   // Whether to use quick validation for checkpointed blocks
	highestCheckpointHeight uint32 // Highest checkpoint height for validation checks
}

// catchup orchestrates the complete blockchain synchronization process.
// It follows a clear sequence of steps to safely synchronize with a peer:
//
// 1. Acquire catchup lock (prevent concurrent catchups)
// 2. Fetch headers from peer
// 3. Find and validate common ancestor
// 4. Check coinbase maturity constraints
// 5. Detect secret mining attempts
// 6. Filter headers to process
// 7. Build header chain cache
// 8. Verify chain continuity
// 9. Fetch and validate blocks
// 10. Clean up resources
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - blockUpTo: Target block to sync up to
//   - baseURL: URL of the peer to sync from
//
// Returns:
//   - error: If any step fails or safety checks are violated
func (u *Server) catchup(ctx context.Context, blockUpTo *model.Block, baseURL string, peerID string) (err error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "catchup",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[catchup][%s] starting catchup to %s", blockUpTo.Hash().String(), baseURL),
	)
	defer deferFn()

	// Validate that we have a baseURL for making HTTP requests
	if baseURL == "" {
		return errors.NewInvalidArgumentError("baseURL is required for catchup")
	}

	// Use baseURL as fallback if peerID is not provided (for backward compatibility)
	if peerID == "" {
		peerID = baseURL
	}

	catchupCtx := &CatchupContext{
		blockUpTo: blockUpTo,
		baseURL:   baseURL,
		peerID:    peerID,
		startTime: time.Now(),
	}

	// Step 1: Acquire exclusive catchup lock
	if err = u.acquireCatchupLock(catchupCtx); err != nil {
		return err
	}
	defer u.releaseCatchupLock(catchupCtx, &err)

	// Step 2: Fetch block headers from peer
	if err = u.fetchHeaders(ctx, catchupCtx); err != nil {
		return err
	}

	// Early exit if no headers to process
	if len(catchupCtx.headersFetchResult.Headers) == 0 {
		u.logger.Infof("[catchup][%s] block already exists or no headers needed", blockUpTo.Hash().String())
		return nil
	}

	// Step 3: Find common ancestor between chains
	if err = u.findCommonAncestor(ctx, catchupCtx); err != nil {
		return err
	}

	// Step 4: Validate fork depth against coinbase maturity
	if err = u.validateForkDepth(catchupCtx); err != nil {
		return err
	}

	// Step 5: Check for secret mining attempts
	if err = u.checkSecretMining(ctx, catchupCtx); err != nil {
		return err
	}

	// Step 6: Filter headers to only those we need to catchup
	if err = u.filterHeaders(ctx, catchupCtx); err != nil {
		return err
	}

	// Early exit if no new blocks to process
	if len(catchupCtx.blockHeaders) == 0 {
		u.logger.Infof("[catchup][%s] no new blocks to fetch - already synced", blockUpTo.Hash().String())
		return nil
	}

	// Step 7: Build header chain cache for validation
	if err = u.buildHeaderCache(catchupCtx); err != nil {
		return err
	}

	// Step 8: Verify chain continuity
	if err = u.verifyChainContinuity(ctx, catchupCtx); err != nil {
		return err
	}

	// Step 9: Verify checkpoints and determine if quick validation can be used
	// This step ensures we're on the correct chain by validating checkpoint hashes
	if err = u.verifyCheckpointsInHeaderChain(catchupCtx); err != nil {
		u.logger.Errorf("[catchup][%s] Checkpoint verification failed: %v", blockUpTo.Hash().String(), err)
		return err
	}

	// Step 10: Fetch and validate blocks
	if err = u.fetchAndValidateBlocks(ctx, catchupCtx); err != nil {
		return err
	}

	// Step 11: Clean up resources
	u.cleanup(catchupCtx)

	return nil
}

// acquireCatchupLock ensures only one catchup runs at a time.
// Sets the catchup flag and initializes metrics.
//
// Parameters:
//   - ctx: Catchup context containing operation state
//
// Returns:
//   - error: If another catchup is already in progress
func (u *Server) acquireCatchupLock(ctx *CatchupContext) error {
	if !u.isCatchingUp.CompareAndSwap(false, true) {
		return errors.NewError("[catchup][%s] another catchup is currently in progress", ctx.blockUpTo.Hash().String())
	}

	// Initialize metrics (check for nil in tests)
	if prometheusCatchupActive != nil {
		prometheusCatchupActive.Set(1)
	}
	u.catchupAttempts.Add(1)

	return nil
}

// releaseCatchupLock releases the catchup lock and records metrics.
// Updates health check tracking and records success/failure metrics.
//
// Parameters:
//   - ctx: Catchup context containing operation state
//   - err: Pointer to error from catchup operation
func (u *Server) releaseCatchupLock(ctx *CatchupContext, err *error) {
	u.isCatchingUp.Store(false)
	if prometheusCatchupActive != nil {
		prometheusCatchupActive.Set(0)
	}

	// Update catchup tracking for health checks
	u.catchupStatsMu.Lock()
	u.lastCatchupTime = time.Now()
	u.lastCatchupResult = *err == nil
	u.catchupStatsMu.Unlock()

	// Record catchup duration metric
	success := "true"
	if *err != nil {
		success = "false"
	} else {
		u.catchupSuccesses.Add(1)
	}

	if prometheusCatchupDuration != nil {
		prometheusCatchupDuration.WithLabelValues(ctx.baseURL, success).Observe(time.Since(ctx.startTime).Seconds())
	}
}

// fetchHeaders retrieves block headers from the peer using block locator pattern.
// Uses the headers_from_common_ancestor endpoint for efficient fetching.
// IMPORTANT: Headers should extend to at least a checkpoint when possible to ensure
// we can verify we're on the correct chain.
//
// Parameters:
//   - ctx: Context for cancellation
//   - catchupCtx: Catchup context to store results
//
// Returns:
//   - error: If fetching headers fails
func (u *Server) fetchHeaders(ctx context.Context, catchupCtx *CatchupContext) error {
	u.logger.Debugf("[catchup][%s] Step 1: Fetching headers from peer %s", catchupCtx.blockUpTo.Hash().String(), catchupCtx.baseURL)

	result, _, err := u.catchupGetBlockHeaders(ctx, catchupCtx.blockUpTo, catchupCtx.baseURL, catchupCtx.peerID)
	if err != nil {
		return errors.NewProcessingError("[catchup][%s] failed to get block headers: %w", catchupCtx.blockUpTo.Hash().String(), err)
	}

	catchupCtx.headersFetchResult = result

	u.logger.Infof("[catchup][%s] Fetched %d headers from peer", catchupCtx.blockUpTo.Hash().String(), len(result.Headers))

	return nil
}

// findCommonAncestor locates the newest block that exists in both chains.
// Uses block locator pattern for O(log n) search efficiency.
//
// Parameters:
//   - ctx: Context for cancellation
//   - catchupCtx: Catchup context to store ancestor information
//
// Returns:
//   - error: If common ancestor cannot be found
func (u *Server) findCommonAncestor(ctx context.Context, catchupCtx *CatchupContext) error {
	u.logger.Debugf("[catchup][%s] Step 2: Finding common ancestor", catchupCtx.blockUpTo.Hash().String())

	// Headers from headers_from_common_ancestor endpoint are already in oldest-to-newest order
	peerHeaders := catchupCtx.headersFetchResult.Headers
	if len(peerHeaders) == 0 {
		return errors.NewProcessingError("[catchup][%s] no headers received from peer", catchupCtx.blockUpTo.Hash().String())
	}

	currentHeight := u.utxoStore.GetBlockHeight()
	catchupCtx.currentHeight = currentHeight

	// Walk through peer's headers (oldest to newest) to find the highest common ancestor
	commonAncestorIndex := -1
	u.logger.Debugf("[catchup][%s] Checking %d peer headers for common ancestor", catchupCtx.blockUpTo.Hash().String(), len(peerHeaders))

	for i, header := range peerHeaders {
		exists, err := u.blockchainClient.GetBlockExists(ctx, header.Hash())
		if err != nil {
			return errors.NewProcessingError("[catchup][%s] failed to check if block %s exists: %v", catchupCtx.blockUpTo.Hash().String(), header.Hash().String(), err)
		}

		if exists {
			commonAncestorIndex = i // Keep updating to find the LAST match
			u.logger.Debugf("[catchup][%s] Block %s exists in our chain (index %d)", catchupCtx.blockUpTo.Hash().String(), header.Hash().String(), i)
		} else {
			u.logger.Debugf("[catchup][%s] Block %s not in our chain - stopping search", catchupCtx.blockUpTo.Hash().String(), header.Hash().String())
			break // Once we find a header we don't have, stop
		}
	}

	if commonAncestorIndex == -1 {
		return errors.NewProcessingError("[catchup][%s] no common ancestor found in peer headers", catchupCtx.blockUpTo.Hash().String())
	}

	// Get the common ancestor header and its metadata
	commonAncestorHeader := peerHeaders[commonAncestorIndex]
	commonAncestorHash := commonAncestorHeader.Hash()

	// Get metadata for the common ancestor
	_, commonAncestorMeta, err := u.blockchainClient.GetBlockHeader(ctx, commonAncestorHash)
	if err != nil {
		return errors.NewProcessingError("[catchup][%s] failed to get metadata for common ancestor %s: %v", catchupCtx.blockUpTo.Hash().String(), commonAncestorHash.String(), err)
	}

	if commonAncestorMeta.Invalid {
		return errors.NewProcessingError("[catchup][%s] common ancestor %s at height %d is marked invalid, not catching up", catchupCtx.blockUpTo.Hash().String(), commonAncestorHash.String(), commonAncestorMeta.Height)
	}

	// Calculate fork depth
	forkDepth := uint32(0)
	if commonAncestorMeta.Height < currentHeight {
		forkDepth = currentHeight - commonAncestorMeta.Height
	}

	catchupCtx.commonAncestorHash = commonAncestorHash
	catchupCtx.commonAncestorMeta = commonAncestorMeta
	catchupCtx.commonAncestorIndex = commonAncestorIndex
	catchupCtx.forkDepth = forkDepth

	u.logger.Infof("[catchup][%s] Found common ancestor: %s at height %d (index %d), fork depth: %d", catchupCtx.blockUpTo.Hash().String(), commonAncestorHash.String(), commonAncestorMeta.Height, commonAncestorIndex, forkDepth)

	return nil
}

// validateForkDepth ensures the fork doesn't exceed coinbase maturity limits.
// Prevents deep reorganizations that could invalidate spent coinbase outputs.
//
// Parameters:
//   - catchupCtx: Catchup context with fork depth information
//
// Returns:
//   - error: If fork depth exceeds coinbase maturity
func (u *Server) validateForkDepth(catchupCtx *CatchupContext) error {
	u.logger.Debugf("[catchup][%s] Step 3: Validating fork depth against coinbase maturity", catchupCtx.blockUpTo.Hash().String())

	if catchupCtx.forkDepth > uint32(u.settings.ChainCfgParams.CoinbaseMaturity) {
		u.logger.Errorf("[catchup][%s] fork depth (%d blocks) exceeds coinbase maturity (%d blocks)", catchupCtx.blockUpTo.Hash().String(), catchupCtx.forkDepth, u.settings.ChainCfgParams.CoinbaseMaturity)

		// Record malicious attempt
		u.recordMaliciousAttempt(catchupCtx.peerID, "coinbase_maturity_violation")

		// Record error metric
		if prometheusCatchupErrors != nil {
			prometheusCatchupErrors.WithLabelValues(catchupCtx.peerID, "coinbase_maturity_violation").Inc()
		}

		return errors.NewServiceError("[catchup][%s] fork depth (%d) exceeds coinbase maturity (%d)", catchupCtx.blockUpTo.Hash().String(), catchupCtx.forkDepth, u.settings.ChainCfgParams.CoinbaseMaturity)
	}

	u.logger.Infof("[catchup][%s] Fork depth %d is within coinbase maturity limit of %d", catchupCtx.blockUpTo.Hash().String(), catchupCtx.forkDepth, u.settings.ChainCfgParams.CoinbaseMaturity)

	return nil
}

// checkSecretMining detects if the peer withheld blocks (secret mining attack).
// Delegates to checkSecretMiningFromCommonAncestor for the actual check.
//
// Parameters:
//   - ctx: Context for cancellation
//   - catchupCtx: Catchup context with ancestor information
//
// Returns:
//   - error: If secret mining is detected
func (u *Server) checkSecretMining(ctx context.Context, catchupCtx *CatchupContext) error {
	u.logger.Debugf("[catchup][%s] Step 4: Checking for secret mining", catchupCtx.blockUpTo.Hash().String())

	return u.checkSecretMiningFromCommonAncestor(ctx, catchupCtx.blockUpTo, catchupCtx.baseURL, catchupCtx.peerID, catchupCtx.commonAncestorHash, catchupCtx.commonAncestorMeta)
}

// filterHeaders filters headers to only those after the common ancestor that we don't have.
// Removes headers we already have in our blockchain to avoid redundant processing.
//
// Parameters:
//   - ctx: Context for cancellation
//   - catchupCtx: Catchup context with headers to filter
//
// Returns:
//   - error: If filtering fails
func (u *Server) filterHeaders(ctx context.Context, catchupCtx *CatchupContext) error {
	u.logger.Debugf("[catchup][%s] Step 5: Filtering headers to process", catchupCtx.blockUpTo.Hash().String())

	peerHeaders := catchupCtx.headersFetchResult.Headers
	if len(peerHeaders) == 0 {
		return errors.NewProcessingError("[catchup][%s] no headers to filter", catchupCtx.blockUpTo.Hash().String())
	}

	// Since we found the true common ancestor, all headers after that index should be new to us
	// No need for complex filtering - just take everything after the common ancestor
	if catchupCtx.commonAncestorIndex < 0 || catchupCtx.commonAncestorIndex >= len(peerHeaders) {
		return errors.NewProcessingError("[catchup][%s] invalid common ancestor index: %d", catchupCtx.blockUpTo.Hash().String(), catchupCtx.commonAncestorIndex)
	}

	// Extract headers after common ancestor (commonAncestorIndex+1 onwards)
	headersToProcess := make([]*model.BlockHeader, 0)
	if catchupCtx.commonAncestorIndex+1 < len(peerHeaders) {
		headersToProcess = peerHeaders[catchupCtx.commonAncestorIndex+1:]
	}

	catchupCtx.blockHeaders = headersToProcess

	u.logger.Infof("[catchup][%s] Taking %d headers after common ancestor (index %d)", catchupCtx.blockUpTo.Hash().String(), len(headersToProcess), catchupCtx.commonAncestorIndex)

	return nil
}

// buildHeaderCache builds the header chain cache for efficient validation.
// Pre-computes validation headers to reduce database queries during block validation.
//
// Parameters:
//   - catchupCtx: Catchup context with headers to cache
//
// Returns:
//   - error: If cache building fails
func (u *Server) buildHeaderCache(catchupCtx *CatchupContext) error {
	u.logger.Debugf("[catchup][%s] Step 6: Building header chain cache", catchupCtx.blockUpTo.Hash().String())

	// Verify first header connects to common ancestor
	if len(catchupCtx.blockHeaders) > 0 {
		firstHeaderParent := catchupCtx.blockHeaders[0].HashPrevBlock
		if !firstHeaderParent.IsEqual(catchupCtx.commonAncestorHash) {
			u.logger.Warnf("[catchup][%s] first header parent %s doesn't match common ancestor %s", catchupCtx.blockUpTo.Hash().String(), firstHeaderParent.String(), catchupCtx.commonAncestorHash.String())
		}
	}

	// Build the cache
	if err := u.headerChainCache.BuildFromHeaders(catchupCtx.blockHeaders, u.settings.BlockValidation.PreviousBlockHeaderCount); err != nil {
		return errors.NewProcessingError("[catchup][%s] failed to build header chain cache: %v", catchupCtx.blockUpTo.Hash().String(), err)
	}

	u.logger.Infof("[catchup][%s] Built header chain cache with %d headers", catchupCtx.blockUpTo.Hash().String(), len(catchupCtx.blockHeaders))

	// Record metric
	if len(catchupCtx.blockHeaders) > 0 && prometheusCatchupHeadersFetched != nil {
		prometheusCatchupHeadersFetched.WithLabelValues(catchupCtx.baseURL).Add(float64(len(catchupCtx.blockHeaders)))
	}

	return nil
}

// verifyCheckpointsInHeaderChain verifies that any checkpoints within our header range match.
// This ensures we're on the correct chain before proceeding with catchup.
//
// Parameters:
//   - catchupCtx: Catchup context with block information
//
// Returns:
//   - error: If checkpoint verification fails
func (u *Server) verifyCheckpointsInHeaderChain(catchupCtx *CatchupContext) error {
	// Get checkpoints from settings
	if u.settings.ChainCfgParams == nil || len(u.settings.ChainCfgParams.Checkpoints) == 0 {
		u.logger.Debugf("[catchup][%s] No checkpoints configured", catchupCtx.blockUpTo.Hash().String())
		return nil // No checkpoints to verify
	}

	// Cannot verify checkpoints if there's a fork
	if catchupCtx.forkDepth > 0 {
		u.logger.Debugf("[catchup][%s] Fork detected (depth %d), checkpoint verification not applicable", catchupCtx.blockUpTo.Hash().String(), catchupCtx.forkDepth)
		return nil // Fork handling takes precedence
	}

	// Get the highest checkpoint height for reference
	highestCheckpointHeight := getHighestCheckpointHeight(u.settings.ChainCfgParams.Checkpoints)
	catchupCtx.highestCheckpointHeight = highestCheckpointHeight

	// Calculate the height range - much simpler now since headers are sequential with no gaps
	if len(catchupCtx.blockHeaders) == 0 {
		u.logger.Debugf("[catchup][%s] No headers to verify", catchupCtx.blockUpTo.Hash().String())
		return nil
	}

	firstBlockHeight := catchupCtx.commonAncestorMeta.Height + 1
	lastBlockHeight := catchupCtx.commonAncestorMeta.Height + uint32(len(catchupCtx.blockHeaders))

	u.logger.Debugf("[catchup][%s] Verifying checkpoints in height range %d-%d (common ancestor at %d)", catchupCtx.blockUpTo.Hash().String(), firstBlockHeight, lastBlockHeight, catchupCtx.commonAncestorMeta.Height)

	// Verify checkpoints within our header range
	checkpointsChecked := 0
	for _, checkpoint := range u.settings.ChainCfgParams.Checkpoints {
		checkpointHeight := uint32(checkpoint.Height)

		// Skip checkpoints at or below the common ancestor height
		if checkpointHeight <= catchupCtx.commonAncestorMeta.Height {
			u.logger.Debugf("[catchup][%s] Skipping checkpoint at height %d (at/below common ancestor height %d)", catchupCtx.blockUpTo.Hash().String(), checkpointHeight, catchupCtx.commonAncestorMeta.Height)
			continue
		}

		// Check if checkpoint is within our header range
		if checkpointHeight >= firstBlockHeight && checkpointHeight <= lastBlockHeight {
			// Calculate the index in blockHeaders (simple sequential calculation)
			headerIndex := checkpointHeight - firstBlockHeight
			if int(headerIndex) >= len(catchupCtx.blockHeaders) {
				return errors.NewProcessingError("[catchup][%s] internal error: checkpoint height %d maps to invalid header index %d", catchupCtx.blockUpTo.Hash().String(), checkpointHeight, headerIndex)
			}

			headerHash := catchupCtx.blockHeaders[headerIndex].Hash()
			if !headerHash.IsEqual(checkpoint.Hash) {
				// CRITICAL: Checkpoint hash mismatch - we're on the wrong chain!
				return errors.NewProcessingError("[catchup][%s] CHECKPOINT VERIFICATION FAILED: checkpoint at height %d requires hash %s but got %s - stopping catchup", catchupCtx.blockUpTo.Hash().String(), checkpointHeight, checkpoint.Hash.String(), headerHash.String())
			}

			u.logger.Infof("[catchup][%s] Verified checkpoint at height %d with hash %s", catchupCtx.blockUpTo.Hash().String(), checkpointHeight, checkpoint.Hash.String())
			checkpointsChecked++
		}
	}

	if checkpointsChecked > 0 {
		u.logger.Infof("[catchup][%s] Successfully verified %d checkpoint(s) in header chain", catchupCtx.blockUpTo.Hash().String(), checkpointsChecked)
		catchupCtx.useQuickValidation = true
	} else {
		catchupCtx.useQuickValidation = false
	}

	return nil
}

// verifyChainContinuity ensures the first block properly connects to our chain.
// Validates that the parent of the first new block exists in our blockchain.
//
// Parameters:
//   - ctx: Context for cancellation
//   - catchupCtx: Catchup context with headers to verify
//
// Returns:
//   - error: If chain continuity is broken
func (u *Server) verifyChainContinuity(ctx context.Context, catchupCtx *CatchupContext) error {
	u.logger.Debugf("[catchup][%s] Step 7: Verifying chain continuity", catchupCtx.blockUpTo.Hash().String())

	if len(catchupCtx.blockHeaders) == 0 {
		return nil
	}

	firstBlock := catchupCtx.blockHeaders[0]

	// Verify parent exists (should be common ancestor)
	parentExists, err := u.blockValidation.GetBlockExists(ctx, firstBlock.HashPrevBlock)
	if err != nil {
		return errors.NewProcessingError("[catchup][%s] failed to check if parent block exists: %v", catchupCtx.blockUpTo.Hash().String(), err)
	}

	if !parentExists {
		return errors.NewProcessingError("[catchup][%s] parent block %s not found - cannot establish chain connection", catchupCtx.blockUpTo.Hash().String(), firstBlock.HashPrevBlock.String())
	}

	u.logger.Infof("[catchup][%s] Chain continuity verified, parent %s exists", catchupCtx.blockUpTo.Hash().String(), firstBlock.HashPrevBlock.String())

	return nil
}

// fetchAndValidateBlocks fetches full blocks from peer and validates them.
// Coordinates concurrent fetching and sequential validation for optimal performance.
//
// Parameters:
//   - ctx: Context for cancellation
//   - catchupCtx: Catchup context with headers of blocks to fetch
//
// Returns:
//   - error: If fetching or validation fails
func (u *Server) fetchAndValidateBlocks(ctx context.Context, catchupCtx *CatchupContext) error {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "fetchAndValidateBlocks",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[catchup:fetchAndValidateBlocks][%s] starting to fetch and validate %d blocks", catchupCtx.blockUpTo.Hash().String(), len(catchupCtx.blockHeaders)),
	)
	defer deferFn()

	// Set up channels and counters
	var size atomic.Int64
	size.Store(int64(len(catchupCtx.blockHeaders)))
	validateBlocksChan := make(chan *model.Block, size.Load())

	bestBlockHeader, _, err := u.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return errors.NewProcessingError("failed to get best block header", err)
	}

	// Check if we need to change FSM state
	newBlocksOnOurChain := len(catchupCtx.blockHeaders) > 0 && catchupCtx.blockHeaders[0].HashPrevBlock.IsEqual(bestBlockHeader.Hash())

	// Set FSM state if needed
	if newBlocksOnOurChain {
		if err := u.setFSMCatchingBlocks(ctx, catchupCtx, &size); err != nil {
			return err
		}

		defer u.restoreFSMState(ctx, catchupCtx)
	}

	// Create error group for concurrent operations
	errorGroup, gCtx := errgroup.WithContext(ctx)

	// Start fetching blocks
	errorGroup.Go(func() error {
		return u.fetchBlocksConcurrently(gCtx, catchupCtx, validateBlocksChan, &size)
	})

	// Start validation in parallel
	errorGroup.Go(func() error {
		return u.validateBlocksOnChannel(validateBlocksChan, gCtx, catchupCtx, &size)
	})

	// Wait for both operations to complete
	return errorGroup.Wait()
}

// cleanup cleans up resources after catchup.
// Clears the header chain cache to free memory.
//
// Parameters:
//   - catchupCtx: Catchup context for logging
func (u *Server) cleanup(catchupCtx *CatchupContext) {
	u.logger.Debugf("[catchup][%s] Step 9: Cleaning up resources", catchupCtx.blockUpTo.Hash().String())

	// Clear the header chain cache
	u.headerChainCache.Clear()

	u.logger.Infof("[catchup][%s] Catchup completed successfully", catchupCtx.blockUpTo.Hash().String())
}

// ============================================================================
// Helper functions for catchup process
// ============================================================================

// extractHeadersAfterAncestor returns headers that come after the common ancestor.
// Filters out headers up to and including the common ancestor.
//
// Parameters:
//   - headers: All headers in oldest-to-newest order
//   - ancestorHash: Hash of the common ancestor
//
// Returns:
//   - []*model.BlockHeader: Headers after the common ancestor
func (u *Server) extractHeadersAfterAncestor(headers []*model.BlockHeader, ancestorHash *chainhash.Hash) []*model.BlockHeader {
	// Headers are in oldest-to-newest order after reversal
	for i, header := range headers {
		if header.Hash().IsEqual(ancestorHash) {
			// Found ancestor, return everything after it
			if i+1 < len(headers) {
				return headers[i+1:]
			}
			return nil
		} else if i == 0 && header.HashPrevBlock.IsEqual(ancestorHash) {
			// Common ancestor is parent of first header
			return headers
		}
	}

	// Default: assume first header's parent is ancestor
	if len(headers) > 0 && headers[0].HashPrevBlock.IsEqual(ancestorHash) {
		return headers
	}

	// Fallback: return all headers with warning
	if len(headers) > 0 {
		u.logger.Warnf("Could not determine relationship to common ancestor, using all headers")
		return headers
	}

	return nil
}

// filterExistingBlocks removes blocks that already exist in our database.
// Checks each header against the blockchain to avoid redundant processing.
//
// Parameters:
//   - ctx: Context for cancellation
//   - headers: Headers to check
//   - blockUpTo: Target block for logging
//
// Returns:
//   - []*model.BlockHeader: Headers that don't exist in our database
//   - error: If database check fails
func (u *Server) filterExistingBlocks(ctx context.Context, headers []*model.BlockHeader, blockUpTo *model.Block) ([]*model.BlockHeader, error) {
	var newHeaders []*model.BlockHeader

	for _, header := range headers {
		exists, err := u.blockValidation.GetBlockExists(ctx, header.Hash())
		if err != nil {
			u.logger.Warnf("[catchup][%s] failed to check if block %s exists: %v", blockUpTo.Hash().String(), header.Hash().String(), err)
			// Include it to be safe
			newHeaders = append(newHeaders, header)
		} else if !exists {
			newHeaders = append(newHeaders, header)
		} else {
			u.logger.Debugf("[catchup][%s] skipping block %s - already exists", blockUpTo.Hash().String(), header.Hash().String())
		}
	}

	return newHeaders, nil
}

// recordMaliciousAttempt records a malicious attempt from a peer.
// Updates peer metrics and logs security warnings.
//
// Parameters:
//   - peerID: P2P peer identifier of the malicious peer
//   - reason: Description of the malicious behavior
func (u *Server) recordMaliciousAttempt(peerID string, reason string) {
	if u.peerMetrics != nil && peerID != "" {
		peerMetric := u.peerMetrics.GetOrCreatePeerMetrics(peerID)
		peerMetric.RecordMaliciousAttempt()
		u.logger.Warnf("Recorded malicious attempt from peer %s: %s", peerID, reason)
	}

	if peerID != "" {
		u.logger.Errorf("SECURITY: Peer %s attempted %s - should be banned (banning not yet implemented)", peerID, reason)
	}
}

// setFSMCatchingBlocks sets the FSM state to CATCHINGBLOCKS.
// Notifies the blockchain service that we're syncing new blocks.
//
// Parameters:
//   - ctx: Context for cancellation
//   - catchupCtx: Catchup context for logging
//   - size: Number of blocks to catch up
//
// Returns:
//   - error: If FSM state change fails
func (u *Server) setFSMCatchingBlocks(ctx context.Context, catchupCtx *CatchupContext, size *atomic.Int64) error {
	u.logger.Infof("[catchup][%s] Setting node to CATCHINGBLOCKS state for %d blocks", catchupCtx.blockUpTo.Hash().String(), size.Load())

	if err := u.blockchainClient.CatchUpBlocks(ctx); err != nil {
		return errors.NewProcessingError("[catchup][%s] failed to send CATCHUPBLOCKS event: %w", catchupCtx.blockUpTo.Hash().String(), err)
	}

	return nil
}

// restoreFSMState restores the FSM state after catchup.
// Returns the node to RUN state if it was in CATCHINGBLOCKS state.
//
// Parameters:
//   - ctx: Context for cancellation
//   - catchupCtx: Catchup context for logging
func (u *Server) restoreFSMState(ctx context.Context, catchupCtx *CatchupContext) {
	state, err := u.blockchainClient.GetFSMCurrentState(ctx)
	if err != nil {
		u.logger.Errorf("[catchup] failed to get FSM current state: %v", err)
		return
	}

	if state != nil && *state == blockchain.FSMStateCATCHINGBLOCKS {
		u.logger.Infof("[catchup][%s] Restoring FSM to RUN state", catchupCtx.blockUpTo.Hash().String())

		if err = u.blockchainClient.Run(ctx, "blockvalidation/Server"); err != nil {
			u.logger.Errorf("[catchup][%s] failed to send RUN event: %v", catchupCtx.blockUpTo.Hash().String(), err)
		}
	}
}

// validateBlocksOnChannel processes and validates blocks received from the channel.
// Validates blocks sequentially to maintain chain order.
//
// Parameters:
//   - validateBlocksChan: Channel providing blocks to validate
//   - gCtx: Context for cancellation
//   - catchupCtx: Catchup context with validation mode information
//   - size: Atomic counter for remaining blocks
//
// Returns:
//   - error: If validation fails or context is cancelled
func (u *Server) validateBlocksOnChannel(validateBlocksChan chan *model.Block, gCtx context.Context, catchupCtx *CatchupContext, size *atomic.Int64) error {
	i := 0
	blockUpTo := catchupCtx.blockUpTo
	baseURL := catchupCtx.baseURL
	peerID := catchupCtx.peerID

	// validate the blocks while getting them from the other node
	// this will block until all blocks are validated
	for block := range validateBlocksChan {
		// Check context cancellation before processing each block
		select {
		case <-gCtx.Done():
			u.logger.Infof("[catchup:validateBlocksOnChannel][%s] context cancelled during block validation", blockUpTo.Hash().String())
			return gCtx.Err()
		default:
			i++
			// Use debug logging during catchup, with progress logs every 100 blocks
			if i%100 == 0 || i == 1 {
				u.logger.Infof("[catchup:validateBlocksOnChannel][%s] validating block %s %d/%d", blockUpTo.Hash().String(), block.Hash().String(), i, size.Load())
			} else {
				u.logger.Debugf("[catchup:validateBlocksOnChannel][%s] validating block %s %d/%d", blockUpTo.Hash().String(), block.Hash().String(), i, size.Load())
			}

			// Wait for block assembly to be ready if needed
			if err := blockassemblyutil.WaitForBlockAssemblyReady(gCtx, u.logger, u.blockAssemblyClient, block.Height, uint32(u.settings.ChainCfgParams.CoinbaseMaturity/2)); err != nil {
				return errors.NewProcessingError("[catchup:validateBlocksOnChannel][%s] failed to wait for block assembly for block %s: %v", blockUpTo.Hash().String(), block.Hash().String(), err)
			}

			// Get cached headers for validation
			cachedHeaders, _ := u.headerChainCache.GetValidationHeaders(block.Hash())

			// Try quick validation if applicable
			tryNormalValidation, err := u.tryQuickValidation(gCtx, block, catchupCtx, baseURL)
			if err != nil {
				return err
			}

			if tryNormalValidation {
				// Standard validation path for blocks not verified by checkpoints
				// Create validation options with cached headers
				opts := &ValidateBlockOptions{
					CachedHeaders:           cachedHeaders,
					IsCatchupMode:           true,
					DisableOptimisticMining: true,
				}

				// Validate the block using standard validation
				if err := u.blockValidation.ValidateBlockWithOptions(gCtx, block, peerID, nil, opts); err != nil {
					u.logger.Errorf("[catchup:validateBlocksOnChannel][%s] failed to validate block %s at position %d: %v", blockUpTo.Hash().String(), block.Hash().String(), i, err)

					// Only mark block as invalid for consensus violations (ErrBlockInvalid or ErrTxInvalid)
					// Other errors like missing data, storage issues, or processing errors should not mark the block as invalid
					if errors.Is(err, errors.ErrBlockInvalid) || errors.Is(err, errors.ErrTxInvalid) {
						u.logger.Warnf("[catchup:validateBlocksOnChannel][%s] block %s violates consensus rules, marking as invalid", blockUpTo.Hash().String(), block.Hash().String())
						if markErr := u.blockchainClient.AddBlock(gCtx, block, peerID, options.WithInvalid(true)); markErr != nil {
							u.logger.Errorf("[catchup:validateBlocksOnChannel][%s] failed to store invalid block %s: %v", blockUpTo.Hash().String(), block.Hash().String(), markErr)
						}
					}
					// For recoverable errors (storage, processing, missing data), don't mark as invalid - just fail and retry later

					// Record metric for validation failure
					if prometheusCatchupErrors != nil {
						prometheusCatchupErrors.WithLabelValues(peerID, "validation_failure").Inc()
					}

					return err
				}
			}

			// Update the remaining block count
			remaining := size.Add(-1)
			if remaining%100 == 0 && remaining > 0 {
				u.logger.Infof("[catchup:validateBlocksOnChannel][%s] %d blocks remaining", blockUpTo.Hash().String(), remaining)
			}
		}
	}

	u.logger.Infof("[catchup:validateBlocksOnChannel][%s] completed validation of %d blocks", blockUpTo.Hash().String(), i)

	return nil
}

// tryQuickValidation attempts quick validation for checkpointed blocks
// Returns true if normal validation should be tried, false if quick validation succeeded
func (u *Server) tryQuickValidation(ctx context.Context, block *model.Block, catchupCtx *CatchupContext, baseURL string) (bool, error) {
	// Determine if this specific block can use quick validation
	// A block can use quick validation if it's at or below the highest verified checkpoint height
	canUseQuickValidation := catchupCtx.useQuickValidation && block.Height <= catchupCtx.highestCheckpointHeight

	// If block is not eligible for quick validation, use normal validation
	if !canUseQuickValidation {
		return true, nil
	}

	// Quick validation: create UTXOs for the block and validate transactions in parallel
	if err := u.blockValidation.quickValidateBlock(ctx, block, baseURL); err != nil {
		if prometheusCatchupErrors != nil {
			prometheusCatchupErrors.WithLabelValues(baseURL, "validation_failure").Inc()
		}

		u.logger.Warnf("[catchup:validateBlocksOnChannel][%s] quick validation failed for block %s, removing .subtree files: %v",
			catchupCtx.blockUpTo.Hash().String(), block.Hash().String(), err)

		// since the quick validation failed, we will have to remove the .subtree files, which will trigger
		// the normal validation to re-create the UTXOs and validate the transactions
		for _, subtreeHash := range block.Subtrees {
			if err = u.subtreeStore.Del(ctx, subtreeHash[:], fileformat.FileTypeSubtree); err != nil {
				if !errors.Is(err, errors.ErrNotFound) {
					return false, errors.NewProcessingError("[catchup:validateBlocksOnChannel][%s] failed to remove subtree file %s",
						catchupCtx.blockUpTo.Hash().String(), subtreeHash.String(), err)
				}
			}
		}
		// Quick validation failed, try normal validation
		return true, nil
	}

	// Quick validation succeeded, skip normal validation
	return false, nil
}

// getHighestCheckpointHeight returns the height of the highest checkpoint
func getHighestCheckpointHeight(checkpoints []chaincfg.Checkpoint) uint32 {
	if len(checkpoints) == 0 {
		return 0
	}

	var highestHeight uint32
	for _, checkpoint := range checkpoints {
		if uint32(checkpoint.Height) > highestHeight {
			highestHeight = uint32(checkpoint.Height)
		}
	}
	return highestHeight
}

// getLowestCheckpointHeight returns the height of the lowest checkpoint
func getLowestCheckpointHeight(checkpoints []chaincfg.Checkpoint) uint32 {
	if len(checkpoints) == 0 {
		return 0
	}

	lowestHeight := uint32(checkpoints[0].Height)
	for _, checkpoint := range checkpoints[1:] {
		if uint32(checkpoint.Height) < lowestHeight {
			lowestHeight = uint32(checkpoint.Height)
		}
	}
	return lowestHeight
}

// checkSecretMiningFromCommonAncestor detects if a peer withheld blocks (secret mining).
// Checks if common ancestor is too far behind, indicating potential attack.
//
// Parameters:
//   - ctx: Context for cancellation
//   - blockUpTo: Target block being synced to
//   - baseURL: Peer URL for metrics
//   - commonAncestorHash: Hash of the common ancestor
//   - commonAncestorMeta: Metadata of the common ancestor
//
// Returns:
//   - error: If secret mining is detected
func (u *Server) checkSecretMiningFromCommonAncestor(ctx context.Context, blockUpTo *model.Block, baseURL string, peerID string, commonAncestorHash *chainhash.Hash, commonAncestorMeta *model.BlockHeaderMeta) error {
	// Check whether the common ancestor is more than X blocks behind our current chain.
	// This indicates potential secret mining.
	currentHeight := u.utxoStore.GetBlockHeight()
	blocksBehind := currentHeight - commonAncestorMeta.Height

	// If we're not far enough in the chain, or the ancestor is not too far behind, it's not secret mining
	if currentHeight <= u.settings.BlockValidation.SecretMiningThreshold || blocksBehind <= u.settings.BlockValidation.SecretMiningThreshold {
		return nil
	}

	// The chain is potentially a secretly mined chain
	u.logger.Errorf("[catchup][%s] is potentially a secretly mined chain from common ancestor %s at height %d, ignoring", blockUpTo.Hash().String(), commonAncestorHash.String(), commonAncestorMeta.Height)

	// Record error metric for secret mining
	if prometheusCatchupErrors != nil && peerID != "" {
		prometheusCatchupErrors.WithLabelValues(peerID, "secret_mining").Inc()
	}

	// Log metrics for secret mining detection
	u.logger.Warnf("[catchup:secret_mining] Detected potential secret mining from peer %s: ancestor height %d, current height %d, difference %d > threshold %d | metrics: detected=1 height_difference=%d threshold=%d",
		baseURL, commonAncestorMeta.Height, currentHeight, currentHeight-commonAncestorMeta.Height, u.settings.BlockValidation.SecretMiningThreshold,
		currentHeight-commonAncestorMeta.Height, u.settings.BlockValidation.SecretMiningThreshold)

	// Record the malicious attempt for this peer
	if u.peerMetrics != nil && peerID != "" {
		peerMetric := u.peerMetrics.GetOrCreatePeerMetrics(peerID)
		peerMetric.RecordMaliciousAttempt()
		u.logger.Warnf("[catchup][%s] recorded malicious attempt from peer %s for secret mining", blockUpTo.Hash().String(), baseURL)
	}

	// Log ban request - actual banning should be handled by the P2P service
	u.logger.Errorf("[catchup][%s] SECURITY: Peer %s attempted secret mining - should be banned (banning not yet implemented)", blockUpTo.Hash().String(), baseURL)

	return errors.NewServiceError("[catchup][%s] is potentially a secretly mined chain from common ancestor at height %d, ignoring", blockUpTo.Hash().String(), commonAncestorMeta.Height)
}

// validateBatchHeaders validates a batch of block headers.
//
// Parameters:
//   - ctx: Context for cancellation
//   - headers: Headers to validate
//
// Returns:
//   - error: If any header fails validation
//
// Validates each header for:
// - Proof of work
// - Merkle root sanity
// - Timestamp bounds
// - Checkpoint conflicts (if height is known)
//
// Processes headers individually with context cancellation checks.
func (u *Server) validateBatchHeaders(ctx context.Context, headers []*model.BlockHeader) error {
	if len(headers) == 0 {
		return nil
	}

	// Note: Checkpoint validation is handled separately in verifyCheckpointsInHeaderChain()
	// This function focuses on basic header validation (PoW, merkle root, timestamp)

	for i, header := range headers {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Validate proof of work
		if err := catchup.ValidateHeaderProofOfWork(header); err != nil {
			u.logger.Errorf("[catchup:validateBatchHeaders] header %d/%d fails PoW validation: %v",
				i+1, len(headers), err)
			return err
		}

		// Validate merkle root
		if err := catchup.ValidateHeaderMerkleRoot(header); err != nil {
			u.logger.Errorf("[catchup:validateBatchHeaders] header %d/%d has invalid merkle root: %v",
				i+1, len(headers), err)
			return err
		}

		// Validate timestamp
		if err := catchup.ValidateHeaderTimestamp(header); err != nil {
			u.logger.Errorf("[catchup:validateBatchHeaders] header %d/%d has invalid timestamp: %v",
				i+1, len(headers), err)
			return err
		}

		// Checkpoint validation is handled separately in verifyCheckpointsInHeaderChain()
	}

	u.logger.Debugf("[catchup:validateBatchHeaders] validated %d headers successfully", len(headers))
	return nil
}

// newHashFromStr converts the passed big-endian hex string into a
// chainhash.Hash.  It only differs from the one available in chainhash in that
// it panics on an error since it will only (and must only) be called with
// hard-coded, and therefore known good, hashes.
func newHashFromStr(hexStr string) *chainhash.Hash {
	hash, err := chainhash.NewHashFromStr(hexStr)
	if err != nil {
		panic(err)
	}

	return hash
}
