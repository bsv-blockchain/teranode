// This file contains header fetching utilities for catchup operations.
package blockvalidation

import (
	"context"
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/catchup"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// catchupGetBlockHeaders fetches block headers from a peer for catchup synchronization.
// This function iteratively requests headers using the headers_from_common_ancestor endpoint,
// which returns headers from the common ancestor onwards in ascending order, up to a specified limit.
//
// The function continues requesting headers until it reaches the target block or receives
// fewer headers than the maximum, indicating it has reached the chain tip. Each iteration
// uses the last received header as the new block locator for the next request.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - blockUpTo: Target block to sync up to
//   - baseURL: URL of the peer to fetch headers from
//
// Returns:
//   - *CatchupResult: Result containing headers and metrics
//   - *model.BlockHeader: Best block header from our chain
//   - error: If fetching or parsing headers fails
func (u *Server) catchupGetBlockHeaders(ctx context.Context, blockUpTo *model.Block, peerID, baseURL string) (*catchup.Result, *model.BlockHeader, error) {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "catchupGetBlockHeaders",
		tracing.WithParentStat(u.stats),
		tracing.WithStartTime(), // startTime is read back from ctx below
		tracing.WithLogMessage(u.logger, "[catchup][%s] fetching headers up to %s from peer %s", blockUpTo.Hash().String(), baseURL, peerID),
		tracing.WithContextTimeout(time.Duration(u.settings.BlockValidation.CatchupOperationTimeout)*time.Second),
	)
	defer deferFn()

	// Get start time from context, or use current time if not present (for tests)
	var startTime time.Time
	if st := ctx.Value(tracing.StartTime); st != nil {
		startTime = st.(time.Time)
	} else {
		startTime = time.Now()
	}
	failedIterations := make([]catchup.IterationError, 0, 10) // Preallocate for up to 10 failed iterations

	// Validate that we have a baseURL for making HTTP requests
	if baseURL == "" {
		return catchup.CreateCatchupResult(nil, blockUpTo.Hash(), nil, 0, startTime, baseURL, 0, failedIterations, false, "No baseURL provided"), nil, errors.NewInvalidArgumentError("baseURL is required for fetching headers")
	}

	// Use baseURL as fallback if peerID is not provided (for backward compatibility)
	identifier := peerID
	if identifier == "" {
		identifier = baseURL
	}

	// Check if we're using circuit breaker
	var circuitBreaker *catchup.CircuitBreaker
	if u.peerCircuitBreakers != nil {
		circuitBreaker = u.peerCircuitBreakers.GetBreaker(identifier)
		if !circuitBreaker.CanCall() {
			return catchup.CreateCatchupResult(nil, blockUpTo.Hash(), nil, 0, startTime, baseURL, 0, failedIterations, false, "Circuit breaker open for peer"), nil, errors.NewServiceUnavailableError("circuit breaker open for peer %s", identifier)
		}
	}

	// Check peer reputation via P2P service. A peer the P2P service has already
	// flagged as malicious must not be used for catchup at all — abort before
	// fetching or processing any headers from it.
	if u.isPeerMalicious(ctx, identifier) {
		u.logger.Warnf("[catchup][%s] aborting catchup: peer %s is marked as malicious by P2P service", blockUpTo.Hash().String(), identifier)

		if circuitBreaker != nil {
			circuitBreaker.RecordFailure()
		}

		return catchup.CreateCatchupResult(nil, blockUpTo.Hash(), nil, 0, startTime, baseURL, 0, failedIterations, false, "Peer marked malicious by P2P service"), nil, errors.NewNetworkPeerMaliciousError("peer %s is marked as malicious by P2P service", identifier)
	}

	// Check if target block already exists
	exists, err := u.blockValidation.GetBlockExists(ctx, blockUpTo.Hash())
	if err != nil {
		if circuitBreaker != nil {
			circuitBreaker.RecordFailure()
		}

		return catchup.CreateCatchupResult(nil, blockUpTo.Hash(), nil, 0, startTime, baseURL, 0, failedIterations, false, "Failed to check block existence"), nil, errors.NewServiceError("[catchup][%s] failed to check if block exists", blockUpTo.Hash().String(), err)
	}

	// If the block already exists, we can return immediately
	if exists {
		if circuitBreaker != nil {
			circuitBreaker.RecordSuccess()
		}

		return catchup.CreateCatchupResult(nil, blockUpTo.Hash(), nil, 0, startTime, baseURL, 0, failedIterations, true, "Block already exists"), nil, nil
	}

	// Get our current best block
	bestBlockHeader, bestBlockMeta, err := u.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		if circuitBreaker != nil {
			circuitBreaker.RecordFailure()
		}
		return catchup.CreateCatchupResult(nil, blockUpTo.Hash(), nil, 0, startTime, baseURL, 0, failedIterations, false, "Failed to get best block header"), nil, errors.NewServiceError("[catchup][%s] failed to get best block header", blockUpTo.Hash().String(), err)
	}

	startHash := bestBlockHeader.Hash()
	startHeight := bestBlockMeta.Height

	// locatorHeader/locatorHeight are used for building the block locator.
	// They default to the chain tip but may be capped to UTXO height below.
	locatorHeader := bestBlockHeader
	locatorHeight := bestBlockMeta.Height

	// Cap block locator at UTXO height when the blockchain store is ahead.
	//
	// The blockchain store (PostgreSQL) is updated synchronously by AddBlock during
	// catchup, but the UTXO store height is updated asynchronously — it receives a
	// NotificationType_Block notification on a channel, then makes a GetBestHeightAndTime
	// RPC call back to the blockchain service. During a long catchup or after a failed
	// catchup that validated some blocks before erroring out, the blockchain store can
	// be many blocks ahead of the UTXO store.
	//
	// The cap existed because findCommonAncestor used to reject any block above that same
	// UTXO height, so a locator built from the chain tip yielded headers it rejected all of
	// — "no common ancestor found". That is no longer the case: the ancestor ceiling is now
	// the blockchain tip, and a locator built from the tip would resolve normally.
	//
	// What the cap does now is start the peer's header stream lower than necessary, so the
	// ancestor walk climbs back through headers we already hold. That costs one GetBlockHeader
	// lookup per block of lag, where the old ceiling stopped the walk after roughly two (any
	// header above the UTXO height was rejected outright). The outcome is unchanged only while
	// the lag stays below CatchupMaxAccumulatedHeaders: the fetch truncates there (see the
	// maxAccumulatedHeaders check below), so a larger lag ends the served stream *below* our
	// tip, the walk takes its ancestor at that point, and the depth is overstated by
	// (lag - CatchupMaxAccumulatedHeaders) — the same over-measurement this ceiling change
	// exists to remove, reached by a different route. That needs a lag of 100k blocks by
	// default, so the cap is kept for now rather than removed alongside the ceiling fix, but
	// it is a correctness trade and not the free conservatism it looks like. Note it also sets
	// startHash/startHeight, which are reported on the catchup Result and not used in any
	// decision.
	if u.utxoStore != nil {
		utxoHeight := u.utxoStore.GetBlockHeight()
		if bestBlockMeta.Height > utxoHeight {
			u.logger.Infof("[catchup][%s] blockchain height %d ahead of UTXO height %d, capping locator",
				blockUpTo.Hash().String(), bestBlockMeta.Height, utxoHeight)

			// Use GetBlockByHeight which walks the best chain (by chain_work) via a
			// recursive CTE, guaranteeing we get the main-chain block. GetBlockHeadersFromHeight
			// returns all forks at a height and would silently break capping during fork scenarios.
			utxoBlock, capErr := u.blockchainClient.GetBlockByHeight(ctx, utxoHeight)
			if capErr != nil {
				u.logger.Warnf("[catchup][%s] failed to get block at UTXO height %d, using blockchain height: %v",
					blockUpTo.Hash().String(), utxoHeight, capErr)
			} else {
				locatorHeader = utxoBlock.Header
				locatorHeight = utxoHeight
				startHash = utxoBlock.Header.Hash()
				startHeight = utxoHeight
			}
		}
	}

	// Create block locator
	locatorHashes, err := u.blockchainClient.GetBlockLocator(ctx, locatorHeader.Hash(), locatorHeight)
	if err != nil {
		if circuitBreaker != nil {
			circuitBreaker.RecordFailure()
		}

		return catchup.CreateCatchupResult(nil, blockUpTo.Hash(), startHash, startHeight, startTime, baseURL, 0, failedIterations, false, "Failed to get block locator"), nil, errors.NewServiceError("[catchup][%s] failed to get block locator", blockUpTo.Hash().String(), err)
	}

	maxRetries := u.settings.BlockValidation.CatchupMaxRetries
	if maxRetries <= 0 {
		maxRetries = 3
	}

	chainTipHash := u.catchupTargetHash(ctx, blockUpTo, peerID)

	// Collect all headers through iteration
	allCatchupHeaders := make([]*model.BlockHeader, 0, maxBlockHeadersPerRequest)
	currentLocatorHashes := locatorHashes

	// iteration variables
	iteration := 0
	maxAccumulatedHeaders := u.settings.BlockValidation.CatchupMaxAccumulatedHeaders
	totalHeadersFetched := 0
	reachedTarget := false
	stopReason := ""

	// Iterate until we reach the target or chain tip
	for iteration < maxCatchupIterations {
		iteration++

		if peerID == "" {
			u.logger.Warnf("[catchup][%s] No peerID provided for peer at %s", blockUpTo.Hash().String(), baseURL)
			return catchup.CreateCatchupResult(nil, blockUpTo.Hash(), nil, 0, startTime, baseURL, 0, failedIterations, false, "No peerID provided"), nil, errors.NewProcessingError("[catchup][%s] peerID is required but not provided for peer %s", blockUpTo.Hash().String(), baseURL)
		}
		// Check if peer is marked as malicious by P2P service. The peer can be
		// flagged mid-catchup (e.g. based on behaviour reported during earlier
		// iterations), so abort as soon as we see the flag rather than fetching
		// the next header batch.
		if u.isPeerMalicious(ctx, identifier) {
			u.logger.Warnf("[catchup][%s] aborting catchup: peer %s is marked as malicious by P2P service", chainTipHash.String(), identifier)

			if circuitBreaker != nil {
				circuitBreaker.RecordFailure()
			}

			return catchup.CreateCatchupResult(allCatchupHeaders, blockUpTo.Hash(), startHash, startHeight, startTime, baseURL, iteration, failedIterations, false, "Peer marked malicious by P2P service"), nil, errors.NewNetworkPeerMaliciousError("peer %s is marked as malicious by P2P service", identifier)
		}

		// Create context with iteration timeout to prevent slow-loris attacks
		iterationTimeout := time.Duration(u.settings.BlockValidation.CatchupIterationTimeout) * time.Second
		if iterationTimeout <= 0 {
			iterationTimeout = 30 * time.Second // Default timeout
		}
		iterCtx, iterCancel := context.WithTimeout(ctx, iterationTimeout)

		// Build request URL with current block locator
		blockLocatorStr := catchup.BuildBlockLocatorString(currentLocatorHashes)
		requestURL := fmt.Sprintf("%s/headers_from_common_ancestor/%s?block_locator_hashes=%s&n=%d",
			baseURL,
			chainTipHash.String(),
			blockLocatorStr,
			maxBlockHeadersPerRequest,
		)

		u.logger.Debugf("[catchup][%s] iteration %d: requesting headers with locator starting at %s (timeout: %v)", chainTipHash.String(), iteration, currentLocatorHashes[0].String(), iterationTimeout)

		// Fetch with retry using iteration context with timeout
		blockHeadersBytes, err := catchup.FetchHeadersWithRetry(iterCtx, u.logger, requestURL, maxRetries)
		iterCancel() // Clean up the iteration context
		if err != nil {
			// Check if it's specifically a context deadline exceeded from the iteration timeout
			// This indicates the peer is too slow to respond within our timeout
			if errors.Is(err, context.DeadlineExceeded) {
				// The iteration timeout expired - peer is too slow
				elapsed := time.Since(startTime)
				u.logger.Warnf("[catchup][%s] iteration %d: peer %s timed out after %v", chainTipHash.String(), iteration, baseURL, elapsed)

				// Record failure in circuit breaker
				if circuitBreaker != nil {
					circuitBreaker.RecordFailure()
				}

				// Report slow response as catchup failure to P2P service
				u.reportCatchupFailure(ctx, identifier)

				iterErr := catchup.IterationError{
					Iteration:  iteration,
					Error:      err,
					Timestamp:  time.Now(),
					PeerURL:    baseURL,
					RetryCount: 0,
					Duration:   elapsed,
				}
				failedIterations = append(failedIterations, iterErr)

				// Return a timeout error - this is just a slow peer, not necessarily malicious.
				// Marked as already-reported so the top-level catchup handler does not
				// record a second failure for the same attempt.
				return catchup.CreateCatchupResult(
					allCatchupHeaders, blockUpTo.Hash(), startHash, startHeight, startTime, baseURL,
					iteration, failedIterations, false, "Peer response timeout",
				), nil, markCatchupFailureReported(errors.NewNetworkTimeoutError("peer %s timed out after %v during iteration %d", baseURL, elapsed, iteration))
			}

			// Handle other non-timeout errors
			iterErr := catchup.IterationError{
				Iteration:  iteration,
				Error:      err,
				Timestamp:  time.Now(),
				PeerURL:    baseURL,
				RetryCount: 0,
				Duration:   time.Since(startTime),
			}
			failedIterations = append(failedIterations, iterErr)

			if circuitBreaker != nil {
				circuitBreaker.RecordFailure()
			}

			// Report failed request to P2P service
			u.reportCatchupFailure(ctx, identifier)

			// Check if this is a malicious response. Both returns below are marked
			// as already-reported (the failure was recorded just above) so the
			// top-level catchup handler does not record a second failure for the
			// same attempt; the malicious classification itself is unaffected.
			if errors.IsMaliciousResponseError(err) {
				return catchup.CreateCatchupResult(
					allCatchupHeaders, blockUpTo.Hash(), startHash, startHeight, startTime, baseURL,
					iteration, failedIterations, false, "Malicious peer detected",
				), nil, markCatchupFailureReported(errors.NewNetworkPeerMaliciousError("peer returned malicious response", err))
			}

			return catchup.CreateCatchupResult(
				allCatchupHeaders, blockUpTo.Hash(), startHash, startHeight, startTime, baseURL,
				iteration, failedIterations, false, "Failed to fetch headers",
			), nil, markCatchupFailureReported(err)
		}

		// Validate header bytes
		if err = catchup.ValidateBlockHeaderBytes(blockHeadersBytes); err != nil {
			if circuitBreaker != nil {
				circuitBreaker.RecordFailure()
			}
			return catchup.CreateCatchupResult(
				allCatchupHeaders, blockUpTo.Hash(), startHash, startHeight, startTime, baseURL,
				iteration, failedIterations, false, "Invalid header bytes",
			), nil, err
		}

		// Parse headers
		blockHeaders, parseErr := catchup.ParseBlockHeaders(blockHeadersBytes)
		if parseErr != nil {
			u.logger.Errorf("[catchup][%s] iteration %d: header parse error: %v", chainTipHash.String(), iteration, parseErr)

			// Check if error indicates malicious behavior
			if errors.IsMaliciousResponseError(parseErr) {
				// Report malicious behavior to P2P service
				u.reportCatchupMalicious(ctx, identifier, "malicious response during header parsing")

				u.logger.Errorf("[catchup][%s] SECURITY: Peer %s sent malicious headers - should be banned (banning not yet implemented)", chainTipHash.String(), baseURL)

				return catchup.CreateCatchupResult(
					allCatchupHeaders, blockUpTo.Hash(), startHash, startHeight, startTime, baseURL,
					iteration, failedIterations, false, "Malicious headers detected",
				), nil, errors.NewNetworkPeerMaliciousError("peer sent invalid headers", parseErr)
			}

			// For non-malicious parse errors, still fail but with different error type
			if circuitBreaker != nil {
				circuitBreaker.RecordFailure()
			}

			return catchup.CreateCatchupResult(
				allCatchupHeaders, blockUpTo.Hash(), startHash, startHeight, startTime, baseURL,
				iteration, failedIterations, false, "Header parse failed",
			), nil, errors.NewNetworkInvalidResponseError("failed to parse headers", parseErr)
		}

		// Check if we got any headers
		if len(blockHeaders) == 0 {
			if iteration == 1 {
				// No headers on first iteration - this is an error condition
				return catchup.CreateCatchupResult(
					allCatchupHeaders, blockUpTo.Hash(), startHash, startHeight, startTime, baseURL,
					iteration, failedIterations, false, "No headers received",
				), nil, errors.NewNotFoundError("no headers received from peer")
			} else {
				// No headers on subsequent iterations means we've reached the tip
				stopReason = "Reached chain tip"
			}
			break
		}

		u.logger.Infof("[catchup][%s] iteration %d: received %d headers from peer", chainTipHash.String(), iteration, len(blockHeaders))

		// Validate headers batch (checkpoint validation) and proof of work
		if err = u.validateBatchHeaders(ctx, blockHeaders); err != nil {
			if errors.IsMaliciousResponseError(err) {
				// Report malicious behavior for checkpoint violation to P2P service
				u.reportCatchupMalicious(ctx, identifier, "checkpoint violation during header validation")

				return catchup.CreateCatchupResult(
					allCatchupHeaders, blockUpTo.Hash(), startHash, startHeight, startTime, baseURL,
					iteration, failedIterations, false, "Checkpoint violation",
				), nil, err
			}

			// Non-malicious validation error
			return catchup.CreateCatchupResult(
				allCatchupHeaders, blockUpTo.Hash(), startHash, startHeight, startTime, baseURL,
				iteration, failedIterations, false, "Header validation failed",
			), nil, err
		}

		// Append new headers to our collection
		if len(blockHeaders) > 0 {
			headersToAppend := blockHeaders

			// Skip first header after first iteration - GetBlockHeadersFromOldest includes the starting block,
			// which is a duplicate of the last header from the previous iteration
			if iteration > 1 && len(allCatchupHeaders) > 0 {
				// Verify the first header is indeed a duplicate before skipping
				if blockHeaders[0].Hash().IsEqual(allCatchupHeaders[len(allCatchupHeaders)-1].Hash()) {
					headersToAppend = blockHeaders[1:]
					u.logger.Debugf("[catchup][%s] iteration %d: skipping duplicate header %s", chainTipHash.String(), iteration, blockHeaders[0].Hash().String())
				}
			}

			// Check memory limit after duplicate removal
			if len(allCatchupHeaders)+len(headersToAppend) > maxAccumulatedHeaders {
				remainingCapacity := maxAccumulatedHeaders - len(allCatchupHeaders)
				if remainingCapacity > 0 {
					u.logger.Warnf("[catchup][%s] truncating %d headers to %d to stay within memory limit", chainTipHash.String(), len(headersToAppend), remainingCapacity)
					headersToAppend = headersToAppend[:remainingCapacity]
					allCatchupHeaders = append(allCatchupHeaders, headersToAppend...)
					totalHeadersFetched += len(headersToAppend)
				}
				stopReason = fmt.Sprintf("Memory limit reached (%d headers)", maxAccumulatedHeaders)
				break
			}

			// Only append if we have headers after skipping duplicates
			if len(headersToAppend) > 0 {
				allCatchupHeaders = append(allCatchupHeaders, headersToAppend...)
				totalHeadersFetched += len(headersToAppend)
			} else {
				// All headers were duplicates - we've reached the chain tip
				u.logger.Debugf("[catchup][%s] iteration %d: all headers were duplicates, stopping", chainTipHash.String(), iteration)
				stopReason = "All headers were duplicates (chain tip reached)"
				break
			}

			// Check if we've reached the target block
			for _, header := range headersToAppend {
				if header.Hash().IsEqual(blockUpTo.Hash()) {
					reachedTarget = true
					stopReason = "Reached target block"
					break
				}
			}

			if reachedTarget {
				break
			}
		}

		// If we received fewer headers than max, we've reached the chain tip
		if len(blockHeaders) < maxBlockHeadersPerRequest {
			stopReason = "Reached chain tip (received less than max headers)"
			break
		}

		// Update block locator for next iteration - use only the last header received
		lastHeader := blockHeaders[len(blockHeaders)-1]
		currentLocatorHashes = []*chainhash.Hash{lastHeader.Hash()}

		u.logger.Debugf("[catchup][%s] iteration %d complete: fetched %d new headers, next locator: %s", chainTipHash.String(), iteration, len(blockHeaders), lastHeader.Hash().String())
	}

	// Check if we hit the iteration limit
	if iteration >= maxCatchupIterations && stopReason == "" {
		stopReason = fmt.Sprintf("Reached maximum iterations (%d)", maxCatchupIterations)
		u.logger.Warnf("[catchup][%s] stopped after %d iterations without reaching target", chainTipHash.String(), iteration)
	}

	// Credit the peer for serving headers. This is a generic interaction success
	// (reputation), deliberately NOT a catchup success: the whole catchup records
	// exactly one attempt (doCatchup) and one outcome, so counting this stage as a
	// catchup success would let CatchupSuccesses exceed CatchupAttempts.
	if totalHeadersFetched > 0 {
		u.reportValidBlockHeaders(ctx, identifier, time.Since(startTime))
	}

	// Set default stop reason if none was set
	if stopReason == "" {
		if len(allCatchupHeaders) == 0 {
			stopReason = "No new headers to fetch"
		} else {
			stopReason = fmt.Sprintf("Fetched %d headers in %d iterations", totalHeadersFetched, iteration)
		}
	}

	// Record success with circuit breaker if we succeeded
	if circuitBreaker != nil && (reachedTarget || totalHeadersFetched > 0) {
		circuitBreaker.RecordSuccess()
	}

	u.logger.Infof("[catchup][%s] completed: %d headers fetched in %d iterations, reached target: %v, reason: %s", chainTipHash.String(), totalHeadersFetched, iteration, reachedTarget, stopReason)

	result := catchup.CreateCatchupResultWithLocator(allCatchupHeaders, blockUpTo.Hash(), startHash, startHeight, startTime, baseURL, iteration, failedIterations, reachedTarget, stopReason, locatorHashes)

	return result, bestBlockHeader, nil
}

// getPeerChainTip retrieves the peer's actual chain tip hash from the P2P registry.
// This returns the peer's BestBlockHash (from their node_status messages), which represents
// their actual chain position, not just blocks they've announced or are relaying.
//
// Parameters:
//   - ctx: Context for the operation
//   - peerID: The peer ID to look up
//
// Returns:
//   - *chainhash.Hash: The peer's chain tip hash, or nil if not found or P2P client unavailable
//   - error: Any error encountered
func (u *Server) getPeerChainTip(ctx context.Context, peerID string) (*chainhash.Hash, error) {
	// Check if P2P client is available
	if u.p2pClient == nil {
		return nil, errors.NewServiceError("P2P client not available")
	}

	// Get peer info from P2P registry
	peerInfo, err := u.p2pClient.GetPeer(ctx, peerID)
	if err != nil {
		return nil, errors.NewServiceError("failed to get peer info from P2P service", err)
	}

	// Check if peer was found
	if peerInfo == nil {
		return nil, errors.NewNotFoundError("peer %s not found in P2P registry", peerID)
	}

	// Check if peer has a block hash
	if peerInfo.BlockHash == nil {
		return nil, errors.NewNotFoundError("peer %s has no block hash in registry", peerID)
	}

	// Use the block hash directly (no need to parse)
	chainTipHash := peerInfo.BlockHash

	return chainTipHash, nil
}

// catchupTargetHash picks the block to request headers up to.
//
// The default is the block the peer announced. The P2P registry's BestBlockHash is preferred
// where it is genuinely more advanced, because it lets one catchup fetch the peer's whole chain
// rather than stopping at a block that may itself be relayed or stale.
//
// The registry entry is only refreshed from the peer's node_status every 10 seconds and nothing
// upstream discards a stale one — sanitizeAdvertisedTip caps tips that are too high but passes
// anything too low straight through — so it can name a block well behind the one the peer just
// announced. That matters because the served header stream ends at whatever we ask for: a stale
// tip truncates it below our own tip, every header in it is one we already hold, and the
// common-ancestor walk takes its ancestor at the end of a list in which nothing diverged. The
// resulting fork depth describes no fork, and validateForkDepth records a coinbase-maturity
// violation against a peer sitting on our own chain.
//
// Already holding the block is exactly the tell that the entry is stale: it can teach us nothing
// we do not have. blockUpTo cannot be stale in the same way — catchupGetBlockHeaders returns
// early if it already exists, so by this point it is a block we lack.
//
// Returns the announced block's hash on any doubt: a registry lookup failure, an existence check
// we could not complete, or an entry naming a block we hold.
func (u *Server) catchupTargetHash(ctx context.Context, blockUpTo *model.Block, peerID string) *chainhash.Hash {
	announced := blockUpTo.Hash()

	if peerID == "" {
		return announced
	}

	peerChainTip, err := u.getPeerChainTip(ctx, peerID)
	if err != nil {
		u.logger.Warnf("[catchup][%s] Could not get peer chain tip from P2P registry for peer %s: %v, falling back to announced block", announced.String(), peerID, err)
		return announced
	}

	if peerChainTip == nil {
		return announced
	}

	alreadyHave, existsErr := u.blockchainClient.GetBlockExists(ctx, peerChainTip)

	switch {
	case existsErr != nil:
		u.logger.Warnf("[catchup][%s] could not check whether peer %s's registry tip %s is already held: %v, falling back to announced block", announced.String(), peerID, peerChainTip.String(), existsErr)
		return announced
	case alreadyHave:
		u.logger.Infof("[catchup][%s] ignoring peer %s's registry tip %s: we already hold it, so the entry is stale - using announced block", announced.String(), peerID, peerChainTip.String())
		return announced
	default:
		u.logger.Infof("[catchup][%s] Using peer %s's actual chain tip %s instead of announced block %s", announced.String(), peerID, peerChainTip.String(), announced.String())
		return peerChainTip
	}
}
