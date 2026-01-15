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

	// Check peer reputation via P2P service
	if u.isPeerMalicious(ctx, identifier) {
		u.logger.Warnf("[catchup][%s] peer %s is marked as malicious by P2P service", blockUpTo.Hash().String(), identifier)
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

	// Create block locator
	locatorHashes, err := u.blockchainClient.GetBlockLocator(ctx, bestBlockHeader.Hash(), bestBlockMeta.Height)
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

	// Get the peer's actual chain tip from P2P registry
	// This is the peer's BestBlockHash from their node_status messages,
	// not just a block they announced (which could be invalid or relayed)
	chainTipHash := blockUpTo.Hash() // Default to announced block
	if peerID != "" {
		peerChainTip, err := u.getPeerChainTip(ctx, peerID)
		if err != nil {
			// Log but don't fail - fall back to using blockUpTo
			u.logger.Warnf("[catchup][%s] Could not get peer chain tip from P2P registry for peer %s: %v, falling back to announced block", blockUpTo.Hash().String(), peerID, err)
		} else if peerChainTip != nil {
			u.logger.Infof("[catchup][%s] Using peer %s's actual chain tip %s instead of announced block %s", blockUpTo.Hash().String(), peerID, peerChainTip.String(), blockUpTo.Hash().String())
			chainTipHash = peerChainTip
		}
	}

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
		// Check if peer is marked as malicious by P2P service
		if u.isPeerMalicious(ctx, identifier) {
			u.logger.Warnf("[catchup][%s] peer %s is marked as malicious by P2P service, should skip catchup", chainTipHash.String(), baseURL)
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

				// Return a timeout error - this is just a slow peer, not necessarily malicious
				return catchup.CreateCatchupResult(
					allCatchupHeaders, blockUpTo.Hash(), startHash, startHeight, startTime, baseURL,
					iteration, failedIterations, false, "Peer response timeout",
				), nil, errors.NewNetworkTimeoutError("peer %s timed out after %v during iteration %d", baseURL, elapsed, iteration)
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

			// Check if this is a malicious response
			if errors.IsMaliciousResponseError(err) {
				return catchup.CreateCatchupResult(
					allCatchupHeaders, blockUpTo.Hash(), startHash, startHeight, startTime, baseURL,
					iteration, failedIterations, false, "Malicious peer detected",
				), nil, errors.NewNetworkPeerMaliciousError("peer returned malicious response: %w", err)
			}

			return catchup.CreateCatchupResult(
				allCatchupHeaders, blockUpTo.Hash(), startHash, startHeight, startTime, baseURL,
				iteration, failedIterations, false, "Failed to fetch headers",
			), nil, err
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
				), nil, errors.NewNetworkPeerMaliciousError("peer sent invalid headers: %w", parseErr)
			}

			// For non-malicious parse errors, still fail but with different error type
			if circuitBreaker != nil {
				circuitBreaker.RecordFailure()
			}

			return catchup.CreateCatchupResult(
				allCatchupHeaders, blockUpTo.Hash(), startHash, startHeight, startTime, baseURL,
				iteration, failedIterations, false, "Header parse failed",
			), nil, errors.NewNetworkInvalidResponseError("failed to parse headers: %w", parseErr)
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

	// Report successful catchup to P2P service (if we got any headers)
	if totalHeadersFetched > 0 {
		responseTime := time.Since(startTime)
		u.reportCatchupSuccess(ctx, identifier, responseTime)
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
		return nil, errors.NewServiceError("failed to get peer info from P2P service: %w", err)
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
