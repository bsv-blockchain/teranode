// This file contains the main catchup logic for blockchain synchronization.
package blockvalidation

import (
	"context"
	"encoding/binary"
	"math/big"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/services/blockchain/work"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/catchup"
	"github.com/bsv-blockchain/teranode/util/blockassemblyutil"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

const (
	// maxBlockHeadersPerRequest is the maximum number of headers to request in a single batch
	maxBlockHeadersPerRequest = 10_000

	// maxCatchupIterations was the old iteration limit, kept for reference but no longer used
	// since we now make a single request for headers
	maxCatchupIterations = 1000

	// catchupReputationReportTimeout bounds the best-effort peer-reputation gRPC calls
	// made when releasing the catchup lock, so a slow or hung P2P service cannot stall
	// catchup teardown or detach the calls from shutdown.
	catchupReputationReportTimeout = 5 * time.Second
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
	highestCheckpointHeight uint32 // Highest checkpoint height hash-verified in THIS catchup run (not the highest configured checkpoint)
	catchupError            error  // Any error encountered during catchup
	incompleteBlockHash     string // Block hash reported when a peer serves an incomplete block

	// failedPeers records the peers that actually failed to serve data during this
	// catchup cycle, deduplicated by peer ID (last error message wins). Written from
	// concurrent per-subtree fetch goroutines via recordCatchupPeerFailure and drained
	// by releaseCatchupLock, which charges each of them once. Deduplication keeps
	// CatchupFailures from exceeding CatchupAttempts when several concurrent subtree
	// fetches fail against the same peer.
	failedPeersMu sync.Mutex
	failedPeers   map[string]string

	// Performance monitoring and dynamic peer switching
	performanceMonitor   *CatchupPerformanceMonitor
	enableParallelFetch  bool // Whether to fetch subtrees from multiple peers in parallel
	parallelFetchWorkers int  // Number of parallel fetch workers (default: 3)

	// Checkpoints to use for this catchup (isolated copy, not shared with settings)
	checkpoints []chaincfg.Checkpoint
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
func (u *Server) catchup(ctx context.Context, blockUpTo *model.Block, peerID, baseURL string) (err error) {
	ctx, _, deferFn := tracing.Tracer("blockvalidation").Start(ctx, "catchup",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[catchup][%s] starting catchup to %s", blockUpTo.Hash().String(), baseURL),
	)
	defer deferFn()

	// Initialize checkpoints for this catchup session (isolated copy, doesn't affect settings)
	var checkpoints []chaincfg.Checkpoint

	// Check if there's a checkpoint override in settings (WARNING: This is a dangerous setting that bypasses
	// the standard checkpoint verification. It should only be used for testing or in controlled
	// environments where the checkpoint hash and height are known to be valid. Using incorrect
	// values can lead to accepting an invalid blockchain state.)
	if u.settings.BlockValidation.CatchupCheckpointHash != "" && u.settings.BlockValidation.CatchupCheckpointHeight != 0 {
		checkpointHash, err := chainhash.NewHashFromStr(u.settings.BlockValidation.CatchupCheckpointHash)
		if err != nil {
			return errors.NewInvalidArgumentError("invalid catchup checkpoint hash '%s': %v", u.settings.BlockValidation.CatchupCheckpointHash, err)
		}

		u.logger.Warnf("[catchup][%s] WARNING: Using custom checkpoint override at height %d, hash %s. This bypasses standard checkpoint verification and should only be used for testing!", blockUpTo.Hash().String(), u.settings.BlockValidation.CatchupCheckpointHeight, checkpointHash.String())

		// Use the custom checkpoint (replaces all standard checkpoints for this catchup only)
		checkpoints = []chaincfg.Checkpoint{
			{Height: u.settings.BlockValidation.CatchupCheckpointHeight, Hash: checkpointHash},
		}
	} else if u.settings.ChainCfgParams != nil && len(u.settings.ChainCfgParams.Checkpoints) > 0 {
		// Make a copy of the chain config checkpoints (don't mutate the shared settings)
		checkpoints = make([]chaincfg.Checkpoint, len(u.settings.ChainCfgParams.Checkpoints))
		copy(checkpoints, u.settings.ChainCfgParams.Checkpoints)
	}

	// Validate that we have a baseURL for making HTTP requests
	if baseURL == "" {
		return errors.NewInvalidArgumentError("baseURL is required for catchup")
	}

	// Validate that baseURL is a proper HTTP/HTTPS URL and not a peer ID
	// Special case: "legacy" is allowed for blocks from the legacy service
	if baseURL != "legacy" {
		parsedURL, err := url.Parse(baseURL)
		if err != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") {
			u.logger.Errorf("[catchup][%s] Invalid baseURL '%s' - must be valid http/https URL, not peer ID. PeerID: %s",
				blockUpTo.Hash().String(), baseURL, peerID)
			return errors.NewProcessingError("[catchup][%s] invalid baseURL - not a valid http/https URL", blockUpTo.Hash().String())
		}
	}

	// Use baseURL as fallback if peerID is not provided (for backward compatibility)
	if peerID == "" {
		peerID = baseURL
	}

	// Report catchup attempt to P2P service
	u.reportCatchupAttempt(ctx, peerID)

	// Configure performance monitoring
	perfConfig := DefaultPerformanceMonitorConfig()
	if u.settings.BlockValidation.CatchupMinThroughputKBps > 0 {
		perfConfig.MinThroughputKBPerSec = float64(u.settings.BlockValidation.CatchupMinThroughputKBps)
	}

	catchupCtx := &CatchupContext{
		blockUpTo:            blockUpTo,
		baseURL:              baseURL,
		peerID:               peerID,
		startTime:            time.Now(),
		performanceMonitor:   NewCatchupPerformanceMonitor(u.logger, peerID, baseURL, perfConfig),
		enableParallelFetch:  u.settings.BlockValidation.CatchupParallelFetchEnabled,
		parallelFetchWorkers: u.settings.BlockValidation.CatchupParallelFetchWorkers,
		checkpoints:          checkpoints,
	}

	// Default parallel fetch workers if not configured
	if catchupCtx.parallelFetchWorkers <= 0 {
		catchupCtx.parallelFetchWorkers = 3
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

	// Step 5.5: If fork detected, reset mined_set on old blocks for transaction state consistency.
	// This must run only AFTER the fork-depth and secret-mining safety checks pass; otherwise a peer
	// merely announcing a deep or secretly-mined fork would un-stamp this node's own still-canonical
	// txs for a fork that is then rejected, churning honest tx-mined state for no reorg (issue #1145).
	if catchupCtx.forkDepth > 0 {
		u.logger.Infof("[catchup][%s] Fork detected (depth %d), clearing mined_set on old blocks",
			catchupCtx.blockUpTo.Hash().String(), catchupCtx.forkDepth)

		currentHeight := catchupCtx.currentHeight
		headers, _, err := u.blockchainClient.GetBlockHeadersByHeight(ctx,
			catchupCtx.commonAncestorMeta.Height+1, currentHeight)
		if err != nil {
			u.logger.Errorf("[catchup][%s] Failed to get fork block headers: %v",
				catchupCtx.blockUpTo.Hash().String(), err)
		} else {
			var clearErrors, notifyErrors int
			for _, header := range headers {
				if err := u.blockchainClient.ClearBlockMinedSet(ctx, header.Hash()); err != nil {
					clearErrors++
				} else {
					// Send BlockMinedUnset notification to trigger immediate transaction status update
					// This ensures BlockValidation processes the block immediately instead of waiting
					// for the periodic job (which runs every 1 minute). Same pattern as InvalidateBlock RPC.
					if err := u.blockchainClient.SendNotification(ctx, &blockchain_api.Notification{
						Type: model.NotificationType_BlockMinedUnset,
						Hash: header.Hash().CloneBytes(),
					}); err != nil {
						notifyErrors++
					}
				}
			}
			if clearErrors > 0 || notifyErrors > 0 {
				u.logger.Errorf("[catchup][%s] Fork block cleanup: %d/%d clear failures, %d notification failures",
					catchupCtx.blockUpTo.Hash().String(), clearErrors, len(headers), notifyErrors)
			}
			u.logger.Infof("[catchup][%s] Cleared mined_set on %d fork blocks and sent notifications",
				catchupCtx.blockUpTo.Hash().String(), len(headers))
		}
	}

	// Step 6: Filter headers to only those we need to catchup
	if err = u.filterHeaders(ctx, catchupCtx); err != nil {
		return err
	}

	// Early exit if no new blocks to process.
	if len(catchupCtx.blockHeaders) == 0 {
		return u.handleNoNewHeaders(ctx, blockUpTo)
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

	u.reportValidatedHeaderProgress(catchupCtx)

	// Step 9.5: Validate header-chain difficulty (DAA) before fetching full blocks.
	// Header-fetch validation only checks each header's PoW against its own nBits; this
	// recomputes the DAA-required nBits from the header chain and rejects a peer that
	// supplied a self-consistent low-difficulty chain.
	if err = u.validateCatchupHeaderDifficulty(ctx, catchupCtx); err != nil {
		return err
	}

	// Step 10: Fetch and validate blocks
	if err = u.fetchAndValidateBlocks(ctx, catchupCtx); err != nil {
		return err
	}

	// Step 11: Clean up resources
	u.cleanup(catchupCtx)

	// Report successful catchup to P2P service
	u.reportCatchupSuccess(ctx, catchupCtx.peerID, time.Since(catchupCtx.startTime))

	return nil
}

// handleNoNewHeaders is invoked when filterHeaders leaves zero headers to process.
// A peer on a dead or shorter fork can legitimately return zero new headers even when
// blockUpTo is unknown to us. Returning nil unconditionally would make the outer
// peer-selection loop treat this as success and stop trying other peers, leaving the
// node unable to sync past the announced block. Only treat it as "already synced"
// when blockUpTo is known locally (present in our blocks table); otherwise surface
// an error so the caller moves on to the next peer.
//
// Note: this uses GetBlockExists, which is a local-existence check — it returns true
// for blocks on the main chain as well as for known off-chain (stale) blocks. That is
// intentional here: if we already have the announced block in any form, we have at
// least caught up to (or past) it and there is no value in asking another peer for it.
func (u *Server) handleNoNewHeaders(ctx context.Context, blockUpTo *model.Block) error {
	exists, err := u.blockchainClient.GetBlockExists(ctx, blockUpTo.Hash())
	if err != nil {
		return errors.NewProcessingError("[catchup][%s] peer returned no new headers and block existence check failed", blockUpTo.Hash().String(), err)
	}
	if !exists {
		return errors.NewProcessingError("[catchup][%s] peer returned no new headers but target block is not known locally - peer may be on a dead fork", blockUpTo.Hash().String())
	}
	u.logger.Infof("[catchup][%s] no new blocks to fetch - target block already known locally", blockUpTo.Hash().String())
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
		return errors.NewCatchupInProgressError("[catchup][%s] another catchup is currently in progress", ctx.blockUpTo.Hash().String())
	}

	// Initialize metrics (check for nil in tests)
	if prometheusCatchupActive != nil {
		prometheusCatchupActive.Set(1)
	}
	u.catchupAttempts.Add(1)

	// Store the active catchup context for status reporting
	u.activeCatchupCtxMu.Lock()
	u.activeCatchupCtx = ctx
	u.activeCatchupCtxMu.Unlock()

	// Reset progress counters
	u.blocksFetched.Store(0)
	u.blocksValidated.Store(0)

	return nil
}

// releaseCatchupLock releases the catchup lock and records metrics.
// Updates health check tracking and records success/failure metrics.
// If catchup failed, stores details in previousCatchupAttempt for dashboard display.
//
// Parameters:
//   - ctx: Catchup context containing operation state
//   - err: Pointer to error from catchup operation
func (u *Server) releaseCatchupLock(ctx *CatchupContext, err *error) {
	u.isCatchingUp.Store(false)
	if prometheusCatchupActive != nil {
		prometheusCatchupActive.Set(0)
	}

	// Reputation reporting decisions captured under the lock, executed after release.
	// We must NOT make gRPC calls while holding activeCatchupCtxMu: a slow or hung P2P
	// service would block the lock indefinitely, which in turn blocks GetCatchupStatus
	// (it RLocks the same mutex) and prevents the active catchup context from clearing.
	var (
		reportMalicious       bool
		reportPeerErr         bool
		reportIncompleteBlock bool
		peerID                string
		errorMsg              string
		incompleteBlockHash   string
		failedPeers           map[string]string
	)

	// Capture failure details for dashboard before clearing context
	u.activeCatchupCtxMu.Lock()
	if *err != nil && ctx != nil {
		// Determine error type based on error characteristics
		errorType := "unknown_error"
		errorMsg = (*err).Error()
		peerID = ctx.peerID
		isPeerError := true // Track if this is a peer-related error

		// TODO: all of these should be using error types, and not checking the strings (!)
		switch {
		case errors.Is(*err, errors.ErrBlockInvalid) || errors.Is(*err, errors.ErrTxInvalid):
			errorType = "validation_failure"
			// Mark peer as malicious for validation failure (reported after unlock)
			reportMalicious = true
		case errors.Is(*err, errors.ErrExternal):
			// Every peer attempt failed to fetch subtree data. The individual failures
			// were already attributed to the peers that produced them
			// (recordCatchupPeerFailure), so charging the catchup primary here as well
			// would penalize a healthy peer for another peer's failure — the 0%-success
			// symptom in issue 1368. Must precede the IsNetworkError case: one peer's
			// connection error inside the chain would otherwise classify the whole
			// all-peers-failed error as a network error against the primary.
			errorType = "peer_data_unavailable"
			isPeerError = false
		case errors.IsNetworkError(*err):
			errorType = "network_error"
		case strings.Contains(errorMsg, "secret mining") || strings.Contains(errorMsg, "secretly mined"):
			errorType = "secret_mining"
		case strings.Contains(errorMsg, "coinbase maturity"):
			errorType = "coinbase_maturity_violation"
		case strings.Contains(errorMsg, "checkpoint"):
			errorType = "checkpoint_verification_failed"
		case strings.Contains(errorMsg, "connection") || strings.Contains(errorMsg, "timeout"):
			errorType = "connection_error"
		case strings.Contains(errorMsg, "block assembly is behind"):
			// Block assembly being behind is a local system error, not a peer error
			errorType = "local_system_not_ready"
			isPeerError = false
		case errors.Is(*err, errors.ErrServiceUnavailable):
			// Service unavailable errors are local system issues, not peer errors
			errorType = "local_service_unavailable"
			isPeerError = false
		case errors.IsTransientBlockIncomplete(*err):
			// Transient LOCAL catchup-ordering gap (unabsorbed parent, issue 1031). Shares the
			// ErrBlockIncomplete code, so this case must precede the generic one below. Abort
			// and retry another peer, but do NOT report a peer failure or open a full-storage
			// penalty window: the serving peer is honest and possibly the sole source ahead.
			errorType = "block_incomplete_transient"
			isPeerError = false
		case errors.Is(*err, errors.ErrBlockIncomplete):
			// Peer-attributable incomplete block: the peer served header work it could not back
			// with a full block body (e.g. seeded peer without full block data). Penalize so a
			// header-only non-deliverer stops holding top-tier sync eligibility.
			errorType = "block_incomplete"
			isPeerError = false
			reportIncompleteBlock = true
			incompleteBlockHash = ctx.incompleteBlockHash
		}

		ctx.failedPeersMu.Lock()
		if len(ctx.failedPeers) > 0 {
			failedPeers = make(map[string]string, len(ctx.failedPeers))
			for id, msg := range ctx.failedPeers {
				failedPeers[id] = msg
			}
		}
		ctx.failedPeersMu.Unlock()

		u.previousCatchupAttempt = &PreviousAttempt{
			PeerID:            ctx.peerID,
			PeerURL:           ctx.baseURL,
			TargetBlockHash:   ctx.blockUpTo.Hash().String(),
			TargetBlockHeight: ctx.blockUpTo.Height,
			ErrorMessage:      errorMsg,
			ErrorType:         errorType,
			AttemptTime:       time.Now().UnixMilli(),
			DurationMs:        time.Since(ctx.startTime).Milliseconds(),
			BlocksValidated:   u.blocksValidated.Load(),
		}

		// Only store generic peer errors in the peer registry. Local system errors
		// and non-malicious incomplete-block reports use their own handling.
		if isPeerError {
			reportPeerErr = true
		} else {
			u.logger.Infof("[catchup][%s] Skipping generic peer error report for catchup error type: %s", ctx.blockUpTo.Hash().String(), errorType)
		}
	}

	// Clear the active catchup context
	u.activeCatchupCtx = nil
	u.activeCatchupCtxMu.Unlock()

	// Make the fire-and-forget reputation gRPC calls outside the lock with a bounded
	// context, so a stalled P2P service can neither hold activeCatchupCtxMu nor outlive
	// shutdown. These are best-effort; failures are logged inside the helpers.
	if reportMalicious || reportPeerErr || reportIncompleteBlock || len(failedPeers) > 0 {
		rpcCtx, cancel := context.WithTimeout(context.Background(), catchupReputationReportTimeout)
		defer cancel()

		// Charge each peer that actually failed to serve data, once, with its own
		// error text (issue 1368: a healthy peer used to carry another peer's 404).
		//
		// Accounting note: this charges a CatchupFailure without a paired
		// CatchupAttempt for the failing peer (only the primary gets a
		// reportCatchupAttempt call, at the start of catchup — see catchup.go's
		// call site). That is new to this change, and there is no precedent for it
		// in this package: the other sites that charge non-primary peers reach them
		// through catchup()/catchupFunc(), which calls reportCatchupAttempt
		// unconditionally, so those charges do have a paired attempt. Accepted
		// regardless, because CatchupAttempts/CatchupFailures are display/telemetry
		// counters, not reputation inputs (reputation is computed from the separate
		// Interaction* counters, which RecordCatchupFailureWithKind keeps internally
		// consistent). The tradeoff: the displayed attempts/failures ratio for a
		// fallback-only peer is not a rate.
		//
		// This loop is authoritative for every peer in failedPeers, including the
		// primary — deliberately, even though that means a peer which both failed
		// a subtree fetch mid-cycle AND caused the cycle's terminal error gets
		// charged twice for one attempt. An earlier version tried to skip the
		// primary here whenever the terminal error was a generic peer error, on
		// the assumption that Server.go's processCatchupChItem would charge it
		// once instead, using the terminal error. That assumption does not hold
		// for every terminal error shape: a genuinely local failure produced by
		// fetchAndStoreSubtreeAndSubtreeData (e.g. its "Local error fetching
		// subtree ... not retrying with other peers" wrap) is coded
		// ErrServiceError, which this switch has no specific case for (it falls
		// to the unknown_error default with isPeerError left true), and
		// Server.go's ErrServiceError branch returns early WITHOUT ever calling
		// reportCatchupFailureForError. Skipping the primary here in that case
		// charged it zero times for a real subtree failure it caused — an
		// under-charge that hides a genuine failure entirely, which is worse
		// than an over-charge that is at least directionally accurate. Both
		// increments feed InteractionAttempts/InteractionFailures consistently
		// (see the peer registry), so the occasional double-charge is a
		// telemetry precision cost, not a reputation-math break. The one
		// exception is reportIncompleteBlock, guarded below, where both charges
		// live in this same function and the skip carries no such cross-file risk.
		for failedPeerID, failedMsg := range failedPeers {
			if reportIncompleteBlock && failedPeerID == peerID {
				// reportIncompleteBlock already charges the primary below via
				// reportCatchupFailureWithKind, with the more specific
				// catchupFailureKindBlockIncomplete (which drives a documented
				// incomplete-block penalty window a generic charge would not).
				// Both calls are local to this function, so — unlike the
				// cross-file assumption described above — this skip is safe.
				// Still store the subtree-level error text so it isn't lost;
				// only the generic failure counter is skipped.
				u.reportCatchupError(rpcCtx, failedPeerID, failedMsg)
				continue
			}

			u.reportCatchupFailure(rpcCtx, failedPeerID)
			u.reportCatchupError(rpcCtx, failedPeerID, failedMsg)
		}

		if reportMalicious {
			u.reportCatchupMalicious(rpcCtx, peerID, "validation_failure")
		}

		if reportPeerErr {
			u.reportCatchupError(rpcCtx, peerID, errorMsg)
		}

		if reportIncompleteBlock {
			u.reportCatchupFailureWithKind(rpcCtx, peerID, catchupFailureKindBlockIncomplete, incompleteBlockHash)
		}
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

// recordCatchupPeerFailure attributes a data-serving failure to the peer that caused
// it, for the current catchup cycle. Best-effort: with no active catchup context
// (e.g. a direct block fetch outside catchup) the call is a no-op.
//
// errors.IsLocalError is checked here — not at each call site — so every caller
// gets the guard for free: a context cancellation (catchup abort / peer switch /
// shutdown landing mid-retry) or a local storage failure (subtreeStore.Set) is ours,
// not the peer's, and must never land an innocent peer in failedPeers.
func (u *Server) recordCatchupPeerFailure(peerID string, err error) {
	if peerID == "" || err == nil || errors.IsLocalError(err) {
		return
	}

	u.activeCatchupCtxMu.RLock()
	catchupCtx := u.activeCatchupCtx
	u.activeCatchupCtxMu.RUnlock()

	if catchupCtx == nil {
		return
	}

	catchupCtx.failedPeersMu.Lock()
	defer catchupCtx.failedPeersMu.Unlock()

	if catchupCtx.failedPeers == nil {
		catchupCtx.failedPeers = make(map[string]string)
	}

	catchupCtx.failedPeers[peerID] = err.Error()
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

	result, _, err := u.catchupGetBlockHeaders(ctx, catchupCtx.blockUpTo, catchupCtx.peerID, catchupCtx.baseURL)
	if err != nil {
		return errors.NewProcessingError("[catchup][%s] failed to get block headers", catchupCtx.blockUpTo.Hash().String(), err)
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
	var commonAncestorMeta *model.BlockHeaderMeta
	u.logger.Debugf("[catchup][%s] Checking %d peer headers for common ancestor (current UTXO height: %d)", catchupCtx.blockUpTo.Hash().String(), len(peerHeaders), currentHeight)

	for i, header := range peerHeaders {
		// GetBlockHeader conveys both existence and height in a single RPC: a
		// not-found error means the block is absent from our chain (stop the search),
		// while any other error is a genuine failure.
		_, meta, err := u.blockchainClient.GetBlockHeader(ctx, header.Hash())
		if err != nil {
			if errors.Is(err, errors.ErrBlockNotFound) {
				u.logger.Debugf("[catchup][%s] Block %s not in our chain - stopping search", catchupCtx.blockUpTo.Hash().String(), header.Hash().String())
				break // Once we find a header we don't have, stop
			}

			return errors.NewProcessingError("[catchup][%s] failed to get header for block %s: %v", catchupCtx.blockUpTo.Hash().String(), header.Hash().String(), err)
		}

		// Only consider blocks at or below our current UTXO height as potential common ancestors
		// Blocks ahead of our UTXO height exist in blockchain store but aren't fully processed yet
		if meta.Height > currentHeight {
			u.logger.Debugf("[catchup][%s] Block %s at height %d is ahead of current UTXO height %d - stopping search", catchupCtx.blockUpTo.Hash().String(), header.Hash().String(), meta.Height, currentHeight)
			break
		}

		commonAncestorIndex = i // Keep updating to find the LAST match
		commonAncestorMeta = meta
		u.logger.Debugf("[catchup][%s] Block %s exists in our chain at height %d (index %d)", catchupCtx.blockUpTo.Hash().String(), header.Hash().String(), meta.Height, i)
	}

	if commonAncestorIndex == -1 {
		return errors.NewProcessingError("[catchup][%s] no common ancestor found in peer headers", catchupCtx.blockUpTo.Hash().String())
	}

	// The common ancestor's metadata was already fetched during the walk above.
	commonAncestorHash := peerHeaders[commonAncestorIndex].Hash()

	if commonAncestorMeta.Invalid {
		return errors.NewBlockInvalidError("[catchup][%s] common ancestor %s at height %d is marked invalid, not catching up", catchupCtx.blockUpTo.Hash().String(), commonAncestorHash.String(), commonAncestorMeta.Height)
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

	// Reject only when fork depth strictly exceeds CoinbaseMaturity. A reorg of depth d
	// invalidates a coinbase at height G iff G >= H - d + 1; for that coinbase to have
	// been spent in the old chain we need G + CoinbaseMaturity <= H, which has a solution
	// only when d > CoinbaseMaturity. So d == CoinbaseMaturity is safe (no matured
	// coinbase in the reorged range can yet have been spent), and `>` is the correct
	// operator here, not `>=`. See issue #4592.
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

	// The headers the peer offers beyond the common ancestor form the candidate chain
	// whose cumulative work we weigh against our local chain. Not yet filtered at this
	// point, so slice directly after the common ancestor index.
	var offeredHeaders []*model.BlockHeader
	if catchupCtx.headersFetchResult != nil {
		peerHeaders := catchupCtx.headersFetchResult.Headers
		if catchupCtx.commonAncestorIndex >= 0 && catchupCtx.commonAncestorIndex+1 < len(peerHeaders) {
			offeredHeaders = peerHeaders[catchupCtx.commonAncestorIndex+1:]
		}
	}

	return u.checkSecretMiningFromCommonAncestor(ctx, catchupCtx.blockUpTo, catchupCtx.peerID, catchupCtx.baseURL, catchupCtx.commonAncestorHash, catchupCtx.commonAncestorMeta, offeredHeaders)
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
	catchupCtx.useQuickValidation = false

	// Quick validation disabled, no need to verify checkpoints
	if !u.settings.BlockValidation.CatchupAllowQuickValidation {
		return nil
	}

	// Get checkpoints from catchup context
	if len(catchupCtx.checkpoints) == 0 {
		u.logger.Debugf("[catchup][%s] No checkpoints configured", catchupCtx.blockUpTo.Hash().String())
		return nil // No checkpoints to verify
	}

	// Cannot verify checkpoints if there's a fork
	if catchupCtx.forkDepth > 0 {
		u.logger.Debugf("[catchup][%s] Fork detected (depth %d), checkpoint verification not applicable", catchupCtx.blockUpTo.Hash().String(), catchupCtx.forkDepth)
		return nil // Fork handling takes precedence
	}

	// Calculate the height range - much simpler now since headers are sequential with no gaps
	if len(catchupCtx.blockHeaders) == 0 {
		u.logger.Debugf("[catchup][%s] No headers to verify", catchupCtx.blockUpTo.Hash().String())
		return nil
	}

	// Verify checkpoints and determine if quick validation can be used
	checkpointsChecked, err := u.verifyCheckpointsAgainstHeaders(catchupCtx)
	if err != nil {
		return err
	}

	if checkpointsChecked > 0 {
		u.logger.Infof("[catchup][%s] Successfully verified %d checkpoint(s) in header chain", catchupCtx.blockUpTo.Hash().String(), checkpointsChecked)
		catchupCtx.useQuickValidation = true // enabled: BlockAssembly sync check added in tryQuickValidation
	}

	return nil
}

// verifyCheckpointsAgainstHeaders verifies checkpoints against the fetched headers
// and determines the highest checkpoint height for quick validation eligibility.
//
// Parameters:
//   - catchupCtx: Catchup context with headers and checkpoints to verify
//
// Returns:
//   - int: Number of checkpoints successfully verified
//   - error: If checkpoint verification fails (hash mismatch)
func (u *Server) verifyCheckpointsAgainstHeaders(catchupCtx *CatchupContext) (int, error) {
	// Track the highest checkpoint height actually verified (hash-matched) in this run.
	// Quick validation eligibility must be bound to what was genuinely proven this run,
	// not to the highest checkpoint in the globally configured list - a checkpoint that
	// is merely configured but falls outside the current catchup range (or is never
	// reached by the loop below) provides no cryptographic guarantee for this session.
	var highestVerifiedCheckpointHeight uint32

	firstBlockHeight := catchupCtx.commonAncestorMeta.Height + 1
	lastBlockHeight := catchupCtx.commonAncestorMeta.Height + uint32(len(catchupCtx.blockHeaders))

	u.logger.Debugf("[catchup][%s] Verifying checkpoints in height range %d-%d (common ancestor at %d)", catchupCtx.blockUpTo.Hash().String(), firstBlockHeight, lastBlockHeight, catchupCtx.commonAncestorMeta.Height)

	checkpointsChecked := 0
	for _, checkpoint := range catchupCtx.checkpoints {
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
				return 0, errors.NewProcessingError("[catchup][%s] internal error: checkpoint height %d maps to invalid header index %d", catchupCtx.blockUpTo.Hash().String(), checkpointHeight, headerIndex)
			}

			headerHash := catchupCtx.blockHeaders[headerIndex].Hash()
			if !headerHash.IsEqual(checkpoint.Hash) {
				// CRITICAL: Checkpoint hash mismatch - we're on the wrong chain!
				return 0, errors.NewProcessingError("[catchup][%s] CHECKPOINT VERIFICATION FAILED: checkpoint at height %d requires hash %s but got %s - stopping catchup", catchupCtx.blockUpTo.Hash().String(), checkpointHeight, checkpoint.Hash.String(), headerHash.String())
			}

			u.logger.Infof("[catchup][%s] Verified checkpoint at height %d with hash %s", catchupCtx.blockUpTo.Hash().String(), checkpointHeight, checkpoint.Hash.String())
			checkpointsChecked++

			if checkpointHeight > highestVerifiedCheckpointHeight {
				highestVerifiedCheckpointHeight = checkpointHeight
			}
		}
	}

	catchupCtx.highestCheckpointHeight = highestVerifiedCheckpointHeight

	return checkpointsChecked, nil
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

	// Limit validation channel buffer to prevent workers from racing too far ahead
	// This creates backpressure so workers don't fetch blocks 2000+ ahead of validation
	const maxValidationBuffer = 50
	validationBufferSize := min(int(size.Load()), maxValidationBuffer)
	validateBlocksChan := make(chan blockForValidation, validationBufferSize)

	// Channel for async subtree file writes (only used by quick validation)
	var writeJobsChan chan *SubtreeWriteJob

	// Transition FSM to CATCHINGBLOCKS for all catchup (chain-extending and fork blocks).
	// If the FSM rejects the transition, the error propagates
	// up to the catchupCh handler which handles it gracefully without penalizing the peer.
	if err := u.setFSMCatchingBlocks(ctx, catchupCtx, &size); err != nil {
		return err
	}
	defer u.restoreFSMState(ctx, catchupCtx)

	// Create error group for concurrent operations
	errorGroup, gCtx := errgroup.WithContext(ctx)

	// Start async write workers only if quick validation is enabled
	if catchupCtx.useQuickValidation {
		const writeJobsBufferSize = 256
		writeJobsChan = make(chan *SubtreeWriteJob, writeJobsBufferSize)

		numWriteWorkers := u.settings.BlockValidation.SubtreeBatchWriteConcurrency
		if numWriteWorkers <= 0 {
			numWriteWorkers = 16
		}
		for i := 0; i < numWriteWorkers; i++ {
			errorGroup.Go(func() error {
				return u.blockValidation.subtreeWriteWorker(gCtx, writeJobsChan)
			})
		}
	}

	// Start fetching blocks
	errorGroup.Go(func() error {
		return u.fetchBlocksConcurrently(gCtx, catchupCtx, validateBlocksChan, &size)
	})

	// Start validation in parallel
	errorGroup.Go(func() error {
		if writeJobsChan != nil {
			defer close(writeJobsChan)
		}
		return u.validateBlocksOnChannel(validateBlocksChan, gCtx, catchupCtx, &size, writeJobsChan)
	})

	// Wait for both operations to complete
	err := errorGroup.Wait()
	if err != nil {
		catchupCtx.catchupError = err
	}

	return err
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

// reportValidatedHeaderProgress records locally verified header work for the
// peer that served the header chain. The report is advisory and best-effort.
//
// This runs after the per-header proof-of-work checks but before Step 10's full
// block validation, so the credited value is per-header-PoW work, not yet
// difficulty-adjustment-validated work. That is safe against upward forgery: a
// higher credited value requires correspondingly harder bits backed by real PoW,
// so a peer cannot inflate its rank. A valid-PoW but wrong-difficulty chain can
// still be credited here; it is rejected later by full block validation.
func (u *Server) reportValidatedHeaderProgress(catchupCtx *CatchupContext) {
	height, blockHash, workBytes, ok := u.computeValidatedHeaderProgress(catchupCtx)
	if !ok {
		return
	}

	// Advisory best-effort report made while the global catchup single-flight lock is
	// held: bound it with the same timeout releaseCatchupLock uses for its reputation
	// gRPC calls so a wedged p2p service whose transport still answers keepalives cannot
	// stall all node catchup on the unbounded service context.
	rpcCtx, cancel := context.WithTimeout(context.Background(), catchupReputationReportTimeout)
	defer cancel()

	u.reportValidatedChainProgress(rpcCtx, catchupCtx.peerID, height, blockHash.String(), workBytes)
}

func (u *Server) computeValidatedHeaderProgress(catchupCtx *CatchupContext) (uint32, *chainhash.Hash, []byte, bool) {
	if catchupCtx == nil || catchupCtx.commonAncestorMeta == nil {
		return 0, nil, nil, false
	}

	if len(catchupCtx.blockHeaders) == 0 {
		return 0, nil, nil, false
	}

	if len(catchupCtx.commonAncestorMeta.ChainWork) == 0 {
		u.logger.Warnf("[catchup][%s] Skipping validated progress report: common ancestor chainwork is empty", catchupCtx.blockUpTo.Hash().String())
		return 0, nil, nil, false
	}

	totalWork := new(big.Int).SetBytes(catchupCtx.commonAncestorMeta.ChainWork)
	if totalWork.Sign() <= 0 {
		u.logger.Warnf("[catchup][%s] Skipping validated progress report: common ancestor chainwork is malformed", catchupCtx.blockUpTo.Hash().String())
		return 0, nil, nil, false
	}

	height := catchupCtx.commonAncestorMeta.Height
	var lastHash *chainhash.Hash

	for _, header := range catchupCtx.blockHeaders {
		if header == nil {
			u.logger.Warnf("[catchup][%s] Skipping validated progress report: nil header in validated range", catchupCtx.blockUpTo.Hash().String())
			return 0, nil, nil, false
		}

		bits := binary.LittleEndian.Uint32(header.Bits.CloneBytes())
		totalWork.Add(totalWork, work.CalcBlockWork(bits))
		height++
		lastHash = header.Hash()
	}

	if lastHash == nil {
		return 0, nil, nil, false
	}

	return height, lastHash, totalWork.Bytes(), true
}

// recordMaliciousAttempt records a malicious attempt from a peer.
// Updates peer metrics and logs security warnings.
//
// Parameters:
//   - peerID: P2P peer identifier of the malicious peer
//   - reason: Description of the malicious behavior
func (u *Server) recordMaliciousAttempt(peerID string, reason string) {
	if peerID == "" {
		return
	}

	// Report to P2P service (uses helper that falls back to local metrics)
	u.reportCatchupMalicious(context.Background(), peerID, reason)
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
		if errors.Is(err, errors.ErrStateError) {
			return errors.NewStateError("[catchup][%s] FSM rejected CATCHUPBLOCKS transition", catchupCtx.blockUpTo.Hash().String(), err)
		}
		return errors.NewServiceError("[catchup][%s] failed to transition FSM to CATCHINGBLOCKS", catchupCtx.blockUpTo.Hash().String(), err)
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
//   - writeJobsChan: Channel for async subtree writes (used only by quick validation)
//
// Returns:
//   - error: If validation fails or context is cancelled
func (u *Server) validateBlocksOnChannel(validateBlocksChan chan blockForValidation, gCtx context.Context, catchupCtx *CatchupContext, size *atomic.Int64, writeJobsChan chan<- *SubtreeWriteJob) error {
	i := 0
	blockUpTo := catchupCtx.blockUpTo
	baseURL := catchupCtx.baseURL
	peerID := catchupCtx.peerID

	// validate the blocks while getting them from the other node
	// this will block until all blocks are validated
	for item := range validateBlocksChan {
		block := item.block
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
			if err := blockassemblyutil.WaitForBlockAssemblyReady(gCtx, u.logger, u.blockAssemblyClient, block.Height, u.settings.BlockValidation.MaxBlocksBehindBlockAssembly); err != nil {
				return errors.NewProcessingError("[catchup:validateBlocksOnChannel][%s] failed to wait for block assembly for block %s: %v", blockUpTo.Hash().String(), block.Hash().String(), err)
			}

			// Get cached headers for validation
			cachedHeaders, _ := u.headerChainCache.GetValidationHeaders(block.Hash())

			// Try quick validation if applicable
			tryNormalValidation, err := u.tryQuickValidation(gCtx, block, catchupCtx, peerID, baseURL, writeJobsChan)
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
					PeerID:                  peerID,
				}

				// Validate the block using standard validation
				// Note: ValidateBlockWithOptions now automatically stores invalid blocks with invalid=true
				if err := u.blockValidation.ValidateBlockWithOptions(gCtx, block, baseURL, opts); err != nil {
					u.logger.Errorf("[catchup:validateBlocksOnChannel][%s] failed to validate block %s at position %d: %v", blockUpTo.Hash().String(), block.Hash().String(), i, err)

					// Incomplete block (e.g. no coinbase from seeded peer) — abort catchup on this peer
					// Block was NOT stored as invalid, so another peer can provide the full version
					// Failure reporting is centralized in releaseCatchupLock.
					if errors.Is(err, errors.ErrBlockIncomplete) {
						catchupCtx.incompleteBlockHash = block.Hash().String()
						u.logger.Warnf("[catchup:validateBlocksOnChannel][%s] block %s from peer %s is incomplete, aborting catchup", blockUpTo.Hash().String(), block.Hash().String(), peerID)
					} else if errors.Is(err, errors.ErrBlockInvalid) || errors.Is(err, errors.ErrTxInvalid) {
						// ValidateBlockWithOptions already stored the block as invalid if it's a consensus violation
						u.logger.Warnf("[catchup:validateBlocksOnChannel][%s] block %s violates consensus rules (already stored as invalid by ValidateBlockWithOptions)", blockUpTo.Hash().String(), block.Hash().String())
						u.reportCatchupMalicious(gCtx, peerID, "invalid_block_validation")
					}

					// Record metric for validation failure
					if prometheusCatchupErrors != nil {
						prometheusCatchupErrors.WithLabelValues(peerID, "validation_failure").Inc()
					}

					return err
				}
			}

			// Block validated successfully — credit reputation to all peers that contributed data
			u.reportValidBlockForPeers(gCtx, peerID, block.Hash().String(), item.contributingPeers)

			// Update the remaining block count
			remaining := size.Add(-1)
			if remaining%100 == 0 && remaining > 0 {
				u.logger.Infof("[catchup:validateBlocksOnChannel][%s] %d blocks remaining", blockUpTo.Hash().String(), remaining)
			}

			// Update validated counter for progress tracking
			u.blocksValidated.Add(1)
		}
	}

	u.logger.Infof("[catchup:validateBlocksOnChannel][%s] completed validation of %d blocks", blockUpTo.Hash().String(), i)

	return nil
}

// tryQuickValidation attempts quick validation for checkpointed blocks
// Returns true if normal validation should be tried, false if quick validation succeeded
func (u *Server) tryQuickValidation(ctx context.Context, block *model.Block, catchupCtx *CatchupContext, peerID, baseURL string, writeJobsChan chan<- *SubtreeWriteJob) (bool, error) {
	// Determine if this specific block can use quick validation
	// A block can use quick validation if it's at or below the highest verified checkpoint height
	canUseQuickValidation := catchupCtx.useQuickValidation && block.Height <= catchupCtx.highestCheckpointHeight

	// If block is not eligible for quick validation, use normal validation
	if !canUseQuickValidation {
		return true, nil
	}

	// Wait for block assembly to be ready before quick validation.
	// This ensures coinbase UTXOs are created by BlockAssembly before we try to spend them.
	// Without this check, quick validation could outpace BlockAssembly by more than the
	// coinbase maturity period (100 blocks), causing attempts to spend non-existent UTXOs.
	if err := blockassemblyutil.WaitForBlockAssemblyReady(
		ctx,
		u.logger,
		u.blockAssemblyClient,
		block.Height,
		u.settings.BlockValidation.MaxBlocksBehindBlockAssembly,
	); err != nil {
		u.logger.Warnf("[tryQuickValidation][%s] block assembly not ready, falling back to normal validation: %v",
			block.Hash().String(), err)
		return true, nil // Fall back to normal validation
	}

	// Quick validation: create UTXOs for the block and validate transactions in parallel
	if err := u.blockValidation.quickValidateBlockAsync(ctx, block, peerID, baseURL, writeJobsChan); err != nil {
		if prometheusCatchupErrors != nil {
			prometheusCatchupErrors.WithLabelValues(peerID, "validation_failure").Inc()
		}

		// Block is incomplete (e.g. seeded peer without full block data) — abort catchup for this peer
		// Keep subtree files — they contain valid data that the next peer's validation can reuse
		// Failure reporting is centralized in releaseCatchupLock.
		if errors.Is(err, errors.ErrBlockIncomplete) {
			catchupCtx.incompleteBlockHash = block.Hash().String()
			u.logger.Warnf("[catchup:tryQuickValidation][%s] block %s from peer %s is incomplete (no coinbase), aborting catchup",
				catchupCtx.blockUpTo.Hash().String(), block.Hash().String(), peerID)

			return false, err
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
//
// A common ancestor more than SecretMiningThreshold blocks back is a necessary but not
// sufficient signal: a legitimate reorg carrying more accumulated proof-of-work can fork
// just as deeply. The malicious verdict is therefore gated on WORK, not depth — a deep
// fork is only treated as secret mining when the chain the peer offers fails to exceed
// our local validated chainwork. This keeps the reputation penalty ("peer is malicious")
// separate from the policy decision of whether to auto-apply a deep reorg, which is
// enforced independently by validateForkDepth against coinbase maturity.
//
// Parameters:
//   - ctx: Context for cancellation
//   - blockUpTo: Target block being synced to
//   - baseURL: Peer URL for metrics
//   - commonAncestorHash: Hash of the common ancestor
//   - commonAncestorMeta: Metadata of the common ancestor
//   - offeredHeaders: Peer's headers after the common ancestor (the candidate chain)
//
// Returns:
//   - error: If secret mining is detected, or the deep fork cannot be safely followed
func (u *Server) checkSecretMiningFromCommonAncestor(ctx context.Context, blockUpTo *model.Block, peerID, baseURL string, commonAncestorHash *chainhash.Hash, commonAncestorMeta *model.BlockHeaderMeta, offeredHeaders []*model.BlockHeader) error {
	// Read the local best chain tip once, from the same source used for the work comparison
	// below, so the depth trigger and the work gate reason about the same tip (they can
	// momentarily disagree during catchup). If it can't be read we cannot evaluate the fork:
	// abort this catchup without penalising the peer — uncertainty must not be treated as malice.
	_, bestMeta, err := u.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		u.logger.Warnf("[catchup][%s] cannot read best block header for secret-mining check from peer %s: %v - aborting without flagging malicious", blockUpTo.Hash().String(), baseURL, err)
		return errors.NewProcessingError("[catchup][%s] unable to read best block header for secret-mining check", blockUpTo.Hash().String(), err)
	}

	currentHeight := bestMeta.Height

	// Common ancestor should always be at or below current height due to findCommonAncestor
	// validation. If not, we cannot reason about the fork - abort without penalising the peer.
	if commonAncestorMeta.Height > currentHeight {
		return errors.NewProcessingError("[catchup][%s] common ancestor height %d is ahead of current height %d - this should not happen", blockUpTo.Hash().String(), commonAncestorMeta.Height, currentHeight)
	}

	blocksBehind := currentHeight - commonAncestorMeta.Height

	// If we're not far enough in the chain, or the ancestor is not too far behind, it's not secret mining
	if currentHeight <= u.settings.BlockValidation.SecretMiningThreshold || blocksBehind <= u.settings.BlockValidation.SecretMiningThreshold {
		return nil
	}

	// The fork is deep. Without any offered headers we cannot weigh the candidate chain, so we
	// can prove neither that it is heavier nor that it is a withheld shorter chain. Treat this
	// like any other uncertainty: abort without penalising the peer.
	if len(offeredHeaders) == 0 {
		u.logger.Warnf("[catchup][%s] deep fork from peer %s (%d blocks behind) but no offered headers to weigh - aborting without flagging malicious", blockUpTo.Hash().String(), baseURL, blocksBehind)
		return errors.NewProcessingError("[catchup][%s] deep fork with no offered headers to compare chainwork from common ancestor at height %d", blockUpTo.Hash().String(), commonAncestorMeta.Height)
	}

	// Weigh the offered chain's cumulative work against our own before penalising.
	if offeredChainHasMoreWork(bestMeta.ChainWork, commonAncestorMeta, offeredHeaders) {
		// A legitimate heavier chain that happens to fork deep. Follow it; do not penalise the peer.
		u.logger.Infof("[catchup][%s] deep reorg (%d blocks) from peer %s carries more validated work than local chain - following heavier chain, not flagging secret mining", blockUpTo.Hash().String(), blocksBehind, baseURL)
		return nil
	}

	// A deep fork that offers no additional work: potential secret mining (withheld shorter chain).
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
	u.reportCatchupMalicious(ctx, peerID, "secret_mining")

	// Log ban request - actual banning should be handled by the P2P service
	u.logger.Errorf("[catchup][%s] SECURITY: Peer %s attempted secret mining - should be banned (banning not yet implemented)", blockUpTo.Hash().String(), baseURL)

	return errors.NewServiceError("[catchup][%s] is potentially a secretly mined chain from common ancestor at height %d, ignoring", blockUpTo.Hash().String(), commonAncestorMeta.Height)
}

// offeredChainHasMoreWork reports whether the chain offered by the peer — the common
// ancestor extended by offeredHeaders — carries strictly more cumulative proof-of-work
// than our local validated best chain (localChainWork, the best block header's cumulative
// work). It distinguishes a legitimate heavier deep reorg from a withheld shorter-work
// chain ("secret mining").
//
// Parameters:
//   - localChainWork: Cumulative work of the local best chain tip (big-endian bytes)
//   - commonAncestorMeta: Metadata of the common ancestor (supplies its cumulative work)
//   - offeredHeaders: Peer's headers after the common ancestor
//
// Returns:
//   - bool: True if the offered chain has strictly more cumulative work than the local chain
func offeredChainHasMoreWork(localChainWork []byte, commonAncestorMeta *model.BlockHeaderMeta, offeredHeaders []*model.BlockHeader) bool {
	// ChainWork is stored big-endian (see stores/blockchain/sql calculateAndPrepareChainWork),
	// so SetBytes reconstructs the cumulative work value directly.
	localWork := new(big.Int).SetBytes(localChainWork)

	// Start from the common ancestor's cumulative work and add each offered block's work,
	// mirroring work.CalculateWork (work = 2^256 / (target+1)).
	offeredWork := new(big.Int).SetBytes(commonAncestorMeta.ChainWork)
	for _, header := range offeredHeaders {
		if header == nil {
			continue
		}

		bits := binary.LittleEndian.Uint32(header.Bits.CloneBytes())
		offeredWork.Add(offeredWork, work.CalcBlockWork(bits))
	}

	return offeredWork.Cmp(localWork) > 0
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
