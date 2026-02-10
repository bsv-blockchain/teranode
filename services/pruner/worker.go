package pruner

import (
	"context"
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util/retry"
)

// checkBlockAssemblySafeForPruner verifies that block assembly is in "running" state
// and has caught up to the specified height. Returns true if safe, false otherwise.
// This function will retry checking the block assembly state until the configured
// timeout is reached, allowing for temporary state transitions (e.g., brief reorgs).
func (s *Server) checkBlockAssemblySafeForPruner(ctx context.Context, phase string, height uint32) bool {
	// If no block assembly client (e.g., in tests), skip safety check
	if s.blockAssemblyClient == nil {
		return true
	}

	// Create a context with timeout based on settings
	timeoutCtx, cancel := context.WithTimeout(ctx, s.settings.Pruner.BlockAssemblyWaitTimeout)
	defer cancel()

	// Use retry logic to wait for Block Assembly to be in "running" state and at correct height
	_, err := retry.Retry(timeoutCtx, s.logger, func() (bool, error) {
		state, err := s.blockAssemblyClient.GetBlockAssemblyState(timeoutCtx)
		if err != nil {
			return false, errors.NewProcessingError("failed to get block assembly state", err)
		}

		if state.BlockAssemblyState != "running" {
			return false, errors.NewProcessingError("block assembly state is %s (not running)", state.BlockAssemblyState)
		}

		// Check that block assembly has caught up to the height being pruned
		// Allow 1 block tolerance for race conditions during rapid block generation
		if state.CurrentHeight < height-1 {
			return false, errors.NewProcessingError("block assembly height %d is behind pruner height %d", state.CurrentHeight, height)
		}

		// If within tolerance but not caught up, log and retry
		if state.CurrentHeight < height {
			s.logger.Debugf("[pruner][height:%d] block assembly catching up (ba:%d, pruner:%d)", height, state.CurrentHeight, height)
			return false, errors.NewProcessingError("block assembly catching up")
		}

		// State is "running" and height is correct, success!
		return true, nil
	},
		retry.WithBackoffDurationType(1*time.Second),
		retry.WithBackoffMultiplier(1), // Linear backoff for predictable timing
		retry.WithRetryCount(1000),     // 1000 attempts with 1s intervals = ~1000s max
		retry.WithMessage(fmt.Sprintf("[pruner][height:%d] waiting for block assembly to be ready for %s", height, phase)),
	)

	if err != nil {
		// Timeout or persistent error - log and skip pruning
		s.logger.Warnf("Skipping %s for height %d: block assembly wait timeout or error: %v", phase, height, err)
		prunerSkipped.WithLabelValues("block_assembly_timeout").Inc()
		return false
	}

	// Block Assembly is ready
	return true
}

// waitForBlockMinedStatus waits for the block to have mined_set=true, indicating that
// block validation has completed. Returns true if mined, false otherwise.
// This function will retry checking the mined status until the configured timeout is reached,
// allowing time for block validation to complete.
func (s *Server) waitForBlockMinedStatus(ctx context.Context, blockHash *chainhash.Hash) bool {
	// Create a context with timeout based on settings
	timeoutCtx, cancel := context.WithTimeout(ctx, s.settings.Pruner.BlockAssemblyWaitTimeout)
	defer cancel()

	// Use retry logic to wait for block to have mined_set=true
	_, err := retry.Retry(timeoutCtx, s.logger, func() (bool, error) {
		isMined, err := s.blockchainClient.GetBlockIsMined(timeoutCtx, blockHash)
		if err != nil {
			return false, errors.NewProcessingError("failed to check mined_set status", err)
		}

		if !isMined {
			return false, errors.NewProcessingError("block has mined_set=false")
		}

		// Block has mined_set=true, success!
		return true, nil
	},
		retry.WithBackoffDurationType(500*time.Millisecond), // Faster initial checks
		retry.WithBackoffMultiplier(1),                      // Linear backoff for predictable timing
		retry.WithRetryCount(1000),                          // 1000 attempts with 500ms intervals = ~500s max
		retry.WithMessage(fmt.Sprintf("[Pruner] Waiting for block %s to have mined_set=true", blockHash)),
	)

	if err != nil {
		// Timeout or persistent error - log and skip
		s.logger.Debugf("Block %s mined_set wait timeout or error: %v", blockHash, err)
		return false
	}

	// Block has mined_set=true
	return true
}

// prunerProcessor processes pruner requests from the pruner channel.
// It drains the channel to get the latest height (deduplication), then performs
// a two-phase pruning operation:
//
// PHASE 1 - PARENT PRESERVATION:
// Preserves parents of old unmined transactions by setting PreserveUntil flags.
// This ensures parent transactions remain available if unmined children are later
// mined or resubmitted. Only runs when UTXO store is available.
//
// PHASE 2 - DAH DELETION:
// Delete-at-height pruning removes old transaction records from storage.
// Records are only deleted if they've passed the retention window and are not preserved.
//
// CATCHUP SKIP MODE:
// When SkipDuringCatchup is enabled (default: false), the pruner skips all operations
// during FSMStateCATCHINGBLOCKS state. This prevents race conditions where block
// validation marks transactions as mined faster than the pruner can preserve their parents.
// Once the node transitions to FSMStateRUNNING, the pruner resumes normal operation.
//
// SAFETY CHECKS:
// Block assembly state is checked before pruning to ensure it's safe to proceed. This prevents
// pruning during reorgs or other state transitions.
//
// DEDUPLICATION:
// Only one pruner operation runs at a time. The channel is drained to process only the latest
// height, which is important during catchup when multiple heights may be queued.
func (s *Server) prunerProcessor(ctx context.Context) {
	s.logger.Infof("Starting pruner processor")

	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("Stopping pruner processor")
			return

		case req := <-s.prunerCh:
			// Deduplicate: drain channel and process latest request only
			// This is important during block catchup when multiple heights may be queued
			latestReq := req
			var hashStr string
			drained := false
		drainLoop:
			for {
				select {
				case nextReq := <-s.prunerCh:
					latestReq = nextReq
					drained = true
				default:
					break drainLoop
				}
			}

			if drained {
				if latestReq.BlockHash != nil {
					hashStr = latestReq.BlockHash.String()
				} else {
					hashStr = "<unknown>"
				}
				s.logger.Debugf("[pruner][%s:%d] deduplicating operations, skipping to height %d",
					hashStr, latestReq.Height, latestReq.Height)
			}

			// Check FSM state - skip during CATCHINGBLOCKS if configured
			if s.settings.Pruner.SkipDuringCatchup {
				fsmState, err := s.blockchainClient.GetFSMCurrentState(ctx)
				if err != nil {
					s.logger.Warnf("Failed to get FSM state, skipping pruner: %v", err)
					prunerSkipped.WithLabelValues("fsm_error").Inc()
					continue
				}
				if fsmState != nil && *fsmState == blockchain.FSMStateCATCHINGBLOCKS {
					if latestReq.BlockHash != nil {
						hashStr = latestReq.BlockHash.String()
					} else {
						hashStr = "<unknown>"
					}
					s.logger.Debugf("[pruner][%s:%d] skipping during catchup", hashStr, latestReq.Height)
					prunerSkipped.WithLabelValues("catchup_mode").Inc()
					continue
				}
			}

			// Initialize hashStr for logging
			if latestReq.BlockHash != nil {
				hashStr = latestReq.BlockHash.String()
			} else {
				hashStr = "<nil>"
			}

			// Wait for block to be mined (only if blockHash provided AND block assembly is running)
			// BlockPersisted notifications have nil blockHash (already mined)
			// Block notifications have blockHash and need mined_set wait only when block assembly is active
			// When block assembly is not running (e.g., in tests), blocks are immediately mined
			if latestReq.BlockHash != nil && s.blockAssemblyClient != nil {
				s.logger.Debugf("[pruner][%s:%d] waiting for mined_set=true",
					hashStr, latestReq.Height)
				if !s.waitForBlockMinedStatus(ctx, latestReq.BlockHash) {
					// Already logged by waitForBlockMinedStatus
					continue
				}
				s.logger.Debugf("[pruner][%s:%d] block has mined_set=true",
					hashStr, latestReq.Height)
			}

			// Safety check before pruning
			if !s.checkBlockAssemblySafeForPruner(ctx, "pruner", latestReq.Height) {
				continue
			}

			// Safety check passed - now queue blob pruning to run concurrently with Phase 1&2
			select {
			case s.blobDeletionCh <- &PruneRequest{
				Height:    latestReq.Height,
				BlockHash: latestReq.BlockHash,
			}:
				hashStr := "<unknown>"
				if latestReq.BlockHash != nil {
					hashStr = latestReq.BlockHash.String()
				}
				s.logger.Debugf("[pruner][%s:%d] queued blob pruning", hashStr, latestReq.Height)
			default:
				hashStr := "<unknown>"
				if latestReq.BlockHash != nil {
					hashStr = latestReq.BlockHash.String()
				}
				s.logger.Warnf("[pruner][%s:%d] blob deletion channel full, skipping blob pruning", hashStr, latestReq.Height)
			}

			prunerActive.Set(1)

			// Phase 1: Preserve parents of old unmined transactions
			// This must run before Phase 2 to protect parents from deletion
			if s.utxoStore != nil {
				hashStr := "<unknown>"
				if latestReq.BlockHash != nil {
					hashStr = latestReq.BlockHash.String()
				}
				s.logger.Debugf("[pruner][%s:%d] phase 1: preserving parents", hashStr, latestReq.Height)
				startTimePhase1 := time.Now()
				if count, err := utxo.PreserveParentsOfOldUnminedTransactions(
					ctx, s.utxoStore, latestReq.Height, hashStr, s.settings, s.logger,
				); err != nil {
					s.logger.Warnf("[pruner][%s:%d] phase 1: failed to preserve parents: %v", hashStr, latestReq.Height, err)
					prunerErrors.WithLabelValues("parent_preservation").Inc()
					// Continue to Phase 2 - best effort, don't block pruning
				} else {
					prunerDuration.WithLabelValues("preserve_parents").Observe(time.Since(startTimePhase1).Seconds())
					if count > 0 {
						// Detailed logging already happens in pruner_unmined.go with correct parent/tx counts
						prunerUpdatingParents.Add(float64(count))
					}
				}
			}

			// Phase 2: DAH pruning (deletion)
			// Deletes transactions marked for deletion at or before the current height
			if s.prunerService != nil {
				hashStr := "<unknown>"
				if latestReq.BlockHash != nil {
					hashStr = latestReq.BlockHash.String()
				}
				startTime := time.Now()

				recordsProcessed, err := s.prunerService.Prune(ctx, latestReq.Height, hashStr)
				if err != nil {
					s.logger.Errorf("[pruner][%s:%d] phase 2: DAH pruner failed: %v", hashStr, latestReq.Height, err)
					prunerErrors.WithLabelValues("dah_pruner").Inc()
				} else {
					prunerDuration.WithLabelValues("dah_pruner").Observe(time.Since(startTime).Seconds())
					prunerDeletingChildren.Add(float64(recordsProcessed))
				}
			}

			prunerCurrentHeight.Set(float64(latestReq.Height))
			prunerActive.Set(0)

			// Update last processed height atomically
			s.lastProcessedHeight.Store(latestReq.Height)
		}
	}
}
