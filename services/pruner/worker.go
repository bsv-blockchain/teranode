package pruner

import (
	"context"
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
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

// prunerProcessor processes pruning operations triggered by notification signals.
// It reads the latest target height from an atomic variable (set by the notification handler),
// then performs a two-phase pruning operation:
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
// Only one pruner operation runs at a time. The atomic target height always reflects the latest
// notification, so intermediate heights are naturally skipped.
func (s *Server) prunerProcessor(ctx context.Context) {
	s.logger.Infof("Starting pruner processor")

	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("Stopping pruner processor")
			return

		case sig := <-s.pruneNotify:
			blockHeight := sig.blockHeight
			blockHashStr := sig.blockHash.String()

			if blockHeight <= s.lastProcessedHeight.Load() {
				continue
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
					s.logger.Debugf("[pruner][%s:%d] skipping during catchup", blockHashStr, blockHeight)
					prunerSkipped.WithLabelValues("catchup_mode").Inc()
					continue
				}
			}

			// Wait for block to be mined (only in OnBlockMined trigger mode AND when block assembly is running)
			// OnBlockPersisted mode receives notifications after block is already mined
			// OnBlockMined mode needs to wait for mined_set=true before proceeding
			if s.settings.Pruner.BlockTrigger == settings.PrunerBlockTriggerOnBlockMined && s.blockAssemblyClient != nil {
				s.logger.Debugf("[pruner][%s:%d] waiting for mined_set=true", blockHashStr, blockHeight)
				if !s.waitForBlockMinedStatus(ctx, &sig.blockHash) {
					continue
				}
				s.logger.Debugf("[pruner][%s:%d] block has mined_set=true", blockHashStr, blockHeight)
			}

			// Safety check before pruning
			if !s.checkBlockAssemblySafeForPruner(ctx, "pruner", blockHeight) {
				continue
			}

			// Safety check passed - wake blob deletion worker to run concurrently with Phase 1&2
			select {
			case <-s.blobNotify:
			default:
			}
			s.blobNotify <- sig
			s.logger.Debugf("[pruner][%s:%d] notified blob deletion worker", blockHashStr, blockHeight)

			prunerActive.Set(1)

			// Phase 1: Preserve parents of old unmined transactions
			// This must run before Phase 2 to protect parents from deletion
			if s.utxoStore != nil && !s.settings.Pruner.SkipPreserveParents {
				s.logger.Debugf("[pruner][%s:%d] phase 1: preserving parents", blockHashStr, blockHeight)
				startTime := time.Now()
				if recordsProcessed, err := utxo.PreserveParentsOfOldUnminedTransactions(
					ctx, s.utxoStore, blockHeight, blockHashStr, s.settings, s.logger,
				); err != nil {
					s.logger.Warnf("[pruner][%s:%d] phase 1: failed to preserve parents: %v", blockHashStr, blockHeight, err)
					prunerErrors.WithLabelValues("parent_preservation").Inc()
				} else {
					prunerDuration.WithLabelValues("preserve_parents").Observe(time.Since(startTime).Seconds())
					if recordsProcessed > 0 {
						prunerUpdatingParents.Add(float64(recordsProcessed))
					}
				}
			} else if s.settings.Pruner.SkipPreserveParents {
				s.logger.Infof("[pruner][%s:%d] phase 1: skipped (pruner_skipPreserveParents=true)", blockHashStr, blockHeight)
			}

			// Phase 2: DAH pruning (deletion)
			// Deletes transactions marked for deletion at or before the current height
			if s.prunerService != nil {
				startTime := time.Now()
				recordsProcessed, err := s.prunerService.Prune(ctx, blockHeight, blockHashStr)
				if err != nil {
					s.logger.Errorf("[pruner][%s:%d] phase 2: DAH pruner failed: %v", blockHashStr, blockHeight, err)
					prunerErrors.WithLabelValues("dah_pruner").Inc()
				} else {
					prunerDuration.WithLabelValues("dah_pruner").Observe(time.Since(startTime).Seconds())
					prunerDeletingChildren.Add(float64(recordsProcessed))
				}
			}

			prunerCurrentHeight.Set(float64(blockHeight))
			prunerActive.Set(0)

			// Update last processed height atomically
			s.lastProcessedHeight.Store(blockHeight)
		}
	}
}
