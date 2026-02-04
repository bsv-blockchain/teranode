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
// and safe to proceed with pruner operations. Returns true if safe, false otherwise.
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

	// Use retry logic to wait for Block Assembly to be in "running" state
	_, err := retry.Retry(timeoutCtx, s.logger, func() (bool, error) {
		state, err := s.blockAssemblyClient.GetBlockAssemblyState(timeoutCtx)
		if err != nil {
			return false, errors.NewProcessingError("failed to get block assembly state", err)
		}

		if state.BlockAssemblyState != "running" {
			return false, errors.NewProcessingError("block assembly state is %s (not running)", state.BlockAssemblyState)
		}

		// State is "running", success!
		return true, nil
	},
		retry.WithBackoffDurationType(1*time.Second),
		retry.WithBackoffMultiplier(2),
		retry.WithRetryCount(1000), // High count - timeout context will stop retries after BlockAssemblyWaitTimeout
		retry.WithMessage(fmt.Sprintf("[Pruner] Waiting for block assembly to be ready for %s at height %d", phase, height)),
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
		retry.WithBackoffDurationType(1*time.Second),
		retry.WithBackoffMultiplier(2),
		retry.WithRetryCount(1000), // High count - timeout context will stop retries after BlockAssemblyWaitTimeout
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

		case height := <-s.prunerCh:
			// Deduplicate: drain channel and process latest height only
			// This is important during block catchup when multiple heights may be queued
			latestHeight := height
			drained := false
		drainLoop:
			for {
				select {
				case nextHeight := <-s.prunerCh:
					latestHeight = nextHeight
					drained = true
				default:
					break drainLoop
				}
			}

			if drained {
				s.logger.Debugf("Deduplicating pruner operations, skipping to height %d", latestHeight)
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
					s.logger.Debugf("Skipping pruner during catchup (height %d)", latestHeight)
					prunerSkipped.WithLabelValues("catchup_mode").Inc()
					continue
				}
			}

			// Safety check before pruning
			if !s.checkBlockAssemblySafeForPruner(ctx, "pruner", latestHeight) {
				continue
			}

			// Phase 1: Preserve parents of old unmined transactions
			// This must run before Phase 2 to protect parents from deletion
			if s.utxoStore != nil {
				s.logger.Debugf("Phase 1: Preserving parents at height %d", latestHeight)
				if count, err := utxo.PreserveParentsOfOldUnminedTransactions(
					ctx, s.utxoStore, latestHeight, s.settings, s.logger,
				); err != nil {
					s.logger.Warnf("Phase 1: Failed to preserve parents at height %d: %v", latestHeight, err)
					prunerErrors.WithLabelValues("parent_preservation").Inc()
					// Continue to Phase 2 - best effort, don't block pruning
				} else if count > 0 {
					s.logger.Infof("Phase 1: Preserved parents for %d unmined transactions at height %d", count, latestHeight)
				}
			}

			// Phase 2: DAH pruning (deletion)
			// Deletes transactions marked for deletion at or before the current height
			if s.prunerService != nil {
				s.logger.Infof("Phase 2: Starting DAH pruner for height %d", latestHeight)
				startTime := time.Now()

				recordsProcessed, err := s.prunerService.Prune(ctx, latestHeight)
				if err != nil {
					s.logger.Errorf("Phase 2: DAH pruner failed for height %d: %v", latestHeight, err)
					prunerErrors.WithLabelValues("dah_pruner").Inc()
				} else {
					s.logger.Infof("Phase 2: Pruned %d records at height %d", recordsProcessed, latestHeight)
					prunerDuration.WithLabelValues("dah_pruner").Observe(time.Since(startTime).Seconds())
					prunerProcessed.Inc()
				}
			}

			// Update last processed height atomically
			s.lastProcessedHeight.Store(latestHeight)
		}
	}
}
