package pruner

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
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

// defaultRepairHoldPollInterval is how often phase 2 checks the hold while it deletes, and
// defaultRepairHoldRecheckInterval how long a held height waits before it is tried again.
const (
	defaultRepairHoldPollInterval    = time.Second
	defaultRepairHoldRecheckInterval = 30 * time.Second
)

// checkRepairHoldSlack makes sure a hold written just after a cycle's last check cannot let
// that cycle delete a parent block validation still needs. Such a parent's delete height is
// its spend height plus the UTXO retention, and its child was mined within the unmined
// retention of the spend (or the parent was preserved), so it is due at the cycle's height
// only if the overtaken block is more than retention minus unmined retention below the tip.
// That must exceed the deepest fork the node accepts, CoinbaseMaturity. On mainnet and
// testnet a configuration without that slack is refused; elsewhere it is logged.
func (s *Server) checkRepairHoldSlack() error {
	if s.settings == nil || s.settings.ChainCfgParams == nil {
		return nil
	}

	retention := s.settings.GetUtxoStoreBlockHeightRetention()
	unmined := s.settings.UtxoStore.UnminedTxRetention
	maturity := uint32(s.settings.ChainCfgParams.CoinbaseMaturity)

	var slack uint32
	if retention > unmined {
		slack = retention - unmined
	}

	if slack >= maturity {
		return nil
	}

	msg := "overtaken-tip repair hold slack too small: utxostore block height retention %d minus utxostore_unminedTxRetention %d is %d, below CoinbaseMaturity %d"

	switch s.settings.ChainCfgParams.Name {
	case "mainnet", "testnet":
		return errors.NewConfigurationError(msg, retention, unmined, slack, maturity)
	default:
		s.logger.Errorf("[pruner] "+msg+"; a deep enough reorg can let the pruner delete a parent the overtaken-tip repair still needs", retention, unmined, slack, maturity)
		return nil
	}
}

// watchRepairHold checks the overtaken-tip repair hold while phase 2 deletes, and cancels the
// deletion when one appears. The returned function stops the watch and reports whether it
// cancelled.
func (s *Server) watchRepairHold(ctx context.Context, cancel context.CancelFunc, blockHashStr string, blockHeight uint32) func() bool {
	interval := s.repairHoldPollInterval
	if interval <= 0 {
		interval = defaultRepairHoldPollInterval
	}

	var held atomic.Bool

	done := make(chan struct{})

	go func() {
		defer close(done)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// The deletion's own context: a read that fails because it was cancelled
				// is not a hold.
				if s.overtakenTipRepairHeld(ctx, blockHashStr, blockHeight) && ctx.Err() == nil {
					held.Store(true)
					cancel()

					return
				}
			}
		}
	}()

	return func() bool {
		cancel()
		<-done

		return held.Load()
	}
}

// retryHeldHeight tries a held height again after a while, so pruning resumes once the hold
// is released even when no new block arrives. A newer signal takes its place.
func (s *Server) retryHeldHeight(sig pruneSignal) {
	interval := s.repairHoldRecheckInterval
	if interval <= 0 {
		interval = defaultRepairHoldRecheckInterval
	}

	if s.repairHoldRetry != nil {
		s.repairHoldRetry.Stop()
	}

	s.repairHoldRetry = time.AfterFunc(interval, func() {
		select {
		case s.pruneNotify <- sig:
		default: // a newer signal is waiting
		}
	})
}

// overtakenTipRepairHeld reports whether block validation holds pruning for an overtaken
// block whose repair has not finished (model.OvertakenTipRepairHoldKey). No record means no
// hold. A record that cannot be read may be holding, so it counts as held.
func (s *Server) overtakenTipRepairHeld(ctx context.Context, blockHashStr string, blockHeight uint32) bool {
	if s.blockchainClient == nil {
		return false
	}

	data, err := s.blockchainClient.GetState(ctx, model.OvertakenTipRepairHoldKey)
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), sql.ErrNoRows.Error()) {
			prunerOvertakenTipRepairHeldBlocks.Set(0)
			return false
		}

		s.logger.Errorf("[pruner][%s:%d] failed to read the overtaken-tip repair hold, deleting nothing this cycle: %v", blockHashStr, blockHeight, err)
		prunerErrors.WithLabelValues("overtaken_tip_repair_hold").Inc()

		return true
	}

	held, err := model.DecodeOvertakenTipRepairHold(data)
	if err != nil {
		s.logger.Errorf("[pruner][%s:%d] unreadable overtaken-tip repair hold, deleting nothing this cycle: %v", blockHashStr, blockHeight, err)
		prunerErrors.WithLabelValues("overtaken_tip_repair_hold").Inc()

		return true
	}

	prunerOvertakenTipRepairHeldBlocks.Set(float64(len(held)))

	if len(held) == 0 {
		return false
	}

	s.logger.Warnf("[pruner][%s:%d] deleting nothing: block validation is still repairing %d overtaken blocks, lowest %s at height %d", blockHashStr, blockHeight, len(held), held[0].Hash.String(), held[0].Height)

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
// It never runs in a cycle where Phase 1 failed: the next block notification retries
// both, and teranode_pruner_preserve_parents_consecutive_failures counts the streak.
//
// CATCHUP SKIP MODE:
// When SkipDuringCatchup is enabled (default: false), the pruner skips all operations
// in any state other than FSMStateRUNNING (CATCHINGBLOCKS, or IDLE after an operator
// STOP mid-catchup). This prevents race conditions where block
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

			// Skip all pruning until block height exceeds minimum threshold
			if s.settings.Pruner.MinBlockHeight > 0 && blockHeight <= s.settings.Pruner.MinBlockHeight {
				s.logger.Debugf("[pruner][%s:%d] skipping - block height below minimum (%d)", blockHashStr, blockHeight, s.settings.Pruner.MinBlockHeight)
				prunerSkipped.WithLabelValues("below_min_height").Inc()
				continue
			}

			// Check FSM state - skip during CATCHINGBLOCKS if configured.
			// Guard against a nil blockchainClient (e.g. in tests) the same
			// way the blockAssemblyClient check below does.
			if s.settings.Pruner.SkipDuringCatchup && s.blockchainClient != nil {
				fsmState, err := s.blockchainClient.GetFSMCurrentState(ctx)
				if err != nil {
					s.logger.Warnf("Failed to get FSM state, skipping pruner: %v", err)
					prunerSkipped.WithLabelValues("fsm_error").Inc()
					continue
				}
				// Only RUNNING proves no catchup is in flight: an operator STOP
				// parks a catching-up node in IDLE while its batch still runs.
				if fsmState == nil || *fsmState != blockchain.FSMStateRUNNING {
					s.logger.Debugf("[pruner][%s:%d] skipping during catchup", blockHashStr, blockHeight)
					prunerSkipped.WithLabelValues("catchup_mode").Inc()
					continue
				}
			}

			// Wait for block to have mined_set=true before pruning.
			// Both trigger modes need this: OnBlockMined notifications arrive before
			// setTxMined completes, and OnBlockPersisted notifications arrive before
			// setTxMined completes because block persister doesn't check mined_set.
			// Without this wait, the pruner can delete transactions via DAH that
			// setTxMined still needs to process.
			if s.blockAssemblyClient != nil {
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

			// While block validation is repairing an overtaken block, some transactions may look
			// mined while off the longest chain, and Phase 1 cannot preserve their parents.
			// Nothing is deleted until the repair finishes: no blob deletion, which would take
			// the overtaken block's subtrees the repair reads, and no Phase 2.
			held := s.overtakenTipRepairHeld(ctx, blockHashStr, blockHeight)

			// Safety check passed - wake blob deletion worker to run concurrently with Phase 1&2.
			// It works off the blockchain service's queued blob batches, independent of parent
			// preservation, so it runs even in a cycle whose Phase 1 fails. A held cycle skips
			// it; the next notification deletes everything queued up to its height.
			if !held {
				select {
				case <-s.blobNotify:
				default:
				}
				s.blobNotify <- sig
				s.logger.Debugf("[pruner][%s:%d] notified blob deletion worker", blockHashStr, blockHeight)
			}

			prunerActive.Set(1)

			// Phase 1: Preserve parents of old unmined transactions
			// This must run before Phase 2 to protect parents from deletion
			phase1Failed := false
			if s.utxoStore != nil && !s.settings.Pruner.SkipPreserveParents {
				s.logger.Debugf("[pruner][%s:%d] phase 1: preserving parents", blockHashStr, blockHeight)
				startTime := time.Now()
				if recordsProcessed, err := utxo.PreserveParentsOfOldUnminedTransactions(
					ctx, s.utxoStore, blockHeight, blockHashStr, s.settings, s.logger,
				); err != nil {
					if ctx.Err() != nil {
						// shutting down, not a Phase 1 failure
						prunerActive.Set(0)
						return
					}

					s.logger.Errorf("[pruner][%s:%d] phase 1: failed to preserve parents, phase 2 skipped this cycle: %v", blockHashStr, blockHeight, err)
					prunerErrors.WithLabelValues("preserve_parents").Inc()
					prunerPreserveParentsFailing.Inc()
					phase1Failed = true
				} else {
					prunerPreserveParentsFailing.Set(0)
					prunerDuration.WithLabelValues("preserve_parents").Observe(time.Since(startTime).Seconds())
					if recordsProcessed > 0 {
						prunerUpdatingParents.Add(float64(recordsProcessed))
					}
				}
			} else if s.settings.Pruner.SkipPreserveParents {
				prunerPreserveParentsFailing.Set(0)
				s.logger.Infof("[pruner][%s:%d] phase 1: skipped (pruner_skipPreserveParents=true)", blockHashStr, blockHeight)
			}

			// Phase 1b: Expire old parent preservations
			// Clears PreserveUntil on preserved parents that have reached their expiry height and
			// re-stamps DeleteAtHeight ONLY when the parent is genuinely safe to prune (the eligibility
			// check lives in the store). Without this, preserved parents keep PreserveUntil set and
			// DeleteAtHeight cleared forever and are never pruned. Runs before Phase 2; any DAH it
			// re-stamps is in the future (currentHeight+retention), so nothing it touches is deleted
			// this cycle. It still runs when Phase 1 failed: it deletes nothing, and a DAH it stamps
			// on a parent that turns out to have unmined children is cleared again by the next
			// successful Phase 1 before any Phase 2 runs.
			if s.utxoStore != nil && !s.settings.Pruner.SkipProcessExpiredPreservations {
				s.logger.Debugf("[pruner][%s:%d] phase 1b: expiring old preservations", blockHashStr, blockHeight)
				startTime := time.Now()
				if err := s.utxoStore.ProcessExpiredPreservations(ctx, blockHeight); err != nil {
					s.logger.Warnf("[pruner][%s:%d] phase 1b: failed to expire preservations: %v", blockHashStr, blockHeight, err)
					prunerErrors.WithLabelValues("expire_preservations").Inc()
				} else {
					prunerDuration.WithLabelValues("expire_preservations").Observe(time.Since(startTime).Seconds())
				}
			} else if s.settings.Pruner.SkipProcessExpiredPreservations {
				s.logger.Infof("[pruner][%s:%d] phase 1b: skipped (pruner_skipProcessExpiredPreservations=true)", blockHashStr, blockHeight)
			}

			// Phase 2: DAH pruning (deletion)
			// Deletes transactions marked for deletion at or before the current height.
			// Never runs in a cycle where Phase 1 failed: a stale DeleteAtHeight on a parent of an old
			// unmined transaction would not have been cleared, so Phase 2 could delete it.
			if phase1Failed {
				s.logger.Warnf("[pruner][%s:%d] phase 2: skipped because phase 1 failed; the next block notification retries pruning", blockHashStr, blockHeight)
				prunerSkipped.WithLabelValues("preserve_parents_failed").Inc()
				prunerActive.Set(0)

				// Do not update lastProcessedHeight: the next notification must be processed
				continue
			}

			// Read again: block validation may have written a hold while phase 1 ran.
			if !held {
				held = s.overtakenTipRepairHeld(ctx, blockHashStr, blockHeight)
			}

			if held {
				prunerSkipped.WithLabelValues("overtaken_tip_repair_hold").Inc()
				prunerActive.Set(0)
				s.retryHeldHeight(sig)

				// Do not update lastProcessedHeight: the height is retried
				continue
			}

			if s.prunerService != nil {
				startTime := time.Now()

				// A hold written once deletion has started cancels it: block validation may
				// have found an overtaken block between the last check and now.
				pruneCtx, cancelPrune := context.WithCancel(ctx)
				heldDuringPrune := s.watchRepairHold(pruneCtx, cancelPrune, blockHashStr, blockHeight)
				recordsProcessed, err := s.prunerService.Prune(pruneCtx, blockHeight, blockHashStr)

				if heldDuringPrune() {
					s.logger.Warnf("[pruner][%s:%d] phase 2: cancelled, block validation started holding pruning while it ran", blockHashStr, blockHeight)
					prunerSkipped.WithLabelValues("overtaken_tip_repair_hold").Inc()
					prunerActive.Set(0)
					s.retryHeldHeight(sig)

					// Do not update lastProcessedHeight: the height is retried
					continue
				}

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
