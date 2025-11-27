package pruner

import (
	"context"
	"time"

	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// checkBlockAssemblySafeForPruner verifies that block assembly is in "running" state
// and safe to proceed with pruner operations. Returns true if safe, false otherwise.
func (s *Server) checkBlockAssemblySafeForPruner(ctx context.Context, phase string, height uint32) bool {
	state, err := s.blockAssemblyClient.GetBlockAssemblyState(ctx)
	if err != nil {
		s.logger.Errorf("Failed to get block assembly state before %s: %v", phase, err)
		prunerErrors.WithLabelValues("state_check").Inc()
		return false
	}

	if state.BlockAssemblyState != "running" {
		s.logger.Infof("Skipping %s for height %d: block assembly state is %s (not running)", phase, height, state.BlockAssemblyState)
		prunerSkipped.WithLabelValues("not_running").Inc()
		return false
	}

	return true
}

// prunerProcessor processes pruner requests from the pruner channel.
// It drains the channel to get the latest height (deduplication), then performs
// pruner in two sequential steps:
// 1. Preserve parents of old unmined transactions
// 2. Delete-at-height (DAH) pruner
//
// Safety checks (block assembly state) are performed immediately before each phase
// to prevent race conditions where state could change between queueing and execution.
//
// This goroutine ensures only one pruner operation runs at a time by processing
// from a buffered channel (size 1).
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

			// Safety check before preserve parents phase
			if !s.checkBlockAssemblySafeForPruner(ctx, "preserve parents", latestHeight) {
				continue
			}

			// Step 1: Preserve parents of old unmined transactions FIRST
			// This ensures parents of old unmined transactions are not deleted by DAH pruner
			// CRITICAL: If this phase fails, we MUST NOT proceed to subsequent phases,
			// as DAH pruner could delete parents that should be preserved.
			s.logger.Infof("Starting pruner for height %d: preserving parents", latestHeight)
			startTime := time.Now()

			if s.utxoStore != nil {
				_, err := utxo.PreserveParentsOfOldUnminedTransactions(
					ctx, s.utxoStore, latestHeight, s.settings, s.logger)
				if err != nil {
					s.logger.Errorf("CRITICAL: Failed to preserve parents at height %d, ABORTING pruner to prevent data loss: %v", latestHeight, err)
					prunerErrors.WithLabelValues("preserve_parents_failed").Inc()
					prunerSkipped.WithLabelValues("preserve_failed").Inc()
					// ABORT: Do not proceed to Phase 2 (DAH pruner) - could cause data loss
					continue
				}
				prunerDuration.WithLabelValues("preserve_parents").Observe(time.Since(startTime).Seconds())
			}

			// Step 2: Then trigger DAH pruner and WAIT for it to complete
			// DAH pruner deletes transactions marked for deletion at or before the current height
			if s.prunerService != nil {
				s.logger.Infof("Starting pruner for height %d: DAH pruner", latestHeight)
				startTime = time.Now()
				doneCh := make(chan string, 1)

				if err := s.prunerService.UpdateBlockHeight(latestHeight, doneCh); err != nil {
					s.logger.Errorf("Pruner service error updating block height %d: %v", latestHeight, err)
					prunerErrors.WithLabelValues("dah_pruner").Inc()
					continue
				}

				// Wait for pruner to complete with timeout
				prunerTimeout := s.settings.Pruner.JobTimeout
				timeoutTimer := time.NewTimer(prunerTimeout)
				defer timeoutTimer.Stop()

				select {
				case status := <-doneCh:
					if status != "completed" {
						s.logger.Warnf("Pruner for height %d finished with status: %s", latestHeight, status)
						prunerErrors.WithLabelValues("dah_pruner").Inc()
					} else {
						s.logger.Infof("Pruner for height %d completed successfully", latestHeight)
						prunerDuration.WithLabelValues("dah_pruner").Observe(time.Since(startTime).Seconds())
						prunerProcessed.Inc()
					}
				case <-timeoutTimer.C:
					s.logger.Infof("Pruner for height %d exceeded coordinator timeout of %v - pruner continues in background, re-queuing immediately", latestHeight, prunerTimeout)
					// Note: This is not an error - the pruner job continues processing in the background.
					// The coordinator re-queues immediately to check again.
					// Very large pruners may take longer than the timeout and require multiple iterations.

					// Immediately re-queue to check again (non-blocking)
					select {
					case s.prunerCh <- latestHeight:
						s.logger.Debugf("Re-queued pruner for height %d after timeout", latestHeight)
					default:
						// Channel full, will be retried when notifications trigger again
						s.logger.Debugf("Pruner channel full, will retry on next notification")
					}
				case <-ctx.Done():
					return
				}
			}

			// Update last processed height atomically
			s.lastProcessedHeight.Store(latestHeight)
		}
	}
}
