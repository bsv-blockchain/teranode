package pruner

import (
	"context"
	"time"
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
// Delete-at-height (DAH) pruning to remove old transaction records from storage.
//
// PARENT PRESERVATION:
// Parent preservation of unmined transactions is handled in Block Assembly/Validation, not here:
// - During FSMStateRUNNING: Preserved per-block in BlockValidation after UpdateTxMinedStatus
// - During FSMStateCATCHINGBLOCKS: Preserved once at Block Assembly startup
//
// This separation keeps the pruner focused on deletion while Block Assembly manages the
// unmined transaction lifecycle (where parent preservation logically belongs).
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

			// Safety check before DAH pruning
			if !s.checkBlockAssemblySafeForPruner(ctx, "DAH pruner", latestHeight) {
				continue
			}

			// Call DAH pruner directly (synchronous)
			// DAH pruner deletes transactions marked for deletion at or before the current height
			// Parent preservation now happens in Block Assembly, not here
			if s.prunerService != nil {
				s.logger.Infof("Starting pruner for height %d: DAH pruner", latestHeight)
				startTime := time.Now()

				recordsProcessed, err := s.prunerService.Prune(ctx, latestHeight)
				if err != nil {
					s.logger.Errorf("Pruner failed for height %d: %v", latestHeight, err)
					prunerErrors.WithLabelValues("dah_pruner").Inc()
				} else {
					s.logger.Infof("Pruner for height %d completed successfully, pruned %d records", latestHeight, recordsProcessed)
					prunerDuration.WithLabelValues("dah_pruner").Observe(time.Since(startTime).Seconds())
					prunerProcessed.Inc()
				}
			}

			// Update last processed height atomically
			s.lastProcessedHeight.Store(latestHeight)
		}
	}
}
