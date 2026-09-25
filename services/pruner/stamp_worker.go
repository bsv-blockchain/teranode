package pruner

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/util/chainancestry"
	"github.com/bsv-blockchain/teranode/util/chainancestry/clientfetch"
)

// THE STAMP WORKER. A second goroutine in the pruner service, beside the pruner processor and
// never in front of it. It exists only for a UTXO store that implements pruner.Stamper, which
// today is the utxoset store: its UTXOs get their block written once, by a pass that runs when
// a 288-block window is 288 blocks deep, and this worker is what drives that pass.
//
// The store cannot see the chain. This worker holds the blockchain client, reads the best tip
// past the response cache, builds one proven chain answer per drain by walking parent links,
// and hands both to the store one transaction at a time: open the drain, then for each window
// lowest first, begin it, stamp its pages, complete it. Between every one of those store calls
// it re-reads the tip and checks that the drain's anchor is still an ancestor of it. A chain
// that only grew keeps the same answer; a chain that switched branches abandons the drain with
// the committed pages standing and no completion record, and the next wake starts over.
//
// Two things wake it. The pruner processor forwards its notification after the four start
// conditions have passed, on a channel that keeps only the latest signal. And a retry timer,
// armed whenever a drain ends with windows still stampable or fails to start for any reason
// but "nothing to stamp", runs a drain with no notification at all. That second route is what
// lets a node that has stopped accepting blocks, and so receives no notifications, still work
// off its backlog.
//
// A service with no blockchain client keeps the worker: each wake counts and stamps nothing.
// No window then gets a completion record, so no containment window drops, and the counter is
// how that shows.

// stampRetryInterval is how long after a short drain the worker tries again. A compile-time
// constant, not a setting: an operator has no information to set it by.
const stampRetryInterval = 60 * time.Second

// bestTip is one uncached read of the best block header, the three things the drain uses.
type bestTip struct {
	hash   chainhash.Hash
	height uint32
	id     uint32
}

// stampDrainReport is what one wake did. outcome is the label counted on stampDrains or
// stampWakesSkipped; residualLag is the number of windows still stampable at the last tip read
// with no completion record, which is zero when the drain caught up.
type stampDrainReport struct {
	outcome     string
	windows     int
	pages       int
	residualLag uint32
}

// rearm reports whether the retry timer should be armed after this wake. A missing client,
// a height below the configured minimum and a drain that found nothing to stamp all wait for
// a notification instead; everything else is retried.
func (r stampDrainReport) rearm() bool {
	switch r.outcome {
	case "no_chain_client", "nothing_stampable", "below_min_height":
		return false
	case "completed":
		return r.residualLag > 0
	default:
		return true
	}
}

// findStamper finds the stamp side of the UTXO store by type assertion, exactly as Init finds
// the pruner provider. A store that does not implement it gets no stamp worker.
func (s *Server) findStamper() pruner.Stamper {
	if s.utxoStore == nil {
		return nil
	}

	st, ok := s.utxoStore.(pruner.Stamper)
	if !ok {
		return nil
	}

	return st
}

// stampWorker is the goroutine. One wake is one drain, and a signal that arrives during a drain
// waits in the one-slot channel and starts the next.
func (s *Server) stampWorker(ctx context.Context) {
	s.logger.Infof("[pruner][stamp] stamp worker started; retry timer armed for %s", s.stampRetry)

	// Armed once at startup, so a node restarted while it is refusing blocks still drains.
	timer := time.NewTimer(s.stampRetry)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("[pruner][stamp] stamp worker stopping")

			return

		case sig := <-s.stampNotify:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}

			if s.runStampDrain(ctx, &sig, false).rearm() {
				timer.Reset(s.stampRetry)
			}

		case <-timer.C:
			// A notification that arrived meanwhile carries the start conditions with it, so
			// it is preferred over a drain with none.
			var report stampDrainReport

			select {
			case sig := <-s.stampNotify:
				report = s.runStampDrain(ctx, &sig, false)
			default:
				report = s.runStampDrain(ctx, nil, true)
			}

			if report.rearm() {
				timer.Reset(s.stampRetry)
			}
		}
	}
}

// runStampDrain is one wake. sig is the notification that woke the worker, or nil on a
// timer-driven wake, whose start conditions are re-checked here because nobody else has.
func (s *Server) runStampDrain(ctx context.Context, sig *pruneSignal, timerDriven bool) stampDrainReport {
	if s.blockchainClient == nil {
		stampWakesSkipped.WithLabelValues("no_chain_client").Inc()
		s.logger.Warnf("[pruner][stamp] wake with no blockchain client: nothing stamped, so no window can drop")

		return stampDrainReport{outcome: "no_chain_client"}
	}

	if timerDriven {
		stampTimerDrains.Inc()
	}

	drain, floors, err := s.stamper.OpenDrain(ctx)
	if err != nil {
		if errors.Is(err, pruner.ErrStampDrainBusy) {
			stampWakesSkipped.WithLabelValues("lock_held").Inc()
			s.logger.Infof("[pruner][stamp] wake skipped: another drain holds the session lock")

			return stampDrainReport{outcome: "lock_held"}
		}

		stampDrains.WithLabelValues("error").Inc()
		s.logger.Errorf("[pruner][stamp] open drain: %v", err)

		return stampDrainReport{outcome: "error"}
	}

	started := time.Now()
	stampDrainStarted.Set(float64(started.Unix()))

	report := s.drainWindows(ctx, drain, floors, sig, timerDriven)

	if err := drain.Close(); err != nil {
		s.logger.Errorf("[pruner][stamp] close drain: %v", err)
	}

	stampDrainStarted.Set(0)
	stampDrainDuration.Observe(time.Since(started).Seconds())
	stampWindowsPerDrain.Observe(float64(report.windows))
	stampPagesPerDrain.Observe(float64(report.pages))
	stampResidualLag.Set(float64(report.residualLag))

	switch report.outcome {
	case "completed", "nothing_stampable", "abandoned", "ancestry_rejected", "error":
		stampDrains.WithLabelValues(report.outcome).Inc()
	default:
		stampWakesSkipped.WithLabelValues(report.outcome).Inc()
	}

	s.logger.Infof("[pruner][stamp] drain %s: %d windows, %d pages, %d windows still stampable, %s", report.outcome, report.windows, report.pages, report.residualLag, time.Since(started).Round(time.Millisecond))

	return report
}

// drainWindows is the drain proper, with the drain open and the session lock held.
func (s *Server) drainWindows(ctx context.Context, drain pruner.StampDrain, floors pruner.StampFloors, sig *pruneSignal, timerDriven bool) stampDrainReport {
	width := s.stamper.WindowBlocks()
	depth := s.stamper.StampDepth()
	lo := floors.StampCompleteFloor

	// Residual lag is computed at exit from whatever tip was read last and the floors then.
	var last bestTip

	report := func(outcome string, windows, pages int) stampDrainReport {
		r := stampDrainReport{outcome: outcome, windows: windows, pages: pages}

		if outcome == "no_chain_client" || outcome == "lock_held" {
			return r
		}

		if f, err := s.stamper.Floors(ctx); err != nil {
			s.logger.Warnf("[pruner][stamp] read floors at drain exit: %v", err)
		} else {
			r.residualLag = stampableWindows(f.StampCompleteFloor, last.height, width, depth)
		}

		return r
	}

	anchor, err := s.readBestTipUncached(ctx)
	if err != nil {
		s.logger.Errorf("[pruner][stamp] read best tip: %v", err)

		return report("error", 0, 0)
	}

	last = anchor

	if timerDriven {
		if reason := s.stampStartConditions(ctx, anchor.height); reason != "" {
			return report(reason, 0, 0)
		}
	}

	if stampableWindows(lo, anchor.height, width, depth) == 0 {
		s.logger.Debugf("[pruner][stamp] nothing stampable: completion floor %d, tip %d, depth %d", lo, anchor.height, depth)

		return report("nothing_stampable", 0, 0)
	}

	if sig != nil && sig.blockHash != (chainhash.Hash{}) && sig.blockHash != anchor.hash {
		s.checkAnchorHint(ctx, sig.blockHash, anchor)
	}

	anc, err := chainancestry.Build(ctx, clientfetch.ClientFetcher{Client: s.blockchainClient}, anchor.hash, anchor.height, lo, 0)
	if err != nil {
		var rejected *chainancestry.RejectedError
		if errors.As(err, &rejected) {
			stampAncestryRejected.WithLabelValues(rejected.Check).Inc()
			s.logger.Warnf("[pruner][stamp] ancestry over [%d, %d] rejected: %v", lo, anchor.height, err)

			return report("ancestry_rejected", 0, 0)
		}

		s.logger.Errorf("[pruner][stamp] build ancestry over [%d, %d]: %v", lo, anchor.height, err)

		return report("error", 0, 0)
	}

	windows, pages := 0, 0
	wLo := lo

	// fresh re-reads the tip and answers whether the drain's anchor is still on the best chain.
	fresh := func() bool {
		tip, ok := s.stampDrainFresh(ctx, anchor)
		if ok {
			last = tip
		}

		return ok
	}

	abandon := func() stampDrainReport {
		stampDrainsAbandoned.Inc()
		s.logger.Warnf("[pruner][stamp] drain abandoned at window %d after %d windows and %d pages: the best chain left anchor %s at %d", wLo, windows, pages, anchor.hash, anchor.height)

		return report("abandoned", windows, pages)
	}

	for {
		if !fresh() {
			return abandon()
		}

		state, err := drain.BeginWindow(ctx, wLo, anc)
		if err != nil {
			s.logger.Errorf("[pruner][stamp] begin window %d: %v", wLo, err)

			return report("error", windows, pages)
		}

		switch state {
		case pruner.StampWindowNotDeep:
			return report("completed", windows, pages)
		case pruner.StampWindowSkipped:
			wLo += width

			continue
		case pruner.StampWindowReady, pruner.StampWindowResumed:
		}

		for page := 0; page < drain.PagesPerWindow(); page++ {
			if !fresh() {
				return abandon()
			}

			if _, err := drain.StampPage(ctx, wLo, anc, page); err != nil {
				s.logger.Errorf("[pruner][stamp] window %d page %d: %v", wLo, page, err)

				return report("error", windows, pages)
			}

			pages++
		}

		// The tip read by this check is the live tip the completion record is judged against.
		if !fresh() {
			return abandon()
		}

		if err := drain.CompleteWindow(ctx, wLo, anc, last.height); err != nil {
			s.logger.Errorf("[pruner][stamp] complete window %d: %v", wLo, err)

			return report("error", windows, pages)
		}

		windows++
		wLo += width
	}
}

// stampableWindows counts the windows from completeFloor upward whose last block is at least
// depth below tip: the windows a drain at tip could stamp.
func stampableWindows(completeFloor, tip, width, depth uint32) uint32 {
	if tip < width-1+depth {
		return 0
	}

	highest := (tip - (width - 1) - depth) / width * width
	if highest < completeFloor {
		return 0
	}

	return (highest-completeFloor)/width + 1
}

// readBestTipUncached reads the best header past the blockchain store's response cache.
func (s *Server) readBestTipUncached(ctx context.Context) (bestTip, error) {
	header, meta, err := s.blockchainClient.GetBestBlockHeaderUncached(ctx)
	if err != nil {
		return bestTip{}, err
	}

	if header == nil || meta == nil {
		return bestTip{}, errors.NewProcessingError("[pruner][stamp] uncached best header came back nil")
	}

	return bestTip{hash: *header.Hash(), height: meta.Height, id: meta.ID}, nil
}

// stampDrainFresh is the freshness check. If the tip's hash is the anchor's, the chain has not
// moved. If it differs, the anchor must be an ancestor of the new tip, which means the chain
// only grew and the ancestry still holds. Anything else, including an error from either read,
// is a no: nothing is written on a no, so that direction is the safe one.
func (s *Server) stampDrainFresh(ctx context.Context, anchor bestTip) (bestTip, bool) {
	tip, err := s.readBestTipUncached(ctx)
	if err != nil {
		s.logger.Warnf("[pruner][stamp] freshness check: read best tip: %v", err)

		return bestTip{}, false
	}

	if tip.hash == anchor.hash {
		return tip, true
	}

	isAncestor, err := s.blockchainClient.CheckBlockIsAncestorOfBlock(ctx, []uint32{anchor.id}, &tip.hash)
	if err != nil {
		s.logger.Warnf("[pruner][stamp] freshness check: is anchor %d an ancestor of %s: %v", anchor.id, tip.hash, err)

		return tip, false
	}

	return tip, isAncestor
}

// checkAnchorHint counts a notification whose block is neither the tip nor an ancestor of it:
// the notification described a block that is no longer on the best chain. The anchor of record
// is the tip the drain read itself, so the hint changes nothing else.
func (s *Server) checkAnchorHint(ctx context.Context, hint chainhash.Hash, anchor bestTip) {
	_, meta, err := s.blockchainClient.GetBlockHeader(ctx, &hint)
	if err == nil && meta != nil {
		var isAncestor bool

		isAncestor, err = s.blockchainClient.CheckBlockIsAncestorOfBlock(ctx, []uint32{meta.ID}, &anchor.hash)
		if err == nil && isAncestor {
			return
		}
	}

	stampStaleAnchorHints.Inc()
	s.logger.Warnf("[pruner][stamp] notification block %s is not on the best chain below tip %s at %d (%v)", hint, anchor.hash, anchor.height, err)
}

// stampStartConditions re-runs, for a timer-driven drain, the start conditions a
// notification-driven drain got from the pruner processor. It returns the skip reason, or ""
// to proceed. The mined-status wait is not re-run, because there is no notified block to wait
// on; the store checks the window's own mined flags inside every pass.
func (s *Server) stampStartConditions(ctx context.Context, height uint32) string {
	if s.settings.Pruner.MinBlockHeight > 0 && height <= s.settings.Pruner.MinBlockHeight {
		return "below_min_height"
	}

	if s.settings.Pruner.SkipDuringCatchup {
		fsmState, err := s.blockchainClient.GetFSMCurrentState(ctx)
		if err != nil {
			s.logger.Warnf("[pruner][stamp] timer-driven drain: get FSM state: %v", err)

			return "fsm_error"
		}

		if fsmState != nil && *fsmState == blockchain.FSMStateCATCHINGBLOCKS {
			return "catchup_mode"
		}
	}

	if !s.checkBlockAssemblySafeForPruner(ctx, "stamp", height) {
		return "block_assembly_timeout"
	}

	return ""
}
