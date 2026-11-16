package sql

import (
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
)

const (
	// onMainChainRepairInitialBackoff is the wait before the second repair attempt. The
	// first attempt runs as soon as the failure is recorded.
	onMainChainRepairInitialBackoff = time.Second
	// onMainChainRepairMaxBackoff caps the doubling wait between attempts.
	onMainChainRepairMaxBackoff = 30 * time.Second
	// noRepairFloor marks a record with no known lowest stale height.
	noRepairFloor int64 = -1
	// startupWatchLockWait bounds how long the startup goroutine waits for slowPathMu to
	// read whether a chain write overlapped its rebuild. It holds New's guard count while it
	// waits, and slowPathMu can be held for up to 30 min by an operator rebuild, so it does
	// not wait for it. Past the bound it records the overlap it cannot rule out instead.
	startupWatchLockWait = 10 * time.Second
)

// onMainChainRepairState records a failed on_main_chain write that the running process
// still owes a repair for.
//
// Four paths write on_main_chain and then swallow a failure of the write that was meant
// to finish the job: InvalidateBlock and RevalidateBlock commit their UPDATE and then log
// a failed full rebuild, the startup goroutine logs a failed rebuild, and StoreBlock's
// slow path logs a failed sibling-fork clear or cannot classify the block it just wrote.
// Before this record existed, the guard dropped with the call, every flag reader went
// back to trusting the column, and the flags stayed wrong until a restart or the next
// operator action.
//
// While a repair is owed, mainChainRebuilding carries one extra count on this record's
// behalf, so every flag reader keeps taking its flag-free walk, and the repair loop
// retries rebuildOnMainChainFlag with backoff until an attempt lands. The count is
// released by whichever repair lands first: the loop's own attempt, or a full rebuild a
// later operator action runs successfully while no loop attempt is in flight.
type onMainChainRepairState struct {
	mu sync.Mutex
	// pending is true while a repair is owed.
	pending bool
	// full is true when the owed repair must walk to genesis: the failed write was one
	// whose fork point can lie arbitrarily deep, so the bounded window is not enough.
	full bool
	// floor is the lowest height a bounded repair must reach, recorded by the marks and
	// kept across retries. A bounded walk's own window is measured from the tip at the
	// time of the attempt, and blocks keep arriving while the repair keeps failing, so
	// without the floor the tip could carry the window past the rows that need the
	// repair. noRepairFloor when no bounded mark is owed.
	floor int64
	// generation counts marks. The loop compares it before and after an attempt, so a
	// failure recorded while the attempt ran is not cleared by that attempt's success.
	generation uint64
	// attempts counts failed loop attempts since the repair became owed.
	attempts int
	// fullGeneration is the generation of the latest mark that owed a full walk. An
	// overlapped full attempt narrows the record to a bounded walk only when no such mark
	// arrived while it ran.
	fullGeneration uint64
	// watches are the open chain-write watches; see watchOnMainChainWrites.
	watches map[*chainWriteWatch]struct{}
	// startupLockWait is startupWatchLockWait; tests shorten it.
	startupLockWait time.Duration
	// running is true while the loop has an attempt in flight. A rebuild outside the loop
	// does not release the record then, because the attempt can commit after it from an
	// older snapshot; the loop decides once the attempt has finished.
	running bool
	// backoff is the wait before the second attempt; it doubles from there. Tests
	// shorten it.
	backoff time.Duration
	// wake carries one signal per mark to the repair loop. It is buffered, so a mark
	// recorded before the loop is listening is not lost.
	wake chan struct{}
	// stopped is closed when the repair loop has exited.
	stopped chan struct{}
}

// markOnMainChainDirty records that a write to on_main_chain failed after an earlier
// write in the same path committed, so the column may no longer describe the best chain.
//
// full says whether the owed repair must walk to genesis. floor is the lowest height
// whose flag the failed write may have left wrong; a bounded mark that cannot name one
// (noRepairFloor) is owed a full walk, because no bounded window can be shown to cover
// it. path labels the failure counter. The guard is raised on the transition from clean
// to owed and held until the repair lands, so the caller's own bracketing of
// mainChainRebuilding, if any, can be released as before.
//
// A failure while the store is shutting down is not recorded: it is almost always the
// shutdown cancelling the write, and the record would die with the process anyway.
func (s *SQL) markOnMainChainDirty(full bool, floor int64, path string, cause error) {
	initPrometheusMetrics()

	r := &s.onMainChainRepair

	r.mu.Lock()

	// Checked under r.mu: the loop releases its gauge under r.mu, and only after Close has
	// signalled, so a mark either sees the store closing here or is seen by that release.
	if s.closing() {
		r.mu.Unlock()

		s.logger.Infof("on_main_chain: %s did not finish because the store is shutting down: %v", path, cause)

		return
	}

	if !r.pending {
		r.pending = true
		r.attempts = 0
		r.floor = noRepairFloor

		s.mainChainRebuilding.Add(1)
		prometheusOnMainChainRepairPending.Inc()
	}

	if !full && floor == noRepairFloor {
		full = true
	}

	r.full = r.full || full

	if !full && (r.floor == noRepairFloor || floor < r.floor) {
		r.floor = floor
	}

	r.generation++

	if full {
		r.fullGeneration = r.generation
	}

	r.mu.Unlock()

	// GetBlockByHeight and others answer from the response cache before they consult the
	// guard, so an answer cached from the stale column between the failed write and now
	// would otherwise outlive the mark.
	s.ResetResponseCache()

	prometheusOnMainChainWriteFailures.WithLabelValues(path).Inc()
	s.logger.Errorf("on_main_chain: %s left the column inconsistent, flag readers take the flag-free walk until the background repair lands: %v", path, cause)

	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// onMainChainRepaired tells the record that a rebuild has just landed outside the loop.
// A full walk satisfies any owed repair, because it leaves every flag right. A bounded
// walk satisfies only an owed bounded repair.
//
// While a loop attempt is running the record is left alone. That attempt may commit after
// this rebuild from a snapshot older than it, so only the loop can tell whether the flags
// are right once it has finished. The callers note themselves with noteOnMainChainWrite,
// so the loop sees the overlap and runs again.
func (s *SQL) onMainChainRepaired(full bool) {
	r := &s.onMainChainRepair

	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.pending || r.running || (r.full && !full) {
		return
	}

	s.clearOnMainChainRepairLocked("a later rebuild")
}

// chainWriteWatch collects the chain writes that commit while a walk without slowPathMu
// runs. See watchOnMainChainWrites.
type chainWriteWatch struct {
	// moved is true once a write has been noted.
	moved bool
	// floor is the lowest height any noted write gave, or noRepairFloor when one of them
	// could not name one.
	floor int64
}

// watchOnMainChainWrites opens a watch for a walk about to run without slowPathMu.
//
// Such a walk reads its rows from a snapshot taken when it began. A chain write that
// commits during the walk can touch only rows the walk does not, so REPEATABLE READ aborts
// neither, and the walk can then commit its older view over the write. The owner opens a
// watch before the walk and closes it under slowPathMu after the walk has committed, so
// every write that committed in between has been noted on it.
//
// Each write notes the lowest height at which it can have moved the best chain. Every row
// below every noted height has the same membership on the chain the walk saw as on the
// chain after the writes, so the walk left it right; only rows at or above the lowest noted
// height can be wrong, and a bounded walk down to it repairs them.
func (s *SQL) watchOnMainChainWrites() *chainWriteWatch {
	r := &s.onMainChainRepair

	w := &chainWriteWatch{floor: noRepairFloor}

	r.mu.Lock()
	if r.watches == nil {
		r.watches = make(map[*chainWriteWatch]struct{})
	}

	r.watches[w] = struct{}{}
	r.mu.Unlock()

	return w
}

// noteOnMainChainWrite records a committed slow-path write that moved chain membership on
// every open watch. floor is the lowest height at which the write can have changed the
// best chain, or noRepairFloor when the write cannot bound it.
//
// The caller holds slowPathMu and calls it after its last commit and before it releases
// the lock, so a watch closed under slowPathMu once its walk has committed has seen every
// write that could have landed during the walk. A write that failed before it committed
// is not noted: it moved nothing, and noting it would make a walk run again for nothing.
func (s *SQL) noteOnMainChainWrite(floor int64) {
	r := &s.onMainChainRepair

	r.mu.Lock()
	defer r.mu.Unlock()

	for w := range r.watches {
		if !w.moved {
			w.moved = true
			w.floor = floor
		} else if w.floor != noRepairFloor && (floor == noRepairFloor || floor < w.floor) {
			w.floor = floor
		}
	}
}

// closeOnMainChainWatch closes w and reports whether a chain write was noted on it and
// the lowest height any of them gave. It takes slowPathMu for the read, so no write sits
// between its commit and its note.
func (s *SQL) closeOnMainChainWatch(w *chainWriteWatch) (moved bool, floor int64) {
	s.slowPathMu.Lock()
	defer s.slowPathMu.Unlock()

	return s.dropOnMainChainWatch(w)
}

// closeOnMainChainWatchWithin is closeOnMainChainWatch that waits at most wait for
// slowPathMu. ok is false when it could not take the lock in time; the watch is closed
// either way, and the caller must then assume a write it cannot bound overlapped.
func (s *SQL) closeOnMainChainWatchWithin(w *chainWriteWatch, wait time.Duration) (moved bool, floor int64, ok bool) {
	deadline := time.Now().Add(wait)

	for !s.slowPathMu.TryLock() {
		if s.closing() || time.Now().After(deadline) {
			s.dropOnMainChainWatch(w)

			return true, noRepairFloor, false
		}

		time.Sleep(5 * time.Millisecond)
	}

	defer s.slowPathMu.Unlock()

	moved, floor = s.dropOnMainChainWatch(w)

	return moved, floor, true
}

// dropOnMainChainWatch closes w without taking slowPathMu, for a walk whose result will
// not be trusted anyway, and returns what was noted on it.
func (s *SQL) dropOnMainChainWatch(w *chainWriteWatch) (moved bool, floor int64) {
	r := &s.onMainChainRepair

	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.watches, w)

	return w.moved, w.floor
}

// lowerRepairFloor returns the lower of two floors, where noRepairFloor means none.
func lowerRepairFloor(a, b int64) int64 {
	if a == noRepairFloor || (b != noRepairFloor && b < a) {
		return b
	}

	return a
}

// clearOnMainChainRepairLocked releases the record. The caller holds r.mu.
func (s *SQL) clearOnMainChainRepairLocked(how string) {
	initPrometheusMetrics()

	r := &s.onMainChainRepair

	attempts := r.attempts
	r.pending = false
	r.full = false
	r.floor = noRepairFloor
	r.attempts = 0

	s.mainChainRebuilding.Add(-1)
	prometheusOnMainChainRepairPending.Dec()

	s.logger.Infof("on_main_chain repair landed by %s after %d failed background attempt(s), flag readers are back on the column", how, attempts)
}

// onMainChainRepairLoop waits for a mark and then retries the owed repair until it lands
// or the store shuts down. Started once by New.
func (s *SQL) onMainChainRepairLoop() {
	r := &s.onMainChainRepair

	defer close(r.stopped)
	defer s.releaseOnMainChainRepairGauge()

	for {
		select {
		case <-s.backgroundDone:
			return
		case <-r.wake:
		}

		if !s.repairOnMainChainUntilClean() {
			return
		}
	}
}

// repairOnMainChainUntilClean runs repair attempts until the record is clear. A failed
// attempt, and a full one overlapped by a write that named no height, wait before the
// next, starting at the record's backoff and doubling to onMainChainRepairMaxBackoff. It
// returns false when the store shut down first.
func (s *SQL) repairOnMainChainUntilClean() bool {
	r := &s.onMainChainRepair

	r.mu.Lock()
	wait := r.backoff
	r.mu.Unlock()

	for {
		outcome, err := s.attemptOnMainChainRepair()

		switch outcome {
		case repairLanded:
			return true
		case repairRerun:
			continue
		case repairOverlapped:
			s.logger.Infof("on_main_chain repair overlapped an operator chain write, running the full walk again in %s", wait)
		case repairFailed:
			s.logger.Warnf("on_main_chain repair failed, next attempt in %s: %v", wait, err)
		}

		select {
		case <-s.backgroundDone:
			return false
		case <-time.After(wait):
		}

		wait *= 2
		if wait > onMainChainRepairMaxBackoff {
			wait = onMainChainRepairMaxBackoff
		}
	}
}

// repairOutcome is what one call of attemptOnMainChainRepair came to.
type repairOutcome int

const (
	// repairLanded: the record is clear, by this attempt or by a rebuild outside the loop.
	repairLanded repairOutcome = iota
	// repairRerun: set the attempt up again at once. A mark recorded while it ran, or while
	// it waited for slowPathMu, changed what it owes, or an overlapped full walk narrowed
	// the record to a bounded walk.
	repairRerun
	// repairOverlapped: the walk landed without slowPathMu and a chain write that could not
	// bound its reach committed during it, so the walk may have written an older view over
	// that write and must run again in full.
	repairOverlapped
	// repairFailed: the walk returned an error.
	repairFailed
)

// attemptOnMainChainRepair runs one repair attempt.
//
// A bounded attempt runs under slowPathMu, the lock the StoreBlock paths that owed it
// held, so no slow-path writer interleaves with a walk over the rows at the tip. It is
// short: the window is the tip's plus the recorded floor, under a 30 s budget.
//
// On PostgreSQL a full attempt never takes slowPathMu. It can run for up to 30 min, and
// while the database stays slow it is retried every 30 s, so holding the lock would stop
// block storage for as long as the fault lasts. Readers are on their flag-free walk
// because of the guard, and rows a concurrent StoreBlock inserts are flagged by that
// StoreBlock. A chain write that commits during the attempt can leave the attempt's older
// view in place (see watchOnMainChainWrites), so the attempt watches for chain writes and
// reports an overlap when one was noted. When every noted write named the lowest height it
// can have moved, and no mark owing a full walk arrived meanwhile, the walk left every row
// below that height right, and the record is narrowed to a bounded walk down to it, which
// runs next under slowPathMu like any bounded attempt and which no write can overlap. When
// one of them could not name a height, which is InvalidateBlock and RevalidateBlock, the
// full walk runs again, unlocked, after the backoff. A steady stream of such operator
// writes can keep the repair owed; readers stay on the flag-free walk meanwhile, which
// costs speed, not correctness.
//
// On SQLite a full attempt does take slowPathMu. SQLite lets one writer in at a time, so a
// long walk would make each StoreBlock that needs to write fail with SQLITE_BUSY after the
// 5 s busy timeout; waiting on the lock delays block storage without failing it. Every
// SQLite attempt is therefore locked, and none can be overlapped.
//
// A locked attempt reads the record only once it holds the lock. A chain write it queued
// behind has finished by then and cannot overlap it, and a rebuild outside the loop that
// landed while it waited has already released the record.
func (s *SQL) attemptOnMainChainRepair() (repairOutcome, error) {
	r := &s.onMainChainRepair

	r.mu.Lock()

	if !r.pending {
		r.mu.Unlock()

		return repairLanded, nil
	}

	full := r.full
	locked := s.onMainChainRepairNeedsLockLocked()

	r.mu.Unlock()

	if locked {
		s.slowPathMu.Lock()
		defer s.slowPathMu.Unlock()
	}

	r.mu.Lock()

	if !r.pending {
		r.mu.Unlock()

		return repairLanded, nil
	}

	if r.full != full {
		// A mark while this attempt waited for the lock changed the walk it owes.
		r.mu.Unlock()

		return repairRerun, nil
	}

	generation := r.generation
	floor := r.floor
	attempt := r.attempts + 1
	r.running = true

	r.mu.Unlock()

	var watch *chainWriteWatch
	if !locked {
		watch = s.watchOnMainChainWrites()
	}

	err := s.runOnMainChainRepair(full, floor)

	overlapped := false
	writeFloor := noRepairFloor

	if watch != nil {
		if err == nil {
			overlapped, writeFloor = s.closeOnMainChainWatch(watch)
		} else {
			s.dropOnMainChainWatch(watch)
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.running = false

	if err != nil {
		r.attempts++

		return repairFailed, errors.NewProcessingError("attempt %d (full walk %v, floor %d, holding slowPathMu %v)", attempt, full, floor, locked, err)
	}

	// pending is still true here. onMainChainRepaired leaves the record alone while
	// running is set, and its callers cannot run at all during a locked attempt.
	if overlapped {
		if writeFloor != noRepairFloor && r.fullGeneration <= generation {
			// The full walk left every row below writeFloor right. Owe only the rows at or
			// above it, together with any bounded floor a mark recorded meanwhile.
			r.full = false
			r.floor = lowerRepairFloor(r.floor, writeFloor)

			return repairRerun, nil
		}

		return repairOverlapped, nil
	}

	if r.generation != generation {
		// A new failure was recorded while this attempt ran, so the flags are not known to
		// be right.
		return repairRerun, nil
	}

	s.clearOnMainChainRepairLocked("the background repair")

	return repairLanded, nil
}

// onMainChainRepairNeedsLockLocked reports whether the next attempt must hold slowPathMu:
// every bounded attempt, and a full one unless the engine is PostgreSQL. The caller holds
// r.mu.
func (s *SQL) onMainChainRepairNeedsLockLocked() bool {
	return !s.onMainChainRepair.full || !s.unlockedFullRepair
}

// runOnMainChainRepair runs the walk of one attempt; attemptOnMainChainRepair decides
// whether it holds slowPathMu. rebuildOnMainChainFlag raises the guard itself for its
// duration, on top of the count this record holds.
func (s *SQL) runOnMainChainRepair(full bool, floor int64) error {
	timeout := rebuildOffChainSetTimeout
	if full {
		timeout = migrationFullRebuildTimeout
	}

	ctx, cancel := s.shutdownAwareContext(timeout)
	defer cancel()

	if err := s.rebuildOnMainChainFlagFrom(ctx, full, floor); err != nil {
		return err
	}

	// Drop any answer a reader cached from the column between the failed write and the
	// mark. Everything cached since came from a flag-free walk and is still right.
	s.ResetResponseCache()

	return nil
}

// releaseOnMainChainRepairGauge drops this store's contribution to the pending gauge when
// its repair loop exits with a repair still owed, so a closed store does not hold the
// process-wide gauge up for the stores that outlive it.
func (s *SQL) releaseOnMainChainRepairGauge() {
	r := &s.onMainChainRepair

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.pending {
		// Nothing can land the repair now, so stop a later acknowledgment from releasing
		// the gauge a second time. The guard stays up: the store is closed.
		r.pending = false
		prometheusOnMainChainRepairPending.Dec()
	}
}

// closing reports whether Close has signalled the store's background work to stop.
func (s *SQL) closing() bool {
	select {
	case <-s.backgroundDone:
		return true
	default:
		return false
	}
}
