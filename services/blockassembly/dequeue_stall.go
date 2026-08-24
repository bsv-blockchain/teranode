package blockassembly

import "time"

// dequeueStallThreshold is the hard-coded 30s bound beyond which a consumer
// that has not reached its dequeue branch is worth reporting - see issue
// #1429. Deliberately not a setting: nobody can yet justify a different
// number, and the severity split below means the cases where 30s is arguably
// routine (a large moveForwardBlock, say) report at info rather than paging
// anyone. If the staleness gauge this ships alongside shows warnings on
// ordinary blocks, that is the evidence for making it configurable; guessing
// at a number now is what the gauge exists to avoid.
const dequeueStallThreshold = 30 * time.Second

// dequeueStallWarnRepeat bounds how often the "consumer stalled" warning
// repeats while the condition persists. Chosen so an operator sees it promptly
// but is not paged every 5 seconds for the duration of an incident that may run
// for many minutes; 2 minutes still gives ~15+ log lines across the 35-minute
// incident on record (see dequeueDuringBlockMovement's docstring) without
// flooding logs.
const dequeueStallWarnRepeat = 2 * time.Minute

// dequeueStallEvent is what an observation tells the caller to log, if
// anything.
type dequeueStallEvent int

const (
	// dequeueStallNone means nothing to report on this tick.
	dequeueStallNone dequeueStallEvent = iota
	// dequeueStallBegan is the rising edge: log immediately, every time.
	dequeueStallBegan
	// dequeueStallContinues is the reduced-cadence repeat while stalled.
	dequeueStallContinues
	// dequeueStallEnded means the consumer genuinely resumed.
	dequeueStallEnded
)

// dequeueStallState is the state carried between observations. Its zero value
// is the correct starting state: not stalled, nothing warned yet.
type dequeueStallState struct {
	stalled bool
	// beforeConsumerStarted records that this incident is still explained by the
	// consumer not existing yet. It only affects how the incident is reported: a
	// gap that opens before the consumer exists is startup, not a wedge, and its
	// end is the consumer arriving rather than recovering.
	//
	// It has to be carried rather than read live at the closing edge, because by
	// the time an incident ends the consumer has necessarily started -
	// SubtreeProcessor.Start re-seeds the dequeue timestamp, which is what ends
	// a startup gap in the first place - so the live reading can no longer tell
	// startup finishing from a wedge recovering.
	//
	// Set on the rising edge and cleared again if a later tick finds the
	// consumer started while the stall persists, which means startup gave way to
	// a genuine wedge. Without that clearing, a consumer that starts into the
	// backlog the reload accumulated and immediately jams would warn correctly
	// throughout and then close with "consumer started", crediting the startup
	// sequence for a wedge.
	beforeConsumerStarted bool
	// sawQueuedWork records whether the queue has been non-empty at any point
	// during this incident. It decides how loudly the caller reports and
	// nothing else - see the entry condition below for why depth must not
	// decide whether there is an incident at all.
	//
	// It is latched over the whole incident rather than read per tick for the
	// same reason the entry condition ignores depth: a single sample is not
	// trustworthy. reorgBlocks and moveForwardBlock drain the queue from inside
	// the branch that is blocking the consumer, so the tick that lands after
	// that drain reads zero in the middle of the exact failure this exists for.
	// Latching means severity only ever escalates: once work has been seen
	// stacking up behind a wedged consumer, the incident stays a warning even
	// if a later tick happens to catch the queue empty.
	sawQueuedWork bool
	// stalledSince is when the consumer stopped, not when we noticed. Set
	// once on the rising edge and never touched again until recovery, so one
	// incident is reported as one incident.
	stalledSince time.Time
	// lastWarn is when a warning was last emitted, for the repeat cadence.
	lastWarn time.Time
}

// observeDequeueStall folds one observation of the intake queue into the stall
// state and reports what, if anything, the operator should be told. It is pure:
// every input including the clock is a parameter, so the whole state machine is
// table-testable without waiting on real tick intervals.
//
// Both edges key on staleness alone, and neither looks at queue depth:
//
//   - A stall BEGINS when the consumer has not touched the dequeue branch for
//     dequeueStallThreshold, whatever the queue currently holds. Gating entry
//     on a non-empty queue looks right - issue #1429 is about intake growing
//     without bound - but depth is the untrustworthy half of the pair, on the
//     way in as much as on the way out. In the incident on record the blocked
//     handler was draining the queue from inside the branch that blocked it,
//     so a tick landing on a low reading would have declined to open an
//     incident that was already 35 minutes old. Depth decides how loudly the
//     caller reports - via sawQueuedWork, latched across the incident rather
//     than sampled - and not whether there is anything to report.
//
//   - consumerStarted does not change WHETHER an incident is reported, only how.
//     Both conditions are worth a line - a queue nobody is draining is worth
//     knowing about either way, and a loadUnminedTransactions that never returns
//     is a failure mode on record - but they have different causes and different
//     severities, so the caller is told which one it is rather than left to
//     guess between them. Suppressing the pre-start case outright would hide a
//     stuck unmined reload, which is the one failure the startup window has.
//
//   - A stall ENDS only when staleness drops back to the threshold. Keying the
//     exit on depth instead looks equivalent but is
//     not: reorgBlocks and moveForwardBlock drain the queue from inside the
//     very select branches that stop the consumer dequeuing, via
//     dequeueDuringBlockMovement, which does not stamp lastDequeueMillis. A
//     long handler therefore empties its own queue partway through and would
//     otherwise be reported as recovered while still parked, with the staleness
//     gauge still climbing - a false all-clear on the exact signal this
//     machinery exists to provide. The same applies at startup, where
//     BlockAssembler.Start calls DrainQueue before SubtreeProcessor.Start
//     re-seeds the timestamp.
//
// Staleness is the trustworthy half because the consumer stamps on every loop
// iteration, including iterations where the queue is empty and it dequeues
// nothing. Low staleness therefore means the consumer really is running.
//
// Returns the next state, the event to log, and - for dequeueStallEnded only -
// how long the incident lasted end to end.
func observeDequeueStall(state dequeueStallState, now time.Time, queueLength int64, staleness time.Duration, consumerStarted bool) (dequeueStallState, dequeueStallEvent, time.Duration) {
	if !state.stalled {
		if staleness > dequeueStallThreshold {
			return dequeueStallState{
				stalled:               true,
				beforeConsumerStarted: !consumerStarted,
				sawQueuedWork:         queueLength > 0,
				// Backdate to the last dequeue rather than to detection: the
				// stall began when the consumer stopped, which is at least
				// dequeueStallThreshold plus up to one tick ago. lastWarn
				// must stay at now, or a stall already older than
				// dequeueStallWarnRepeat would repeat on the very next tick.
				stalledSince: now.Add(-staleness),
				lastWarn:     now,
			}, dequeueStallBegan, 0
		}

		return state, dequeueStallNone, 0
	}

	if staleness <= dequeueStallThreshold {
		// End the incident when the consumer resumed, not when this tick
		// noticed - the same correction stalledSince applies to the start.
		// Recovery is only visible once staleness has fallen back to the
		// threshold, so by now the consumer has been running again for
		// staleness, which is up to dequeueStallThreshold plus a tick. Timing
		// to now would add that to every reported duration. The result cannot
		// go negative: staleness here is at most the threshold, and the
		// staleness that opened the incident exceeded it.
		return dequeueStallState{}, dequeueStallEnded, now.Add(-staleness).Sub(state.stalledSince)
	}

	// Still stalled, and the consumer has started since the incident opened.
	// That reclassifies it: what looked like startup is now a consumer that
	// exists and is not dequeuing, so the closing line must not credit the
	// startup sequence for a wedge. Clearing here rather than at the closing
	// edge is what makes it safe - an ordinary startup gap never reaches this
	// point, because SubtreeProcessor.Start re-seeds the dequeue timestamp in
	// the same breath as setting the flag, so the tick that first sees the flag
	// true also sees low staleness and takes the ended branch above. Reaching
	// here with the flag true therefore means the consumer started and then
	// failed to dequeue for the whole threshold, which is a wedge.
	if consumerStarted {
		state.beforeConsumerStarted = false
	}

	// Escalate-only: see sawQueuedWork's docstring for why a later empty
	// reading must not walk this back.
	if queueLength > 0 {
		state.sawQueuedWork = true
	}

	if now.Sub(state.lastWarn) >= dequeueStallWarnRepeat {
		state.lastWarn = now
		return state, dequeueStallContinues, 0
	}

	return state, dequeueStallNone, 0
}
