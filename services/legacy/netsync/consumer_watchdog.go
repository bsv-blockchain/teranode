package netsync

import (
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Why this file exists.
//
// The consumer goroutine is the only thing that admits a block, and every reason
// it can stop admitting is a local variable on that goroutine or a field only it
// touches: the pending dispatch waiting for capacity, the park job it is holding
// for a worker, and the window's own frontier. None of them is reachable from any
// other goroutine, so when the loop stops making progress a goroutine profile can
// say only which line it is parked on.
//
// That is exactly what happened on mainnet on 2026-09-08 at height 754,895. The
// node committed nothing for eighteen minutes. The profile showed block validation
// entirely idle, both park workers idle, the consumer parked in its select, and
// thirteen goroutines each holding a block's byte weight against the 256 MiB
// download budget with neither a hand-off nor a reply. Two peer read loops were
// blocked inside that budget's acquire, and a blocked read loop reads nothing more
// from its socket, so the one block the chain was waiting for was requested and
// never received. Every observable was a consequence; the cause was three values
// the profile could not print.
//
// So the consumer publishes them. It writes a snapshot immediately before it
// blocks, and a watchdog on another goroutine prints that snapshot when nothing
// has been admitted for a while. Only the consumer writes and only the watchdog
// reads, through an atomic pointer, so the two never share a value.
//
// This changes no decision the loop makes. It is a diagnostic, and it stays
// cheap: one small allocation per loop iteration, which is once per block, against
// blocks that run to hundreds of megabytes.

// consumerStallReportInterval is how often a stalled consumer is described. Long
// enough that a genuinely slow block does not fill the log, short enough that a
// wedge is visible without waiting for the next sample.
const consumerStallReportInterval = 60 * time.Second

// consumerStallAfter is how long the loop may admit nothing before the watchdog
// treats the quiet as worth explaining. A single mainnet block in the 2 GB era
// takes minutes to validate, and admitting nothing while one is in flight is
// correct rather than stuck, so the report names what it is waiting for and
// leaves the judgement to the reader.
const consumerStallAfter = 90 * time.Second

// consumerWait is what the consumer last saw before it blocked. Every field
// answers one question about why the loop is not admitting anything.
type consumerWait struct {
	at time.Time

	// queueArmOpen is whether the loop was willing to take another block off the
	// queue. False is the interesting case: it means the loop had already
	// accepted work it could not place, so arriving blocks pile up behind it
	// holding their download budget.
	queueArmOpen bool

	// pendingHash and pendingHeight name the head-processed block waiting for
	// window capacity, and are empty when there is none. A pending block is what
	// closes the queue arm, so a wedge with the arm shut almost always has one.
	pendingHash   string
	pendingHeight uint32

	// pendingWindowed distinguishes the two admission rules: a windowed block
	// needs a slot and budget, an unwindowed one needs an empty frontier.
	pendingWindowed bool

	// parkJobHeld is whether the loop was holding a park job no worker had taken.
	// It closes the queue arm for the same reason the pending dispatch does.
	parkJobHeld bool

	// barrier is the checkpoint gate. Set, it refuses every block whatever the
	// frontier holds, so it is worth stating outright rather than inferring.
	barrier bool

	// frontier describes each entry the window still holds, as
	// "height:hash8 settled|running[ failed][ aborted]". An entry that is settled
	// and still here is a completion that was never processed; one that is running
	// with no worker goroutine alive is a completion that was lost.
	frontier []string

	// inflightBytes and budget are the byte half of the admission rule, so a
	// refusal for want of budget can be told apart from one for want of a slot.
	inflightBytes int64
	budget        int64

	// drainQueued is how many parents are waiting to have their parked children
	// looked at. It is the loop's second admission source, so a wedge with work
	// queued here is different from one with none.
	drainQueued int

	// parked is how many blocks are on disk waiting for a parent. A large park
	// with nothing being admitted is the shape the operator sees as "idle with
	// blocks downloaded".
	parked int

	// drainShutByWindow records that parents were queued for a drain and the
	// drain could not have its turn, because the drain only opens when the
	// dispatcher's window is empty.
	//
	// It is the difference between "nothing to commit" and "something to commit
	// and no way to commit it", and the second is the fault an operator sees as
	// a tip that will not move with blocks piled on disk. Measured on mainnet on
	// 2026-09-10: the block for the next height was on disk with its parent
	// already committed, 127 blocks stacked behind it, and the sweep offering it
	// every thirty seconds for two and a half hours.
	drainShutByWindow bool

	// downloadBudget, downloadHeld and downloadWaiters describe the byte budget
	// that admits a block off the wire, which is a DIFFERENT budget from the
	// window's and the one this report was missing.
	//
	// It is the budget that can silence every peer at once. A read-loop blocked
	// acquiring it reads nothing further from its socket, so that peer stops
	// delivering whatever it owes, and eight quiet peers look exactly like eight
	// peers with nothing to send. The wedge this whole file was written for, at
	// height 754,895 on 2026-09-08, was thirteen goroutines holding weight here
	// with two read-loops blocked in the acquire — and the report described the
	// window instead, which is empty during precisely that fault.
	//
	// downloadBudget is zero when prefetch is disabled, which is a real state
	// (synchronous ingestion) rather than a missing reading, so the report says
	// nothing at all rather than inventing a constraint of zero.
	downloadBudget  int64
	downloadHeld    int64
	downloadWaiters int64

	// drainDeclines is how many turns have been offered to the drain and given
	// back. It is the fact the first version of this report was missing: a drain
	// that walks its queue, rules every parent out and drops them leaves no
	// trace in the queue length, so a snapshot taken afterwards showed nothing
	// queued and nothing in flight, which read as a loop with no work rather
	// than a loop that had just thrown a turn away.
	drainDeclines int64
}

// publishConsumerWait records what the loop is about to block on. Called by the
// consumer only, immediately before its select, so the snapshot describes the
// wait rather than the work that preceded it.
func (sm *SyncManager) publishConsumerWait(now time.Time, queueArmOpen bool, pending *blockDispatch) {
	w := &consumerWait{
		at:           now,
		queueArmOpen: queueArmOpen,
		parkJobHeld:  sm.parkJobHeld != nil,
		drainQueued:  len(sm.drainQueue),
	}

	if pending != nil {
		w.pendingHash = shortHash(pending.msgHash().String())
		w.pendingHeight = pending.height
		w.pendingWindowed = pending.windowed
	}

	if bd := sm.dispatcher; bd != nil {
		w.drainShutByWindow = len(sm.drainQueue) > 0 && !bd.frontierEmpty()
		w.barrier = bd.barrier
		w.inflightBytes = bd.inflight
		w.budget = bd.budget
		w.frontier = make([]string, 0, len(bd.frontier))

		for _, e := range bd.frontier {
			w.frontier = append(w.frontier, describeFrontierEntry(e))
		}
	}

	if sm.blockPark != nil {
		w.parked = sm.blockPark.Len()
	}

	if sm.blockPrefetchBudget != nil {
		w.downloadBudget = sm.blockPrefetchBudgetBytes
		w.downloadHeld = sm.blockPrefetchReserved.Load()
		w.downloadWaiters = sm.blockPrefetchWaiters.Load()
	}

	w.drainDeclines = sm.drainDeclined.Load()

	sm.consumerWaitState.Store(w)
}

// describeFrontierEntry renders one window entry for the report. Reading settled
// through a non-blocking receive rather than a flag keeps the description exactly
// as truthful as the channel the dispatcher itself waits on.
func describeFrontierEntry(e *frontierEntry) string {
	if e == nil {
		return "nil"
	}

	var b strings.Builder

	b.WriteString(shortHash(e.hash.String()))
	b.WriteString(" at ")
	b.WriteString(strconv.FormatUint(uint64(e.height), 10))

	select {
	case <-e.settled:
		b.WriteString(" settled")
	default:
		b.WriteString(" running")
	}

	if e.failed.Load() {
		b.WriteString(" failed")
	}

	if e.aborted.Load() {
		b.WriteString(" aborted")
	}

	return b.String()
}

// noteDrainDeclined counts a turn the drain was given and did not use. Written
// by the consumer only, and read by the watchdog, for the same reason as the
// snapshot beside it.
func (sm *SyncManager) noteDrainDeclined() {
	sm.drainDeclined.Add(1)
}

// noteConsumerAdmitted records that the loop placed work. The watchdog measures
// its silence from here rather than from the last committed block, because
// admitting is what the loop controls: a block that takes six minutes to validate
// is slow, while a loop that has admitted nothing for six minutes is stuck.
func (sm *SyncManager) noteConsumerAdmitted(now time.Time) {
	sm.consumerAdmittedAt.Store(now.UnixNano())
}

// reportConsumerStall describes a consumer that has admitted nothing for
// consumerStallAfter. It runs on the message-handling goroutine's existing ticker,
// which is deliberately not the consumer's own: a report that needed the stuck
// goroutine to print it could never be printed.
//
// It reports and returns; it repairs nothing. What the wedge on 2026-09-08 needed
// first was to be named, and a watchdog that acted on a state nobody had yet read
// would have been a guess with a lock held.
func (sm *SyncManager) reportConsumerStall(now time.Time) {
	if sm.logger == nil {
		return
	}

	last := sm.consumerAdmittedAt.Load()
	if last == 0 {
		// Nothing has been admitted yet in this process, so there is no silence
		// to measure against. The first admission starts the clock.
		return
	}

	if now.Sub(time.Unix(0, last)) < consumerStallAfter {
		return
	}

	if prev := sm.consumerStallLoggedAt.Load(); prev != 0 &&
		now.Sub(time.Unix(0, prev)) < consumerStallReportInterval {
		return
	}

	w, _ := sm.consumerWaitState.Load().(*consumerWait)
	if w == nil {
		// The loop has never reached its select, which is itself worth saying:
		// it means the goroutine is stuck in the work above it, not in the wait.
		sm.logger.Warnf("[consumerWatchdog] no block admitted for %s and the block loop has not reached its wait, so it is stuck in a block's own work rather than waiting for capacity",
			now.Sub(time.Unix(0, last)).Round(time.Second))

		sm.consumerStallLoggedAt.Store(now.UnixNano())

		return
	}

	sm.consumerStallLoggedAt.Store(now.UnixNano())

	// The header round is appended rather than folded into describe(), because
	// describe() reads only the consumer's own snapshot and must stay lock-free:
	// this call takes headerMu, which is legal here and nowhere on the consumer
	// goroutine. See headerRoundSummary.
	report := w.describe(now)
	if round := sm.headerRoundSummary(); round != "" {
		report += "; " + round
	}

	sm.logger.Warnf("[consumerWatchdog] no block admitted for %s: %s",
		now.Sub(time.Unix(0, last)).Round(time.Second), report)
}

// describe says, in one line, what the loop is waiting for and what is waiting on
// it. The order is the order a reader needs it: whether the loop would even take
// another block, what it is already holding that stops it, and what the window
// says about capacity.
func (w *consumerWait) describe(now time.Time) string {
	var b strings.Builder

	b.WriteString("the block loop has been waiting ")
	b.WriteString(now.Sub(w.at).Round(time.Second).String())

	if w.queueArmOpen {
		b.WriteString(" with its queue arm open, so it would take another block and none is arriving")
	} else {
		b.WriteString(" with its queue arm shut, so arriving blocks are piling up behind work it has already accepted")
	}

	if w.pendingHash != "" {
		b.WriteString("; it is holding block ")
		b.WriteString(w.pendingHash)
		b.WriteString(" at height ")
		b.WriteString(strconv.FormatUint(uint64(w.pendingHeight), 10))
		b.WriteString(" for capacity (")

		if w.pendingWindowed {
			b.WriteString("windowed, so it needs a free slot and budget")
		} else {
			b.WriteString("not windowed, so it needs an empty window")
		}

		b.WriteString(")")
	}

	if w.parkJobHeld {
		b.WriteString("; it is holding a park job no worker has taken")
	}

	if w.barrier {
		b.WriteString("; the checkpoint barrier is set, which refuses every block until the checkpoint's tail runs")
	}

	if len(w.frontier) == 0 {
		b.WriteString("; the window is empty")
	} else {
		b.WriteString("; the window holds ")
		b.WriteString(strings.Join(w.frontier, ", "))
	}

	b.WriteString("; ")
	b.WriteString(strconv.FormatInt(w.inflightBytes, 10))
	b.WriteString(" of ")
	b.WriteString(strconv.FormatInt(w.budget, 10))
	b.WriteString(" window bytes charged")

	if w.drainQueued > 0 {
		b.WriteString("; ")
		b.WriteString(strconv.Itoa(w.drainQueued))
		b.WriteString(" parents queued for a drain")

		if w.drainShutByWindow {
			b.WriteString(", but the drain cannot have its turn until the window is empty, so a block that is already on disk and whose parent is committed is waiting on whatever the window still holds")
		}
	}

	b.WriteString("; ")
	b.WriteString(strconv.Itoa(w.parked))
	b.WriteString(" blocks parked")

	// The download budget, and the read-loops parked on it. Said last of the
	// capacity figures because it is the one a reader has never seen before, and
	// said at all because a full one explains a silence the rest of this line
	// cannot: every other figure here describes work the node has already taken
	// in, and this is the gate that decides whether any more arrives.
	if w.downloadBudget > 0 {
		b.WriteString("; ")
		b.WriteString(strconv.FormatInt(w.downloadHeld, 10))
		b.WriteString(" of ")
		b.WriteString(strconv.FormatInt(w.downloadBudget, 10))
		b.WriteString(" download budget bytes reserved")

		if w.downloadWaiters > 0 {
			b.WriteString(" with ")
			b.WriteString(strconv.FormatInt(w.downloadWaiters, 10))
			b.WriteString(" peer read loops blocked on it, so those peers are reading nothing at all")
		}
	}

	// Last because it is cumulative rather than a snapshot, and it was collected
	// but never printed: the field's own comment calls it the fact the first
	// version of this report was missing, and the report went on missing it. A
	// drain that walks its queue, rules every parent out and drops them leaves
	// no trace in the queue length, so without this a loop that has just thrown
	// a turn away is indistinguishable from a loop with no work.
	if w.drainDeclines > 0 {
		b.WriteString("; the drain has declined ")
		b.WriteString(strconv.FormatInt(w.drainDeclines, 10))
		b.WriteString(" turns since start")
	}

	return b.String()
}

// consumerWatchdogState and the two clocks beside it are embedded in SyncManager. They
// are declared here rather than with the rest of the manager's fields so the
// whole diagnostic can be read, and removed, as one piece.
type consumerWatchdogState struct {
	// consumerWaitState is the last snapshot the consumer published. An atomic
	// pointer rather than a mutex because the consumer must never wait on a
	// reader: it is the goroutine whose progress is in question.
	consumerWaitState atomic.Value

	// consumerAdmittedAt is when the loop last placed work, in Unix nanoseconds.
	consumerAdmittedAt atomic.Int64

	// consumerStallLoggedAt is when the watchdog last spoke, so a wedge is
	// described once a minute rather than on every tick.
	consumerStallLoggedAt atomic.Int64

	// drainDeclined counts turns the drain was offered and gave back.
	drainDeclined atomic.Int64
}

// shortHash trims a block hash to its leading digits for a log line. The full
// hash is 64 characters and three of them in one line is unreadable; eight is
// enough to find the block in the same log.
func shortHash(h string) string {
	if len(h) <= 8 {
		return h
	}

	return h[:8]
}
