package netsync

import (
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
)

// The parking workers exist because parking a block is expensive and commits are
// not allowed to wait for it.
//
// The cost of parking a block is the streamed write of the whole block into the
// blob store under legacy_parkStoreTimeout, waiting on write permits shared
// process-wide with subtree writes, transaction writes and both persisters. On
// mainnet those blocks run to gigabytes, so that write is minutes. It used to
// run on the single goroutine that commits blocks in order, so a node with an
// out-of-order backlog spent its time filing blocks instead of committing them.
//
// It used to pay a second cost, a merkle rebuild over every transaction, which
// was the larger of the two at a measured three minutes and more. That is gone;
// see validateParkCandidate, which now reads the header and nothing else.
//
// What stays on the commit goroutine — the block-queue consumer, which under
// the quick window is the dispatcher — is everything that is ordered or that
// touches shared chain state: the peer bookkeeping, the header list, the parent
// lookup, admitting the entry to the park, and applying the disposition when a
// write fails. What moves is the check and the write.
//
// The ordering that makes this safe is in block_park.go: Admit registers the
// entry, its parent edge and its byte charge BEFORE the write, flagged, so a
// parent that commits mid-write finds the block in the index and every reader
// refuses to act on bytes that are not there yet.

// parkJob is one block handed to a worker. It carries two copies of the entry
// because they answer different questions: entry is what Admit registered and is
// what the park is asked about, while blamed names the resolved association
// primary, which is where any reject has to go.
type parkJob struct {
	entry          parkedBlock
	blamed         parkedBlock
	msgBlock       *wire.MsgBlock
	reply          chan error
	catchingBlocks bool
}

// parkOutcome is one finished job, handed back to the commit goroutine.
type parkOutcome struct {
	job    parkJob
	result parkResult
	// drainParent is set when a drain for this block's parent ran while the
	// block was still being written and was refused. The drain is driven by a
	// commit that has already happened, so nothing will repeat it on its own.
	drainParent bool
}

// startParkWorkers brings up the pool. Called from New, and deliberately after
// the last step there that can fail: a goroutine started above an early error
// return leaks, because the caller receives a nil SyncManager and can never call
// Stop.
func (sm *SyncManager) startParkWorkers(workers int) {
	if workers <= 0 {
		workers = 1
	}

	sm.parkJobs = make(chan parkJob)
	sm.parkOutcomes = make(chan parkOutcome, workers)

	// One slot per commit a sweep tick can post, so a tick never waits on the
	// consumer for room and the consumer never waits on the sweep for anything.
	sm.parkCommits = make(chan parkCommit, parkSweepRPCBudget)

	for i := 0; i < workers; i++ {
		sm.parkWorkers.Add(1)

		go sm.parkWorkerLoop()
	}
}

// parkWorkerLoop takes jobs until the manager stops.
func (sm *SyncManager) parkWorkerLoop() {
	defer sm.parkWorkers.Done()

	for {
		select {
		case <-sm.quit:
			return

		case job := <-sm.parkJobs:
			sm.runParkJob(job)
		}
	}
}

// runParkJob does the expensive half and hands the answer back.
func (sm *SyncManager) runParkJob(job parkJob) {
	outcome := parkOutcome{
		job:    job,
		result: sm.blockPark.WriteAdmitted(sm.ctx, job.entry, job.msgBlock),
	}

	if outcome.result == parkAccepted {
		outcome.drainParent = sm.blockPark.FinishWrite(job.entry.hash)
	}

	select {
	case sm.parkOutcomes <- outcome:

	case <-sm.quit:
		// The commit goroutine has gone, so nothing will apply this outcome. The
		// peer waiting on the reply must still be answered or its
		// awaitBlockResult holds its prefetch budget until its own context ends.
		sm.replyToParkJob(job, errors.NewServiceError("sync manager shutting down"))
	}
}

// submitParkJob hands a job to the pool, or does the work here when there is no
// pool to hand it to.
//
// The inline path is not a fallback bolted on for tests, though it is what keeps
// the many SyncManagers built as struct literals working. It is the behaviour
// this package had before the pool existed, and it is the only correct answer
// for a manager that has no workers: a send on a nil channel blocks forever, and
// the goroutine it would block is the one that commits blocks.
func (sm *SyncManager) submitParkJob(job parkJob) {
	if sm.parkJobs == nil {
		outcome := parkOutcome{
			job:    job,
			result: sm.blockPark.WriteAdmitted(sm.ctx, job.entry, job.msgBlock),
		}

		if outcome.result == parkAccepted {
			outcome.drainParent = sm.blockPark.FinishWrite(job.entry.hash)
		}

		sm.applyParkOutcome(outcome)

		return
	}

	// Handing the job to the consumer's own select, when there is one, rather than
	// waiting for a worker here.
	//
	// Waiting here at all is what this avoids. With the drain no longer on the
	// consumer this is the last place that goroutine blocks, and it reaches it far
	// more often, because in the measured regime most arrivals park. A wait here
	// stops completions, sweep posts and drain steps being serviced for as long as
	// a park write takes, and a park write of a mainnet giant block is minutes.
	//
	// The slot preserves the backpressure exactly. While it is set the consumer
	// disables its queue arm, so nothing else is head-processed, which is the same
	// rule the pending dispatch slot follows.
	if sm.parkJobAsync.Load() {
		// Offered once, without blocking. A worker that is free takes it now; one
		// that is not leaves it for the consumer's loop to offer again, which
		// costs a field assignment on the goroutine that owns it. The head runs
		// on that same goroutine, which is why this can be a field at all.
		select {
		case sm.parkJobs <- job:
		default:
			sm.parkJobHeld = &job
		}

		return
	}

	// Otherwise wait here, draining outcomes while waiting. This is the
	// pre-window consumer's path, which has no slot to hand a job to.
	//
	// Draining while waiting is load-bearing, and it used to be a bare send on
	// the strength of "parkOutcomes has a slot for every worker, so no worker can
	// be stuck posting". A worker can always post ONCE. The only goroutine that
	// drains outcomes is this one, and while it is blocked on the send it drains
	// nothing, so a consumer that took two queue messages in a row with every
	// worker mid-write (select picks uniformly among ready arms, so that is a coin
	// toss, not a corner case) filled every slot; the workers then blocked
	// posting, this blocked sending, and the three waited on each other for as
	// long as the process ran. That stopped mainnet on 2026-09-08, minutes after a
	// restart delivered twenty-four out-of-order blocks in three seconds.
	for {
		select {
		case sm.parkJobs <- job:
			return

		case outcome := <-sm.parkOutcomes:
			sm.applyParkOutcome(outcome)

		case <-sm.quit:
			sm.replyToParkJob(job, errors.NewServiceError("sync manager shutting down"))

			return
		}
	}
}

// applyParkOutcome runs on the commit goroutine, which is where every
// disposition is applied and the only place the header list is rewound.
func (sm *SyncManager) applyParkOutcome(outcome parkOutcome) {
	job := outcome.job

	switch {
	case outcome.result == parkAccepted:
		// The optimistic half already ran when the block was admitted: it was
		// logged as parked, the pipeline was topped up and the parent was asked
		// for. Nothing more is owed unless a drain was refused while we wrote.
		if outcome.drainParent {
			sm.logger.Infof("[applyParkOutcome][%s] its parent committed while it was being written, draining now", job.entry.hash)

			sm.scheduleDrain(job.entry.prevBlock, 0)
		}

	default:
		// The write did not land, so the entry, its edge and its bytes have
		// already been given back by WriteAdmitted. What is left is the part
		// that only this goroutine may do: put the download walk back on the
		// block, and blame the peer when the block itself was the problem.
		d := parkWriteOutcome(outcome.result)

		if job.catchingBlocks {
			d = d.withoutBlame()
		}

		sm.logger.Infof("Block %v was admitted to the park and then not kept (%s)", job.entry.hash, d.reason)

		sm.applyParkDisposition(job.blamed, d)
	}

	sm.replyToParkJob(job, nil)
}

// replyToParkJob answers the queued block's caller. The reply is transferred
// from the queue message to the job at hand-off rather than sent when
// handleBlockMsg returns, so the prefetch budget the block is charged against is
// held until the worker has finished with the decoded block rather than released
// while a worker still holds it.
func (sm *SyncManager) replyToParkJob(job parkJob, err error) {
	if job.reply == nil {
		return
	}

	job.reply <- err
}
