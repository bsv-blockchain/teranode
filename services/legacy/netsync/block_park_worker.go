package netsync

import (
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
)

// The parking workers exist because parking a block is expensive and commits are
// not allowed to wait for it.
//
// A parked block pays two costs: a stateless check that rebuilds the merkle tree
// over every transaction in it, and a streamed write of the whole block into the
// blob store under legacy_parkStoreTimeout, waiting on write permits shared
// process-wide with subtree writes, transaction writes and both persisters. On
// mainnet those blocks run to gigabytes and the check alone has been measured at
// over three minutes. All of it used to run on the single goroutine that commits
// blocks in order, so a node with an out-of-order backlog spent its time filing
// blocks instead of committing them.
//
// What stays on the commit goroutine is everything that is ordered or that
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

	// Blocking here is the backpressure: with every worker busy the commit
	// goroutine waits rather than admitting blocks faster than they can be
	// written. It cannot deadlock, because parkOutcomes has a slot for every
	// worker, so no worker can be stuck posting an outcome while this waits.
	select {
	case sm.parkJobs <- job:

	case <-sm.quit:
		sm.replyToParkJob(job, errors.NewServiceError("sync manager shutting down"))
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

			sm.drainParkedDescendants(job.entry.prevBlock)
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
