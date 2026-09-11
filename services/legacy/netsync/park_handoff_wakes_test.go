package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The fifth deadlock of 2026-09-08, and the one that outlived the other four
// fixes. Mainnet stopped at height 756,370 with the block loop reporting, once a
// minute for thirty-four minutes, that it was "holding a park job no worker has
// taken". A goroutine profile off the running node showed no park job running
// anywhere: both workers sat idle in their receive, waiting for work, while the
// loop held the job in its hand.
//
// The loop offered a held job only at the top of each turn, with a send that
// could not block. That send fails whenever no worker happens to be sitting in
// its receive at that instant, and the loop then blocked in a select with no arm
// for the offer. Nothing else was due: the window was empty so no completion was
// coming, the queue arm is shut while a job is held, and no worker would post an
// outcome because none was running. The loop slept, the workers slept, and each
// was waiting on the other.
//
// The hand-off is now an arm of that select, so a worker takes the job as soon
// as it is ready and no wake-up from anywhere else is needed.
func TestParkHandoff_AHeldJobReachesAWorkerWithNothingElseHappening(t *testing.T) {
	h := newLoopHarness(t, 1)

	h.sm.dispatcher.run = func(context.Context, *blockDispatch, *inflightParent) error { return nil }

	// The pool's channels, with nobody behind them yet. That is the state which
	// makes the loop hold the job: its offer has no taker.
	h.sm.parkJobs = make(chan parkJob)
	h.sm.parkOutcomes = make(chan parkOutcome, 1)

	// Written before the goroutine starts, so there is no race over a field the
	// consumer otherwise owns outright.
	h.sm.parkJobHeld = &parkJob{}

	go h.sm.dispatchBlocks(h.queue)
	t.Cleanup(func() { close(h.sm.quit) })

	// Let the loop reach its wait while holding the job, which is the whole
	// premise: the offer has already failed once by the time anyone is listening.
	require.True(t, WaitUntil(func() bool {
		w, _ := h.sm.consumerWaitState.Load().(*consumerWait)

		return w != nil && w.parkJobHeld && !w.queueArmOpen
	}, 5*time.Second), "precondition: the loop is parked in its wait still holding the job")

	// A worker turning up afterwards, doing nothing but what a real one does
	// first: block in its receive. Nothing else happens in this test — no
	// completion, no outcome, no sweep, no arriving block — so the hand-off has
	// to be what wakes the loop.
	taken := make(chan struct{})

	go func() {
		<-h.sm.parkJobs
		close(taken)
	}()

	select {
	case <-taken:
	case <-time.After(10 * time.Second):
		require.Fail(t, "the loop never handed the job over, so an idle worker and a held job waited on each other")
	}

	// Handed over exactly once. The slot has to be cleared when the arm fires, or
	// the same block is given to worker after worker, each one rebuilding its
	// merkle tree and streaming it to disk again.
	second := make(chan struct{})

	go func() {
		select {
		case <-h.sm.parkJobs:
			close(second)
		case <-time.After(3 * time.Second):
		}
	}()

	select {
	case <-second:
		require.Fail(t, "the job was handed over twice, so the held slot was not cleared when the arm fired")
	case <-time.After(4 * time.Second):
	}
}

// TestParkHandoff_NothingIsHandedOverWhenNoJobIsHeld is the other side of the
// arm, and it is not hypothetical: a select evaluates every send case's operands
// on the way in, so the arm is always constructed even when there is no job. If
// the channel is not left nil in that case, the loop offers workers a zero-value
// job, over and over, for as long as it has nothing better to do.
func TestParkHandoff_NothingIsHandedOverWhenNoJobIsHeld(t *testing.T) {
	h := newLoopHarness(t, 1)

	h.sm.dispatcher.run = func(context.Context, *blockDispatch, *inflightParent) error { return nil }

	h.sm.parkJobs = make(chan parkJob)
	h.sm.parkOutcomes = make(chan parkOutcome, 1)

	// Deliberately nothing held.
	h.sm.parkJobHeld = nil

	go h.sm.dispatchBlocks(h.queue)
	t.Cleanup(func() { close(h.sm.quit) })

	require.True(t, WaitUntil(func() bool {
		w, _ := h.sm.consumerWaitState.Load().(*consumerWait)

		return w != nil && !w.parkJobHeld && w.queueArmOpen
	}, 5*time.Second), "precondition: the loop is waiting with nothing held and its queue arm open")

	// A worker standing by. It must be given nothing, because there is nothing.
	handed := make(chan parkJob, 1)

	go func() {
		select {
		case j := <-h.sm.parkJobs:
			handed <- j
		case <-time.After(3 * time.Second):
		}
	}()

	select {
	case j := <-handed:
		require.Failf(t, "a job was handed over when none was held",
			"the worker received %+v, which is the zero value a disabled arm must never send", j)
	case <-time.After(4 * time.Second):
	}
}
