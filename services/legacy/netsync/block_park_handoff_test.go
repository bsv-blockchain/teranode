package netsync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestParkWorkers_TheHandOffDrainsOutcomesWhileItWaits reproduces the deadlock
// that stopped mainnet on 2026-09-08, minutes after a restart that brought
// twenty-four out-of-order blocks in three seconds.
//
// The consumer hands a job to a worker with a blocking send, and the workers
// hand their outcomes back on a channel with one slot per worker. The comment
// on the send said that could not deadlock, because a worker can always post.
// It can, exactly once: the only goroutine that drains outcomes is the consumer,
// and while the consumer is blocked handing over a job it drains nothing. A
// consumer that takes two queue messages in a row while every worker is mid
// write fills every slot, then the workers block posting, then the consumer
// blocks sending, and the goroutine dump shows all three waiting on each other
// for as long as the process runs. Every prefetch goroutine then waits on
// budget the undelivered replies hold, and the sync stops.
//
// One worker, so one outcome slot. The first job's outcome fills it; the worker
// blocks posting the second; the third hand-off used to wait forever.
func TestParkWorkers_TheHandOffDrainsOutcomesWhileItWaits(t *testing.T) {
	h := newParkWiringHarness(t, true)

	startParkPool(t, h, 1)

	jobs := make([]parkJob, 0, len(h.blocks))

	for i, block := range h.blocks {
		msgBlock := block.MsgBlock()

		entry := parkedBlock{
			hash:      msgBlock.BlockHash(),
			prevBlock: msgBlock.Header.PrevBlock,
			height:    int32(i + 1),
			peer:      h.peer,
		}

		stored, admitted := h.sm.blockPark.Admit(entry, msgBlock)
		require.Equal(t, admitRegistered, admitted)

		jobs = append(jobs, parkJob{
			entry:          stored,
			blamed:         stored,
			msgBlock:       msgBlock,
			reply:          make(chan error, 1),
			catchingBlocks: true,
		})
	}

	// The consumer's side, on its own goroutine so the test can put a clock on it.
	handedOff := make(chan struct{})

	go func() {
		for _, job := range jobs {
			h.sm.submitParkJob(job)
		}

		close(handedOff)
	}()

	select {
	case <-handedOff:
	case <-time.After(10 * time.Second):
		t.Fatal("the hand-off waited on an outcome only the consumer can drain: the park deadlocked")
	}

	// The last job's outcome has nobody waiting to drain it now the consumer's
	// loop has ended, so the test plays the consumer for it.
	select {
	case outcome := <-h.sm.parkOutcomes:
		h.sm.applyParkOutcome(outcome)
	case <-time.After(5 * time.Second):
		t.Fatal("the worker never posted the last outcome")
	}

	for i, job := range jobs {
		select {
		case err := <-job.reply:
			require.NoError(t, err, "job %d", i)
		case <-time.After(5 * time.Second):
			t.Fatalf("job %d was never answered; its outcome was never applied", i)
		}
	}

	require.Equal(t, len(jobs), h.sm.blockPark.Len(), "every block is parked, none was lost in the hand-off")
}
