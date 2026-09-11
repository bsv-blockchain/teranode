package netsync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// startParkPool gives a harness manager a real worker pool and takes it down
// again, so a test can tell hand-off from inline work. Without one,
// submitParkJob does the write on the calling goroutine, which is the right
// answer for a manager that has no workers and is what every other wiring test
// exercises.
func startParkPool(t *testing.T, h *parkWiringHarness, workers int) {
	t.Helper()

	h.sm.quit = make(chan struct{})
	h.sm.startParkWorkers(workers)

	t.Cleanup(func() {
		close(h.sm.quit)
		h.sm.parkWorkers.Wait()
	})
}

// TestParkWorkers_TheCommitGoroutineDoesNotWaitForTheWrite is the whole point of
// the pool.
//
// Parking a block costs a merkle rebuild over every transaction in it and a
// streamed write of the whole block. Both used to run on the one goroutine that
// commits blocks in order, so a node with an out-of-order backlog spent its time
// filing blocks rather than committing them. This asserts the commit goroutine
// is free while the write is still in flight.
func TestParkWorkers_TheCommitGoroutineDoesNotWaitForTheWrite(t *testing.T) {
	h := newParkWiringHarness(t, true)

	// HandleBlockDirect asks this before the parent lookup that makes the block
	// an orphan. Nothing is stored in this harness.
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	gate := &gatedWriteStore{
		Store:   h.sm.blockPark.store,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	h.sm.blockPark.store = gate

	startParkPool(t, h, 1)

	msgBlock := h.blocks[0].MsgBlock()
	hash := msgBlock.BlockHash()

	h.sm.blockDownloads.Add(h.peer, hash)

	reply := make(chan error, 1)

	returned := make(chan error, 1)

	go func() {
		returned <- h.sm.processQueuedBlock(&blockQueueMsg{
			block:       msgBlock,
			blockHash:   hash,
			blockHeight: 1,
			peer:        h.peer,
			reply:       reply,
		})
	}()

	select {
	case <-gate.started:
	case <-time.After(10 * time.Second):
		t.Fatal("the block never reached the blob write")
	}

	select {
	case err := <-returned:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the commit goroutine was still waiting for the write; the hand-off did not happen")
	}

	require.True(t, h.sm.blockPark.Has(hash), "the entry is registered before the write, not after it")

	// The prefetch budget this block is charged against is released when the
	// caller is answered, so answering at hand-off would let the peer read
	// another block while a worker still holds this one decoded.
	select {
	case <-reply:
		t.Fatal("the caller was answered while a worker still held the decoded block")
	default:
	}

	close(gate.release)

	select {
	case outcome := <-h.sm.parkOutcomes:
		require.Equal(t, parkAccepted, outcome.result)
		require.Equal(t, hash, outcome.job.entry.hash)

		h.sm.applyParkOutcome(outcome)
	case <-time.After(10 * time.Second):
		t.Fatal("the worker never posted its outcome")
	}

	select {
	case err := <-reply:
		require.NoError(t, err, "the caller is answered once the worker has finished with the block")
	case <-time.After(10 * time.Second):
		t.Fatal("the caller was never answered")
	}
}

// TestParkWorkers_AParentThatCommitsMidWriteStillDrainsTheChild covers the race
// the register-before-write ordering exists for, end to end.
//
// The drain is driven by a commit that has already happened, so it does not come
// round again on its own. A child refused mid-write has to be asked for by
// whoever finishes the write, or it sits in the park behind a parent that is
// already in the chain until the sweep notices.
func TestParkWorkers_AParentThatCommitsMidWriteStillDrainsTheChild(t *testing.T) {
	h := newParkWiringHarness(t, true)

	// HandleBlockDirect asks this before the parent lookup that makes the block
	// an orphan. Nothing is stored in this harness.
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	gate := &gatedWriteStore{
		Store:   h.sm.blockPark.store,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	h.sm.blockPark.store = gate

	startParkPool(t, h, 1)

	msgBlock := h.blocks[1].MsgBlock()
	hash := msgBlock.BlockHash()
	parent := msgBlock.Header.PrevBlock

	h.sm.blockDownloads.Add(h.peer, hash)

	go func() {
		_ = h.sm.processQueuedBlock(&blockQueueMsg{
			block:       msgBlock,
			blockHash:   hash,
			blockHeight: 2,
			peer:        h.peer,
		})
	}()

	select {
	case <-gate.started:
	case <-time.After(10 * time.Second):
		t.Fatal("the block never reached the blob write")
	}

	// The parent commits here, in the window where the child's bytes are not on
	// disk. The drain finds the child in the index and refuses it, which is the
	// only safe answer, and records that it did.
	require.Empty(t, h.sm.blockPark.TakeChildren(parent),
		"a drain must not take a block whose bytes are not on disk yet")

	close(gate.release)

	select {
	case outcome := <-h.sm.parkOutcomes:
		require.Equal(t, parkAccepted, outcome.result)
		require.True(t, outcome.drainParent,
			"the worker must ask for the drain that was refused while it was writing")
	case <-time.After(10 * time.Second):
		t.Fatal("the worker never posted its outcome")
	}
}
