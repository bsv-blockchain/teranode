package subtreeprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

// The four public entry points - Reorg, MoveForwardBlock, Reset,
// CheckSubtreeProcessor - hand work to the dispatcher goroutine via an
// unbuffered request channel and then block on an unbuffered response
// channel. Pre-fix, neither side was ctx-aware: callers who arrived
// after the dispatcher exited (clean shutdown, ctx cancellation, panic
// caught by the goroutine-level recover) blocked forever on the send.
// Callers who arrived during the work but whose response was lost
// (dispatcher dying mid-handler) blocked forever on the receive.
//
// The fix selects processorContext().Done() against both halves so a
// stopped processor returns an actionable error in O(ms) instead of
// hanging the caller indefinitely.
//
// These tests pin the contract by Stop()ping the processor first, then
// calling each entry point and asserting it returns an error within a
// short timeout. Pre-fix the goroutine-style timeout would fire and the
// helper would t.Fatal. Post-fix the entry point returns "processor
// stopped" and the test passes.

func TestShutdown_Reorg_DoesNotHang(t *testing.T) {
	stp := newStoppedSubtreeProcessor(t)

	done := make(chan error, 1)
	go func() { done <- stp.Reorg(nil, nil) }()

	requireErrorBeforeTimeout(t, done, "Reorg")
}

func TestShutdown_MoveForwardBlock_DoesNotHang(t *testing.T) {
	stp := newStoppedSubtreeProcessor(t)

	done := make(chan error, 1)
	go func() { done <- stp.MoveForwardBlock(&model.Block{Header: model.GenesisBlockHeader}) }()

	requireErrorBeforeTimeout(t, done, "MoveForwardBlock")
}

func TestShutdown_Reset_DoesNotHang(t *testing.T) {
	stp := newStoppedSubtreeProcessor(t)

	done := make(chan ResetResponse, 1)
	go func() { done <- stp.Reset(model.GenesisBlockHeader, nil, nil, false, nil) }()

	select {
	case resp := <-done:
		require.Error(t, resp.Err,
			"Reset must return an error when called against a stopped processor, not hang on the response channel")
		require.Contains(t, resp.Err.Error(), "processor stopped")
	case <-time.After(2 * time.Second):
		t.Fatal("Reset hung after Stop(); the unbuffered request-channel send blocks forever pre-fix")
	}
}

func TestShutdown_CheckSubtreeProcessor_DoesNotHang(t *testing.T) {
	stp := newStoppedSubtreeProcessor(t)

	done := make(chan error, 1)
	go func() { done <- stp.CheckSubtreeProcessor() }()

	requireErrorBeforeTimeout(t, done, "CheckSubtreeProcessor")
}

// TestShutdown_PanicExit_DoesNotHang pins the contract for the harder
// exit path: the dispatcher is killed by a recovered panic, NOT by
// Stop(). Pre-fix this branch left processorContext() live (only Stop's
// cancel() closed it), so entry-point callers selecting on
// processorContext().Done() would still block forever on the request
// send despite the dispatcher being dead. The Major-item fix cancels
// the context in the dispatcher's deferred recover so any exit path -
// clean ctx-done, fall-through, panic recovery - releases blocked
// callers.
//
// The panic is induced by closing the response channel of a lengthCh
// request before the dispatcher consumes it. The dispatcher's
// `lengthCh <- stp.currentSubtree.Load().Length()` send on a closed
// channel panics; the goroutine-level recover at the top of Start
// catches it.
func TestShutdown_PanicExit_DoesNotHang(t *testing.T) {
	stp := setupTestSubtreeProcessor(t)

	// Inject a panic into the dispatcher's lengthCh handler: close the
	// response channel before the dispatcher writes to it. The dispatcher
	// runs `lengthCh <- ...` which panics on send-to-closed-channel.
	poisoned := make(chan int)
	close(poisoned)
	stp.lengthCh <- poisoned

	// Wait for the goroutine-level recover to fire and set stopped=true.
	// Bounded poll, not a sleep loop - tests under load may take longer
	// than a single sleep would tolerate.
	require.Eventually(t, stp.stopped.Load, 2*time.Second, 10*time.Millisecond,
		"dispatcher did not exit within 2 s after the induced panic; the goroutine-level recover may not have fired")

	// Confirm the entry-point caller now unblocks on processorContext().Done()
	// even though Stop() was never called.
	done := make(chan error, 1)
	go func() { done <- stp.Reorg(nil, nil) }()
	requireErrorBeforeTimeout(t, done, "Reorg-after-panic-exit")
}

// newStoppedSubtreeProcessor returns a SubtreeProcessor with its dispatcher
// goroutine torn down. setupTestSubtreeProcessor already registers a Cleanup
// hook that calls Stop again, which is safe via stopOnce.
func newStoppedSubtreeProcessor(t *testing.T) *SubtreeProcessor {
	t.Helper()
	stp := setupTestSubtreeProcessor(t)
	stp.Stop(context.Background())
	return stp
}

func requireErrorBeforeTimeout(t *testing.T, done <-chan error, label string) {
	t.Helper()
	select {
	case err := <-done:
		require.Error(t, err,
			"%s must return an error when called against a stopped processor", label)
		require.Contains(t, err.Error(), "processor stopped",
			"the surfaced error should identify the shutdown path so operators can spot it in logs")
	case <-time.After(2 * time.Second):
		t.Fatalf("%s hung after Stop(); the unbuffered channel patterns block forever pre-fix", label)
	}
}
