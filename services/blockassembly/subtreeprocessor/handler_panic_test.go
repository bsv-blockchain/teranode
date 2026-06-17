package subtreeprocessor

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

// TestReorg_PanicSurfacesAsError pins that a panic inside reorgBlocks
// returns an error to the Reorg() caller rather than leaving them
// blocked forever on errChan. Pre-fix, the goroutine-level recover at
// the dispatcher (SubtreeProcessor.go ~626) only logged the panic and
// exited the processor goroutine; the in-flight Reorg() call hung on
// <-errChan, and the next Reorg() also blocked sending on
// reorgBlockChan (no reader). A peer-controllable panic in reorg
// processing would therefore wedge BlockAssembly's reorg pipeline.
//
// We trigger a deterministic panic by passing a nil *model.Block in
// moveForwardBlocks with empty moveBackBlocks: this hits the catch-up
// fast path which calls moveForwardBlocks[len-1].Hash() at
// SubtreeProcessor.go:3038, where Hash() on nil dereferences.
func TestReorg_PanicSurfacesAsError(t *testing.T) {
	stp := setupTestSubtreeProcessor(t)

	moveForwardBlocks := []*model.Block{nil}
	moveBackBlocks := []*model.Block{}

	done := make(chan error, 1)
	go func() {
		done <- stp.Reorg(moveBackBlocks, moveForwardBlocks)
	}()

	select {
	case err := <-done:
		require.Error(t, err,
			"Reorg must return an error when reorgBlocks panics, not nil; "+
				"otherwise the caller has no way to know the work failed")
		require.Contains(t, err.Error(), "panicked",
			"the surfaced error should explicitly identify a panic so operators "+
				"can recognise the dispatcher's panic-recovery path fired")
	case <-time.After(5 * time.Second):
		t.Fatal("Reorg hung waiting on errChan - panic in handler did not unblock the caller. " +
			"This is the pre-fix behaviour: dispatcher's goroutine-level recover exits without " +
			"sending anything to the request's errChan.")
	}
}

// TestMoveForwardBlock_PanicSurfacesAsError pins the same contract on
// the MoveForwardBlock dispatcher case, which shares the
// runHandlerWithRecover protection with the reorgBlocks case.
func TestMoveForwardBlock_PanicSurfacesAsError(t *testing.T) {
	stp := setupTestSubtreeProcessor(t)

	done := make(chan error, 1)
	go func() {
		// Passing a nil block triggers a nil-deref inside the
		// moveForwardBlock handler when it dereferences block.Hash() or
		// similar methods on the input.
		done <- stp.MoveForwardBlock(nil)
	}()

	select {
	case err := <-done:
		require.Error(t, err,
			"MoveForwardBlock must return an error when its handler panics")
		require.Contains(t, err.Error(), "panicked")
	case <-time.After(5 * time.Second):
		t.Fatal("MoveForwardBlock hung waiting on errChan after handler panic")
	}
}
