package subtreeprocessor

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
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
// We trigger a panic by passing a nil *model.Block in moveForwardBlocks
// with empty moveBackBlocks: this reaches the catch-up fast path which
// dereferences moveForwardBlocks[len-1].Hash() (SubtreeProcessor.go).
// The contract this test pins is "caller gets an error, does not hang" -
// it does NOT assert the error string contains "panicked", because a
// future defensive nil-check on this code path would return a regular
// error and would still satisfy the contract.
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
			"Reorg must return an error rather than nil when handed an input "+
				"that triggers a panic or hits a defensive early-return")
	case <-time.After(5 * time.Second):
		t.Fatal("Reorg hung waiting on errChan - panic in handler did not unblock the caller. " +
			"This is the pre-fix behaviour: dispatcher's goroutine-level recover exits without " +
			"sending anything to the request's errChan.")
	}
}

// TestMoveForwardBlock_PanicSurfacesAsError pins the same contract on
// the MoveForwardBlock dispatcher case, which shares the
// runHandlerWithRecover protection with the reorgBlocks case.
//
// As with TestReorg_PanicSurfacesAsError, the contract is "caller gets
// an error, does not hang"; the test does not assert "panicked" in the
// message so a future defensive nil-check stays compatible.
func TestMoveForwardBlock_PanicSurfacesAsError(t *testing.T) {
	stp := setupTestSubtreeProcessor(t)

	done := make(chan error, 1)
	go func() {
		// Passing a nil block triggers a nil-deref inside the
		// dispatcher's moveForward case (logger.Infof with block.String()).
		done <- stp.MoveForwardBlock(nil)
	}()

	select {
	case err := <-done:
		require.Error(t, err,
			"MoveForwardBlock must return an error rather than nil when handed "+
				"an input that triggers a panic or hits a defensive early-return")
	case <-time.After(5 * time.Second):
		t.Fatal("MoveForwardBlock hung waiting on errChan after handler panic")
	}
}

// TestReset_PanicSurfacesAsError pins the same contract on the reset
// dispatcher case. A panicking postProcess callback (a caller-supplied
// hook invoked from inside reset()) must not wedge the responseCh —
// the dispatcher must catch the panic and report it back via
// ResetResponse.Err so BlockAssembler.reset() can log and continue.
func TestReset_PanicSurfacesAsError(t *testing.T) {
	stp := setupTestSubtreeProcessor(t)

	done := make(chan ResetResponse, 1)
	go func() {
		done <- stp.Reset(model.GenesisBlockHeader, nil, nil, false, func() error {
			panic("simulated panic in postProcess")
		})
	}()

	select {
	case resp := <-done:
		require.Error(t, resp.Err,
			"Reset must return an error when reset()/postProcess panics, not nil")
		require.Contains(t, resp.Err.Error(), "panicked",
			"the surfaced error should identify a panic so operators can spot the recovery path firing")
	case <-time.After(5 * time.Second):
		t.Fatal("Reset hung waiting on responseCh after handler panic")
	}
}

// TestCheckSubtreeProcessor_NoWedgeOnInconsistency pins that
// checkSubtreeProcessor returns the first inconsistency as a normal
// error without wedging the dispatcher. Pre-fix, the helper sent
// each detected error on the unbuffered response channel and then
// also sent nil at the end; the caller only reads one value, so any
// inconsistency wedged the dispatcher goroutine on the second send
// and every subsequent reorg/moveForward/reset call hung.
//
// We seed currentSubtree with a non-coinbase node that is NOT in
// currentTxMap. checkSubtreeProcessor must surface that as the
// returned error AND the dispatcher must still respond to a
// follow-up call within the timeout.
func TestCheckSubtreeProcessor_NoWedgeOnInconsistency(t *testing.T) {
	stp := setupTestSubtreeProcessor(t)

	bogus := chainhash.HashH([]byte("not-in-tx-map"))
	cur := stp.currentSubtree.Load()
	require.NoError(t, cur.AddNode(bogus, 1, 0))
	stp.currentSubtree.Store(cur)
	// Do NOT register `bogus` in currentTxMap — that is the inconsistency.

	first := make(chan error, 1)
	go func() { first <- stp.CheckSubtreeProcessor() }()

	select {
	case err := <-first:
		require.Error(t, err,
			"CheckSubtreeProcessor must report the inconsistency")
		require.Contains(t, err.Error(), "currentSubtree not in currentTxMap")
	case <-time.After(5 * time.Second):
		t.Fatal("CheckSubtreeProcessor hung — dispatcher wedged on the response channel")
	}

	// Heal the state, then call again. The dispatcher must still be
	// responsive: a second call returns within the timeout, which the
	// pre-fix multi-send path could not satisfy because the dispatcher
	// was stuck on the unread errCh from the first call.
	stp.currentTxMap.Set(bogus, &subtreepkg.TxInpoints{})

	second := make(chan error, 1)
	go func() { second <- stp.CheckSubtreeProcessor() }()

	select {
	case err := <-second:
		// May still report the txmap-vs-txcount tally error; what
		// matters is that the call RETURNS rather than hanging.
		_ = err
	case <-time.After(5 * time.Second):
		t.Fatal("Second CheckSubtreeProcessor hung — dispatcher did not recover")
	}
}
