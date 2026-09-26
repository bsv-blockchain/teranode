package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// The wedge this file guards against, from mainnet on 2026-09-08 at height
// 755,112.
//
// The block loop takes at most one committable block per turn: it tries the
// drain queue first, and when nothing there is ready it waits on a completion
// from a dispatched worker or a parkCommit posted by the sweep or a streamed
// arrival. The drain is allowed to decline after it has been chosen: it walks
// the queued parents itself and can find that none of them has a child it can
// commit yet.
//
// A declined drain used to cost the whole turn under the old dual-source
// admission (a live decoded-block queue alongside the drain), which this
// package no longer has — every block is parked first. The equivalent
// property under the current single-path loop (dispatchBlocks, manager.go) is
// that a declined, dropped drain request must not stop the very next turn
// picking up a parkCommit that is already sitting there, ready to schedule the
// real drain behind it.
func TestADeclinedDrainDoesNotCostTheTurn(t *testing.T) {
	h := newParkWiringHarness(t, true)
	bd := h.withDispatcher(t)
	bd.depth = 1

	h.sm.quit = make(chan struct{})
	// New() builds this channel unconditionally; wired by hand here so the
	// parent's own commit below goes through the async parkCommits arm of the
	// loop instead of committing inline.
	h.sm.parkCommits = make(chan parkCommit, parkSweepRPCBudget)
	t.Cleanup(func() { close(h.sm.quit) })

	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	bd.parkedRun = func(context.Context, *blockDispatch) error {
		<-release

		return nil
	}

	// The child parks behind its parent through the ordinary arrival path.
	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	// A decoy drain request for a parent with nothing parked behind it, queued
	// before the loop starts. drainStep must decline and drop it (FirstChildFor
	// finds no child) rather than let it wedge the turn a real parkCommit is
	// about to need.
	h.sm.drainAsync.Store(true)
	h.sm.scheduleDrain(chainhash.Hash{0xaa}, 0)

	h.chainHolds(t, h.blocks[0].MsgBlock().Header.PrevBlock)
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

	go h.sm.dispatchBlocks()

	// The parent arrives. Its own parent (genesis) is in the chain, so
	// handleBlockOnDiskMsg posts a parkCommit for it (parkCommits is non-nil)
	// rather than committing it inline — the async path the loop above is
	// running, and the one the decoy must not be allowed to starve.
	require.NoError(t, h.deliver(t, 0))

	close(release)

	require.True(t, WaitUntil(func() bool { return h.sm.blockPark.Len() == 0 }, 5*time.Second),
		"the parent's parkCommit, and the child drained behind it, must not be stalled by the declined decoy")
}
