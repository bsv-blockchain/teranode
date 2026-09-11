package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// The wedge this file guards against, from mainnet on 2026-09-08 at height
// 755,112.
//
// The block loop takes one admission per turn and alternates between its two
// sources, a block that arrived on the wire and a parked block whose parent has
// just landed, so that neither can starve the other. The drain is allowed to
// decline after it has been chosen: it walks the queued parents itself and can
// find that none of them has a child it can commit yet.
//
// A declined drain used to cost the whole turn. The loop fell through to its
// wait, and the wait is unreachable from anywhere: the queue arm is shut while a
// block is pending, the window was empty so no completion was coming, the park
// workers were idle and the sweep had nothing to offer. The node stopped for
// good with a block ready to go and nothing left alive to notice.
//
// The ordering is not a corner case. A block's own tail is what queues a drain
// for its parked children, and the tail runs on the loop's goroutine, so the
// turn straight after any commit has both sources ready. That is the turn the
// alternation gives to the drain.

func TestADeclinedDrainDoesNotCostTheTurn(t *testing.T) {
	h := newLoopHarness(t, 2)

	// An enabled park with nothing in it, which is what makes the drain decline:
	// the parent it is asked about has no child on disk to commit.
	park, _ := newTestPark(t, "")
	h.sm.blockPark = park

	// The block's own work is not what is under test; the loop's choice of what
	// to admit is. Stubbed so a block completes without a store behind it.
	h.sm.dispatcher.run = func(context.Context, *blockDispatch, *inflightParent) error { return nil }

	// The alternation's state at the moment that matters. False means the next
	// turn with both sources ready belongs to the drain.
	h.sm.lastDispatchWasDrained = false

	// A drain queued from a tail, which is where the real one comes from and is
	// why this is safe to write here: the tail runs on the loop's own goroutine,
	// the only goroutine allowed to touch the queue.
	queued := false
	orig := h.sm.dispatcher.tail

	h.sm.dispatcher.tail = func(d *blockDispatch, err error) error {
		out := orig(d, err)

		if !queued {
			queued = true

			h.sm.scheduleDrain(chainhash.Hash{0xaa}, 0)
		}

		return out
	}

	// Depth one is what mainnet runs, and it is what makes the second block wait:
	// while the first is in flight the window has no room, so the second is
	// head-processed and held, which shuts the queue arm behind it.
	h.sm.dispatcher.depth = 1

	first := h.enqueue(0)
	second := h.enqueue(1)

	go h.sm.dispatchBlocks(h.queue)
	t.Cleanup(func() { close(h.sm.quit) })

	select {
	case err := <-first:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		require.Fail(t, "the first block never finished, so the test never reached the state it is about")
	}

	// The block the wedge stranded. Its parent has committed, the window is
	// empty and it is ready; the only thing between it and the window was a turn
	// the drain took and did not use.
	select {
	case err := <-second:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		require.Fail(t, "the block loop stopped admitting after the drain declined, which is the mainnet wedge")
	}

	require.True(t, queued, "the test never queued a drain, so it proved nothing")
}

func TestNextAdmissionGivesADeclinedDrainsTurnToALiveBlock(t *testing.T) {
	// The rule the loop leans on for the retry: with the drain taken out of the
	// running, a ready live block gets the turn and an empty hand does not spin.
	require.Equal(t, admitLive, nextAdmission(false, true, false))
	require.Equal(t, admitLive, nextAdmission(true, true, false))
	require.Equal(t, admitNothing, nextAdmission(false, false, false))
	require.Equal(t, admitNothing, nextAdmission(true, false, false))

	// And the alternation itself, unchanged: with both ready the turn goes to
	// whichever did not have the last one.
	require.Equal(t, admitDrained, nextAdmission(false, true, true))
	require.Equal(t, admitLive, nextAdmission(true, true, true))
}
