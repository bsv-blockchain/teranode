package netsync

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestConsumerWait_Describe_NamesADrainShutByTheWindow is the regression test for
// the fault an operator actually watches, which this report could not see.
//
// Measured on mainnet on 2026-09-10: the tip sat at 783,277 with the block for
// 783,278 already on disk and its parent already committed, 127 more blocks
// stacked behind it in one contiguous chain, and the sweep announcing every
// thirty seconds that it was committing the block. Nothing committed for over
// five minutes. The watchdog never spoke, and when it did speak it would not have
// said the useful thing, because "parents queued for a drain" reads as work in
// hand rather than as work that cannot be started.
//
// The drain only gets a turn when the dispatcher's window is empty. Queued and
// shut is a wedge; queued and open is a loop about to do something.
func TestConsumerWait_Describe_NamesADrainShutByTheWindow(t *testing.T) {
	now := time.Now()

	t.Run("queued and shut says so, and says why", func(t *testing.T) {
		w := &consumerWait{
			at:                now,
			queueArmOpen:      true,
			parked:            128,
			drainQueued:       1,
			drainShutByWindow: true,
			frontier:          []string{"deadbeef at 783278 running"},
		}

		line := w.describe(now)

		require.Contains(t, line, "1 parents queued for a drain")
		require.Contains(t, line, "cannot have its turn until the window is empty",
			"the reader has to be told the difference between nothing to commit and no way to commit it")
	})

	t.Run("queued and open does not claim a wedge", func(t *testing.T) {
		w := &consumerWait{at: now, queueArmOpen: true, drainQueued: 1}

		line := w.describe(now)

		require.Contains(t, line, "1 parents queued for a drain")
		require.False(t, strings.Contains(line, "cannot have its turn"),
			"a drain that can run is not the fault, and saying so would send the next reader to the wrong place")
	})

	t.Run("nothing queued says nothing about the drain", func(t *testing.T) {
		w := &consumerWait{at: now, queueArmOpen: true, drainShutByWindow: true}

		require.False(t, strings.Contains(w.describe(now), "queued for a drain"),
			"with no parents queued there is no drain to be shut")
	})
}

// TestBlockHandlerAdmission_ParkWorkIsNotProgress pins the premise the loop now
// holds, because the comment that used to sit in it held the opposite: that a
// loop parking blocks it cannot commit is working, and only a loop doing nothing
// at all is wedged.
//
// That is what blinded the watchdog. Blocks kept arriving, kept being received
// and parked, and each of those stamped the "loop placed work" clock, so a node
// committing nothing for minutes reported itself healthy.
//
// Asserted by counting the stamps in the source rather than by running the loop,
// because the loop needs a dispatcher, park workers and a live block queue, and a
// test that built all three would be testing those instead of this.
func TestBlockHandlerAdmission_ParkWorkIsNotProgress(t *testing.T) {
	src := readManagerSource(t)

	// The two park hand-off sites and the queue receive must not stamp.
	for _, marker := range []string{
		"case sm.parkJobs <- *sm.parkJobHeld:",
		"case parkArm <- parkWork:",
		"case msg := <-queueArm:",
	} {
		idx := strings.Index(src, marker)
		require.Positive(t, idx, "the loop no longer has %q, so this test needs rewriting rather than deleting", marker)

		// Look only at the lines that belong to this arm: everything up to the
		// next case or default label at any indentation. A fixed byte window
		// would run past the arm and pick up a stamp that legitimately belongs
		// to the next one.
		window := src[idx+len(marker):]
		if cut := nextSwitchLabel(window); cut > 0 {
			window = window[:cut]
		}

		// The CALL, not the identifier: the arms carry comments that name the
		// function to explain why they deliberately do not call it, and matching
		// the bare name would match that explanation.
		require.False(t, strings.Contains(window, "sm.noteConsumerAdmitted("),
			"%q must not count as placing work: parking a block moves it to disk and commits nothing, and receiving one decides nothing", marker)
	}

	// And the two that genuinely place work must still stamp, so this test cannot
	// pass by the clock never being stamped at all.
	require.Contains(t, src, "if sm.drainStep(bd) {",
		"the drained-commit path is where a stamp belongs")
}

// nextSwitchLabel returns the offset of the next case or default label in src, or
// -1 if there is none. Used to bound a single select arm for inspection.
func nextSwitchLabel(src string) int {
	best := -1

	for _, label := range []string{"\n\t\t\tcase ", "\n\t\t\tdefault:", "\n\t\tcase ", "\n\t\tdefault:"} {
		if i := strings.Index(src, label); i >= 0 && (best < 0 || i < best) {
			best = i
		}
	}

	return best
}

// TestPublishConsumerWait_CarriesTheShutDrain pins the WIRING, separately from the
// rendering above. A mutation that reported the drain as shut whenever anything
// was queued passed every rendering test, because those build the snapshot by
// hand and never ask the manager to fill it in. Same trap as a settings field
// that is declared and never loaded.
func TestPublishConsumerWait_CarriesTheShutDrain(t *testing.T) {
	t.Run("queued with a non-empty window is shut", func(t *testing.T) {
		h := newParkWiringHarness(t, true)
		h.sm.dispatcher = newBlockDispatcher(h.sm)
		bd := h.sm.dispatcher

		// One entry in the window, which is the state that shuts the drain.
		bd.frontier = append(bd.frontier, &frontierEntry{hash: h.blocks[2].MsgBlock().BlockHash(), height: 3})
		require.False(t, bd.frontierEmpty(), "precondition: the window holds something")

		h.sm.drainQueue = []drainRequest{{parent: h.blocks[0].MsgBlock().BlockHash()}}

		h.sm.publishConsumerWait(time.Now(), true, nil)

		w, _ := h.sm.consumerWaitState.Load().(*consumerWait)
		require.NotNil(t, w)
		require.Equal(t, 1, w.drainQueued)
		require.Equal(t, !bd.frontierEmpty(), w.drainShutByWindow,
			"the flag must be read from the live window, not assumed from the queue being non-empty")
	})

	t.Run("queued with an empty window is not shut", func(t *testing.T) {
		h := newParkWiringHarness(t, true)
		h.sm.dispatcher = newBlockDispatcher(h.sm)

		require.True(t, h.sm.dispatcher.frontierEmpty(), "precondition: a fresh window holds nothing")

		h.sm.drainQueue = []drainRequest{{parent: h.blocks[0].MsgBlock().BlockHash()}}
		h.sm.publishConsumerWait(time.Now(), true, nil)

		w, _ := h.sm.consumerWaitState.Load().(*consumerWait)
		require.NotNil(t, w)
		require.Equal(t, 1, w.drainQueued)
		require.False(t, w.drainShutByWindow,
			"a drain that can run must not be reported as wedged, or the next reader is sent to the wrong place")
	})
}
