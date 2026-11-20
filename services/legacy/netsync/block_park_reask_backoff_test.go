package netsync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestReAskBackoff_TheWaitDoublesUpToItsCap pins the schedule: the free attempts wait nothing,
// then reAskBackoffBase, doubling, never past reAskBackoffMax.
func TestReAskBackoff_TheWaitDoublesUpToItsCap(t *testing.T) {
	for asked := 1; asked <= reAskFreeAttempts; asked++ {
		require.Zero(t, reAskBackoff(asked))
	}

	require.Equal(t, reAskBackoffBase, reAskBackoff(reAskFreeAttempts+1))
	require.Equal(t, 2*reAskBackoffBase, reAskBackoff(reAskFreeAttempts+2))
	require.Equal(t, 4*reAskBackoffBase, reAskBackoff(reAskFreeAttempts+3))
	require.Equal(t, reAskBackoffMax, reAskBackoff(reAskFreeAttempts+50))
}

// TestApplyParkDisposition_ARepeatedDropIsAskedForAgainOnABackoff is the review of 76be891d6:
// a drop whose cause survives a re-download (a persistent record_corrupt is the case the row's
// own comment names) re-downloaded and dropped the block in a loop at network speed, because
// each re-ask was a fresh getdata that no filter stopped once the previous copy had arrived.
//
// The block is dropped, asked for, delivered, and dropped again. The first reAskFreeAttempts
// asks go at once; the next waits for the park sweep to find it due, and not before; a commit
// ends the pacing.
func TestApplyParkDisposition_ARepeatedDropIsAskedForAgainOnABackoff(t *testing.T) {
	h, child := newReAskHarness(t)

	entry := parkedBlock{hash: child, peer: h.peer}

	dropAndDeliver := func() int {
		before := h.rec.getDataCount()

		h.sm.applyParkDisposition(entry, parkDispositionRecordCorrupt)

		return before
	}

	for i := 0; i < reAskFreeAttempts; i++ {
		before := dropAndDeliver()

		require.True(t, WaitUntil(func() bool { return h.rec.askedForSince(before, child) }, 5*time.Second),
			"drop %d is asked for again at once", i+1)

		// The copy arrives, which discharges the request, and is dropped again.
		h.sm.blockDownloads.Delivered(h.peer, child)
	}

	before := dropAndDeliver()

	require.False(t, WaitUntil(func() bool { return h.rec.askedForSince(before, child) }, 300*time.Millisecond),
		"a drop past the free attempts is not asked for again at once")

	h.sm.sendDueReAsks(time.Now())
	require.False(t, WaitUntil(func() bool { return h.rec.askedForSince(before, child) }, 300*time.Millisecond),
		"nor before its wait has passed")

	// Another drop while the paced re-ask waits does not pull it forward.
	h.sm.applyParkDisposition(entry, parkDispositionRecordCorrupt)
	require.False(t, WaitUntil(func() bool { return h.rec.askedForSince(before, child) }, 300*time.Millisecond),
		"a drop while a re-ask waits leaves the wait as it is")

	h.sm.sendDueReAsks(time.Now().Add(reAskBackoffBase + time.Second))
	require.True(t, WaitUntil(func() bool { return h.rec.askedForSince(before, child) }, 5*time.Second),
		"once the wait has passed the park sweep asks for the block again")

	// The next drop waits twice as long.
	h.sm.blockDownloads.Delivered(h.peer, child)

	before = dropAndDeliver()

	h.sm.sendDueReAsks(time.Now().Add(reAskBackoffBase + time.Second))
	require.False(t, WaitUntil(func() bool { return h.rec.askedForSince(before, child) }, 300*time.Millisecond),
		"the wait doubles")

	h.sm.sendDueReAsks(time.Now().Add(2*reAskBackoffBase + time.Second))
	require.True(t, WaitUntil(func() bool { return h.rec.askedForSince(before, child) }, 5*time.Second))

	// A commit ends the pacing.
	h.sm.applyParkDisposition(entry, parkDispositionCommitted)

	h.sm.reAsksMu.Lock()
	_, tracked := h.sm.reAsks[child]
	h.sm.reAsksMu.Unlock()

	require.False(t, tracked, "a block in the chain is not wanted back, so its pacing record is gone")
}

// TestSendDueReAsks_ForgetsAnIdleRecord pins the bound on records nothing else ends: a block
// that committed by a route other than the park's committed row.
func TestSendDueReAsks_ForgetsAnIdleRecord(t *testing.T) {
	h, child := newReAskHarness(t)

	require.True(t, h.sm.admitDroppedReAsk(parkedBlock{hash: child, peer: h.peer}, time.Now()))

	h.sm.sendDueReAsks(time.Now().Add(reAskForgetAfter + time.Second))

	h.sm.reAsksMu.Lock()
	defer h.sm.reAsksMu.Unlock()

	require.Empty(t, h.sm.reAsks)
}
