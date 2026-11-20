package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// A copy whose read has got to its full declared length is not judged. A racer that took over has
// its full body in its side file, but its stream stays active while it waits for the copy it took
// over to stop. 30 s later its live rate was zero, the race judged it a staller, and the next round
// of drops disconnected that honest peer. The full body is here, so the block is not raced either.
func TestRaceNeverJudgesACompleteCopy(t *testing.T) {
	r := newStreamRegistry()
	now := time.Now()

	trickler := newTestPeer(t, "10.0.0.1:8333")
	racer := newTestPeer(t, "10.0.0.2:8333")

	// 1 MB of 100 MB in 5 minutes.
	streamAt(r, 1, 1001, trickler, 100<<20, 1<<20, now.Add(-5*time.Minute))

	// The racer read its full body in 10 s and has waited 50 s since. The reader starts after the
	// 80-byte header, so a full read is the declared payload less the header.
	streamAt(r, 1, 1001, racer, 100<<20, 100<<20-wire.MaxBlockHeaderPayload, now.Add(-time.Minute))

	r.sampleStreams(now.Add(-40 * time.Second))
	r.sampleStreams(now)

	_, _, stalling, ok := r.pickRace(now, 1000, 1)
	require.False(t, ok, "the block's full body is here: it is not raced")
	require.Empty(t, stalling)
}

// The only copy of a block, read to its full length, is not judged while its conversion completes.
func TestRaceNeverJudgesTheOnlyCopyOnceItIsComplete(t *testing.T) {
	r := newStreamRegistry()
	now := time.Now()

	streamAt(r, 1, 1001, newTestPeer(t, "10.0.0.1:8333"), 100<<20, 100<<20-wire.MaxBlockHeaderPayload, now.Add(-2*time.Minute))
	r.sampleStreams(now.Add(-40 * time.Second))
	r.sampleStreams(now)

	_, _, _, ok := r.pickRace(now, 1000, 1)
	require.False(t, ok)

	// One byte short of the full length, the same copy is judged: its live rate is zero.
	r2 := newStreamRegistry()
	short := streamAt(r2, 1, 1001, newTestPeer(t, "10.0.0.2:8333"), 100<<20, 100<<20-wire.MaxBlockHeaderPayload-1, now.Add(-2*time.Minute))
	r2.sampleStreams(now.Add(-40 * time.Second))
	r2.sampleStreams(now)

	got, _, _, ok := r2.pickRace(now, 1000, 1)
	require.True(t, ok)
	require.Same(t, short, got)
}
