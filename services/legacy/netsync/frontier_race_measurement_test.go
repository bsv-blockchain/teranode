package netsync

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestReadDelta_ReportsTheBytesItMeasured covers the split that lets the race's
// decline say what it measured. isPullingBytes answered yes or no, and on
// mainnet it answered yes 1,663 consecutive times through a stall while the
// peer's own socket had read nothing for twelve minutes. Whether the bytes were
// real decides which fault that is, so the figure has to reach the log.
func TestReadDelta_ReportsTheBytesItMeasured(t *testing.T) {
	const minSpeed = 1000

	tick := uint64(frontierCheckInterval.Seconds())
	require.NotZero(t, tick, "the tick has to be whole seconds for the floor to mean anything")

	t.Run("one sample is not a rate", func(t *testing.T) {
		s := &peerSyncState{}
		s.assocReadBytes.Store(1 << 20)
		s.throughputTicks.Store(1)

		delta, pulling := s.readDelta(minSpeed)

		require.Zero(t, delta, "a lifetime total is not a delta and must not be reported as one")
		require.False(t, pulling)
	})

	t.Run("above the floor reports the delta and says yes", func(t *testing.T) {
		s := &peerSyncState{}
		s.assocReadBytesLastTick.Store(1000)
		s.assocReadBytes.Store(1000 + minSpeed*tick)
		s.throughputTicks.Store(2)

		delta, pulling := s.readDelta(minSpeed)

		require.Equal(t, minSpeed*tick, delta)
		require.True(t, pulling)
	})

	t.Run("below the floor still reports the delta", func(t *testing.T) {
		s := &peerSyncState{}
		s.assocReadBytesLastTick.Store(1000)
		s.assocReadBytes.Store(1001)
		s.throughputTicks.Store(2)

		delta, pulling := s.readDelta(minSpeed)

		require.Equal(t, uint64(1), delta,
			"a trickle is the interesting case and its size is the whole point of reporting it")
		require.False(t, pulling)
	})

	t.Run("a shrinking total is not progress", func(t *testing.T) {
		s := &peerSyncState{}
		s.assocReadBytesLastTick.Store(5000)
		s.assocReadBytes.Store(4000)
		s.throughputTicks.Store(2)

		delta, pulling := s.readDelta(minSpeed)

		require.Zero(t, delta)
		require.False(t, pulling)
	})

	t.Run("isPullingBytes still answers the same question", func(t *testing.T) {
		s := &peerSyncState{}
		s.assocReadBytesLastTick.Store(1000)
		s.assocReadBytes.Store(1000 + minSpeed*tick)
		s.throughputTicks.Store(2)

		require.True(t, s.isPullingBytes(minSpeed))
		require.False(t, (*peerSyncState)(nil).isPullingBytes(minSpeed))
	})
}

// TestNoteRaceDeclinedAs_RateLimitsOnTheGivenKey is why the keyed variant exists.
// The measured decline carries a different byte count every tick, so a key
// derived from the message would give each one its own bucket and put the line
// in the log once every five seconds.
func TestNoteRaceDeclinedAs_RateLimitsOnTheGivenKey(t *testing.T) {
	sm := newRaceManager(t)

	for i := 0; i < 5; i++ {
		sm.noteRaceDeclinedAs("an owner is visibly pulling bytes",
			"an owner is visibly pulling bytes: pulled "+time.Duration(i).String()+" bytes")
	}

	sm.raceDeclinedMu.Lock()
	defer sm.raceDeclinedMu.Unlock()

	require.Len(t, sm.raceDeclinedAt, 1,
		"five differently worded declines must share one bucket, or the rate limit does nothing")
	require.Equal(t, 5, sm.raceDeclinedCount["an owner is visibly pulling bytes"],
		"the count has to keep rising while the log stays quiet")
}

// TestConsumerWait_Describe_NamesDeclinedDrainTurns covers the field that was
// collected and never rendered. A drain that walks its queue, rules every parent
// out and drops them leaves the queue length at zero, so without this a loop
// that has just thrown a turn away reads as a loop with no work.
func TestConsumerWait_Describe_NamesDeclinedDrainTurns(t *testing.T) {
	now := time.Now()

	w := &consumerWait{at: now, queueArmOpen: true, parked: 113, drainDeclines: 41}

	line := w.describe(now)

	require.Contains(t, line, "the drain has declined 41 turns",
		"a report that collects the count and prints nothing is the diagnostic stopping where it gets interesting")

	quiet := (&consumerWait{at: now, queueArmOpen: true}).describe(now)
	require.False(t, strings.Contains(quiet, "declined"),
		"a drain that has never declined a turn must not add a clause saying so")
}
