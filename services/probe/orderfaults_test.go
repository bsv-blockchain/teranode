package probe

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type hubEvent struct {
	ok bool
}

var (
	success = hubEvent{ok: true}
	failure = hubEvent{ok: false}
)

func apply(f *faults, seq uint64, events []hubEvent) {
	for _, e := range events {
		if e.ok {
			f.hub.succeededAt(seq)
		} else {
			f.hub.failedAt(seq, "websocket client writer", "x")
		}
	}
}

func TestHubStreakIsOrderIndependent(t *testing.T) {
	orderings := [][]hubEvent{
		{success, failure},
		{failure, success},
		{success, success, failure},
		{failure, failure, success},
		{success, failure, success},
		{failure, success, failure},
	}

	n := len(orderings)
	combos := 1

	for range maxConsecutivePanics {
		combos *= n
	}

	for combo := range combos {
		f := newFaults(&captureLogger{})
		pick := combo

		for seq := uint64(1); seq <= maxConsecutivePanics; seq++ {
			apply(f, seq, orderings[pick%n])
			pick /= n

			if seq < maxConsecutivePanics {
				require.False(t, f.signal.stopped(), "combo %d seq %d", combo, seq)
				require.Equal(t, int(seq), hubCounterStreak(f), "combo %d seq %d", combo, seq)
			}
		}

		require.Equal(t, maxConsecutivePanics, hubCounterStreak(f), "combo %d", combo)
		require.True(t, f.signal.stopped(), "combo %d", combo)
	}
}

func TestHubCleanBroadcastResetsRegardlessOfOrder(t *testing.T) {
	for _, clean := range [][]hubEvent{{success}, {success, success}} {
		f := newFaults(&captureLogger{})

		apply(f, 1, []hubEvent{success, failure})
		apply(f, 2, []hubEvent{failure, success})
		apply(f, 3, clean)
		require.Zero(t, hubCounterStreak(f))

		apply(f, 4, []hubEvent{success, failure})
		require.Equal(t, 1, hubCounterStreak(f))
		require.False(t, f.signal.stopped())
	}
}

func TestLateFailureForOlderSeqIsIgnored(t *testing.T) {
	f := newFaults(&captureLogger{})

	f.hub.failedAt(10, "websocket client writer", "x")
	f.hub.succeededAt(12)
	f.hub.failedAt(11, "websocket client writer", "late")

	require.Zero(t, hubCounterStreak(f))
	require.False(t, f.signal.stopped())
}

func TestMixedWritersStopRegardlessOfRecordingOrder(t *testing.T) {
	for run := range 20 {
		logger := &captureLogger{}

		var writes atomic.Int64

		srv := newProbe(t, probeOpts{logger: logger})
		srv.setFaultHook(func(point string) {
			if point == faultClientWrite && writes.Add(1)%2 == 0 {
				panic("injected writer fault")
			}
		})
		require.NoError(t, srv.Init(context.Background()))

		cancel, errCh := startServer(t, srv)

		first := reconnectLoop(t, srv, time.Millisecond)
		second := reconnectLoop(t, srv, time.Millisecond)

		select {
		case err := <-errCh:
			require.NoError(t, err, "run %d", run)
		case <-time.After(15 * time.Second):
			cancel()
			first()
			second()
			require.FailNow(t, "probe did not stop itself", "run %d, %d panic lines", run, countLines(logger.Lines(), "panicked"))
		}

		first()
		second()
		cancel()

		status, msg, err := srv.Health(context.Background(), false)
		require.NoError(t, err)
		require.Equal(t, 200, status)
		require.Equal(t, probeStoppedHealth, msg)
		requireStopLogged(t, logger.Lines(), "websocket hub")
	}
}

func stepClock() func() time.Time {
	var n atomic.Int64

	base := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)

	return func() time.Time { return base.Add(time.Duration(n.Add(1)) * 2 * time.Second) }
}

func TestHubPanicLoggingIsBounded(t *testing.T) {
	logger := &captureLogger{}
	clock := newFakeClock()

	f := newFaults(logger)
	f.setClock(clock.Now)

	for ms := range 3000 {
		seq := uint64(ms/20 + 1)

		f.hub.failedAt(seq, "websocket client writer", "x")
		f.hub.failedAt(seq, "websocket client writer", "x")
		clock.Advance(time.Millisecond)
	}

	require.True(t, f.signal.stopped())

	logged := []string{}

	for _, l := range logger.Lines() {
		if countLines([]string{l}, "panicked") == 1 {
			logged = append(logged, l)
		}
	}

	require.Len(t, logged, 3)
	require.NotContains(t, logged[0], "suppressed")
	require.Contains(t, logged[1], "suppressed 1999 earlier hub panic lines")
	require.Contains(t, logged[2], "suppressed 1999 earlier hub panic lines")

	for _, l := range logged {
		require.NotContains(t, l, "\n")
	}
}

func TestHubPanicLoggingIsBoundedLive(t *testing.T) {
	logger := &captureLogger{}

	srv := newProbe(t, probeOpts{logger: logger})
	srv.setFaultHook(panicAt(faultClientWrite, -1))
	require.NoError(t, srv.Init(context.Background()))

	cancel, errCh := startServer(t, srv)
	defer cancel()

	start := time.Now()
	first := reconnectLoop(t, srv, time.Millisecond)
	second := reconnectLoop(t, srv, time.Millisecond)

	awaitSelfStop(t, srv, errCh)
	first()
	second()

	elapsed := time.Since(start)
	lines := logger.Lines()

	require.LessOrEqual(t, countLines(lines, "websocket client writer panicked"), 1+int(elapsed/time.Second))
	requireStopLogged(t, lines, "websocket hub")
}
