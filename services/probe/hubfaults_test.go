package probe

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func requireStopLogged(t *testing.T, lines []string, component string) {
	t.Helper()

	stops := 0

	for _, l := range lines {
		if countLines([]string{l}, probeStoppedMessage) == 1 {
			stops++

			require.Contains(t, l, component)
			require.Contains(t, l, "ERROR")
		}
	}

	require.Equal(t, 1, stops)
}

func requirePortFree(t *testing.T, addr string) {
	t.Helper()

	ln, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	require.NoError(t, ln.Close())
}

func TestPersistentHubPanicSelfStopsAfterFive(t *testing.T) {
	logger := &captureLogger{}

	srv := newProbe(t, probeOpts{logger: logger})
	srv.logClock = stepClock()
	srv.setFaultHook(panicAt(faultHub, -1))
	require.NoError(t, srv.Init(context.Background()))

	cancel, errCh := startServer(t, srv)
	defer cancel()

	addr := srv.Addr()

	awaitSelfStop(t, srv, errCh)
	requirePortFree(t, addr)

	lines := logger.Lines()
	require.Equal(t, maxConsecutivePanics, countLines(lines, "websocket hub panicked"))
	requireStopLogged(t, lines, "websocket hub")
}

func TestCleanPipelineTickDoesNotResetHubStreak(t *testing.T) {
	f := newFaults(&captureLogger{})

	for seq := uint64(1); seq < maxConsecutivePanics; seq++ {
		f.hub.failedAt(seq, "websocket hub", "x")
	}

	f.pipeline.reset()
	require.False(t, f.signal.stopped())

	f.hub.failedAt(maxConsecutivePanics, "websocket hub", "x")
	require.True(t, f.signal.stopped())
	require.Contains(t, f.signal.Reason(), "websocket hub")
}

func TestCleanHubBroadcastDoesNotResetPipelineStreak(t *testing.T) {
	f := newFaults(&captureLogger{})

	for range maxConsecutivePanics - 1 {
		f.pipeline.Panicked("pipeline", "x")
	}

	f.hub.succeededAt(1)
	f.hub.succeededAt(2)
	require.False(t, f.signal.stopped())

	f.pipeline.Panicked("pipeline", "x")
	require.True(t, f.signal.stopped())
	require.Contains(t, f.signal.Reason(), "pipeline")
}

func hubCounterStreak(f *faults) int {
	f.hub.mu.Lock()
	defer f.hub.mu.Unlock()

	return f.hub.streak
}

func TestHubCountsOneFailurePerBroadcast(t *testing.T) {
	f := newFaults(&captureLogger{})

	for range maxConsecutivePanics {
		f.hub.failedAt(1, "websocket client writer", "x")
	}

	require.Equal(t, 1, hubCounterStreak(f))
	require.False(t, f.signal.stopped())

	for seq := uint64(2); seq < maxConsecutivePanics; seq++ {
		for range maxConsecutivePanics {
			f.hub.failedAt(seq, "websocket client writer", "x")
		}
	}

	require.Equal(t, maxConsecutivePanics-1, hubCounterStreak(f))
	require.False(t, f.signal.stopped())

	f.hub.failedAt(maxConsecutivePanics, "websocket client writer", "x")
	require.True(t, f.signal.stopped())
}

func TestHubSuccessOnlyResetsForNewerBroadcast(t *testing.T) {
	f := newFaults(&captureLogger{})

	f.hub.failedAt(3, "websocket client writer", "x")
	f.hub.failedAt(4, "websocket client writer", "x")
	f.hub.succeededAt(3)
	require.Equal(t, 2, hubCounterStreak(f))

	f.hub.succeededAt(4)
	require.Equal(t, 2, hubCounterStreak(f))

	f.hub.succeededAt(5)
	require.Zero(t, hubCounterStreak(f))

	f.hub.failedAt(4, "websocket client writer", "late")
	require.Zero(t, hubCounterStreak(f))
}

func TestTwoCorruptionsInOneSecondKeepBothFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "probe_state.json")
	fixed := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)

	srv := newProbe(t, probeOpts{statePath: path})
	srv.now = func() time.Time { return fixed }

	contents := []string{"first garbage", "second garbage", "third garbage"}

	for _, c := range contents {
		require.NoError(t, os.WriteFile(path, []byte(c), 0o600))

		store, reason, err := srv.openStore()
		require.NoError(t, err)
		require.Empty(t, reason)
		require.NoError(t, store.Close())
	}

	base := path + ".corrupt-20261028T120000Z"

	for i, name := range []string{base, base + "-1", base + "-2"} {
		raw, err := os.ReadFile(name)
		require.NoError(t, err)
		require.Equal(t, contents[i], string(raw))
	}

	matches, err := filepath.Glob(path + ".corrupt-*")
	require.NoError(t, err)
	require.Len(t, matches, 3)
}

func TestMoveAsideNeverOverwrites(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")

	require.NoError(t, os.WriteFile(src, []byte("new"), 0o600))
	require.NoError(t, os.WriteFile(dst, []byte("old"), 0o600))

	err := moveAside(src, dst)
	require.ErrorIs(t, err, os.ErrExist)

	raw, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.Equal(t, "old", string(raw))

	raw, err = os.ReadFile(src)
	require.NoError(t, err)
	require.Equal(t, "new", string(raw))
}
