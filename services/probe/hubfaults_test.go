package probe

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
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

func TestPersistentClientWriterPanicSelfStopsAfterFive(t *testing.T) {
	logger := &captureLogger{}

	srv := newProbe(t, probeOpts{logger: logger})
	require.NoError(t, srv.Init(context.Background()))

	cancel, errCh := startServer(t, srv)
	defer cancel()

	addr := srv.Addr()

	for range maxConsecutivePanics {
		conn := authedWS(t, srv)
		readSnapshot(t, conn, 2*time.Second)
	}

	require.Equal(t, maxConsecutivePanics, srv.hubClients())

	srv.setFaultHook(panicAt(faultClientWrite, -1))

	awaitSelfStop(t, srv, errCh)
	requirePortFree(t, addr)

	lines := logger.Lines()
	require.Equal(t, maxConsecutivePanics, countLines(lines, "websocket client writer panicked"))
	requireStopLogged(t, lines, "websocket hub")
}

func TestCleanHubBroadcastResetsHubStreak(t *testing.T) {
	logger := &captureLogger{}

	var hits atomic.Int64

	srv := newProbe(t, probeOpts{logger: logger})
	srv.setFaultHook(func(point string) {
		if point != faultHub {
			return
		}

		n := hits.Add(1)
		if (n >= 1 && n <= 4) || (n >= 6 && n <= 9) {
			panic("injected hub fault")
		}
	})

	errCh := runProbe(t, srv)

	require.Eventually(t, func() bool { return hits.Load() >= 20 }, 10*time.Second, 5*time.Millisecond)

	select {
	case err := <-errCh:
		require.FailNow(t, "probe stopped", "%v", err)
	default:
	}

	status, msg, err := srv.Health(context.Background(), false)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	require.NotEqual(t, probeStoppedHealth, msg)
	require.Equal(t, 8, countLines(logger.Lines(), "websocket hub panicked"))
	require.Zero(t, countLines(logger.Lines(), probeStoppedMessage))
}

func TestCleanPipelineTickDoesNotResetHubStreak(t *testing.T) {
	f := newFaults(&captureLogger{})

	for range maxConsecutivePanics - 1 {
		f.hub.Panicked("websocket hub", "x")
	}

	f.pipeline.reset()
	require.False(t, f.signal.stopped())

	f.hub.Panicked("websocket hub", "x")
	require.True(t, f.signal.stopped())
	require.Contains(t, f.signal.Reason(), "websocket hub")
}

func TestCleanHubBroadcastDoesNotResetPipelineStreak(t *testing.T) {
	f := newFaults(&captureLogger{})

	for range maxConsecutivePanics - 1 {
		f.pipeline.Panicked("pipeline", "x")
	}

	f.hub.settle()
	f.hub.settle()
	require.False(t, f.signal.stopped())

	f.pipeline.Panicked("pipeline", "x")
	require.True(t, f.signal.stopped())
	require.Contains(t, f.signal.Reason(), "pipeline")
}

func TestHubStreakNeedsCleanBroadcastToReset(t *testing.T) {
	f := newFaults(&captureLogger{})

	f.hub.settle()
	f.hub.Panicked("websocket client writer", "x")
	f.hub.settle()
	f.hub.Panicked("websocket client writer", "x")
	f.hub.settle()
	f.hub.settle()
	f.hub.Panicked("websocket client writer", "x")

	f.hub.mu.Lock()
	streak := f.hub.streak
	f.hub.mu.Unlock()

	require.Equal(t, 1, streak)
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
