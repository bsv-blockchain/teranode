package probe

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func dialWSAddr(addr string) (*websocket.Conn, *http.Response, error) {
	return websocket.DefaultDialer.Dial("ws://"+addr+"/ws", nil)
}

func reconnectLoop(t *testing.T, srv *Server, every time.Duration) func() {
	t.Helper()

	addr := srv.Addr()
	stop := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)

		ticker := time.NewTicker(every)
		defer ticker.Stop()

		for {
			conn, resp, err := dialWSAddr(addr)
			if err == nil {
				_ = resp.Body.Close()

				if conn.WriteJSON(map[string]string{"auth": testToken}) == nil {
					_ = conn.SetReadDeadline(time.Now().Add(every))

					for {
						if _, _, rerr := conn.ReadMessage(); rerr != nil {
							break
						}
					}
				}

				_ = conn.Close()
			}

			select {
			case <-stop:
				return
			case <-ticker.C:
			}
		}
	}()

	return func() {
		close(stop)
		<-done
	}
}

func requireRunning(t *testing.T, srv *Server, errCh chan error) {
	t.Helper()

	select {
	case err := <-errCh:
		require.FailNow(t, "probe stopped", "%v", err)
	default:
	}

	status, msg, err := srv.Health(context.Background(), false)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	require.NotEqual(t, probeStoppedHealth, msg)
}

func hubStreak(srv *Server) int {
	run := srv.current()

	run.faults.hub.mu.Lock()
	defer run.faults.hub.mu.Unlock()

	return run.faults.hub.streak
}

func TestPersistentWriterPanicWithSlowReconnectSelfStops(t *testing.T) {
	logger := &captureLogger{}

	srv := newProbe(t, probeOpts{logger: logger})
	srv.logClock = stepClock()
	srv.setFaultHook(panicAt(faultClientWrite, -1))
	require.NoError(t, srv.Init(context.Background()))

	cancel, errCh := startServer(t, srv)
	defer cancel()

	addr := srv.Addr()
	stopReconnect := reconnectLoop(t, srv, 3*testSampleInterval)

	awaitSelfStop(t, srv, errCh)
	stopReconnect()
	requirePortFree(t, addr)

	lines := logger.Lines()
	require.Equal(t, maxConsecutivePanics, countLines(lines, "websocket client writer panicked"))
	requireStopLogged(t, lines, "websocket hub")
}

func TestPersistentClientWriterPanicSelfStopsAfterFive(t *testing.T) {
	logger := &captureLogger{}

	srv := newProbe(t, probeOpts{logger: logger})
	srv.logClock = stepClock()
	srv.setFaultHook(panicAt(faultClientWrite, -1))
	require.NoError(t, srv.Init(context.Background()))

	cancel, errCh := startServer(t, srv)
	defer cancel()

	addr := srv.Addr()
	stopReconnect := reconnectLoop(t, srv, time.Millisecond)

	awaitSelfStop(t, srv, errCh)
	stopReconnect()
	requirePortFree(t, addr)

	lines := logger.Lines()
	require.GreaterOrEqual(t, countLines(lines, "websocket client writer panicked"), maxConsecutivePanics)
	requireStopLogged(t, lines, "websocket hub")
}

func TestBroadcastToZeroClientsIsNeutral(t *testing.T) {
	logger := &captureLogger{}

	var hits atomic.Int64

	srv := newProbe(t, probeOpts{logger: logger})
	srv.logClock = stepClock()
	srv.setFaultHook(func(point string) {
		if point != faultHub {
			return
		}

		if n := hits.Add(1); n <= 4 || n == 12 {
			panic("injected hub fault")
		}
	})
	require.NoError(t, srv.Init(context.Background()))

	cancel, errCh := startServer(t, srv)
	defer cancel()

	awaitSelfStop(t, srv, errCh)
	require.GreaterOrEqual(t, hits.Load(), int64(12))

	lines := logger.Lines()
	require.Equal(t, maxConsecutivePanics, countLines(lines, "websocket hub panicked"))
	requireStopLogged(t, lines, "websocket hub")
}

func TestSuccessfulClientWriteResetsStreak(t *testing.T) {
	logger := &captureLogger{}
	interval := 100 * time.Millisecond

	srv := newProbe(t, probeOpts{logger: logger, sampleInterval: interval})
	srv.logClock = stepClock()
	errCh := runProbe(t, srv)

	conn := authedWS(t, srv)
	readSnapshot(t, conn, 2*time.Second)

	go func() {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	var hits atomic.Int64

	srv.setFaultHook(func(point string) {
		if point != faultHub {
			return
		}

		if n := hits.Add(1); (n >= 1 && n <= 4) || (n >= 6 && n <= 9) {
			panic("injected hub fault")
		}
	})

	require.Eventually(t, func() bool { return hits.Load() >= 14 }, 20*time.Second, 5*time.Millisecond)

	requireRunning(t, srv, errCh)
	require.Equal(t, 8, countLines(logger.Lines(), "websocket hub panicked"))
	require.Zero(t, countLines(logger.Lines(), probeStoppedMessage))
}

func TestManyWritersPanickingOnOneSnapshotCountOnce(t *testing.T) {
	logger := &captureLogger{}
	interval := 100 * time.Millisecond

	srv := newProbe(t, probeOpts{logger: logger, sampleInterval: interval})
	errCh := runProbe(t, srv)

	for range maxConsecutivePanics {
		conn := authedWS(t, srv)
		readSnapshot(t, conn, 2*time.Second)
	}

	require.Equal(t, maxConsecutivePanics, srv.hubClients())

	var armed atomic.Bool

	srv.setFaultHook(func(point string) {
		if point == faultClientWrite && armed.Load() {
			panic("injected writer fault")
		}
	})

	time.Sleep(interval / 2)
	armed.Store(true)

	require.Eventually(t, func() bool {
		return srv.hubClients() == 0 && countLines(logger.Lines(), "websocket client writer panicked") == 1
	}, 5*time.Second, 5*time.Millisecond)
	require.Equal(t, 1, hubStreak(srv))

	requireRunning(t, srv, errCh)
}

func TestMoveAsideReportsRealError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}

	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	locked := filepath.Join(dir, "locked")

	require.NoError(t, os.WriteFile(src, []byte("data"), 0o600))
	require.NoError(t, os.MkdirAll(locked, 0o700))
	require.NoError(t, os.Chmod(locked, 0o000))

	t.Cleanup(func() { _ = os.Chmod(locked, 0o700) })

	err := moveAside(src, filepath.Join(locked, "dst"))
	require.Error(t, err)
	require.ErrorIs(t, err, os.ErrPermission)
	require.NotErrorIs(t, err, os.ErrExist)

	srv := newProbe(t, probeOpts{})
	_, err = srv.moveCorrupt(filepath.Join(locked, "state.json"))
	require.ErrorIs(t, err, os.ErrPermission)
	require.NotContains(t, err.Error(), "no free name")

	raw, err := os.ReadFile(src)
	require.NoError(t, err)
	require.Equal(t, "data", string(raw))
}
