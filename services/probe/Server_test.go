package probe

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/probe/guard"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

const testToken = "test-token"

func TestNewSetsDefaultWriteTimeout(t *testing.T) {
	srv := New(ulogger.TestLogger{}, settings.NewSettings(), nil, nil)
	require.Equal(t, defaultWriteTimeout, srv.writeTimeout)
}

func newTestServer(t *testing.T, token string) *Server {
	t.Helper()

	s := settings.NewSettings()
	s.Probe.HTTPListenAddress = "localhost:0"
	s.Probe.AuthToken = token
	s.Probe.StatePath = filepath.Join(t.TempDir(), "probe_state.json")

	srv := New(ulogger.TestLogger{}, s, nil, nil)
	srv.collectors = func(context.Context, guard.Recorder) collectorSet {
		return collectorSet{fast: []Collector{newFake("metrics", okPatch(healthyPatch))}}
	}

	return srv
}

func startServer(t *testing.T, srv *Server) (context.CancelFunc, chan error) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	errCh := make(chan error, 1)

	go func() { errCh <- srv.Start(ctx, ready) }()

	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("server did not become ready")
	}

	return cancel, errCh
}

func get(t *testing.T, url, token string) int {
	t.Helper()

	req, err := http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)

	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	return resp.StatusCode
}

func waitForClose(t *testing.T, conn net.Conn, within time.Duration) {
	t.Helper()

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(within)))

	buf := make([]byte, 4096)

	for {
		if _, err := conn.Read(buf); err != nil {
			require.NotErrorIs(t, err, os.ErrDeadlineExceeded)

			return
		}
	}
}

func TestIdleKeepAliveConnectionIsClosed(t *testing.T) {
	srv := newTestServer(t, testToken)
	srv.idleTimeout = 200 * time.Millisecond

	cancel, errCh := startServer(t, srv)
	defer func() {
		cancel()
		require.NoError(t, <-errCh)
	}()

	conn, err := net.Dial("tcp", srv.Addr())
	require.NoError(t, err)

	defer func() { _ = conn.Close() }()

	_, err = conn.Write([]byte("GET /index.html HTTP/1.1\r\nHost: probe\r\n\r\n"))
	require.NoError(t, err)

	waitForClose(t, conn, 5*time.Second)
}

func TestSlowRequestBodyConnectionIsClosed(t *testing.T) {
	srv := newTestServer(t, testToken)
	srv.readTimeout = 200 * time.Millisecond

	cancel, errCh := startServer(t, srv)
	defer func() {
		cancel()
		require.NoError(t, <-errCh)
	}()

	conn, err := net.Dial("tcp", srv.Addr())
	require.NoError(t, err)

	defer func() { _ = conn.Close() }()

	_, err = conn.Write([]byte("POST /api/snooze HTTP/1.1\r\nHost: probe\r\nAuthorization: Bearer " + testToken + "\r\nContent-Length: 100\r\n\r\n{"))
	require.NoError(t, err)

	waitForClose(t, conn, 5*time.Second)
}

func TestInitFailsWithEmptyToken(t *testing.T) {
	srv := newTestServer(t, "")

	require.Error(t, srv.Init(context.Background()))
}

func TestInitRejectsUnknownFloorKey(t *testing.T) {
	srv := newTestServer(t, testToken)
	srv.settings.Probe.Floors = []string{"cache.hitRate=0.9", "cache.hitrate=0.9"}

	err := srv.Init(context.Background())
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrConfiguration))
	require.Contains(t, err.Error(), "cache.hitrate")

	srv.settings.Probe.Floors = []string{"cache.hitRate=0.9", "chain.persisterBehind=5"}
	require.NoError(t, srv.Init(context.Background()))
}

func TestHealthRequiresToken(t *testing.T) {
	srv := newTestServer(t, testToken)
	require.NoError(t, srv.Init(context.Background()))

	cancel, errCh := startServer(t, srv)
	defer func() {
		cancel()
		require.NoError(t, <-errCh)
	}()

	url := "http://" + srv.Addr() + "/health"

	require.Equal(t, http.StatusUnauthorized, get(t, url, ""))
	require.Equal(t, http.StatusUnauthorized, get(t, url, "wrong"))
	require.Equal(t, http.StatusOK, get(t, url, testToken))
}

func TestEmptyTokenNeverAuthenticates(t *testing.T) {
	srv := newTestServer(t, "")

	ts := httptest.NewServer(srv.newRouter())
	defer ts.Close()

	require.Equal(t, http.StatusUnauthorized, get(t, ts.URL+"/health", ""))

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/health", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer ")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestStartRefusesEmptyToken(t *testing.T) {
	srv := newTestServer(t, "")

	require.Error(t, srv.Start(context.Background(), make(chan struct{})))
	require.Empty(t, srv.Addr())
}

func TestStartReleasesPortOnStop(t *testing.T) {
	srv := newTestServer(t, testToken)
	require.NoError(t, srv.Init(context.Background()))

	cancel, errCh := startServer(t, srv)
	addr := srv.Addr()

	cancel()
	require.NoError(t, srv.Stop(context.Background()))
	require.NoError(t, <-errCh)

	ln, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	require.NoError(t, ln.Close())
}

func TestStalledReaderCannotHoldAResponseWriteOpen(t *testing.T) {
	srv := newTestServer(t, testToken)
	srv.writeTimeout = 300 * time.Millisecond

	returned := make(chan struct{})

	httpSrv := srv.newHTTPServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		defer close(returned)

		chunk := make([]byte, 1<<20)

		for range 256 {
			if _, err := w.Write(chunk); err != nil {
				return
			}
		}
	}))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() { _ = httpSrv.Serve(ln) }()

	defer func() { _ = httpSrv.Close() }()

	dialer := net.Dialer{Control: func(_, _ string, c syscall.RawConn) error {
		return c.Control(func(fd uintptr) {
			_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 2048)
		})
	}}

	conn, err := dialer.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)

	defer func() { _ = conn.Close() }()

	_, err = conn.Write([]byte("GET /view.js HTTP/1.1\r\nHost: probe\r\n\r\n"))
	require.NoError(t, err)

	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		t.Fatal("the handler is still blocked in a write to a client that does not read")
	}
}

func TestWebSocketOutlivesTheHTTPTimeouts(t *testing.T) {
	srv := newTestServer(t, testToken)
	srv.readTimeout = 200 * time.Millisecond
	srv.writeTimeout = 200 * time.Millisecond
	srv.idleTimeout = 200 * time.Millisecond

	cancel, errCh := startServer(t, srv)
	defer func() {
		cancel()
		_ = <-errCh
	}()

	conn := authedWS(t, srv)

	defer func() { _ = conn.Close() }()

	deadline := time.Now().Add(1500 * time.Millisecond)

	for time.Now().Before(deadline) {
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))

		_, _, err := conn.ReadMessage()
		require.NoError(t, err, "the socket must stay open past the read, write and idle timeouts")
	}
}
