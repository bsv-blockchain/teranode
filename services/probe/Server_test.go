package probe

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

const testToken = "test-token"

func newTestServer(t *testing.T, token string) *Server {
	t.Helper()

	s := settings.NewSettings()
	s.Probe.HTTPListenAddress = "localhost:0"
	s.Probe.AuthToken = token

	return New(ulogger.TestLogger{}, s, nil, nil)
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

func TestInitFailsWithEmptyToken(t *testing.T) {
	srv := newTestServer(t, "")

	require.Error(t, srv.Init(context.Background()))
}

func TestHealthRequiresToken(t *testing.T) {
	srv := newTestServer(t, testToken)
	require.NoError(t, srv.Init(context.Background()))

	cancel, errCh := startServer(t, srv)
	defer func() {
		cancel()
		<-errCh
	}()

	url := "http://" + srv.Addr() + "/health"

	require.Equal(t, http.StatusUnauthorized, get(t, url, ""))
	require.Equal(t, http.StatusUnauthorized, get(t, url, "wrong"))
	require.Equal(t, http.StatusOK, get(t, url, testToken))
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
