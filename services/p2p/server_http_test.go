package p2p

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// newHTTPTestServer builds a minimal server for exercising StartHTTP and Health.
// Each test gets a unique settings context because util.GetListener caches
// listeners per (context, service, schema) key.
func newHTTPTestServer(t *testing.T, name string, securityLevel int, certFile, keyFile string) *Server {
	t.Helper()

	s := &Server{
		logger: ulogger.TestLogger{},
		gCtx:   context.Background(),
		settings: &settings.Settings{
			Context:           fmt.Sprintf("test-p2p-http-%s", name),
			SecurityLevelHTTP: securityLevel,
			ServerCertFile:    certFile,
			ServerKeyFile:     keyFile,
			P2P: settings.P2PSettings{
				HTTPListenAddress: "127.0.0.1:0",
			},
		},
	}
	s.e = s.setupHTTPServer()

	return s
}

func TestStartHTTPMissingCertFailsStartup(t *testing.T) {
	s := newHTTPTestServer(t, "missing-cert", 1, "", "some.key")

	err := s.StartHTTP(context.Background())
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrConfiguration))
	require.Contains(t, err.Error(), "server_certFile")
}

func TestStartHTTPMissingKeyFailsStartup(t *testing.T) {
	s := newHTTPTestServer(t, "missing-key", 1, "some.crt", "")

	err := s.StartHTTP(context.Background())
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrConfiguration))
	require.Contains(t, err.Error(), "server_keyFile")
}

func TestStartHTTPServeErrorSurfacesInHealth(t *testing.T) {
	// Cert/key are set but unreadable, so ServeTLS fails after StartHTTP returns.
	s := newHTTPTestServer(t, "bad-cert", 1, "/nonexistent/tls.crt", "/nonexistent/tls.key")

	ctx := t.Context()

	require.NoError(t, s.StartHTTP(ctx))

	require.Eventually(t, func() bool {
		return s.httpServeError() != nil
	}, 5*time.Second, 10*time.Millisecond, "serve error was not recorded")

	status, _, err := s.Health(ctx, true)
	require.Error(t, err)
	require.Equal(t, http.StatusServiceUnavailable, status)

	status, msg, err := s.Health(ctx, false)
	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, status)
	require.Contains(t, msg, `"resource": "HTTPServer", "status": "503"`)
}

func TestStartHTTPServesAndShutsDownCleanly(t *testing.T) {
	s := newHTTPTestServer(t, "plain", 0, "", "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, s.StartHTTP(ctx))

	url := fmt.Sprintf("http://%s/health", s.e.Listener.Addr().String())

	var body []byte

	require.Eventually(t, func() bool {
		resp, err := http.Get(url) //nolint:gosec // test-local URL
		if err != nil {
			return false
		}
		defer resp.Body.Close()

		body, err = io.ReadAll(resp.Body)

		return err == nil && resp.StatusCode == http.StatusOK
	}, 5*time.Second, 10*time.Millisecond, "HTTP endpoint never came up")
	require.Equal(t, "OK", string(body))

	status, _, err := s.Health(ctx, true)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)

	// Graceful shutdown must not be reported as a serve failure.
	cancel()
	require.Eventually(t, func() bool {
		_, err := http.Get(url) //nolint:gosec // test-local URL
		return err != nil
	}, 5*time.Second, 10*time.Millisecond, "HTTP endpoint never shut down")

	// Give the serve goroutine time to observe ErrServerClosed and (not) record it.
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, s.httpServeError())

	status, _, err = s.Health(context.Background(), true)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
}
