package util

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"syscall"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

var peerHTTPErrorCallers = []struct {
	name    string
	request func(context.Context, string) error
}{
	{"one shot", func(ctx context.Context, rawURL string) error {
		_, err := DoHTTPRequest(ctx, rawURL)
		return err
	}},
	{"bounded retry", func(ctx context.Context, rawURL string) error {
		_, err := doHTTPRequestBoundedWithRetry(ctx, rawURL, 1024, testRetryConfig, nil)
		return err
	}},
	{"reader retry", func(ctx context.Context, rawURL string) error {
		body, err := doHTTPRequestBodyReaderWithRetry(ctx, rawURL, testRetryConfig, nil)
		if body != nil {
			defer body.Close()
			_, err = io.ReadAll(body)
		}
		return err
	}},
}

// A real net/http parser can quote hostile header bytes in its error. Those
// bytes must never reach the legacy substring-based local-error classifiers.
func TestHTTPTransportErrorsDiscardPeerHeaderText(t *testing.T) {
	for _, caller := range peerHTTPErrorCallers {
		t.Run(caller.name, func(t *testing.T) {
			for _, response := range []string{
				"HTTP/1.1 200 OK\r\ncontext canceled private-token\r\n\r\n",
				"HTTP/1.1 200 OK\r\ncontext deadline exceeded private-token\r\n\r\n",
				"HTTP/1.1 context canceled private-token\r\n\r\n",
				"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTrailer: X-Trailer\r\n\r\n1\r\nx\r\n0\r\ncontext canceled private-token\r\n\r\n",
			} {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				require.NoError(t, err)
				t.Cleanup(func() { _ = listener.Close() })
				done := make(chan error, 1)
				go func() {
					conn, err := listener.Accept()
					if err != nil {
						done <- err
						return
					}
					defer conn.Close()
					_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
					_, err = http.ReadRequest(bufio.NewReader(conn))
					if err == nil {
						_, err = io.WriteString(conn, response)
					}
					done <- err
				}()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err = caller.request(ctx, "http://"+listener.Addr().String())
				cancel()
				require.NoError(t, <-done)
				require.Error(t, err)
				require.False(t, errors.IsLocalError(err), "peer protocol error must remain external: %v", err)
				require.False(t, errors.IsTransientLocalError(err), "peer protocol error must not look like local service failure: %v", err)
				require.ErrorIs(t, err, errors.ErrNetworkError)
				require.NotContains(t, err.Error(), "private-token")
			}
		})
	}
}

func TestHTTPTransportErrorsKeepConnectionRefused(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	server.Close()
	for _, caller := range peerHTTPErrorCallers {
		t.Run(caller.name, func(t *testing.T) {
			err := caller.request(context.Background(), server.URL)
			require.ErrorIs(t, err, errors.ErrNetworkConnectionRefused)
			require.False(t, errors.IsLocalError(err))
			require.False(t, errors.IsTransientLocalError(err))
		})
	}
}

func TestHTTPRetriesExhaustionIsPeerFailure(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
		t.Cleanup(server.Close)
		for _, caller := range peerHTTPErrorCallers[1:] {
			t.Run(http.StatusText(status)+"/"+caller.name, func(t *testing.T) {
				err := caller.request(context.Background(), server.URL)
				require.ErrorIs(t, err, errors.ErrNetworkError)
				require.False(t, errors.IsLocalError(err))
				require.False(t, errors.IsTransientLocalError(err), "exhausted peer retries must not resemble local overload")
			})
		}
	}
}

func TestHTTPRetriesKeepLocalPacingFailure(t *testing.T) {
	localErr := errors.NewServiceUnavailableError("local limiter unavailable")
	attempts := 0
	beforeAttempt := func(context.Context) error {
		attempts++
		return localErr
	}
	_, err := doHTTPRequestBoundedWithRetry(context.Background(), "http://unused.test", 1024, testRetryConfig, beforeAttempt)
	require.Same(t, localErr, err)
	require.Equal(t, 1, attempts, "local pacing failure must not become an exhausted peer failure")
	attempts = 0
	_, err = doHTTPRequestBodyReaderWithRetry(context.Background(), "http://unused.test", testRetryConfig, beforeAttempt)
	require.Same(t, localErr, err)
	require.Equal(t, 1, attempts)
}

func TestSanitizeHTTPTransportErrorStructuralClassification(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cause error
		want  error
		local bool
	}{
		{"foreign wrapped cancellation", fmt.Errorf("request stopped: %w", context.Canceled), errors.ErrContextCanceled, true},     //nolint:forbidigo // Exercise foreign standard-library wrappers, not teranode errors.
		{"foreign wrapped deadline", fmt.Errorf("request stopped: %w", context.DeadlineExceeded), errors.ErrNetworkTimeout, false}, //nolint:forbidigo // Exercise foreign standard-library wrappers, not teranode errors.
		{"network timeout", &net.DNSError{Err: "private-token context canceled", IsTimeout: true}, errors.ErrNetworkTimeout, false},
		{"connection refused", fmt.Errorf("private-token: %w", syscall.ECONNREFUSED), errors.ErrNetworkConnectionRefused, false}, //nolint:forbidigo // Exercise foreign standard-library wrappers, not teranode errors.
		{"forged cancellation", errors.NewServiceError("private-token context canceled"), errors.ErrNetworkError, false},
		{"forged deadline", errors.NewServiceError("private-token context deadline exceeded"), errors.ErrNetworkError, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := sanitizeHTTPTransportError(&url.Error{Op: "Get", URL: "http://peer.test/private-token", Err: tc.cause}, "http://peer.test/private-token")
			require.ErrorIs(t, err, tc.want)
			require.Equal(t, tc.local, errors.IsLocalError(err))
			require.NotContains(t, err.Error(), "private-token")
			require.False(t, errors.IsTransientLocalError(err))
			if tc.local {
				require.ErrorIs(t, err, context.Canceled)
			}
		})
	}
}

func TestLocalServiceHTTPTransportFailureStaysLocal(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	server.Close()
	_, err := DoLocalServiceHTTPRequestBodyReader(context.Background(), server.URL)
	require.ErrorIs(t, err, errors.ErrServiceError)
	require.True(t, errors.IsTransientLocalError(err), "operator-configured local service failure must stay local")
}
