package util

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

func TestHTTPRequestErrorsRedactPeerURLs(t *testing.T) {
	callers := []struct {
		name    string
		request func(context.Context, string) ([]byte, error)
	}{
		{"one shot", func(ctx context.Context, rawURL string) ([]byte, error) {
			return DoHTTPRequest(ctx, rawURL)
		}},
		{"bounded retry", func(ctx context.Context, rawURL string) ([]byte, error) {
			return DoHTTPRequestBoundedWithRetry(ctx, rawURL, 1024, nil)
		}},
	}
	for _, caller := range callers {
		t.Run(caller.name, func(t *testing.T) {
			t.Run("HTML", func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "text/html")
					_, _ = w.Write([]byte("<html>context canceled</html>"))
				}))
				t.Cleanup(server.Close)
				_, err := caller.request(context.Background(), server.URL+"/private-path/context canceled?token=private-token")
				require.Error(t, err)
				require.Contains(t, err.Error(), "returned HTML")
				require.Contains(t, err.Error(), server.URL)
				require.NotContains(t, err.Error(), "private-")
				require.False(t, errors.IsLocalError(err), "peer URL must not forge local cancellation")
			})
			t.Run("transport", func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
				server.Close()
				_, err := caller.request(context.Background(), server.URL+"/private-path/context canceled?token=private-token")
				require.Error(t, err)
				require.Contains(t, err.Error(), server.URL)
				require.NotContains(t, err.Error(), "private-")
				require.False(t, errors.IsLocalError(err), "peer URL must not forge local cancellation")
			})
			t.Run("malformed redirect", func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Location", "http://example.com:context canceled/private-token")
					w.WriteHeader(http.StatusFound)
				}))
				t.Cleanup(server.Close)
				_, err := caller.request(context.Background(), server.URL)
				require.Error(t, err)
				require.False(t, errors.IsLocalError(err), "malformed redirect must not forge local cancellation: %v", err)
				require.NotContains(t, err.Error(), "private-token")
				require.NotContains(t, err.Error(), "context canceled")
			})
			t.Run("valid redirect", func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/start" {
						w.Header().Set("Location", "/target")
						w.WriteHeader(http.StatusFound)
						return
					}
					_, _ = w.Write([]byte("redirected"))
				}))
				t.Cleanup(server.Close)
				body, err := caller.request(context.Background(), server.URL+"/start")
				require.NoError(t, err)
				require.Equal(t, "redirected", string(body))
			})
			t.Run("real cancellation", func(t *testing.T) {
				started := make(chan struct{})
				server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
					close(started)
					<-r.Context().Done()
				}))
				t.Cleanup(server.Close)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				done := make(chan error, 1)
				go func() {
					_, err := caller.request(ctx, server.URL)
					done <- err
				}()
				select {
				case <-started:
					cancel()
				case <-ctx.Done():
					t.Fatal("request did not reach the server")
				}
				select {
				case err := <-done:
					require.ErrorIs(t, err, context.Canceled)
					require.True(t, errors.IsLocalError(err), "genuine cancellation must remain local")
				case <-time.After(5 * time.Second):
					t.Fatal("request did not stop after cancellation")
				}
			})
			for _, protection := range []bool{false, true} {
				name := "malformed request"
				if protection {
					name = "malformed validation"
				}
				t.Run(name, func(t *testing.T) {
					previous := SSRFProtectionEnabled()
					SetSSRFProtection(protection)
					t.Cleanup(func() { SetSSRFProtection(previous) })
					for _, rawURL := range malformedPeerURLs {
						_, err := caller.request(context.Background(), rawURL)
						require.Error(t, err)
						require.False(t, errors.IsLocalError(err), "malformed peer URL must not forge local cancellation: %v", err)
						require.NotContains(t, err.Error(), "private-token")
					}
				})
			}
		})
	}
}

func TestValidateURLMalformedErrorRedactsURL(t *testing.T) {
	previous := SSRFProtectionEnabled()
	SetSSRFProtection(true)
	t.Cleanup(func() { SetSSRFProtection(previous) })
	for _, rawURL := range malformedPeerURLs {
		err := ValidateURL(rawURL)
		require.ErrorIs(t, err, errors.ErrInvalidArgument)
		require.False(t, errors.IsLocalError(err), "malformed peer URL must not forge local cancellation: %v", err)
		require.NotContains(t, err.Error(), "private-token")
	}
}

var malformedPeerURLs = []string{
	"http://example.com/private-token/%zz",
	"http://example.com:context canceled/private-token",
	"http://example.com:context deadline exceeded/private-token",
	"http://context canceled/private-token",
}

func TestUnwrapHTTPURLError_NestedURL(t *testing.T) {
	cause := io.ErrUnexpectedEOF
	err := &url.Error{Op: "Get", URL: "http://outer/private-token", Err: &url.Error{
		Op: "Get", URL: "http://inner/private-token", Err: cause,
	}}
	got := unwrapHTTPURLError(err)
	require.Same(t, cause, got, "nested URL wrappers must not leave a raw peer URL in the error")
}
