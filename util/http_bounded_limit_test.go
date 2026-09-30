package util

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

func TestBoundedHTTPRequestRejectsNegativeLimit(t *testing.T) {
	protected := SSRFProtectionEnabled()
	SetSSRFProtection(false)
	t.Cleanup(func() { SetSSRFProtection(protected) })
	var requests, hooks atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = w.Write(make([]byte, 1<<20))
	}))
	t.Cleanup(server.Close)

	for _, retry := range []bool{false, true} {
		name := "one shot"
		if retry {
			name = "retry"
		}
		t.Run(name, func(t *testing.T) {
			requests.Store(0)
			hooks.Store(0)
			var body []byte
			var err error
			if retry {
				body, err = DoHTTPRequestBoundedWithRetry(context.Background(), server.URL, -1, func(context.Context) error {
					hooks.Add(1)
					return nil
				})
			} else {
				body, err = DoHTTPRequestBounded(context.Background(), server.URL, -1)
			}
			require.ErrorIs(t, err, errors.ErrConfiguration)
			require.True(t, errors.IsLocalError(err))
			require.Nil(t, body)
			require.Zero(t, requests.Load(), "invalid receive limits must fail before HTTP")
			require.Zero(t, hooks.Load(), "invalid receive limits must fail before local pacing")
		})
	}
}

func TestBoundedHTTPRequestZeroLimit(t *testing.T) {
	protected := SSRFProtectionEnabled()
	SetSSRFProtection(false)
	t.Cleanup(func() { SetSSRFProtection(protected) })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/nonempty" {
			_, _ = w.Write([]byte("x"))
		}
	}))
	t.Cleanup(server.Close)

	for _, request := range []func(context.Context, string, int64) ([]byte, error){
		func(ctx context.Context, url string, maxBytes int64) ([]byte, error) {
			return DoHTTPRequestBounded(ctx, url, maxBytes)
		},
		func(ctx context.Context, url string, maxBytes int64) ([]byte, error) {
			return DoHTTPRequestBoundedWithRetry(ctx, url, maxBytes, nil)
		},
	} {
		body, err := request(context.Background(), server.URL, 0)
		require.NoError(t, err)
		require.Empty(t, body)
		body, err = request(context.Background(), server.URL+"/nonempty", 0)
		require.ErrorIs(t, err, errors.ErrExternal)
		require.Nil(t, body)
	}
}
