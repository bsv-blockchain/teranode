package util

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// errorBodyServer serves a fixed error body of exactly size bytes with the given
// status, and reports how many TCP connections were accepted so connection reuse is
// directly observable.
func errorBodyServer(t *testing.T, status, size int) (*httptest.Server, *atomic.Int64) {
	t.Helper()

	body := strings.Repeat("x", size)

	var conns atomic.Int64

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))

	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			conns.Add(1)
		}
	}

	srv.Start()
	t.Cleanup(srv.Close)

	return srv, &conns
}

// TestBuildHTTPError_TruncatesAndMarks pins that an oversized error body is cut at
// the documented cap and SAYS SO. A silent truncation is a bad diagnostic in exactly
// the place an operator is trying to diagnose something: the message looked like the
// peer's complete error.
func TestBuildHTTPError_TruncatesAndMarks(t *testing.T) {
	srv, _ := errorBodyServer(t, http.StatusBadGateway, 10*1024)

	_, err := DoHTTPRequest(context.Background(), srv.URL)
	require.Error(t, err)

	msg := err.Error()
	require.Contains(t, msg, "(truncated)", "a cut-off peer error must be marked as such")
	require.Less(t, len(msg), 10*1024, "the error must not retain the whole body")
}

// TestBuildHTTPError_ExactSizeBodyNotMarkedTruncated pins the boundary on both sides.
//
// io.LimitReader(body, max) returns exactly max bytes both for a body of exactly max
// and for a longer one, so a length-equality test would report a COMPLETE peer error
// as cut short. Detection therefore reads one byte past the cap and marks truncation
// only when that byte actually arrives.
func TestBuildHTTPError_ExactSizeBodyNotMarkedTruncated(t *testing.T) {
	cases := []struct {
		name      string
		size      int
		truncated bool
	}{
		{name: "one below the cap", size: maxHTTPErrorBodyBytes - 1, truncated: false},
		{name: "exactly the cap", size: maxHTTPErrorBodyBytes, truncated: false},
		{name: "one above the cap", size: maxHTTPErrorBodyBytes + 1, truncated: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := errorBodyServer(t, http.StatusBadGateway, tc.size)

			_, err := DoHTTPRequest(context.Background(), srv.URL)
			require.Error(t, err)

			if tc.truncated {
				require.Contains(t, err.Error(), "(truncated)")
				return
			}

			require.NotContains(t, err.Error(), "(truncated)",
				"a body that fits within the cap must not be reported as cut short")
			require.Contains(t, err.Error(), strings.Repeat("x", tc.size),
				"a body that fits must appear in full")
		})
	}
}

// TestBuildHTTPError_DrainsBodyForConnectionReuse pins the regression the drain fixes.
//
// http.Transport only returns a connection to the idle pool once its body has been
// read to EOF. Capping the read at maxHTTPErrorBodyBytes and closing therefore turned
// every error body larger than the cap into a fresh TCP (and, over TLS, a fresh
// handshake) per request — on the high-rate failure path, which is precisely when
// handshakes hurt most.
//
// The body here is a few KiB: over the snippet cap, well under the drain budget, so
// the remainder is drained and the connection reused. N sequential requests must
// therefore establish exactly one connection.
func TestBuildHTTPError_DrainsBodyForConnectionReuse(t *testing.T) {
	const requests = 5

	srv, conns := errorBodyServer(t, http.StatusBadGateway, 4*1024)

	for i := 0; i < requests; i++ {
		_, err := DoHTTPRequest(context.Background(), srv.URL)
		require.Error(t, err, "every request fails with the error status")
	}

	require.Equal(t, int64(1), conns.Load(),
		"the error body must be drained so the connection is reused; a value of %d means one connection per request", requests)
}

// TestBuildHTTPError_AbandonsUnboundedBody pins that the drain is itself BOUNDED.
//
// An unbounded io.Copy(io.Discard, body) would restore connection reuse while
// reopening the hole the snippet cap exists to close: a hostile peer answering with
// an error status and then streaming forever. The handler here streams far past the
// drain budget, so buildHTTPError must return promptly and abandon the connection
// rather than read the peer's stream to its (non-existent) end.
func TestBuildHTTPError_AbandonsUnboundedBody(t *testing.T) {
	streaming := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)

		chunk := []byte(strings.Repeat("x", 32*1024))

		for {
			if _, err := w.Write(chunk); err != nil {
				return
			}

			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}

			select {
			case <-r.Context().Done():
				return
			case <-streaming:
				return
			default:
			}
		}
	}))

	t.Cleanup(func() {
		close(streaming)
		srv.Close()
	})

	done := make(chan error, 1)

	go func() {
		_, err := DoHTTPRequest(context.Background(), srv.URL)
		done <- err
	}()

	select {
	case err := <-done:
		require.Error(t, err, "the error status still surfaces")
	case <-time.After(20 * time.Second):
		t.Fatal("buildHTTPError did not return: the drain is not bounded and followed the peer's stream")
	}
}

// TestBuildHTTPError_StatusClassMapping is the regression guard for the branching
// callers rely on: the status-to-error-type mapping must be untouched by the drain
// and truncation-marker changes.
func TestBuildHTTPError_StatusClassMapping(t *testing.T) {
	cases := []struct {
		name   string
		status int
		target error
	}{
		{name: "404 maps to not-found", status: http.StatusNotFound, target: errors.ErrNotFound},
		{name: "503 maps to service-unavailable", status: http.StatusServiceUnavailable, target: errors.ErrServiceUnavailable},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A body over the snippet cap, so the mapping is asserted on the same path
			// that now truncates and drains.
			srv, _ := errorBodyServer(t, tc.status, maxHTTPErrorBodyBytes+512)

			_, err := DoHTTPRequest(context.Background(), srv.URL)
			require.Error(t, err)
			require.ErrorIs(t, err, tc.target, "the status class mapping must survive the drain change")
		})
	}
}
