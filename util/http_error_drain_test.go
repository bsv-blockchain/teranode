package util

import (
	"context"
	"io"
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
// the remainder is drained and the connection reused.
//
// Reuse only happens because the drain is CALLER-SYNCHRONOUS up to
// maxHTTPErrorBodyDrainWait. DoHTTPRequest defers cancelFn(), which cancels the request
// context the moment it returns, and body reads honour that context — so a drain still
// in flight at that point is killed and the connection discarded. An earlier revision
// ran the drain purely on a goroutine and reuse dropped to zero: 5 requests, 5
// connections, every time.
//
// The assertion allows 2 rather than requiring exactly 1 because a runner stalled past
// the 250ms drain budget would abandon a drain and lose that connection. The regression
// this guards costs one connection per request, so a bound well below N discriminates
// cleanly without depending on scheduling. The byte bound is asserted separately and
// deterministically by TestDrainErrorBody_BoundedAndCloses.
func TestBuildHTTPError_DrainsBodyForConnectionReuse(t *testing.T) {
	const requests = 5

	srv, conns := errorBodyServer(t, http.StatusBadGateway, 4*1024)

	for i := 0; i < requests; i++ {
		_, err := DoHTTPRequest(context.Background(), srv.URL)
		require.Error(t, err, "every request fails with the error status")
	}

	require.LessOrEqual(t, conns.Load(), int64(2),
		"the error body must be drained so connections are reused; %d requests opened %d connections", requests, conns.Load())
}

// TestDrainErrorBody_BoundedAndCloses asserts the two properties of the drain directly
// and deterministically — no server, no goroutine, no timing.
//
// An unbounded io.Copy(io.Discard, body) would restore connection reuse while reopening
// the hole the snippet cap exists to close: a hostile peer answering with an error
// status and then streaming forever.
func TestDrainErrorBody_BoundedAndCloses(t *testing.T) {
	t.Run("a body larger than the budget is abandoned at the cap", func(t *testing.T) {
		body := &countingBody{remaining: maxHTTPErrorBodyDrainBytes * 4}

		drainErrorBody(body)

		require.Equal(t, maxHTTPErrorBodyDrainBytes, body.read, "the drain must stop at its byte budget")
		require.True(t, body.closed, "the body is closed even when abandoned, or the connection leaks")
	})

	t.Run("a body inside the budget is fully drained", func(t *testing.T) {
		body := &countingBody{remaining: 4 * 1024}

		drainErrorBody(body)

		require.Equal(t, 4*1024, body.read)
		require.True(t, body.closed)
	})

	t.Run("a read error still closes the body", func(t *testing.T) {
		body := &countingBody{remaining: 1024, errAfter: 512}

		drainErrorBody(body)

		require.True(t, body.closed, "a failed drain must not leak the connection")
	})
}

// countingBody is an io.ReadCloser that yields remaining bytes (optionally erroring
// part-way) and records how much was read and whether it was closed.
type countingBody struct {
	remaining int
	errAfter  int
	read      int
	closed    bool
}

func (b *countingBody) Read(p []byte) (int, error) {
	if b.errAfter > 0 && b.read >= b.errAfter {
		return 0, errors.NewServiceError("peer read failure")
	}

	if b.remaining == 0 {
		return 0, io.EOF
	}

	n := len(p)
	if n > b.remaining {
		n = b.remaining
	}

	b.remaining -= n
	b.read += n

	return n, nil
}

func (b *countingBody) Close() error {
	b.closed = true

	return nil
}

// TestBuildHTTPError_AbandonsUnboundedBody pins that a peer streaming forever on an
// error status cannot hold the caller. The handler streams far past the drain budget, so
// buildHTTPError must still return promptly.
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

// TestBuildHTTPError_RemainderDrainDoesNotBlockCaller pins that the REMAINDER drain is
// bounded in the caller's TIME, not just in bytes.
//
// Response-body reads honour the request context, so an unbounded-in-time remainder
// drain let a peer dribbling up to the 64 KiB budget hold the caller for the whole
// http_streaming_timeout (5 minutes by default), compounded by up to six 503 retries.
// The drain now waits at most maxHTTPErrorBodyDrainWait and then abandons the body to a
// background goroutine.
//
// The handler delivers the snippet bytes IMMEDIATELY — so the synchronous snippet read
// completes — and then stalls. Anything holding the caller past that point is the
// remainder drain. Note what this does NOT claim: the snippet read itself is bounded in
// bytes but not in time, and that exposure is pre-existing (it replaced an unbounded
// io.ReadAll).
//
// The wall-clock assertion is a hang detector with a very large margin — expected
// elapsed is the 250ms drain budget against a 10s ceiling — so a loaded runner cannot
// trip it. It fails hard on a drain with no time bound, which would sit here until the
// request context expired.
func TestBuildHTTPError_RemainderDrainDoesNotBlockCaller(t *testing.T) {
	stalled := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)

		// Enough to satisfy the snippet read (cap + the one detection byte) and to put
		// the response over the truncation threshold.
		_, _ = w.Write([]byte(strings.Repeat("x", maxHTTPErrorBodyBytes+1)))

		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}

		// Now stall with the body still open: only the remainder drain is waiting.
		select {
		case <-r.Context().Done():
		case <-stalled:
		}
	}))

	t.Cleanup(func() {
		close(stalled)
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
		require.Contains(t, err.Error(), "truncated", "the snippet read completed and reported truncation")
	case <-time.After(10 * time.Second):
		t.Fatal("buildHTTPError did not return: the caller is being held by the remainder drain")
	}
}

// TestExecuteHTTPRequest_HTMLResponseClosesBody pins the leak the drain work would
// otherwise have left in place: the HTML-detection branch returned an error WITHOUT
// closing resp.Body, so the body was never read to EOF and the connection could never
// go back into http.Transport's idle pool. That defeats the stated purpose of bounding
// and draining error bodies a few lines away. It is a pre-existing leak on this
// 2xx-with-unexpected-content-type path, not a regression introduced by the drain
// change.
//
// executeHTTPRequest is called DIRECTLY, with a cancel func this test never invokes,
// because that is what makes the property observable. Going through DoHTTPRequest would
// defer cancelFn(), which cancels the request context on return and tears the
// connection down regardless of whether the body was closed — so connection counts
// there cannot tell the fixed code from the unfixed code.
//
// With the body closed (and drained), sequential requests reuse one connection. Without
// it, each request needs a fresh one. The bound is 2 rather than 1 because the drain
// runs on its own goroutine and can in principle lose a scheduling race; the regression
// costs one connection per request, so a bound well below the request count discriminates
// cleanly without depending on scheduling.
func TestExecuteHTTPRequest_HTMLResponseClosesBody(t *testing.T) {
	var conns atomic.Int64

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("<html>not what we asked for</html>"))
	}))

	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			conns.Add(1)
		}
	}

	srv.Start()
	t.Cleanup(srv.Close)

	const requests = 4

	for i := 0; i < requests; i++ {
		body, cancelFn, err := executeHTTPRequest(context.Background(), func() {}, srv.URL)

		require.Error(t, err, "an HTML response is an error")
		require.Contains(t, err.Error(), "returned HTML")
		require.Nil(t, body, "no reader is handed to the caller, so the body must be closed internally")
		require.NotNil(t, cancelFn)
	}

	require.LessOrEqual(t, conns.Load(), int64(2),
		"the HTML branch must close the body so the connection is reusable; %d requests opened %d connections", requests, conns.Load())
}

// TestDoHTTPRequestForStreaming_HTMLResponseClosesBody is the streaming counterpart.
//
// Connection reuse is deliberately NOT asserted here. This path calls cancelFn before
// returning, which cancels the request context and makes the connection unusable
// whatever we do with the body, so the body is closed rather than drained and no
// reuse is possible to observe. What is assertable is the contract: the HTML response
// is an error and no reader is handed back, so nothing is left for a caller to close.
func TestDoHTTPRequestForStreaming_HTMLResponseClosesBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("<html>not what we asked for</html>"))
	}))
	t.Cleanup(srv.Close)

	reader, retryAfter, err := doHTTPRequestForStreamingWithRetryAfter(context.Background(), srv.URL)
	require.Error(t, err, "an HTML response is an error on the streaming path too")
	require.Contains(t, err.Error(), "returned HTML")
	require.Nil(t, reader, "no reader is handed to the caller, so the body must be closed internally")
	require.Zero(t, retryAfter)
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
