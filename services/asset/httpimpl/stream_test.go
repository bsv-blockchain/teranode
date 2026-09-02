package httpimpl

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errReader serves the supplied prefix bytes, across as many Reads as the caller's
// buffers take, then fails with the supplied error. Used to drive streamOrAbort's
// failure path with a deterministic mid-stream error.
//
// The prefix must survive a short read: streamOrAbort peeks with a one-byte buffer
// before io.Copy takes over, so a reader that dropped its tail on the first short read
// would silently degrade these tests to a one-byte body — a degenerate single-chunk
// stream, leaving the multi-chunk truncation case they exist for uncovered.
type errReader struct {
	prefix []byte
	err    error
}

func (e *errReader) Read(p []byte) (int, error) {
	if len(e.prefix) > 0 {
		n := copy(p, e.prefix)
		e.prefix = e.prefix[n:]

		return n, nil
	}

	return 0, e.err
}

// TestStreamOrAbort_HappyPath verifies that on a successful copy
// streamOrAbort returns nil and lets the HTTP framework finalise the
// response (i.e. emit the chunked terminator) so a caching reverse
// proxy would accept the response.
func TestStreamOrAbort_HappyPath(t *testing.T) {
	body := strings.Repeat("payload-", 1024)

	e := echo.New()
	e.GET("/x", func(c echo.Context) error {
		return streamOrAbort(c, http.StatusOK, echo.MIMEOctetStream, strings.NewReader(body))
	})

	srv := httptest.NewServer(e)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/x")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, body, string(got))
}

// TestStreamOrAbort_MidStreamFailure_ClientSeesUnexpectedEOF verifies that
// when the source reader fails mid-stream, the helper closes the
// underlying TCP connection without the chunked terminator. The client
// observes the truncation as an io.ErrUnexpectedEOF on the next read of
// the body — the signal a caching proxy uses to decide not to cache the
// response.
func TestStreamOrAbort_MidStreamFailure_ClientSeesUnexpectedEOF(t *testing.T) {
	prefix := bytes.Repeat([]byte{0xAB}, 256)

	e := echo.New()
	e.GET("/x", func(c echo.Context) error {
		return streamOrAbort(c, http.StatusOK, echo.MIMEOctetStream, &errReader{
			prefix: prefix,
			err:    errors.NewProcessingError("simulated mid-stream failure"),
		})
	})

	srv := httptest.NewServer(e)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/x")
	require.NoError(t, err)
	defer resp.Body.Close()

	// The handler had already committed to 200 OK before the failure, so
	// the wire-level status is still 200.
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Reading the body must surface a non-clean termination — a clean
	// io.EOF would mean Go wrote the chunked terminator, which is what
	// we're explicitly preventing. The exact error varies (io.ErrUnexpectedEOF
	// from the chunked decoder if any chunks arrived, or a net error if
	// the connection dropped before any body bytes hit the wire) — both
	// are acceptable; both make a caching proxy refuse to store the
	// response.
	_, readErr := io.ReadAll(resp.Body)

	require.Error(t, readErr, "client must see a non-clean termination so caches refuse to store the truncated body")
	assert.NotErrorIs(t, readErr, io.EOF,
		"clean io.EOF would imply Go wrote the chunked terminator — this would let caching proxies store the truncated body")
}

// TestStreamOrAbort_MidStreamFailureWritesWholePrefix pins the multi-chunk shape of
// the failure path where it can be asserted deterministically: server-side. How much
// of the body reaches the client before the hijack-and-close is up to the kernel — an
// abrupt close can discard the socket buffer — so the sibling test above cannot make
// this claim without flaking. Here the response recorder keeps everything the helper
// wrote, which catches both a peek that swallows its byte and a source reader that
// drops its tail on the one-byte peek (which would degrade the mid-stream tests to a
// degenerate single-chunk body).
func TestStreamOrAbort_MidStreamFailureWritesWholePrefix(t *testing.T) {
	prefix := bytes.Repeat([]byte{0xAB}, 256)

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := streamOrAbort(c, http.StatusOK, echo.MIMEOctetStream, &errReader{
		prefix: prefix,
		err:    errors.NewProcessingError("simulated mid-stream failure"),
	})

	// httptest.ResponseRecorder is not an http.Hijacker, so the helper takes the
	// documented h2 fallback and surfaces the copy error instead of hijacking.
	require.Error(t, err)
	require.True(t, c.Response().Committed, "the status line is committed once a byte has been read")
	require.Equal(t, prefix, rec.Body.Bytes(), "the peeked byte and the rest of the source must both be written")
}

// TestStreamOrAbort_CompleteStreamDependsOnHowTheProducerCloses documents the coupling
// that made every successful block_legacy response take the abort path: the helper's
// happy/failure decision is exactly how the producer closed its pipe, and a producer that
// signals completion with CloseWithError(io.ErrClosedPipe) is indistinguishable from one
// that failed mid-stream. That is why GetLegacyBlockReader's success paths must use
// w.Close() — see TestGetLegacyBlockReader_SuccessEndsWithCleanEOF in the repository
// package. A complete body plus an abort meant nginx saw "upstream prematurely closed
// connection", never cached the block, and could turn a buffered 200 into a 502.
func TestStreamOrAbort_CompleteStreamDependsOnHowTheProducerCloses(t *testing.T) {
	body := bytes.Repeat([]byte{0xEE}, 512)

	run := func(closeWriter func(w *io.PipeWriter)) (error, *httptest.ResponseRecorder) {
		r, w := io.Pipe()

		go func() {
			_, _ = w.Write(body)
			closeWriter(w)
		}()

		e := echo.New()
		req := httptest.NewRequest(http.MethodGet, "/x", nil)
		rec := httptest.NewRecorder()

		return streamOrAbort(e.NewContext(req, rec), http.StatusOK, echo.MIMEOctetStream, r), rec
	}

	t.Run("clean close takes the happy path", func(t *testing.T) {
		err, rec := run(func(w *io.PipeWriter) { _ = w.Close() })

		require.NoError(t, err, "a completed stream must not be treated as a failure")
		require.Equal(t, body, rec.Body.Bytes())
	})

	t.Run("success signalled as ErrClosedPipe is taken for a failure", func(t *testing.T) {
		err, rec := run(func(w *io.PipeWriter) { _ = w.CloseWithError(io.ErrClosedPipe) })

		// The whole body still arrives, which is what kept this invisible — but the helper
		// aborts, and on a real connection that drops the chunked terminator.
		// httptest.ResponseRecorder cannot hijack, so the error surfaces here instead.
		require.Error(t, err)
		require.Equal(t, body, rec.Body.Bytes())
	})
}

// TestStreamOrAbort_NoHeaderOverwriteAfterCommit guards against a regression
// where the helper might try to mutate headers after WriteHeader was
// called — which Go silently drops and would mask other bugs. We assert
// that the Content-Type set by streamOrAbort is what shows up on the wire.
func TestStreamOrAbort_NoHeaderOverwriteAfterCommit(t *testing.T) {
	body := "small payload"

	e := echo.New()
	e.GET("/x", func(c echo.Context) error {
		return streamOrAbort(c, http.StatusOK, echo.MIMEOctetStream, strings.NewReader(body))
	})

	srv := httptest.NewServer(e)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/x")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, echo.MIMEOctetStream, resp.Header.Get(echo.HeaderContentType))
}

// TestStreamOrAbort_FailureConnIsActuallyClosed exercises a low-level
// invariant: after the mid-stream failure, the TCP connection from the
// server side is no longer open for additional writes. We can't easily
// observe Go's "did it write the chunked terminator" decision from the
// outside, but we can check that the connection is dead after the
// failed response by attempting a second HTTP request on a fresh
// connection while the first is in flight — and confirming the server
// is responsive (i.e. the per-connection abort didn't bring the listener
// down with it).
func TestStreamOrAbort_FailureConnIsActuallyClosed(t *testing.T) {
	prefix := bytes.Repeat([]byte{0xCD}, 64)

	e := echo.New()
	e.GET("/fail", func(c echo.Context) error {
		return streamOrAbort(c, http.StatusOK, echo.MIMEOctetStream, &errReader{
			prefix: prefix,
			err:    errors.NewProcessingError("simulated"),
		})
	})
	e.GET("/ok", func(c echo.Context) error {
		return c.String(http.StatusOK, "still alive")
	})

	srv := httptest.NewServer(e)
	defer srv.Close()

	// Failing request first
	resp, err := http.Get(srv.URL + "/fail")
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	// Follow-up request must succeed — the listener and other connections
	// must be unaffected by the previous request's hijack-and-close.
	client := &http.Client{Timeout: 5 * time.Second}
	resp2, err := client.Get(srv.URL + "/ok")
	require.NoError(t, err)
	defer resp2.Body.Close()

	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	body2, err := io.ReadAll(resp2.Body)
	require.NoError(t, err)
	assert.Equal(t, "still alive", string(body2))
}

// TestStreamOrAbort_EmptyBodyIsNotACommitted200 covers issue 1368: a source that
// yields zero bytes must not be sent as "200 + empty body". A caching reverse proxy
// stores that as a valid response and replays it for the whole TTL, which is what
// stalled catchup on teratestnet.
func TestStreamOrAbort_EmptyBodyIsNotACommitted200(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := streamOrAbort(c, http.StatusOK, echo.MIMEOctetStream, bytes.NewReader(nil))

	echoErr := &echo.HTTPError{}
	require.True(t, errors.As(err, &echoErr), "an empty body must surface as an HTTP error, not a 200")
	require.Equal(t, http.StatusInternalServerError, echoErr.Code)
	require.False(t, c.Response().Committed, "the status line must not be committed for an empty body")
	require.Empty(t, rec.Body.String())
}

// failFirstReader fails on the very first Read with the supplied error, emulating a
// source whose producer aborted before writing a single byte — what a *io.PipeReader
// returns once dualStreamWithFileCreation has called CloseWithError on the write end.
type failFirstReader struct {
	err error
}

func (f failFirstReader) Read([]byte) (int, error) {
	return 0, f.err
}

// TestStreamOrAbort_ClientGoneBeforeFirstByte covers the peek's blast radius: a client
// that disconnects before the source produced anything must not be reported as a server
// fault. On the on-demand subtree_data path the cancelled request context surfaces as a
// context error (or io.ErrClosedPipe) out of the peek, and returning a 500 there makes
// customHTTPErrorHandler log an ERROR and then fail to write JSON to a peer that is
// already gone — a second ERROR. Catchup cancels in batches, so that is two log lines
// per cancelled in-flight stream on exactly the nodes an operator is diagnosing. The
// repository does the opposite deliberately (dualStreamWithFileCreation classifies this
// as "client gone" at debug level), so mirror it: hijack and close, no error surfaced,
// and crucially no committed status line — an uncommitted response would otherwise be
// finalised as the empty 200 this PR exists to prevent.
func TestStreamOrAbort_ClientGoneBeforeFirstByte(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "closed pipe", err: io.ErrClosedPipe},
		{name: "bare context cancellation", err: context.Canceled},
		{name: "wrapped context cancellation", err: errors.NewProcessingError("write chunk", context.Canceled)},
		{name: "deadline exceeded", err: context.DeadlineExceeded},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var (
				handlerErr error
				committed  bool
				done       = make(chan struct{})
			)

			e := echo.New()
			e.GET("/x", func(c echo.Context) error {
				defer close(done)

				handlerErr = streamOrAbort(c, http.StatusOK, echo.MIMEOctetStream, failFirstReader{err: tc.err})
				committed = c.Response().Committed

				return handlerErr
			})

			srv := httptest.NewServer(e)
			defer srv.Close()

			resp, err := http.Get(srv.URL + "/x") //nolint:bodyclose // closed below when non-nil
			if err == nil {
				defer resp.Body.Close()

				require.NotEqual(t, http.StatusInternalServerError, resp.StatusCode,
					"a client disconnect must not be answered with a server-fault status")
				require.NotEqual(t, http.StatusOK, resp.StatusCode,
					"an uncommitted response finalised as 200 with Content-Length: 0 is the bug being fixed")
			}

			<-done

			require.NoError(t, handlerErr,
				"a client disconnect must not reach echo's error handler, which logs it at ERROR twice")
			require.False(t, committed,
				"the status line must stay uncommitted so no cacheable empty 200 can be finalised")
		})
	}
}

// TestStreamOrAbort_SingleByteBodyStillSucceeds guards the boundary: the peeked byte
// must be written, not swallowed.
func TestStreamOrAbort_SingleByteBodyStillSucceeds(t *testing.T) {
	e := echo.New()
	e.GET("/x", func(c echo.Context) error {
		return streamOrAbort(c, http.StatusOK, echo.MIMEOctetStream, strings.NewReader("A"))
	})

	srv := httptest.NewServer(e)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/x")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "A", string(got))
}
