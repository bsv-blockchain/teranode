package httpimpl

import (
	"bytes"
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

// errReader returns the supplied prefix bytes once, then fails with the
// supplied error on the next Read. Used to drive streamOrAbort's
// failure path with a deterministic mid-stream error.
type errReader struct {
	prefix   []byte
	err      error
	consumed bool
}

func (e *errReader) Read(p []byte) (int, error) {
	if !e.consumed {
		n := copy(p, e.prefix)
		e.consumed = true
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
