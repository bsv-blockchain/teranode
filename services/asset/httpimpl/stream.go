package httpimpl

import (
	"io"
	"net/http"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/labstack/echo/v4"
)

// streamOrAbort is a drop-in replacement for echo.Context.Stream that prevents
// truncated bodies from being cached by a reverse proxy (nginx in particular)
// when the source reader fails mid-stream.
//
// Background — the bug we're closing
//
// echo.Context.Stream commits to a 200 OK status line the moment the first
// byte is read from the source reader, then io.Copy's the rest of the body
// to the response. If the source reader fails partway through (e.g. the
// on-demand subtreeData generation in services/asset/repository can't find
// some tx in the local store), io.Copy returns the error and the handler
// returns. So far so good — except Go's net/http server, in finishRequest(),
// then unconditionally writes the chunked-transfer terminator "0\r\n\r\n"
// to close the response cleanly. Wire-syntactically the chunked stream
// looks complete. A caching reverse proxy in front of the asset service
// (nginx's proxy_cache) takes that at face value and persists the truncated
// body to the cache. Every subsequent client request hits the bad cache
// entry, and the catchup loop loses the race against the corruption.
//
// # Fix
//
// On the happy path, behave exactly like c.Stream — the chunked terminator
// is written normally and nginx caches the response as today. On the
// failure path, hijack the underlying TCP connection and close it
// **without** writing the terminator. nginx detects "upstream prematurely
// closed connection while reading upstream", refuses to commit the partial
// response to cache (this is the same path nginx already uses for any
// truncated chunked response from upstream, documented behaviour), and the
// next client request goes back to the asset service for a fresh attempt.
//
// # Caller contract
//
// On the happy path, and on a streaming failure where the hijack succeeds,
// streamOrAbort returns nil to its echo caller — once we hijack the
// connection we own it, and any further writes from echo's after-handler
// middleware would either fail or escape our control. Returning nil to
// echo signals "response already finalised, don't touch it." The error
// from io.Copy is consumed inside this helper (an abrupt hijack + close
// is sufficient signalling to the client, which will surface the
// truncation as io.ErrUnexpectedEOF in its body parse). A source that
// yields zero bytes now returns an *echo.HTTPError with status 500 and
// never commits a status line, so callers must not have written headers
// before calling. A client that disconnects before the first byte returns
// nil through the same hijack-and-close, again without committing a status
// line — that is a client fault, not a server one, and surfacing it as a
// 500 would only log an error twice for a peer that is already gone.
//
// # HTTP/2 limitation
//
// Hijacking is only supported on HTTP/1.x. On HTTP/2, echo's
// Response.Hijack (which delegates to http.ResponseController.Hijack)
// returns http.ErrNotSupported — it does not panic — and we fall through
// to returning the io.Copy error. The response is already committed at
// that point, so echo's error handler only logs it; but the connection is
// closed cleanly and the cache-bypass guarantee does NOT hold on h2. In
// production the asset service sits behind an HTTP/1.1 nginx, so the
// hijack path is the one that runs; only a client (or caching proxy)
// speaking h2 directly to the asset service hits the fallback.
func streamOrAbort(c echo.Context, code int, contentType string, r io.Reader) error {
	// Peek one byte BEFORE committing the status line. A source that yields zero bytes
	// — an empty subtreeData file, or an on-demand generation that failed before its
	// first write — would otherwise be sent as "200 OK + empty body", which a caching
	// reverse proxy stores as a valid response and replays for the whole TTL. That is
	// the poisoning in issue 1368: a 32768-byte subtree (1024 tx hashes) whose
	// subtree_data came back 200 with content-length 0, cached, stalling every
	// requester's catchup. None of the endpoints using this helper can legitimately
	// return an empty body, so an empty source is a server-side fault: report it as
	// 500, which the cache configuration (proxy_cache_valid 200 5m) never stores.
	//
	// Scope: the routes going through here are subtree_data, subtree in BINARY_STREAM
	// mode, block_legacy (both URL shapes) and the mining-candidate legacy block. The
	// subtree HEX route does NOT — it commits 200 and then copies into a hex encoder, so
	// a zero-byte source there is still a committed empty 200. It is out of the cache's
	// reach: asset-cache-nginx.conf's cached location regex is $-anchored on the hash, so
	// /api/v1/subtree/<hash>/hex never matches it and is never stored. The endpoint
	// families that regex does match besides these (header, headers, block, blocks,
	// headers_{to,from}_common_ancestor) answer with c.Blob on a fully materialised
	// slice, so they always carry a Content-Length and none of this applies to them.
	var first [1]byte

	// With a one-byte buffer io.ReadFull returns a nil error only when n == 1, and
	// io.ErrUnexpectedEOF only for 0 < n < 1 — impossible. So inside n == 0, readErr is
	// always non-nil and the condition is simply "which failure": a client that went
	// away, or an exhausted source. Grow the buffer and both assumptions break.
	n, readErr := io.ReadFull(r, first[:])
	if n == 0 {
		// Client (or proxy) went away before the source produced anything. Not a server
		// fault: the request context is cancelled, the producer returns ctx.Err() or the
		// pipe is closed, and the blocked read above surfaces that. Answering 500 would
		// make customHTTPErrorHandler log an ERROR and then fail to write JSON to a peer
		// that is already gone — a second ERROR — for every cancelled in-flight stream,
		// and catchup cancels in batches. Mirror the debug-level "client gone"
		// segregation in dualStreamWithFileCreation instead: hijack and close, which
		// leaves the response uncommitted so nothing cacheable is finalised.
		if errors.IsContextError(readErr) || errors.Is(readErr, io.ErrClosedPipe) {
			if conn, _, hjErr := c.Response().Hijack(); hjErr == nil {
				_ = conn.Close()
				return nil
			}
			// h2: no hijack available, fall through to the 500.
		}

		if errors.Is(readErr, io.EOF) {
			return echo.NewHTTPError(http.StatusInternalServerError, "empty response body")
		}

		return echo.NewHTTPError(http.StatusInternalServerError, readErr.Error())
	}

	h := c.Response().Header()
	h.Set(echo.HeaderContentType, contentType)
	c.Response().WriteHeader(code)

	copyErr := writeStreamRemainder(c, first[:n], r)
	if copyErr == nil {
		// Happy path — let echo / Go finalise the chunked stream normally.
		return nil
	}

	// Mid-stream failure. Hijack the connection and close it before Go's
	// HTTP server has a chance to write the chunked terminator. nginx then
	// sees an incomplete chunked response and discards its cache file.
	if conn, _, hjErr := c.Response().Hijack(); hjErr == nil {
		_ = conn.Close()
		return nil
	}

	// Hijack unsupported (HTTP/2 — see the doc comment) — fall back to
	// surfacing the copy error so echo logs it. The response is already
	// committed, so echo's error handler won't write to it; the cache-bypass
	// guarantee does not hold on this path.
	return copyErr
}

// writeStreamRemainder writes the peeked prefix and then copies the rest of the
// source to the response.
func writeStreamRemainder(c echo.Context, prefix []byte, r io.Reader) error {
	if _, err := c.Response().Write(prefix); err != nil {
		return err
	}

	_, err := io.Copy(c.Response(), r)

	return err
}
