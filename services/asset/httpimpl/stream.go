package httpimpl

import (
	"io"

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
// truncation as io.ErrUnexpectedEOF in its body parse).
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
	h := c.Response().Header()
	h.Set(echo.HeaderContentType, contentType)
	c.Response().WriteHeader(code)

	_, copyErr := io.Copy(c.Response(), r)
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
	// surfacing the io.Copy error so echo logs it. The response is already
	// committed, so echo's error handler won't write to it; the cache-bypass
	// guarantee does not hold on this path.
	return copyErr
}
