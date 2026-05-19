package util

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/ordishs/gocore"
)

var (
	// httpRequestTimeout defines the default HTTP request timeout in milliseconds
	// when no deadline is set on the context.
	httpRequestTimeout, _ = gocore.Config().GetInt("http_timeout", 60000)

	// httpStreamingTimeout defines the default HTTP streaming timeout in milliseconds
	// for operations that stream large responses. This is longer than httpRequestTimeout
	// to accommodate large block/subtree downloads during catchup.
	httpStreamingTimeout, _ = gocore.Config().GetInt("http_streaming_timeout", 300000) // 5 minutes default

	// httpClient is configured with connection pooling optimized for high-concurrency
	// operations like P2P catchup. Default MaxIdleConnsPerHost=2 is far too low for catchup
	// operations that can have 128+ concurrent requests per peer (16 workers * 8 subtree fetchers).
	httpClient = &http.Client{
		Transport: func() *http.Transport {
			t := http.DefaultTransport.(*http.Transport).Clone()
			t.MaxIdleConns = 1000       // Total idle connections across all hosts (default: 100)
			t.MaxIdleConnsPerHost = 100 // Per-host idle connections (default: 2)
			t.MaxConnsPerHost = 200     // Per-host total connections (default: 0/unlimited)
			return t
		}(),
	}
)

// HTTPClient returns the shared HTTP client for use with httpmock.ActivateNonDefault() in tests.
func HTTPClient() *http.Client {
	return httpClient
}

// DoHTTPRequest performs an HTTP GET or POST request and returns the response body as bytes.
// Uses GET by default, switches to POST if requestBody is provided.
// Automatically handles timeouts and validates response status codes.
func DoHTTPRequest(ctx context.Context, url string, requestBody ...[]byte) ([]byte, error) {
	bodyReaderCloser, cancelFn, err := doHTTPRequest(ctx, url, requestBody...)
	defer cancelFn()

	if err != nil {
		return nil, err
	}

	defer func() {
		if closeErr := bodyReaderCloser.Close(); closeErr != nil {
			// Log the error but don't override the main return value
		}
	}()

	// Read body with context deadline support
	// Create a channel to handle the read operation
	done := make(chan struct{})
	var blockBytes []byte
	var readErr error

	go func() {
		blockBytes, readErr = io.ReadAll(bodyReaderCloser)
		close(done)
	}()

	// Wait for either read completion or context timeout
	select {
	case <-ctx.Done():
		return nil, errors.NewNetworkTimeoutError("http request [%s] timed out while reading body", url)
	case <-done:
		if readErr != nil {
			return nil, errors.NewServiceError("http request [%s] failed to read body", url, readErr)
		}
		return blockBytes, nil
	}
}

// DoHTTPRequestBounded behaves like DoHTTPRequest but caps the response body at maxBytes.
//
// Why a separate function: DoHTTPRequest uses io.ReadAll on a peer-supplied response, so a
// hostile peer can stream arbitrary bytes within the request timeout and force the node to
// allocate gigabytes. Callers that fetch peer-controlled data (subtree fetches, etc.) must
// bound the allocation. We read up to maxBytes+1 bytes via io.LimitReader; if the result is
// longer than maxBytes the body was over the cap and we return ErrExternal without retaining
// the bytes for the caller.
func DoHTTPRequestBounded(ctx context.Context, url string, maxBytes int64, requestBody ...[]byte) ([]byte, error) {
	bodyReaderCloser, cancelFn, err := doHTTPRequest(ctx, url, requestBody...)
	defer cancelFn()

	if err != nil {
		return nil, err
	}

	defer func() {
		if closeErr := bodyReaderCloser.Close(); closeErr != nil {
			// Log the error but don't override the main return value
		}
	}()

	bounded := io.LimitReader(bodyReaderCloser, maxBytes+1)

	done := make(chan struct{})
	var blockBytes []byte
	var readErr error

	go func() {
		blockBytes, readErr = io.ReadAll(bounded)
		close(done)
	}()

	select {
	case <-ctx.Done():
		return nil, errors.NewNetworkTimeoutError("http request [%s] timed out while reading body", url)
	case <-done:
		if readErr != nil {
			return nil, errors.NewServiceError("http request [%s] failed to read body", url, readErr)
		}

		if int64(len(blockBytes)) > maxBytes {
			return nil, errors.NewExternalError("http request [%s] response body exceeds %d bytes", url, maxBytes)
		}

		return blockBytes, nil
	}
}

// readCloserWithCancel wraps an io.ReadCloser and calls a cancel function when closed.
type readCloserWithCancel struct {
	io.ReadCloser
	cancelFn context.CancelFunc
}

func (r *readCloserWithCancel) Close() error {
	defer r.cancelFn()
	return r.ReadCloser.Close()
}

// DoHTTPRequestBodyReader performs an HTTP request and returns the response body as a ReadCloser.
// This is more memory-efficient for large responses as it streams the data.
// Caller is responsible for closing the returned ReadCloser.
// Applies a default timeout of 5 minutes (configurable via http_streaming_timeout) when no
// deadline is set on the context. This timeout is longer than the standard HTTP timeout
// to accommodate large file downloads during operations like P2P catchup.
func DoHTTPRequestBodyReader(ctx context.Context, url string, requestBody ...[]byte) (io.ReadCloser, error) {
	bodyReaderCloser, cancelFn, err := doHTTPRequestForStreaming(ctx, url, requestBody...)
	if err != nil {
		cancelFn()
		return nil, err
	}

	return &readCloserWithCancel{
		ReadCloser: bodyReaderCloser,
		cancelFn:   cancelFn,
	}, nil
}

func doHTTPRequest(ctx context.Context, url string, requestBody ...[]byte) (io.ReadCloser, context.CancelFunc, error) {
	cancelFn := func() {
		// noop
	}

	if _, ok := ctx.Deadline(); !ok {
		ctx, cancelFn = context.WithTimeout(ctx, time.Duration(httpRequestTimeout)*time.Millisecond)
	}

	return executeHTTPRequest(ctx, cancelFn, url, requestBody...)
}

// doHTTPRequestForStreaming performs an HTTP request with a longer timeout suitable for streaming.
// Applies httpStreamingTimeout (default 5 minutes) when no deadline exists on the context.
func doHTTPRequestForStreaming(ctx context.Context, url string, requestBody ...[]byte) (io.ReadCloser, context.CancelFunc, error) {
	cancelFn := func() {
		// noop
	}

	if _, ok := ctx.Deadline(); !ok {
		ctx, cancelFn = context.WithTimeout(ctx, time.Duration(httpStreamingTimeout)*time.Millisecond)
	}

	return executeHTTPRequest(ctx, cancelFn, url, requestBody...)
}

// ssrfProtectionEnabled controls whether SSRF validation is active.
// Tests may call SetSSRFProtection(false) to allow requests to localhost test servers.
var ssrfProtectionEnabled = true

// SetSSRFProtection enables or disables SSRF URL validation.
// This is intended for use in tests that make HTTP requests to localhost test servers.
func SetSSRFProtection(enabled bool) {
	ssrfProtectionEnabled = enabled
}

// ValidateURL checks that the given URL is safe to request, rejecting non-HTTP schemes
// and URLs containing link-local IP addresses to prevent SSRF attacks against cloud
// metadata endpoints (e.g. AWS 169.254.169.254).
// Private RFC1918 ranges (10.x, 172.16-31.x, 192.168.x) and loopback are intentionally
// allowed because teranode peers legitimately communicate over private networks.
// DNS resolution is not performed - only IP literals in the hostname are checked.
func ValidateURL(rawURL string) error {
	if !ssrfProtectionEnabled {
		return nil
	}

	parsed, err := url.Parse(rawURL)
	if err != nil {
		return errors.NewInvalidArgumentError("invalid URL: %s", err)
	}

	scheme := strings.ToLower(parsed.Scheme)

	// Only validate http/https URLs. Non-HTTP strings (e.g. "legacy" sentinel
	// values used internally as baseURL placeholders) are allowed through since
	// they will fail naturally at the HTTP client level if actually requested.
	if scheme != "http" && scheme != "https" {
		return nil
	}

	hostname := parsed.Hostname()
	if hostname == "" {
		return errors.NewInvalidArgumentError("URL has no hostname")
	}

	// Check IP literals directly (no DNS resolution to avoid test/latency issues).
	// Hostnames that resolve to link-local at runtime will be caught by the OS/network layer.
	if ip := net.ParseIP(hostname); ip != nil {
		if isBlockedIP(ip) {
			return errors.NewInvalidArgumentError("URL contains blocked IP address %s", ip.String())
		}
	}

	return nil
}

// isBlockedIP returns true if the IP is in a link-local or unspecified range.
// These are blocked because link-local addresses (169.254.x.x) include cloud
// metadata endpoints (e.g. AWS 169.254.169.254) which are the primary SSRF target.
// Loopback and private RFC1918 ranges are allowed since peers communicate over
// private networks in real deployments.
func isBlockedIP(ip net.IP) bool {
	if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
		return true
	}

	// Block IPv6 link-local equivalent
	linkLocal6 := []string{"fe80::/10"}
	for _, r := range linkLocal6 {
		_, cidr, err := net.ParseCIDR(r)
		if err != nil {
			continue
		}
		if cidr.Contains(ip) {
			return true
		}
	}

	return false
}

// executeHTTPRequest performs the actual HTTP request with the given context.
func executeHTTPRequest(ctx context.Context, cancelFn context.CancelFunc, rawURL string, requestBody ...[]byte) (io.ReadCloser, context.CancelFunc, error) {
	if err := ValidateURL(rawURL); err != nil {
		return nil, cancelFn, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, cancelFn, errors.NewServiceError("failed to create http request", err)
	}

	// If there is a request body assume we want a POST and write request body.
	// Content-Type is application/octet-stream because every internal POST that
	// goes through this helper sends raw bytes (e.g. /api/v1/subtree/{hash}/txs
	// streams packed 32-byte tx hashes). Tagging it as application/json caused a
	// WAF in front of asset (ModSecurity) to run the JSON body parser, fail on
	// the binary payload, and reject the request with HTTP 400 — degrading peer
	// catchup reputation across the network.
	if len(requestBody) > 0 && requestBody[0] != nil {
		req.Body = io.NopCloser(bytes.NewReader(requestBody[0]))
		req.Method = http.MethodPost
		req.Header.Set("Content-Type", "application/octet-stream")
	}

	var resp *http.Response
	resp, err = httpClient.Do(req)
	if err != nil {
		return nil, cancelFn, errors.NewServiceError("failed to do http request", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, cancelFn, buildHTTPError(resp, rawURL)
	}

	ct := strings.ToLower(resp.Header.Get("content-type"))
	isHTML := strings.HasPrefix(ct, "text/html")
	if isHTML {
		return nil, cancelFn, errors.NewServiceError("http request [%s] returned HTML - assume bad URL", rawURL)
	}

	return resp.Body, cancelFn, nil
}

// buildHTTPError constructs an appropriate error from a non-OK HTTP response.
func buildHTTPError(resp *http.Response, rawURL string) error {
	errFn := errors.NewServiceError
	if resp.StatusCode == http.StatusNotFound {
		errFn = errors.NewNotFoundError
	}

	if resp.Body != nil {
		defer func() {
			_ = resp.Body.Close()
		}()

		b, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return errFn("http request [%s] returned status code [%d]", rawURL, resp.StatusCode, readErr)
		}

		if b != nil {
			return errFn("http request [%s] returned status code [%d] with body [%s]", rawURL, resp.StatusCode, string(b))
		}
	}

	return errFn("http request [%s] returned status code [%d]", rawURL, resp.StatusCode)
}
