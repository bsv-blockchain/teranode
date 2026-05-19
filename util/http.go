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

// ssrfSafeDialer wraps the default dialer and rejects connections to private/loopback IPs
// after DNS resolution. This closes the DNS-rebinding gap that the static IP-literal check
// in ValidateURL cannot cover: a peer could pass http://internal.cluster.local/ whose
// hostname resolves to 10.x or 127.x only at dial time.
//
// Loopback and RFC1918 ranges are intentionally blocked here because the dialer-level check
// is specifically for hostnames that resolve to private IPs (SSRF via DNS). The static
// ValidateURL check still allows explicit private-IP literals for backwards compatibility
// with direct peer-IP URLs; if that policy changes, update isBlockedIP.
var ssrfSafeDialer = &net.Dialer{
	Timeout:   30 * time.Second,
	KeepAlive: 30 * time.Second,
}

// ssrfDialContext wraps ssrfSafeDialer.DialContext and rejects resolved addresses that
// fall into loopback or RFC1918 ranges. It is installed as the Transport.DialContext for
// httpClient so that every outgoing connection is checked, including those that follow
// HTTP redirects.
func ssrfDialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if !ssrfProtectionEnabled {
		return ssrfSafeDialer.DialContext(ctx, network, addr)
	}

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, errors.NewInvalidArgumentError("SSRF dial check: cannot split host/port from %q: %v", addr, err)
	}

	ips, err := net.DefaultResolver.LookupHost(ctx, host)
	if err != nil {
		return nil, errors.NewServiceError("SSRF dial check: failed to resolve %q", host, err)
	}

	for _, ipStr := range ips {
		ip := net.ParseIP(ipStr)
		if ip == nil {
			continue
		}
		if isBlockedDialIP(ip) {
			return nil, errors.NewInvalidArgumentError("SSRF dial check: resolved address %s for host %q is a blocked IP", ipStr, host)
		}
	}

	return ssrfSafeDialer.DialContext(ctx, network, net.JoinHostPort(host, port))
}

// isBlockedDialIP returns true for IPs that are unsafe to connect to when the hostname
// came from a peer-controlled URL. Unlike isBlockedIP (which only guards link-local for
// the static ValidateURL pre-check), this also blocks loopback and RFC1918 ranges because
// a peer should never be able to make us dial cluster-internal services.
func isBlockedDialIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
		return true
	}

	privateRanges := []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"fc00::/7",   // IPv6 unique-local
		"fe80::/10",  // IPv6 link-local
		"::1/128",    // IPv6 loopback
	}
	for _, cidrStr := range privateRanges {
		_, cidr, err := net.ParseCIDR(cidrStr)
		if err != nil {
			continue
		}
		if cidr.Contains(ip) {
			return true
		}
	}

	return false
}

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
	//
	// The transport uses ssrfDialContext so that DNS-resolved private/loopback IPs are
	// rejected at dial time, closing the SSRF-via-hostname gap. CheckRedirect applies the
	// same validation to redirect targets so a peer-controlled server cannot bounce us to
	// an internal address.
	httpClient = &http.Client{
		Transport: func() *http.Transport {
			t := http.DefaultTransport.(*http.Transport).Clone()
			t.MaxIdleConns = 1000       // Total idle connections across all hosts (default: 100)
			t.MaxIdleConnsPerHost = 100 // Per-host idle connections (default: 2)
			t.MaxConnsPerHost = 200     // Per-host total connections (default: 0/unlimited)
			t.DialContext = ssrfDialContext
			return t
		}(),
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return errors.NewInvalidArgumentError("stopped after 10 redirects")
			}
			if err := ValidateURL(req.URL.String()); err != nil {
				return errors.NewInvalidArgumentError("SSRF redirect check: %v", err)
			}
			return nil
		},
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

	// Reject credentials embedded in the URL (e.g. http://user:pass@host/). Userinfo
	// has no legitimate use here and can be used to bypass auth or confuse logging.
	if parsed.User != nil {
		return errors.NewInvalidArgumentError("URL must not contain userinfo (credentials)")
	}

	hostname := parsed.Hostname()
	if hostname == "" {
		return errors.NewInvalidArgumentError("URL has no hostname")
	}

	// Check IP literals directly. DNS-resolved addresses are validated later by
	// ssrfDialContext at connection time.
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
