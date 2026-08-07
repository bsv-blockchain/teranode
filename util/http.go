package util

import (
	"bytes"
	"context"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/ordishs/gocore"
)

// ssrfSafeDialer wraps the default dialer and rejects connections to link-local/loopback
// IPs after DNS resolution. This closes the DNS-rebinding gap that the static IP-literal
// check in ValidateURL cannot cover: a peer could pass http://internal.cluster.local/ whose
// hostname resolves to 169.254.169.254 (the cloud metadata endpoint) only at dial time.
//
// Only link-local and loopback are blocked — see isBlockedDialIP for the rationale. RFC1918
// ranges are deliberately allowed because teranode peers, k8s pods, and private miner
// interconnects all communicate over private networks in real deployments; this matches the
// static ValidateURL/isBlockedIP policy.
var ssrfSafeDialer = &net.Dialer{
	Timeout:   30 * time.Second,
	KeepAlive: 30 * time.Second,
}

// ssrfLookupHost resolves a hostname to its IP addresses. It is a package var so tests can
// substitute a resolver that reproduces DNS-rebinding behaviour (e.g. returning a private
// address for a name that "looks" public).
var ssrfLookupHost = net.DefaultResolver.LookupHost

// ssrfDialContext wraps ssrfSafeDialer.DialContext and rejects resolved addresses that
// fall into link-local or loopback ranges (see isBlockedDialIP). It is installed as the
// Transport.DialContext for httpClient so that every outgoing connection is checked,
// including those that follow HTTP redirects.
//
// Critically, after validating the resolved addresses we dial those exact IPs rather than
// the hostname. Dialing by hostname would let net.Dialer perform a SECOND, independent DNS
// resolution at connect time — the classic DNS-rebinding TOCTOU bypass: a peer-controlled
// authoritative server with TTL=0 can return a public IP for our validation lookup and
// 169.254.169.254 / 127.0.0.1 for the dialer's lookup. Connecting to the already-validated
// IP closes that window. (The Transport still derives the TLS ServerName and Host header
// from the original URL, so dialing by IP does not break virtual hosting or HTTPS.)
func ssrfDialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if !ssrfProtectionEnabled.Load() {
		return ssrfSafeDialer.DialContext(ctx, network, addr)
	}

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, errors.NewInvalidArgumentError("SSRF dial check: cannot split host/port from %q: %v", addr, err)
	}

	ips, err := ssrfLookupHost(ctx, host)
	if err != nil {
		return nil, errors.NewServiceError("SSRF dial check: failed to resolve %q", host, err)
	}

	// Validate every resolved address first; reject outright if any is blocked so a
	// mixed public/private answer cannot smuggle an internal target through failover.
	validated := make([]net.IP, 0, len(ips))
	for _, ipStr := range ips {
		ip := net.ParseIP(ipStr)
		if ip == nil {
			continue
		}
		if isBlockedDialIP(ip) {
			return nil, errors.NewInvalidArgumentError("SSRF dial check: resolved address %s for host %q is a blocked IP", ipStr, host)
		}
		validated = append(validated, ip)
	}

	if len(validated) == 0 {
		return nil, errors.NewServiceError("SSRF dial check: no usable addresses resolved for host %q", host)
	}

	// Dial the validated IPs directly (no re-resolution), trying each to preserve
	// multi-A-record failover.
	var lastErr error
	for _, ip := range validated {
		conn, dialErr := ssrfSafeDialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if dialErr != nil {
			lastErr = dialErr
			continue
		}
		return conn, nil
	}

	return nil, lastErr
}

// isBlockedDialIP returns true for IPs that are unsafe to connect to when the hostname
// came from a peer-controlled URL, evaluated after DNS resolution to close the
// DNS-rebinding gap that the static ValidateURL pre-check cannot cover.
//
// It blocks only:
//   - link-local (169.254.0.0/16, fe80::/10) — the real SSRF target, since the cloud
//     metadata endpoint 169.254.169.254 lives here;
//   - loopback (127.0.0.0/8, ::1) — a peer should never make us dial our own localhost
//     admin/RPC services, and no legitimate peer advertises a loopback fetch source;
//   - unspecified (0.0.0.0, ::).
//
// RFC1918 ranges (10/8, 172.16/12, 192.168/16) and IPv6 ULA (fc00::/7) are intentionally
// NOT blocked: teranode peers, k8s pods, and privately-routed miner interconnects all
// communicate over private networks in real deployments. Blocking them here would reject
// legitimate peer traffic and contradicts isBlockedIP, which allows the same ranges for
// the static ValidateURL check.
func isBlockedDialIP(ip net.IP) bool {
	return ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified()
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

	return readBodyWithCtx(ctx, url, bodyReaderCloser, -1)
}

// readBodyWithCtx reads r fully while honoring ctx during the read (a slow/stalled peer
// can't block past the request deadline), with ONE shared cancel-vs-deadline
// classification so every caller agrees: a context deadline (peer too slow) → a non-local
// network timeout (the peer is at fault); a cancel (e.g. shutdown) → a local context error
// (we are at fault, don't blame the peer). Any other read error → a generic service error.
// maxBytes < 0 means unbounded; otherwise the body is capped and ErrExternal is returned if
// the peer streams more than the cap.
func readBodyWithCtx(ctx context.Context, url string, r io.Reader, maxBytes int64) ([]byte, error) {
	// url is used only for error text here (the request already happened). Redact it to
	// scheme://host so peer-gossiped path/query bytes never reach logs or the substring-matching
	// error classifiers (IsNetworkError, IsMaliciousResponseError). See RedactPeerURL.
	url = RedactPeerURL(url)

	if maxBytes >= 0 {
		r = io.LimitReader(r, maxBytes+1)
	}

	done := make(chan struct{})
	var b []byte
	var readErr error
	go func() {
		b, readErr = io.ReadAll(r)
		close(done)
	}()

	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil, errors.NewContextCanceledError("http request [%s] canceled while reading body", url, context.Canceled)
		}
		return nil, errors.NewNetworkTimeoutError("http request [%s] timed out while reading body", url)
	case <-done:
		if readErr != nil {
			if errors.Is(readErr, context.DeadlineExceeded) {
				return nil, errors.NewNetworkTimeoutError("http request [%s] timed out while reading body", url)
			}
			if errors.Is(readErr, context.Canceled) {
				return nil, errors.NewContextCanceledError("http request [%s] canceled while reading body", url, context.Canceled)
			}
			return nil, errors.NewServiceError("http request [%s] failed to read body", url, readErr)
		}
		if maxBytes >= 0 && int64(len(b)) > maxBytes {
			return nil, errors.NewExternalError("http request [%s] response body exceeds %d bytes", url, maxBytes)
		}
		return b, nil
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

	return readBodyWithCtx(ctx, url, bodyReaderCloser, maxBytes)
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
// It is an atomic.Bool because SetSSRFProtection can be toggled while requests are in
// flight (notably under `go test -race`), and the dial/validate paths read it concurrently.
var ssrfProtectionEnabled = func() *atomic.Bool {
	b := &atomic.Bool{}
	b.Store(true)
	return b
}()

// SetSSRFProtection enables or disables SSRF URL validation.
// This is intended for use in tests that make HTTP requests to localhost test servers.
func SetSSRFProtection(enabled bool) {
	ssrfProtectionEnabled.Store(enabled)
}

// ValidateURL checks that the given URL is safe to request, rejecting non-HTTP schemes
// and URLs containing link-local IP addresses to prevent SSRF attacks against cloud
// metadata endpoints (e.g. AWS 169.254.169.254).
// Private RFC1918 ranges (10.x, 172.16-31.x, 192.168.x) and loopback are intentionally
// allowed because teranode peers legitimately communicate over private networks.
// DNS resolution is not performed - only IP literals in the hostname are checked.
func ValidateURL(rawURL string) error {
	if !ssrfProtectionEnabled.Load() {
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

// newSignedRequest builds a validated, optionally-signed *http.Request for rawURL.
// GET by default; POST with an octet-stream body when requestBody is provided.
//
// This is the single request-builder shared by both the one-shot (executeHTTPRequest)
// and retrying (doRequestReaderWithRetryAfter) paths, so request signing, body
// Content-Type, and URL validation can never diverge between them — a divergence
// previously sent retrying catchup fetches unsigned and lost the peer rate-limit
// exemption.
func newSignedRequest(ctx context.Context, rawURL string, requestBody ...[]byte) (*http.Request, error) {
	if err := ValidateURL(rawURL); err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, errors.NewServiceError("failed to create http request", err)
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

	// Sign the request if a signer is configured (silently skip on error)
	if signer := loadHTTPRequestSigner(); signer != nil {
		_ = signer.SignRequest(req)
	}

	return req, nil
}

// executeHTTPRequest performs the actual HTTP request with the given context.
func executeHTTPRequest(ctx context.Context, cancelFn context.CancelFunc, rawURL string, requestBody ...[]byte) (io.ReadCloser, context.CancelFunc, error) {
	req, err := newSignedRequest(ctx, rawURL, requestBody...)
	if err != nil {
		return nil, cancelFn, err
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

// maxErrorBodyBytes caps how much of a non-OK response body is drained on error paths.
// The body is peer-controlled and is NOT embedded in the error message (see buildHTTPError),
// so this only bounds the drain read used to keep the connection reusable. Sized generously
// (64 KiB): real 429/503 error bodies from the asset server are tiny, and draining them in full
// lets net/http return the connection to the pool instead of forcing a fresh TCP+TLS handshake on
// the next backoff retry. A body larger than this is anomalous/hostile and its connection is
// intentionally left undrained (dropped rather than reused).
//
// NB: this deliberately diverges from upstream's body-in-message form. Embedding the peer body
// (even %q-escaped) feeds it into (*Error).Is's substring classification at the catchup gates, so a
// peer body containing "context canceled" could forge a local classification. Draining without
// embedding closes that channel while still bounding the read against a hostile infinite stream.
const maxErrorBodyBytes = 64 << 10 // 64 KiB

// RedactPeerURL reduces a possibly peer-supplied URL to scheme://host[:port], dropping path/query.
// A peer-gossiped DataHubURL is validated for scheme + SSRF host only, so its path can carry
// arbitrary bytes; interpolating the raw string into an error that feeds (*Error).Is substring
// classification lets a peer forge a "local" verdict (e.g. a path containing "context canceled").
// Returns "[redacted url]" when the input can't be parsed into a host.
func RedactPeerURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "[redacted url]"
	}
	return u.Scheme + "://" + u.Host
}

// buildHTTPError constructs an appropriate error from a non-OK HTTP response.
//
// The error type is chosen to let callers branch with errors.Is:
//   - 404 → ErrNotFound
//   - 503 → ErrServiceUnavailable (typically retryable; see DoHTTPRequestBodyReaderWithRetry)
//   - 429 → ErrServiceUnavailable (rate limited; same retryable class so the retry
//     helpers back off rather than fail the caller — see the *WithRetry helpers)
//   - other → generic ServiceError
//
// The body is drained up to maxErrorBodyBytes (bounding a hostile peer that answers with an error
// status and then streams indefinitely) but is NOT embedded in the message — only the status code,
// the caller-supplied URL and the drained byte count. The body is peer-controlled and this error
// feeds substring-based classification at the catchup gates, so echoing it (even escaped) would let
// a peer forge a "local" classification. Callers on peer-controlled paths should redact the URL too.
func buildHTTPError(resp *http.Response, rawURL string) error {
	// Redact to scheme://host: rawURL may be a peer-gossiped DataHubURL whose crafted path (e.g.
	// one containing "context canceled") would otherwise ride into this message and be substring-
	// matched by the catchup classifiers.
	rawURL = RedactPeerURL(rawURL)
	errFn := errors.NewServiceError
	switch resp.StatusCode {
	case http.StatusNotFound:
		errFn = errors.NewNotFoundError
	case http.StatusServiceUnavailable, http.StatusTooManyRequests:
		errFn = errors.NewServiceUnavailableError
	}

	if resp.Body != nil {
		defer func() {
			_ = resp.Body.Close()
		}()

		// Drain a bounded amount of the body (helps keep-alive connection reuse) but do
		// NOT embed the peer-controlled content in the error message. This error feeds
		// substring-based classification at the catchup reputation gates (IsContextError /
		// releaseCatchupLock's strings.Contains checks). A hostile or unlucky peer whose
		// body contained e.g. "context deadline exceeded" or "block assembly is behind"
		// could otherwise forge a "local" classification — clearing its reputation penalty
		// AND halting failover to honest peers, re-opening the #1174 wedge. Only the
		// status code and body length go into the message (plus rawURL, which callers
		// on peer-controlled paths must redact — see buildHTTPError callers).
		n, _ := io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorBodyBytes))
		if n > 0 {
			return errFn("http request [%s] returned status code [%d] (%d body bytes, omitted)", rawURL, resp.StatusCode, n)
		}
	}

	return errFn("http request [%s] returned status code [%d]", rawURL, resp.StatusCode)
}

// parseRetryAfter parses an HTTP Retry-After header value into a duration.
// Per RFC 7231 the value is either delta-seconds (a non-negative integer) or an
// HTTP-date; we only accept the delta-seconds form (the asset server emits it that
// way) and treat HTTP-date as "no retry hint". Explicit integer parsing avoids
// time.ParseDuration's quirky acceptance of fractional/signed/unit-suffixed inputs
// like "-5s", "0.5s" or "1m".
// Returns 0 if the header is absent, non-numeric, or non-positive.
func parseRetryAfter(h string) time.Duration {
	if h == "" {
		return 0
	}
	secs, err := strconv.Atoi(h)
	if err != nil || secs <= 0 {
		return 0
	}
	// Clamp so secs*time.Second cannot overflow int64 (which would wrap to a negative duration
	// and be silently dropped). retryHTTP clamps again to its own ceiling before use.
	const maxRetryAfterSeconds = 1 << 33 // ~272 years
	if secs > maxRetryAfterSeconds {
		secs = maxRetryAfterSeconds
	}
	return time.Duration(secs) * time.Second
}

// retryConfig parameterizes DoHTTPRequestBodyReaderWithRetry. Exposed at package level so
// tests can shrink delays without using the production constants.
type retryConfig struct {
	maxAttempts  int
	initialDelay time.Duration
	maxDelay     time.Duration
	// maxRetryAfter caps an honored server Retry-After hint. It is deliberately larger than
	// maxDelay (which bounds the jittered exponential backoff): a legitimate peer whose advertised
	// backoff exceeds the 5s backoff cap must still be honored rather than re-hit every 5s. Zero
	// falls back to maxDelay (keeps shrunk test configs valid).
	maxRetryAfter time.Duration
	// maxBackoffTotal bounds the CUMULATIVE time spent sleeping between attempts of one call
	// (both exponential backoff and honored Retry-After waits). Without it, a peer answering
	// every request with a max-ceiling Retry-After could pin a single fetch for
	// maxAttempts*maxRetryAfter before failover. When the next wait would breach the budget the
	// loop stops and returns the terminal error rather than sleeping a truncated wait. Zero = no
	// cumulative bound (attempt-count only), keeping shrunk test configs valid.
	maxBackoffTotal time.Duration
}

var defaultRetryConfig = retryConfig{
	maxAttempts:     6,
	initialDelay:    250 * time.Millisecond,
	maxDelay:        5 * time.Second,
	maxRetryAfter:   30 * time.Second,
	maxBackoffTotal: 60 * time.Second,
}

// jitterDelay returns a randomised duration in [d/2, d] — i.e. "equal jitter"
// (half fixed, half random), not "full jitter" ([0, d]). The d/2 floor is
// deliberate: it avoids waking too early and re-bursting into the per-peer rate
// limiter. De-synchronising the backoff matters
// during p2p catchup: many heavy fetches hit the same per-peer rate limiter at
// once, so without jitter every retry wakes on the same tick and re-bursts into
// the limiter. Guarded so rand.Int64N never receives 0.
func jitterDelay(d time.Duration) time.Duration {
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + time.Duration(rand.Int64N(int64(half)+1))
}

// retryHTTP runs attempt with exponential backoff + equal jitter (see jitterDelay), retrying only
// while attempt returns an error of the retryable transient class
// (errors.Is(err, errors.ErrServiceUnavailable) — which buildHTTPError assigns
// to both HTTP 503 and HTTP 429). Any other error is returned immediately.
//
// attempt returns its result T, an optional server Retry-After hint (0 if none),
// and an error. A positive Retry-After is honored (jittered upward, clamped to
// maxRetryAfter); otherwise the jittered exponential backoff is used. ctx
// cancellation aborts the loop and returns the ctx error.
func retryHTTP[T any](ctx context.Context, cfg retryConfig, attempt func(context.Context) (T, time.Duration, error)) (T, error) {
	var zero T
	delay := cfg.initialDelay
	var lastErr error
	var sleptTotal time.Duration
	attemptsMade := 0

	for n := 1; n <= cfg.maxAttempts; n++ {
		attemptsMade = n
		res, retryAfter, err := attempt(ctx)
		if err == nil {
			return res, nil
		}
		if !errors.Is(err, errors.ErrServiceUnavailable) {
			return zero, err
		}
		lastErr = err

		if n == cfg.maxAttempts {
			break
		}

		sleepFor := jitterDelay(delay)
		if retryAfter > 0 {
			// Server named a minimum — honor it (never wake earlier), and add UPWARD jitter
			// ([retryAfter, 1.5*retryAfter]) so a fan-out of concurrent same-peer fetches doesn't all
			// re-issue on the same tick. The de-sync is effective for hints comfortably below the
			// ceiling and shrinks to zero at exactly maxRetryAfter — at the ceiling there is no
			// headroom to wait longer without exceeding our own cap, and we will not wait less than
			// the server's stated minimum, so that case is unavoidably deterministic. Clamp to
			// maxRetryAfter (an absolute ceiling above the backoff cap maxDelay) so an honest-but-busy
			// peer whose hint exceeds maxDelay is honored while a hostile hint can't stall us.
			ceiling := cfg.maxRetryAfter
			if ceiling <= 0 {
				ceiling = cfg.maxDelay
			}
			// Clamp the hint to the ceiling BEFORE adding jitter: a hostile Retry-After can put
			// retryAfter within 2^63 ns of overflow, where retryAfter+jitter wraps negative and
			// time.After(negative) fires immediately (no backoff at all). With retryAfter <= ceiling,
			// retryAfter + retryAfter/2 + 1 cannot overflow.
			if retryAfter > ceiling {
				retryAfter = ceiling
			}
			sleepFor = retryAfter + time.Duration(rand.Int64N(int64(retryAfter/2)+1))
			if sleepFor > ceiling {
				sleepFor = ceiling
			}
		}

		// Bound the CUMULATIVE backoff so a peer answering every attempt with a max-ceiling
		// Retry-After can't pin one fetch for maxAttempts*ceiling. When the next wait would breach
		// the budget, stop and fail over rather than sleep a truncated (sub-minimum) wait —
		// catchup always has other peers.
		if cfg.maxBackoffTotal > 0 && sleptTotal+sleepFor > cfg.maxBackoffTotal {
			break
		}
		sleptTotal += sleepFor

		select {
		case <-ctx.Done():
			// If the deadline expired while we were backing off from a real retryable
			// peer fault, attribute it to the peer (it stalled us out) rather than
			// returning a bare local context error — otherwise a peer that 429-spams us
			// until our fetch budget runs out evades any reputation penalty. A cancel
			// (e.g. shutdown), or a deadline with no prior peer fault, stays local.
			if lastErr != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
				// Peer too slow / rate-limiting ran out our budget. Use a network-timeout
				// (classified as a peer fault by error CODE, not by a fragile message
				// substring) so the reputation gates blame the peer. lastErr is carried as
				// the wrapped error, so no trailing format verb (would render %!v(MISSING)).
				return zero, errors.NewNetworkTimeoutError("http request aborted after %d attempt(s) (peer too slow or rate-limiting)", n, lastErr)
			}
			return zero, ctx.Err()
		case <-time.After(sleepFor):
		}

		delay *= 2
		if delay > cfg.maxDelay {
			delay = cfg.maxDelay
		}
	}

	return zero, errors.NewServiceUnavailableError("http request still failing after %d attempt(s)", attemptsMade, lastErr)
}

// DoHTTPRequestBodyReaderWithRetry behaves like DoHTTPRequestBodyReader but retries on
// HTTP 503/429 with exponential backoff. Used for endpoints where the server signals
// admission-control rejection or rate limiting (e.g. asset /subtree_data) and the right
// behavior is to back off and retry rather than fail the caller.
//
// Behavior:
//   - Retries only on errors satisfying errors.Is(err, errors.ErrServiceUnavailable)
//     (HTTP 503 and 429).
//   - Other errors (404, 500, network errors) are returned immediately — they are not
//     transient admission rejections.
//   - Backoff is exponential starting at 250ms, doubling, capped at 5s, with equal jitter ([d/2,d]).
//     Up to 6 attempts.
//   - Honors the server's Retry-After header when present (jittered upward, clamped to
//     maxRetryAfter, default 30s — deliberately above the 5s backoff cap).
//   - ctx cancellation aborts the retry loop and returns the parent ctx error.
//
// Each attempt is a fresh GET — for POST callers passing requestBody, the body is re-sent
// each time. Make sure that's idempotent before using this helper for non-GET workloads.
func DoHTTPRequestBodyReaderWithRetry(ctx context.Context, url string, requestBody ...[]byte) (io.ReadCloser, error) {
	return doHTTPRequestBodyReaderWithRetry(ctx, url, defaultRetryConfig, nil, requestBody...)
}

// DoHTTPRequestBodyReaderWithRetryFunc is DoHTTPRequestBodyReaderWithRetry with a
// per-attempt hook (e.g. a per-peer rate-limit wait) run before every attempt, so the
// limiter meters retries too, not just the first issuance. A nil hook is a no-op.
func DoHTTPRequestBodyReaderWithRetryFunc(ctx context.Context, url string, beforeAttempt func(context.Context) error, requestBody ...[]byte) (io.ReadCloser, error) {
	return doHTTPRequestBodyReaderWithRetry(ctx, url, defaultRetryConfig, beforeAttempt, requestBody...)
}

func doHTTPRequestBodyReaderWithRetry(ctx context.Context, url string, cfg retryConfig, beforeAttempt func(context.Context) error, requestBody ...[]byte) (io.ReadCloser, error) {
	return retryHTTP(ctx, cfg, func(c context.Context) (io.ReadCloser, time.Duration, error) {
		if beforeAttempt != nil {
			if err := beforeAttempt(c); err != nil {
				return nil, 0, err
			}
		}
		return doHTTPRequestForStreamingWithRetryAfter(c, url, requestBody...)
	})
}

// doHTTPRequestWithRetry behaves like DoHTTPRequest (reads the full body into memory)
// but retries on HTTP 503/429 with jittered exponential backoff. Intended for catchup
// heavy fetches (e.g. /blocks batches, single blocks) that must back off when a peer's
// asset endpoint rate-limits, instead of re-bursting and re-tripping the limiter.
// beforeAttempt (nil = no-op) runs before every attempt, e.g. a per-peer rate-limit wait.
func doHTTPRequestWithRetry(ctx context.Context, url string, cfg retryConfig, beforeAttempt func(context.Context) error, requestBody ...[]byte) ([]byte, error) {
	return retryHTTP(ctx, cfg, func(c context.Context) ([]byte, time.Duration, error) {
		if beforeAttempt != nil {
			if err := beforeAttempt(c); err != nil {
				return nil, 0, err
			}
		}
		return readBodyWithRetryAfter(c, url, -1, requestBody...)
	})
}

// DoHTTPRequestBoundedWithRetry behaves like DoHTTPRequestBounded (caps the body at
// maxBytes) but retries on HTTP 503/429 with jittered exponential backoff. Intended for
// catchup subtree fetches against peer-controlled asset endpoints.
// beforeAttempt (nil = no-op) runs before every attempt, e.g. a per-peer rate-limit wait.
func DoHTTPRequestBoundedWithRetry(ctx context.Context, url string, maxBytes int64, beforeAttempt func(context.Context) error, requestBody ...[]byte) ([]byte, error) {
	return doHTTPRequestBoundedWithRetry(ctx, url, maxBytes, defaultRetryConfig, beforeAttempt, requestBody...)
}

func doHTTPRequestBoundedWithRetry(ctx context.Context, url string, maxBytes int64, cfg retryConfig, beforeAttempt func(context.Context) error, requestBody ...[]byte) ([]byte, error) {
	return retryHTTP(ctx, cfg, func(c context.Context) ([]byte, time.Duration, error) {
		if beforeAttempt != nil {
			if err := beforeAttempt(c); err != nil {
				return nil, 0, err
			}
		}
		return readBodyWithRetryAfter(c, url, maxBytes, requestBody...)
	})
}

// readBodyWithRetryAfter performs a single HTTP request and reads the full response body
// into memory, returning any server Retry-After hint alongside the error so retryHTTP can
// honor it. maxBytes < 0 means unbounded; maxBytes >= 0 caps the body and returns
// ErrExternal if the peer streams more than the cap (mirrors DoHTTPRequestBounded).
//
// The body read is guarded by ctx.Done() (mirroring DoHTTPRequest/DoHTTPRequestBounded):
// a context timeout/cancel during the read returns NewNetworkTimeoutError — a non-local
// error — so a peer stalling mid-stream is correctly attributed to the peer rather than
// classified as a local error and silently absolved.
func readBodyWithRetryAfter(ctx context.Context, url string, maxBytes int64, requestBody ...[]byte) ([]byte, time.Duration, error) {
	// Use the standard request timeout (not the streaming timeout) to preserve the
	// behavior of the non-retry DoHTTPRequest/DoHTTPRequestBounded these helpers replace.
	reader, retryAfter, err := doRequestReaderWithRetryAfter(ctx, time.Duration(httpRequestTimeout)*time.Millisecond, url, requestBody...)
	if err != nil {
		return nil, retryAfter, err
	}
	defer func() { _ = reader.Close() }()

	// Shared body read + cancel-vs-deadline classification (see readBodyWithCtx).
	b, err := readBodyWithCtx(ctx, url, reader, maxBytes)
	return b, 0, err
}

// doHTTPRequestForStreamingWithRetryAfter is doHTTPRequestForStreaming + extracts
// the Retry-After header on non-OK responses. On success returns (body, 0, nil).
// Uses the longer streaming timeout, appropriate for large body-reader downloads.
func doHTTPRequestForStreamingWithRetryAfter(ctx context.Context, rawURL string, requestBody ...[]byte) (io.ReadCloser, time.Duration, error) {
	return doRequestReaderWithRetryAfter(ctx, time.Duration(httpStreamingTimeout)*time.Millisecond, rawURL, requestBody...)
}

// doRequestReaderWithRetryAfter performs a single GET/POST and returns the body
// reader plus any server Retry-After hint (extracted on non-OK responses). The
// timeout is applied only when ctx has no deadline. Callers choose the timeout so
// streaming downloads get the longer http_streaming_timeout while bounded/whole-body
// byte fetches keep the shorter http_timeout they had before retries were added.
func doRequestReaderWithRetryAfter(ctx context.Context, timeout time.Duration, rawURL string, requestBody ...[]byte) (io.ReadCloser, time.Duration, error) {
	// displayURL is the redacted (scheme://host) form for error text; rawURL itself is still used
	// to build the request. Peer-gossiped path/query bytes must never reach error messages, both to
	// avoid leaking them and to keep them out of the substring-matching error classifiers.
	displayURL := RedactPeerURL(rawURL)

	cancelFn := func() {}
	if _, ok := ctx.Deadline(); !ok {
		ctx, cancelFn = context.WithTimeout(ctx, timeout)
	}

	// Shared builder: validates, sets body Content-Type, and signs (the retry path
	// must sign too, or peers reject the request and we lose the rate-limit exemption).
	req, err := newSignedRequest(ctx, rawURL, requestBody...)
	if err != nil {
		cancelFn()
		return nil, 0, err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		cancelFn()
		// Classify context-derived failures the same way as the body-read path: a
		// deadline before the response (connect/TLS/header stall) means the peer was
		// too slow — a peer fault, surfaced as a non-local network timeout so the
		// reputation gate blames the peer and catchup keeps failing over. A cancel
		// (e.g. node shutdown) is local. Other Do errors stay generic ServiceErrors.
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, 0, errors.NewNetworkTimeoutError("http request [%s] timed out before response", displayURL)
		}
		if errors.Is(err, context.Canceled) {
			return nil, 0, errors.NewContextCanceledError("http request [%s] canceled before response", displayURL, context.Canceled)
		}
		// *url.Error renders as `Get "<full rawURL>": <cause>`, embedding the peer path/query.
		// Unwrap to the cause and attach only the redacted display URL ourselves.
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}
		return nil, 0, errors.NewServiceError("http request [%s] failed", displayURL, err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
		err := buildHTTPError(resp, rawURL)
		cancelFn()
		return nil, retryAfter, err
	}

	ct := strings.ToLower(resp.Header.Get("content-type"))
	if strings.HasPrefix(ct, "text/html") {
		cancelFn()
		return nil, 0, errors.NewServiceError("http request [%s] returned HTML - assume bad URL", displayURL)
	}

	return &readCloserWithCancel{ReadCloser: resp.Body, cancelFn: cancelFn}, 0, nil
}
