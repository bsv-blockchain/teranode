package util

import (
	"bytes"
	"context"
	stderrors "errors" //nolint:depguard // Structural unwrapping must bypass teranode message-based error classification.
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/ordishs/gocore"
)

// ssrfSafeDialer holds the connection settings used by the dialers built by
// NewSSRFSafeDialContext, which reject connections to unsafe IPs after DNS resolution.
// That closes the DNS-rebinding gap the static IP-literal check in ValidateURL cannot
// cover: a peer could pass http://internal.cluster.local/ whose hostname resolves to
// 169.254.169.254 (the cloud metadata endpoint) only at dial time.
var ssrfSafeDialer = &net.Dialer{
	Timeout:   30 * time.Second,
	KeepAlive: 30 * time.Second,
}

// ssrfLookupHost resolves a hostname to its IP addresses. It is a package var so tests can
// substitute a resolver that reproduces DNS-rebinding behaviour (e.g. returning a private
// address for a name that "looks" public).
var ssrfLookupHost = net.DefaultResolver.LookupHost

// SSRFDialPolicy reports why an IP resolved from a peer-supplied hostname is unsafe to
// connect to, or "" when it is safe. It is a parameter rather than a fixed rule so a caller
// can reuse the resolve-then-dial machinery below with its own address rules; callers
// fetching peer-supplied URLs should pass DefaultSSRFDialPolicy so every such path enforces
// one policy. Note that a policy stricter than the fetch path's is usually a mistake: it
// makes a peer unusable that block and subtree fetches would have talked to happily.
type SSRFDialPolicy func(net.IP) string

// NewSSRFSafeDialContext returns a DialContext that resolves the target hostname, rejects
// the dial if policy flags any resolved address, and otherwise connects to a validated IP.
// Install it as http.Transport.DialContext so every outgoing connection is checked,
// including connections made while following HTTP redirects.
//
// Critically, after validating the resolved addresses we dial those exact IPs rather than
// the hostname. Dialing by hostname would let net.Dialer perform a SECOND, independent DNS
// resolution at connect time — the classic DNS-rebinding TOCTOU bypass: a peer-controlled
// authoritative server with TTL=0 can return a public IP for our validation lookup and
// 169.254.169.254 / 127.0.0.1 for the dialer's lookup. Connecting to the already-validated
// IP closes that window. (The Transport still derives the TLS ServerName and Host header
// from the original URL, so dialing by IP does not break virtual hosting or HTTPS.)
//
// The returned dialer is a no-op passthrough when SSRF protection is disabled via
// SetSSRFProtection(false), which test daemons use to talk to localhost nodes.
func NewSSRFSafeDialContext(policy SSRFDialPolicy) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
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

			if reason := policy(ip); reason != "" {
				return nil, errors.NewInvalidArgumentError("SSRF dial check: resolved address %s for host %q is a %s", ipStr, host, reason)
			}

			validated = append(validated, ip)
		}

		if len(validated) == 0 {
			return nil, errors.NewServiceError("SSRF dial check: no usable addresses resolved for host %q", host)
		}

		// The policy judges this one answer. The guard compares it with earlier answers for
		// the same hostname, which is what stops a peer's DNS flipping a name from a public
		// address to a private one between two requests (issue 4843).
		if net.ParseIP(host) == nil {
			if reason := ssrfRebind.check(host, validated, time.Now()); reason != "" {
				return nil, errors.NewInvalidArgumentError("SSRF dial check: %s", reason)
			}
		}

		// Dial the validated IPs directly (no re-resolution), trying each to preserve
		// multi-A-record failover. Dialing by IP loses net.Dialer's dual-stack fast
		// fallback, so each attempt gets a slice of the remaining budget: without that, a
		// blackholed first address would consume the caller's whole deadline and a
		// reachable second address would never be tried. That matters most for short
		// budgets such as the p2p peer health probe.
		var lastErr error

		for i, ip := range validated {
			attemptCtx, cancelAttempt := dialAttemptContext(ctx, len(validated)-i)

			conn, dialErr := ssrfSafeDialer.DialContext(attemptCtx, network, net.JoinHostPort(ip.String(), port))

			cancelAttempt() // established connections are unaffected by cancelling the dial context

			if dialErr != nil {
				lastErr = dialErr
				continue
			}

			return conn, nil
		}

		return nil, lastErr
	}
}

// minDialAttemptBudget floors the per-address share of the deadline. An even split alone
// punishes well-behaved multi-address peers: under the 2s peer probe timeout, a hostname with
// four A records would give the first (usually working) address only 500ms, where a plain
// sequential dial would have let it use the whole remaining budget. The floor keeps the
// failover intent - a blackholed address cannot eat the entire deadline - without failing a
// reachable address that merely has an unremarkable RTT.
const minDialAttemptBudget = 500 * time.Millisecond

// dialAttemptContext bounds one dial attempt: each address gets its even share of the
// remaining deadline, floored at minDialAttemptBudget and never more than what remains. With
// no deadline set it returns the context unchanged.
func dialAttemptContext(ctx context.Context, remainingCandidates int) (context.Context, context.CancelFunc) {
	deadline, ok := ctx.Deadline()
	if !ok || remainingCandidates <= 1 {
		return context.WithCancel(ctx)
	}

	remaining := time.Until(deadline)
	if remaining <= 0 {
		return context.WithCancel(ctx)
	}

	budget := remaining / time.Duration(remainingCandidates)
	if budget < minDialAttemptBudget {
		budget = minDialAttemptBudget
	}

	if budget >= remaining {
		// The share (or the floor) covers everything left; no sub-deadline to impose.
		return context.WithCancel(ctx)
	}

	return context.WithTimeout(ctx, budget)
}

// DefaultSSRFDialPolicy is the dial policy applied to peer-supplied URLs by this package's
// shared client, returning the reason an address is unsafe or "" when it is safe. Services
// fetching peer-supplied URLs should reuse it so every such path enforces the same rules.
//
// It always blocks:
//   - link-local (169.254.0.0/16, fe80::/10), since the cloud metadata endpoint
//     169.254.169.254 lives here;
//   - loopback (127.0.0.0/8, ::1), since a peer should never make us dial our own
//     localhost admin/RPC services, and no legitimate peer advertises a loopback source;
//   - unspecified (0.0.0.0, ::).
//
// Private-network ranges (RFC1918, IPv6 ULA fc00::/7 and shared address space
// 100.64.0.0/10) are blocked unless SetSSRFAllowPrivateNetworks(true), which the daemon
// sets from p2p_allow_private_ips. That matches the static check on announced DataHub
// URLs, so a hostname resolving to a private address is treated like a private IP literal.
// Before issue 4843 private ranges were always allowed here, and a peer-controlled hostname
// could steer a request carrying a chosen body into an internal service such as a Kafka
// admin API.
func DefaultSSRFDialPolicy(ip net.IP) string {
	switch {
	case ip.IsLoopback():
		return "loopback address"
	case ip.IsLinkLocalUnicast(), ip.IsLinkLocalMulticast():
		return "link-local address"
	case ip.IsUnspecified():
		return "unspecified address"
	default:
		return privateNetworkDialPolicy(ip)
	}
}

// privateNetworkDialPolicy is the part of DefaultSSRFDialPolicy that follows
// SetSSRFAllowPrivateNetworks.
func privateNetworkDialPolicy(ip net.IP) string {
	if !ssrfAllowPrivateNetworks.Load() && ssrfIsPrivateNetwork(ip) {
		return "private-network address (set p2p_allow_private_ips to allow)"
	}

	return ""
}

// ssrfAllowPrivateNetworks holds whether peer-supplied URLs may resolve to private-network
// addresses. The zero value refuses them, so a process that never configures it fails closed.
var ssrfAllowPrivateNetworks atomic.Bool

// SetSSRFAllowPrivateNetworks sets whether connections for peer-supplied URLs may go to
// private-network addresses (RFC1918, fc00::/7, 100.64.0.0/10). The daemon calls it at
// startup with p2p_allow_private_ips.
func SetSSRFAllowPrivateNetworks(allowed bool) {
	ssrfAllowPrivateNetworks.Store(allowed)
}

// SSRFAllowPrivateNetworks reports the value last set by SetSSRFAllowPrivateNetworks.
func SSRFAllowPrivateNetworks() bool {
	return ssrfAllowPrivateNetworks.Load()
}

// sharedAddressSpace is RFC 6598's 100.64.0.0/10. Carrier NAT and some Kubernetes pod
// networks (EKS custom networking among them) use it for internal addresses, which
// net.IP.IsPrivate does not cover.
var sharedAddressSpace = &net.IPNet{IP: net.IPv4(100, 64, 0, 0).To4(), Mask: net.CIDRMask(10, 32)}

func isPrivateNetworkIP(ip net.IP) bool {
	return ip.IsPrivate() || sharedAddressSpace.Contains(ip)
}

// ssrfIsPrivateNetwork classifies an address as private-network. It is a package var so
// tests can have a loopback address stand in for a private one.
var ssrfIsPrivateNetwork = isPrivateNetworkIP

const (
	// ssrfRebindWindow is how long a hostname that resolved to a public address is barred
	// from resolving to a private-network one. The attack needs the two answers seconds
	// apart; a legitimate move from public to private hosting is rare and can wait.
	ssrfRebindWindow = 10 * time.Minute

	// ssrfRebindMaxHosts bounds the guard's memory.
	ssrfRebindMaxHosts = 10_000
)

// ssrfRebind is the process-wide rebinding guard consulted by every SSRF-safe dialer.
var ssrfRebind = newRebindGuard(ssrfRebindWindow, ssrfRebindMaxHosts)

// rebindGuard catches a hostname whose DNS answer moves from public to private-network
// between connections. Each dial resolves and validates on its own, and when private
// networks are allowed a peer's DNS could answer with its public server for a GET and an
// internal service for the POST that follows (issue 4843). Pinning per fetch would not
// help: subtree validation can reuse a stored subtree and send the POST with no GET first.
//
// It deliberately does not pin a hostname to exact addresses, since legitimate peers move
// between public addresses (failover, CDNs). It also cannot catch a hostname whose first
// answer is already private; only refusing private networks does that.
type rebindGuard struct {
	mu          sync.Mutex
	window      time.Duration
	maxHosts    int
	publicUntil map[string]time.Time
}

func newRebindGuard(window time.Duration, maxHosts int) *rebindGuard {
	return &rebindGuard{
		window:      window,
		maxHosts:    maxHosts,
		publicUntil: make(map[string]time.Time),
	}
}

// check records host's validated answer and returns why it must be refused, or "".
func (g *rebindGuard) check(host string, ips []net.IP, now time.Time) string {
	var public, private bool

	for _, ip := range ips {
		if ssrfIsPrivateNetwork(ip) {
			private = true
		} else {
			public = true
		}
	}

	// A mixed answer lets the dialer's per-address failover reach the private address
	// once the public one refuses the connection.
	if public && private {
		return fmt.Sprintf("host %q resolved to both public and private-network addresses", host)
	}

	key := strings.ToLower(strings.TrimRight(host, "."))

	g.mu.Lock()
	defer g.mu.Unlock()

	if private {
		if until, ok := g.publicUntil[key]; ok && now.Before(until) {
			return fmt.Sprintf("host %q resolved to a public address within the last %s and now to private-network address %s", host, g.window, ips[0])
		}

		return ""
	}

	if _, ok := g.publicUntil[key]; !ok && len(g.publicUntil) >= g.maxHosts {
		g.evictLocked(now)
	}

	g.publicUntil[key] = now.Add(g.window)

	return ""
}

// evictLocked frees room for one entry: expired entries go first, then an arbitrary one.
func (g *rebindGuard) evictLocked(now time.Time) {
	for host, until := range g.publicUntil {
		if !now.Before(until) {
			delete(g.publicUntil, host)
		}
	}

	for host := range g.publicUntil {
		if len(g.publicUntil) < g.maxHosts {
			return
		}

		delete(g.publicUntil, host)
	}
}

// ssrfDialContext is the DialContext installed on httpClient, enforcing
// DefaultSSRFDialPolicy on every connection made for peer-supplied URLs.
var ssrfDialContext = NewSSRFSafeDialContext(DefaultSSRFDialPolicy)

// maxSSRFRedirects bounds redirect chains followed while fetching peer-supplied URLs.
const maxSSRFRedirects = 10

// ssrfCheckRedirect builds the CheckRedirect used for peer-supplied URLs: it bounds the hop
// count, then rejects a redirect target that leaves http/https, carries credentials, or names
// a blocked IP literal. Targets naming a hostname are caught by the dialer instead, so this
// is a cheap pre-check that avoids attempting the connection at all.
//
// Both the shared httpClient and every client from NewSSRFSafeHTTPClient use this, so there is
// one redirect rule for the threat rather than two that can drift apart.
func ssrfCheckRedirect(policy SSRFDialPolicy) func(req *http.Request, via []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if len(via) >= maxSSRFRedirects {
			return errors.NewInvalidArgumentError("stopped after %d redirects", maxSSRFRedirects)
		}

		if !ssrfProtectionEnabled.Load() {
			return nil
		}

		if len(via) > 0 && via[0] != nil {
			// A POST body was built from peer-supplied data. Go does not replay it on a 307
			// or 308, but a 301, 302 or 303 turns it into a GET to wherever the peer points.
			if via[0].Method == http.MethodPost {
				return errors.NewInvalidArgumentError("SSRF redirect check: refusing to follow a redirect of a POST")
			}

			if via[0].URL != nil && !sameOriginOrUpgrade(via[0].URL, req.URL) {
				return errors.NewInvalidArgumentError("SSRF redirect check: redirect leaves the origin of the requested URL")
			}
		}

		scheme := strings.ToLower(req.URL.Scheme)
		if scheme != "http" && scheme != "https" {
			return errors.NewInvalidArgumentError("SSRF redirect check: invalid scheme %q", scheme)
		}

		// Userinfo has no legitimate use here and can be used to confuse logging or
		// smuggle credentials; ValidateURL rejects it on the initial request too.
		if req.URL.User != nil {
			return errors.NewInvalidArgumentError("SSRF redirect check: target must not contain userinfo (credentials)")
		}

		if ip := net.ParseIP(req.URL.Hostname()); ip != nil {
			if reason := policy(ip); reason != "" {
				return errors.NewInvalidArgumentError("SSRF redirect check: target %s is a %s", ip.String(), reason)
			}
		}

		return nil
	}
}

// sameOriginOrUpgrade reports whether a redirect from one URL to another keeps the same
// origin (scheme, host and port), allowing only an http to https upgrade on the same host
// between the default ports. Teranode's asset service never redirects, so a cross-origin hop
// can only be a peer steering the request somewhere else.
func sameOriginOrUpgrade(from, to *url.URL) bool {
	canonicalHost := func(u *url.URL) string {
		return strings.ToLower(strings.TrimRight(u.Hostname(), "."))
	}

	effectivePort := func(u *url.URL, scheme string) string {
		if p := u.Port(); p != "" {
			return p
		}

		if scheme == "https" {
			return "443"
		}

		return "80"
	}

	if canonicalHost(from) != canonicalHost(to) {
		return false
	}

	fromScheme, toScheme := strings.ToLower(from.Scheme), strings.ToLower(to.Scheme)
	fromPort, toPort := effectivePort(from, fromScheme), effectivePort(to, toScheme)

	if fromScheme == toScheme {
		return fromPort == toPort
	}

	return fromScheme == "http" && toScheme == "https" && fromPort == "80" && toPort == "443"
}

// NewSSRFSafeHTTPClient returns an HTTP client for fetching peer-supplied URLs. Every
// connection it makes - including connections for redirect hops - is checked against
// policy after DNS resolution, and redirect targets are additionally rejected if they
// leave http/https or name a blocked IP literal.
//
// timeout bounds the whole request; pass 0 to rely on the request context instead.
//
// Caveat: the transport keeps http.DefaultTransport's ProxyFromEnvironment, matching the
// shared httpClient below. With HTTP_PROXY/HTTPS_PROXY set, connections are made to the
// proxy - so policy validates the proxy's address and the proxy fetches the peer-supplied
// target on our behalf, outside the reach of this check.
func NewSSRFSafeHTTPClient(timeout time.Duration, policy SSRFDialPolicy) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = NewSSRFSafeDialContext(policy)

	return &http.Client{
		Timeout:       timeout,
		Transport:     transport,
		CheckRedirect: ssrfCheckRedirect(policy),
	}
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
	// same policy to redirect targets - the identical check NewSSRFSafeHTTPClient installs -
	// so a peer-controlled server cannot bounce us to an internal address.
	httpClient = &http.Client{
		Transport: func() *http.Transport {
			t := http.DefaultTransport.(*http.Transport).Clone()
			t.MaxIdleConns = 1000       // Total idle connections across all hosts (default: 100)
			t.MaxIdleConnsPerHost = 100 // Per-host idle connections (default: 2)
			t.MaxConnsPerHost = 200     // Per-host total connections (default: 0/unlimited)
			t.DialContext = ssrfDialContext
			return t
		}(),
		CheckRedirect: ssrfCheckRedirect(DefaultSSRFDialPolicy),
	}
)

// localServiceHTTPClient reaches this node's own services at operator-configured
// addresses, which are routinely loopback (the default asset_httpAddress is localhost) or a
// private container address. It has no SSRF dial guard, so it must never be given a
// peer-supplied URL.
var localServiceHTTPClient = &http.Client{
	Transport: func() *http.Transport {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.MaxIdleConns = 100
		t.MaxIdleConnsPerHost = 100
		return t
	}(),
}

// DoLocalServiceHTTPRequestBodyReader streams a GET from one of this node's own services,
// such as the legacy service reading blocks from the local asset service. The URL must come
// from this node's settings, never from a peer: unlike DoHTTPRequestBodyReader it does not
// refuse loopback or private addresses. The default timeout matches DoHTTPRequestBodyReader.
func DoLocalServiceHTTPRequestBodyReader(ctx context.Context, url string) (io.ReadCloser, error) {
	cancelFn := func() {
		// noop
	}

	if _, ok := ctx.Deadline(); !ok {
		ctx, cancelFn = context.WithTimeout(ctx, time.Duration(httpStreamingTimeout)*time.Millisecond)
	}

	bodyReaderCloser, cancelFn, err := executeHTTPRequestWithClient(ctx, cancelFn, localServiceHTTPClient, url)
	if err != nil {
		cancelFn()
		return nil, err
	}

	return &readCloserWithCancel{
		ReadCloser: bodyReaderCloser,
		cancelFn:   cancelFn,
		rawURL:     url,
	}, nil
}

// HTTPClient returns the shared HTTP client for use with httpmock.ActivateNonDefault() in tests.
func HTTPClient() *http.Client {
	return httpClient
}

// DoHTTPRequest performs an HTTP GET or POST request and returns the response body as bytes.
// Uses GET by default, switches to POST if requestBody is provided.
// Automatically handles timeouts and validates response status codes.
//
// Deprecated: this reads the whole body with io.ReadAll and applies no cap, so a peer-controlled
// response of unbounded size is read into memory in full (bitcoin-sv/teranode#4742). It has no
// production callers left. Use DoHTTPRequestBounded for a caller-known size, or
// DoHTTPRequestBodyReader to stream and bound the parse itself.
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
// (we are at fault, don't blame the peer). Other read errors are sanitized network errors.
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
			return nil, sanitizeHTTPTransportError(readErr, url)
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
	if maxBytes < 0 {
		return nil, errors.NewConfigurationError("bounded HTTP response byte limit must be non-negative")
	}

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
	cancelFn       context.CancelFunc
	rawURL         string
	sanitizeErrors bool
}

// Read sanitizes protocol errors that surface only after response headers, such
// as malformed chunked trailers. EOF remains the reader completion sentinel.
func (r *readCloserWithCancel) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if r.sanitizeErrors && err != nil && err != io.EOF {
		err = sanitizeHTTPTransportError(err, r.rawURL)
	}
	return n, err
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
		ReadCloser:     bodyReaderCloser,
		cancelFn:       cancelFn,
		rawURL:         url,
		sanitizeErrors: true,
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

// SSRFProtectionEnabled reports whether SSRF URL validation is currently active.
// Tests that disable it should restore this value rather than assuming the
// default, so a nested or subsequent test cannot be silently left unprotected —
// or protected when its caller had deliberately turned it off.
func SSRFProtectionEnabled() bool {
	return ssrfProtectionEnabled.Load()
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
		// Parse causes can echo invalid ports or hosts containing classifier sentinels.
		return errors.NewInvalidArgumentError("invalid URL")
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

// sanitizeHTTPTransportError keeps only structural failure classifications.
// net/http parser errors can quote peer-controlled header and redirect bytes;
// retaining any raw cause lets legacy substring classifiers forge a local fault.
func sanitizeHTTPTransportError(err error, rawURL string) error {
	displayURL := RedactPeerURL(rawURL)
	// Do not call the teranode Error.Is method for context classification: it
	// also accepts message substrings. Genuine wrapped sentinels remain intact.
	for cause := err; cause != nil; cause = stderrors.Unwrap(cause) {
		// The body reader may already have sanitized this error. Preserve its
		// code while constructing a fresh fixed message without wrapped text.
		if typed, ok := cause.(*errors.Error); ok {
			switch typed.Code() {
			case errors.ERR_NETWORK_TIMEOUT:
				return errors.NewNetworkTimeoutError("http request [%s] timed out", displayURL)
			case errors.ERR_NETWORK_CONNECTION_REFUSED:
				return errors.NewNetworkConnectionRefusedError("http request [%s] connection refused", displayURL)
			}
		}
		switch cause {
		case context.Canceled:
			return errors.NewContextCanceledError("http request [%s] canceled", displayURL, context.Canceled)
		case context.DeadlineExceeded:
			return errors.NewNetworkTimeoutError("http request [%s] timed out", displayURL)
		}
	}
	var netErr net.Error
	if stderrors.As(err, &netErr) && netErr.Timeout() {
		return errors.NewNetworkTimeoutError("http request [%s] timed out", displayURL)
	}
	if stderrors.Is(err, syscall.ECONNREFUSED) {
		return errors.NewNetworkConnectionRefusedError("http request [%s] connection refused", displayURL)
	}
	return errors.NewNetworkError("http request [%s] transport failure", displayURL)
}

// newSignedRequest builds a validated, optionally-signed *http.Request for rawURL.
// GET by default; POST with an octet-stream body when requestBody is provided.
//
// Both peer entry points validate URLs and share buildSignedRequest with the
// local-service path, so retries retain request signing and the binary content type.
func newSignedRequest(ctx context.Context, rawURL string, requestBody ...[]byte) (*http.Request, error) {
	if err := ValidateURL(rawURL); err != nil {
		return nil, err
	}

	return buildSignedRequest(ctx, rawURL, requestBody...)
}

// buildSignedRequest also supports local services, whose operator-configured URLs
// are not subject to the peer URL policy.
func buildSignedRequest(ctx context.Context, rawURL string, requestBody ...[]byte) (*http.Request, error) {

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		// Request construction may fail while parsing peer-controlled URL text.
		// Its parse cause is unsafe even after removing the outer *url.Error.
		return nil, errors.NewServiceError("failed to create http request")
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

// executeHTTPRequest performs a request through the SSRF-guarded peer client.
func executeHTTPRequest(ctx context.Context, cancelFn context.CancelFunc, rawURL string, requestBody ...[]byte) (io.ReadCloser, context.CancelFunc, error) {
	if err := ValidateURL(rawURL); err != nil {
		return nil, cancelFn, err
	}
	return executeHTTPRequestWithClient(ctx, cancelFn, httpClient, rawURL, requestBody...)
}

// executeHTTPRequestWithClient also serves operator-configured local URLs.
func executeHTTPRequestWithClient(ctx context.Context, cancelFn context.CancelFunc, client *http.Client, rawURL string, requestBody ...[]byte) (io.ReadCloser, context.CancelFunc, error) {
	req, err := buildSignedRequest(ctx, rawURL, requestBody...)
	if err != nil {
		return nil, cancelFn, err
	}

	var resp *http.Response
	resp, err = client.Do(req)
	if err != nil {
		if client == localServiceHTTPClient {
			// Operator-configured services belong to this node. Preserve their
			// local classification instead of attributing failures to a peer.
			return nil, cancelFn, errors.NewServiceError("failed to do local http request", err)
		}
		return nil, cancelFn, sanitizeHTTPTransportError(err, rawURL)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, cancelFn, buildHTTPError(resp, rawURL)
	}

	ct := strings.ToLower(resp.Header.Get("content-type"))
	isHTML := strings.HasPrefix(ct, "text/html")
	if isHTML {
		// The body is never returned on this path, so it has to be closed here or the
		// connection leaks outright — which defeats the point of draining error bodies
		// a few lines up. A 2xx with an unexpected content type is worth draining for
		// reuse rather than tearing down, so the same bounded helper applies.
		drainAndCloseErrorBody(resp.Body)

		return nil, cancelFn, errors.NewServiceError("http request [%s] returned HTML - assume bad URL", RedactPeerURL(rawURL))
	}

	return resp.Body, cancelFn, nil
}

// maxErrorBodyBytes caps the initial error-body prefix counted for diagnostics.
// The remaining bytes have a separate bounded drain for connection reuse.
// Never embed peer body text in classified errors: message-based legacy context
// detection would let that text forge local cancellation and suppress failover.
const maxErrorBodyBytes = 2 << 10 // 2 KiB

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
	// Host is decoded by url.Parse, including spaces in IPv6 zone identifiers.
	// Serialize again so those bytes cannot enter legacy message classifiers.
	return (&url.URL{Scheme: u.Scheme, Host: u.Host}).String()
}

// maxHTTPErrorBodyDrainBytes bounds how much of the REMAINDER of an error body is
// drained, after the prefix above has been read, purely so the connection can go
// back into http.Transport's idle pool.
//
// The trade-off is deliberate. A body must be read to EOF for the connection to be
// reusable, so capping the read at maxErrorBodyBytes and closing turned every
// error larger than 2 KiB into a fresh TCP (+TLS) handshake — on the high-rate
// failure path, which is exactly when handshakes hurt most. An unbounded
// io.Copy(io.Discard, ...) would restore reuse but reopen the hole the prefix cap
// was added to close: a hostile peer answering with an error status and streaming
// forever. So the drain is itself bounded. A body whose remainder fits is drained
// and its connection reused; anything larger is abandoned and Close tears the
// connection down, which is the correct outcome for a peer that streams
// unboundedly on an error status.
//
// The budget is measured from wherever the prefix read stopped, so bodies just
// over the prefix cap — the common case for a verbose error page — are fully
// drained.
const maxHTTPErrorBodyDrainBytes = 64 * 1024

// maxHTTPErrorBodyDrainWait bounds how long the CALLER waits for the remainder drain
// before abandoning it to the background.
//
// It exists because the drain's two requirements pull in opposite directions:
//
//   - Connection reuse needs the body read to EOF and CLOSED *before the caller
//     returns*. Every caller of buildHTTPError cancels its request context immediately
//     afterwards — DoHTTPRequest defers cancelFn, doHTTPRequestForStreamingWithRetryAfter
//     calls it outright — and response-body reads honour that context. So a drain that
//     has not finished by then is killed and the connection is discarded: a purely
//     asynchronous drain delivers no reuse at all, which is the entire point of
//     draining.
//   - A peer that dribbles must not hold the caller. Body reads are otherwise bounded
//     only by the request context, up to the 5-minute streaming timeout, compounded by
//     up to six 503 retries.
//
// So the drain is synchronous up to this budget and abandoned past it. A peer whose
// remainder is already buffered — the common case for a verbose error page — drains in
// microseconds and its connection is reused; a dribbler costs the caller this much and
// no more.
//
// Across a retry ladder that budget multiplies: DoHTTPRequestBodyReaderWithRetry makes
// up to defaultRetryConfig.maxAttempts attempts, so its worst case is
// maxAttempts x this budget — 6 x 250ms = 1.5s — reached only against a peer that
// dribbles its error body on every one of the six attempts. Set that against the
// ~7.75s of exponential backoff the same ladder already spends between those attempts
// (250ms doubling, capped at 5s), and the added share is bounded enough that the
// budget is deliberately not shortened: cutting it would trade away the connection
// reuse the synchronous drain exists to buy.
const maxHTTPErrorBodyDrainWait = 250 * time.Millisecond

// drainAndCloseErrorBody drains the remainder of an error body and closes it so the
// connection can return to http.Transport's idle pool, while bounding what that costs
// the caller.
//
// Bounded three ways: in BYTES by maxHTTPErrorBodyDrainBytes, against a peer that
// streams forever; in the CALLER'S TIME by maxHTTPErrorBodyDrainWait; and, once
// abandoned, by the request context its caller is about to cancel anyway.
//
// The goroutine takes sole ownership of the body and closes it exactly once, whether it
// finished or was abandoned — callers must not touch the body afterwards.
//
// Note what this does NOT bound: a caller can still block on the prefix read in
// buildHTTPError, which is synchronous and capped in bytes (2 KiB) but not in time.
// That exposure is pre-existing and strictly smaller than the unbounded io.ReadAll it
// replaced; bounding it in time needs a bounded-time reader and would trade away the
// initial byte-count diagnostic for a legitimately slow error response.
func drainAndCloseErrorBody(body io.ReadCloser) {
	if body == nil {
		return
	}

	done := make(chan struct{})

	go func() {
		defer close(done)

		drainErrorBody(body)
	}()

	timer := time.NewTimer(maxHTTPErrorBodyDrainWait)
	defer timer.Stop()

	select {
	case <-done:
		// Drained and closed before the caller returns, so the connection is reusable.
	case <-timer.C:
		// Abandoned. The goroutine keeps sole ownership and closes the body when it
		// stops, at the byte cap or when the request context dies.
	}
}

// drainErrorBody is the byte-bounded drain-then-close that drainAndCloseErrorBody waits
// on. Split out so the byte bound and the close can be asserted directly, without a
// test having to race a goroutine.
func drainErrorBody(body io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, maxHTTPErrorBodyDrainBytes))
	_ = body.Close()
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
// The body is never embedded in the message: peer-controlled text could forge a
// local error through legacy substring classification. After counting a small prefix,
// drainAndCloseErrorBody bounds the remaining drain in bytes and caller time so
// buffered error responses can reuse their connection without following an endless stream.
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
		// Ownership of the body transfers to the drain once this returns; the prefix
		// read below completes first, because the deferred call runs last.
		defer drainAndCloseErrorBody(resp.Body)

		// Drain a bounded amount of the body (helps keep-alive connection reuse) but do
		// NOT embed the peer-controlled content in the error message. This error feeds
		// substring-based classification at the catchup reputation gates (IsContextError /
		// releaseCatchupLock's strings.Contains checks). A hostile or unlucky peer whose
		// body contained e.g. "context deadline exceeded" or "block assembly is behind"
		// could otherwise forge a "local" classification — clearing its reputation penalty
		// AND halting failover to honest peers, re-opening the #1174 wedge. Only the
		// status code, prefix length, and redacted URL go into the message.
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
	secs, err := strconv.ParseInt(h, 10, 64)
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
	return half + time.Duration(rand.Int64N(int64(half)+1)) // #nosec G404 -- retry jitter needs dispersion, not secrecy.
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
	var sleptTotal time.Duration
	attemptsMade := 0

	for n := 1; n <= cfg.maxAttempts; n++ {
		attemptsMade = n
		res, retryAfter, err := attempt(ctx)
		if err == nil {
			return res, nil
		}
		if localErr, ok := err.(*localHTTPAttemptError); ok {
			return zero, localErr.error
		}
		if !errors.Is(err, errors.ErrServiceUnavailable) {
			return zero, err
		}

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
			sleepFor = retryAfter + time.Duration(rand.Int64N(int64(retryAfter/2)+1)) // #nosec G404 -- retry jitter is not security-sensitive.
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
			if ctx.Err() == context.DeadlineExceeded {
				// Peer too slow / rate-limiting ran out our budget. Use a network-timeout
				// (classified as a peer fault by error CODE, not by a fragile message
				// substring) so the reputation gates blame the peer. Do not wrap the
				// retryable service cause, which would also classify as a local fault.
				return zero, errors.NewNetworkTimeoutError("http request aborted after %d attempt(s) (peer too slow or rate-limiting)", n)
			}
			return zero, ctx.Err()
		case <-time.After(sleepFor):
		}

		delay *= 2
		if delay > cfg.maxDelay {
			delay = cfg.maxDelay
		}
	}

	// Do not retain the ServiceUnavailable cause: local-error classifiers walk
	// the whole chain, and would otherwise absolve the exhausted peer.
	return zero, errors.NewNetworkError("http request still failing after %d attempt(s)", attemptsMade)
}

// localHTTPAttemptError keeps a local pacing failure out of the peer retry ladder.
// retryHTTP returns the original error to preserve the caller's classification.
type localHTTPAttemptError struct{ error }

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
				return nil, 0, &localHTTPAttemptError{err}
			}
		}
		return doHTTPRequestForStreamingWithRetryAfter(c, url, requestBody...)
	})
}

// DoHTTPRequestBoundedWithRetry behaves like DoHTTPRequestBounded (caps the body at
// maxBytes) but retries on HTTP 503/429 with jittered exponential backoff. Intended for
// catchup subtree fetches against peer-controlled asset endpoints.
// beforeAttempt (nil = no-op) runs before every attempt, e.g. a per-peer rate-limit wait.
func DoHTTPRequestBoundedWithRetry(ctx context.Context, url string, maxBytes int64, beforeAttempt func(context.Context) error, requestBody ...[]byte) ([]byte, error) {
	if maxBytes < 0 {
		return nil, errors.NewConfigurationError("bounded HTTP response byte limit must be non-negative")
	}

	return doHTTPRequestBoundedWithRetry(ctx, url, maxBytes, defaultRetryConfig, beforeAttempt, requestBody...)
}

func doHTTPRequestBoundedWithRetry(ctx context.Context, url string, maxBytes int64, cfg retryConfig, beforeAttempt func(context.Context) error, requestBody ...[]byte) ([]byte, error) {
	return retryHTTP(ctx, cfg, func(c context.Context) ([]byte, time.Duration, error) {
		if beforeAttempt != nil {
			if err := beforeAttempt(c); err != nil {
				return nil, 0, &localHTTPAttemptError{err}
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
// a deadline during the read returns NewNetworkTimeoutError so a peer stalling
// mid-stream remains a peer fault; explicit cancellation stays local.
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
		return nil, 0, sanitizeHTTPTransportError(err, rawURL)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
		err := buildHTTPError(resp, rawURL)
		cancelFn()
		return nil, retryAfter, err
	}

	ct := strings.ToLower(resp.Header.Get("content-type"))
	if strings.HasPrefix(ct, "text/html") {
		// The body is not handed to the caller on this path, so it must be closed here
		// or the connection leaks outright. Closed rather than drained: cancelFn below
		// cancels the request context, which makes the connection unusable anyway, so a
		// drain for reuse would be spending a goroutine on nothing.
		if resp.Body != nil {
			_ = resp.Body.Close()
		}

		cancelFn()
		return nil, 0, errors.NewServiceError("http request [%s] returned HTML - assume bad URL", displayURL)
	}

	return &readCloserWithCancel{ReadCloser: resp.Body, cancelFn: cancelFn, rawURL: rawURL, sanitizeErrors: true}, 0, nil
}
