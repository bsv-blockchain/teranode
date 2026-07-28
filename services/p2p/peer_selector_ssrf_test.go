package p2p

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func newSelectorWithPrivateIPs(t *testing.T, allowPrivateIPs bool) *PeerSelector {
	t.Helper()

	return NewPeerSelector(ulogger.TestLogger{}, &settings.Settings{
		P2P: settings.P2PSettings{
			HealthCheckEnabled: true,
			AllowPrivateIPs:    allowPrivateIPs,
		},
	})
}

// TestPeerHealthCheck_RejectsHostnameResolvingToLoopback is the regression test for the
// SSRF bypass: validateDataHubURL only inspects IP literals, so a peer can advertise a
// hostname and have the health check probe whatever it resolves to. The probe must be
// refused at dial time, after resolution, and the target server must see no request.
func TestPeerHealthCheck_RejectsHostnameResolvingToLoopback(t *testing.T) {
	var hits int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	_, port, err := net.SplitHostPort(mustParseURL(t, server.URL).Host)
	require.NoError(t, err)

	// A hostname, not an IP literal - so the static validateDataHubURL IP check never
	// sees an address, and only DNS resolution reveals that it is internal.
	dataHubURL := "http://localhost:" + port + "/api/v1"

	ps := newSelectorWithPrivateIPs(t, false)

	healthy, err := ps.checkPeerAvailability(context.Background(), dataHubURL)
	require.False(t, healthy)
	require.Error(t, err)
	require.Contains(t, err.Error(), "loopback address")
	require.Zero(t, hits, "the probe must not reach the internal target")
}

// TestPeerHealthCheck_AllowPrivateIPsBypass verifies the escape hatch used by local and
// docker deployments still lets the probe through, and that a reachable peer is reported
// healthy (i.e. the guarded client is otherwise a working HTTP client).
func TestPeerHealthCheck_AllowPrivateIPsBypass(t *testing.T) {
	var path string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	_, port, err := net.SplitHostPort(mustParseURL(t, server.URL).Host)
	require.NoError(t, err)

	ps := newSelectorWithPrivateIPs(t, true)

	healthy, err := ps.checkPeerAvailability(context.Background(), "http://localhost:"+port+"/api/v1")
	require.NoError(t, err)
	require.True(t, healthy)
	require.Equal(t, "/api/v1/bestblockheader", path)
}

// TestPeerHealthCheck_RejectsLoopbackIPLiteral covers the IP-literal case going through
// the same dialer, so the guard does not depend on the static pre-check having run.
func TestPeerHealthCheck_RejectsLoopbackIPLiteral(t *testing.T) {
	ps := newSelectorWithPrivateIPs(t, false)

	healthy, err := ps.checkPeerAvailability(context.Background(), "http://127.0.0.1:1/api/v1")
	require.False(t, healthy)
	require.Error(t, err)
	require.Contains(t, err.Error(), "loopback address")
}

func TestPeerHealthCheck_EmptyURL(t *testing.T) {
	ps := newSelectorWithPrivateIPs(t, false)

	healthy, err := ps.checkPeerAvailability(context.Background(), "")
	require.False(t, healthy)
	require.NoError(t, err)
}

func TestDataHubDialPolicy(t *testing.T) {
	blocked := map[string]string{
		"127.0.0.1":       "loopback address",
		"::1":             "loopback address",
		"10.0.0.5":        "private address",
		"192.168.1.10":    "private address",
		"172.16.4.4":      "private address",
		"169.254.169.254": "link-local address",
		"fe80::1":         "link-local address",
		"0.0.0.0":         "unspecified address",
	}

	strict := newSelectorWithPrivateIPs(t, false)
	permissive := newSelectorWithPrivateIPs(t, true)

	for ipStr, reason := range blocked {
		ip := net.ParseIP(ipStr)
		require.NotNil(t, ip, ipStr)
		require.Equal(t, reason, strict.dataHubDialPolicy(ip), "expected %s to be rejected", ipStr)
		require.Empty(t, permissive.dataHubDialPolicy(ip), "AllowPrivateIPs must bypass the check for %s", ipStr)
	}

	for _, ipStr := range []string{"8.8.8.8", "1.2.3.4", "2606:4700::1111"} {
		ip := net.ParseIP(ipStr)
		require.NotNil(t, ip, ipStr)
		require.Empty(t, strict.dataHubDialPolicy(ip), "expected %s to be allowed", ipStr)
	}
}

// TestPeerHealthCheck_RedirectGuard covers the second hop: a peer-controlled server that
// answers the probe with a redirect must not be able to point the node at an internal
// address or a non-HTTP scheme.
func TestPeerHealthCheck_RedirectGuard(t *testing.T) {
	ps := newSelectorWithPrivateIPs(t, false)

	redirectTo := func(t *testing.T, rawURL string, hops int) error {
		t.Helper()

		via := make([]*http.Request, hops)
		for i := range via {
			via[i] = &http.Request{}
		}

		return ps.httpClient.CheckRedirect(&http.Request{URL: mustParseURL(t, rawURL)}, via)
	}

	err := redirectTo(t, "http://169.254.169.254/latest/meta-data/", 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "link-local address")

	err = redirectTo(t, "http://10.0.0.5/admin", 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "private address")

	err = redirectTo(t, "file:///etc/passwd", 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid scheme")

	err = redirectTo(t, "http://peer.example/api/v1", maxPeerHealthCheckRedirects)
	require.Error(t, err)
	require.Contains(t, err.Error(), "redirects")

	require.NoError(t, redirectTo(t, "https://peer.example/api/v1", 1))
}

// TestValidateDataHubURL_HostnamePassesStaticCheck pins the documented division of
// labour: the static check accepts peer hostnames (it does no DNS), which is exactly why
// the dial-time guard above has to exist.
func TestValidateDataHubURL_HostnamePassesStaticCheck(t *testing.T) {
	s := &Server{settings: &settings.Settings{}}

	require.NoError(t, s.validateDataHubURL("http://metadata.attacker.example/api/v1"))
	require.Error(t, s.validateDataHubURL("http://169.254.169.254/api/v1"))
}

func mustParseURL(t *testing.T, rawURL string) *url.URL {
	t.Helper()

	parsed, err := url.Parse(rawURL)
	require.NoError(t, err)

	return parsed
}
