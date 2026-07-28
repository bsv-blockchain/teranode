package p2p

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
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
// SSRF bypass: validateDataHubURL only inspects IP literals, so a peer could advertise a
// hostname and have the health check probe whatever it resolved to. The probe must be
// refused after resolution, and the target must see no request.
func TestPeerHealthCheck_RejectsHostnameResolvingToLoopback(t *testing.T) {
	var hits atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// A hostname, not an IP literal - so the static validateDataHubURL IP check never sees
	// an address, and only DNS resolution reveals that the target is internal.
	dataHubURL := "http://localhost:" + serverPort(t, server) + "/api/v1"

	ps := newSelectorWithPrivateIPs(t, false)

	healthy, err := ps.checkPeerAvailability(context.Background(), dataHubURL)
	require.False(t, healthy)
	require.Error(t, err)
	require.Contains(t, err.Error(), "loopback address")
	require.Zero(t, hits.Load(), "the probe must not reach the internal target")
}

// TestPeerHealthCheck_AllowPrivateIPsBypass verifies the escape hatch used by single-host
// deployments still lets the probe through, and that a reachable peer is reported healthy
// (i.e. the guarded client is otherwise a working HTTP client, hitting the right path).
func TestPeerHealthCheck_AllowPrivateIPsBypass(t *testing.T) {
	var path atomic.Value

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path.Store(r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ps := newSelectorWithPrivateIPs(t, true)

	// "localhost" may resolve to ::1 before 127.0.0.1 while httptest listens on 127.0.0.1
	// only; the dialer's per-address failover is what makes this reach the server.
	healthy, err := ps.checkPeerAvailability(context.Background(), "http://localhost:"+serverPort(t, server)+"/api/v1")
	require.NoError(t, err)
	require.True(t, healthy)
	require.Equal(t, "/api/v1/bestblockheader", path.Load())
}

// TestPeerHealthCheck_RejectsInternalIPLiterals covers literals going through the same
// dialer, so the guard does not depend on the static pre-check having run first.
func TestPeerHealthCheck_RejectsInternalIPLiterals(t *testing.T) {
	tests := map[string]string{
		"http://127.0.0.1:1/api/v1":     "loopback address",
		"http://[::1]:1/api/v1":         "loopback address",
		"http://169.254.169.254/api/v1": "link-local address",
		"http://[fe80::1]:8090/api/v1":  "link-local address",
		"http://0.0.0.0:8090/api/v1":    "unspecified address",
	}

	ps := newSelectorWithPrivateIPs(t, false)

	for dataHubURL, reason := range tests {
		t.Run(dataHubURL, func(t *testing.T) {
			healthy, err := ps.checkPeerAvailability(context.Background(), dataHubURL)
			require.False(t, healthy)
			require.Error(t, err)
			require.Contains(t, err.Error(), reason)
		})
	}
}

// TestPeerHealthCheck_AllowsPrivateAddresses pins the deliberate tradeoff: RFC1918 targets
// stay allowed, matching the shared block/subtree fetch client. Blocking them here would
// make peers on k8s or private miner networks permanently unselectable even though the
// fetch path would talk to them happily.
func TestPeerHealthCheck_AllowsPrivateAddresses(t *testing.T) {
	ps := newSelectorWithPrivateIPs(t, false)

	for _, ipStr := range []string{"10.0.0.5", "192.168.1.10", "172.16.4.4", "fc00::1"} {
		ip := net.ParseIP(ipStr)
		require.NotNil(t, ip, ipStr)
		require.Empty(t, ps.dataHubDialPolicy(ip), "expected %s to be allowed", ipStr)
	}

	for _, ipStr := range []string{"8.8.8.8", "2606:4700::1111"} {
		ip := net.ParseIP(ipStr)
		require.NotNil(t, ip, ipStr)
		require.Empty(t, ps.dataHubDialPolicy(ip), "expected %s to be allowed", ipStr)
	}
}

// TestDataHubDialPolicy_AllowPrivateIPsKeepsMetadataBlocked: the local-dev bypass relaxes
// loopback, but never the link-local range the cloud metadata endpoint lives in.
func TestDataHubDialPolicy_AllowPrivateIPsKeepsMetadataBlocked(t *testing.T) {
	permissive := newSelectorWithPrivateIPs(t, true)

	require.Empty(t, permissive.dataHubDialPolicy(net.ParseIP("127.0.0.1")))
	require.Equal(t, "link-local address", permissive.dataHubDialPolicy(net.ParseIP("169.254.169.254")))
	require.Equal(t, "link-local address", permissive.dataHubDialPolicy(net.ParseIP("fe80::1")))
	require.Equal(t, "unspecified address", permissive.dataHubDialPolicy(net.ParseIP("0.0.0.0")))
}

// TestPeerHealthCheck_RejectsRedirectToInternal covers the second hop end-to-end: a
// reachable peer that answers the probe with a redirect must not be able to steer it at
// the metadata endpoint or off http/https. AllowPrivateIPs makes the loopback peer itself
// reachable while leaving the redirect targets blocked.
func TestPeerHealthCheck_RejectsRedirectToInternal(t *testing.T) {
	var location atomic.Value

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, location.Load().(string), http.StatusFound)
	}))
	defer server.Close()

	ps := newSelectorWithPrivateIPs(t, true)
	dataHubURL := "http://localhost:" + serverPort(t, server) + "/api/v1"

	tests := map[string]string{
		"http://169.254.169.254/latest/meta-data/": "link-local address",
		"file:///etc/passwd":                       "invalid scheme",
	}

	for target, reason := range tests {
		t.Run(target, func(t *testing.T) {
			location.Store(target)

			healthy, err := ps.checkPeerAvailability(context.Background(), dataHubURL)
			require.False(t, healthy)
			require.Error(t, err)
			require.Contains(t, err.Error(), reason)
		})
	}
}

func TestPeerHealthCheck_NonOKAndMalformedURLs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	ps := newSelectorWithPrivateIPs(t, true)

	// A reachable peer answering non-2xx is unhealthy, and the reason must surface as an
	// error rather than a bare nil the caller would log as "unhealthy: <nil>".
	healthy, err := ps.checkPeerAvailability(context.Background(), "http://localhost:"+serverPort(t, server)+"/api/v1")
	require.False(t, healthy)
	require.Error(t, err)
	require.Contains(t, err.Error(), "500")

	// An empty URL is "no DataHub", not an error.
	healthy, err = ps.checkPeerAvailability(context.Background(), "")
	require.False(t, healthy)
	require.NoError(t, err)

	// Garbage from a peer must not panic.
	healthy, err = ps.checkPeerAvailability(context.Background(), "not-a-url")
	require.False(t, healthy)
	require.Error(t, err)
}

// TestValidateDataHubURL_HostnamePassesStaticCheck pins the documented division of labour:
// the static check accepts peer hostnames (it does no DNS), which is exactly why the
// dial-time guard above has to exist.
func TestValidateDataHubURL_HostnamePassesStaticCheck(t *testing.T) {
	s := &Server{settings: &settings.Settings{}}

	require.NoError(t, s.validateDataHubURL("http://metadata.attacker.example/api/v1"))
	require.Error(t, s.validateDataHubURL("http://169.254.169.254/api/v1"))
}

func serverPort(t *testing.T, server *httptest.Server) string {
	t.Helper()

	parsed, err := url.Parse(server.URL)
	require.NoError(t, err)

	_, port, err := net.SplitHostPort(parsed.Host)
	require.NoError(t, err)

	return port
}
