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
	"github.com/bsv-blockchain/teranode/util"
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

// allowLoopbackProbes disables the dial guard for one test. No production configuration
// permits probing loopback, so tests that need to reach an httptest server (which only ever
// listens on 127.0.0.1) have to use the same global escape hatch the test daemons use.
func allowLoopbackProbes(t *testing.T) {
	t.Helper()

	util.SetSSRFProtection(false)
	t.Cleanup(func() { util.SetSSRFProtection(true) })
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

// TestPeerHealthCheck_RejectsInternalAddresses covers every class the policy blocks,
// including RFC1918 (named in the issue) and IPv6 forms, going through the real client so
// the guard does not depend on the static pre-check having run first.
func TestPeerHealthCheck_RejectsInternalAddresses(t *testing.T) {
	tests := map[string]string{
		"http://127.0.0.1:1/api/v1":       "loopback address",
		"http://[::1]:1/api/v1":           "loopback address",
		"http://169.254.169.254/api/v1":   "link-local address",
		"http://[fe80::1]:8090/api/v1":    "link-local address",
		"http://0.0.0.0:8090/api/v1":      "unspecified address",
		"http://10.0.0.5:8090/api/v1":     "private address",
		"http://192.168.1.10:8090/api/v1": "private address",
		"http://172.16.4.4:8090/api/v1":   "private address",
		"http://[fc00::1]:8090/api/v1":    "private address",
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

// TestDataHubDialPolicy pins the policy itself: it must enforce the same address classes as
// validateDataHubURL's isUnsafeIP, so a hostname cannot reach what a literal cannot.
func TestDataHubDialPolicy(t *testing.T) {
	strict := newSelectorWithPrivateIPs(t, false)

	for ipStr, reason := range map[string]string{
		"127.0.0.1":       "loopback address",
		"::1":             "loopback address",
		"169.254.169.254": "link-local address",
		"fe80::1":         "link-local address",
		"0.0.0.0":         "unspecified address",
		"10.0.0.5":        "private address",
		"192.168.1.10":    "private address",
		"172.16.4.4":      "private address",
		"fc00::1":         "private address",
	} {
		ip := net.ParseIP(ipStr)
		require.NotNil(t, ip, ipStr)
		require.Equal(t, reason, strict.dataHubDialPolicy(ip), "expected %s to be rejected", ipStr)
		require.Equal(t, isUnsafeIP(ip), strict.dataHubDialPolicy(ip),
			"the dial policy must agree with validateDataHubURL for %s", ipStr)
	}

	for _, ipStr := range []string{"8.8.8.8", "1.2.3.4", "2606:4700::1111"} {
		ip := net.ParseIP(ipStr)
		require.NotNil(t, ip, ipStr)
		require.Empty(t, strict.dataHubDialPolicy(ip), "expected %s to be allowed", ipStr)
	}
}

// TestDataHubDialPolicy_AllowPrivateIPs: the setting relaxes the private ranges (which the
// fetch path also allows), but never loopback, link-local or unspecified - so no
// configuration lets a peer steer the probe at a metadata endpoint or at localhost.
func TestDataHubDialPolicy_AllowPrivateIPs(t *testing.T) {
	permissive := newSelectorWithPrivateIPs(t, true)

	for _, ipStr := range []string{"10.0.0.5", "192.168.1.10", "172.16.4.4", "fc00::1"} {
		ip := net.ParseIP(ipStr)
		require.NotNil(t, ip, ipStr)
		require.Empty(t, permissive.dataHubDialPolicy(ip), "AllowPrivateIPs must allow %s", ipStr)
	}

	require.Equal(t, "loopback address", permissive.dataHubDialPolicy(net.ParseIP("127.0.0.1")))
	require.Equal(t, "link-local address", permissive.dataHubDialPolicy(net.ParseIP("169.254.169.254")))
	require.Equal(t, "link-local address", permissive.dataHubDialPolicy(net.ParseIP("fe80::1")))
	require.Equal(t, "unspecified address", permissive.dataHubDialPolicy(net.ParseIP("0.0.0.0")))
}

// TestPeerHealthCheck_ProbesReachablePeer checks the guarded client is otherwise a working
// HTTP client: right URL joining, and only a 2xx counts as available. The guard is disabled
// here because no configuration allows probing the loopback address httptest listens on.
func TestPeerHealthCheck_ProbesReachablePeer(t *testing.T) {
	allowLoopbackProbes(t)

	var path atomic.Value

	status := atomic.Int64{}
	status.Store(http.StatusOK)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path.Store(r.URL.Path)
		w.WriteHeader(int(status.Load()))
	}))
	defer server.Close()

	ps := newSelectorWithPrivateIPs(t, false)
	dataHubURL := "http://localhost:" + serverPort(t, server) + "/api/v1"

	healthy, err := ps.checkPeerAvailability(context.Background(), dataHubURL)
	require.NoError(t, err)
	require.True(t, healthy)
	require.Equal(t, "/api/v1/bestblockheader", path.Load())

	// A reachable peer answering non-2xx is unhealthy, and the reason must surface as an
	// error rather than a bare nil the caller would log as "unhealthy: <nil>".
	status.Store(http.StatusInternalServerError)

	healthy, err = ps.checkPeerAvailability(context.Background(), dataHubURL)
	require.False(t, healthy)
	require.Error(t, err)
	require.Contains(t, err.Error(), "500")
}

func TestPeerHealthCheck_EmptyAndMalformedURLs(t *testing.T) {
	ps := newSelectorWithPrivateIPs(t, false)

	// An empty URL is "no DataHub", not an error.
	healthy, err := ps.checkPeerAvailability(context.Background(), "")
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
