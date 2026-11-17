package p2p

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
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

// TestPeerHealthCheck_RejectsInternalAddresses covers every class the probe refuses, in
// both IPv4 and IPv6 form, driven through the real client so the guard does not depend on
// the static pre-check having run first.
func TestPeerHealthCheck_RejectsInternalAddresses(t *testing.T) {
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

// TestPeerProbePrivateAddressesFollowSetting is the availability guard-rail. The probe only
// decides whether to fetch from a peer, so it must refuse exactly what the block/subtree fetch
// path refuses: a probe stricter than the fetch would drop peers catchup could have used, and
// one looser would be pointless. Both share util.DefaultSSRFDialPolicy, where private-network
// addresses follow p2p_allow_private_ips (applied process-wide by the daemon) since issue 4843.
//
// With the setting on, the dial is expected to fail as a network error (nothing is
// listening) rather than an SSRF rejection. With it off, the guard refuses before dialing.
func TestPeerProbePrivateAddressesFollowSetting(t *testing.T) {
	origAllowPrivate := util.SSRFAllowPrivateNetworks()
	t.Cleanup(func() { util.SetSSRFAllowPrivateNetworks(origAllowPrivate) })

	for _, allowPrivateIPs := range []bool{false, true} {
		util.SetSSRFAllowPrivateNetworks(allowPrivateIPs)

		ps := newSelectorWithPrivateIPs(t, allowPrivateIPs)

		for _, hostPort := range []string{"10.255.255.1:1", "192.168.255.254:1", "[fc00::1]:1", "100.64.0.1:1"} {
			t.Run(fmt.Sprintf("%s_allow_private_%v", hostPort, allowPrivateIPs), func(t *testing.T) {
				// Bounded so an unroutable private address cannot stall the test.
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer cancel()

				_, err := ps.checkPeerAvailability(ctx, "http://"+hostPort+"/api/v1")
				require.Error(t, err)

				if allowPrivateIPs {
					require.NotContains(t, err.Error(), "SSRF dial check", "private address must not be refused by the guard")
				} else {
					require.Contains(t, err.Error(), "private-network address", "private address must be refused by the guard")
				}
			})
		}

		// The shared policy is the single source of truth for both paths.
		for _, ipStr := range []string{"10.0.0.5", "192.168.1.10", "172.16.4.4", "fc00::1"} {
			ip := net.ParseIP(ipStr)
			require.NotNil(t, ip, ipStr)
			require.Equal(t, allowPrivateIPs, util.DefaultSSRFDialPolicy(ip) == "", "the fetch path policy for %s", ipStr)
		}
	}
}

// TestPeerProbeIgnoresAllowPrivateIPs pins the deliberate asymmetry with validateDataHubURL,
// which short-circuits every IP check when the flag is set (server_helpers.go). The
// connection-time guard does not: loopback, link-local and unspecified stay refused with the
// flag on, so no configuration lets a peer steer a probe at localhost or at the cloud
// metadata endpoint. Announcement acceptance and probe reachability are separate questions.
func TestPeerProbeIgnoresAllowPrivateIPs(t *testing.T) {
	s := &Server{settings: &settings.Settings{P2P: settings.P2PSettings{AllowPrivateIPs: true}}}
	require.NoError(t, s.validateDataHubURL("http://localhost:8090/api/v1"),
		"the flag makes the static check accept a loopback URL")

	ps := newSelectorWithPrivateIPs(t, true)

	healthy, err := ps.checkPeerAvailability(context.Background(), "http://localhost:8090/api/v1")
	require.False(t, healthy, "the probe still refuses loopback with the flag set")
	require.Error(t, err)
	require.Contains(t, err.Error(), "loopback address")
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

// TestPeerProbe_QueryBaseSendsNothing is the probe's half of the audit finding. A base URL
// ending in "?x=" used to turn the appended health path into query data, so the probe would
// GET whatever path the peer named. The catch-all responder counts every request, so the
// assertion is that nothing was sent at all, not merely that the path differed.
func TestPeerProbe_QueryBaseSendsNothing(t *testing.T) {
	allowLoopbackProbes(t)

	var hits atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ps := newSelectorWithPrivateIPs(t, false)

	for _, base := range []string{
		"http://localhost:" + serverPort(t, server) + "/v1/debug/bundle?x=",
		"http://localhost:" + serverPort(t, server) + "/v1/debug/bundle?",
		"http://localhost:" + serverPort(t, server) + "/api/v1#frag",
		"http://user:pass@localhost:" + serverPort(t, server) + "/api/v1",
	} {
		t.Run(base, func(t *testing.T) {
			healthy, err := ps.checkPeerAvailability(context.Background(), base)
			require.False(t, healthy)
			require.Error(t, err)
			require.Zero(t, hits.Load(), "the probe must send nothing when the base URL is unusable")
		})
	}
}

// TestPeerProbeRefusalIsInvalidArgument pins what the selector logs at warning level. A peer
// refused before any packet leaves - unusable base URL, or an address this node's own policy
// bars - is a local configuration cause that silently drops the peer from selection, so the
// probe loop raises it above debug. An unreachable peer must not match, or every dead peer in
// the registry would warn on every round.
func TestPeerProbeRefusalIsInvalidArgument(t *testing.T) {
	origAllowPrivate := util.SSRFAllowPrivateNetworks()
	t.Cleanup(func() { util.SetSSRFAllowPrivateNetworks(origAllowPrivate) })

	util.SetSSRFAllowPrivateNetworks(false)

	ps := newSelectorWithPrivateIPs(t, false)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_, err := ps.checkPeerAvailability(ctx, "http://10.255.255.1:1/api/v1")
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrInvalidArgument), "policy refusal must classify as invalid argument, got %v", err)

	_, err = ps.checkPeerAvailability(ctx, "http://localhost:1/v1/debug/bundle?x=")
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrInvalidArgument), "unusable base URL must classify as invalid argument, got %v", err)

	// A reachable-but-dead public address is an ordinary network failure and stays at debug.
	allowLoopbackProbes(t)

	_, err = ps.checkPeerAvailability(ctx, "http://localhost:1/api/v1")
	require.Error(t, err)
	require.False(t, errors.Is(err, errors.ErrInvalidArgument), "an unreachable peer must not warn, got %v", err)
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
