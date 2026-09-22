package util

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// useTestDialPolicy turns SSRF protection on and points the shared client's dialer at
// policy, restoring everything when the test ends. Tests can only reach loopback servers,
// which the default policy always refuses, so a test policy stands in for it.
func useTestDialPolicy(t *testing.T, policy SSRFDialPolicy) {
	t.Helper()

	transport, ok := httpClient.Transport.(*http.Transport)
	require.True(t, ok)

	origDial := transport.DialContext
	origProtection := SSRFProtectionEnabled()
	origAllowPrivate := SSRFAllowPrivateNetworks()
	origLookup := ssrfLookupHost
	origClassifier := ssrfIsPrivateNetwork
	origGuard := ssrfRebind

	transport.DialContext = NewSSRFSafeDialContext(policy)
	ssrfRebind = newRebindGuard(ssrfRebindWindow, ssrfRebindMaxHosts)
	SetSSRFProtection(true)

	t.Cleanup(func() {
		transport.CloseIdleConnections()
		transport.DialContext = origDial
		SetSSRFProtection(origProtection)
		SetSSRFAllowPrivateNetworks(origAllowPrivate)
		ssrfLookupHost = origLookup
		ssrfIsPrivateNetwork = origClassifier
		ssrfRebind = origGuard
	})
}

// listenSamePort starts an IPv4 loopback server and an IPv6 loopback server on one port,
// so a single host:port URL can reach either depending on what the hostname resolves to.
func listenSamePort(t *testing.T, first, second http.Handler) (port string) {
	t.Helper()

	for attempt := 0; attempt < 20; attempt++ {
		l4, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)

		_, p, err := net.SplitHostPort(l4.Addr().String())
		require.NoError(t, err)

		l6, err := net.Listen("tcp", net.JoinHostPort("::1", p))
		if err != nil {
			_ = l4.Close()

			if attempt == 0 {
				if _, v6Err := net.Listen("tcp", "[::1]:0"); v6Err != nil {
					t.Skipf("IPv6 loopback unavailable: %v", v6Err)
				}
			}

			continue
		}

		s4 := &httptest.Server{Listener: l4, Config: &http.Server{Handler: first, ReadHeaderTimeout: time.Second}}
		s6 := &httptest.Server{Listener: l6, Config: &http.Server{Handler: second, ReadHeaderTimeout: time.Second}}
		s4.Start()
		s6.Start()
		t.Cleanup(s4.Close)
		t.Cleanup(s6.Close)

		return p
	}

	t.Fatal("could not bind the same port on IPv4 and IPv6 loopback")

	return ""
}

// TestRebinding_POSTAfterPublicGETNeverReachesPrivateService is the regression test issue
// 4843 asks for, following the audit's own proof: 127.0.0.1 stands in for the attacker's
// public address and ::1 for the victim's private one. The attacker serves the subtree
// GET and closes the connection; the same hostname then resolves to the victim, and the
// missing-transaction POST must fail before the victim sees it.
func TestRebinding_POSTAfterPublicGETNeverReachesPrivateService(t *testing.T) {
	for _, allowPrivate := range []bool{false, true} {
		t.Run("allow_private_"+strconv.FormatBool(allowPrivate), func(t *testing.T) {
			var attackerHits, victimHits atomic.Int64

			attacker := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				attackerHits.Add(1)
				w.Header().Set("Connection", "close")
				w.Header().Set("Content-Type", "application/octet-stream")
				_, _ = w.Write(make([]byte, 64))
			})
			victim := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				victimHits.Add(1)
				w.WriteHeader(http.StatusOK)
			})

			port := listenSamePort(t, attacker, victim)

			// Only the private-network rule is under test: loopback addresses are standing in
			// for real ones, so the always-blocked loopback rule is left out of the policy.
			useTestDialPolicy(t, privateNetworkDialPolicy)
			SetSSRFAllowPrivateNetworks(allowPrivate)
			ssrfIsPrivateNetwork = func(ip net.IP) bool { return ip.Equal(net.IPv6loopback) }

			var lookups atomic.Int64
			ssrfLookupHost = func(_ context.Context, host string) ([]string, error) {
				require.Equal(t, "rebind.attacker.example", host)
				if lookups.Add(1) == 1 {
					return []string{"127.0.0.1"}, nil
				}
				return []string{"::1"}, nil
			}

			base := "http://rebind.attacker.example:" + port + "/api/v1"
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			getURL, err := JoinPeerURL(base, "subtree", "aa")
			require.NoError(t, err)

			body, err := DoHTTPRequest(ctx, getURL)
			require.NoError(t, err)
			require.Len(t, body, 64)
			require.EqualValues(t, 1, attackerHits.Load())

			postURL, err := JoinPeerURL(base, "subtree", "aa", "txs")
			require.NoError(t, err)

			reader, err := DoHTTPRequestBodyReader(ctx, postURL, body)
			if reader != nil {
				_ = reader.Close()
			}

			require.Error(t, err)
			require.Contains(t, err.Error(), "private-network address")
			require.GreaterOrEqual(t, lookups.Load(), int64(2), "the POST must have needed a fresh connection")
			require.Zero(t, victimHits.Load(), "the private service must never see the POST")
		})
	}
}

func TestDefaultSSRFDialPolicy_PrivateNetworksFollowSetting(t *testing.T) {
	orig := SSRFAllowPrivateNetworks()
	defer SetSSRFAllowPrivateNetworks(orig)

	private := []string{
		"10.0.0.1", "172.16.0.1", "172.31.255.255", "192.168.1.1",
		"fc00::1", "fd12:3456::1",
		"100.64.0.1", "100.127.255.254",
		"::ffff:10.0.0.1",
	}
	public := []string{"8.8.8.8", "100.63.255.255", "100.128.0.1", "172.32.0.1", "2606:4700::1"}
	alwaysBlocked := []string{"127.0.0.1", "::1", "169.254.169.254", "fe80::1", "0.0.0.0", "::"}

	for _, allowPrivate := range []bool{false, true} {
		SetSSRFAllowPrivateNetworks(allowPrivate)

		for _, s := range private {
			reason := DefaultSSRFDialPolicy(net.ParseIP(s))
			if allowPrivate {
				require.Empty(t, reason, "%s with private networks allowed", s)
			} else {
				require.Contains(t, reason, "private-network address", "%s with private networks refused", s)
			}
		}

		for _, s := range public {
			require.Empty(t, DefaultSSRFDialPolicy(net.ParseIP(s)), s)
		}

		for _, s := range alwaysBlocked {
			require.NotEmpty(t, DefaultSSRFDialPolicy(net.ParseIP(s)), s)
		}
	}
}

func TestRebindGuard(t *testing.T) {
	ips := func(ss ...string) []net.IP {
		out := make([]net.IP, 0, len(ss))
		for _, s := range ss {
			out = append(out, net.ParseIP(s))
		}
		return out
	}

	now := time.Unix(1_700_000_000, 0)

	t.Run("mixed public and private answer is refused", func(t *testing.T) {
		g := newRebindGuard(time.Minute, 10)
		require.Contains(t, g.check("peer.example", ips("8.8.8.8", "10.0.0.5"), now), "both public and private-network")
	})

	t.Run("public then private inside the window is refused", func(t *testing.T) {
		g := newRebindGuard(time.Minute, 10)
		require.Empty(t, g.check("peer.example", ips("8.8.8.8"), now))
		require.Contains(t, g.check("PEER.example.", ips("10.0.0.5"), now.Add(30*time.Second)), "resolved to a public address")
	})

	t.Run("public then private after the window is allowed", func(t *testing.T) {
		g := newRebindGuard(time.Minute, 10)
		require.Empty(t, g.check("peer.example", ips("8.8.8.8"), now))
		require.Empty(t, g.check("peer.example", ips("10.0.0.5"), now.Add(2*time.Minute)))
	})

	t.Run("a public answer refreshes the window", func(t *testing.T) {
		g := newRebindGuard(time.Minute, 10)
		require.Empty(t, g.check("peer.example", ips("8.8.8.8"), now))
		require.Empty(t, g.check("peer.example", ips("1.1.1.1"), now.Add(50*time.Second)))
		require.NotEmpty(t, g.check("peer.example", ips("10.0.0.5"), now.Add(100*time.Second)))
	})

	t.Run("private first is not pinned", func(t *testing.T) {
		g := newRebindGuard(time.Minute, 10)
		require.Empty(t, g.check("asset.svc.cluster.local", ips("10.0.0.5"), now))
		require.Empty(t, g.check("asset.svc.cluster.local", ips("10.0.0.6"), now))
		require.Empty(t, g.check("asset.svc.cluster.local", ips("8.8.8.8"), now))
	})

	t.Run("memory stays bounded and drops expired entries first", func(t *testing.T) {
		g := newRebindGuard(time.Minute, 3)
		require.Empty(t, g.check("old.example", ips("8.8.8.8"), now))
		require.Empty(t, g.check("a.example", ips("8.8.8.8"), now.Add(2*time.Minute)))
		require.Empty(t, g.check("b.example", ips("8.8.8.8"), now.Add(2*time.Minute)))
		require.Empty(t, g.check("c.example", ips("8.8.8.8"), now.Add(2*time.Minute)))
		require.Len(t, g.publicUntil, 3)
		require.NotContains(t, g.publicUntil, "old.example")

		require.Empty(t, g.check("d.example", ips("8.8.8.8"), now.Add(2*time.Minute)))
		require.Len(t, g.publicUntil, 3)
		require.Contains(t, g.publicUntil, "d.example")
	})
}

// TestSSRFDialContext_RebindGuardRunsBeforeDial proves the dialer consults the guard and
// refuses before any connection attempt.
func TestSSRFDialContext_RebindGuardRunsBeforeDial(t *testing.T) {
	useTestDialPolicy(t, privateNetworkDialPolicy)
	SetSSRFAllowPrivateNetworks(true)

	answers := [][]string{{"203.0.113.10"}, {"10.0.0.5"}}
	var calls atomic.Int64
	ssrfLookupHost = func(_ context.Context, _ string) ([]string, error) {
		return answers[calls.Add(1)-1], nil
	}

	dial := NewSSRFSafeDialContext(privateNetworkDialPolicy)

	// The first answer is unreachable TEST-NET space; only the recorded pin matters.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	conn, _ := dial(ctx, "tcp", "flip.attacker.example:80")
	if conn != nil {
		_ = conn.Close()
	}

	_, err := dial(context.Background(), "tcp", "flip.attacker.example:80")
	require.Error(t, err)
	require.Contains(t, err.Error(), "resolved to a public address")
}

func TestSSRFCheckRedirect_StaysOnOrigin(t *testing.T) {
	origProtection := SSRFProtectionEnabled()

	SetSSRFProtection(true)
	defer SetSSRFProtection(origProtection)

	check := ssrfCheckRedirect(DefaultSSRFDialPolicy)

	hop := func(t *testing.T, from, method, to string) error {
		t.Helper()

		fromURL, err := url.Parse(from)
		require.NoError(t, err)
		toURL, err := url.Parse(to)
		require.NoError(t, err)

		return check(&http.Request{URL: toURL, Method: http.MethodGet}, []*http.Request{{URL: fromURL, Method: method}})
	}

	require.NoError(t, hop(t, "http://peer.example/api/v1/block/aa", http.MethodGet, "http://peer.example/api/v1/block/bb"))
	require.NoError(t, hop(t, "http://peer.example/api/v1/block/aa", http.MethodGet, "https://PEER.example./api/v1/block/aa"))
	require.NoError(t, hop(t, "http://peer.example:8090/a", http.MethodGet, "http://peer.example:8090/b"))

	err := hop(t, "http://peer.example/a", http.MethodGet, "http://other.example/a")
	require.ErrorContains(t, err, "leaves the origin")

	err = hop(t, "http://peer.example:8090/a", http.MethodGet, "http://peer.example:9644/v1/debug/bundle")
	require.ErrorContains(t, err, "leaves the origin")

	err = hop(t, "https://peer.example/a", http.MethodGet, "http://peer.example/a")
	require.ErrorContains(t, err, "leaves the origin")

	err = hop(t, "http://peer.example:8090/a", http.MethodGet, "https://peer.example:8443/a")
	require.ErrorContains(t, err, "leaves the origin")

	err = hop(t, "http://peer.example/api/v1/subtree/aa/txs", http.MethodPost, "http://peer.example/api/v1/subtree/aa/txs")
	require.ErrorContains(t, err, "POST")
}

// TestDoHTTPRequest_POSTRedirectNotFollowed drives a live 302 answer to a POST, which Go
// would otherwise follow as a GET.
func TestDoHTTPRequest_POSTRedirectNotFollowed(t *testing.T) {
	var redirectedHits atomic.Int64

	mux := http.NewServeMux()
	mux.HandleFunc("/txs", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/elsewhere", http.StatusFound)
	})
	mux.HandleFunc("/elsewhere", func(w http.ResponseWriter, _ *http.Request) {
		redirectedHits.Add(1)
		w.WriteHeader(http.StatusOK)
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	useTestDialPolicy(t, func(net.IP) string { return "" })

	reader, err := DoHTTPRequestBodyReader(context.Background(), server.URL+"/txs", make([]byte, 32))
	if reader != nil {
		_ = reader.Close()
	}

	require.Error(t, err)
	require.Contains(t, err.Error(), "POST")
	require.Zero(t, redirectedHits.Load())
}

// TestDoLocalServiceHTTPRequestBodyReader_ReachesLoopback covers the legacy service's
// fetch from this node's own asset service, whose default address is localhost. The peer
// guard refuses loopback, so that fetch must not go through it.
func TestDoLocalServiceHTTPRequestBodyReader_ReachesLoopback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("block"))
	}))
	defer server.Close()

	origProtection := SSRFProtectionEnabled()
	SetSSRFProtection(true)
	defer SetSSRFProtection(origProtection)

	parsed, err := url.Parse(server.URL)
	require.NoError(t, err)

	localURL := "http://localhost:" + parsed.Port() + "/api/v1/block_legacy/aa?wire=1"

	_, err = DoHTTPRequestBodyReader(context.Background(), localURL)
	require.ErrorContains(t, err, "loopback address", "the peer client must still refuse it")

	reader, err := DoLocalServiceHTTPRequestBodyReader(context.Background(), localURL)
	require.NoError(t, err)

	defer func() { _ = reader.Close() }()

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "block", string(body))
}
