package p2p

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestSetupHTTPServer_IPExtractorIgnoresUntrustedXFF proves that c.RealIP()
// (used to key the /p2p-ws connection cap and the HTTP rate limiter) is not
// attacker-controlled by default: a request from an untrusted remote address
// carrying a forged X-Forwarded-For header must not have that header trusted.
func TestSetupHTTPServer_IPExtractorIgnoresUntrustedXFF(t *testing.T) {
	s := &Server{
		logger: ulogger.TestLogger{},
		gCtx:   context.Background(),
		settings: &settings.Settings{
			P2P: settings.P2PSettings{HTTPListenAddress: "127.0.0.1:0"},
		},
	}

	e, err := s.setupHTTPServer()
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.RemoteAddr = "203.0.113.5:41000" // public IP, not loopback/RFC1918 -> untrusted proxy
	req.Header.Set("X-Forwarded-For", "9.9.9.9")

	c := e.NewContext(req, httptest.NewRecorder())
	require.Equal(t, "203.0.113.5", c.RealIP(),
		"an untrusted remote address's forged X-Forwarded-For must not be trusted, "+
			"otherwise the per-IP websocket cap and the HTTP rate limiter are bypassable")
}

// TestSetupHTTPServer_IPExtractorTrustsConfiguredProxyCIDR verifies that once
// an operator explicitly trusts a proxy CIDR, X-Forwarded-For from that range
// is honoured.
func TestSetupHTTPServer_IPExtractorTrustsConfiguredProxyCIDR(t *testing.T) {
	s := &Server{
		logger: ulogger.TestLogger{},
		gCtx:   context.Background(),
		settings: &settings.Settings{
			P2P: settings.P2PSettings{
				HTTPListenAddress: "127.0.0.1:0",
				TrustedProxyCIDRs: "203.0.113.0/24",
			},
		},
	}

	e, err := s.setupHTTPServer()
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.RemoteAddr = "203.0.113.5:41000"
	req.Header.Set("X-Forwarded-For", "9.9.9.9")

	c := e.NewContext(req, httptest.NewRecorder())
	require.Equal(t, "9.9.9.9", c.RealIP())
}

// TestSetupHTTPServer_ConfiguredCIDRsNarrowTheTrustBoundary is the realistic
// Kubernetes case: external clients are SNATed to a node/pod address inside
// an RFC1918 range, so "the direct peer is private" is not evidence that it
// is a proxy. Once an operator names their ingress range explicitly, a
// private-but-unlisted direct peer must not have its X-Forwarded-For
// honoured - otherwise it mints a fresh per-IP cap / rate-limit bucket per
// request. echo's TrustIPRange only appends to a default that trusts every
// private, loopback and link-local peer, so this only passes because
// TrustedProxyIPExtractor disables those defaults first.
func TestSetupHTTPServer_ConfiguredCIDRsNarrowTheTrustBoundary(t *testing.T) {
	s := &Server{
		logger: ulogger.TestLogger{},
		gCtx:   context.Background(),
		settings: &settings.Settings{
			P2P: settings.P2PSettings{
				HTTPListenAddress: "127.0.0.1:0",
				// The operator's real ingress range - deliberately NOT the
				// range the forging client connects from.
				TrustedProxyCIDRs: "10.42.0.0/16",
			},
		},
	}

	e, err := s.setupHTTPServer()
	require.NoError(t, err)

	for _, remoteAddr := range []string{
		"10.0.0.7:41000",      // RFC1918, outside the configured range
		"192.168.1.20:41000",  // RFC1918, outside the configured range
		"127.0.0.1:41000",     // loopback
		"169.254.10.10:41000", // link-local
		"[fd00::1]:41000",     // IPv6 unique-local
	} {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		req.RemoteAddr = remoteAddr
		req.Header.Set("X-Forwarded-For", "9.9.9.9")

		c := e.NewContext(req, httptest.NewRecorder())

		host, _, splitErr := net.SplitHostPort(remoteAddr)
		require.NoError(t, splitErr)

		require.Equal(t, host, c.RealIP(),
			"%s is not in the configured trusted-proxy range, so its forged "+
				"X-Forwarded-For must be ignored", remoteAddr)
	}

	// The configured range itself is still trusted.
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.RemoteAddr = "10.42.0.9:41000"
	req.Header.Set("X-Forwarded-For", "9.9.9.9")

	c := e.NewContext(req, httptest.NewRecorder())
	require.Equal(t, "9.9.9.9", c.RealIP())
}

// TestSetupHTTPServer_InvalidTrustedProxyCIDRsFailsLoudly verifies that a
// typo'd p2p_trustedProxyCIDRs fails startup instead of silently widening
// the trust boundary.
func TestSetupHTTPServer_InvalidTrustedProxyCIDRsFailsLoudly(t *testing.T) {
	s := &Server{
		logger: ulogger.TestLogger{},
		gCtx:   context.Background(),
		settings: &settings.Settings{
			P2P: settings.P2PSettings{
				HTTPListenAddress: "127.0.0.1:0",
				TrustedProxyCIDRs: "not-a-cidr",
			},
		},
	}

	_, err := s.setupHTTPServer()
	require.Error(t, err)
}
