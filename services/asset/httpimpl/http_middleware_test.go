package httpimpl

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bsv-blockchain/teranode/services/asset/repository"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

const testHashHex = "0000000000000000000000000000000000000000000000000000000000000001"

// corsProbe serves a single GET through the supplied CORS config and returns
// the recorder so the response headers can be asserted.
func corsProbe(t *testing.T, cfg middleware.CORSConfig, origin string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	e.Use(middleware.CORSWithConfig(cfg))
	e.GET("/probe", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	req := httptest.NewRequest(http.MethodGet, "/probe", nil)
	req.Header.Set(echo.HeaderOrigin, origin)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	return rec
}

// TestAssetCORSConfig_EmptyAllowlistNeverAllowsCredentials — with no
// asset_corsAllowedOrigins the legacy reflect-any behaviour is preserved, but
// credentialed cross-origin responses must be refused. Reflecting an arbitrary
// origin *and* allowing credentials is what let a hostile same-site origin ride
// an operator's ambient cookie into the admin routes.
func TestAssetCORSConfig_EmptyAllowlistNeverAllowsCredentials(t *testing.T) {
	rec := corsProbe(t, assetCORSConfig(nil), "https://evil.example.com")

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "https://evil.example.com", rec.Header().Get(echo.HeaderAccessControlAllowOrigin),
		"empty allowlist keeps the legacy reflect-any behaviour")
	require.Empty(t, rec.Header().Get(echo.HeaderAccessControlAllowCredentials),
		"a reflected arbitrary origin must never be granted credentials")
}

// TestAssetCORSConfig_ExplicitAllowlistIsStrict — a configured allowlist is
// matched exactly, and only then are credentials permitted.
func TestAssetCORSConfig_ExplicitAllowlistIsStrict(t *testing.T) {
	origins, err := parseCORSAllowedOrigins(" https://ops.example.com | https://admin.example.com ")
	require.NoError(t, err)
	require.Equal(t, []string{"https://ops.example.com", "https://admin.example.com"}, origins)

	cfg := assetCORSConfig(origins)

	t.Run("allowed origin gets credentials", func(t *testing.T) {
		rec := corsProbe(t, cfg, "https://ops.example.com")
		require.Equal(t, "https://ops.example.com", rec.Header().Get(echo.HeaderAccessControlAllowOrigin))
		require.Equal(t, "true", rec.Header().Get(echo.HeaderAccessControlAllowCredentials))
	})

	t.Run("unlisted origin is not reflected", func(t *testing.T) {
		rec := corsProbe(t, cfg, "https://evil.example.com")
		require.Empty(t, rec.Header().Get(echo.HeaderAccessControlAllowOrigin),
			"an unlisted origin must not be reflected")
		require.Empty(t, rec.Header().Get(echo.HeaderAccessControlAllowCredentials))
	})
}

// TestAssetCORSConfig_AllowlistIsExactMatchOnly — Echo's built-in AllowOrigins
// matching honours "*"/"?" globs, subdomain matching and the literal "null",
// and grants Access-Control-Allow-Credentials to anything it matches. A
// configured allowlist entry must only ever match itself, byte for byte.
func TestAssetCORSConfig_AllowlistIsExactMatchOnly(t *testing.T) {
	cfg := assetCORSConfig([]string{"https://ops.example.com"})

	t.Run("exact origin is allowed", func(t *testing.T) {
		rec := corsProbe(t, cfg, "https://ops.example.com")
		require.Equal(t, "https://ops.example.com", rec.Header().Get(echo.HeaderAccessControlAllowOrigin))
		require.Equal(t, "true", rec.Header().Get(echo.HeaderAccessControlAllowCredentials))
	})

	t.Run("subdomain of an allowed origin is not allowed", func(t *testing.T) {
		rec := corsProbe(t, cfg, "https://evil.ops.example.com")
		require.Empty(t, rec.Header().Get(echo.HeaderAccessControlAllowOrigin))
		require.Empty(t, rec.Header().Get(echo.HeaderAccessControlAllowCredentials))
	})

	t.Run("glob-like configured entry only matches itself literally", func(t *testing.T) {
		globCfg := assetCORSConfig([]string{"https://*.example.com"})

		rec := corsProbe(t, globCfg, "https://ops.example.com")
		require.Empty(t, rec.Header().Get(echo.HeaderAccessControlAllowOrigin),
			"a glob-like allowlist entry must not expand into a pattern match")
		require.Empty(t, rec.Header().Get(echo.HeaderAccessControlAllowCredentials))
	})

	t.Run("null origin is not allowed", func(t *testing.T) {
		rec := corsProbe(t, cfg, "null")
		require.Empty(t, rec.Header().Get(echo.HeaderAccessControlAllowOrigin))
		require.Empty(t, rec.Header().Get(echo.HeaderAccessControlAllowCredentials))
	})
}

// TestParseCORSAllowedOrigins_NormalisesCaseAndTrailingSlash — an operator
// typo in scheme/host case, or a trailing slash, must still match the
// canonical origin a browser sends.
func TestParseCORSAllowedOrigins_NormalisesCaseAndTrailingSlash(t *testing.T) {
	origins, err := parseCORSAllowedOrigins("HTTPS://Ops.Example.COM/")
	require.NoError(t, err)
	require.Equal(t, []string{"https://ops.example.com"}, origins)

	cfg := assetCORSConfig(origins)
	rec := corsProbe(t, cfg, "https://ops.example.com")
	require.Equal(t, "https://ops.example.com", rec.Header().Get(echo.HeaderAccessControlAllowOrigin),
		"a normalised configured entry must match the canonical origin a browser sends")
}

// TestParseCORSAllowedOrigins_StripsDefaultPorts — browsers omit the scheme's
// default port from the Origin header, so a configured https://host:443 or
// http://host:80 would otherwise never match. A non-default port is part of the
// origin and must be kept.
func TestParseCORSAllowedOrigins_StripsDefaultPorts(t *testing.T) {
	origins, err := parseCORSAllowedOrigins("https://ops.example.com:443|http://dev.example.com:80|https://alt.example.com:8443")
	require.NoError(t, err)
	require.Equal(t, []string{"https://ops.example.com", "http://dev.example.com", "https://alt.example.com:8443"}, origins)
}

// TestParseCORSAllowedOrigins_RejectsUserinfo — an origin never carries
// userinfo. Silently dropping it would hide an operator mistake (and a
// credential sitting in config), so it must fail startup instead.
func TestParseCORSAllowedOrigins_RejectsUserinfo(t *testing.T) {
	_, err := parseCORSAllowedOrigins("https://user@ops.example.com")
	require.Error(t, err)
}

// TestParseCORSAllowedOrigins_RejectsWildcardHost — matching is exact, so a
// scheme-qualified wildcard such as https://*.example.com could only ever match
// itself literally, which no browser sends. It must fail startup rather than be
// accepted as an entry that silently matches nothing.
func TestParseCORSAllowedOrigins_RejectsWildcardHost(t *testing.T) {
	for _, entry := range []string{"https://*.example.com", "https://ops.*.com", "*"} {
		_, err := parseCORSAllowedOrigins(entry)
		require.Error(t, err, entry)
	}
}

// TestParseCORSAllowedOrigins_RejectsNullOrigin — "null" can never be a
// legitimate operator origin (it's the Origin header a sandboxed/opaque
// request sends), so it must fail loudly at startup rather than being
// silently accepted or dropped.
func TestParseCORSAllowedOrigins_RejectsNullOrigin(t *testing.T) {
	_, err := parseCORSAllowedOrigins("null")
	require.Error(t, err)
}

// TestParseCORSAllowedOrigins_RejectsEntryWithPath — an origin is
// scheme+host+port only; a configured entry with a path is an operator
// mistake that should fail startup with a clear error, not be silently
// truncated or accepted as a no-op path segment.
func TestParseCORSAllowedOrigins_RejectsEntryWithPath(t *testing.T) {
	_, err := parseCORSAllowedOrigins("https://ops.example.com/dashboard")
	require.Error(t, err)
}

// newTestServer builds a real HTTP server over a minimal repository so route
// and middleware wiring can be exercised end to end.
func newTestServer(t *testing.T, tSettings *settings.Settings) *HTTP {
	t.Helper()

	repo, err := repository.NewRepository(ulogger.TestLogger{}, tSettings, nil, nil, &blockchain.Mock{}, nil, memory.New(), nil, nil, nil)
	require.NoError(t, err)

	srv, err := New(ulogger.TestLogger{}, tSettings, repo, nil)
	require.NoError(t, err)

	return srv
}

func baseTestSettings() *settings.Settings {
	return &settings.Settings{
		Asset: settings.AssetSettings{
			APIPrefix: "/api/v1",
		},
		Dashboard:         settings.DashboardSettings{Enabled: false},
		SecurityLevelHTTP: 0,
	}
}

// TestNew_DashboardCORSUsesTheSameAllowlist — there are two CORS configs on the
// Asset listener (the default one and the one registered in the dashboard
// branch). Narrowing only one leaves the defect intact, so both must honour
// asset_corsAllowedOrigins.
func TestNew_DashboardCORSUsesTheSameAllowlist(t *testing.T) {
	tSettings := baseTestSettings()
	tSettings.Asset.CORSAllowedOrigins = "https://ops.example.com"
	tSettings.Dashboard.Enabled = true
	tSettings.RPC = settings.RPCSettings{RPCUser: "bitcoin", RPCPass: "bitcoin"}

	srv := newTestServer(t, tSettings)

	req := httptest.NewRequest(http.MethodGet, "/alive", nil)
	req.Header.Set(echo.HeaderOrigin, "https://evil.example.com")
	rec := httptest.NewRecorder()
	srv.e.ServeHTTP(rec, req)

	require.Empty(t, rec.Header().Get(echo.HeaderAccessControlAllowOrigin),
		"the dashboard CORS config must not reflect an unlisted origin either")
}

// fireRequests serves n requests for method/path against srv and returns how
// many got a 429.
func fireRequests(srv *HTTP, method, path string, n int) (tooMany int) {
	for i := 0; i < n; i++ {
		req := httptest.NewRequest(method, path, nil)
		rec := httptest.NewRecorder()
		srv.e.ServeHTTP(rec, req)
		if rec.Code == http.StatusTooManyRequests {
			tooMany++
		}
	}
	return tooMany
}

// TestNew_HeavyRateLimiterSplitsBurstByCatchupRoute — only the five routes
// peer catchup depends on get the raised burst. Every other heavy route
// (including POST /utxos, up to 1024-way Aerospike fan-out) must keep
// burst == rate, so a bulk caller there can't spend the catchup floor.
func TestNew_HeavyRateLimiterSplitsBurstByCatchupRoute(t *testing.T) {
	tSettings := baseTestSettings()
	tSettings.Asset.HTTPHeavyRateLimit = 2
	tSettings.SubtreeValidation.GetMissingTransactions = 6

	catchupRoutes := []struct {
		name   string
		method string
		path   string
	}{
		{"GET /subtree/:hash", http.MethodGet, fmt.Sprintf("/api/v1/subtree/%s", testHashHex)},
		{"GET /subtree_data/:hash", http.MethodGet, fmt.Sprintf("/api/v1/subtree_data/%s", testHashHex)},
		{"POST /subtree/:hash/txs", http.MethodPost, fmt.Sprintf("/api/v1/subtree/%s/txs", testHashHex)},
		{"GET /blocks/:hash", http.MethodGet, fmt.Sprintf("/api/v1/blocks/%s", testHashHex)},
		{"GET /block/:hash", http.MethodGet, fmt.Sprintf("/api/v1/block/%s", testHashHex)},
	}

	for _, rt := range catchupRoutes {
		t.Run(rt.name+" gets the raised burst", func(t *testing.T) {
			srv := newTestServer(t, tSettings)

			require.Zero(t, fireRequests(srv, rt.method, rt.path, 6),
				"burst should admit the full catchup fan-out of 6 before any 429")
			require.Equal(t, 1, fireRequests(srv, rt.method, rt.path, 1),
				"the request past the floor must be rejected")
		})
	}

	t.Run("a non-catchup heavy route keeps burst == rate", func(t *testing.T) {
		srv := newTestServer(t, tSettings)
		path := fmt.Sprintf("/api/v1/subtree/%s/hex", testHashHex)

		require.Zero(t, fireRequests(srv, http.MethodGet, path, 2),
			"burst should equal the sustained rate of 2")
		require.Equal(t, 1, fireRequests(srv, http.MethodGet, path, 1),
			"the third request must be rejected: this route must not get the catchup floor")
	})

	t.Run("POST /utxos keeps burst == rate, not the catchup floor", func(t *testing.T) {
		srv := newTestServer(t, tSettings)

		require.Zero(t, fireRequests(srv, http.MethodPost, "/api/v1/utxos", 2),
			"burst should equal the sustained rate of 2")
		require.Equal(t, 1, fireRequests(srv, http.MethodPost, "/api/v1/utxos", 1),
			"the third request must be rejected: this route must not get the catchup floor")
	})
}

// TestNew_HeavyRateLimiterUsesDistinctMetricLabelForCatchupRoutes — the two
// heavy limiters are independent per-IP buckets, so a 429 on a catchup route
// must be counted under its own "heavy_catchup" label, distinct from "heavy"
// for the non-catchup heavy routes; otherwise the two independent budgets are
// indistinguishable on teranode_asset_http_rate_limited_total.
func TestNew_HeavyRateLimiterUsesDistinctMetricLabelForCatchupRoutes(t *testing.T) {
	tSettings := baseTestSettings()
	tSettings.Asset.HTTPHeavyRateLimit = 1
	tSettings.SubtreeValidation.GetMissingTransactions = 1

	srv := newTestServer(t, tSettings)

	catchupPath := fmt.Sprintf("/api/v1/subtree/%s", testHashHex)
	otherHeavyPath := fmt.Sprintf("/api/v1/subtree/%s/hex", testHashHex)

	beforeCatchup := testutil.ToFloat64(prometheusAssetHTTPRateLimited.WithLabelValues("heavy_catchup"))
	beforeHeavy := testutil.ToFloat64(prometheusAssetHTTPRateLimited.WithLabelValues("heavy"))

	require.Equal(t, 1, fireRequests(srv, http.MethodGet, catchupPath, 2),
		"burst 1: the second request on the catchup route must be rejected")

	require.Equal(t, beforeCatchup+1, testutil.ToFloat64(prometheusAssetHTTPRateLimited.WithLabelValues("heavy_catchup")),
		"the catchup-route rejection must be counted under heavy_catchup")
	require.Equal(t, beforeHeavy, testutil.ToFloat64(prometheusAssetHTTPRateLimited.WithLabelValues("heavy")),
		"a catchup-route rejection must not be counted under heavy")

	require.Equal(t, 1, fireRequests(srv, http.MethodGet, otherHeavyPath, 2),
		"burst 1: the second request on the non-catchup heavy route must be rejected")

	require.Equal(t, beforeHeavy+1, testutil.ToFloat64(prometheusAssetHTTPRateLimited.WithLabelValues("heavy")),
		"the non-catchup heavy-route rejection must be counted under heavy")
}

// TestNew_TrustedProxyCIDRsReplaceEchoDefaults — echo.TrustIPRange is additive:
// link-local and private networks stay trusted unless explicitly disabled. An
// operator who configures an allowlist means that list plus loopback and
// nothing else, otherwise any RFC1918 client can forge X-Forwarded-For.
func TestNew_TrustedProxyCIDRsReplaceEchoDefaults(t *testing.T) {
	tSettings := baseTestSettings()
	tSettings.Asset.TrustedProxyCIDRs = "203.0.113.0/24"

	srv := newTestServer(t, tSettings)
	require.NotNil(t, srv.e.IPExtractor)

	t.Run("untrusted private client cannot forge XFF", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/alive", nil)
		req.RemoteAddr = "10.1.2.3:4444"
		req.Header.Set(echo.HeaderXForwardedFor, "8.8.8.8")

		require.Equal(t, "10.1.2.3", srv.e.IPExtractor(req),
			"an RFC1918 peer outside the configured allowlist must not be trusted as a proxy")
	})

	t.Run("configured proxy is still trusted", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/alive", nil)
		req.RemoteAddr = "203.0.113.7:4444"
		req.Header.Set(echo.HeaderXForwardedFor, "8.8.8.8")

		require.Equal(t, "8.8.8.8", srv.e.IPExtractor(req),
			"a peer inside the configured allowlist must still be trusted")
	})

	t.Run("empty setting keeps the documented Echo defaults", func(t *testing.T) {
		defaults := newTestServer(t, baseTestSettings())

		req := httptest.NewRequest(http.MethodGet, "/alive", nil)
		req.RemoteAddr = "10.1.2.3:4444"
		req.Header.Set(echo.HeaderXForwardedFor, "8.8.8.8")

		require.Equal(t, "8.8.8.8", defaults.e.IPExtractor(req),
			"with no allowlist the documented default (trust loopback and private networks) is unchanged")
	})
}

// TestNormalizeHTTPMethod — the Prometheus method label must come from a fixed
// domain; anything else buckets to "other".
func TestNormalizeHTTPMethod(t *testing.T) {
	for _, method := range []string{
		http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut,
		http.MethodPatch, http.MethodDelete, http.MethodOptions,
		http.MethodConnect, http.MethodTrace,
	} {
		require.Equal(t, method, normalizeHTTPMethod(method))
	}

	require.Equal(t, "other", normalizeHTTPMethod("FROBNICATE"))
	require.Equal(t, "other", normalizeHTTPMethod("get"))
	require.Equal(t, "other", normalizeHTTPMethod(""))
}

// TestAccessLogMiddleware_BoundsMethodLabelCardinality — arbitrary method
// tokens are accepted by net/http before routing and histogram children are
// never evicted, so a raw method label is permanent unbounded memory growth.
func TestAccessLogMiddleware_BoundsMethodLabelCardinality(t *testing.T) {
	initPrometheusMetrics()

	e := echo.New()
	e.Use(accessLogMiddleware(ulogger.TestLogger{}))
	e.Any("/cardinality", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	prometheusAssetHTTPRequestDuration.DeletePartialMatch(prometheus.Labels{"path": "/cardinality"})

	for _, method := range []string{"FROBNICATE", "WIBBLE", "ZORK"} {
		req := httptest.NewRequest(method, "/cardinality", nil)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
	}

	require.Equal(t, 0,
		prometheusAssetHTTPRequestDuration.DeletePartialMatch(prometheus.Labels{"path": "/cardinality", "method": "FROBNICATE"}),
		"a raw attacker-supplied method token must never become a label value")
	require.Equal(t, 1,
		prometheusAssetHTTPRequestDuration.DeletePartialMatch(prometheus.Labels{"path": "/cardinality"}),
		"three distinct unknown method tokens must collapse into a single 'other' series")
}

// TestNew_RateLimitedRequestsAreNotMetered — the access-log middleware was
// registered outside the global limiter, so 429-rejected requests still minted
// histogram children (the cheapest way to drive the cardinality growth above).
// The limiter has its own counter; rejected requests must not also land in the
// request histograms.
func TestNew_RateLimitedRequestsAreNotMetered(t *testing.T) {
	initPrometheusMetrics()

	tSettings := baseTestSettings()
	tSettings.Asset.HTTPRateLimit = 1

	srv := newTestServer(t, tSettings)

	prometheusAssetHTTPRequestDuration.DeletePartialMatch(prometheus.Labels{"path": "/alive"})

	var lastCode int

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/alive", nil)
		req.RemoteAddr = "198.51.100.9:5555"
		rec := httptest.NewRecorder()
		srv.e.ServeHTTP(rec, req)
		lastCode = rec.Code
	}

	require.Equal(t, http.StatusTooManyRequests, lastCode, "second request must be rate limited")

	require.Equal(t, 0,
		prometheusAssetHTTPRequestDuration.DeletePartialMatch(prometheus.Labels{"path": "/alive", "status": "429"}),
		"a rejected request must not create a request-histogram series; it is counted by http_rate_limited_total")
	require.Equal(t, 1,
		prometheusAssetHTTPRequestDuration.DeletePartialMatch(prometheus.Labels{"path": "/alive"}),
		"only the served request may create a histogram series")
}

// TestPostAuthEnforcement — asset_enforcePostAuth actually applies the
// dashboard POST credential check (Echo's Group.Use only covers routes
// registered afterwards, so the old call site was dead), while the peer-catchup
// POST stays reachable unconditionally.
func TestPostAuthEnforcement(t *testing.T) {
	const (
		catchupPath = "/api/v1/subtree/" + testHashHex + "/txs"
		bulkPath    = "/api/v1/utxos/json"
	)

	mkSettings := func(dashboard, enforce bool) *settings.Settings {
		tSettings := baseTestSettings()
		tSettings.Dashboard.Enabled = dashboard
		tSettings.Asset.EnforcePostAuth = enforce
		tSettings.RPC = settings.RPCSettings{RPCUser: "bitcoin", RPCPass: "bitcoin"}

		return tSettings
	}

	post := func(t *testing.T, srv *HTTP, path string) int {
		t.Helper()

		req := httptest.NewRequest(http.MethodPost, path, nil)
		rec := httptest.NewRecorder()
		srv.e.ServeHTTP(rec, req)

		return rec.Code
	}

	t.Run("flag on: unauthenticated bulk POST is rejected", func(t *testing.T) {
		srv := newTestServer(t, mkSettings(false, true))
		require.Equal(t, http.StatusUnauthorized, post(t, srv, bulkPath))
	})

	t.Run("flag on: peer catchup POST stays reachable", func(t *testing.T) {
		srv := newTestServer(t, mkSettings(false, true))
		require.NotEqual(t, http.StatusUnauthorized, post(t, srv, catchupPath),
			"catchup requests carry no credentials; 401 here wedges every peer catching up from this node")
	})

	t.Run("flag on with dashboard enabled: peer catchup POST stays reachable", func(t *testing.T) {
		srv := newTestServer(t, mkSettings(true, true))
		require.Equal(t, http.StatusUnauthorized, post(t, srv, bulkPath))
		require.NotEqual(t, http.StatusUnauthorized, post(t, srv, catchupPath),
			"enabling the dashboard must never break peer catchup")
	})

	t.Run("dashboard enabled, flag off: no POST is rejected", func(t *testing.T) {
		srv := newTestServer(t, mkSettings(true, false))
		require.NotEqual(t, http.StatusUnauthorized, post(t, srv, bulkPath),
			"enforcement is opt-in; enabling the dashboard alone must not start rejecting POSTs")
		require.NotEqual(t, http.StatusUnauthorized, post(t, srv, catchupPath))
	})
}

// TestSign_DeclaresItsScope — X-Signature covers the resource identifier only,
// never the status or the serialized body. The header has to say so; clients
// must not read it as body integrity.
func TestSign_DeclaresItsScope(t *testing.T) {
	srv, _, c, rec := GetMockHTTP(t, nil)

	require.NoError(t, srv.Sign(c.Response(), []byte("resource-id")))
	require.NotEmpty(t, rec.Header().Get("X-Signature"))
	require.Equal(t, signatureScopeResourceIdentifier, rec.Header().Get("X-Signature-Scope"),
		"the signature scope must be stated on the wire so X-Signature is not mistaken for body integrity")
}

// TestNew_PreflightAdvertisesTheCSRFHeader — Echo's CORS middleware answers a
// preflight with c.NoContent(204) and never calls next, so only the first
// registered CORS middleware is ever reached for OPTIONS. A single root-level
// config must therefore carry every header the listener accepts, including the
// dashboard's X-CSRF-Token, whether or not the dashboard is enabled.
func TestNew_PreflightAdvertisesTheCSRFHeader(t *testing.T) {
	for _, dashboardEnabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("dashboard=%v", dashboardEnabled), func(t *testing.T) {
			tSettings := baseTestSettings()
			tSettings.Asset.CORSAllowedOrigins = "https://ops.example.com"
			tSettings.Dashboard.Enabled = dashboardEnabled
			tSettings.RPC = settings.RPCSettings{RPCUser: "bitcoin", RPCPass: "bitcoin"}

			srv := newTestServer(t, tSettings)

			req := httptest.NewRequest(http.MethodOptions, "/alive", nil)
			req.Header.Set(echo.HeaderOrigin, "https://ops.example.com")
			req.Header.Set(echo.HeaderAccessControlRequestMethod, http.MethodPost)
			req.Header.Set(echo.HeaderAccessControlRequestHeaders, "X-CSRF-Token")
			rec := httptest.NewRecorder()
			srv.e.ServeHTTP(rec, req)

			require.Equal(t, "https://ops.example.com", rec.Header().Get(echo.HeaderAccessControlAllowOrigin))
			require.Contains(t, rec.Header().Get(echo.HeaderAccessControlAllowHeaders), "X-CSRF-Token",
				"a credentialed cross-origin dashboard request preflighting X-CSRF-Token must be allowed")
		})
	}
}

// TestNew_TrustedProxyCIDRsKeepLoopbackTrusted — a loopback peer is same-host
// by definition and cannot be an external attacker spoofing XFF. Dropping that
// trust collapses RealIP to 127.0.0.1 for every request in the very common
// sidecar / same-pod ingress topology, which keys the whole world into one
// rate-limit bucket and makes a ban of one client a ban of everything.
func TestNew_TrustedProxyCIDRsKeepLoopbackTrusted(t *testing.T) {
	tSettings := baseTestSettings()
	tSettings.Asset.TrustedProxyCIDRs = "203.0.113.0/24"

	srv := newTestServer(t, tSettings)
	require.NotNil(t, srv.e.IPExtractor)

	t.Run("sidecar proxy on loopback", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/alive", nil)
		req.RemoteAddr = "127.0.0.1:4444"
		req.Header.Set(echo.HeaderXForwardedFor, "8.8.8.8")

		require.Equal(t, "8.8.8.8", srv.e.IPExtractor(req),
			"a same-host proxy must stay trusted so RealIP does not collapse to 127.0.0.1")
	})

	t.Run("loopback behind a configured edge proxy", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/alive", nil)
		req.RemoteAddr = "127.0.0.1:4444"
		req.Header.Set(echo.HeaderXForwardedFor, "8.8.8.8, 203.0.113.7")

		require.Equal(t, "8.8.8.8", srv.e.IPExtractor(req),
			"the chain must resolve through both the loopback hop and the allowlisted edge")
	})
}
