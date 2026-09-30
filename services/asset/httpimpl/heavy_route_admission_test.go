package httpimpl

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bsv-blockchain/teranode/services/asset/repository"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// testHash is a syntactically-plausible 32-byte hash used only to satisfy
// path parameters; the rate limiter middleware runs before any handler
// validates or resolves it, so its value is otherwise irrelevant to these
// tests.
const testHash = "0000000000000000000000000000000000000000000000000000000000000001"

// newHeavyAdmissionTestServer builds a real HTTP server via New() with the
// heavy rate limiter set to burst=1 and the global rate limiter disabled, so
// exactly one unauthenticated request per route is allowed before a second
// one is rejected with 429 — isolating whether a route is wired to heavyMW().
func newHeavyAdmissionTestServer(t *testing.T) *HTTP {
	t.Helper()

	logger := ulogger.TestLogger{}
	testSettings := &settings.Settings{
		Asset: settings.AssetSettings{
			APIPrefix:              "/api/v1",
			HTTPRateLimit:          0, // disable the global limiter to isolate the heavy one
			HTTPHeavyRateLimit:     1, // burst of 1 per unverified IP bucket
			HTTPPeerRateMultiplier: 1,
		},
		Dashboard:         settings.DashboardSettings{Enabled: false},
		SecurityLevelHTTP: 0,
	}

	repo := &repository.Repository{}

	httpServer, err := New(logger, testSettings, repo, nil)
	require.NoError(t, err)

	return httpServer
}

// TestHeavyRouteAdmission_ExpensiveRoutesAreRateLimited proves that the
// expensive public routes identified by the Codex security review are now
// admitted to the heavy rate limiter: with burst=1, the first unauthenticated
// request to each route is let through (whatever the handler then does with
// it) and the second is rejected with 429 by the rate limiter itself, before
// the handler runs.
func TestHeavyRouteAdmission_ExpensiveRoutesAreRateLimited(t *testing.T) {
	routes := []struct {
		name   string
		method string
		path   string
	}{
		{"headers_to_common_ancestor binary", http.MethodGet, "/api/v1/headers_to_common_ancestor/" + testHash},
		{"headers_to_common_ancestor hex", http.MethodGet, "/api/v1/headers_to_common_ancestor/" + testHash + "/hex"},
		{"headers_to_common_ancestor json", http.MethodGet, "/api/v1/headers_to_common_ancestor/" + testHash + "/json"},
		{"utxos by tx json", http.MethodGet, "/api/v1/utxos/" + testHash + "/json"},
		{"blockgraphdata", http.MethodGet, "/api/v1/blockgraphdata/all"},
		{"subtree txs json (paginated)", http.MethodGet, "/api/v1/subtree/" + testHash + "/txs/json"},
		{"merkle_proof binary", http.MethodGet, "/api/v1/merkle_proof/" + testHash},
		{"merkle_proof hex", http.MethodGet, "/api/v1/merkle_proof/" + testHash + "/hex"},
		{"merkle_proof json", http.MethodGet, "/api/v1/merkle_proof/" + testHash + "/json"},
	}

	for _, rt := range routes {
		t.Run(rt.name, func(t *testing.T) {
			httpServer := newHeavyAdmissionTestServer(t)

			// First request: allowed by the heavy limiter (burst=1). Whatever
			// the handler does with a fabricated hash and an empty repository
			// is irrelevant here — global middleware.Recover() turns any
			// handler panic into a 500, never a 429.
			rec1 := httptest.NewRecorder()
			req1 := httptest.NewRequest(rt.method, rt.path, nil)
			httpServer.e.ServeHTTP(rec1, req1)
			require.NotEqual(t, http.StatusTooManyRequests, rec1.Code, "first request should not be rate-limited")

			// Second request from the same (unverified) IP bucket: rejected
			// by the heavy limiter before reaching the handler.
			rec2 := httptest.NewRecorder()
			req2 := httptest.NewRequest(rt.method, rt.path, nil)
			httpServer.e.ServeHTTP(rec2, req2)
			require.Equal(t, http.StatusTooManyRequests, rec2.Code, "second request must be rejected by the heavy rate limiter")
		})
	}
}

// TestHeavyRouteAdmission_CatchupHeadersFromCommonAncestorNotThrottled proves
// that headers_from_common_ancestor — the endpoint used by peer-to-peer block
// catchup (services/blockvalidation/catchup_get_block_headers.go) — is
// deliberately NOT admitted to the heavy limiter. Catchup calls this endpoint
// with a plain unauthenticated HTTP GET (no peer-auth signature), so it is
// always charged against the tierUnverified bucket regardless of caller
// identity; admitting it to the heavy limiter (default 10 req/s per IP) would
// throttle legitimate inter-node catchup, which issues iterations back-to-back
// with no client-side pacing.
func TestHeavyRouteAdmission_CatchupHeadersFromCommonAncestorNotThrottled(t *testing.T) {
	httpServer := newHeavyAdmissionTestServer(t)

	path := "/api/v1/headers_from_common_ancestor/" + testHash

	for i := 0; i < 5; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		httpServer.e.ServeHTTP(rec, req)
		require.NotEqual(t, http.StatusTooManyRequests, rec.Code, "request %d must not be rate-limited by the heavy limiter", i+1)
	}
}
