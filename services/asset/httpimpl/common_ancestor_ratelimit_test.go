package httpimpl

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bsv-blockchain/teranode/services/asset/repository"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestCommonAncestorRoutesAreHeavyRateLimited pins which of the common-ancestor
// routes the heavy limiter covers.
//
// headers_to_common_ancestor is unauthenticated and resolves an ancestor that a
// fork or stale target can still make expensive, so it is charged the heavy
// limit (10 req/s) like the block and subtree routes.
//
// headers_from_common_ancestor is deliberately left out. Peer catch-up calls it
// every iteration, and the heavy bucket is shared with the /blocks and /subtree
// fetches of the same round, so charging it starves those and fails catch-up
// against healthy peers.
func TestCommonAncestorRoutesAreHeavyRateLimited(t *testing.T) {
	const (
		heavyLimit = 1
		requests   = 5
	)

	// A syntactically valid hash: the limiter runs before the handler, so the
	// request does not need to reach a working repository.
	const someHash = "0000000000000000000000000000000000000000000000000000000000000001"

	heavy := []string{
		"/api/v1/headers_to_common_ancestor/" + someHash,
		"/api/v1/headers_to_common_ancestor/" + someHash + "/hex",
		"/api/v1/headers_to_common_ancestor/" + someHash + "/json",
	}

	for _, target := range heavy {
		t.Run(target, func(t *testing.T) {
			require.True(t, sawTooManyRequests(t, target, heavyLimit, requests),
				"an unauthenticated caller must be charged the heavy rate limit on this route")
		})
	}

	// The catch-up route, and a cheap single-header lookup as a control: both keep
	// only the global limit, which also shows the cases above are detecting the
	// heavy limiter rather than the global one.
	notHeavy := []string{
		"/api/v1/headers_from_common_ancestor/" + someHash,
		"/api/v1/headers_from_common_ancestor/" + someHash + "/hex",
		"/api/v1/headers_from_common_ancestor/" + someHash + "/json",
		"/api/v1/header/" + someHash,
	}

	for _, target := range notHeavy {
		t.Run("not throttled: "+target, func(t *testing.T) {
			require.False(t, sawTooManyRequests(t, target, heavyLimit, requests),
				"the heavy limiter must not be attached to this route")
		})
	}
}

// sawTooManyRequests issues n requests from one IP against a server whose heavy
// limit is heavyLimit req/s and whose global limit is far above n, and reports
// whether any response was 429.
func sawTooManyRequests(t *testing.T, target string, heavyLimit, n int) bool {
	t.Helper()

	tSettings := &settings.Settings{
		Asset: settings.AssetSettings{
			APIPrefix:              "/api/v1",
			HTTPRateLimit:          10_000, // effectively off, so only the heavy limiter can trip
			HTTPHeavyRateLimit:     heavyLimit,
			HTTPPeerRateMultiplier: 1,
			HTTPMinerRateLimit:     10_000,
		},
		SecurityLevelHTTP: 0,
	}

	httpServer, err := New(ulogger.TestLogger{}, tSettings, &repository.Repository{}, nil)
	require.NoError(t, err)

	for i := 0; i < n; i++ {
		req := httptest.NewRequest(http.MethodGet, target+"?block_locator_hashes=0000000000000000000000000000000000000000000000000000000000000002", nil)
		req.RemoteAddr = "198.51.100.7:34567"

		rec := httptest.NewRecorder()
		httpServer.e.ServeHTTP(rec, req)

		if rec.Code == http.StatusTooManyRequests {
			return true
		}

		require.NotEqual(t, http.StatusNotFound, rec.Code, fmt.Sprintf("route %s is not registered", target))
	}

	return false
}
