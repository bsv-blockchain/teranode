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

// TestCommonAncestorRoutesAreHeavyRateLimited pins the mitigation for
// bitcoin-sv/teranode#4894. A locator that matches nothing makes the blockchain
// service walk back from the target in 1,000-header pages until it hits a match
// or the start of the chain, so one unauthenticated request costs work
// proportional to chain height. The global limiter (1024 req/s by default) does
// not price that; the heavy limiter (10 req/s) is what the equally expensive
// block and subtree routes already carry.
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
		"/api/v1/headers_from_common_ancestor/" + someHash,
		"/api/v1/headers_from_common_ancestor/" + someHash + "/hex",
		"/api/v1/headers_from_common_ancestor/" + someHash + "/json",
	}

	for _, target := range heavy {
		t.Run(target, func(t *testing.T) {
			require.True(t, sawTooManyRequests(t, target, heavyLimit, requests),
				"an unauthenticated caller must be charged the heavy rate limit on this route")
		})
	}

	// Control: a cheap single-header lookup keeps only the global limit, so the
	// test above is detecting the heavy limiter rather than the global one.
	t.Run("cheap header route keeps only the global limit", func(t *testing.T) {
		require.False(t, sawTooManyRequests(t, "/api/v1/header/"+someHash, heavyLimit, requests),
			"the heavy limiter must not be attached to the single-header route")
	})
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
