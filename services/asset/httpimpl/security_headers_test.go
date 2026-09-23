package httpimpl

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bsv-blockchain/teranode/services/asset/repository"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

// TestSecurityHeadersMiddleware_SetsContentSecurityPolicy asserts the policy is emitted and says
// what it says. The name deliberately does NOT claim the policy is strict, and this test must not
// be read as a safety claim: with 'unsafe-inline' in script-src an inline `onerror=` handler still
// fires, and with connect-src https: a same-origin fetch() can still post to an attacker origin.
// Escaping at the dashboard's HTML sink is the fix for markup in peer-controlled fields; this
// header is the second line, and what it does buy is blocking the remote-module amplification step
// (bitcoin-sv/teranode#4844).
func TestSecurityHeadersMiddleware_SetsContentSecurityPolicy(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	ctx := e.NewContext(req, rec)

	handler := securityHeadersMiddleware()(func(c echo.Context) error {
		return c.NoContent(http.StatusOK)
	})
	require.NoError(t, handler(ctx))

	csp := rec.Header().Get("Content-Security-Policy")
	require.Equal(t, contentSecurityPolicy, csp, "the served policy must be the greppable package constant")

	// script-src must not admit a remote origin: that is the directive doing the real work here,
	// because it is what stops import('https://attacker/...') from turning a coinbase-sized
	// payload into an arbitrary module.
	scriptSrc := ""

	for _, directive := range strings.Split(csp, ";") {
		directive = strings.TrimSpace(directive)
		if strings.HasPrefix(directive, "script-src ") {
			scriptSrc = directive
		}
	}

	require.NotEmpty(t, scriptSrc, "script-src must be present")
	require.NotContains(t, scriptSrc, "http", "script-src must not admit a remote origin")
	require.NotContains(t, scriptSrc, "*", "script-src must not admit a wildcard origin")

	for _, directive := range []string{
		"object-src 'none'",
		"base-uri 'self'",
		"frame-ancestors 'none'",
		"form-action 'self'",
	} {
		require.Contains(t, csp, directive)
	}

	// The dashboard opens its live feed over ws:// when it is itself served over plain http, so both
	// websocket schemes are named rather than left to 'self', whose coverage of a same-origin ws://
	// URL is a CSP3 refinement rather than something the directive plainly says. The ws: behaviour,
	// as opposed to the string, is asserted in a real browser by ui/dashboard/tests/csp.spec.ts;
	// wss: is covered by https: under CSP3 scheme matching, so only its string is checked.
	require.Contains(t, csp, "connect-src ")
	require.Contains(t, csp, " ws:", "ws: must be explicit, not left to 'self'")
	require.Contains(t, csp, " wss:")

	// The three pre-existing headers are unchanged.
	require.Equal(t, "nosniff", rec.Header().Get("X-Content-Type-Options"))
	require.Equal(t, "DENY", rec.Header().Get("X-Frame-Options"))
	require.Equal(t, "max-age=31536000; includeSubDomains", rec.Header().Get("Strict-Transport-Security"))

	// Referrer-Policy matches the development copy in hooks.server.ts, so production does not leak
	// a full dashboard URL as the Referer on a cross-origin request (bitcoin-sv/teranode#4844).
	require.Equal(t, "strict-origin-when-cross-origin", rec.Header().Get("Referrer-Policy"))
}

// TestSecurityHeaders_PresentOnEarlyRejection runs the real middleware chain from New and sends a
// CORS preflight, which the CORS middleware answers without calling the next handler. The security
// headers must still be on that response, which holds only if they are registered ahead of the
// middlewares that can end a request early (the ban list and CORS) (bitcoin-sv/teranode#4844).
func TestSecurityHeaders_PresentOnEarlyRejection(t *testing.T) {
	testSettings := &settings.Settings{
		Asset: settings.AssetSettings{
			APIPrefix: "/api/v1",
		},
		Dashboard:         settings.DashboardSettings{Enabled: false},
		SecurityLevelHTTP: 0,
	}

	httpServer, err := New(ulogger.TestLogger{}, testSettings, &repository.Repository{}, nil)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodOptions, "/api/v1/bestblockheader", nil)
	req.Header.Set(echo.HeaderOrigin, "http://example.invalid")
	req.Header.Set(echo.HeaderAccessControlRequestMethod, http.MethodGet)

	rec := httptest.NewRecorder()
	httpServer.e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Code, "fixture precondition: the CORS middleware answered the preflight itself")
	require.Equal(t, contentSecurityPolicy, rec.Header().Get("Content-Security-Policy"))
	require.Equal(t, "nosniff", rec.Header().Get("X-Content-Type-Options"))
}

// TestContentSecurityPolicy_MatchesDashboardCopy is the enforced drift check between the two copies
// of the policy (bitcoin-sv/teranode#4844).
//
// The dashboard carries its own copy in hooks.server.ts for development, where this Go middleware
// does not run. Two hand-maintained copies of a security header drift, and the browser test asserts
// the behaviour of the DASHBOARD's copy — so without this check the browser could be proving things
// about a string production never serves. Comparing them here is what makes that test meaningful.
func TestContentSecurityPolicy_MatchesDashboardCopy(t *testing.T) {
	// Walk up from services/asset/httpimpl to the repository root.
	hooksPath := filepath.Join("..", "..", "..", "ui", "dashboard", "src", "hooks.server.ts")

	source, err := os.ReadFile(hooksPath)
	require.NoError(t, err, "the dashboard copy of the policy must be readable from here")

	// The TypeScript copy is written as adjacent quoted string literals, one directive per line.
	// Reassemble it the way the TypeScript compiler would, so the comparison is against the value
	// the dashboard actually serves rather than against its formatting.
	const marker = "export const CONTENT_SECURITY_POLICY ="

	idx := strings.Index(string(source), marker)
	require.GreaterOrEqual(t, idx, 0, "hooks.server.ts must export CONTENT_SECURITY_POLICY")

	tail := string(source)[idx+len(marker):]
	end := strings.Index(tail, "\n\n")
	require.GreaterOrEqual(t, end, 0, "could not find the end of the policy declaration")

	var assembled strings.Builder

	for _, piece := range strings.Split(tail[:end], "\n") {
		piece = strings.TrimSpace(piece)

		first := strings.Index(piece, `"`)
		last := strings.LastIndex(piece, `"`)

		if first < 0 || last <= first {
			continue
		}

		assembled.WriteString(piece[first+1 : last])
	}

	require.Equal(t, contentSecurityPolicy, assembled.String(),
		"the dashboard copy of the Content-Security-Policy has drifted from the one this service serves")
}
