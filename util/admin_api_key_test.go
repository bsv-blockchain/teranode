package util

import (
	"fmt"
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// capturingLogger records Warnf/Errorf messages so tests can assert that the
// cleartext-exposure warning and the placeholder-ignored error actually fired
// (rather than the guard silently doing nothing).
type capturingLogger struct {
	ulogger.TestLogger
	warns  []string
	errors []string
}

func (l *capturingLogger) Warnf(format string, args ...interface{}) {
	l.warns = append(l.warns, fmt.Sprintf(format, args...))
}

func (l *capturingLogger) Errorf(format string, args ...interface{}) {
	l.errors = append(l.errors, fmt.Sprintf(format, args...))
}

func TestValidateAdminAPIKey(t *testing.T) {
	tests := []struct {
		name          string
		apiKey        string
		listenAddress string
		securityLevel int
		wantIgnore    bool
		wantWarn      bool
		wantError     bool
	}{
		{
			name:   "empty key is not ignored (fail-closed random key path)",
			apiKey: "",
		},
		{
			name:          "whitespace-only key is treated as empty",
			apiKey:        "   ",
			listenAddress: "0.0.0.0:9904",
		},
		{
			name:          "committed placeholder testkey is ignored (not hard-fail)",
			apiKey:        "testkey",
			listenAddress: "127.0.0.1:9904",
			wantIgnore:    true,
			wantError:     true,
		},
		{
			name:          "placeholder ignored regardless of case",
			apiKey:        "TestKey",
			listenAddress: "127.0.0.1:9904",
			wantIgnore:    true,
			wantError:     true,
		},
		{
			name:          "placeholder ignored with surrounding whitespace",
			apiKey:        "  changeme  ",
			listenAddress: "127.0.0.1:9904",
			wantIgnore:    true,
			wantError:     true,
		},
		{
			name:          "real key on loopback listener is accepted without warning",
			apiKey:        "a-strong-random-admin-secret-value",
			listenAddress: "127.0.0.1:9904",
		},
		{
			name:          "short real key warns about length even on loopback",
			apiKey:        "shortkey",
			listenAddress: "127.0.0.1:9904",
			wantWarn:      true,
		},
		{
			name:          "real key on non-loopback listener without TLS warns but is kept",
			apiKey:        "a-strong-random-admin-secret-value",
			listenAddress: "0.0.0.0:9904",
			securityLevel: 0,
			wantWarn:      true,
		},
		{
			name:          "real key on non-loopback listener with unverified TLS (level 1) also warns",
			apiKey:        "a-strong-random-admin-secret-value",
			listenAddress: "0.0.0.0:9904",
			securityLevel: 1,
			wantWarn:      true,
		},
		{
			name:          "real key on non-loopback listener with verified TLS (level 2) is accepted",
			apiKey:        "a-strong-random-admin-secret-value",
			listenAddress: "0.0.0.0:9904",
			securityLevel: 2,
		},
		{
			name:          "test key used by e2e suite is not a placeholder",
			apiKey:        "test-ban-list-api-key",
			listenAddress: "0.0.0.0:9904",
			wantWarn:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := &capturingLogger{}
			ignore := ValidateAdminAPIKey(logger, "P2P", tt.apiKey, tt.listenAddress, tt.securityLevel)

			require.Equal(t, tt.wantIgnore, ignore)

			if tt.wantError {
				require.NotEmpty(t, logger.errors, "expected a placeholder-ignored error log")
				require.Contains(t, logger.errors[0], "grpc_admin_api_key")
			} else {
				require.Empty(t, logger.errors, "did not expect an error log, got %v", logger.errors)
			}

			if tt.wantWarn {
				require.NotEmpty(t, logger.warns, "expected a cleartext-exposure warning")
				require.Contains(t, logger.warns[0], "grpc_admin_api_key")
			} else {
				require.Empty(t, logger.warns, "did not expect a warning, got %v", logger.warns)
			}
		})
	}
}

func TestIsPlaceholderAdminAPIKey(t *testing.T) {
	placeholders := []string{"testkey", "TESTKEY", " test ", "changeme", "change_me", "change-me", "password", "secret", "admin", "apikey", "api_key", "default"}
	for _, k := range placeholders {
		require.True(t, IsPlaceholderAdminAPIKey(k), "expected %q to be a placeholder", k)
	}

	// Exact match only: longer strings that merely contain a placeholder are real keys.
	real := []string{"", "a-strong-random-admin-secret-value", "test-ban-list-api-key", "testkey123", "docker-e2e-test-admin-key"}
	for _, k := range real {
		require.False(t, IsPlaceholderAdminAPIKey(k), "expected %q not to be a placeholder", k)
	}
}

func TestIsLoopbackListenAddress(t *testing.T) {
	tests := []struct {
		name string
		addr string
		want bool
	}{
		{"empty", "", false},
		{"port-only binds all interfaces", ":9904", false},
		{"ipv4 unspecified", "0.0.0.0:9904", false},
		{"ipv6 unspecified", "[::]:9904", false},
		{"ipv4 loopback with port", "127.0.0.1:9904", true},
		{"ipv4 loopback subnet with port", "127.0.0.53:9904", true},
		{"ipv6 loopback with port", "[::1]:9904", true},
		{"localhost with port", "localhost:9904", true},
		{"localhost mixed case", "LocalHost:9904", true},
		{"bare localhost no port", "localhost", true},
		{"bare ipv4 loopback no port", "127.0.0.1", true},
		{"bare ipv6 loopback no port", "::1", true},
		{"ipv4-mapped ipv6 loopback", "[::ffff:127.0.0.1]:9904", true},
		{"private ipv4 not loopback", "192.168.1.5:9904", false},
		{"hostname not loopback", "example.com:9904", false},
		{"malformed host with extra colons", "host:1:2", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isLoopbackListenAddress(tt.addr))
		})
	}
}
