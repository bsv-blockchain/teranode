package util

import (
	"fmt"
	"strings"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// capturingLogger records Warnf messages so tests can assert that the cleartext
// exposure warning actually fired (rather than the guard silently doing nothing).
type capturingLogger struct {
	ulogger.TestLogger
	warns []string
}

func (l *capturingLogger) Warnf(format string, args ...interface{}) {
	l.warns = append(l.warns, fmt.Sprintf(format, args...))
}

func TestValidateAdminAPIKey(t *testing.T) {
	tests := []struct {
		name          string
		apiKey        string
		listenAddress string
		securityLevel int
		wantErr       bool
		wantWarn      bool
	}{
		{
			name:   "empty key is allowed (fail-closed random key path)",
			apiKey: "",
		},
		{
			name:          "whitespace-only key is treated as empty",
			apiKey:        "   ",
			listenAddress: "0.0.0.0:9904",
		},
		{
			name:          "committed placeholder testkey is rejected",
			apiKey:        "testkey",
			listenAddress: "127.0.0.1:9904",
			wantErr:       true,
		},
		{
			name:          "placeholder rejected regardless of case",
			apiKey:        "TestKey",
			listenAddress: "127.0.0.1:9904",
			wantErr:       true,
		},
		{
			name:          "placeholder rejected with surrounding whitespace",
			apiKey:        "  changeme  ",
			listenAddress: "127.0.0.1:9904",
			wantErr:       true,
		},
		{
			name:          "real key on loopback listener is accepted without warning",
			apiKey:        "a-strong-random-admin-secret-value",
			listenAddress: "127.0.0.1:9904",
		},
		{
			name:          "real key on non-loopback listener without TLS warns but does not fail",
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
			err := ValidateAdminAPIKey(logger, "P2P", tt.apiKey, tt.listenAddress, tt.securityLevel)

			if tt.wantErr {
				require.Error(t, err)
				require.True(t, errors.Is(err, errors.ErrConfiguration), "expected a configuration error, got %v", err)
			} else {
				require.NoError(t, err)
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

// TestValidateAdminAPIKeyMatchesServerEmptyCheck guards the invariant behind
// finding 6: the servers treat GRPCAdminAPIKey == "" as "generate a random key",
// and the setting is trimmed at load, so a whitespace-only value must resolve to
// empty in both places rather than becoming a live secret.
func TestValidateAdminAPIKeyMatchesServerEmptyCheck(t *testing.T) {
	require.Equal(t, "", strings.TrimSpace("   "))
	logger := &capturingLogger{}
	require.NoError(t, ValidateAdminAPIKey(logger, "P2P", "   ", "0.0.0.0:9904", 0))
	require.Empty(t, logger.warns)
}
