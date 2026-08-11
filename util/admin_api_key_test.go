package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateAdminAPIKey(t *testing.T) {
	logger := mockLogger()

	tests := []struct {
		name          string
		apiKey        string
		listenAddress string
		securityLevel int
		wantErr       bool
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
			name:          "real key on loopback listener is accepted",
			apiKey:        "a-strong-random-admin-secret-value",
			listenAddress: "127.0.0.1:9904",
		},
		{
			name:          "real key on non-loopback listener without TLS warns but does not fail",
			apiKey:        "a-strong-random-admin-secret-value",
			listenAddress: "0.0.0.0:9904",
			securityLevel: 0,
		},
		{
			name:          "real key on non-loopback listener with TLS is accepted",
			apiKey:        "a-strong-random-admin-secret-value",
			listenAddress: "0.0.0.0:9904",
			securityLevel: 1,
		},
		{
			name:          "test key used by e2e suite is not a placeholder",
			apiKey:        "test-ban-list-api-key",
			listenAddress: "0.0.0.0:9904",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateAdminAPIKey(logger, "P2P", tt.apiKey, tt.listenAddress, tt.securityLevel)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestIsPlaceholderAdminAPIKey(t *testing.T) {
	placeholders := []string{"testkey", "TESTKEY", " test ", "changeme", "change_me", "change-me", "password", "secret", "admin", "apikey", "api_key", "default"}
	for _, k := range placeholders {
		require.True(t, IsPlaceholderAdminAPIKey(k), "expected %q to be a placeholder", k)
	}

	real := []string{"", "a-strong-random-admin-secret-value", "test-ban-list-api-key", "testkey123"}
	for _, k := range real {
		require.False(t, IsPlaceholderAdminAPIKey(k), "expected %q not to be a placeholder", k)
	}
}

func TestIsLoopbackListenAddress(t *testing.T) {
	tests := []struct {
		addr string
		want bool
	}{
		{"", false},
		{":9904", false},
		{"0.0.0.0:9904", false},
		{"[::]:9904", false},
		{"127.0.0.1:9904", true},
		{"127.0.0.53:9904", true},
		{"[::1]:9904", true},
		{"localhost:9904", true},
		{"LocalHost:9904", true},
		{"localhost", true},
		{"192.168.1.5:9904", false},
		{"example.com:9904", false},
	}

	for _, tt := range tests {
		t.Run(tt.addr, func(t *testing.T) {
			require.Equal(t, tt.want, isLoopbackListenAddress(tt.addr))
		})
	}
}
