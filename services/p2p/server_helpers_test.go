package p2p

import (
	"net"
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsUnsafeIP(t *testing.T) {
	tests := []struct {
		name     string
		ip       string
		expected string
	}{
		// Safe public IPs
		{"public_ipv4", "8.8.8.8", ""},
		{"public_ipv4_2", "1.1.1.1", ""},
		{"public_ipv6", "2607:f8b0:4004:800::200e", ""},

		// Loopback addresses
		{"loopback_ipv4", "127.0.0.1", "loopback address"},
		{"loopback_ipv4_other", "127.0.0.2", "loopback address"},
		{"loopback_ipv6", "::1", "loopback address"},

		// Private addresses
		{"private_10", "10.0.0.1", "private address"},
		{"private_172", "172.16.0.1", "private address"},
		{"private_192", "192.168.1.1", "private address"},
		{"private_ipv6", "fd00::1", "private address"},

		// Link-local addresses
		{"linklocal_ipv4", "169.254.1.1", "link-local address"},
		{"linklocal_ipv6", "fe80::1", "link-local address"},

		// Unspecified addresses
		{"unspecified_ipv4", "0.0.0.0", "unspecified address"},
		{"unspecified_ipv6", "::", "unspecified address"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip := net.ParseIP(tt.ip)
			require.NotNil(t, ip, "Failed to parse IP: %s", tt.ip)
			result := isUnsafeIP(ip)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsLocalhostHostname(t *testing.T) {
	tests := []struct {
		hostname string
		expected bool
	}{
		{"localhost", true},
		{"sub.localhost", true},
		{"deep.sub.localhost", true},
		{"example.com", false},
		{"localhosted.com", false},
		{"notlocalhost", false},
		{"localhost.example.com", false},
	}

	for _, tt := range tests {
		t.Run(tt.hostname, func(t *testing.T) {
			result := isLocalhostHostname(tt.hostname)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValidateDataHubURL(t *testing.T) {
	server := &Server{
		logger: ulogger.New("test"),
	}

	tests := []struct {
		name        string
		url         string
		expectError bool
		errorMsg    string
	}{
		// Valid URLs
		{"valid_http", "http://example.com/api/v1", false, ""},
		{"valid_https", "https://example.com/api/v1", false, ""},
		{"valid_with_port", "http://example.com:8080/api", false, ""},
		{"valid_public_ip", "http://8.8.8.8/api", false, ""},
		{"valid_public_ipv6", "http://[2607:f8b0:4004:800::200e]/api", false, ""},

		// Empty URL
		{"empty_url", "", true, "empty"},

		// Invalid scheme
		{"ftp_scheme", "ftp://example.com/file", true, "invalid scheme"},
		{"file_scheme", "file:///etc/passwd", true, "invalid scheme"},
		{"no_scheme", "example.com/api", true, "invalid scheme"},

		// No hostname
		{"no_hostname", "http:///path", true, "no hostname"},

		// Loopback addresses
		{"loopback_127", "http://127.0.0.1/api", true, "loopback"},
		{"loopback_127_other", "http://127.0.0.2:8080/api", true, "loopback"},
		{"loopback_ipv6", "http://[::1]/api", true, "loopback"},

		// Private addresses
		{"private_10", "http://10.0.0.1/api", true, "private"},
		{"private_172", "http://172.16.0.1/api", true, "private"},
		{"private_192", "http://192.168.1.1/api", true, "private"},

		// Link-local addresses
		{"linklocal_169", "http://169.254.1.1/api", true, "link-local"},
		{"linklocal_ipv6", "http://[fe80::1]/api", true, "link-local"},

		// Unspecified addresses
		{"unspecified_0000", "http://0.0.0.0/api", true, "unspecified"},

		// Localhost hostname
		{"localhost", "http://localhost/api", true, "localhost"},
		{"localhost_port", "http://localhost:8080/api", true, "localhost"},
		{"sub_localhost", "http://sub.localhost/api", true, "localhost"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := server.validateDataHubURL(tt.url)
			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
