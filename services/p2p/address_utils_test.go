package p2p

import (
	"testing"

	p2p "github.com/bsv-blockchain/go-p2p"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsPrivateIPString(t *testing.T) {
	tests := []struct {
		name      string
		ip        string
		isPrivate bool
	}{
		// Private IPv4 ranges
		{"10.x.x.x range", "10.0.0.1", true},
		{"10.x.x.x range upper", "10.255.255.254", true},
		{"172.16.x.x range lower", "172.16.0.1", true},
		{"172.16.x.x range upper", "172.31.255.254", true},
		{"192.168.x.x range", "192.168.1.1", true},
		{"192.168.x.x range upper", "192.168.255.254", true},

		// Special IPv4 ranges
		{"localhost", "127.0.0.1", true},
		{"loopback range", "127.5.5.5", true},
		{"link-local", "169.254.1.1", true},

		// Public IPv4
		{"public Google DNS", "8.8.8.8", false},
		{"public Cloudflare DNS", "1.1.1.1", false},
		{"public IP", "93.184.216.34", false},
		{"172.15.x.x (not in private range)", "172.15.0.1", false},
		{"172.32.x.x (not in private range)", "172.32.0.1", false},

		// With ports
		{"private with port", "192.168.1.1:8080", true},
		{"public with port", "8.8.8.8:53", false},

		// IPv6
		{"IPv6 loopback", "::1", true},
		{"IPv6 link-local", "fe80::1", true},
		{"IPv6 unique local", "fc00::1", true},
		{"IPv6 unique local fd", "fd00::1", true},
		{"IPv6 public", "2001:4860:4860::8888", false},

		// Invalid
		{"invalid IP", "not-an-ip", false},
		{"empty string", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := p2p.IsPrivateIPString(tt.ip)
			assert.Equal(t, tt.isPrivate, result, "IP: %s", tt.ip)
		})
	}
}

func TestP2PGetIPFromMultiaddr(t *testing.T) {
	tests := []struct {
		name        string
		addrStr     string
		expectedIP  string
		expectError bool
	}{
		{"IPv4 with TCP", "/ip4/192.168.1.1/tcp/8333", "192.168.1.1", false},
		{"IPv4 with UDP", "/ip4/10.0.0.1/udp/8333", "10.0.0.1", false},
		{"IPv6 with TCP", "/ip6/2001:db8::1/tcp/8333", "2001:db8::1", false},
		{"IPv4 with p2p", "/ip4/8.8.8.8/tcp/8333/p2p/QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N", "8.8.8.8", false},
		{"DNS address", "/dns4/example.com/tcp/8333", "example.com", false},  // DNS addresses return the hostname
		{"DNS6 address", "/dns6/example.com/tcp/8333", "example.com", false}, // DNS6 also returns the hostname
		{"DNSADDR", "/dnsaddr/example.com", "", true},                        // DNSADDR should error (not supported)
		{"Unix socket", "/unix/tmp/test.sock", "", true},                     // Unix socket should error
		{"IPv6 loopback", "/ip6/::1/tcp/9905", "::1", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, err := ma.NewMultiaddr(tt.addrStr)
			require.NoError(t, err)

			ip, err := p2p.GetIPFromMultiaddr(addr)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedIP, ip)
			}
		})
	}
}

func TestFilterPublicAddresses(t *testing.T) {
	// Create test addresses
	privateAddr1, _ := ma.NewMultiaddr("/ip4/192.168.1.1/tcp/8333")
	privateAddr2, _ := ma.NewMultiaddr("/ip4/10.0.0.1/tcp/8333")
	publicAddr1, _ := ma.NewMultiaddr("/ip4/8.8.8.8/tcp/8333")
	publicAddr2, _ := ma.NewMultiaddr("/ip4/1.1.1.1/tcp/8333")
	localhostAddr, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/8333")

	tests := []struct {
		name          string
		input         []ma.Multiaddr
		expectedCount int
		expectedAddrs []ma.Multiaddr
	}{
		{
			name:          "mixed public and private",
			input:         []ma.Multiaddr{privateAddr1, publicAddr1, privateAddr2, publicAddr2},
			expectedCount: 2,
			expectedAddrs: []ma.Multiaddr{publicAddr1, publicAddr2},
		},
		{
			name:          "only private",
			input:         []ma.Multiaddr{privateAddr1, privateAddr2, localhostAddr},
			expectedCount: 0,
			expectedAddrs: []ma.Multiaddr{},
		},
		{
			name:          "only public",
			input:         []ma.Multiaddr{publicAddr1, publicAddr2},
			expectedCount: 2,
			expectedAddrs: []ma.Multiaddr{publicAddr1, publicAddr2},
		},
		{
			name:          "empty input",
			input:         []ma.Multiaddr{},
			expectedCount: 0,
			expectedAddrs: []ma.Multiaddr{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FilterPublicAddresses(tt.input)
			assert.Len(t, result, tt.expectedCount)
			if tt.expectedCount > 0 {
				assert.ElementsMatch(t, tt.expectedAddrs, result)
			}
		})
	}
}

func TestSelectBestAddress(t *testing.T) {
	// Create test addresses
	privateAddr, _ := ma.NewMultiaddr("/ip4/192.168.1.1/tcp/8333")
	publicAddr, _ := ma.NewMultiaddr("/ip4/8.8.8.8/tcp/8333")
	configuredAddr, _ := ma.NewMultiaddr("/ip4/203.0.113.1/tcp/8333")

	tests := []struct {
		name            string
		addrs           []ma.Multiaddr
		configuredAddrs []string
		expected        ma.Multiaddr
	}{
		{
			name:            "prefer configured address",
			addrs:           []ma.Multiaddr{privateAddr, publicAddr, configuredAddr},
			configuredAddrs: []string{"203.0.113.1"},
			expected:        configuredAddr,
		},
		{
			name:            "prefer public when no configured",
			addrs:           []ma.Multiaddr{privateAddr, publicAddr},
			configuredAddrs: []string{},
			expected:        publicAddr,
		},
		{
			name:            "fall back to private when only private",
			addrs:           []ma.Multiaddr{privateAddr},
			configuredAddrs: []string{},
			expected:        privateAddr,
		},
		{
			name:            "empty addresses returns nil",
			addrs:           []ma.Multiaddr{},
			configuredAddrs: []string{},
			expected:        nil,
		},
		{
			name:            "public address first in list",
			addrs:           []ma.Multiaddr{publicAddr, privateAddr},
			configuredAddrs: []string{},
			expected:        publicAddr,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SelectBestAddress(tt.addrs, tt.configuredAddrs)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsAddressAdvertised(t *testing.T) {
	addr1, _ := ma.NewMultiaddr("/ip4/203.0.113.1/tcp/8333")
	addr2, _ := ma.NewMultiaddr("/ip4/192.168.1.1/tcp/8333")
	addr3, _ := ma.NewMultiaddr("/ip4/8.8.8.8/tcp/8333/p2p/QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N")

	tests := []struct {
		name            string
		addr            ma.Multiaddr
		configuredAddrs []string
		expected        bool
	}{
		{
			name:            "exact IP match",
			addr:            addr1,
			configuredAddrs: []string{"203.0.113.1"},
			expected:        true,
		},
		{
			name:            "no match",
			addr:            addr2,
			configuredAddrs: []string{"203.0.113.1"},
			expected:        false,
		},
		{
			name:            "partial match with p2p",
			addr:            addr3,
			configuredAddrs: []string{"8.8.8.8"},
			expected:        true,
		},
		{
			name:            "empty configured list",
			addr:            addr1,
			configuredAddrs: []string{},
			expected:        false,
		},
		{
			name:            "multiple configured, one matches",
			addr:            addr1,
			configuredAddrs: []string{"192.168.1.1", "203.0.113.1", "10.0.0.1"},
			expected:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsAddressAdvertised(tt.addr, tt.configuredAddrs)
			assert.Equal(t, tt.expected, result)
		})
	}
}
