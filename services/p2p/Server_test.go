package p2p

import (
	"context"
	"net"
	"testing"

	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetIPFromMultiaddr(t *testing.T) {
	s := &Server{}
	ctx := context.Background()

	tests := []struct {
		name     string
		addrStr  string
		expected net.IP
		hasError bool
	}{
		{"IPv4", "/ip4/192.168.1.1/tcp/8080", net.ParseIP("192.168.1.1"), false},
		{"IPv6", "/ip6/2001:db8::1/tcp/8080", net.ParseIP("2001:db8::1"), false},
		{"DNS4", "/dns4/example.com/tcp/8080", nil, false},
		{"DNS6", "/dns6/example.com/tcp/8080", nil, false},
		{"Invalid", "/tcp/8080", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			maddr, err := multiaddr.NewMultiaddr(tt.addrStr)
			require.NoError(t, err)

			ip, err := s.getIPFromMultiaddr(ctx, maddr)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)

				if tt.name == "DNS4" || tt.name == "DNS6" {
					assert.NotNil(t, ip, "Expected resolved IP for DNS address")
				} else {
					assert.Equal(t, tt.expected, ip)
				}
			}
		})
	}
}

func TestResolveDNS(t *testing.T) {
	s := &Server{}
	ctx := context.Background()

	tests := []struct {
		name     string
		addrStr  string
		hasError bool
	}{
		{"Valid DNS", "/dns4/example.com/tcp/8080", false},
		{"Invalid DNS", "/dns4/invalid.example/tcp/8080", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			maddr, err := multiaddr.NewMultiaddr(tt.addrStr)
			require.NoError(t, err)

			ip, err := s.resolveDNS(ctx, maddr)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, ip)
				assert.True(t, ip.To4() != nil || ip.To16() != nil)
			}
		})
	}
}
