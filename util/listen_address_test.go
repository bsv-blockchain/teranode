package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
			require.Equal(t, tt.want, IsLoopbackListenAddress(tt.addr))
		})
	}
}
