package blockpersister

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIsLoopbackListener pins that the cleartext warning follows the address actually bound,
// not the configured string.
func TestIsLoopbackListener(t *testing.T) {
	tests := []struct {
		name     string
		address  string
		loopback bool
	}{
		{name: "IPv4 loopback", address: "127.0.0.1:0", loopback: true},
		{name: "localhost", address: "localhost:0", loopback: true},
		{name: "IPv6 loopback", address: "[::1]:0", loopback: true},
		{name: "unspecified", address: "0.0.0.0:0", loopback: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", tt.address)
			if err != nil {
				t.Skipf("cannot listen on %s here: %v", tt.address, err)
			}

			defer listener.Close()

			require.Equal(t, tt.loopback, isLoopbackListener(listener))
		})
	}
}
