package util

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWebsocketOriginChecker is the single table covering the origin
// predicate shared by /p2p-ws and the Asset service's
// /connection/websocket: same-host and no-Origin requests are always
// allowed, everything else must be explicitly allowlisted. It replaces the
// previous CheckOrigin implementations, which unconditionally returned true
// for any origin, and the two near-identical copies of this check that
// followed them.
func TestWebsocketOriginChecker(t *testing.T) {
	tests := []struct {
		name           string
		allowedOrigins []string
		origin         string
		host           string
		want           bool
	}{
		{
			name: "no origin header is allowed",
			host: "node.example.com",
			want: true,
		},
		{
			name:   "same-host origin is allowed",
			origin: "https://node.example.com",
			host:   "node.example.com",
			want:   true,
		},
		{
			name:   "same-host origin with different scheme is allowed",
			origin: "http://node.example.com",
			host:   "node.example.com",
			want:   true,
		},
		{
			name:   "cross-origin request is denied by default",
			origin: "https://evil.example.com",
			host:   "node.example.com",
			want:   false,
		},
		{
			name:           "cross-origin request in the allowlist is allowed",
			allowedOrigins: []string{"https://dashboard.example.com"},
			origin:         "https://dashboard.example.com",
			host:           "node.example.com",
			want:           true,
		},
		{
			name:           "cross-origin request not in the allowlist is denied",
			allowedOrigins: []string{"https://dashboard.example.com"},
			origin:         "https://evil.example.com",
			host:           "node.example.com",
			want:           false,
		},
		{
			name:           "allowlist match is case-insensitive",
			allowedOrigins: []string{"https://Dashboard.Example.Com"},
			origin:         "https://dashboard.example.com",
			host:           "node.example.com",
			want:           true,
		},
		{
			name:           "dev-server origin is allowed once allowlisted",
			allowedOrigins: []string{"http://localhost:5173"},
			origin:         "http://localhost:5173",
			host:           "localhost:8090",
			want:           true,
		},
		{
			name:   "empty allowlist denies every cross-origin request",
			origin: "http://localhost:5173",
			host:   "localhost:8090",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checkOrigin := WebsocketOriginChecker(tt.allowedOrigins)

			req := httptest.NewRequest(http.MethodGet, "http://"+tt.host+"/ws", nil)
			req.Host = tt.host

			if tt.origin != "" {
				req.Header.Set("Origin", tt.origin)
			}

			require.Equal(t, tt.want, checkOrigin(req))
		})
	}
}

// TestLoopbackListenAddress pins the gate for the dev-server origin escape
// hatch. Getting this wrong in the permissive direction re-opens the
// cross-site websocket hijacking the origin check exists to close, on every
// production node.
func TestLoopbackListenAddress(t *testing.T) {
	loopback := []string{
		"127.0.0.1:8090",
		"127.0.0.1",
		"localhost:9906",
		"LOCALHOST:9906",
		"[::1]:8090",
		"::1",
	}

	for _, addr := range loopback {
		require.True(t, LoopbackListenAddress(addr), "%q binds loopback only", addr)
	}

	networkReachable := []string{
		"",
		":8090",             // wildcard - every interface
		"0.0.0.0:8090",      // wildcard
		"[::]:8090",         // wildcard
		"10.0.0.5:8090",     // RFC1918, reachable from the pod/VPC network
		"192.168.1.10:8090", // RFC1918
		"203.0.113.5:8090",  // public
		"node.example.com:8090",
	}

	for _, addr := range networkReachable {
		require.False(t, LoopbackListenAddress(addr), "%q is reachable off-host", addr)
	}
}

// TestDevServerOrigins verifies both schemes are produced for each port.
func TestDevServerOrigins(t *testing.T) {
	require.Equal(t,
		[]string{
			"http://localhost:5173",
			"https://localhost:5173",
			"http://localhost:4173",
			"https://localhost:4173",
		},
		DevServerOrigins([]int{5173, 4173}),
	)

	require.Empty(t, DevServerOrigins(nil))
}
