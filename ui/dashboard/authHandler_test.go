package dashboard

import (
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestAuthHandler() *AuthHandler {
	testSettings := &settings.Settings{
		Dashboard: settings.DashboardSettings{
			DevServerPorts: []int{5173, 4173},
			WebSocketPort:  "8090",
			WebSocketPath:  "/connection/websocket",
		},
	}

	return NewAuthHandler(ulogger.TestLogger{}, testSettings)
}

func TestWebSocketConfigHandler(t *testing.T) {
	tests := []struct {
		name         string
		host         string
		scheme       string
		tls          bool
		expectedURL  string
		expectedPort string
	}{
		{
			name:         "Development server Vite port 5173",
			host:         "localhost:5173",
			scheme:       "http",
			tls:          false,
			expectedURL:  "ws://localhost:8090/connection/websocket",
			expectedPort: "8090",
		},
		{
			name:         "Development server Vite port 4173",
			host:         "localhost:4173",
			scheme:       "http",
			tls:          false,
			expectedURL:  "ws://localhost:8090/connection/websocket",
			expectedPort: "8090",
		},
		{
			name:         "Production HTTP",
			host:         "localhost:8090",
			scheme:       "http",
			tls:          false,
			expectedURL:  "ws://localhost:8090/connection/websocket",
			expectedPort: "8090",
		},
		{
			name:         "Production HTTPS",
			host:         "localhost:8090",
			scheme:       "https",
			tls:          true,
			expectedURL:  "wss://localhost:8090/connection/websocket",
			expectedPort: "8090",
		},
		{
			name:         "Docker environment",
			host:         "teranode1:8090",
			scheme:       "http",
			tls:          false,
			expectedURL:  "ws://teranode1:8090/connection/websocket",
			expectedPort: "8090",
		},
		{
			name:         "IPv4 with port",
			host:         "192.168.1.100:8090",
			scheme:       "http",
			tls:          false,
			expectedURL:  "ws://192.168.1.100:8090/connection/websocket",
			expectedPort: "8090",
		},
		{
			name:         "IPv6 with port",
			host:         "[2406:da18:1f7:353a:b079:da22:c7d5:e166]:8090",
			scheme:       "http",
			tls:          false,
			expectedURL:  "ws://2406:da18:1f7:353a:b079:da22:c7d5:e166:8090/connection/websocket",
			expectedPort: "8090",
		},
		{
			name:         "IPv6 with port HTTPS",
			host:         "[2406:da18:1f7:353a:b079:da22:c7d5:e166]:8090",
			scheme:       "https",
			tls:          true,
			expectedURL:  "wss://2406:da18:1f7:353a:b079:da22:c7d5:e166:8090/connection/websocket",
			expectedPort: "8090",
		},
		{
			name:         "Standard HTTP port (no explicit port)",
			host:         "example.com",
			scheme:       "http",
			tls:          false,
			expectedURL:  "ws://example.com/connection/websocket",
			expectedPort: "",
		},
		{
			name:         "Standard HTTPS port (no explicit port)",
			host:         "example.com",
			scheme:       "https",
			tls:          true,
			expectedURL:  "wss://example.com/connection/websocket",
			expectedPort: "",
		},
		{
			name:         "Custom domain with port",
			host:         "dashboard.mycompany.com:9000",
			scheme:       "https",
			tls:          true,
			expectedURL:  "wss://dashboard.mycompany.com:9000/connection/websocket",
			expectedPort: "9000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authHandler := createTestAuthHandler()

			// Create Echo context
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, "/api/config/websocket", nil)
			req.Host = tt.host

			// Set scheme and TLS
			if tt.tls {
				req.TLS = &tls.ConnectionState{}
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			// Mock the scheme
			if tt.scheme == "https" {
				c.Set("scheme", "https")
			}

			// Call the handler
			err := authHandler.WebSocketConfigHandler(c)
			require.NoError(t, err)

			// Check response
			assert.Equal(t, http.StatusOK, rec.Code)

			var response map[string]interface{}
			err = json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)

			websocketURL, exists := response["websocketUrl"]
			require.True(t, exists, "websocketUrl should be present in response")

			assert.Equal(t, tt.expectedURL, websocketURL,
				"WebSocket URL should match expected for host: %s", tt.host)
		})
	}
}

func TestWebSocketConfigHandler_ConfigurablePorts(t *testing.T) {
	// Test with different configuration
	testSettings := &settings.Settings{
		Dashboard: settings.DashboardSettings{
			DevServerPorts: []int{3000, 3001}, // Different dev ports
			WebSocketPort:  "9090",            // Different WebSocket port
			WebSocketPath:  "/ws",             // Different path
		},
	}

	authHandler := NewAuthHandler(ulogger.TestLogger{}, testSettings)

	tests := []struct {
		name        string
		host        string
		expectedURL string
	}{
		{
			name:        "Custom dev port 3000",
			host:        "localhost:3000",
			expectedURL: "ws://localhost:9090/ws",
		},
		{
			name:        "Custom dev port 3001",
			host:        "localhost:3001",
			expectedURL: "ws://localhost:9090/ws",
		},
		{
			name:        "Production uses current port",
			host:        "localhost:8080",
			expectedURL: "ws://localhost:8080/ws",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, "/api/config/websocket", nil)
			req.Host = tt.host
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := authHandler.WebSocketConfigHandler(c)
			require.NoError(t, err)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response map[string]interface{}
			err = json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)

			websocketURL := response["websocketUrl"]
			assert.Equal(t, tt.expectedURL, websocketURL)
		})
	}
}

func TestWebSocketConfigHandler_EdgeCases(t *testing.T) {
	authHandler := createTestAuthHandler()

	tests := []struct {
		name        string
		host        string
		expectedURL string
		description string
	}{
		{
			name:        "IPv6 localhost",
			host:        "[::1]:5173",
			expectedURL: "ws://::1:5173/connection/websocket",
			description: "IPv6 localhost uses current port (not treated as dev server)",
		},
		{
			name:        "Malformed host handled gracefully",
			host:        "invalid::host::format",
			expectedURL: "ws://invalid::host::format/connection/websocket",
			description: "Invalid host should be passed through (net.SplitHostPort will fail gracefully)",
		},
		{
			name:        "Empty host",
			host:        "",
			expectedURL: "ws:///connection/websocket",
			description: "Empty host should be handled",
		},
		{
			name:        "Host with no port",
			host:        "myserver",
			expectedURL: "ws://myserver/connection/websocket",
			description: "Host without port should work",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, "/api/config/websocket", nil)
			req.Host = tt.host
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := authHandler.WebSocketConfigHandler(c)
			require.NoError(t, err, tt.description)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response map[string]interface{}
			err = json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)

			websocketURL := response["websocketUrl"]
			assert.Equal(t, tt.expectedURL, websocketURL, tt.description)
		})
	}
}

func TestWebSocketConfigHandler_ProtocolDetection(t *testing.T) {
	authHandler := createTestAuthHandler()

	tests := []struct {
		name          string
		host          string
		hasTLS        bool
		scheme        string
		expectedProto string
	}{
		{
			name:          "HTTP without TLS",
			host:          "localhost:8090",
			hasTLS:        false,
			scheme:        "http",
			expectedProto: "ws",
		},
		{
			name:          "HTTPS with TLS",
			host:          "localhost:8090",
			hasTLS:        true,
			scheme:        "https",
			expectedProto: "wss",
		},
		{
			name:          "TLS detected from connection state",
			host:          "localhost:8090",
			hasTLS:        true,
			scheme:        "http", // Even if scheme is http, TLS connection should use wss
			expectedProto: "wss",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, "/api/config/websocket", nil)
			req.Host = tt.host

			if tt.hasTLS {
				req.TLS = &tls.ConnectionState{}
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := authHandler.WebSocketConfigHandler(c)
			require.NoError(t, err)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response map[string]interface{}
			err = json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)

			websocketURL := response["websocketUrl"].(string)
			assert.True(t, strings.HasPrefix(websocketURL, tt.expectedProto+"://"),
				"Expected protocol %s, got URL: %s", tt.expectedProto, websocketURL)
		})
	}
}
