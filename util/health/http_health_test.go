package health_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/util/health"
	"github.com/stretchr/testify/assert"
)

// TestCheckHTTPServerHealthyServer tests the health check against a healthy HTTP server
func TestCheckHTTPServerHealthyServer(t *testing.T) {
	// Create a test HTTP server that returns 200 OK
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK"))
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	// Create the health check
	check := health.CheckHTTPServer(server.URL, "/health")

	// Execute the health check
	ctx := context.Background()
	status, message, err := check(ctx, false)

	// Verify results
	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, message, "HTTP server")
	assert.Contains(t, message, "listening and accepting requests")
	assert.NoError(t, err)
}

// TestCheckHTTPServerUnhealthyServer tests the health check against an unhealthy HTTP server
func TestCheckHTTPServerUnhealthyServer(t *testing.T) {
	// Create a test HTTP server that returns 503 Service Unavailable
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("Service Unavailable"))
	}))
	defer server.Close()

	// Create the health check
	check := health.CheckHTTPServer(server.URL, "/health")

	// Execute the health check
	ctx := context.Background()
	status, message, err := check(ctx, false)

	// Verify results - should fail because server returns 503
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, message, "HTTP server")
	assert.Contains(t, message, "returned status 503")
	assert.NoError(t, err) // No error, just unhealthy status
}

// TestCheckHTTPServerServerNotRunning tests the health check when the server is not running
func TestCheckHTTPServerServerNotRunning(t *testing.T) {
	// Create the health check for a non-existent server
	check := health.CheckHTTPServer("http://localhost:59999", "/health")

	// Execute the health check
	ctx := context.Background()
	status, message, err := check(ctx, false)

	// Verify results - should fail because server is not running
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, message, "HTTP server")
	assert.Contains(t, message, "not accepting connections")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")
}

// TestCheckHTTPServerVariousStatusCodes tests different HTTP status codes
func TestCheckHTTPServerVariousStatusCodes(t *testing.T) {
	tests := []struct {
		name           string
		statusCode     int
		expectedStatus int
		expectSuccess  bool
	}{
		{
			name:           "200 OK",
			statusCode:     http.StatusOK,
			expectedStatus: http.StatusOK,
			expectSuccess:  true,
		},
		{
			name:           "201 Created",
			statusCode:     http.StatusCreated,
			expectedStatus: http.StatusOK,
			expectSuccess:  true,
		},
		{
			name:           "204 No Content",
			statusCode:     http.StatusNoContent,
			expectedStatus: http.StatusOK,
			expectSuccess:  true,
		},
		{
			name:           "299 Success Edge",
			statusCode:     299,
			expectedStatus: http.StatusOK,
			expectSuccess:  true,
		},
		{
			name:           "300 Redirect",
			statusCode:     http.StatusMultipleChoices,
			expectedStatus: http.StatusServiceUnavailable,
			expectSuccess:  false,
		},
		{
			name:           "400 Bad Request",
			statusCode:     http.StatusBadRequest,
			expectedStatus: http.StatusServiceUnavailable,
			expectSuccess:  false,
		},
		{
			name:           "404 Not Found",
			statusCode:     http.StatusNotFound,
			expectedStatus: http.StatusServiceUnavailable,
			expectSuccess:  false,
		},
		{
			name:           "500 Internal Server Error",
			statusCode:     http.StatusInternalServerError,
			expectedStatus: http.StatusServiceUnavailable,
			expectSuccess:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a test HTTP server that returns the specified status code
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
			}))
			defer server.Close()

			// Create the health check
			check := health.CheckHTTPServer(server.URL, "/health")

			// Execute the health check
			ctx := context.Background()
			status, message, err := check(ctx, false)

			// Verify results
			assert.Equal(t, tt.expectedStatus, status)
			assert.NoError(t, err) // HTTP errors don't return as Go errors, just status codes

			if tt.expectSuccess {
				assert.Contains(t, message, "listening and accepting requests")
			} else {
				assert.Contains(t, message, fmt.Sprintf("returned status %d", tt.statusCode))
			}
		})
	}
}

// TestCheckHTTPServerURLFormatting tests various URL formatting scenarios
func TestCheckHTTPServerURLFormatting(t *testing.T) {
	// Create a test HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Respond based on the path
		switch r.URL.Path {
		case "/health", "/api/health", "/v1/health":
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	tests := []struct {
		name       string
		address    string
		healthPath string
		expectOK   bool
	}{
		{
			name:       "normal path",
			address:    server.URL,
			healthPath: "/health",
			expectOK:   true,
		},
		{
			name:       "address with trailing slash, path with leading slash",
			address:    server.URL + "/",
			healthPath: "/health",
			expectOK:   true,
		},
		{
			name:       "nested path",
			address:    server.URL,
			healthPath: "/api/health",
			expectOK:   true,
		},
		{
			name:       "versioned path",
			address:    server.URL,
			healthPath: "/v1/health",
			expectOK:   true,
		},
		{
			name:       "non-existent path",
			address:    server.URL,
			healthPath: "/nonexistent",
			expectOK:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create the health check
			check := health.CheckHTTPServer(tt.address, tt.healthPath)

			// Execute the health check
			ctx := context.Background()
			status, message, err := check(ctx, false)

			// Verify results
			assert.NoError(t, err)

			if tt.expectOK {
				assert.Equal(t, http.StatusOK, status)
				assert.Contains(t, message, "listening and accepting requests")
			} else {
				assert.Equal(t, http.StatusServiceUnavailable, status)
				assert.Contains(t, message, "returned status")
			}
		})
	}
}

// TestCheckHTTPServerTimeout tests that the health check times out appropriately
func TestCheckHTTPServerTimeout(t *testing.T) {
	// Create a test HTTP server that delays responses
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Delay longer than the client timeout (2 seconds)
		time.Sleep(3 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Create the health check
	check := health.CheckHTTPServer(server.URL, "/health")

	// Execute the health check with a context that has a shorter timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	status, message, err := check(ctx, false)
	duration := time.Since(start)

	// Verify results - should timeout
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, message, "not accepting connections")
	assert.Error(t, err)
	// Should the timeout within the context timeout + some buffer
	assert.Less(t, duration, 500*time.Millisecond, "Should timeout quickly")
}

// TestCheckHTTPServerContextCancellation tests context cancellation
func TestCheckHTTPServerContextCancellation(t *testing.T) {
	// Create a test HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Create the health check
	check := health.CheckHTTPServer(server.URL, "/health")

	// Create a context and cancel it immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Execute the health check
	status, message, err := check(ctx, false)

	// Verify results - should fail due to context cancellation
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, message, "not accepting connections")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context canceled")
}

// TestCheckHTTPServerLivenessCheck tests the liveness check parameter
func TestCheckHTTPServerLivenessCheck(t *testing.T) {
	// Create a test HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))
	defer server.Close()

	// Create the health check
	check := health.CheckHTTPServer(server.URL, "/health")

	t.Run("readiness check", func(t *testing.T) {
		ctx := context.Background()
		status, message, err := check(ctx, false) // checkLiveness = false

		assert.Equal(t, http.StatusOK, status)
		assert.Contains(t, message, "listening and accepting requests")
		assert.NoError(t, err)
	})

	t.Run("liveness check", func(t *testing.T) {
		ctx := context.Background()
		status, message, err := check(ctx, true) // checkLiveness = true

		// Should behave the same for liveness check in this implementation
		assert.Equal(t, http.StatusOK, status)
		assert.Contains(t, message, "listening and accepting requests")
		assert.NoError(t, err)
	})
}

// TestCheckHTTPServerRequestCreationError tests error handling when request creation fails
func TestCheckHTTPServerRequestCreationError(t *testing.T) {
	// Create the health check with an invalid URL that will cause NewRequestWithContext to fail
	// Using a URL with invalid characters
	check := health.CheckHTTPServer("http://[::1", "/health") // Invalid IPv6 address format

	// Execute the health check
	ctx := context.Background()
	status, message, err := check(ctx, false)

	// Verify results - should fail with request creation error
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, message, "failed to create request")
	assert.Error(t, err)
}

// TestCheckHTTPServerLargeResponseBody tests that the health check handles large response bodies
func TestCheckHTTPServerLargeResponseBody(t *testing.T) {
	// Create a test HTTP server that returns a large response
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		// Write a large response body (1MB)
		largeBody := make([]byte, 1024*1024)
		for i := range largeBody {
			largeBody[i] = 'A'
		}
		_, _ = w.Write(largeBody)
	}))
	defer server.Close()

	// Create the health check
	check := health.CheckHTTPServer(server.URL, "/health")

	// Execute the health check
	ctx := context.Background()
	status, message, err := check(ctx, false)

	// Verify results - should still work with large response
	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, message, "listening and accepting requests")
	assert.NoError(t, err)
}

// TestCheckHTTPServerHTTPSServer tests the health check with HTTPS server
func TestCheckHTTPServerHTTPSServer(t *testing.T) {
	// Create a test HTTPS server
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))
	defer server.Close()

	// Create the health check
	// Note: httptest.NewTLSServer uses self-signed certificates which would normally fail
	// The HTTP client in CheckHTTPServer doesn't have custom TLS configuration
	check := health.CheckHTTPServer(server.URL, "/health")

	// Execute the health check
	ctx := context.Background()
	status, message, err := check(ctx, false)

	// The check will fail because the default HTTP client doesn't trust the self-signed cert
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, message, "not accepting connections")
	assert.Error(t, err)
	// The error will be about certificate verification
}

// TestCheckHTTPServerRedirectHandling tests how the health check handles redirects
func TestCheckHTTPServerRedirectHandling(t *testing.T) {
	// Create a chain of servers for redirect testing
	finalServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))
	defer finalServer.Close()

	// Create a server that redirects to the final server
	redirectServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, finalServer.URL+"/health", http.StatusFound)
	}))
	defer redirectServer.Close()

	// Create the health check
	check := health.CheckHTTPServer(redirectServer.URL, "/health")

	// Execute the health check
	ctx := context.Background()
	status, message, err := check(ctx, false)

	// The default HTTP client follows redirects, so this should succeed
	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, message, "listening and accepting requests")
	assert.NoError(t, err)
}

// TestCheckHTTPServerMethodNotAllowed tests when server doesn't accept GET requests
func TestCheckHTTPServerMethodNotAllowed(t *testing.T) {
	// Create a test HTTP server that only accepts POST requests
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			_, _ = w.Write([]byte("Method Not Allowed"))
		} else {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK"))
		}
	}))
	defer server.Close()

	// Create the health check (uses GET method)
	check := health.CheckHTTPServer(server.URL, "/health")

	// Execute the health check
	ctx := context.Background()
	status, message, err := check(ctx, false)

	// Verify results - should fail because GET is not allowed
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, message, "returned status 405")
	assert.NoError(t, err)
}

// TestCheckHTTPServerEmptyPath tests with empty health path
func TestCheckHTTPServerEmptyPath(t *testing.T) {
	// Create a test HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" || r.URL.Path == "" {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	// Create the health check with empty path
	check := health.CheckHTTPServer(server.URL, "")

	// Execute the health check
	ctx := context.Background()
	status, message, err := check(ctx, false)

	// Verify results
	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, message, "listening and accepting requests")
	assert.NoError(t, err)
}

// BenchmarkCheckHTTPServer benchmarks the HTTP health check
func BenchmarkCheckHTTPServer(b *testing.B) {
	// Create a test HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))
	defer server.Close()

	// Create the health check
	check := health.CheckHTTPServer(server.URL, "/health")
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = check(ctx, false)
	}
}
