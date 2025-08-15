package health

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// CheckHTTPServer creates a health check that verifies an HTTP server is listening and accepting requests.
// It attempts to make an HTTP GET request to the specified health endpoint.
//
// Parameters:
//   - address: The HTTP server address (e.g., "http://localhost:8080")
//   - healthPath: The path to the health endpoint (e.g., "/health")
//
// Returns a Check function that can be used with CheckAll
func CheckHTTPServer(address string, healthPath string) func(context.Context, bool) (int, string, error) {
	return func(ctx context.Context, checkLiveness bool) (int, string, error) {
		// Create an HTTP client with a timeout
		client := &http.Client{
			Timeout: 2 * time.Second,
		}

		// Construct the full URL
		url := fmt.Sprintf("%s%s", address, healthPath)
		if len(address) > 0 && len(healthPath) > 0 && address[len(address)-1] == '/' && healthPath[0] == '/' {
			url = fmt.Sprintf("%s%s", address, healthPath[1:])
		}

		// Create a request with context
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return http.StatusServiceUnavailable, fmt.Sprintf("HTTP server at %s failed to create request", address), err
		}

		// Make the request
		resp, err := client.Do(req)
		if err != nil {
			return http.StatusServiceUnavailable, fmt.Sprintf("HTTP server at %s not accepting connections", address), err
		}
		defer resp.Body.Close()

		// Check the response status
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return http.StatusOK, fmt.Sprintf("HTTP server at %s is listening and accepting requests", address), nil
		}

		return http.StatusServiceUnavailable, fmt.Sprintf("HTTP server at %s returned status %d", address, resp.StatusCode), nil
	}
}
