package health

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// defaultCheckTimeout bounds a single health check request when the caller supplies no client.
const defaultCheckTimeout = 2 * time.Second

// CheckHTTPServer creates a health check that verifies an HTTP server is listening and accepting requests.
// It attempts to make an HTTP GET request to the specified health endpoint.
//
// The address is expected to be one this node configured itself. To probe an address that
// came from a peer, use CheckHTTPServerWithClient with an SSRF-safe client instead.
//
// Parameters:
//   - address: The HTTP server address (e.g., "http://localhost:8080")
//   - healthPath: The path to the health endpoint (e.g., "/health")
//
// Returns a Check function that can be used with CheckAll
func CheckHTTPServer(address string, healthPath string) func(context.Context, bool) (int, string, error) {
	return CheckHTTPServerWithClient(nil, address, healthPath)
}

// CheckHTTPServerWithClient behaves like CheckHTTPServer but uses the supplied client.
// Callers probing peer-supplied addresses must use this variant with a client whose
// Transport.DialContext enforces an SSRF policy (see util.NewSSRFSafeHTTPClient), since a
// default client happily connects to whatever a hostname resolves to.
//
// A nil client falls back to a plain client with the default 2s timeout.
func CheckHTTPServerWithClient(client *http.Client, address string, healthPath string) func(context.Context, bool) (int, string, error) {
	if client == nil {
		client = &http.Client{Timeout: defaultCheckTimeout}
	}

	return func(ctx context.Context, checkLiveness bool) (int, string, error) {
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
