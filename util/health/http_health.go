package health

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
)

// defaultCheckTimeout bounds a single health check request when the caller supplies no client.
const defaultCheckTimeout = 2 * time.Second

// CheckHTTPServer creates a health check that verifies an HTTP server is listening and accepting requests.
// It attempts to make an HTTP GET request to the specified health endpoint.
//
// The address must be one this node configured itself: the client used here has no SSRF
// guard, so it connects to whatever the address resolves to. To probe an address that came
// from a peer, use CheckPeerHTTPServer, which requires a guarded client.
//
// Parameters:
//   - address: The HTTP server address (e.g., "http://localhost:8080")
//   - healthPath: The path to the health endpoint (e.g., "/health")
//
// Returns a Check function that can be used with CheckAll
func CheckHTTPServer(address string, healthPath string) func(context.Context, bool) (int, string, error) {
	return checkHTTPServer(&http.Client{Timeout: defaultCheckTimeout}, address, healthPath)
}

// CheckPeerHTTPServer is CheckHTTPServer for an address that came from a peer. It requires a
// client whose Transport.DialContext enforces an SSRF policy (see util.NewSSRFSafeHTTPClient):
// a peer-supplied hostname can resolve to a loopback or cloud-metadata address, which an
// ordinary client would connect to happily.
//
// A nil client is rejected rather than defaulted, so a peer probe cannot silently lose the
// guard by omitting it - the returned check fails closed instead of dialling unguarded.
func CheckPeerHTTPServer(client *http.Client, address string, healthPath string) func(context.Context, bool) (int, string, error) {
	if client == nil {
		return func(context.Context, bool) (int, string, error) {
			return http.StatusServiceUnavailable, fmt.Sprintf("peer health check for %s misconfigured", address),
				errors.NewInvalidArgumentError("CheckPeerHTTPServer requires an SSRF-safe client, got nil")
		}
	}

	return checkHTTPServer(client, address, healthPath)
}

// checkHTTPServer is the shared implementation; client must be non-nil.
func checkHTTPServer(client *http.Client, address string, healthPath string) func(context.Context, bool) (int, string, error) {
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
