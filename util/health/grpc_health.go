package health

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/util"
	"google.golang.org/grpc"
)

// CheckGRPCServerWithSettings creates a health check that verifies a gRPC server is listening
// using the proper connection settings from the service configuration.
//
// Parameters:
//   - address: The gRPC server address to check
//   - tSettings: The settings containing security configuration
//   - healthCheckFunc: A function that performs a service-specific health check
//
// Returns a Check function that can be used with CheckAll
func CheckGRPCServerWithSettings(address string, tSettings *settings.Settings, healthCheckFunc func(ctx context.Context, conn *grpc.ClientConn) error) func(context.Context, bool) (int, string, error) {
	return func(ctx context.Context, checkLiveness bool) (int, string, error) {
		// Skip this check if we're already inside a gRPC call to prevent circular dependency
		// This happens when HealthGRPC is called and it tries to check its own server
		if ctx.Value("skip-grpc-self-check") != nil {
			return http.StatusOK, fmt.Sprintf("gRPC server at %s (skipped self-check)", address), nil
		}

		// Create a context with timeout for the health check
		healthCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		// Use the proper connection method with settings
		conn, err := util.GetGRPCClient(healthCtx, address, &util.ConnectionOptions{
			MaxRetries:   0, // Don't retry for health checks
			RetryBackoff: 0,
		}, tSettings)
		if err != nil {
			return http.StatusServiceUnavailable, fmt.Sprintf("gRPC server at %s not accepting connections", address), err
		}
		defer conn.Close()

		// Perform the custom health check
		if err := healthCheckFunc(healthCtx, conn); err != nil {
			return http.StatusServiceUnavailable, fmt.Sprintf("gRPC health check failed for %s", address), err
		}

		return http.StatusOK, fmt.Sprintf("gRPC server at %s is listening and accepting requests", address), nil
	}
}
