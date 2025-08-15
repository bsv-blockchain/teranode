package health_test

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/util/health"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// mockHealthServer implements the gRPC health check service for testing
type mockHealthServer struct {
	grpc_health_v1.UnimplementedHealthServer
	healthy bool
}

func (m *mockHealthServer) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	if m.healthy {
		return &grpc_health_v1.HealthCheckResponse{
			Status: grpc_health_v1.HealthCheckResponse_SERVING,
		}, nil
	}
	return &grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
	}, nil
}

// TestCheckGRPCServerWithSettings_HealthyServer tests the health check against a healthy gRPC server
func TestCheckGRPCServerWithSettings_HealthyServer(t *testing.T) {
	// Start a test gRPC server
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer listener.Close()

	server := grpc.NewServer()
	healthServer := &mockHealthServer{healthy: true}
	grpc_health_v1.RegisterHealthServer(server, healthServer)

	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Create test settings
	tSettings := settings.NewSettings()

	// Create the health check function
	healthCheckFunc := func(ctx context.Context, conn *grpc.ClientConn) error {
		client := grpc_health_v1.NewHealthClient(conn)
		resp, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
		if err != nil {
			return err
		}
		if resp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
			return errors.NewServiceError("server not serving")
		}
		return nil
	}

	// Create the health check
	check := health.CheckGRPCServerWithSettings(listener.Addr().String(), tSettings, healthCheckFunc)

	// Execute the health check
	ctx := context.Background()
	status, message, err := check(ctx, false)

	// Verify results
	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, message, "gRPC server")
	assert.Contains(t, message, "listening and accepting requests")
	assert.NoError(t, err)
}

// TestCheckGRPCServerWithSettings_UnhealthyServer tests the health check against an unhealthy gRPC server
func TestCheckGRPCServerWithSettings_UnhealthyServer(t *testing.T) {
	// Start a test gRPC server that reports as unhealthy
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer listener.Close()

	server := grpc.NewServer()
	healthServer := &mockHealthServer{healthy: false}
	grpc_health_v1.RegisterHealthServer(server, healthServer)

	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Create test settings
	tSettings := settings.NewSettings()

	// Create the health check function
	healthCheckFunc := func(ctx context.Context, conn *grpc.ClientConn) error {
		client := grpc_health_v1.NewHealthClient(conn)
		resp, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
		if err != nil {
			return err
		}
		if resp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
			return errors.NewServiceError("server not serving")
		}
		return nil
	}

	// Create the health check
	check := health.CheckGRPCServerWithSettings(listener.Addr().String(), tSettings, healthCheckFunc)

	// Execute the health check
	ctx := context.Background()
	status, message, err := check(ctx, false)

	// Verify results - should fail because server reports as unhealthy
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, message, "gRPC health check failed")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "server not serving")
}

// TestCheckGRPCServerWithSettings_ServerNotRunning tests the health check when the server is not running
func TestCheckGRPCServerWithSettings_ServerNotRunning(t *testing.T) {
	// Create test settings
	tSettings := settings.NewSettings()

	// Create the health check function
	healthCheckFunc := func(ctx context.Context, conn *grpc.ClientConn) error {
		client := grpc_health_v1.NewHealthClient(conn)
		_, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
		return err
	}

	// Create the health check for a non-existent server
	check := health.CheckGRPCServerWithSettings("localhost:59999", tSettings, healthCheckFunc)

	// Execute the health check
	ctx := context.Background()
	status, message, err := check(ctx, false)

	// Verify results - should fail because server is not running
	// When connection fails, we get "gRPC health check failed for ..." not "not accepting connections"
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, message, "gRPC health check failed for localhost:59999")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")
}

// TestCheckGRPCServerWithSettings_CircularDependencyPrevention tests that circular dependency is prevented
func TestCheckGRPCServerWithSettings_CircularDependencyPrevention(t *testing.T) {
	// Start a test gRPC server
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer listener.Close()

	server := grpc.NewServer()
	healthServer := &mockHealthServer{healthy: true}
	grpc_health_v1.RegisterHealthServer(server, healthServer)

	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Create test settings
	tSettings := settings.NewSettings()

	// Create the health check function
	healthCheckFunc := func(ctx context.Context, conn *grpc.ClientConn) error {
		client := grpc_health_v1.NewHealthClient(conn)
		resp, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
		if err != nil {
			return err
		}
		if resp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
			return errors.NewServiceError("server not serving")
		}
		return nil
	}

	// Create the health check
	check := health.CheckGRPCServerWithSettings(listener.Addr().String(), tSettings, healthCheckFunc)

	// Execute the health check with the circular dependency prevention context
	ctx := context.WithValue(context.Background(), "skip-grpc-self-check", true)
	status, message, err := check(ctx, false)

	// Verify results - should return OK and skip the actual check
	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, message, "skipped self-check")
	assert.NoError(t, err)
}

// TestCheckGRPCServerWithSettings_AddressFormatting tests various address formats
func TestCheckGRPCServerWithSettings_AddressFormatting(t *testing.T) {
	tests := []struct {
		name            string
		inputAddress    string
		expectedAddress string
	}{
		{
			name:            "address with localhost prefix",
			inputAddress:    "localhost:8080",
			expectedAddress: "localhost:8080",
		},
		{
			name:            "address with port only",
			inputAddress:    ":8080",
			expectedAddress: "localhost:8080",
		},
		{
			name:            "address with IP",
			inputAddress:    "127.0.0.1:8080",
			expectedAddress: "127.0.0.1:8080",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test settings
			tSettings := settings.NewSettings()

			// Create a health check function that always fails
			healthCheckFunc := func(ctx context.Context, conn *grpc.ClientConn) error {
				return errors.NewServiceError("test error") // Always fail to test address formatting in error message
			}

			// Create the health check
			check := health.CheckGRPCServerWithSettings(tt.inputAddress, tSettings, healthCheckFunc)

			// Execute the health check
			ctx := context.Background()
			_, message, _ := check(ctx, false)

			// Verify the address was used in the message (actual formatting happens in GetGRPCClient)
			// The implementation uses the address as-is, not formatting it
			assert.Contains(t, message, tt.inputAddress)
		})
	}
}

// TestCheckGRPCServerWithSettings_Timeout tests that the health check times out appropriately
func TestCheckGRPCServerWithSettings_Timeout(t *testing.T) {
	// Start a test gRPC server that delays responses
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer listener.Close()

	server := grpc.NewServer()
	slowHealthServer := &slowHealthServer{delay: 10 * time.Second}
	grpc_health_v1.RegisterHealthServer(server, slowHealthServer)

	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Create test settings
	tSettings := settings.NewSettings()

	// Create the health check function
	healthCheckFunc := func(ctx context.Context, conn *grpc.ClientConn) error {
		client := grpc_health_v1.NewHealthClient(conn)
		_, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
		return err
	}

	// Create the health check
	check := health.CheckGRPCServerWithSettings(listener.Addr().String(), tSettings, healthCheckFunc)

	// Execute the health check with a short timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	status, message, err := check(ctx, false)
	duration := time.Since(start)

	// Verify results - should timeout
	// When the health check function times out, we get "gRPC health check failed"
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, message, "gRPC health check failed")
	assert.Error(t, err)
	// The error should mention deadline exceeded or timeout
	assert.Contains(t, err.Error(), "DeadlineExceeded")
	// Should timeout within the 100ms context timeout + some buffer
	assert.Less(t, duration, 500*time.Millisecond, "Should timeout quickly")
}

// slowHealthServer is a health server that delays responses for timeout testing
type slowHealthServer struct {
	grpc_health_v1.UnimplementedHealthServer
	delay time.Duration
}

func (s *slowHealthServer) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	time.Sleep(s.delay)
	return &grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_SERVING,
	}, nil
}

// TestCheckGRPCServerWithSettings_WithTLS tests the health check with TLS settings
func TestCheckGRPCServerWithSettings_WithTLS(t *testing.T) {
	// Note: This test uses insecure credentials as configured in the actual implementation
	// when TLS is not explicitly configured in settings

	// Start a test gRPC server without TLS
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer listener.Close()

	server := grpc.NewServer()
	healthServer := &mockHealthServer{healthy: true}
	grpc_health_v1.RegisterHealthServer(server, healthServer)

	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Create test settings without TLS
	tSettings := settings.NewSettings()
	// TLS settings would be configured here if needed
	// For now, the implementation uses insecure credentials

	// Create the health check function
	healthCheckFunc := func(ctx context.Context, conn *grpc.ClientConn) error {
		client := grpc_health_v1.NewHealthClient(conn)
		resp, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
		if err != nil {
			return err
		}
		if resp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
			return errors.NewServiceError("server not serving")
		}
		return nil
	}

	// Create the health check
	check := health.CheckGRPCServerWithSettings(listener.Addr().String(), tSettings, healthCheckFunc)

	// Execute the health check
	ctx := context.Background()
	status, message, err := check(ctx, false)

	// Verify results
	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, message, "gRPC server")
	assert.NoError(t, err)
}

// TestCheckGRPCServerWithSettings_CustomHealthCheckFunc tests with different health check functions
func TestCheckGRPCServerWithSettings_CustomHealthCheckFunc(t *testing.T) {
	// Start a test gRPC server
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer listener.Close()

	server := grpc.NewServer()
	healthServer := &mockHealthServer{healthy: true}
	grpc_health_v1.RegisterHealthServer(server, healthServer)

	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Create test settings
	tSettings := settings.NewSettings()

	t.Run("health check function returns error", func(t *testing.T) {
		// Create a health check function that always returns an error
		healthCheckFunc := func(ctx context.Context, conn *grpc.ClientConn) error {
			return errors.NewServiceError("custom health check error")
		}

		// Create the health check
		check := health.CheckGRPCServerWithSettings(listener.Addr().String(), tSettings, healthCheckFunc)

		// Execute the health check
		ctx := context.Background()
		status, message, err := check(ctx, false)

		// Verify results - should fail with custom error
		assert.Equal(t, http.StatusServiceUnavailable, status)
		assert.Contains(t, message, "gRPC health check failed")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "custom health check error")
	})

	t.Run("health check function succeeds", func(t *testing.T) {
		// Create a health check function that always succeeds
		healthCheckFunc := func(ctx context.Context, conn *grpc.ClientConn) error {
			// Simulate a successful health check without actually calling the server
			return nil
		}

		// Create the health check
		check := health.CheckGRPCServerWithSettings(listener.Addr().String(), tSettings, healthCheckFunc)

		// Execute the health check
		ctx := context.Background()
		status, message, err := check(ctx, false)

		// Verify results - should succeed
		assert.Equal(t, http.StatusOK, status)
		assert.Contains(t, message, "listening and accepting requests")
		assert.NoError(t, err)
	})
}

// TestCheckGRPCServerWithSettings_LivenessCheck tests the liveness check parameter
func TestCheckGRPCServerWithSettings_LivenessCheck(t *testing.T) {
	// Start a test gRPC server
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer listener.Close()

	server := grpc.NewServer()
	healthServer := &mockHealthServer{healthy: true}
	grpc_health_v1.RegisterHealthServer(server, healthServer)

	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Create test settings
	tSettings := settings.NewSettings()

	// Create the health check function
	healthCheckFunc := func(ctx context.Context, conn *grpc.ClientConn) error {
		client := grpc_health_v1.NewHealthClient(conn)
		resp, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
		if err != nil {
			return err
		}
		if resp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
			return errors.NewServiceError("server not serving")
		}
		return nil
	}

	// Create the health check
	check := health.CheckGRPCServerWithSettings(listener.Addr().String(), tSettings, healthCheckFunc)

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
