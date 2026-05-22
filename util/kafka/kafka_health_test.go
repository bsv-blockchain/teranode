package kafka

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getUnusedPort returns a port number that was free at the time of checking
// and is now closed, making it ideal for testing connection failures
func getUnusedPort(t *testing.T) int {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	require.NoError(t, err)
	l, err := net.ListenTCP("tcp", addr)
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}

func TestHealthCheckerNilBrokers(t *testing.T) {
	healthCheck := HealthChecker(context.Background(), nil)

	status, message, err := healthCheck(context.Background(), true)

	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "Kafka is not configured - skipping health check", message)
	assert.NoError(t, err)
}

func TestHealthCheckerEmptyBrokers(t *testing.T) {
	healthCheck := HealthChecker(context.Background(), []string{})

	status, message, err := healthCheck(context.Background(), false)

	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "Kafka is not configured - skipping health check", message)
	assert.NoError(t, err)
}

func TestHealthCheckerInvalidBrokers(t *testing.T) {
	unusedPort1 := getUnusedPort(t)
	unusedPort2 := getUnusedPort(t)
	brokers := []string{
		fmt.Sprintf("invalid-broker:%d", unusedPort1),
		fmt.Sprintf("another-invalid:%d", unusedPort2),
	}
	healthCheck := HealthChecker(context.Background(), brokers)

	status, message, err := healthCheck(context.Background(), false)

	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Equal(t, "Failed to connect to Kafka", message)
	assert.Error(t, err)
}

func TestHealthCheckerLivenessParameter(t *testing.T) {
	unusedPort := getUnusedPort(t)
	invalidBrokers := []string{fmt.Sprintf("localhost:%d", unusedPort)}

	tests := []struct {
		name          string
		checkLiveness bool
		brokers       []string
		expectedMsg   string
		expectedOK    bool
	}{
		{
			name:          "Liveness check with nil brokers",
			checkLiveness: true,
			brokers:       nil,
			expectedMsg:   "Kafka is not configured - skipping health check",
			expectedOK:    true,
		},
		{
			name:          "Readiness check with nil brokers",
			checkLiveness: false,
			brokers:       nil,
			expectedMsg:   "Kafka is not configured - skipping health check",
			expectedOK:    true,
		},
		{
			name:          "Liveness check with brokers skips ping",
			checkLiveness: true,
			brokers:       invalidBrokers,
			expectedMsg:   "Kafka liveness (skipped)",
			expectedOK:    true,
		},
		{
			name:          "Readiness check with invalid brokers pings and fails",
			checkLiveness: false,
			brokers:       invalidBrokers,
			expectedMsg:   "Failed to connect to Kafka",
			expectedOK:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			healthCheck := HealthChecker(context.Background(), tt.brokers)

			status, message, err := healthCheck(context.Background(), tt.checkLiveness)

			if tt.expectedOK {
				assert.Equal(t, http.StatusOK, status)
				assert.NoError(t, err)
			} else {
				assert.Equal(t, http.StatusServiceUnavailable, status)
				assert.Error(t, err)
			}
			assert.Equal(t, tt.expectedMsg, message)
		})
	}
}

func TestHealthCheckerContextHandling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	unusedPort := getUnusedPort(t)
	brokers := []string{fmt.Sprintf("localhost:%d", unusedPort)}

	healthCheck := HealthChecker(ctx, brokers)

	cancel()

	status, message, err := healthCheck(ctx, false)

	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Equal(t, "Failed to connect to Kafka", message)
	assert.Error(t, err)
}

func TestHealthCheckerMultipleInvocations(t *testing.T) {
	healthCheck := HealthChecker(context.Background(), nil)

	// Call health check multiple times to ensure it's stateless
	for i := 0; i < 3; i++ {
		status, message, err := healthCheck(context.Background(), true)

		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, "Kafka is not configured - skipping health check", message)
		assert.NoError(t, err)
	}
}

func TestHealthCheckerErrorScenarios(t *testing.T) {
	// Get dynamic ports for each test case
	unusedPort1 := getUnusedPort(t)
	unusedPort2 := getUnusedPort(t)
	unusedPort3 := getUnusedPort(t)
	unusedPort4 := getUnusedPort(t)

	tests := []struct {
		name            string
		brokers         []string
		expectedStatus  int
		expectedMessage string
		expectError     bool
	}{
		{
			name:            "Single invalid broker",
			brokers:         []string{fmt.Sprintf("non-existent-host:%d", unusedPort1)},
			expectedStatus:  http.StatusServiceUnavailable,
			expectedMessage: "Failed to connect to Kafka",
			expectError:     true,
		},
		{
			name: "Multiple invalid brokers",
			brokers: []string{
				fmt.Sprintf("host1:%d", unusedPort2),
				fmt.Sprintf("host2:%d", unusedPort3),
				fmt.Sprintf("host3:%d", unusedPort4),
			},
			expectedStatus:  http.StatusServiceUnavailable,
			expectedMessage: "Failed to connect to Kafka",
			expectError:     true,
		},
		{
			name:            "Invalid port",
			brokers:         []string{"localhost:99999"},
			expectedStatus:  http.StatusServiceUnavailable,
			expectedMessage: "Failed to connect to Kafka",
			expectError:     true,
		},
		{
			name:            "Malformed broker URL",
			brokers:         []string{"not-a-valid-url"},
			expectedStatus:  http.StatusServiceUnavailable,
			expectedMessage: "Failed to connect to Kafka",
			expectError:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			healthCheck := HealthChecker(context.Background(), tt.brokers)

			status, message, err := healthCheck(context.Background(), false)

			assert.Equal(t, tt.expectedStatus, status)
			assert.Equal(t, tt.expectedMessage, message)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
