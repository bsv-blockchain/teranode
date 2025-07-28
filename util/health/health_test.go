package health

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
)

// Test helpers
func mustParseJSON(t *testing.T, data string) map[string]interface{} {
	t.Helper()
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(data), &result); err != nil {
		t.Fatalf("Failed to parse JSON: %v\nData: %s", err, data)
	}
	return result
}

func assertStatus(t *testing.T, got, want int) {
	t.Helper()
	if got != want {
		t.Errorf("status = %d, want %d", got, want)
	}
}

func assertJSONField(t *testing.T, jsonData map[string]interface{}, field string, want interface{}) {
	t.Helper()
	got, ok := jsonData[field]
	if !ok {
		t.Errorf("missing field %q in JSON", field)
		return
	}
	if fmt.Sprintf("%v", got) != fmt.Sprintf("%v", want) {
		t.Errorf("field %q = %v, want %v", field, got, want)
	}
}

func assertDependencyCount(t *testing.T, jsonData map[string]interface{}, want int) {
	t.Helper()
	deps, ok := jsonData["dependencies"].([]interface{})
	if !ok {
		t.Errorf("dependencies field is not an array")
		return
	}
	if len(deps) != want {
		t.Errorf("dependencies count = %d, want %d", len(deps), want)
	}
}

// Core functionality tests
func TestCheckAll_EmptyChecks(t *testing.T) {
	status, response, err := CheckAll(context.Background(), false, []Check{})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertStatus(t, status, http.StatusOK)

	result := mustParseJSON(t, response)
	assertJSONField(t, result, "status", "200")
	assertDependencyCount(t, result, 0)
}

func TestCheckAll_SingleHealthyCheck(t *testing.T) {
	checks := []Check{{
		Name: "database",
		Check: func(ctx context.Context, liveness bool) (int, string, error) {
			return http.StatusOK, "connected", nil
		},
	}}

	status, response, err := CheckAll(context.Background(), false, checks)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertStatus(t, status, http.StatusOK)

	result := mustParseJSON(t, response)
	assertJSONField(t, result, "status", "200")

	deps := result["dependencies"].([]interface{})
	dep := deps[0].(map[string]interface{})
	assertJSONField(t, dep, "resource", "database")
	assertJSONField(t, dep, "status", "200")
	assertJSONField(t, dep, "error", "<nil>")
	assertJSONField(t, dep, "message", "connected")
}

func TestCheckAll_SingleFailedCheck(t *testing.T) {
	checks := []Check{{
		Name: "database",
		Check: func(ctx context.Context, liveness bool) (int, string, error) {
			return http.StatusServiceUnavailable, "connection failed", errors.NewProcessingError("timeout")
		},
	}}

	status, response, err := CheckAll(context.Background(), false, checks)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertStatus(t, status, http.StatusServiceUnavailable)

	result := mustParseJSON(t, response)
	assertJSONField(t, result, "status", "503")

	deps := result["dependencies"].([]interface{})
	dep := deps[0].(map[string]interface{})
	// The error includes the error type prefix
	errorStr := fmt.Sprintf("%v", dep["error"])
	if !strings.Contains(errorStr, "timeout") {
		t.Errorf("error %q should contain 'timeout'", errorStr)
	}
}

func TestCheckAll_MixedHealthStatuses(t *testing.T) {
	checks := []Check{
		{
			Name: "healthy-service",
			Check: func(ctx context.Context, liveness bool) (int, string, error) {
				return http.StatusOK, "operational", nil
			},
		},
		{
			Name: "failed-service",
			Check: func(ctx context.Context, liveness bool) (int, string, error) {
				return http.StatusServiceUnavailable, "down", errors.NewProcessingError("connection refused")
			},
		},
	}

	status, response, err := CheckAll(context.Background(), false, checks)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Any failure should result in 503
	assertStatus(t, status, http.StatusServiceUnavailable)

	result := mustParseJSON(t, response)
	assertDependencyCount(t, result, 2)
}

func TestCheckAll_JSONDependencies(t *testing.T) {
	tests := []struct {
		name           string
		dependencyJSON string
		expectedDeps   []interface{}
	}{
		{
			name:           "single dependency",
			dependencyJSON: `{"service": "auth", "status": "ok"}`,
			expectedDeps:   []interface{}{map[string]interface{}{"service": "auth", "status": "ok"}},
		},
		{
			name:           "multiple dependencies",
			dependencyJSON: `{"service": "auth", "status": "ok"}, {"service": "user", "status": "ok"}`,
			expectedDeps: []interface{}{
				map[string]interface{}{"service": "auth", "status": "ok"},
				map[string]interface{}{"service": "user", "status": "ok"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checks := []Check{{
				Name: "api-gateway",
				Check: func(ctx context.Context, liveness bool) (int, string, error) {
					return http.StatusOK, tt.dependencyJSON, nil
				},
			}}

			_, response, err := CheckAll(context.Background(), false, checks)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			result := mustParseJSON(t, response)
			deps := result["dependencies"].([]interface{})
			dep := deps[0].(map[string]interface{})

			// Verify it has dependencies field instead of message
			if _, hasMessage := dep["message"]; hasMessage {
				t.Error("should not have message field for JSON dependencies")
			}
			if _, hasDeps := dep["dependencies"]; !hasDeps {
				t.Error("should have dependencies field for JSON response")
			}
		})
	}
}

func TestCheckAll_JSONDependenciesWithError(t *testing.T) {
	checks := []Check{{
		Name: "api-gateway",
		Check: func(ctx context.Context, liveness bool) (int, string, error) {
			return http.StatusServiceUnavailable, `{"service": "auth", "status": "down"}`, errors.NewProcessingError("dependency failed")
		},
	}}

	status, response, err := CheckAll(context.Background(), false, checks)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertStatus(t, status, http.StatusServiceUnavailable)

	result := mustParseJSON(t, response)
	deps := result["dependencies"].([]interface{})
	dep := deps[0].(map[string]interface{})
	// The error includes the error type prefix
	errorStr := fmt.Sprintf("%v", dep["error"])
	if !strings.Contains(errorStr, "dependency failed") {
		t.Errorf("error %q should contain 'dependency failed'", errorStr)
	}
}

func TestCheckAll_LivenessVsReadiness(t *testing.T) {
	var capturedLiveness bool
	checks := []Check{{
		Name: "service",
		Check: func(ctx context.Context, liveness bool) (int, string, error) {
			capturedLiveness = liveness
			if liveness {
				return http.StatusOK, "alive", nil
			}
			return http.StatusOK, "ready", nil
		},
	}}

	// Test liveness
	_, response, _ := CheckAll(context.Background(), true, checks)
	if !capturedLiveness {
		t.Error("liveness flag should be true")
	}
	result := mustParseJSON(t, response)
	deps := result["dependencies"].([]interface{})
	dep := deps[0].(map[string]interface{})
	assertJSONField(t, dep, "message", "alive")

	// Test readiness
	_, response, _ = CheckAll(context.Background(), false, checks)
	if capturedLiveness {
		t.Error("liveness flag should be false")
	}
	result = mustParseJSON(t, response)
	deps = result["dependencies"].([]interface{})
	dep = deps[0].(map[string]interface{})
	assertJSONField(t, dep, "message", "ready")
}

func TestCheckAll_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	checks := []Check{{
		Name: "service",
		Check: func(ctx context.Context, liveness bool) (int, string, error) {
			if ctx.Err() != nil {
				return http.StatusServiceUnavailable, "cancelled", errors.NewProcessingError("context cancelled", ctx.Err())
			}
			return http.StatusOK, "should not reach here", nil
		},
	}}

	status, response, err := CheckAll(ctx, false, checks)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertStatus(t, status, http.StatusServiceUnavailable)

	result := mustParseJSON(t, response)
	deps := result["dependencies"].([]interface{})
	dep := deps[0].(map[string]interface{})
	assertJSONField(t, dep, "message", "cancelled")
}

func TestCheckAll_ContextWithTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	checks := []Check{{
		Name: "slow-service",
		Check: func(ctx context.Context, liveness bool) (int, string, error) {
			select {
			case <-ctx.Done():
				return http.StatusServiceUnavailable, "timeout", errors.NewProcessingError("context timeout", ctx.Err())
			case <-time.After(50 * time.Millisecond):
				return http.StatusOK, "completed", nil
			}
		},
	}}

	status, response, err := CheckAll(ctx, false, checks)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertStatus(t, status, http.StatusOK)

	result := mustParseJSON(t, response)
	deps := result["dependencies"].([]interface{})
	dep := deps[0].(map[string]interface{})
	assertJSONField(t, dep, "message", "completed")
}

func TestCheckAll_EdgeCases(t *testing.T) {
	tests := []struct {
		name           string
		check          func(context.Context, bool) (int, string, error)
		expectedStatus int
		validate       func(t *testing.T, dep map[string]interface{})
	}{
		{
			name: "empty message",
			check: func(ctx context.Context, liveness bool) (int, string, error) {
				return http.StatusOK, "", nil
			},
			expectedStatus: http.StatusOK,
			validate: func(t *testing.T, dep map[string]interface{}) {
				assertJSONField(t, dep, "message", "")
			},
		},
		{
			name: "non-200 status with nil error",
			check: func(ctx context.Context, liveness bool) (int, string, error) {
				return http.StatusServiceUnavailable, "degraded", nil
			},
			expectedStatus: http.StatusServiceUnavailable,
			validate: func(t *testing.T, dep map[string]interface{}) {
				assertJSONField(t, dep, "error", "<nil>")
			},
		},
		{
			name: "200 status with error (should fail)",
			check: func(ctx context.Context, liveness bool) (int, string, error) {
				return http.StatusOK, "partial", errors.NewProcessingError("minor issue")
			},
			expectedStatus: http.StatusServiceUnavailable,
			validate: func(t *testing.T, dep map[string]interface{}) {
				// The error includes the error type prefix
				errorStr := fmt.Sprintf("%v", dep["error"])
				if !strings.Contains(errorStr, "minor issue") {
					t.Errorf("error %q should contain 'minor issue'", errorStr)
				}
			},
		},
		{
			name: "malformed JSON-like message",
			check: func(ctx context.Context, liveness bool) (int, string, error) {
				return http.StatusOK, "{incomplete", nil
			},
			expectedStatus: http.StatusOK,
			validate: func(t *testing.T, dep map[string]interface{}) {
				// Should be treated as regular message, not JSON
				assertJSONField(t, dep, "message", "{incomplete")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checks := []Check{{Name: "test", Check: tt.check}}

			status, response, err := CheckAll(context.Background(), false, checks)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			assertStatus(t, status, tt.expectedStatus)

			result := mustParseJSON(t, response)
			deps := result["dependencies"].([]interface{})
			dep := deps[0].(map[string]interface{})
			tt.validate(t, dep)
		})
	}
}

func TestCheckAll_ErrorConditions(t *testing.T) {
	tests := []struct {
		name           string
		checks         []Check
		expectedStatus int
	}{
		{
			name: "check function panics simulation",
			checks: []Check{{
				Name: "panicking-service",
				Check: func(ctx context.Context, liveness bool) (int, string, error) {
					return http.StatusInternalServerError, "panic occurred", errors.NewProcessingError("runtime panic")
				},
			}},
			expectedStatus: http.StatusServiceUnavailable,
		},
		{
			name: "multiple failing checks",
			checks: []Check{
				{
					Name: "service1",
					Check: func(ctx context.Context, liveness bool) (int, string, error) {
						return http.StatusServiceUnavailable, "down", errors.NewProcessingError("connection failed")
					},
				},
				{
					Name: "service2",
					Check: func(ctx context.Context, liveness bool) (int, string, error) {
						return http.StatusInternalServerError, "error", errors.NewProcessingError("internal error")
					},
				},
			},
			expectedStatus: http.StatusServiceUnavailable,
		},
		{
			name: "mixed success and failure",
			checks: []Check{
				{
					Name: "healthy-service",
					Check: func(ctx context.Context, liveness bool) (int, string, error) {
						return http.StatusOK, "operational", nil
					},
				},
				{
					Name: "degraded-service",
					Check: func(ctx context.Context, liveness bool) (int, string, error) {
						return http.StatusPartialContent, "degraded", nil
					},
				},
				{
					Name: "failed-service",
					Check: func(ctx context.Context, liveness bool) (int, string, error) {
						return http.StatusServiceUnavailable, "failed", errors.NewProcessingError("service unavailable")
					},
				},
			},
			expectedStatus: http.StatusServiceUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, response, err := CheckAll(context.Background(), false, tt.checks)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			assertStatus(t, status, tt.expectedStatus)

			// Verify response is valid JSON
			_ = mustParseJSON(t, response)
		})
	}
}

func TestCheckAll_RealWorldScenario(t *testing.T) {
	// Simulate a microservice with various dependencies
	checks := []Check{
		{
			Name: "postgres",
			Check: func(ctx context.Context, liveness bool) (int, string, error) {
				// Simulate checking connection pool
				return http.StatusOK, "pool: 10/50 connections", nil
			},
		},
		{
			Name: "redis",
			Check: func(ctx context.Context, liveness bool) (int, string, error) {
				// Simulate cache health
				return http.StatusOK, "memory: 45%, hits: 92%", nil
			},
		},
		{
			Name: "payment-api",
			Check: func(ctx context.Context, liveness bool) (int, string, error) {
				// Simulate external API with detailed status
				return http.StatusOK, `{"endpoint": "https://api.payment.com", "latency": "120ms", "rate_limit": "450/500"}`, nil
			},
		},
		{
			Name: "message-queue",
			Check: func(ctx context.Context, liveness bool) (int, string, error) {
				// Simulate a degraded service
				return http.StatusServiceUnavailable, "connection lost", errors.NewProcessingError("broker unreachable")
			},
		},
	}

	status, response, err := CheckAll(context.Background(), false, checks)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should be unhealthy due to message-queue failure
	assertStatus(t, status, http.StatusServiceUnavailable)

	result := mustParseJSON(t, response)
	assertJSONField(t, result, "status", "503")
	assertDependencyCount(t, result, 4)

	// Verify we can parse the entire response
	deps := result["dependencies"].([]interface{})
	for i, dep := range deps {
		depMap := dep.(map[string]interface{})
		if depMap["resource"] == "payment-api" {
			// Should have dependencies field for JSON response
			if _, ok := depMap["dependencies"]; !ok {
				t.Errorf("payment-api should have dependencies field")
			}
		} else {
			// Others should have message field
			if _, ok := depMap["message"]; !ok {
				t.Errorf("dependency %d should have message field", i)
			}
		}
	}
}
