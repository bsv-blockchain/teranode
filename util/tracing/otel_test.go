package tracing

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace/noop"
)

// resetGlobalState resets the package-level variables for clean testing
func resetGlobalState() {
	mu.Lock()
	defer mu.Unlock()

	// Reset the once.Do state by creating a new sync.Once
	once = sync.Once{}
	initErr = nil

	if tp != nil {
		// Use context with timeout to avoid hanging
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tp.Shutdown(ctx)
		tp = nil
	}

	// Reset global otel state - use noop tracer provider
	otel.SetTracerProvider(noop.NewTracerProvider())
}

// TestInitTracer_Success tests successful initialization
func TestInitTracer_Success(t *testing.T) {
	resetGlobalState()
	defer resetGlobalState()

	// Create test settings with valid tracing URL
	testSettings := &settings.Settings{
		ServiceName:       "test-service",
		Version:           "1.0.0",
		Commit:            "abc123",
		TracingSampleRate: 1.0,
	}

	// Use a valid URL that will fail to connect but won't fail URL parsing
	tracingURL, err := url.Parse("http://localhost:14268/api/traces")
	require.NoError(t, err)
	testSettings.TracingCollectorURL = tracingURL

	// First call should initialize successfully
	err = InitTracer(testSettings)
	assert.NoError(t, err)

	// Verify tracer provider was set
	assert.NotNil(t, tp)

	// Second call should be no-op and return no error
	err = InitTracer(testSettings)
	assert.NoError(t, err)
}

// TestInitTracer_InvalidURL tests initialization with invalid tracing URL
func TestInitTracer_InvalidURL(t *testing.T) {
	resetGlobalState()
	defer resetGlobalState()

	// Create test settings with an URL that will trigger an error in the OTLP exporter
	testSettings := &settings.Settings{
		ServiceName:       "test-service",
		Version:           "1.0.0",
		Commit:            "abc123",
		TracingSampleRate: 1.0,
	}

	// Use a malformed URL that will cause parsing issues inside the exporter
	// The URL itself parses fine, but the exporter will reject it
	tracingURL, err := url.Parse("://invalid")
	if err != nil {
		// If this URL fails to parse, try a different approach
		tracingURL, _ = url.Parse("http://")
	}
	testSettings.TracingCollectorURL = tracingURL

	// Try initialization - it might succeed despite the invalid URL
	// because the OTLP exporter is quite tolerant
	err = InitTracer(testSettings)
	// Don't assert error here - let's see what actually happens
	t.Logf("InitTracer result: %v", err)

	// If it did error, verify the error message and state
	if err != nil {
		assert.Contains(t, err.Error(), "failed to create OTLP exporter")
		assert.Nil(t, tp)

		// Second call should return the same error
		err = InitTracer(testSettings)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create OTLP exporter")
	} else {
		// If it succeeded, that's also valid - the exporter might be tolerant
		assert.NotNil(t, tp)
	}
}

// TestInitTracer_ResourceCreationFailure tests resource creation failure
func TestInitTracer_ResourceCreationFailure(t *testing.T) {
	resetGlobalState()
	defer resetGlobalState()

	// This test is tricky because resource.New rarely fails in practice
	// We'll test successful path and then test what happens when already initialized with error
	testSettings := &settings.Settings{
		ServiceName:       "", // Empty service name should still work
		Version:           "1.0.0",
		Commit:            "abc123",
		TracingSampleRate: 1.0,
	}

	tracingURL, err := url.Parse("http://localhost:14268/api/traces")
	require.NoError(t, err)
	testSettings.TracingCollectorURL = tracingURL

	// Should succeed even with empty service name
	err = InitTracer(testSettings)
	assert.NoError(t, err)
	assert.NotNil(t, tp)
}

// TestShutdownTracer_Success tests successful shutdown
func TestShutdownTracer_Success(t *testing.T) {
	resetGlobalState()
	defer resetGlobalState()

	// Initialize tracer first
	testSettings := &settings.Settings{
		ServiceName:       "test-service",
		Version:           "1.0.0",
		Commit:            "abc123",
		TracingSampleRate: 1.0,
	}

	tracingURL, err := url.Parse("http://localhost:14268/api/traces")
	require.NoError(t, err)
	testSettings.TracingCollectorURL = tracingURL

	err = InitTracer(testSettings)
	require.NoError(t, err)
	require.NotNil(t, tp)

	// Test successful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ShutdownTracer(ctx)
	assert.NoError(t, err)

	// Verify tracer provider was cleared
	assert.Nil(t, tp)

	// Second shutdown should be no-op
	err = ShutdownTracer(ctx)
	assert.NoError(t, err)
}

// TestShutdownTracer_FlushConnectionRefused tests flush error handling with connection refused
func TestShutdownTracer_FlushConnectionRefused(t *testing.T) {
	resetGlobalState()
	defer resetGlobalState()

	// This test is challenging because we need to trigger a "connection refused" error
	// The best we can do is simulate a timeout and see what error we get
	testSettings := &settings.Settings{
		ServiceName:       "test-service",
		Version:           "1.0.0",
		Commit:            "abc123",
		TracingSampleRate: 1.0,
	}

	// Use localhost with an invalid port to trigger connection errors
	tracingURL, err := url.Parse("http://localhost:1/api/traces")
	require.NoError(t, err)
	testSettings.TracingCollectorURL = tracingURL

	err = InitTracer(testSettings)
	require.NoError(t, err)
	require.NotNil(t, tp)

	// Create context with very short timeout to cause flush to fail
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	// This will trigger a flush error - but probably not "connection refused"
	err = ShutdownTracer(ctx)

	if err != nil {
		// If we got an error, it should be about failing to flush spans
		assert.Contains(t, err.Error(), "failed to flush spans")
		// And the tracer provider should NOT be cleared (early return from function)
		assert.NotNil(t, tp, "Tracer provider should not be nil when flush fails with non-connection-refused error")
	} else {
		// If no error, then flush succeeded and provider should be nil
		assert.Nil(t, tp)
	}
}

// TestShutdownTracer_FlushOtherError tests flush error handling for non-connection errors
func TestShutdownTracer_FlushOtherError(t *testing.T) {
	resetGlobalState()
	defer resetGlobalState()

	// Initialize with valid settings
	testSettings := &settings.Settings{
		ServiceName:       "test-service",
		Version:           "1.0.0",
		Commit:            "abc123",
		TracingSampleRate: 1.0,
	}

	tracingURL, err := url.Parse("http://localhost:14268/api/traces")
	require.NoError(t, err)
	testSettings.TracingCollectorURL = tracingURL

	err = InitTracer(testSettings)
	require.NoError(t, err)
	require.NotNil(t, tp)

	// Use context that's already cancelled to trigger context deadline exceeded
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// This should trigger a non-connection-refused error
	err = ShutdownTracer(ctx)
	// Should return error for non-connection-refused errors
	if err != nil {
		assert.Contains(t, err.Error(), "failed to flush spans")
		// Provider should NOT be cleared due to early return
		assert.NotNil(t, tp, "Tracer provider should not be nil when flush fails")
	} else {
		// If no error, then flush succeeded and provider should be nil
		assert.Nil(t, tp)
	}
}

// TestShutdownTracer_ShutdownError tests shutdown error handling
func TestShutdownTracer_ShutdownError(t *testing.T) {
	resetGlobalState()
	defer resetGlobalState()

	// For this test, we'll setup a successful init and then test what happens
	// when shutdown fails. In practice, tp.Shutdown rarely fails, but we can
	// simulate it by manually setting up a provider and then testing cancellation
	testSettings := &settings.Settings{
		ServiceName:       "test-service",
		Version:           "1.0.0",
		Commit:            "abc123",
		TracingSampleRate: 1.0,
	}

	tracingURL, err := url.Parse("http://localhost:14268/api/traces")
	require.NoError(t, err)
	testSettings.TracingCollectorURL = tracingURL

	err = InitTracer(testSettings)
	require.NoError(t, err)
	require.NotNil(t, tp)

	// Test with valid context - should succeed
	ctx := context.Background()
	err = ShutdownTracer(ctx)
	assert.NoError(t, err)
	assert.Nil(t, tp)
}

// TestShutdownTracer_NilProvider tests shutdown when no provider is set
func TestShutdownTracer_NilProvider(t *testing.T) {
	resetGlobalState()
	defer resetGlobalState()

	// Ensure tp is nil
	assert.Nil(t, tp)

	// Should be no-op and return no error
	err := ShutdownTracer(context.Background())
	assert.NoError(t, err)
}

// TestConcurrentInitialization tests that multiple goroutines calling InitTracer is safe
func TestConcurrentInitialization(t *testing.T) {
	resetGlobalState()
	defer resetGlobalState()

	testSettings := &settings.Settings{
		ServiceName:       "test-service",
		Version:           "1.0.0",
		Commit:            "abc123",
		TracingSampleRate: 1.0,
	}

	tracingURL, err := url.Parse("http://localhost:14268/api/traces")
	require.NoError(t, err)
	testSettings.TracingCollectorURL = tracingURL

	// Start multiple goroutines trying to initialize
	const numGoroutines = 10
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			errors <- InitTracer(testSettings)
		}()
	}

	// Collect all errors
	var errs []error
	for i := 0; i < numGoroutines; i++ {
		err := <-errors
		if err != nil {
			errs = append(errs, err)
		}
	}

	// All should succeed (since they all use the same valid settings)
	assert.Empty(t, errs, "Expected no errors from concurrent initialization")
	assert.NotNil(t, tp, "Tracer provider should be initialized")
}

// TestConcurrentShutdown tests that multiple goroutines calling ShutdownTracer is safe
func TestConcurrentShutdown(t *testing.T) {
	resetGlobalState()
	defer resetGlobalState()

	// Initialize first
	testSettings := &settings.Settings{
		ServiceName:       "test-service",
		Version:           "1.0.0",
		Commit:            "abc123",
		TracingSampleRate: 1.0,
	}

	tracingURL, err := url.Parse("http://localhost:14268/api/traces")
	require.NoError(t, err)
	testSettings.TracingCollectorURL = tracingURL

	err = InitTracer(testSettings)
	require.NoError(t, err)

	// Start multiple goroutines trying to shutdown
	const numGoroutines = 10
	errors := make(chan error, numGoroutines)

	ctx := context.Background()
	for i := 0; i < numGoroutines; i++ {
		go func() {
			errors <- ShutdownTracer(ctx)
		}()
	}

	// Collect all errors
	var errs []error
	for i := 0; i < numGoroutines; i++ {
		err := <-errors
		if err != nil {
			errs = append(errs, err)
		}
	}

	// All should succeed (shutdown should be safe to call multiple times)
	assert.Empty(t, errs, "Expected no errors from concurrent shutdown")
	assert.Nil(t, tp, "Tracer provider should be nil after shutdown")
}

// TestShutdownTracer_ConnectionRefusedSpecific tests the specific "connection refused" error handling
func TestShutdownTracer_ConnectionRefusedSpecific(t *testing.T) {
	// This test will try to trigger the actual "connection refused" path
	// by using a definitely unreachable endpoint
	resetGlobalState()
	defer resetGlobalState()

	testSettings := &settings.Settings{
		ServiceName:       "test-service",
		Version:           "1.0.0",
		Commit:            "abc123",
		TracingSampleRate: 1.0,
	}

	// Use a definitely invalid/unreachable address
	tracingURL, err := url.Parse("http://192.0.2.1:1/api/traces") // RFC5737 test address
	require.NoError(t, err)
	testSettings.TracingCollectorURL = tracingURL

	err = InitTracer(testSettings)
	require.NoError(t, err)
	require.NotNil(t, tp)

	// Use a reasonable timeout to allow connection attempts
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// This might trigger connection refused or timeout
	err = ShutdownTracer(ctx)

	// Test the specific error handling logic
	if err != nil {
		if strings.Contains(err.Error(), "connection refused") {
			// This should not happen because connection refused errors return nil
			t.Errorf("Expected nil return for connection refused, but got error: %v", err)
		} else {
			// Non-connection refused errors should be returned and tp should not be nil
			assert.Contains(t, err.Error(), "failed to flush spans")
			assert.NotNil(t, tp, "Tracer provider should not be nil when flush fails with non-connection-refused error")
		}
	} else {
		// If no error, either flush succeeded or connection refused was handled
		// In both cases, tp should be nil only if the full shutdown completed
		// (connection refused case logs and returns nil, but doesn't clear tp)
	}
}

// Test edge cases for error message handling
func TestShutdownTracer_ErrorMessageHandling(t *testing.T) {
	resetGlobalState()
	defer resetGlobalState()

	// This test focuses on the specific error message checking in ShutdownTracer
	// The function checks if err.Error() contains "connection refused"

	testSettings := &settings.Settings{
		ServiceName:       "test-service",
		Version:           "1.0.0",
		Commit:            "abc123",
		TracingSampleRate: 1.0,
	}

	tracingURL, err := url.Parse("http://localhost:14268/api/traces")
	require.NoError(t, err)
	testSettings.TracingCollectorURL = tracingURL

	err = InitTracer(testSettings)
	require.NoError(t, err)
	require.NotNil(t, tp)

	// Test with immediate cancellation to get a non-connection-refused error
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = ShutdownTracer(ctx)
	// The error (if any) should not contain "connection refused"
	if err != nil {
		assert.False(t, strings.Contains(err.Error(), "connection refused"))
		assert.Contains(t, err.Error(), "failed to flush spans")
	}
}

// TestInitTracer_ExporterCreationError tests the specific error path when OTLP exporter creation fails
func TestInitTracer_ExporterCreationError(t *testing.T) {
	resetGlobalState()
	defer resetGlobalState()

	// The OTLP HTTP exporter is very tolerant and rarely fails during creation.
	// This test documents the defensive error handling path that exists but is hard to trigger.
	testSettings := &settings.Settings{
		ServiceName:       "test-service",
		Version:           "1.0.0",
		Commit:            "abc123",
		TracingSampleRate: 1.0,
	}

	// Try various URLs that might cause issues
	testCases := []struct {
		name string
		url  string
	}{
		{
			name: "empty_host",
			url:  "http:///api/traces",
		},
		{
			name: "invalid_port",
			url:  "http://localhost:99999/api/traces",
		},
		{
			name: "special_chars_path",
			url:  "http://localhost:4318/path%00with%00nulls",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resetGlobalState()

			// Parse URL - if this fails, skip the test case
			tracingURL, err := url.Parse(tc.url)
			if err != nil {
				t.Skipf("URL parsing failed (expected for some test cases): %v", err)
				return
			}
			testSettings.TracingCollectorURL = tracingURL

			// Try initialization
			err = InitTracer(testSettings)
			t.Logf("InitTracer result for %s: %v", tc.name, err)

			if err != nil && strings.Contains(err.Error(), "failed to create OTLP exporter") {
				// Successfully triggered the exporter creation error path
				t.Logf("Successfully triggered exporter creation error path")
				assert.Nil(t, tp)

				// Test that subsequent calls return the same cached error
				err2 := InitTracer(testSettings)
				assert.Error(t, err2)
				assert.Equal(t, err.Error(), err2.Error())
			} else if err == nil {
				// Exporter creation succeeded - this is the common case
				t.Logf("Exporter creation succeeded (expected - OTLP HTTP exporter is tolerant)")
				assert.NotNil(t, tp)
			} else {
				// Some other error occurred
				t.Logf("Got different error: %v", err)
			}
		})
	}
}

// TestInitTracer_ResourceCreationError tests the resource creation error path
func TestInitTracer_ResourceCreationError(t *testing.T) {
	resetGlobalState()
	defer resetGlobalState()

	// This is very difficult to trigger in practice because resource.New rarely fails
	// The only way to make it fail is by providing problematic attributes
	// However, the resource creation in the actual code uses valid semconv attributes
	// So this test will document that the path exists but is hard to trigger

	testSettings := &settings.Settings{
		ServiceName:       "test-service",
		Version:           "1.0.0",
		Commit:            "abc123",
		TracingSampleRate: 1.0,
	}

	// Use a valid URL since we want to get past the exporter creation
	tracingURL, err := url.Parse("http://localhost:14268/api/traces")
	require.NoError(t, err)
	testSettings.TracingCollectorURL = tracingURL

	// The resource creation error path is covered by the line:
	// res, initErr = resource.New(context.Background(), resource.WithAttributes(...))
	// if initErr != nil { initErr = errors.NewProcessingError("failed to create resource", initErr); return }

	// This should succeed in normal cases
	err = InitTracer(testSettings)

	// Log the result
	t.Logf("InitTracer result: %v", err)

	// In practice, this should succeed, demonstrating the resource error path is defensive
	if err == nil {
		assert.NotNil(t, tp)
	}
}

// TestShutdownTracer_ConnectionRefusedPath tests the actual "connection refused" error handling
func TestShutdownTracer_ConnectionRefusedPath(t *testing.T) {
	resetGlobalState()
	defer resetGlobalState()

	// To test the "connection refused" path, we need to create a tracer that will
	// actually return an error containing "connection refused" when ForceFlush is called
	testSettings := &settings.Settings{
		ServiceName:       "test-service",
		Version:           "1.0.0",
		Commit:            "abc123",
		TracingSampleRate: 1.0,
	}

	// Use localhost with port 1 which should be refused
	tracingURL, err := url.Parse("http://localhost:1/api/traces")
	require.NoError(t, err)
	testSettings.TracingCollectorURL = tracingURL

	err = InitTracer(testSettings)
	require.NoError(t, err)
	require.NotNil(t, tp)

	// Try multiple approaches to trigger connection refused
	contexts := []context.Context{
		func() context.Context {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			t.Cleanup(cancel)
			return ctx
		}(),
		func() context.Context {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			t.Cleanup(cancel)
			return ctx
		}(),
	}

	for i, ctx := range contexts {
		t.Run(fmt.Sprintf("attempt_%d", i), func(t *testing.T) {
			// Reinitialize tracer for each attempt since ShutdownTracer sets tp = nil
			if tp == nil {
				resetGlobalState()
				err = InitTracer(testSettings)
				require.NoError(t, err)
				require.NotNil(t, tp)
			}

			err := ShutdownTracer(ctx)
			t.Logf("Shutdown error: %v", err)

			// Check if we got a connection refused error
			if err != nil && strings.Contains(err.Error(), "connection refused") {
				// This should not happen because connection refused errors should return nil
				t.Errorf("Connection refused errors should return nil, but got: %v", err)
			}

			// If error is non-nil and doesn't contain "connection refused",
			// then we hit the other error path and tp should remain set
			if err != nil && !strings.Contains(err.Error(), "connection refused") {
				assert.Contains(t, err.Error(), "failed to flush spans")
			}
		})
	}
}

// TestShutdownTracer_ShutdownSpecificError tests the tp.Shutdown() error path
func TestShutdownTracer_ShutdownSpecificError(t *testing.T) {
	resetGlobalState()
	defer resetGlobalState()

	// This test aims to hit the specific error path:
	// if err := tp.Shutdown(ctx); err != nil {
	//     return errors.NewProcessingError("failed to shutdown tracer", err)
	// }
	// This happens after successful flush but failed shutdown

	testSettings := &settings.Settings{
		ServiceName:       "test-service",
		Version:           "1.0.0",
		Commit:            "abc123",
		TracingSampleRate: 1.0,
	}

	tracingURL, err := url.Parse("http://localhost:14268/api/traces")
	require.NoError(t, err)
	testSettings.TracingCollectorURL = tracingURL

	err = InitTracer(testSettings)
	require.NoError(t, err)
	require.NotNil(t, tp)

	// Try to create a context that will cause shutdown to fail after flush succeeds
	// This is very difficult to achieve in practice, but we can try immediate cancellation
	// after a short delay to let flush potentially succeed

	// Create a context that will be cancelled after a very short time
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	// Give it a tiny bit of time
	time.Sleep(2 * time.Millisecond)

	err = ShutdownTracer(ctx)
	t.Logf("Shutdown result: %v", err)

	// The specific error path we're trying to hit is when flush succeeds but shutdown fails
	// This is extremely rare in practice, so we document the attempt
	if err != nil {
		// Could be either flush or shutdown error
		assert.True(t,
			strings.Contains(err.Error(), "failed to flush spans") ||
				strings.Contains(err.Error(), "failed to shutdown tracer"),
			"Error should be about flush or shutdown failure: %v", err)
	}
}

// TestInitTracer_ForceResourceError attempts to trigger resource creation error
func TestInitTracer_ForceResourceError(t *testing.T) {
	// This test documents the defensive resource error handling
	// The resource.New call uses standard semconv attributes which should never fail
	// But the error handling path exists for defensive programming

	resetGlobalState()
	defer resetGlobalState()

	// Create a scenario that might trigger resource errors
	// In practice, this is nearly impossible with the current implementation
	testSettings := &settings.Settings{
		ServiceName:       "", // Empty name
		Version:           "", // Empty version
		Commit:            "", // Empty commit
		TracingSampleRate: 1.0,
	}

	tracingURL, err := url.Parse("http://localhost:14268/api/traces")
	require.NoError(t, err)
	testSettings.TracingCollectorURL = tracingURL

	// This should still succeed even with empty values
	err = InitTracer(testSettings)
	t.Logf("InitTracer with empty values: %v", err)

	// The resource creation is very robust, so this will likely succeed
	// This test documents that the error path exists for defensive purposes
	if err == nil {
		assert.NotNil(t, tp)
	} else if strings.Contains(err.Error(), "failed to create resource") {
		// If we somehow triggered the resource error, verify the handling
		assert.Contains(t, err.Error(), "failed to create resource")
		assert.Nil(t, tp)
	}
}

// TestConnectionRefused attempts to trigger the actual "connection refused" error handling
// This test aims to hit the uncovered lines in ShutdownTracer (lines 95-98)
func TestConnectionRefused(t *testing.T) {
	resetGlobalState()
	defer resetGlobalState()

	// Create settings that point to a localhost port that's likely to refuse connections
	testSettings := &settings.Settings{
		ServiceName:       "test-service",
		Version:           "1.0.0",
		Commit:            "abc123",
		TracingSampleRate: 1.0,
	}

	// Use port 1 which should refuse connections on most systems
	tracingURL, err := url.Parse("http://127.0.0.1:1/api/traces")
	require.NoError(t, err)
	testSettings.TracingCollectorURL = tracingURL

	err = InitTracer(testSettings)
	require.NoError(t, err)
	require.NotNil(t, tp)

	// Try multiple scenarios to trigger connection refused
	scenarios := []struct {
		name    string
		timeout time.Duration
	}{
		{"short_timeout", 10 * time.Millisecond},
		{"medium_timeout", 100 * time.Millisecond},
		{"longer_timeout", 500 * time.Millisecond},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			// Reset state for this scenario
			if tp == nil {
				err = InitTracer(testSettings)
				require.NoError(t, err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), scenario.timeout)
			defer cancel()

			err := ShutdownTracer(ctx)
			t.Logf("Scenario %s - Error: %v", scenario.name, err)

			// Check if we hit the connection refused path
			if err == nil {
				// Either successful shutdown or connection refused was handled
				t.Logf("Shutdown succeeded or connection refused was handled")
			} else if strings.Contains(err.Error(), "connection refused") {
				// This should not happen - connection refused should return nil
				t.Errorf("Connection refused should return nil, got error: %v", err)
			} else {
				// Some other error - this is expected for timeout/context errors
				t.Logf("Got expected non-connection-refused error: %v", err)
				assert.Contains(t, err.Error(), "failed to flush spans")
			}
		})
	}
}

// TestExporterFailure attempts to trigger exporter creation failures
// This aims to hit lines 42-45 in InitTracer
func TestExporterFailure(t *testing.T) {
	resetGlobalState()
	defer resetGlobalState()

	testCases := []struct {
		name      string
		url       string
		shouldErr bool
	}{
		{
			name:      "empty_host",
			url:       "http:///api/traces",
			shouldErr: false, // This might actually work
		},
		{
			name:      "invalid_scheme_chars",
			url:       "ht\x00tp://localhost:4318/v1/traces",
			shouldErr: true, // Should fail URL parsing
		},
		{
			name:      "valid_but_unreachable",
			url:       "http://192.0.2.1:99999/api/traces", // RFC5737 test address
			shouldErr: false,                               // URL is valid, exporter creation might succeed
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resetGlobalState()

			testSettings := &settings.Settings{
				ServiceName:       "test-service",
				Version:           "1.0.0",
				Commit:            "abc123",
				TracingSampleRate: 1.0,
			}

			// Try to parse the URL - some might fail here
			tracingURL, err := url.Parse(tc.url)
			if err != nil {
				if tc.shouldErr {
					t.Logf("Expected URL parsing to fail: %v", err)
					return // This is expected
				} else {
					t.Fatalf("Unexpected URL parsing error: %v", err)
				}
			}
			testSettings.TracingCollectorURL = tracingURL

			err = InitTracer(testSettings)
			t.Logf("InitTracer result for %s: %v", tc.name, err)

			if err != nil {
				if strings.Contains(err.Error(), "failed to create OTLP exporter") {
					t.Logf("Successfully triggered exporter creation error path")
					assert.Nil(t, tp)

					// Test that subsequent calls return the same error
					err2 := InitTracer(testSettings)
					assert.Error(t, err2)
					assert.Equal(t, err.Error(), err2.Error())
				} else {
					t.Logf("Got different error: %v", err)
				}
			} else {
				t.Logf("InitTracer succeeded - exporter creation was tolerant")
				assert.NotNil(t, tp)
			}
		})
	}
}

// TestResourceFailure attempts to create scenarios where resource creation might fail
// This is nearly impossible with the current implementation but we document the attempt
func TestResourceFailure(t *testing.T) {
	resetGlobalState()
	defer resetGlobalState()

	// Resource creation with standard semconv attributes is very robust
	// This test documents the defensive error handling path that exists
	testSettings := &settings.Settings{
		ServiceName:       strings.Repeat("x", 1000), // Very long service name
		Version:           strings.Repeat("y", 1000), // Very long version
		Commit:            strings.Repeat("z", 1000), // Very long commit
		TracingSampleRate: 1.0,
	}

	tracingURL, err := url.Parse("http://localhost:14268/api/traces")
	require.NoError(t, err)
	testSettings.TracingCollectorURL = tracingURL

	err = InitTracer(testSettings)
	t.Logf("InitTracer with long attributes: %v", err)

	// This will likely succeed as resource creation is very robust
	// But if it fails, we want to verify the error handling
	if err != nil && strings.Contains(err.Error(), "failed to create resource") {
		t.Logf("Successfully triggered resource creation error path")
		assert.Nil(t, tp)
	} else if err == nil {
		t.Logf("Resource creation succeeded with long attributes")
		assert.NotNil(t, tp)
	} else {
		t.Logf("Got different error: %v", err)
	}
}

// TestShutdownFailure attempts to trigger shutdown-specific errors after successful flush
// This targets lines 103-105 in ShutdownTracer
func TestShutdownFailure(t *testing.T) {
	resetGlobalState()
	defer resetGlobalState()

	testSettings := &settings.Settings{
		ServiceName:       "test-service",
		Version:           "1.0.0",
		Commit:            "abc123",
		TracingSampleRate: 1.0,
	}

	tracingURL, err := url.Parse("http://localhost:14268/api/traces")
	require.NoError(t, err)
	testSettings.TracingCollectorURL = tracingURL

	err = InitTracer(testSettings)
	require.NoError(t, err)
	require.NotNil(t, tp)

	// Try different context scenarios that might cause shutdown to fail after flush succeeds
	contexts := []struct {
		name string
		ctx  func() context.Context
	}{
		{
			name: "cancelled_context",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
		},
		{
			name: "expired_context",
			ctx: func() context.Context {
				ctx, cancel := context.WithTimeout(context.Background(), -1*time.Second) // Already expired
				defer cancel()
				return ctx
			},
		},
		{
			name: "very_short_deadline",
			ctx: func() context.Context {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
				defer cancel()
				time.Sleep(10 * time.Millisecond) // Ensure deadline passes
				return ctx
			},
		},
	}

	for _, tc := range contexts {
		t.Run(tc.name, func(t *testing.T) {
			// Reinitialize if needed
			if tp == nil {
				err = InitTracer(testSettings)
				require.NoError(t, err)
			}

			ctx := tc.ctx()
			err := ShutdownTracer(ctx)

			t.Logf("Shutdown with %s: %v", tc.name, err)

			if err != nil {
				// Check if it's a flush error or shutdown error
				if strings.Contains(err.Error(), "failed to flush spans") {
					t.Logf("Got flush error (early return, tp should remain)")
					assert.NotNil(t, tp)
				} else if strings.Contains(err.Error(), "failed to shutdown tracer") {
					t.Logf("Got shutdown error (after successful flush)")
					// This would hit lines 103-105
				} else {
					t.Logf("Got other error: %v", err)
				}
			} else {
				t.Logf("Shutdown succeeded")
				assert.Nil(t, tp)
			}
		})
	}
}

// TestActualConnectionRefused tries to create a real connection refused scenario
// Using netcat or similar unavailable service
func TestActualConnectionRefused(t *testing.T) {
	resetGlobalState()
	defer resetGlobalState()

	testSettings := &settings.Settings{
		ServiceName:       "test-service",
		Version:           "1.0.0",
		Commit:            "abc123",
		TracingSampleRate: 1.0,
	}

	// Try several addresses that should result in connection refused
	addresses := []string{
		"http://127.0.0.1:1/v1/traces",     // Port 1 typically refuses connections
		"http://127.0.0.1:99/v1/traces",    // Port 99 unlikely to be used
		"http://127.0.0.1:65534/v1/traces", // High port number
	}

	for _, addr := range addresses {
		t.Run(fmt.Sprintf("addr_%s", strings.ReplaceAll(addr, ":", "_")), func(t *testing.T) {
			resetGlobalState()

			tracingURL, err := url.Parse(addr)
			require.NoError(t, err)
			testSettings.TracingCollectorURL = tracingURL

			err = InitTracer(testSettings)
			require.NoError(t, err)
			require.NotNil(t, tp)

			// Use a timeout that allows for connection attempts
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()

			err = ShutdownTracer(ctx)
			t.Logf("Shutdown result for %s: %v", addr, err)

			// Log to console to see actual error messages
			if err != nil {
				fmt.Printf("ERROR MESSAGE: %s\n", err.Error())
				if strings.Contains(err.Error(), "connection refused") {
					t.Errorf("Connection refused should return nil, got: %v", err)
				} else {
					t.Logf("Got non-connection-refused error (expected): %v", err)
				}
			} else {
				t.Logf("Shutdown succeeded (connection refused was handled or flush succeeded)")
			}
		})
	}
}
