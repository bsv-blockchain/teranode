package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewService(t *testing.T) {
	tests := []struct {
		name         string
		serviceName  string
		expectedName string
	}{
		{
			name:         "creates service with given name",
			serviceName:  "TestService",
			expectedName: "TestService",
		},
		{
			name:         "creates service with empty name",
			serviceName:  "",
			expectedName: "",
		},
		{
			name:         "creates service with special characters",
			serviceName:  "Test-Service_123",
			expectedName: "Test-Service_123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := NewService(tt.serviceName)

			assert.NotNil(t, service)
			assert.Equal(t, tt.expectedName, service.name)
			assert.NotNil(t, service.logger)
		})
	}
}

func TestSampleService_Health(t *testing.T) {
	tests := []struct {
		name           string
		serviceName    string
		checkLiveness  bool
		expectedStatus int
		expectedMsg    string
		expectedError  error
	}{
		{
			name:           "health check without liveness",
			serviceName:    "TestService",
			checkLiveness:  false,
			expectedStatus: 0,
			expectedMsg:    "",
			expectedError:  nil,
		},
		{
			name:           "health check with liveness",
			serviceName:    "TestService",
			checkLiveness:  true,
			expectedStatus: 0,
			expectedMsg:    "",
			expectedError:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := NewService(tt.serviceName)
			ctx := context.Background()

			status, msg, err := service.Health(ctx, tt.checkLiveness)

			assert.Equal(t, tt.expectedStatus, status)
			assert.Equal(t, tt.expectedMsg, msg)
			assert.Equal(t, tt.expectedError, err)
		})
	}
}

func TestSampleService_Health_WithCancelledContext(t *testing.T) {
	service := NewService("TestService")
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	status, msg, err := service.Health(ctx, false)

	// Health method currently ignores context cancellation
	assert.Equal(t, 0, status)
	assert.Equal(t, "", msg)
	assert.Nil(t, err)
}

func TestSampleService_Init(t *testing.T) {
	tests := []struct {
		name        string
		serviceName string
	}{
		{
			name:        "init with normal service",
			serviceName: "TestService",
		},
		{
			name:        "init with SvcB service",
			serviceName: "SvcB",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := NewService(tt.serviceName)
			ctx := context.Background()

			err := service.Init(ctx)

			assert.Nil(t, err)
		})
	}
}

func TestSampleService_Init_WithCancelledContext(t *testing.T) {
	service := NewService("TestService")
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := service.Init(ctx)

	// Init method currently ignores context cancellation
	assert.Nil(t, err)
}

func TestSampleService_Start(t *testing.T) {
	tests := []struct {
		name        string
		serviceName string
		expectError bool
	}{
		{
			name:        "start normal service",
			serviceName: "SvcA",
			expectError: false,
		},
		{
			name:        "start SvcC service",
			serviceName: "SvcC",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := NewService(tt.serviceName)
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			ready := make(chan struct{})

			// Start service in goroutine since it blocks
			errCh := make(chan error, 1)
			go func() {
				errCh <- service.Start(ctx, ready)
			}()

			// Wait for ready signal
			select {
			case <-ready:
				// Service is ready
			case <-time.After(50 * time.Millisecond):
				t.Fatal("Service did not signal ready within timeout")
			}

			// Wait for context timeout
			select {
			case err := <-errCh:
				if tt.expectError {
					assert.Error(t, err)
				} else {
					assert.Equal(t, context.DeadlineExceeded, err)
				}
			case <-time.After(200 * time.Millisecond):
				t.Fatal("Service did not return within timeout")
			}
		})
	}
}

func TestSampleService_Start_SvcB_Error(t *testing.T) {
	service := NewService("SvcB")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ready := make(chan struct{})

	// Start service in goroutine since it blocks
	errCh := make(chan error, 1)
	go func() {
		errCh <- service.Start(ctx, ready)
	}()

	// Wait for ready signal
	select {
	case <-ready:
		// Service is ready
	case <-time.After(50 * time.Millisecond):
		t.Fatal("Service did not signal ready within timeout")
	}

	// Wait for error after 2 seconds
	select {
	case err := <-errCh:
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "SvcB start encountered an error")
	case <-time.After(4 * time.Second):
		t.Fatal("Service did not return error within timeout")
	}
}

func TestSampleService_Start_WithNilReady(t *testing.T) {
	service := NewService("SvcA")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Start service in goroutine since it blocks
	errCh := make(chan error, 1)
	go func() {
		errCh <- service.Start(ctx, nil) // nil ready channel
	}()

	// Wait for context timeout
	select {
	case err := <-errCh:
		assert.Equal(t, context.DeadlineExceeded, err)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Service did not return within timeout")
	}
}

func TestSampleService_Start_ContextCancellation(t *testing.T) {
	service := NewService("SvcA")
	ctx, cancel := context.WithCancel(context.Background())

	ready := make(chan struct{})

	// Start service in goroutine since it blocks
	errCh := make(chan error, 1)
	go func() {
		errCh <- service.Start(ctx, ready)
	}()

	// Wait for ready signal
	select {
	case <-ready:
		// Service is ready
	case <-time.After(50 * time.Millisecond):
		t.Fatal("Service did not signal ready within timeout")
	}

	// Cancel context after service is ready
	cancel()

	// Wait for cancellation error
	select {
	case err := <-errCh:
		assert.Equal(t, context.Canceled, err)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Service did not return cancellation error within timeout")
	}
}

func TestSampleService_Stop(t *testing.T) {
	tests := []struct {
		name        string
		serviceName string
	}{
		{
			name:        "stop normal service",
			serviceName: "TestService",
		},
		{
			name:        "stop SvcB service",
			serviceName: "SvcB",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := NewService(tt.serviceName)
			ctx := context.Background()

			err := service.Stop(ctx)

			assert.Nil(t, err)
		})
	}
}

func TestSampleService_Stop_WithTimeout(t *testing.T) {
	service := NewService("TestService")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	err := service.Stop(ctx)
	duration := time.Since(start)

	assert.Nil(t, err)
	// Should take approximately 1 second (the simulated delay)
	assert.True(t, duration >= 900*time.Millisecond)
	assert.True(t, duration < 1500*time.Millisecond)
}

func TestSampleService_Stop_WithCancelledContext(t *testing.T) {
	service := NewService("TestService")
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := service.Stop(ctx)

	assert.Equal(t, context.Canceled, err)
}

func TestSampleService_Stop_WithShortTimeout(t *testing.T) {
	service := NewService("TestService")
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := service.Stop(ctx)

	// Should return context deadline exceeded since stop takes 1 second
	assert.Equal(t, context.DeadlineExceeded, err)
}

// Integration test for the service lifecycle
func TestSampleService_Lifecycle(t *testing.T) {
	service := NewService("TestLifecycle")
	ctx := context.Background()

	// Test initialization
	err := service.Init(ctx)
	require.Nil(t, err)

	// Test health check
	status, msg, err := service.Health(ctx, true)
	assert.Equal(t, 0, status)
	assert.Equal(t, "", msg)
	assert.Nil(t, err)

	// Test start and stop with quick cancellation
	startCtx, startCancel := context.WithCancel(ctx)
	ready := make(chan struct{})

	// Start service
	errCh := make(chan error, 1)
	go func() {
		errCh <- service.Start(startCtx, ready)
	}()

	// Wait for ready signal
	select {
	case <-ready:
		// Service is ready
	case <-time.After(50 * time.Millisecond):
		t.Fatal("Service did not signal ready")
	}

	// Cancel to stop service
	startCancel()

	// Wait for service to stop
	select {
	case err := <-errCh:
		assert.Equal(t, context.Canceled, err)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Service did not stop")
	}

	// Test explicit stop
	stopCtx, stopCancel := context.WithTimeout(ctx, 2*time.Second)
	defer stopCancel()

	err = service.Stop(stopCtx)
	assert.Nil(t, err)
}
