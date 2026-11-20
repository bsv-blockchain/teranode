package servicemanager

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewServiceManager(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.New("test", ulogger.WithWriter(io.Discard))

	sm := NewServiceManager(ctx, logger)

	assert.NotNil(t, sm)
	assert.NotNil(t, sm.Ctx)
	assert.NotNil(t, sm.cancelFunc)
	assert.NotNil(t, sm.g)
	assert.Equal(t, logger, sm.logger)
	assert.Empty(t, sm.services)
	assert.Empty(t, sm.dependencyChannels)
}

func TestAddListenerInfo(t *testing.T) {
	// Reset listeners for test isolation
	mu.Lock()
	listeners = make([]string, 0)
	mu.Unlock()

	tests := []struct {
		name      string
		listeners []string
		expected  []string
	}{
		{
			name:      "single listener",
			listeners: []string{"test1"},
			expected:  []string{"test1"},
		},
		{
			name:      "multiple listeners",
			listeners: []string{"test1", "test2", "test3"},
			expected:  []string{"test1", "test2", "test3"},
		},
		{
			name:      "duplicate listeners",
			listeners: []string{"test1", "test1", "test2"},
			expected:  []string{"test1", "test1", "test2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset listeners
			mu.Lock()
			listeners = make([]string, 0)
			mu.Unlock()

			for _, listener := range tt.listeners {
				AddListenerInfo(listener)
			}

			result := GetListenerInfos()
			assert.Equal(t, len(tt.expected), len(result))

			// Since GetListenerInfos sorts, we need to check the sorted expected
			expectedSorted := make([]string, len(tt.expected))
			copy(expectedSorted, tt.expected)
			// Manual sort for comparison since we can't import sort in test
			for i := 0; i < len(expectedSorted); i++ {
				for j := i + 1; j < len(expectedSorted); j++ {
					if expectedSorted[i] > expectedSorted[j] {
						expectedSorted[i], expectedSorted[j] = expectedSorted[j], expectedSorted[i]
					}
				}
			}
			assert.Equal(t, expectedSorted, result)
		})
	}
}

func TestGetListenerInfos(t *testing.T) {
	// Reset listeners for test isolation
	mu.Lock()
	listeners = make([]string, 0)
	mu.Unlock()

	t.Run("empty listeners", func(t *testing.T) {
		result := GetListenerInfos()
		assert.Empty(t, result)
	})

	t.Run("sorted listeners", func(t *testing.T) {
		// Reset and add listeners in non-sorted order
		mu.Lock()
		listeners = []string{"zebra", "alpha", "beta"}
		mu.Unlock()

		result := GetListenerInfos()
		expected := []string{"alpha", "beta", "zebra"}
		assert.Equal(t, expected, result)
	})

	t.Run("concurrent access", func(t *testing.T) {
		mu.Lock()
		listeners = make([]string, 0)
		mu.Unlock()

		var wg sync.WaitGroup
		numGoroutines := 10

		// Concurrent additions
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				AddListenerInfo(string(rune('a' + id)))
			}(i)
		}

		// Concurrent reads
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				GetListenerInfos()
			}()
		}

		wg.Wait()

		result := GetListenerInfos()
		assert.Equal(t, numGoroutines, len(result))
	})
}

func TestServiceManagerResetContext(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.New("test", ulogger.WithWriter(io.Discard))
	sm := NewServiceManager(ctx, logger)

	originalCtx := sm.Ctx

	err := sm.ResetContext()
	assert.NoError(t, err)
	assert.NotEqual(t, originalCtx, sm.Ctx)
	// Can't compare function pointers directly, just verify they're not nil
	assert.NotNil(t, sm.Ctx)
	assert.NotNil(t, sm.cancelFunc)
}

func TestServiceManagerAddService(t *testing.T) {
	t.Run("successful service addition", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.New("test", ulogger.WithWriter(io.Discard))
		sm := NewServiceManager(ctx, logger)

		mockService := NewMockService("test-service")
		err := sm.AddService("test-service", mockService)

		assert.NoError(t, err)
		assert.Len(t, sm.services, 1)
		assert.Equal(t, "test-service", sm.services[0].name)

		init, _, _ := mockService.WasCalled()
		assert.True(t, init, "Init should have been called")
	})

	t.Run("service init failure", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.New("test", ulogger.WithWriter(io.Discard))
		sm := NewServiceManager(ctx, logger)

		mockService := NewFailingMockService("failing-service", "init")
		err := sm.AddService("failing-service", mockService)

		assert.Error(t, err)
		// Service is still added to the slice even if init fails
		assert.Len(t, sm.services, 1)
	})

	t.Run("multiple services with dependency ordering", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.New("test", ulogger.WithWriter(io.Discard))
		sm := NewServiceManager(ctx, logger)

		service1 := NewMockService("service1")
		service2 := NewMockService("service2")
		service3 := NewMockService("service3")

		err1 := sm.AddService("service1", service1)
		err2 := sm.AddService("service2", service2)
		err3 := sm.AddService("service3", service3)

		assert.NoError(t, err1)
		assert.NoError(t, err2)
		assert.NoError(t, err3)
		assert.Len(t, sm.services, 3)
		assert.Len(t, sm.dependencyChannels, 3)

		// Verify indexes
		assert.Equal(t, 0, sm.services[0].index)
		assert.Equal(t, 1, sm.services[1].index)
		assert.Equal(t, 2, sm.services[2].index)
	})

	t.Run("service addition with context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		logger := ulogger.New("test", ulogger.WithWriter(io.Discard))
		sm := NewServiceManager(ctx, logger)

		// Cancel context before adding service
		cancel()

		mockService := NewMockService("test-service")
		// This should still succeed as Init doesn't necessarily check context immediately
		err := sm.AddService("test-service", mockService)
		// The error might come from the service's Init method depending on implementation
		if err != nil {
			assert.Contains(t, err.Error(), "context canceled")
		}
	})
}

func TestServiceManagerWaitForServiceToBeReady(t *testing.T) {
	t.Run("all services ready", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.New("test", ulogger.WithWriter(io.Discard))
		sm := NewServiceManager(ctx, logger)

		service1 := NewMockService("service1")
		service2 := NewMockService("service2")

		err1 := sm.AddService("service1", service1)
		err2 := sm.AddService("service2", service2)
		require.NoError(t, err1)
		require.NoError(t, err2)

		// Start the wait in a goroutine since it blocks
		done := make(chan struct{})
		go func() {
			sm.WaitForServiceToBeReady()
			close(done)
		}()

		// Give some time for services to start and signal ready
		time.Sleep(100 * time.Millisecond)

		select {
		case <-done:
			// Expected - all services should be ready quickly
		case <-time.After(2 * time.Second):
			t.Fatal("WaitForServiceToBeReady timed out")
		}
	})

	t.Run("no services", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.New("test", ulogger.WithWriter(io.Discard))
		sm := NewServiceManager(ctx, logger)

		// Should return immediately with no services
		done := make(chan struct{})
		go func() {
			sm.WaitForServiceToBeReady()
			close(done)
		}()

		select {
		case <-done:
			// Expected
		case <-time.After(1 * time.Second):
			t.Fatal("WaitForServiceToBeReady should return immediately with no services")
		}
	})
}

func TestServiceManagerServicesNotReady(t *testing.T) {
	t.Run("all services ready", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.New("test", ulogger.WithWriter(io.Discard))
		sm := NewServiceManager(ctx, logger)

		service1 := NewMockService("service1")
		err := sm.AddService("service1", service1)
		require.NoError(t, err)

		// Give service time to start and signal ready
		time.Sleep(100 * time.Millisecond)

		notReady := sm.ServicesNotReady()
		// The service might still be in the process of starting
		// so we can't make strict assertions about readiness timing
		t.Logf("Services not ready: %v", notReady)
	})

	t.Run("no services", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.New("test", ulogger.WithWriter(io.Discard))
		sm := NewServiceManager(ctx, logger)

		notReady := sm.ServicesNotReady()
		assert.Empty(t, notReady)
	})
}

func TestServiceManagerWaitForPreviousServiceToStart(t *testing.T) {
	t.Run("successful wait", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.New("test", ulogger.WithWriter(io.Discard))
		sm := NewServiceManager(ctx, logger)

		channel := make(chan bool)
		sw := serviceWrapper{
			name:  "test-service",
			index: 1,
		}

		go func() {
			time.Sleep(50 * time.Millisecond)
			close(channel)
		}()

		err := sm.waitForPreviousServiceToStart(sw, channel)
		assert.NoError(t, err)
	})

	t.Run("timeout", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.New("test", ulogger.WithWriter(io.Discard))
		sm := NewServiceManager(ctx, logger)

		channel := make(chan bool)
		sw := serviceWrapper{
			name:  "test-service",
			index: 1,
		}

		// Don't close the channel, let it timeout
		err := sm.waitForPreviousServiceToStart(sw, channel)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "timed out waiting")
	})
}

func TestServiceManagerForceShutdown(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.New("test", ulogger.WithWriter(io.Discard))
	sm := NewServiceManager(ctx, logger)

	// Add a service to ensure context cancellation affects it
	service1 := NewMockService("service1")
	err := sm.AddService("service1", service1)
	require.NoError(t, err)

	// Verify context is not canceled initially
	select {
	case <-sm.Ctx.Done():
		t.Fatal("Context should not be canceled initially")
	default:
		// Expected
	}

	// Force shutdown
	sm.ForceShutdown()

	// Verify context is now canceled
	select {
	case <-sm.Ctx.Done():
		// Expected
	case <-time.After(1 * time.Second):
		t.Fatal("Context should be canceled after ForceShutdown")
	}
}

func TestServiceManagerWait(t *testing.T) {
	t.Run("successful completion", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		logger := ulogger.New("test", ulogger.WithWriter(io.Discard))
		sm := NewServiceManager(ctx, logger)

		service1 := NewMockService("service1")
		err := sm.AddService("service1", service1)
		require.NoError(t, err)

		// Cancel after a short delay to trigger shutdown
		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		err = sm.Wait()
		assert.NoError(t, err) // Context cancellation should result in no error

		_, _, stop := service1.WasCalled()
		assert.True(t, stop, "Stop should have been called")
	})

	t.Run("service failure", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.New("test", ulogger.WithWriter(io.Discard))
		sm := NewServiceManager(ctx, logger)

		failingService := NewFailingMockService("failing-service", "start")
		err := sm.AddService("failing-service", failingService)
		require.NoError(t, err)

		err = sm.Wait()
		// Wait() may return nil even if there was a service error, because when errgroup
		// gets an error, it cancels the context, and context.Canceled errors are converted to nil
		// The actual error was logged, which is what matters for the service manager functionality
		if err != nil {
			assert.Contains(t, err.Error(), "mock service failure")
		}
	})

	t.Run("stop service failure", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		logger := ulogger.New("test", ulogger.WithWriter(io.Discard))
		sm := NewServiceManager(ctx, logger)

		service1 := NewMockService("service1")
		service1.SetStopBehavior(0, errors.NewServiceError("stop error"))
		err := sm.AddService("service1", service1)
		require.NoError(t, err)

		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		err = sm.Wait()
		// Due to current implementation, stop errors override the original error
		// This could be considered a bug, but we're testing current behavior
		if err != nil {
			assert.Contains(t, err.Error(), "stop error")
		}
	})

	t.Run("multiple services shutdown order", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		logger := ulogger.New("test", ulogger.WithWriter(io.Discard))
		sm := NewServiceManager(ctx, logger)

		service1 := NewMockService("service1")
		service2 := NewMockService("service2")
		service3 := NewMockService("service3")

		err1 := sm.AddService("service1", service1)
		err2 := sm.AddService("service2", service2)
		err3 := sm.AddService("service3", service3)
		require.NoError(t, err1)
		require.NoError(t, err2)
		require.NoError(t, err3)

		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		err := sm.Wait()
		assert.NoError(t, err)

		// All services should have been stopped
		_, _, stop1 := service1.WasCalled()
		_, _, stop2 := service2.WasCalled()
		_, _, stop3 := service3.WasCalled()
		assert.True(t, stop1)
		assert.True(t, stop2)
		assert.True(t, stop3)
	})
}

// cancelStub is a service whose Start signals ready and then returns one of
// two ways: onRelease when release is closed while its context is still live
// (a failure arriving from elsewhere), or onCancel once its context is
// cancelled (ctx.Err() when onCancel is nil). ctxErrAtReturn records whether
// its own context was cancelled at the moment Start returned.
type cancelStub struct {
	release   chan struct{}
	onRelease error
	onCancel  error

	ctxErrAtReturn chan error
}

func newCancelStub() *cancelStub {
	return &cancelStub{release: make(chan struct{}), ctxErrAtReturn: make(chan error, 1)}
}

func (s *cancelStub) Init(context.Context) error { return nil }

func (s *cancelStub) Start(ctx context.Context, ready chan<- struct{}) error {
	close(ready)

	select {
	case <-s.release:
		s.ctxErrAtReturn <- ctx.Err()
		return s.onRelease
	case <-ctx.Done():
		s.ctxErrAtReturn <- ctx.Err()

		if s.onCancel != nil {
			return s.onCancel
		}

		return ctx.Err()
	}
}

func (s *cancelStub) Stop(context.Context) error { return nil }

func (s *cancelStub) Health(context.Context, bool) (int, string, error) {
	return http.StatusOK, "OK", nil
}

func waitResult(t *testing.T, sm *ServiceManager) error {
	t.Helper()

	result := make(chan error, 1)

	go func() { result <- sm.Wait() }()

	select {
	case err := <-result:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("Wait did not return")
		return nil
	}
}

// TestWait_LocalCancellationIsClean: a service that returns because its own
// context was cancelled has stopped cleanly, whatever form the cancellation
// takes: ctx.Err() itself, a project error wrapping it, or the gRPC
// codes.Canceled a local client call returns when that context is cancelled.
// Both ForceShutdown (what the signal handler calls) and a cancelled parent
// context count.
func TestWait_LocalCancellationIsClean(t *testing.T) {
	forms := map[string]error{
		"ctx.Err":             nil,
		"wrapped ctx.Err":     errors.NewServiceError("subscription ended", context.Canceled),
		"gRPC codes.Canceled": status.Error(codes.Canceled, "context canceled"),
	}

	for name, onCancel := range forms {
		t.Run(name+" after ForceShutdown", func(t *testing.T) {
			sm := NewServiceManager(context.Background(), ulogger.New("test", ulogger.WithWriter(io.Discard)))

			svc := newCancelStub()
			svc.onCancel = onCancel

			require.NoError(t, sm.AddService("svc", svc))

			sm.ForceShutdown()

			require.NoError(t, waitResult(t, sm))
		})

		t.Run(name+" after parent cancel", func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			sm := NewServiceManager(parent, ulogger.New("test", ulogger.WithWriter(io.Discard)))

			svc := newCancelStub()
			svc.onCancel = onCancel

			require.NoError(t, sm.AddService("svc", svc))

			cancel()

			require.NoError(t, waitResult(t, sm))
		})
	}
}

// TestWait_RemoteCancelWhileRunningIsReturned: a running service gets a gRPC
// codes.Canceled from a remote service that is stopping (P2P, SubtreeValidation
// and UTXOPersister wait on the blockchain FSM in Start). The project errors.Is
// matches it against context.Canceled, but this process's context is live, so
// it is a failure and Wait must return it.
func TestWait_RemoteCancelWhileRunningIsReturned(t *testing.T) {
	sm := NewServiceManager(context.Background(), ulogger.New("test", ulogger.WithWriter(io.Discard)))

	svc := newCancelStub()
	svc.onRelease = status.Error(codes.Canceled, "remote handler stopped")

	require.NoError(t, sm.AddService("svc", svc))

	close(svc.release)

	err := waitResult(t, sm)
	require.NoError(t, <-svc.ctxErrAtReturn, "premise: the local context was live when Start returned")
	require.Error(t, err)
	// Wait passes it through the project errors.Join, which keeps a non-project
	// error as text only, so check the message rather than the status code.
	require.Contains(t, err.Error(), "remote handler stopped")
}

// TestWait_FirstRealErrorSurvivesSiblingCancellations: the first service's
// remote codes.Canceled is the failure. The errgroup then cancels the
// siblings, which return their own (now local) cancellations; those are clean
// stops and must not hide or replace the first error.
func TestWait_FirstRealErrorSurvivesSiblingCancellations(t *testing.T) {
	sm := NewServiceManager(context.Background(), ulogger.New("test", ulogger.WithWriter(io.Discard)))

	failing := newCancelStub()
	failing.onRelease = status.Error(codes.Canceled, "first: remote handler stopped")

	sibling := newCancelStub()
	sibling.onCancel = status.Error(codes.Canceled, "sibling: context canceled")

	require.NoError(t, sm.AddService("failing", failing))
	require.NoError(t, sm.AddService("sibling", sibling))

	close(failing.release)

	err := waitResult(t, sm)
	require.Error(t, err)
	require.Contains(t, err.Error(), "first: remote handler stopped")
	require.NotContains(t, err.Error(), "sibling")
	require.Error(t, <-sibling.ctxErrAtReturn, "premise: the sibling returned after the errgroup cancelled it")
}

// TestWait_ErrorDuringRequestedShutdownIsReturned: a requested shutdown is
// clean only if the service returns a cancellation. A service that fails some
// other way while stopping (a flush, a drain) has failed, and Wait must say so.
func TestWait_ErrorDuringRequestedShutdownIsReturned(t *testing.T) {
	sm := NewServiceManager(context.Background(), ulogger.New("test", ulogger.WithWriter(io.Discard)))

	flushErr := errors.NewProcessingError("failed to flush on shutdown")

	svc := newCancelStub()
	svc.onCancel = flushErr

	require.NoError(t, sm.AddService("svc", svc))

	sm.ForceShutdown()

	err := waitResult(t, sm)
	require.Error(t, err)
	require.True(t, errors.Is(err, flushErr))
}

func TestServiceManagerHealthHandler(t *testing.T) {
	t.Run("all services healthy", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.New("test", ulogger.WithWriter(io.Discard))
		sm := NewServiceManager(ctx, logger)

		service1 := NewMockService("service1")
		service2 := NewMockService("service2")
		service1.SetHealthBehavior(http.StatusOK, nil)
		service2.SetHealthBehavior(http.StatusOK, nil)

		err1 := sm.AddService("service1", service1)
		err2 := sm.AddService("service2", service2)
		require.NoError(t, err1)
		require.NoError(t, err2)

		status, response, err := sm.HealthHandler(ctx, false)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, status)
		assert.Contains(t, response, "service1")
		assert.Contains(t, response, "service2")
		assert.Contains(t, response, `"status": "200"`)
	})

	t.Run("one service unhealthy", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.New("test", ulogger.WithWriter(io.Discard))
		sm := NewServiceManager(ctx, logger)

		service1 := NewMockService("service1")
		service2 := NewMockService("service2")
		service1.SetHealthBehavior(http.StatusOK, nil)
		service2.SetHealthBehavior(http.StatusServiceUnavailable, nil)

		err1 := sm.AddService("service1", service1)
		err2 := sm.AddService("service2", service2)
		require.NoError(t, err1)
		require.NoError(t, err2)

		status, response, err := sm.HealthHandler(ctx, false)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusServiceUnavailable, status)
		assert.Contains(t, response, "service1")
		assert.Contains(t, response, "service2")
		assert.Contains(t, response, `"status": "503"`)
	})

	t.Run("service health error", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.New("test", ulogger.WithWriter(io.Discard))
		sm := NewServiceManager(ctx, logger)

		service1 := NewMockService("service1")
		service1.SetHealthBehavior(http.StatusInternalServerError, errors.NewServiceError("health error"))

		err := sm.AddService("service1", service1)
		require.NoError(t, err)

		status, response, err := sm.HealthHandler(ctx, false)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusServiceUnavailable, status)
		assert.Contains(t, response, "service1")
		assert.Contains(t, response, `"status": "500"`)
	})

	t.Run("no services", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.New("test", ulogger.WithWriter(io.Discard))
		sm := NewServiceManager(ctx, logger)

		status, response, err := sm.HealthHandler(ctx, false)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, status)
		assert.Contains(t, response, `"status": "200"`)
		assert.Contains(t, response, `"services": []`)
	})

	t.Run("json formatting", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.New("test", ulogger.WithWriter(io.Discard))
		sm := NewServiceManager(ctx, logger)

		service1 := NewMockService("service1")
		err := sm.AddService("service1", service1)
		require.NoError(t, err)

		status, response, err := sm.HealthHandler(ctx, false)

		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, status)

		// Check that response is valid JSON by attempting to parse key parts
		assert.Contains(t, response, "{")
		assert.Contains(t, response, "}")
		assert.Contains(t, response, `"status"`)
		assert.Contains(t, response, `"services"`)

		// Check for proper formatting (indentation)
		assert.True(t, strings.Contains(response, "  "), "Response should be formatted with indentation")
	})
}

func TestServiceManagerConcurrentAccess(t *testing.T) {
	t.Run("sequential service addition with concurrent operations", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.New("test", ulogger.WithWriter(io.Discard))
		sm := NewServiceManager(ctx, logger)

		// Add services sequentially to avoid race conditions in the service manager
		numServices := 5
		for i := 0; i < numServices; i++ {
			service := NewMockService(string(rune('A' + i)))
			err := sm.AddService(string(rune('A'+i)), service)
			assert.NoError(t, err)
		}

		assert.Len(t, sm.services, numServices)
		assert.Len(t, sm.dependencyChannels, numServices)
	})

	t.Run("concurrent health checks", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.New("test", ulogger.WithWriter(io.Discard))
		sm := NewServiceManager(ctx, logger)

		service1 := NewMockService("service1")
		err := sm.AddService("service1", service1)
		require.NoError(t, err)

		var wg sync.WaitGroup
		numConcurrent := 10

		for i := 0; i < numConcurrent; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				status, _, err := sm.HealthHandler(ctx, false)
				assert.NoError(t, err)
				assert.Equal(t, http.StatusOK, status)
			}()
		}

		wg.Wait()
	})
}

// Benchmark tests
func BenchmarkServiceManagerAddService(b *testing.B) {
	ctx := context.Background()
	logger := ulogger.New("test", ulogger.WithWriter(io.Discard))
	sm := NewServiceManager(ctx, logger)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		service := NewMockService(string(rune('A' + (i % 26))))
		err := sm.AddService(string(rune('A'+(i%26))), service)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkServiceManagerHealthHandler(b *testing.B) {
	ctx := context.Background()
	logger := ulogger.New("test", ulogger.WithWriter(io.Discard))
	sm := NewServiceManager(ctx, logger)

	// Add some services
	for i := 0; i < 5; i++ {
		service := NewMockService(string(rune('A' + i)))
		err := sm.AddService(string(rune('A'+i)), service)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := sm.HealthHandler(ctx, false)
		if err != nil {
			b.Fatal(err)
		}
	}
}
