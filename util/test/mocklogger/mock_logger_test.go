package mocklogger

import (
	"sync"
	"testing"

	"github.com/bitcoin-sv/teranode/ulogger"
)

func TestNewTestLogger(t *testing.T) {
	logger := NewTestLogger()

	if logger == nil {
		t.Fatal("Expected NewTestLogger to return non-nil logger")
	}

	if logger.calls == nil {
		t.Error("Expected calls map to be initialized")
	}

	if len(logger.calls) != 0 {
		t.Error("Expected new logger to have empty calls map")
	}
}

func TestLogLevel(t *testing.T) {
	logger := NewTestLogger()
	level := logger.LogLevel()

	if level != 0 {
		t.Errorf("Expected LogLevel to return 0, got %d", level)
	}
}

func TestSetLogLevel(t *testing.T) {
	logger := NewTestLogger()

	// SetLogLevel should be a no-op, just verify it doesn't panic
	logger.SetLogLevel("debug")
	logger.SetLogLevel("info")
	logger.SetLogLevel("warn")
	logger.SetLogLevel("error")
}

func TestNew(t *testing.T) {
	logger := NewTestLogger()
	newLogger := logger.New("test-service")

	if newLogger == nil {
		t.Fatal("Expected New to return non-nil logger")
	}

	// Should return a new instance, not the same one
	if newLogger == logger {
		t.Error("Expected New to return a new instance")
	}

	mockLogger, ok := newLogger.(*MockLogger)
	if !ok {
		t.Fatal("Expected New to return *MockLogger")
	}

	if mockLogger.calls == nil {
		t.Error("Expected new logger to have initialized calls map")
	}
}

func TestNewWithOptions(t *testing.T) {
	logger := NewTestLogger()

	// Test with various option types (they should be ignored)
	newLogger := logger.New("test-service", ulogger.WithLevel("debug"))

	if newLogger == nil {
		t.Fatal("Expected New to return non-nil logger")
	}
}

func TestDuplicate(t *testing.T) {
	logger := NewTestLogger()
	duplicate := logger.Duplicate()

	if duplicate != logger {
		t.Error("Expected Duplicate to return the same instance")
	}
}

func TestDuplicateWithOptions(t *testing.T) {
	logger := NewTestLogger()

	// Test with options (they should be ignored)
	duplicate := logger.Duplicate(ulogger.WithLevel("debug"))

	if duplicate != logger {
		t.Error("Expected Duplicate to return the same instance even with options")
	}
}

func TestLoggingMethods(t *testing.T) {
	logger := NewTestLogger()

	tests := []struct {
		name    string
		logFunc func()
		method  string
	}{
		{"Debugf", func() { logger.Debugf("test message %s %s", "arg1", "arg2") }, "Debugf"},
		{"Infof", func() { logger.Infof("test message %s %s", "arg1", "arg2") }, "Infof"},
		{"Warnf", func() { logger.Warnf("test message %s %s", "arg1", "arg2") }, "Warnf"},
		{"Errorf", func() { logger.Errorf("test message %s %s", "arg1", "arg2") }, "Errorf"},
		{"Fatalf", func() { logger.Fatalf("test message %s %s", "arg1", "arg2") }, "Fatalf"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger.Reset()

			// Call the logging method
			tt.logFunc()

			// Verify the call was recorded
			logger.AssertNumberOfCalls(t, tt.method, 1)
		})
	}
}

func TestMultipleCallsToSameMethod(t *testing.T) {
	logger := NewTestLogger()

	// Call Infof multiple times
	logger.Infof("message 1")
	logger.Infof("message 2")
	logger.Infof("message 3")

	logger.AssertNumberOfCalls(t, "Infof", 3)
}

func TestMultipleMethodCalls(t *testing.T) {
	logger := NewTestLogger()

	// Call different methods
	logger.Debugf("debug message")
	logger.Infof("info message")
	logger.Infof("another info message")
	logger.Errorf("error message")

	logger.AssertNumberOfCalls(t, "Debugf", 1)
	logger.AssertNumberOfCalls(t, "Infof", 2)
	logger.AssertNumberOfCalls(t, "Warnf", 0)
	logger.AssertNumberOfCalls(t, "Errorf", 1)
	logger.AssertNumberOfCalls(t, "Fatalf", 0)
}

func TestAssertNumberOfCallsFailure(t *testing.T) {
	logger := NewTestLogger()

	// Call Infof once
	logger.Infof("test message")

	// Manually check the call count without using AssertNumberOfCalls
	// to verify the failure case would be detected
	logger.mu.Lock()
	actualCalls := logger.calls["Infof"]
	logger.mu.Unlock()

	if actualCalls != 1 {
		t.Errorf("Expected 1 call to Infof, got %d", actualCalls)
	}

	// Test that AssertNumberOfCalls with correct count passes
	logger.AssertNumberOfCalls(t, "Infof", 1)
}

func TestReset(t *testing.T) {
	logger := NewTestLogger()

	// Make some calls
	logger.Debugf("debug")
	logger.Infof("info")
	logger.Errorf("error")

	// Verify calls were recorded
	logger.AssertNumberOfCalls(t, "Debugf", 1)
	logger.AssertNumberOfCalls(t, "Infof", 1)
	logger.AssertNumberOfCalls(t, "Errorf", 1)

	// Reset and verify all calls are cleared
	logger.Reset()

	logger.AssertNumberOfCalls(t, "Debugf", 0)
	logger.AssertNumberOfCalls(t, "Infof", 0)
	logger.AssertNumberOfCalls(t, "Errorf", 0)
}

func TestConcurrentAccess(t *testing.T) {
	logger := NewTestLogger()
	numGoroutines := 100
	callsPerGoroutine := 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Launch multiple goroutines that call logging methods concurrently
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < callsPerGoroutine; j++ {
				logger.Infof("concurrent call")
			}
		}()
	}

	wg.Wait()

	expectedCalls := numGoroutines * callsPerGoroutine
	logger.AssertNumberOfCalls(t, "Infof", expectedCalls)
}

func TestConcurrentResetAndCalls(t *testing.T) {
	logger := NewTestLogger()

	var wg sync.WaitGroup
	wg.Add(2)

	// Goroutine 1: Make calls
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			logger.Infof("test call")
		}
	}()

	// Goroutine 2: Reset occasionally
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			logger.Reset()
		}
	}()

	wg.Wait()

	// Just verify no race conditions occurred (test passes if no panic)
}
