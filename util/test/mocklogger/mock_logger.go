package mocklogger

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
)

// MockLogger is a mock implementation of ulogger.Logger for testing purposes,
// providing call tracking and thread-safe operation.
type MockLogger struct {
	mu    sync.Mutex
	calls map[string]int
}

// NewTestLogger creates a new instance of MockLogger.
func NewTestLogger() *MockLogger {
	return &MockLogger{
		calls: make(map[string]int),
	}
}

// LogLevel returns the current log level (always 0 for mock).
func (l *MockLogger) LogLevel() int {
	return 0
}

// SetLogLevel sets the log level (no-op for mock).
func (l *MockLogger) SetLogLevel(_ string) {
	// ignore
}

// New creates a new MockLogger instance with the specified service name.
func (l *MockLogger) New(_ string, _ ...ulogger.Option) ulogger.Logger {
	return &MockLogger{
		calls: make(map[string]int, 1),
	}
}

// Duplicate returns a duplicate of the logger instance.
func (l *MockLogger) Duplicate(_ ...ulogger.Option) ulogger.Logger {
	return l
}

// Debugf records a debug level log call.
func (l *MockLogger) Debugf(_ string, _ ...interface{}) {
	l.recordCall("Debugf")
}

// Infof records an info level log call.
func (l *MockLogger) Infof(_ string, _ ...interface{}) {
	l.recordCall("Infof")
}

// Warnf records a warning level log call.
func (l *MockLogger) Warnf(_ string, _ ...interface{}) {
	l.recordCall("Warnf")
}

// Errorf records an error level log call.
func (l *MockLogger) Errorf(_ string, _ ...interface{}) {
	l.recordCall("Errorf")
}

// Fatalf records a fatal level log call.
func (l *MockLogger) Fatalf(_ string, _ ...interface{}) {
	l.recordCall("Fatalf")
}

// recordCall is an internal helper that thread-safely tracks method calls.
func (l *MockLogger) recordCall(methodName string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, exists := l.calls[methodName]; !exists {
		l.calls[methodName] = 0
	}

	l.calls[methodName]++
}

// AssertNumberOfCalls is a test helper that verifies the expected number of calls to a method.
func (l *MockLogger) AssertNumberOfCalls(t *testing.T, methodName string, expectedCalls int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	actualCalls := l.calls[methodName]

	if actualCalls != expectedCalls {
		t.Errorf("Expected %v calls to %s, got %v", expectedCalls, methodName, actualCalls)
	}
}

// Reset clears all recorded method calls.
func (l *MockLogger) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = make(map[string]int)
}

// WithTraceContext returns the same logger since MockLogger is a mock that doesn't
// support structured fields.
func (l *MockLogger) WithTraceContext(_ context.Context) ulogger.Logger {
	return l
}
