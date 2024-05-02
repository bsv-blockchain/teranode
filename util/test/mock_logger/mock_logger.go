package mock_logger

import (
	"sync"
	"testing"

	"github.com/bitcoin-sv/ubsv/ulogger"
)

type MockLogger struct {
	mu    sync.Mutex
	calls map[string]int
}

// NewMockLogger creates a new instance of MockLogger.
func NewTestLogger() *MockLogger {
	return &MockLogger{
		calls: make(map[string]int),
	}
}

func (l *MockLogger) LogLevel() int {
	return 0
}

func (l *MockLogger) SetLogLevel(level string) {}

func (l *MockLogger) New(service string, options ...ulogger.Option) ulogger.Logger {
	return &MockLogger{
		calls: make(map[string]int, 1),
	}
}
func (l *MockLogger) Debugf(format string, args ...interface{}) {
	l.recordCall("Debugf")
}

func (l *MockLogger) Infof(format string, args ...interface{}) {
	l.recordCall("Infof")
}

func (l *MockLogger) Warnf(format string, args ...interface{}) {
	l.recordCall("Warnf")
}

func (l *MockLogger) Errorf(format string, args ...interface{}) {
	l.recordCall("Errorf")
}

func (l *MockLogger) Fatalf(format string, args ...interface{}) {
	l.recordCall("Fatalf")
}

func (l *MockLogger) recordCall(methodName string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, exists := l.calls[methodName]; !exists {
		l.calls[methodName] = 0
	}
	l.calls[methodName]++
}

func (l *MockLogger) AssertNumberOfCalls(t *testing.T, methodName string, expectedCalls int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	actualCalls := l.calls[methodName]
	if actualCalls != expectedCalls {
		t.Errorf("Expected %v calls to %s, got %v", expectedCalls, methodName, actualCalls)
	}
}

func (l *MockLogger) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = make(map[string]int)
}
