package ulogger

import (
	"fmt"
	"sync"
	"testing"
)

// mockTestingT implements TestingT for testing the UnifiedTestLogger
type mockTestingT struct {
	mu         sync.Mutex
	logs       []string
	helpCalled bool
}

func (m *mockTestingT) Errorf(format string, args ...interface{}) {
	// Not used in these tests
}

func (m *mockTestingT) FailNow() {
	// Not used in these tests
}

func (m *mockTestingT) Logf(format string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Format the message like testing.T.Logf does
	m.logs = append(m.logs, fmt.Sprintf(format, args...))
}

func (m *mockTestingT) Helper() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.helpCalled = true
}

func (m *mockTestingT) getLogs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string{}, m.logs...)
}

func TestUnifiedTestLogger_BasicLogging(t *testing.T) {
	mock := &mockTestingT{}
	logger := NewUnifiedTestLogger(mock, "TestExample", "")

	logger.Infof("test message")

	logs := mock.getLogs()
	if len(logs) != 1 {
		t.Fatalf("expected 1 log, got %d", len(logs))
	}

	expected := "[TestExample] INFO: test message"
	if logs[0] != expected {
		t.Errorf("expected %q, got %q", expected, logs[0])
	}
}

func TestUnifiedTestLogger_WithServiceName(t *testing.T) {
	mock := &mockTestingT{}
	logger := NewUnifiedTestLogger(mock, "TestExample", "propagation")

	logger.Infof("processing transaction")

	logs := mock.getLogs()
	if len(logs) != 1 {
		t.Fatalf("expected 1 log, got %d", len(logs))
	}

	expected := "[TestExample:propagation] INFO: processing transaction"
	if logs[0] != expected {
		t.Errorf("expected %q, got %q", expected, logs[0])
	}
}

func TestUnifiedTestLogger_New(t *testing.T) {
	mock := &mockTestingT{}
	baseLogger := NewUnifiedTestLogger(mock, "TestExample", "")

	// Create a new logger with service name (simulates what daemon does)
	serviceLogger := baseLogger.New("blockchain")

	serviceLogger.Infof("starting service")

	logs := mock.getLogs()
	if len(logs) != 1 {
		t.Fatalf("expected 1 log, got %d", len(logs))
	}

	expected := "[TestExample:blockchain] INFO: starting service"
	if logs[0] != expected {
		t.Errorf("expected %q, got %q", expected, logs[0])
	}
}

func TestUnifiedTestLogger_AllLogLevels(t *testing.T) {
	mock := &mockTestingT{}
	logger := NewUnifiedTestLogger(mock, "TestLevels", "svc")
	logger.SetLogLevel("debug") // Enable all levels

	logger.Debugf("debug message")
	logger.Infof("info message")
	logger.Warnf("warn message")
	logger.Errorf("error message")

	logs := mock.getLogs()
	if len(logs) != 4 {
		t.Fatalf("expected 4 logs, got %d", len(logs))
	}

	expected := []string{
		"[TestLevels:svc] DEBUG: debug message",
		"[TestLevels:svc] INFO: info message",
		"[TestLevels:svc] WARN: warn message",
		"[TestLevels:svc] ERROR: error message",
	}

	for i, exp := range expected {
		if logs[i] != exp {
			t.Errorf("log %d: expected %q, got %q", i, exp, logs[i])
		}
	}
}

func TestUnifiedTestLogger_LogLevelFiltering(t *testing.T) {
	mock := &mockTestingT{}
	logger := NewUnifiedTestLogger(mock, "TestFilter", "")
	logger.SetLogLevel("warn") // Only warn and above

	logger.Debugf("debug message")
	logger.Infof("info message")
	logger.Warnf("warn message")
	logger.Errorf("error message")

	logs := mock.getLogs()
	if len(logs) != 2 {
		t.Fatalf("expected 2 logs (warn + error), got %d: %v", len(logs), logs)
	}

	expected := []string{
		"[TestFilter] WARN: warn message",
		"[TestFilter] ERROR: error message",
	}

	for i, exp := range expected {
		if logs[i] != exp {
			t.Errorf("log %d: expected %q, got %q", i, exp, logs[i])
		}
	}
}

func TestUnifiedTestLogger_Shutdown(t *testing.T) {
	mock := &mockTestingT{}
	logger := NewUnifiedTestLogger(mock, "TestShutdown", "")

	logger.Infof("before shutdown")
	logger.Shutdown()
	logger.Infof("after shutdown") // Should be ignored

	logs := mock.getLogs()
	if len(logs) != 1 {
		t.Fatalf("expected 1 log (before shutdown only), got %d: %v", len(logs), logs)
	}
}

func TestUnifiedTestLogger_Duplicate(t *testing.T) {
	mock := &mockTestingT{}
	logger := NewUnifiedTestLogger(mock, "TestDup", "original")

	dup := logger.Duplicate()

	dup.Infof("from duplicate")

	logs := mock.getLogs()
	if len(logs) != 1 {
		t.Fatalf("expected 1 log, got %d", len(logs))
	}

	// Duplicate should preserve testName and serviceName
	expected := "[TestDup:original] INFO: from duplicate"
	if logs[0] != expected {
		t.Errorf("expected %q, got %q", expected, logs[0])
	}
}

func TestUnifiedTestLogger_CancelFn(t *testing.T) {
	mock := &mockTestingT{}
	cancelCalled := false
	cancelFn := func() { cancelCalled = true }

	logger := NewUnifiedTestLogger(mock, "TestCancel", "", cancelFn)
	logger.Fatalf("fatal error")

	if !cancelCalled {
		t.Error("expected cancelFn to be called on Fatalf")
	}
}
