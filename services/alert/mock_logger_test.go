// Package alert implements the Bitcoin SV alert system server and related functionality.
package alert

import (
	"github.com/bitcoin-sv/teranode/ulogger"
)

// MockLogger provides a mock implementation of ulogger.Logger for testing
type MockLogger struct {
	DebugCalls    []string
	InfoCalls     []string
	WarnCalls     []string
	ErrorCalls    []string
	FatalCalls    []string
	LogLevelValue int
}

func (m *MockLogger) Debugf(format string, args ...interface{}) {
	m.DebugCalls = append(m.DebugCalls, format)
}

func (m *MockLogger) Infof(format string, args ...interface{}) {
	m.InfoCalls = append(m.InfoCalls, format)
}

func (m *MockLogger) Warnf(format string, args ...interface{}) {
	m.WarnCalls = append(m.WarnCalls, format)
}

func (m *MockLogger) Errorf(format string, args ...interface{}) {
	m.ErrorCalls = append(m.ErrorCalls, format)
}

func (m *MockLogger) Fatalf(format string, args ...interface{}) {
	m.FatalCalls = append(m.FatalCalls, format)
}

func (m *MockLogger) LogLevel() int {
	return m.LogLevelValue
}

func (m *MockLogger) SetLogLevel(level string) {}

func (m *MockLogger) New(service string, options ...ulogger.Option) ulogger.Logger {
	return m
}

func (m *MockLogger) Duplicate(options ...ulogger.Option) ulogger.Logger {
	return m
}
