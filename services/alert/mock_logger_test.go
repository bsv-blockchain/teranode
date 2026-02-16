// Package alert implements the Bitcoin SV alert system server and related functionality.
package alert

import (
	"context"

	"github.com/bsv-blockchain/teranode/ulogger"
)

// MockLogger provides a mock implementation of ulogger.Logger for testing.
// It records all logging calls made to it, allowing tests to verify that the correct
// logging methods were called with the expected format strings. This is essential
// for testing Logger wrapper functionality without producing actual log output.
type MockLogger struct {
	// DebugCalls records all format strings passed to Debugf calls
	DebugCalls []string
	// InfoCalls records all format strings passed to Infof calls
	InfoCalls []string
	// WarnCalls records all format strings passed to Warnf calls
	WarnCalls []string
	// ErrorCalls records all format strings passed to Errorf calls
	ErrorCalls []string
	// FatalCalls records all format strings passed to Fatalf calls
	FatalCalls []string
	// LogLevelValue stores the configured log level for testing LogLevel() method
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

func (m *MockLogger) WithTraceContext(_ context.Context) ulogger.Logger {
	return m
}
