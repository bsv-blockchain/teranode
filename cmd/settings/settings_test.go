package settings

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
)

// Define a custom logger to capture output for testing.
type BufferLogger struct {
	buffer *bytes.Buffer
}

// SetLogLevel is a no-op for this logger, as we are not testing log levels.
func (l *BufferLogger) SetLogLevel(_ string) {
	// No-op for this logger, as we are not testing log levels.
}

// Warnf is used for warning-level logging, but here we just write to the buffer.
func (l *BufferLogger) Warnf(format string, args ...interface{}) {
	l.buffer.WriteString(fmt.Sprintf(format, args...))
}

// New creates a new logger instance with the specified service name and options.
func (l *BufferLogger) New(_ string, _ ...ulogger.Option) ulogger.Logger {
	return &BufferLogger{}
}

// Infof is used for informational logging, but here we just write to the buffer.
func (l *BufferLogger) Infof(format string, args ...interface{}) {
	l.buffer.WriteString(fmt.Sprintf(format, args...))
}

// Errorf is used for error-level logging, but here we just write to the buffer.
func (l *BufferLogger) Errorf(format string, args ...interface{}) {
	l.buffer.WriteString(fmt.Sprintf(format, args...))
}

// Debugf is used for debug-level logging, but here we just write to the buffer.
func (l *BufferLogger) Debugf(format string, args ...interface{}) {
	l.buffer.WriteString(fmt.Sprintf(format, args...))
}

// Fatalf is typically used for fatal errors, but here we just write to the buffer.
func (l *BufferLogger) Fatalf(format string, args ...interface{}) {
	l.buffer.WriteString(fmt.Sprintf(format, args...))
}

// Duplicate returns a new instance of the logger with the same configuration.
func (l *BufferLogger) Duplicate(_ ...ulogger.Option) ulogger.Logger {
	return l // Return the same logger for simplicity in testing.
}

// LogLevel returns the log level of the logger.
func (l *BufferLogger) LogLevel() int {
	return 0 // Return a default log level for testing purposes.
}

// WithTraceContext returns the same logger since BufferLogger is a test logger.
func (l *BufferLogger) WithTraceContext(_ context.Context) ulogger.Logger {
	return l
}

// TestPrintSettings tests the PrintSettings function to ensure it correctly formats and outputs the settings, version, and commit information.
func TestPrintSettings(t *testing.T) {
	appSettings := &settings.Settings{
		Context:            "TestContext",
		ServiceName:        "TestService",
		ClientName:         "TestClient",
		DataFolder:         "TestDataFolder",
		ServerCertFile:     "TestCertFile",
		ServerKeyFile:      "TestKeyFile",
		ProfilerAddr:       "TestProfilerAddr",
		StatsPrefix:        "TestStatsPrefix",
		PrometheusEndpoint: "TestPrometheusEndpoint",
	}
	version := "11.11.11"
	commit := "abc123"

	// Use the custom logger to capture output.
	var output bytes.Buffer
	mockLogger := &BufferLogger{buffer: &output}

	PrintSettings(mockLogger, appSettings, version, commit)

	assert.NotZero(t, output.Len(), "Expected output, but got none")
	assert.Contains(t, output.String(), version, "Expected version %s in output, but it was missing", version)
	assert.Contains(t, output.String(), commit, "Expected commit %s in output, but it was missing", commit)
	assert.Contains(t, output.String(), "TestService", "Expected service name 'TestService' in output, but it was missing")
	assert.Contains(t, output.String(), "TestContext", "Expected context 'TestContext' in output, but it was missing")
	assert.Contains(t, output.String(), "TestClient", "Expected client name 'TestClient' in output, but it was missing")
	assert.Contains(t, output.String(), "TestDataFolder", "Expected data folder 'TestDataFolder' in output, but it was missing")
	assert.Contains(t, output.String(), "TestCertFile", "Expected server cert file 'TestCertFile' in output, but it was missing")
	assert.Contains(t, output.String(), "TestKeyFile", "Expected server key file 'TestKeyFile' in output, but it was missing")
	assert.Contains(t, output.String(), "TestProfilerAddr", "Expected profiler address 'TestProfilerAddr' in output, but it was missing")
	assert.Contains(t, output.String(), "TestStatsPrefix", "Expected stats prefix 'TestStatsPrefix' in output, but it was missing")
	assert.Contains(t, output.String(),
		"TestPrometheusEndpoint", "Expected Prometheus endpoint 'TestPrometheusEndpoint' in output, but it was missing",
	)
}
