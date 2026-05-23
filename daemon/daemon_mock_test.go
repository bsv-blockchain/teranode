package daemon

import (
	"context"
	"fmt"
	"net/http"

	"github.com/bsv-blockchain/teranode/ulogger"
)

// mockService is a simple mock implementation of servicemanager.Service for testing.
type mockService struct {
	initErr     error
	initialised bool
	name        string
	running     bool
	startErr    error
	stopErr     error
}

// newMockService creates a new instance of mockService with the given name.
func newMockService(name string) *mockService {
	return &mockService{name: name}
}

// Init implements the servicemanager.Service interface for mockService.
func (m *mockService) Init(_ context.Context) error {
	if m.initErr != nil {
		return m.initErr
	}

	m.initialised = true

	return nil
}

// Start implements the servicemanager.Service interface for mockService.
func (m *mockService) Start(_ context.Context, ready chan<- struct{}) error { // Corrected signature
	if m.startErr != nil {
		return m.startErr
	}

	m.running = true

	close(ready) // Signal that the service is ready

	return nil
}

// Stop implements the servicemanager.Service interface for mockService.
func (m *mockService) Stop(_ context.Context) error { // Added ctx context.Context
	m.running = false
	return m.stopErr
}

// Name implements the servicemanager.Service interface for mockService.
func (m *mockService) Name() string {
	return m.name
}

// IsRunning implements the servicemanager.Service interface for mockService.
func (m *mockService) IsRunning() bool {
	return m.running
}

// IsInitialised implements the servicemanager.Service interface for mockService.
func (m *mockService) IsInitialised() bool {
	return m.initialised
}

// IsHealthy implements the servicemanager.Service interface for mockService.
func (m *mockService) IsHealthy() bool {
	return m.initialised && m.running // Simple health check
}

// Health implements the servicemanager.Service interface for mockService.
func (m *mockService) Health(_ context.Context, _ bool) (int, string, error) {
	if m.IsHealthy() {
		return http.StatusOK, "mock service is healthy", nil
	}

	return http.StatusServiceUnavailable, "mock service is not healthy", nil
}

// mockLogger is a lightweight implementation of ulogger.Logger for testing.
type mockLogger struct {
	logs []string
}

// LogLevel returns the log level of the mock logger.
func (ml *mockLogger) LogLevel() int {
	return 0
}

// SetLogLevel sets the log level for the mock logger.
func (ml *mockLogger) SetLogLevel(_ string) {

}

// Debugf logs a debug message and appends it to the log slice.
func (ml *mockLogger) Debugf(format string, args ...interface{}) {
	ml.logs = append(ml.logs, fmt.Sprintf(format, args...))
}

// Warnf logs a warning message and appends it to the logs slice.
func (ml *mockLogger) Warnf(format string, args ...interface{}) {
	ml.logs = append(ml.logs, fmt.Sprintf(format, args...))
}

// Errorf logs an error message and appends it to the log slice.
func (ml *mockLogger) Errorf(format string, args ...interface{}) {
	ml.logs = append(ml.logs, fmt.Sprintf(format, args...))
}

// Fatalf logs a fatal error message and appends it to the log slice.
func (ml *mockLogger) Fatalf(format string, args ...interface{}) {
	ml.logs = append(ml.logs, fmt.Sprintf(format, args...))
}

// New creates a new instance of mockLogger.
func (ml *mockLogger) New(_ string, _ ...ulogger.Option) ulogger.Logger {
	return ml
}

// Duplicate creates a new logger instance with the same configuration as the current one.
func (ml *mockLogger) Duplicate(_ ...ulogger.Option) ulogger.Logger {
	return ml
}

// Infof logs an informational message and appends it to the log slice.
func (ml *mockLogger) Infof(format string, args ...interface{}) {
	ml.logs = append(ml.logs, fmt.Sprintf(format, args...))
}

// WithTraceContext returns the same logger since mockLogger is a test logger.
func (ml *mockLogger) WithTraceContext(_ context.Context) ulogger.Logger {
	return ml
}
