package ulogger

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// UnifiedTestLogger implements the Logger interface and routes all output through
// testing.T's Logf method. It provides consistent log formatting with service name
// prefixes to identify the source component of each log line.
//
// Output format: [testName:serviceName] LEVEL: message
// Example: [TestDoubleSpend:propagation] INFO: Processing transaction abc123
type UnifiedTestLogger struct {
	t           TestingT
	testName    string
	serviceName string
	mutex       sync.RWMutex
	shutdown    atomic.Bool
	cancelFn    func()
	logLevel    atomic.Int32
}

// Log level constants
const (
	LevelDebug = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
)

// NewUnifiedTestLogger creates a new UnifiedTestLogger that routes all logs through t.Logf.
// testName is the name of the test (typically t.Name())
// serviceName is the component/service name (e.g., "propagation", "validator")
func NewUnifiedTestLogger(t TestingT, testName string, serviceName string, cancelFn ...func()) *UnifiedTestLogger {
	logger := &UnifiedTestLogger{
		t:           t,
		testName:    testName,
		serviceName: serviceName,
	}

	// Default to Info level
	logger.logLevel.Store(LevelInfo)

	if len(cancelFn) > 0 {
		logger.cancelFn = cancelFn[0]
	}

	return logger
}

// SetCancelFn sets the cancel function to be called on fatal errors.
func (l *UnifiedTestLogger) SetCancelFn(cancelFn func()) {
	l.cancelFn = cancelFn
}

// Shutdown marks the logger as shutdown, preventing further access to testing.T.
// This should be called before test cleanup to avoid race conditions.
func (l *UnifiedTestLogger) Shutdown() {
	l.shutdown.Store(true)
}

// LogLevel returns the current log level.
func (l *UnifiedTestLogger) LogLevel() int {
	return int(l.logLevel.Load())
}

// SetLogLevel sets the minimum log level. Messages below this level are ignored.
func (l *UnifiedTestLogger) SetLogLevel(level string) {
	switch level {
	case "DEBUG", "debug":
		l.logLevel.Store(LevelDebug)
	case "INFO", "info":
		l.logLevel.Store(LevelInfo)
	case "WARN", "warn":
		l.logLevel.Store(LevelWarn)
	case "ERROR", "error":
		l.logLevel.Store(LevelError)
	case "FATAL", "fatal":
		l.logLevel.Store(LevelFatal)
	}
}

// New creates a new logger instance with the given service name.
// This is called by the daemon's logger factory to create service-specific loggers.
func (l *UnifiedTestLogger) New(serviceName string, options ...Option) Logger {
	if h, ok := l.t.(tHelper); ok {
		h.Helper()
	}

	newLogger := &UnifiedTestLogger{
		t:           l.t,
		testName:    l.testName,
		serviceName: serviceName,
		cancelFn:    l.cancelFn,
	}
	newLogger.logLevel.Store(l.logLevel.Load())

	return newLogger
}

// Duplicate creates a copy of the logger with optional modifications.
func (l *UnifiedTestLogger) Duplicate(options ...Option) Logger {
	if h, ok := l.t.(tHelper); ok {
		h.Helper()
	}

	newLogger := &UnifiedTestLogger{
		t:           l.t,
		testName:    l.testName,
		serviceName: l.serviceName,
		cancelFn:    l.cancelFn,
	}
	newLogger.logLevel.Store(l.logLevel.Load())

	return newLogger
}

// prefix returns the formatted prefix for log messages.
// Format: [testName:serviceName] or [testName] if no service name
func (l *UnifiedTestLogger) prefix() string {
	if l.serviceName != "" {
		return fmt.Sprintf("[%s:%s]", l.testName, l.serviceName)
	}

	return fmt.Sprintf("[%s]", l.testName)
}

// log is the internal logging method that routes to t.Logf.
func (l *UnifiedTestLogger) log(level string, format string, args ...interface{}) {
	// Don't access testing.T if logger is shutdown
	if l.shutdown.Load() {
		return
	}

	l.mutex.RLock()
	defer l.mutex.RUnlock()

	if l.t == nil {
		return
	}

	if h, ok := l.t.(tHelper); ok {
		h.Helper()
	}

	message := fmt.Sprintf(format, args...)
	l.t.Logf("%s %s: %s", l.prefix(), level, message)
}

// Debugf logs a debug message.
func (l *UnifiedTestLogger) Debugf(format string, args ...interface{}) {
	if l.logLevel.Load() > LevelDebug {
		return
	}

	if h, ok := l.t.(tHelper); ok {
		h.Helper()
	}

	l.log("DEBUG", format, args...)
}

// Infof logs an info message.
func (l *UnifiedTestLogger) Infof(format string, args ...interface{}) {
	if l.logLevel.Load() > LevelInfo {
		return
	}

	if h, ok := l.t.(tHelper); ok {
		h.Helper()
	}

	l.log("INFO", format, args...)
}

// Warnf logs a warning message.
func (l *UnifiedTestLogger) Warnf(format string, args ...interface{}) {
	if l.logLevel.Load() > LevelWarn {
		return
	}

	if h, ok := l.t.(tHelper); ok {
		h.Helper()
	}

	l.log("WARN", format, args...)
}

// Errorf logs an error message.
func (l *UnifiedTestLogger) Errorf(format string, args ...interface{}) {
	if l.logLevel.Load() > LevelError {
		return
	}

	if h, ok := l.t.(tHelper); ok {
		h.Helper()
	}

	l.log("ERROR", format, args...)
}

// Fatalf logs a fatal message and optionally cancels the context.
func (l *UnifiedTestLogger) Fatalf(format string, args ...interface{}) {
	if h, ok := l.t.(tHelper); ok {
		h.Helper()
	}

	l.log("FATAL", format, args...)

	if l.cancelFn != nil {
		l.cancelFn()
	}
}
