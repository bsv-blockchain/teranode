// Package alert implements the Bitcoin SV alert system server and related functionality.
// 
// The alert package provides a comprehensive alert system for the Bitcoin SV network,
// enabling notification and enforcement mechanisms for critical network events.
// It includes functionality for sending and receiving network alerts, managing
// blacklisted funds, handling confiscation transactions, and interacting with
// the peer-to-peer network.
//
// Key features of the alert package include:
// - Broadcasting alerts to network participants
// - Managing consensus blacklists for funds
// - Processing confiscation transaction whitelists
// - Interacting with blockchain, UTXO, and P2P services
// - Maintaining a persistent store of alert-related data
//
// The alert system is designed to be highly reliable, ensuring critical
// notifications reach network participants even in degraded network conditions.
package alert

import (
	"fmt"

	"github.com/bitcoin-sv/teranode/ulogger"
)

// Logger provides a standardized logging interface for the alert service.
// It wraps the ulogger.Logger implementation to provide consistent logging
// methods that match the interfaces required by various alert system components.
// This wrapper ensures consistent logging behavior across the alert service
// and allows for future logging implementation changes without affecting clients.
type Logger struct {
	// ulogger is the underlying logger implementation that handles actual logging operations
	ulogger ulogger.Logger
}

// NewLogger creates and returns a new Logger instance using the provided ulogger.
// It wraps the provided logger to provide a standardized interface for the alert service.
//
// Parameters:
//   - logger: The underlying ulogger.Logger implementation to wrap
//
// Returns:
//   - *Logger: A new Logger instance configured with the provided ulogger
func NewLogger(logger ulogger.Logger) *Logger {
	return &Logger{
		ulogger: logger,
	}
}

// Debug logs a message at debug level.
// Multiple arguments are formatted using default formatting.
//
// Parameters:
//   - args: Variable arguments to log
func (l *Logger) Debug(args ...interface{}) {
	l.ulogger.Debugf("%s", args...)
}

// Debugf logs a formatted message at debug level.
//
// Parameters:
//   - msg: Format string following fmt.Printf conventions
//   - args: Arguments to apply to the format string
func (l *Logger) Debugf(msg string, args ...interface{}) {
	l.ulogger.Debugf(msg, args...)
}

// Error logs a message at error level.
// Multiple arguments are formatted using default formatting.
//
// Parameters:
//   - args: Variable arguments to log
func (l *Logger) Error(args ...interface{}) {
	l.ulogger.Errorf("%s", args...)
}

// ErrorWithStack logs a formatted message at error level, typically with stack trace context.
// This method is used to log errors with additional context information.
//
// Parameters:
//   - msg: Format string following fmt.Printf conventions
//   - args: Arguments to apply to the format string
func (l *Logger) ErrorWithStack(msg string, args ...interface{}) {
	l.ulogger.Errorf(msg, args...)
}

// Errorf logs a formatted message at error level.
//
// Parameters:
//   - msg: Format string following fmt.Printf conventions
//   - args: Arguments to apply to the format string
func (l *Logger) Errorf(msg string, args ...interface{}) {
	l.ulogger.Errorf(msg, args...)
}

// Fatal logs a message at fatal level and terminates the program.
// Multiple arguments are formatted using default formatting.
// After logging, the program will exit with a non-zero status code.
//
// Parameters:
//   - args: Variable arguments to log
func (l *Logger) Fatal(args ...interface{}) {
	l.ulogger.Fatalf("%s", args...)
}

// Fatalf logs a formatted message at fatal level and terminates the program.
// After logging, the program will exit with a non-zero status code.
//
// Parameters:
//   - msg: Format string following fmt.Printf conventions
//   - args: Arguments to apply to the format string
func (l *Logger) Fatalf(msg string, args ...interface{}) {
	l.ulogger.Fatalf(msg, args...)
}

// Info logs a message at info level.
// Multiple arguments are formatted using default formatting.
//
// Parameters:
//   - args: Variable arguments to log
func (l *Logger) Info(args ...interface{}) {
	l.ulogger.Infof("%s", args...)
}

// Infof logs a formatted message at info level.
//
// Parameters:
//   - msg: Format string following fmt.Printf conventions
//   - args: Arguments to apply to the format string
func (l *Logger) Infof(msg string, args ...interface{}) {
	l.ulogger.Infof(msg, args...)
}

// LogLevel returns the current log level as a string.
// This is useful for diagnostics and configuration validation.
//
// Returns:
//   - string: The current log level (e.g., "debug", "info", "warn", "error")
func (l *Logger) LogLevel() string {
	return ulogger.LogLevelString(l.ulogger.LogLevel())
}

// Panic logs a message and then panics, causing the current goroutine to crash.
// Unlike the other logging methods, this directly triggers a panic rather than
// logging through the underlying logger implementation.
//
// Parameters:
//   - args: Variable arguments to include in the panic
func (l *Logger) Panic(args ...interface{}) {
	panic(args)
}

// Panicf logs a formatted message and then panics, causing the current goroutine to crash.
// The message is formatted using fmt.Sprintf before triggering the panic.
//
// Parameters:
//   - msg: Format string following fmt.Printf conventions
//   - args: Arguments to apply to the format string
func (l *Logger) Panicf(msg string, args ...interface{}) {
	panic(fmt.Sprintf(msg, args...))
}

// Warn logs a message at warning level.
// Multiple arguments are formatted using default formatting.
//
// Parameters:
//   - args: Variable arguments to log
func (l *Logger) Warn(args ...interface{}) {
	l.ulogger.Warnf("%s", args...)
}

// Warnf logs a formatted message at warning level.
//
// Parameters:
//   - msg: Format string following fmt.Printf conventions
//   - args: Arguments to apply to the format string
func (l *Logger) Warnf(msg string, args ...interface{}) {
	l.ulogger.Warnf(msg, args...)
}

// Printf logs a formatted message at info level.
// This method implements the standard printf-style logging interface used by
// various libraries, mapping it to the info log level for compatibility.
//
// Parameters:
//   - format: Format string following fmt.Printf conventions
//   - v: Arguments to apply to the format string
func (l *Logger) Printf(format string, v ...interface{}) {
	l.ulogger.Infof(format, v...)
}

// CloseWriter implements the io.Closer interface for logger cleanup.
// This method is a no-op for the current logger implementation but is provided
// for interface compatibility with systems that expect to be able to close loggers.
//
// Returns:
//   - error: Always returns nil as there are no resources to clean up
func (l *Logger) CloseWriter() error {
	return nil
}
