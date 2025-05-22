// Package alert implements the Bitcoin SV alert system server and related functionality.
package alert

import (
	"context"
	"time"

	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/mrz1836/go-logger"
)

// GormLogger implements a GORM-compatible logger for the alert service's database operations.
// It provides a bridge between Teranode's logging system and the logging interface
// expected by the GORM ORM library, ensuring that database operations are logged
// consistently with the rest of the alert service.
//
// The GormLogger satisfies the logger.GormLoggerInterface from the mrz1836/go-logger
// library, enabling proper integration with GORM database operations.
type GormLogger struct {
	// ulogger is the underlying Teranode logger implementation that handles actual logging operations
	ulogger ulogger.Logger
}

// NewGormLogger creates and returns a new GORM-compatible logger instance.
// It wraps the provided ulogger to make it compatible with GORM's logging requirements.
//
// Parameters:
//   - logger: The underlying ulogger.Logger implementation to wrap
//
// Returns:
//   - *GormLogger: A new GormLogger instance configured with the provided ulogger
func NewGormLogger(logger ulogger.Logger) *GormLogger {
	return &GormLogger{
		ulogger: logger,
	}
}

// Error logs an error-level message with the given format and arguments.
// This method is part of the logger.GormLoggerInterface and is called by GORM
// when error-level events occur during database operations.
//
// Parameters:
//   - _: Context (unused but required by the interface)
//   - s: Format string for the error message
//   - i: Arguments for the format string
func (g *GormLogger) Error(_ context.Context, s string, i ...interface{}) {
	g.ulogger.Errorf(s, i...)
}

// GetMode returns the current log level mode for GORM operations.
// This method is part of the logger.GormLoggerInterface and allows GORM
// to determine which log levels should be processed.
//
// Returns:
//   - logger.GormLogLevel: The current GORM log level (currently returns 0,
//     which represents the default logging level)
//
// Note: This is marked as TODO in the implementation and currently
// returns a fixed value.
func (g *GormLogger) GetMode() logger.GormLogLevel {
	// TODO
	return logger.GormLogLevel(0)
}

// GetStackLevel returns the current stack trace level for logging.
// This method is part of the logger.GormLoggerInterface and determines how
// much of the call stack should be included in log messages.
//
// Returns:
//   - int: The current stack trace level (0 means no stack trace)
func (g *GormLogger) GetStackLevel() int {
	return 0
}

// Info logs an info-level message with the given format and arguments.
// This method is part of the logger.GormLoggerInterface and is called by GORM
// when informational events occur during database operations.
//
// Parameters:
//   - _: Context (unused but required by the interface)
//   - s: Format string for the info message
//   - i: Arguments for the format string
func (g *GormLogger) Info(_ context.Context, s string, i ...interface{}) {
	g.ulogger.Infof(s, i...)
}

// SetMode configures the log level mode for GORM operations.
// This method is part of the logger.GormLoggerInterface and allows GORM
// to control which log levels should be processed.
//
// Parameters:
//   - _: logger.GormLogLevel (currently unused in the implementation)
//
// Returns:
//   - logger.GormLoggerInterface: The logger interface (returns self for chaining)
//
// Note: The current implementation does not actually change the mode;
// it simply returns the logger instance for method chaining.
func (g *GormLogger) SetMode(_ logger.GormLogLevel) logger.GormLoggerInterface {
	return g
}

// SetStackLevel configures the stack trace level for logging.
// This method is part of the logger.GormLoggerInterface and determines how
// much of the call stack should be included in log messages.
//
// Parameters:
//   - _: int (currently unused in the implementation)
//
// Note: The current implementation is a no-op and does not actually
// change the stack level.
func (g *GormLogger) SetStackLevel(_ int) {}

// Trace logs detailed information about SQL queries executed by GORM.
// This method is part of the logger.GormLoggerInterface and is called by GORM
// to log SQL statements with timing information.
//
// Parameters:
//   - _: Context (unused but required by the interface)
//   - begin: The time when the SQL query began execution
//   - fc: A function that returns the SQL statement and the number of rows affected
//   - err: Any error that occurred during query execution
//
// Note: The current implementation is a no-op and does not actually log
// the trace information. This could be implemented in the future to provide
// more detailed database query logging.
func (g *GormLogger) Trace(_ context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
}

// Warn logs a warning-level message with the given format and arguments.
// This method is part of the logger.GormLoggerInterface and is called by GORM
// when warning-level events occur during database operations.
//
// Parameters:
//   - _: Context (unused but required by the interface)
//   - s: Format string for the warning message
//   - i: Arguments for the format string
func (g *GormLogger) Warn(_ context.Context, s string, i ...interface{}) {
	g.ulogger.Warnf(s, i...)
}
