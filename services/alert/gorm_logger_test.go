// Package alert implements the Bitcoin SV alert system server and related functionality.
package alert

import (
	"context"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/mrz1836/go-logger"
	"github.com/stretchr/testify/require"
)

func TestNewGormLogger(t *testing.T) {
	mockLogger := &MockLogger{}

	gormLogger := NewGormLogger(mockLogger)

	require.NotNil(t, gormLogger)
	require.Equal(t, mockLogger, gormLogger.ulogger)
}

func TestGormLogger_Error(t *testing.T) {
	mockLogger := &MockLogger{}
	gormLogger := NewGormLogger(mockLogger)
	ctx := context.Background()

	gormLogger.Error(ctx, "error: %s", "message")

	require.Len(t, mockLogger.ErrorCalls, 1)
	require.Equal(t, "error: %s", mockLogger.ErrorCalls[0])
}

func TestGormLogger_Error_with_multiple_args(t *testing.T) {
	mockLogger := &MockLogger{}
	gormLogger := NewGormLogger(mockLogger)
	ctx := context.Background()

	gormLogger.Error(ctx, "error: %s %d", "message", 42)

	require.Len(t, mockLogger.ErrorCalls, 1)
	require.Equal(t, "error: %s %d", mockLogger.ErrorCalls[0])
}

func TestGormLogger_GetMode(t *testing.T) {
	mockLogger := &MockLogger{}
	gormLogger := NewGormLogger(mockLogger)

	mode := gormLogger.GetMode()

	require.Equal(t, logger.GormLogLevel(0), mode)
}

func TestGormLogger_GetStackLevel(t *testing.T) {
	mockLogger := &MockLogger{}
	gormLogger := NewGormLogger(mockLogger)

	level := gormLogger.GetStackLevel()

	require.Equal(t, 0, level)
}

func TestGormLogger_Info(t *testing.T) {
	mockLogger := &MockLogger{}
	gormLogger := NewGormLogger(mockLogger)
	ctx := context.Background()

	gormLogger.Info(ctx, "info: %s", "message")

	require.Len(t, mockLogger.InfoCalls, 1)
	require.Equal(t, "info: %s", mockLogger.InfoCalls[0])
}

func TestGormLogger_Info_with_multiple_args(t *testing.T) {
	mockLogger := &MockLogger{}
	gormLogger := NewGormLogger(mockLogger)
	ctx := context.Background()

	gormLogger.Info(ctx, "info: %s %d", "message", 42)

	require.Len(t, mockLogger.InfoCalls, 1)
	require.Equal(t, "info: %s %d", mockLogger.InfoCalls[0])
}

func TestGormLogger_SetMode(t *testing.T) {
	mockLogger := &MockLogger{}
	gormLogger := NewGormLogger(mockLogger)

	result := gormLogger.SetMode(logger.GormLogLevel(1))

	require.Equal(t, gormLogger, result)
}

func TestGormLogger_SetStackLevel(t *testing.T) {
	mockLogger := &MockLogger{}
	gormLogger := NewGormLogger(mockLogger)

	// Should not panic or cause any issues
	require.NotPanics(t, func() {
		gormLogger.SetStackLevel(5)
	})
}

func TestGormLogger_Trace(t *testing.T) {
	mockLogger := &MockLogger{}
	gormLogger := NewGormLogger(mockLogger)
	ctx := context.Background()

	begin := time.Now()
	fc := func() (sql string, rowsAffected int64) {
		return "SELECT * FROM users", 10
	}
	var err error

	// Should not panic or cause any issues since it's a no-op
	require.NotPanics(t, func() {
		gormLogger.Trace(ctx, begin, fc, err)
	})
}

func TestGormLogger_Trace_with_error(t *testing.T) {
	mockLogger := &MockLogger{}
	gormLogger := NewGormLogger(mockLogger)
	ctx := context.Background()

	begin := time.Now()
	fc := func() (sql string, rowsAffected int64) {
		return "SELECT * FROM users", 0
	}
	err := errors.NewProcessingError("database error")

	// Should not panic or cause any issues since it's a no-op
	require.NotPanics(t, func() {
		gormLogger.Trace(ctx, begin, fc, err)
	})
}

func TestGormLogger_Warn(t *testing.T) {
	mockLogger := &MockLogger{}
	gormLogger := NewGormLogger(mockLogger)
	ctx := context.Background()

	gormLogger.Warn(ctx, "warning: %s", "message")

	require.Len(t, mockLogger.WarnCalls, 1)
	require.Equal(t, "warning: %s", mockLogger.WarnCalls[0])
}

func TestGormLogger_Warn_with_multiple_args(t *testing.T) {
	mockLogger := &MockLogger{}
	gormLogger := NewGormLogger(mockLogger)
	ctx := context.Background()

	gormLogger.Warn(ctx, "warning: %s %d", "message", 42)

	require.Len(t, mockLogger.WarnCalls, 1)
	require.Equal(t, "warning: %s %d", mockLogger.WarnCalls[0])
}

func TestGormLogger_interface_compliance(t *testing.T) {
	mockLogger := &MockLogger{}
	gormLogger := NewGormLogger(mockLogger)

	// Verify that GormLogger implements the required interface
	var _ logger.GormLoggerInterface = gormLogger

	require.NotNil(t, gormLogger)
}

func TestGormLogger_with_real_logger(t *testing.T) {
	realLogger := ulogger.TestLogger{}
	gormLogger := NewGormLogger(realLogger)
	ctx := context.Background()

	// Test that real logger methods work without panicking
	require.NotPanics(t, func() {
		gormLogger.Info(ctx, "info: %s", "message")
		gormLogger.Warn(ctx, "warning: %s", "message")
		gormLogger.Error(ctx, "error: %s", "message")

		begin := time.Now()
		fc := func() (sql string, rowsAffected int64) {
			return "SELECT * FROM test", 1
		}
		gormLogger.Trace(ctx, begin, fc, nil)
	})

	mode := gormLogger.GetMode()
	require.Equal(t, logger.GormLogLevel(0), mode)

	level := gormLogger.GetStackLevel()
	require.Equal(t, 0, level)

	result := gormLogger.SetMode(logger.GormLogLevel(2))
	require.Equal(t, gormLogger, result)

	gormLogger.SetStackLevel(3)
}

func TestGormLogger_context_independence(t *testing.T) {
	mockLogger := &MockLogger{}
	gormLogger := NewGormLogger(mockLogger)

	ctx1 := context.Background()
	ctx2 := context.WithValue(context.Background(), "key", "value")

	gormLogger.Info(ctx1, "message from ctx1")
	gormLogger.Info(ctx2, "message from ctx2")

	require.Len(t, mockLogger.InfoCalls, 2)
	require.Equal(t, "message from ctx1", mockLogger.InfoCalls[0])
	require.Equal(t, "message from ctx2", mockLogger.InfoCalls[1])
}

func TestGormLogger_empty_format_string(t *testing.T) {
	mockLogger := &MockLogger{}
	gormLogger := NewGormLogger(mockLogger)
	ctx := context.Background()

	gormLogger.Info(ctx, "")

	require.Len(t, mockLogger.InfoCalls, 1)
	require.Equal(t, "", mockLogger.InfoCalls[0])
}

func TestGormLogger_no_arguments(t *testing.T) {
	mockLogger := &MockLogger{}
	gormLogger := NewGormLogger(mockLogger)
	ctx := context.Background()

	gormLogger.Info(ctx, "simple message")

	require.Len(t, mockLogger.InfoCalls, 1)
	require.Equal(t, "simple message", mockLogger.InfoCalls[0])
}
