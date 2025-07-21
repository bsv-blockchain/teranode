// Package alert implements the Bitcoin SV alert system server and related functionality.
package alert

import (
	"testing"

	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func TestNewLogger(t *testing.T) {
	mockLogger := ulogger.TestLogger{}

	logger := NewLogger(mockLogger)

	require.NotNil(t, logger)
	require.Equal(t, mockLogger, logger.ulogger)
}

func TestLogger_Debug(t *testing.T) {
	mockLogger := &MockLogger{}
	logger := NewLogger(mockLogger)

	logger.Debug("test message")

	require.Len(t, mockLogger.DebugCalls, 1)
	require.Equal(t, "%s", mockLogger.DebugCalls[0])
}

func TestLogger_Debugf(t *testing.T) {
	mockLogger := &MockLogger{}
	logger := NewLogger(mockLogger)

	logger.Debugf("test %s", "message")

	require.Len(t, mockLogger.DebugCalls, 1)
	require.Equal(t, "test %s", mockLogger.DebugCalls[0])
}

func TestLogger_Error(t *testing.T) {
	mockLogger := &MockLogger{}
	logger := NewLogger(mockLogger)

	logger.Error("error message")

	require.Len(t, mockLogger.ErrorCalls, 1)
	require.Equal(t, "%s", mockLogger.ErrorCalls[0])
}

func TestLogger_ErrorWithStack(t *testing.T) {
	mockLogger := &MockLogger{}
	logger := NewLogger(mockLogger)

	logger.ErrorWithStack("error: %s", "stack trace")

	require.Len(t, mockLogger.ErrorCalls, 1)
	require.Equal(t, "error: %s", mockLogger.ErrorCalls[0])
}

func TestLogger_Errorf(t *testing.T) {
	mockLogger := &MockLogger{}
	logger := NewLogger(mockLogger)

	logger.Errorf("error: %s", "message")

	require.Len(t, mockLogger.ErrorCalls, 1)
	require.Equal(t, "error: %s", mockLogger.ErrorCalls[0])
}

func TestLogger_Fatal(t *testing.T) {
	mockLogger := &MockLogger{}
	logger := NewLogger(mockLogger)

	logger.Fatal("fatal message")

	require.Len(t, mockLogger.FatalCalls, 1)
	require.Equal(t, "%s", mockLogger.FatalCalls[0])
}

func TestLogger_Fatalf(t *testing.T) {
	mockLogger := &MockLogger{}
	logger := NewLogger(mockLogger)

	logger.Fatalf("fatal: %s", "message")

	require.Len(t, mockLogger.FatalCalls, 1)
	require.Equal(t, "fatal: %s", mockLogger.FatalCalls[0])
}

func TestLogger_Info(t *testing.T) {
	mockLogger := &MockLogger{}
	logger := NewLogger(mockLogger)

	logger.Info("info message")

	require.Len(t, mockLogger.InfoCalls, 1)
	require.Equal(t, "%s", mockLogger.InfoCalls[0])
}

func TestLogger_Infof(t *testing.T) {
	mockLogger := &MockLogger{}
	logger := NewLogger(mockLogger)

	logger.Infof("info: %s", "message")

	require.Len(t, mockLogger.InfoCalls, 1)
	require.Equal(t, "info: %s", mockLogger.InfoCalls[0])
}

func TestLogger_LogLevel(t *testing.T) {
	mockLogger := &MockLogger{LogLevelValue: ulogger.LogLevelInfo}
	logger := NewLogger(mockLogger)

	level := logger.LogLevel()

	require.Equal(t, "INFO", level)
}

func TestLogger_Panic(t *testing.T) {
	mockLogger := &MockLogger{}
	logger := NewLogger(mockLogger)

	require.Panics(t, func() {
		logger.Panic("panic message")
	})
}

func TestLogger_Panicf(t *testing.T) {
	mockLogger := &MockLogger{}
	logger := NewLogger(mockLogger)

	require.Panics(t, func() {
		logger.Panicf("panic: %s", "message")
	})
}

func TestLogger_Warn(t *testing.T) {
	mockLogger := &MockLogger{}
	logger := NewLogger(mockLogger)

	logger.Warn("warning message")

	require.Len(t, mockLogger.WarnCalls, 1)
	require.Equal(t, "%s", mockLogger.WarnCalls[0])
}

func TestLogger_Warnf(t *testing.T) {
	mockLogger := &MockLogger{}
	logger := NewLogger(mockLogger)

	logger.Warnf("warning: %s", "message")

	require.Len(t, mockLogger.WarnCalls, 1)
	require.Equal(t, "warning: %s", mockLogger.WarnCalls[0])
}

func TestLogger_Printf(t *testing.T) {
	mockLogger := &MockLogger{}
	logger := NewLogger(mockLogger)

	logger.Printf("printf: %s", "message")

	require.Len(t, mockLogger.InfoCalls, 1)
	require.Equal(t, "printf: %s", mockLogger.InfoCalls[0])
}

func TestLogger_CloseWriter(t *testing.T) {
	mockLogger := &MockLogger{}
	logger := NewLogger(mockLogger)

	err := logger.CloseWriter()

	require.NoError(t, err)
}

func TestLogger_with_real_logger(t *testing.T) {
	realLogger := ulogger.TestLogger{}
	logger := NewLogger(realLogger)

	// Test that real logger methods work without panicking
	require.NotPanics(t, func() {
		logger.Debug("debug message")
		logger.Debugf("debug: %s", "formatted")
		logger.Info("info message")
		logger.Infof("info: %s", "formatted")
		logger.Warn("warning message")
		logger.Warnf("warning: %s", "formatted")
		logger.Error("error message")
		logger.Errorf("error: %s", "formatted")
		logger.ErrorWithStack("error with stack: %s", "formatted")
		logger.Printf("printf: %s", "formatted")
	})

	level := logger.LogLevel()
	require.NotEmpty(t, level)

	err := logger.CloseWriter()
	require.NoError(t, err)
}

func TestLogger_multiple_arguments(t *testing.T) {
	mockLogger := &MockLogger{}
	logger := NewLogger(mockLogger)

	logger.Info("arg1", "arg2", "arg3")

	require.Len(t, mockLogger.InfoCalls, 1)
	require.Equal(t, "%s", mockLogger.InfoCalls[0])
}

func TestLogger_panic_with_multiple_arguments(t *testing.T) {
	mockLogger := &MockLogger{}
	logger := NewLogger(mockLogger)

	require.Panics(t, func() {
		logger.Panic("arg1", "arg2", "arg3")
	})
}

func TestLogger_panicf_formatting(t *testing.T) {
	mockLogger := &MockLogger{}
	logger := NewLogger(mockLogger)

	defer func() {
		if r := recover(); r != nil {
			require.Equal(t, "panic: formatted message", r)
		}
	}()

	logger.Panicf("panic: %s", "formatted message")
	require.Fail(t, "Should have panicked")
}
