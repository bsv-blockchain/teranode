package ulogger

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFileLogger(t *testing.T) {
	t.Run("ValidFileLogger", func(t *testing.T) {
		tempDir := t.TempDir()
		logFile := filepath.Join(tempDir, "test.log")

		logger := NewFileLogger("test-service", WithFilePath(logFile))

		assert.NotNil(t, logger)
		assert.Equal(t, "test-service", logger.service)
		assert.Equal(t, LogLevelInfo, logger.logLevel) // Default level
		assert.NotNil(t, logger.logFile)
		assert.Equal(t, 0, logger.skipFrame) // Default skip
	})

	t.Run("WithCustomLogLevel", func(t *testing.T) {
		tempDir := t.TempDir()
		logFile := filepath.Join(tempDir, "test.log")

		logger := NewFileLogger("test-service", WithFilePath(logFile), WithLevel("DEBUG"))

		assert.NotNil(t, logger)
		assert.Equal(t, LogLevelDebug, logger.logLevel)
	})

	t.Run("WithCustomSkipFrame", func(t *testing.T) {
		tempDir := t.TempDir()
		logFile := filepath.Join(tempDir, "test.log")

		logger := NewFileLogger("test-service", WithFilePath(logFile), WithSkipFrame(2))

		assert.NotNil(t, logger)
		assert.Equal(t, 2, logger.skipFrame)
	})

	t.Run("InvalidFilePath", func(t *testing.T) {
		// Try to create a log file in a directory that doesn't exist
		invalidPath := "/nonexistent/directory/test.log"

		logger := NewFileLogger("test-service", WithFilePath(invalidPath))

		assert.Nil(t, logger) // Should return nil on file creation failure
	})

	t.Run("EmptyService", func(t *testing.T) {
		tempDir := t.TempDir()
		logFile := filepath.Join(tempDir, "test.log")

		logger := NewFileLogger("", WithFilePath(logFile))

		assert.NotNil(t, logger)
		assert.Equal(t, "", logger.service)
	})
}

func TestFileLogger_LogLevel(t *testing.T) {
	tempDir := t.TempDir()
	logFile := filepath.Join(tempDir, "test.log")

	t.Run("DefaultLogLevel", func(t *testing.T) {
		logger := NewFileLogger("test", WithFilePath(logFile))
		assert.Equal(t, LogLevelInfo, logger.LogLevel())
	})

	t.Run("CustomLogLevel", func(t *testing.T) {
		logger := NewFileLogger("test", WithFilePath(logFile), WithLevel("ERROR"))
		assert.Equal(t, LogLevelError, logger.LogLevel())
	})
}

func TestFileLogger_SetLogLevel(t *testing.T) {
	tempDir := t.TempDir()
	logFile := filepath.Join(tempDir, "test.log")

	logger := NewFileLogger("test", WithFilePath(logFile))

	testCases := []struct {
		name     string
		level    string
		expected int
	}{
		{"DEBUG", "DEBUG", LogLevelDebug},
		{"INFO", "INFO", LogLevelInfo},
		{"WARNING", "WARNING", LogLevelWarning},
		{"ERROR", "ERROR", LogLevelError},
		{"FATAL", "FATAL", LogLevelFatal},
		{"Invalid", "INVALID", LogLevelInfo}, // Should default to INFO
		{"Lowercase", "debug", LogLevelInfo}, // Should default to INFO for invalid case
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			logger.SetLogLevel(tc.level)
			assert.Equal(t, tc.expected, logger.LogLevel())
		})
	}
}

func TestFileLogger_LoggingMethods(t *testing.T) {
	tempDir := t.TempDir()
	logFile := filepath.Join(tempDir, "test.log")

	// Test each logging method
	t.Run("Debugf", func(t *testing.T) {
		logger := NewFileLogger("test", WithFilePath(logFile), WithLevel("DEBUG"))

		logger.Debugf("Debug message: %s", "test")

		content := readLogFile(t, logFile)
		assert.Contains(t, content, "DEBUG")
		assert.Contains(t, content, "Debug message: test")

		// Clear the log file
		clearLogFile(t, logFile)
	})

	t.Run("Infof", func(t *testing.T) {
		logger := NewFileLogger("test", WithFilePath(logFile), WithLevel("INFO"))

		logger.Infof("Info message: %d", 42)

		content := readLogFile(t, logFile)
		assert.Contains(t, content, "INFO")
		assert.Contains(t, content, "Info message: 42")

		clearLogFile(t, logFile)
	})

	t.Run("Warnf", func(t *testing.T) {
		logger := NewFileLogger("test", WithFilePath(logFile), WithLevel("WARNING"))

		logger.Warnf("Warning message: %v", true)

		content := readLogFile(t, logFile)
		assert.Contains(t, content, "WARNING")
		assert.Contains(t, content, "Warning message: true")

		clearLogFile(t, logFile)
	})

	t.Run("Errorf", func(t *testing.T) {
		logger := NewFileLogger("test", WithFilePath(logFile), WithLevel("ERROR"))

		logger.Errorf("Error message: %s", "failed")

		content := readLogFile(t, logFile)
		assert.Contains(t, content, "ERROR")
		assert.Contains(t, content, "Error message: failed")

		clearLogFile(t, logFile)
	})
}

func TestFileLogger_LogLevelFiltering(t *testing.T) {
	tempDir := t.TempDir()
	logFile := filepath.Join(tempDir, "test.log")

	t.Run("LogLevelError_FiltersLowerLevels", func(t *testing.T) {
		logger := NewFileLogger("test", WithFilePath(logFile), WithLevel("ERROR"))

		logger.Debugf("Debug message")
		logger.Infof("Info message")
		logger.Warnf("Warning message")
		logger.Errorf("Error message")

		content := readLogFile(t, logFile)

		// Should only contain ERROR level message
		assert.NotContains(t, content, "DEBUG")
		assert.NotContains(t, content, "INFO")
		assert.NotContains(t, content, "WARNING")
		assert.Contains(t, content, "ERROR")
		assert.Contains(t, content, "Error message")

		clearLogFile(t, logFile)
	})

	t.Run("LogLevelDebug_ShowsAllLevels", func(t *testing.T) {
		logger := NewFileLogger("test", WithFilePath(logFile), WithLevel("DEBUG"))

		logger.Debugf("Debug message")
		logger.Infof("Info message")
		logger.Warnf("Warning message")
		logger.Errorf("Error message")

		content := readLogFile(t, logFile)

		// Should contain all level messages
		assert.Contains(t, content, "DEBUG")
		assert.Contains(t, content, "INFO")
		assert.Contains(t, content, "WARNING")
		assert.Contains(t, content, "ERROR")

		clearLogFile(t, logFile)
	})
}

func TestFileLogger_New(t *testing.T) {
	tempDir := t.TempDir()
	logFile := filepath.Join(tempDir, "test.log")

	originalLogger := NewFileLogger("original", WithFilePath(logFile), WithLevel("ERROR"))

	t.Run("CreateNewLogger", func(t *testing.T) {
		newLogger := originalLogger.New("new-service")

		// Should be a FileLogger
		fileLogger, ok := newLogger.(*FileLogger)
		assert.True(t, ok)

		// Should have new service name but same log level and file
		assert.Equal(t, "new-service", fileLogger.service)
		assert.Equal(t, LogLevelError, fileLogger.logLevel)
		assert.Equal(t, originalLogger.logFile, fileLogger.logFile)
	})

	t.Run("CreateNewLoggerWithOptions", func(t *testing.T) {
		newLogger := originalLogger.New("new-service", WithLevel("DEBUG"))

		fileLogger, ok := newLogger.(*FileLogger)
		assert.True(t, ok)

		assert.Equal(t, "new-service", fileLogger.service)
		// NOTE: New() method doesn't apply options, it just copies the original logger's settings
		assert.Equal(t, LogLevelError, fileLogger.logLevel) // Should still be ERROR from original
	})
}

func TestFileLogger_Duplicate(t *testing.T) {
	tempDir := t.TempDir()
	logFile := filepath.Join(tempDir, "test.log")

	originalLogger := NewFileLogger("test-service", WithFilePath(logFile), WithLevel("ERROR"), WithSkipFrame(1))

	t.Run("DuplicateBasic", func(t *testing.T) {
		duplicateLogger := originalLogger.Duplicate()

		fileLogger, ok := duplicateLogger.(*FileLogger)
		assert.True(t, ok)

		// Should have same service name, log level, file, and skipFrame
		assert.Equal(t, originalLogger.service, fileLogger.service)
		assert.Equal(t, originalLogger.logLevel, fileLogger.logLevel)
		assert.Equal(t, originalLogger.logFile, fileLogger.logFile)
		assert.Equal(t, originalLogger.skipFrame, fileLogger.skipFrame)
	})

	t.Run("DuplicateWithLevelOption", func(t *testing.T) {
		duplicateLogger := originalLogger.Duplicate(WithLevel("DEBUG"))

		fileLogger, ok := duplicateLogger.(*FileLogger)
		assert.True(t, ok)

		// Should have same service but different log level
		assert.Equal(t, originalLogger.service, fileLogger.service)
		assert.Equal(t, LogLevelDebug, fileLogger.logLevel) // Should be changed
		assert.Equal(t, originalLogger.logFile, fileLogger.logFile)
	})

	t.Run("DuplicateWithSkipFrame", func(t *testing.T) {
		duplicateLogger := originalLogger.Duplicate(WithSkipFrame(5))

		fileLogger, ok := duplicateLogger.(*FileLogger)
		assert.True(t, ok)

		// Should have different skipFrame
		assert.Equal(t, 5, fileLogger.skipFrame)
	})

	t.Run("DuplicateWithSkipFrameIncrement", func(t *testing.T) {
		duplicateLogger := originalLogger.Duplicate(WithSkipFrameIncrement(2))

		fileLogger, ok := duplicateLogger.(*FileLogger)
		assert.True(t, ok)

		// Should increment skipFrame by 2
		assert.Equal(t, originalLogger.skipFrame+2, fileLogger.skipFrame)
	})
}

func TestParseLogLevel(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected int
	}{
		{"DEBUG", "DEBUG", LogLevelDebug},
		{"INFO", "INFO", LogLevelInfo},
		{"WARNING", "WARNING", LogLevelWarning},
		{"ERROR", "ERROR", LogLevelError},
		{"FATAL", "FATAL", LogLevelFatal},
		{"Invalid", "INVALID", LogLevelInfo},
		{"Empty", "", LogLevelInfo},
		{"Lowercase", "debug", LogLevelInfo}, // parseLogLevel is case-sensitive
		{"Mixed", "Debug", LogLevelInfo},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := parseLogLevel(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestLogMessage(t *testing.T) {
	tempDir := t.TempDir()
	logFile := filepath.Join(tempDir, "test.log")

	file, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	defer file.Close()

	t.Run("BasicLogMessage", func(t *testing.T) {
		logMessage(file, "test-service", "INFO", "Test message: %s", "", "", "hello")

		content := readLogFile(t, logFile)

		// Should contain timestamp, level, and message
		assert.Contains(t, content, "INFO")
		assert.Contains(t, content, "Test message: hello")
		// Should contain timestamp (basic check)
		assert.Contains(t, content, "[")
		assert.Contains(t, content, "]")

		clearLogFile(t, logFile)
	})

	t.Run("MessageWithMultipleArgs", func(t *testing.T) {
		logMessage(file, "test-service", "ERROR", "Error: %s - Code: %d", "", "", "failed", 404)

		content := readLogFile(t, logFile)

		assert.Contains(t, content, "ERROR")
		assert.Contains(t, content, "Error: failed - Code: 404")

		clearLogFile(t, logFile)
	})
}

func TestFileLogger_Fatalf(t *testing.T) {
	// Note: Testing Fatalf is tricky because it calls os.Exit(1)
	// We can't really test the os.Exit part in a unit test without forking processes
	// So we'll test that the message gets written but skip the exit part

	tempDir := t.TempDir()
	logFile := filepath.Join(tempDir, "test.log")

	t.Run("FatalfWritesMessage", func(t *testing.T) {
		logger := NewFileLogger("test", WithFilePath(logFile))

		// We can't actually call Fatalf because it will exit the test process
		// Instead, we'll test the underlying logMessage function directly
		logMessage(logger.logFile, logger.service, "FATAL", "Fatal error: %s", "", "", "critical")

		content := readLogFile(t, logFile)
		assert.Contains(t, content, "FATAL")
		assert.Contains(t, content, "Fatal error: critical")
	})
}

func TestFileLogger_LogConstants(t *testing.T) {
	// Test that log level constants are correct
	assert.Equal(t, 0, LogLevelDebug)
	assert.Equal(t, 1, LogLevelInfo)
	assert.Equal(t, 2, LogLevelWarning)
	assert.Equal(t, 3, LogLevelError)
	assert.Equal(t, 4, LogLevelFatal)
}

func TestFileLogger_EdgeCases(t *testing.T) {
	tempDir := t.TempDir()
	logFile := filepath.Join(tempDir, "test.log")

	t.Run("EmptyFormatString", func(t *testing.T) {
		logger := NewFileLogger("test", WithFilePath(logFile))

		logger.Infof("")

		content := readLogFile(t, logFile)
		assert.Contains(t, content, "INFO")

		clearLogFile(t, logFile)
	})

	t.Run("NoArgsForFormat", func(t *testing.T) {
		logger := NewFileLogger("test", WithFilePath(logFile))

		logger.Infof("Simple message")

		content := readLogFile(t, logFile)
		assert.Contains(t, content, "Simple message")

		clearLogFile(t, logFile)
	})

	t.Run("ComplexFormatString", func(t *testing.T) {
		logger := NewFileLogger("test", WithFilePath(logFile))

		logger.Infof("User %s logged in from %s at %d", "john", "192.168.1.1", 1234567890)

		content := readLogFile(t, logFile)
		assert.Contains(t, content, "User john logged in from 192.168.1.1 at 1234567890")

		clearLogFile(t, logFile)
	})

	t.Run("NilArgsHandling", func(t *testing.T) {
		logger := NewFileLogger("test", WithFilePath(logFile))

		var nilValue *string
		logger.Infof("Nil value: %v", nilValue)

		content := readLogFile(t, logFile)
		assert.Contains(t, content, "Nil value: <nil>")

		clearLogFile(t, logFile)
	})
}

func TestFileLogger_AppendMode(t *testing.T) {
	tempDir := t.TempDir()
	logFile := filepath.Join(tempDir, "test.log")

	t.Run("AppendToExistingFile", func(t *testing.T) {
		// Create first logger and write a message
		logger1 := NewFileLogger("test1", WithFilePath(logFile))
		logger1.Infof("First message")

		// Create second logger with same file and write another message
		logger2 := NewFileLogger("test2", WithFilePath(logFile))
		logger2.Infof("Second message")

		content := readLogFile(t, logFile)

		// Should contain both messages
		assert.Contains(t, content, "First message")
		assert.Contains(t, content, "Second message")

		// Count the number of lines - should be at least 2
		lines := strings.Split(strings.TrimSpace(content), "\n")
		assert.GreaterOrEqual(t, len(lines), 2)
	})
}

// Helper functions

func readLogFile(t *testing.T, filename string) string {
	t.Helper()
	content, err := os.ReadFile(filename)
	require.NoError(t, err)
	return string(content)
}

func clearLogFile(t *testing.T, filename string) {
	t.Helper()
	err := os.Truncate(filename, 0)
	require.NoError(t, err)
}
