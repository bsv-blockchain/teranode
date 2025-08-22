package ulogger

import (
	"fmt"
	"io"
	"os"
	"time"
)

type FileLogger struct {
	service   string
	logLevel  int
	writer    io.Writer
	logFile   *os.File
	skipFrame int
}

// Log levels
const (
	LogLevelDebug   = 0
	LogLevelInfo    = 1
	LogLevelWarning = 2
	LogLevelError   = 3
	LogLevelFatal   = 4
)

func NewFileLogger(service string, options ...Option) *FileLogger {
	opts := DefaultOptions()
	for _, o := range options {
		o(opts)
	}

	logFilePath := opts.filepath

	file, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil
	}

	return &FileLogger{
		service:   service,
		logLevel:  parseLogLevel(opts.logLevel),
		writer:    opts.writer,
		logFile:   file,
		skipFrame: opts.skip,
	}
}

func (fl *FileLogger) LogLevel() int {
	return fl.logLevel
}

func (fl *FileLogger) SetLogLevel(level string) {
	fl.logLevel = parseLogLevel(level)
}

func (fl *FileLogger) Debugf(format string, args ...interface{}) {
	if fl.logLevel <= LogLevelDebug {
		logMessage(fl.logFile, fl.service, "DEBUG", format, args...)
	}
}

func (fl *FileLogger) Infof(format string, args ...interface{}) {
	if fl.logLevel <= LogLevelInfo {
		logMessage(fl.logFile, fl.service, "INFO", format, args...)
	}
}

func (fl *FileLogger) Warnf(format string, args ...interface{}) {
	if fl.logLevel <= LogLevelWarning {
		logMessage(fl.logFile, fl.service, "WARNING", format, args...)
	}
}

func (fl *FileLogger) Errorf(format string, args ...interface{}) {
	if fl.logLevel <= LogLevelError {
		logMessage(fl.logFile, fl.service, "ERROR", format, args...)
	}
}

func (fl *FileLogger) Fatalf(format string, args ...interface{}) {
	logMessage(fl.logFile, fl.service, "FATAL", format, args...)
	os.Exit(1)
}

func (fl *FileLogger) New(service string, options ...Option) Logger {
	// Create a new FileLogger with the same log file
	return &FileLogger{
		service:  service,
		logLevel: fl.logLevel,
		writer:   fl.writer,
		logFile:  fl.logFile,
	}
}

func (fl *FileLogger) Duplicate(options ...Option) Logger {
	// Create a new FileLogger with the same log file
	newLogger := &FileLogger{
		service:   fl.service,
		logLevel:  fl.logLevel,
		writer:    fl.writer,
		logFile:   fl.logFile,
		skipFrame: fl.skipFrame,
	}

	defaultOpts := DefaultOptions()
	opts := DefaultOptions()

	for _, o := range options {
		o(opts)
	}

	// apply any options that are not the default, to the new logger
	if opts.logLevel != defaultOpts.logLevel {
		newLogger.SetLogLevel(opts.logLevel)
	}

	if opts.skip != defaultOpts.skip {
		newLogger.skipFrame = opts.skip
	}

	if opts.skipIncrement > 0 {
		newLogger.skipFrame += opts.skipIncrement
	}

	return newLogger
}

func logMessage(logFile *os.File, _, level, format string, args ...interface{}) {
	message := fmt.Sprintf("[%s] [%s] %s\n", time.Now().Format("2006-01-02 15:04:05"), level, fmt.Sprintf(format, args...))

	_, err := logFile.Write([]byte(message))
	if err != nil {
		fmt.Printf("Failed to write log message: %s", err)
	}
}

func parseLogLevel(level string) int {
	switch level {
	case "DEBUG":
		return LogLevelDebug
	case "INFO":
		return LogLevelInfo
	case "WARNING":
		return LogLevelWarning
	case "ERROR":
		return LogLevelError
	case "FATAL":
		return LogLevelFatal
	default:
		return LogLevelInfo
	}
}
