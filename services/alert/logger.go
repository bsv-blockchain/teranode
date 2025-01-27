// Package alert implements the Bitcoin SV alert system server and related functionality.
package alert

import (
	"fmt"

	"github.com/bitcoin-sv/teranode/ulogger"
)

type Logger struct {
	// Logger is the logger wrapper for the alert service
	ulogger ulogger.Logger
}

// NewLogger will return a new logger instance
func NewLogger(logger ulogger.Logger) *Logger {
	return &Logger{
		ulogger: logger,
	}
}

func (l *Logger) Debug(args ...interface{}) {
	l.ulogger.Debugf("%s", args...)
}

func (l *Logger) Debugf(msg string, args ...interface{}) {
	l.ulogger.Debugf(msg, args...)
}

func (l *Logger) Error(args ...interface{}) {
	l.ulogger.Errorf("%s", args...)
}

func (l *Logger) ErrorWithStack(msg string, args ...interface{}) {
	l.ulogger.Errorf(msg, args...)
}

func (l *Logger) Errorf(msg string, args ...interface{}) {
	l.ulogger.Errorf(msg, args...)
}

func (l *Logger) Fatal(args ...interface{}) {
	l.ulogger.Fatalf("%s", args...)
}

func (l *Logger) Fatalf(msg string, args ...interface{}) {
	l.ulogger.Fatalf(msg, args...)
}

func (l *Logger) Info(args ...interface{}) {
	l.ulogger.Infof("%s", args...)
}

func (l *Logger) Infof(msg string, args ...interface{}) {
	l.ulogger.Infof(msg, args...)
}

func (l *Logger) LogLevel() string {
	return ulogger.LogLevelString(l.ulogger.LogLevel())
}

func (l *Logger) Panic(args ...interface{}) {
	panic(args)
}

func (l *Logger) Panicf(msg string, args ...interface{}) {
	panic(fmt.Sprintf(msg, args...))
}

func (l *Logger) Warn(args ...interface{}) {
	l.ulogger.Warnf("%s", args...)
}

func (l *Logger) Warnf(msg string, args ...interface{}) {
	l.ulogger.Warnf(msg, args...)
}

func (l *Logger) Printf(format string, v ...interface{}) {
	l.ulogger.Infof(format, v...)
}

func (l *Logger) CloseWriter() error {
	return nil
}
