package ulogger

import (
	"context"
	"fmt"
	"log"
	"runtime"
)

type TestLogger struct{}

func (l TestLogger) LogLevel() int {
	return 0
}
func (l TestLogger) SetLogLevel(level string) {
	// ignore
}

func (l TestLogger) New(service string, options ...Option) Logger {
	return TestLogger{}
}
func (l TestLogger) Duplicate(options ...Option) Logger {
	return TestLogger{}
}
func (l TestLogger) Debugf(format string, args ...interface{}) {
	// ignore
}
func (l TestLogger) Infof(format string, args ...interface{}) {
	// ignore
}
func (l TestLogger) Warnf(format string, args ...interface{}) {
	// ignore
}
func (l TestLogger) Errorf(format string, args ...interface{}) {
	_, file, line, _ := runtime.Caller(2)

	prefix := fmt.Sprintf("%s:%d: ERR_LEVEL %s ", file, line, format)

	log.Printf(prefix, args...)
}
func (l TestLogger) Fatalf(format string, args ...interface{}) {
	_, file, line, _ := runtime.Caller(2)

	prefix := fmt.Sprintf("%s:%d: ERR_LEVEL %s ", file, line, format)

	log.Printf(prefix, args...)
}

// WithTraceContext returns the same logger since TestLogger is a no-op test logger.
func (l TestLogger) WithTraceContext(_ context.Context) Logger {
	return l
}
