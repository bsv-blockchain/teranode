package ulogger

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
)

type ErrorTestLogger struct {
	t                *testing.T
	skipCancelOnFail atomic.Bool
	cancelFn         func()
}

func NewErrorTestLogger(t *testing.T, cancelFn func()) *ErrorTestLogger {
	return &ErrorTestLogger{
		t:        t,
		cancelFn: cancelFn,
	}
}

func (l *ErrorTestLogger) SetCancelFn(cancelFn func()) {
	l.cancelFn = cancelFn
}

func (l *ErrorTestLogger) EnableVerbose() {
}

func (l *ErrorTestLogger) SkipCancelOnFail(skip bool) {
	l.t.Helper()

	l.skipCancelOnFail.Store(skip)
}

func (l *ErrorTestLogger) LogLevel() int {
	return 0
}

func (l *ErrorTestLogger) SetLogLevel(level string) {}

func (l *ErrorTestLogger) New(service string, options ...Option) Logger {
	l.t.Helper()

	return l
}

func (l *ErrorTestLogger) Duplicate(options ...Option) Logger {
	l.t.Helper()

	return l
}

func (l *ErrorTestLogger) Debugf(format string, args ...interface{}) {
	// l.t.Logf("[DEBUG] "+format, args...)
}

func (l *ErrorTestLogger) Infof(format string, args ...interface{}) {
	// l.t.Logf("[INFO] "+format, args...)
}

func (l *ErrorTestLogger) Warnf(format string, args ...interface{}) {
	// l.t.Logf("[WARN] "+format, args...)
}

func (l *ErrorTestLogger) Errorf(format string, args ...interface{}) {
	l.t.Helper()

	_, file, line, _ := runtime.Caller(2)

	prefix := fmt.Sprintf("%s:%d: [ERROR] %s ", file, line, format)

	if l.skipCancelOnFail.Load() {
		l.t.Logf(prefix, args...)
		return
	}

	l.t.Logf(prefix, args...)
	// if l.cancelFn != nil {
	// 	l.cancelFn()
	// }

	// l.t.FailNow()
}

func (l *ErrorTestLogger) Fatalf(format string, args ...interface{}) {
	l.t.Helper()

	_, file, line, _ := runtime.Caller(2)

	prefix := fmt.Sprintf("%s:%d: [FATAL] %s ", file, line, format)

	if l.skipCancelOnFail.Load() {
		l.t.Logf(prefix, args...)
		return
	}

	l.t.Logf(prefix, args...)
	// if l.cancelFn != nil {
	// 	l.cancelFn()
	// }

	// l.t.FailNow()
}
