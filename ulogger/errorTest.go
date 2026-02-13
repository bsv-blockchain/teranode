package ulogger

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
)

type TestingT interface {
	Errorf(format string, args ...interface{})
	FailNow()
	Logf(format string, args ...any)
}

type tHelper = interface {
	Helper()
}

type ErrorTestLogger struct {
	t                TestingT
	skipCancelOnFail atomic.Bool
	cancelFn         func()
	shutdown         atomic.Bool // Prevents logging after test cleanup
}

func NewErrorTestLogger(t TestingT, cancelFn ...func()) *ErrorTestLogger {
	if len(cancelFn) == 0 {
		return &ErrorTestLogger{
			t: t,
		}
	}

	return &ErrorTestLogger{
		t:        t,
		cancelFn: cancelFn[0],
	}
}

func (l *ErrorTestLogger) SetCancelFn(cancelFn func()) {
	l.cancelFn = cancelFn
}

func (l *ErrorTestLogger) EnableVerbose() {
}

func (l *ErrorTestLogger) SkipCancelOnFail(skip bool) {
	if h, ok := l.t.(tHelper); ok {
		h.Helper()
	}

	l.skipCancelOnFail.Store(skip)
}

// Shutdown marks the logger as shutdown, preventing further access to testing.T
// This should be called before test cleanup to avoid race conditions
func (l *ErrorTestLogger) Shutdown() {
	l.shutdown.Store(true)
}

func (l *ErrorTestLogger) LogLevel() int {
	return 0
}

func (l *ErrorTestLogger) SetLogLevel(level string) {}

func (l *ErrorTestLogger) New(service string, options ...Option) Logger {
	if h, ok := l.t.(tHelper); ok {
		h.Helper()
	}

	return l
}

func (l *ErrorTestLogger) Duplicate(options ...Option) Logger {
	if h, ok := l.t.(tHelper); ok {
		h.Helper()
	}

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
	// Don't access testing.T if logger is shutdown (test is cleaning up)
	if l.shutdown.Load() {
		return
	}

	if h, ok := l.t.(tHelper); ok {
		h.Helper()
	}

	_, file, line, _ := runtime.Caller(2)

	prefix := fmt.Sprintf("%s:%d: ERR_LEVEL %s ", file, line, format)

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
	// Don't access testing.T if logger is shutdown (test is cleaning up)
	if l.shutdown.Load() {
		return
	}

	if h, ok := l.t.(tHelper); ok {
		h.Helper()
	}

	_, file, line, _ := runtime.Caller(2)

	prefix := fmt.Sprintf("%s:%d: FATAL_LEVEL %s ", file, line, format)

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

// WithTraceContext returns the same logger since ErrorTestLogger is a test logger
// that doesn't support structured fields.
func (l *ErrorTestLogger) WithTraceContext(_ context.Context) Logger {
	return l
}
