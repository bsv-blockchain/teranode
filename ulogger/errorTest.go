package ulogger

import (
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
	if h, ok := l.t.(tHelper); ok {
		h.Helper()
	}

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
	if h, ok := l.t.(tHelper); ok {
		h.Helper()
	}

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
