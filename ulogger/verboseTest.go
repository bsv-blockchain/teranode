package ulogger

import (
	"sync/atomic"
	"testing"
)

type VerboseTestLogger struct {
	t atomic.Pointer[testing.T]
}

func NewVerboseTestLogger(t *testing.T) *VerboseTestLogger {
	l := &VerboseTestLogger{}
	l.t.Store(t)

	return l
}

func (l *VerboseTestLogger) LogLevel() int {
	return 0
}

func (l *VerboseTestLogger) SetLogLevel(level string) {
	// No-op
}

func (l *VerboseTestLogger) New(service string, options ...Option) Logger {
	return l
}

func (l *VerboseTestLogger) Duplicate(options ...Option) Logger {
	return l
}

func (l *VerboseTestLogger) Debugf(format string, args ...interface{}) {
	t := l.t.Load()
	if t != nil {
		t.Logf("[DEBUG] "+format, args...)
	}
}

func (l *VerboseTestLogger) Infof(format string, args ...interface{}) {
	t := l.t.Load()
	if t != nil {
		t.Logf("[INFO] "+format, args...)
	}
}

func (l *VerboseTestLogger) Warnf(format string, args ...interface{}) {
	t := l.t.Load()
	if t != nil {
		t.Logf("[WARN] "+format, args...)
	}
}

func (l *VerboseTestLogger) Errorf(format string, args ...interface{}) {
	t := l.t.Load()
	if t != nil {
		t.Logf("[ERROR] "+format, args...)
	}
}

func (l *VerboseTestLogger) Fatalf(format string, args ...interface{}) {
	t := l.t.Load()
	if t != nil {
		t.Fatalf("[FATAL] "+format, args...)
	}
}
