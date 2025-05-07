package ulogger

import (
	"sync"
	"testing"
)

type VerboseTestLogger struct {
	t     *testing.T
	mutex sync.RWMutex
}

func NewVerboseTestLogger(t *testing.T) *VerboseTestLogger {
	return &VerboseTestLogger{
		t: t,
	}
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
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	if l.t != nil {
		l.t.Logf("[DEBUG] "+format, args...)
	}
}

func (l *VerboseTestLogger) Infof(format string, args ...interface{}) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	if l.t != nil {
		l.t.Logf("[INFO] "+format, args...)
	}
}

func (l *VerboseTestLogger) Warnf(format string, args ...interface{}) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	if l.t != nil {
		l.t.Logf("[WARN] "+format, args...)
	}
}

func (l *VerboseTestLogger) Errorf(format string, args ...interface{}) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	if l.t != nil {
		l.t.Logf("[ERROR] "+format, args...)
	}
}

func (l *VerboseTestLogger) Fatalf(format string, args ...interface{}) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	if l.t != nil {
		l.t.Fatalf("[FATAL] "+format, args...)
	}
}
