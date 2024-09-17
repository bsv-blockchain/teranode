package ulogger

import "testing"

type VerboseTestLogger struct {
	t *testing.T
}

func NewVerboseTestLogger(t *testing.T) *VerboseTestLogger {
	return &VerboseTestLogger{t: t}
}

func (l *VerboseTestLogger) LogLevel() int {
	return 0
}

func (l *VerboseTestLogger) SetLogLevel(level string) {}

func (l *VerboseTestLogger) New(service string, options ...Option) Logger {
	return l
}

func (l *VerboseTestLogger) Duplicate(options ...Option) Logger {
	return l
}

func (l *VerboseTestLogger) Debugf(format string, args ...interface{}) {
	l.t.Logf("[DEBUG] "+format, args...)
}

func (l *VerboseTestLogger) Infof(format string, args ...interface{}) {
	l.t.Logf("[INFO] "+format, args...)
}

func (l *VerboseTestLogger) Warnf(format string, args ...interface{}) {
	l.t.Logf("[WARN] "+format, args...)
}

func (l *VerboseTestLogger) Errorf(format string, args ...interface{}) {
	l.t.Logf("[ERROR] "+format, args...)
}

func (l *VerboseTestLogger) Fatalf(format string, args ...interface{}) {
	l.t.Fatalf("[FATAL] "+format, args...)
}
