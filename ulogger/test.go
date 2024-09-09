package ulogger

type TestLogger struct{}

func (l TestLogger) LogLevel() int {
	return 0
}
func (l TestLogger) SetLogLevel(level string) {}
func (l TestLogger) New(service string, options ...Option) Logger {
	return TestLogger{}
}
func (l TestLogger) Duplicate(options ...Option) Logger {
	return TestLogger{}
}
func (l TestLogger) Debugf(format string, args ...interface{}) {}
func (l TestLogger) Infof(format string, args ...interface{})  {}
func (l TestLogger) Warnf(format string, args ...interface{})  {}
func (l TestLogger) Errorf(format string, args ...interface{}) {}
func (l TestLogger) Fatalf(format string, args ...interface{}) {}
