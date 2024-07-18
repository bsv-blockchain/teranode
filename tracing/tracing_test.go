package tracing

import (
	"context"
	"fmt"
	"testing"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/stretchr/testify/assert"
)

type lineLogger struct {
	lastLog string
}

func newLineLogger() *lineLogger {
	return &lineLogger{}
}

func (l *lineLogger) New(service string, options ...ulogger.Option) ulogger.Logger {
	return nil
}
func (l *lineLogger) LogLevel() int {
	return 0
}
func (l *lineLogger) SetLogLevel(level string) {}

func (l *lineLogger) Debugf(format string, args ...interface{}) {
	l.log("DEBUG", format, args...)
}

func (l *lineLogger) Infof(format string, args ...interface{}) {
	l.log("INFO", format, args...)
}

func (l *lineLogger) Warnf(format string, args ...interface{}) {
	l.log("WARN", format, args...)
}

func (l *lineLogger) Errorf(format string, args ...interface{}) {
	l.log("ERROR", format, args...)
}

func (l *lineLogger) Fatalf(format string, args ...interface{}) {
	l.log("FATAL", format, args...)
}

func (l *lineLogger) log(level string, format string, args ...interface{}) {
	l.lastLog = fmt.Sprintf(format, args...)
}

func TestTraceing(t *testing.T) {
	logger := newLineLogger()

	_, _, deferFn := StartTracing(
		context.Background(),
		"TestTracing",
		WithLogMessage(
			logger,
			"%s %s",
			"hello",
			"world",
		),
	)

	assert.Equal(t, "hello world", logger.lastLog)

	deferFn()

	assert.Contains(t, logger.lastLog, "hello world DONE in")

}
