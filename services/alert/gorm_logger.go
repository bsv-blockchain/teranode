// Package alert implements the Bitcoin SV alert system server and related functionality.
package alert

import (
	"context"
	"time"

	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/mrz1836/go-logger"
)

type GormLogger struct {
	// GormLogger is the logger wrapper for the alert service
	ulogger ulogger.Logger
}

// NewGormLogger will return a new Gorm compatible logger instance
func NewGormLogger(logger ulogger.Logger) *GormLogger {
	return &GormLogger{
		ulogger: logger,
	}
}

func (g *GormLogger) Error(_ context.Context, s string, i ...interface{}) {
	g.ulogger.Errorf(s, i...)
}

func (g *GormLogger) GetMode() logger.GormLogLevel {
	// TODO
	return logger.GormLogLevel(0)
}

func (g *GormLogger) GetStackLevel() int {
	return 0
}

func (g *GormLogger) Info(_ context.Context, s string, i ...interface{}) {
	g.ulogger.Infof(s, i...)
}

func (g *GormLogger) SetMode(_ logger.GormLogLevel) logger.GormLoggerInterface {
	return g
}

func (g *GormLogger) SetStackLevel(_ int) {}

func (g *GormLogger) Trace(_ context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
}

func (g *GormLogger) Warn(_ context.Context, s string, i ...interface{}) {
	g.ulogger.Warnf(s, i...)
}
