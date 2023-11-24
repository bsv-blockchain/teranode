package ulogger

import (
	"io"
	"os"
	"strings"
)

type Options struct {
	logLevel   string
	loggerType string
	writer     io.Writer
}

type Option func(*Options)

func DefaultOptions() *Options {
	return &Options{
		logLevel:   "INFO",
		loggerType: "zerolog",
		writer:     os.Stdout,
	}
}

// WithLoggerType sets the logger type
func WithLoggerType(loggerType string) Option {
	return func(o *Options) {
		o.loggerType = strings.ToLower(loggerType)
	}
}

// WithLevel sets the level of the logger
func WithLevel(level string) Option {
	return func(o *Options) {
		o.logLevel = strings.ToUpper(level)
	}
}

// WithWriter sets the writer of the logger
func WithWriter(writer io.Writer) Option {
	return func(o *Options) {
		o.writer = writer
	}
}
