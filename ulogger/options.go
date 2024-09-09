package ulogger

import (
	"io"
	"os"
	"strings"

	"github.com/rs/zerolog"
	"golang.org/x/term"
)

type Options struct {
	logLevel   string
	loggerType string
	writer     io.Writer
	filepath   string
	skip       int
}

type Option func(*Options)

func DefaultOptions() *Options {
	isTerminal := term.IsTerminal(int(os.Stdout.Fd()))

	output := zerolog.ConsoleWriter{
		Out:     os.Stdout,
		NoColor: !isTerminal, // Disable color if output is not a terminal
	}

	return &Options{
		logLevel:   "INFO",
		loggerType: "zerolog",
		writer:     output,
		filepath:   "citest.log",
		skip:       0,
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

func WithFilePath(filePath string) Option {
	return func(o *Options) {
		o.filepath = filePath
	}
}

// WithSkipFrame sets the number of frames to skip when logging
// This is useful when you want to log the calling function from a wrapper function
func WithSkipFrame(skip int) Option {
	return func(o *Options) {
		o.skip = skip
	}
}

func LogLevelString(level int) string {
	switch level {
	case 0:
		return "DEBUG"
	case 1:
		return "INFO"
	case 2:
		return "WARN"
	case 3:
		return "ERROR"
	case 4:
		return "FATAL"
	default:
		return "INFO"
	}
}
