package ulogger

import (
	"github.com/ordishs/gocore"
)

type GoCoreLogger struct {
	*gocore.Logger
	skipFrame int
}

func NewGoCoreLogger(service string, options ...Option) *GoCoreLogger {
	if service == "" {
		service = "ubsv"
	}

	opts := DefaultOptions()
	for _, o := range options {
		o(opts)
	}

	// TODO fix this linux only issue ???
	// if _, ok := opts.writer.(*os.File); !ok {
	//	 panic("GoCoreLogger only supports stdout")
	// }

	return &GoCoreLogger{gocore.Log(service, gocore.NewLogLevelFromString(opts.logLevel)), opts.skip}
}

func (g *GoCoreLogger) New(service string, options ...Option) Logger {
	opts := DefaultOptions()
	for _, o := range options {
		o(opts)
	}

	return &GoCoreLogger{
		gocore.Log(service, g.Logger.GetLogLevel()),
		opts.skip,
	}
}

func (g *GoCoreLogger) Duplicate(options ...Option) Logger {
	newLogger := &GoCoreLogger{g.Logger, g.skipFrame}

	defaultOpts := DefaultOptions()
	opts := DefaultOptions()

	for _, o := range options {
		o(opts)
	}

	if opts.logLevel != defaultOpts.logLevel {
		newLogger.SetLogLevel(opts.logLevel)
	}

	if opts.skip != defaultOpts.skip {
		newLogger.skipFrame = opts.skip
	}

	return newLogger
}

func (g *GoCoreLogger) SetLogLevel(_ string) {
	// noop, has to be set when creating
}
