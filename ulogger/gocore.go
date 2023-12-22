package ulogger

import (
	"github.com/ordishs/gocore"
)

type GoCoreLogger struct {
	*gocore.Logger
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
	//if _, ok := opts.writer.(*os.File); !ok {
	//	panic("GoCoreLogger only supports stdout")
	//}

	return &GoCoreLogger{gocore.Log(service, gocore.NewLogLevelFromString(opts.logLevel))}
}

func (g *GoCoreLogger) New(service string, options ...Option) Logger {
	return &GoCoreLogger{
		gocore.Log(service, g.Logger.GetLogLevel()),
	}
}

func (g *GoCoreLogger) SetLogLevel(_ string) {
	// noop, has to be set when creating
}
