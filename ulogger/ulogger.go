package ulogger

const (
	colorBlack = iota + 30
	colorRed
	colorGreen
	colorYellow
	colorBlue
	colorMagenta
	colorCyan
	colorWhite

	colorBold     = 1
	colorDarkGray = 90
)

type Logger interface {
	LogLevel() int
	SetLogLevel(level string)
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
	New(service string, options ...Option) Logger
	Duplicate(options ...Option) Logger
}

func New(service string, options ...Option) Logger {
	opts := DefaultOptions()
	for _, o := range options {
		o(opts)
	}

	switch opts.loggerType {
	case "gocore":
		return NewGoCoreLogger(service, options...)
	case "file":
		return NewFileLogger(service, options...)
	default:
		return NewZeroLogger(service, options...)
	}
}
