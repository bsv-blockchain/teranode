package util

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/rs/zerolog"
)

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

type ZLoggerWrapper struct {
	zerolog.Logger
	service string
}

func NewLogger(service string, logLevel ...string) utils.Logger {
	useLogger, _ := gocore.Config().Get("logger", "zerolog")
	switch useLogger {
	case "gocore":
		if len(logLevel) > 0 {
			l := gocore.NewLogLevelFromString(logLevel[0])
			return gocore.Log(service, l)
		} else {
			return gocore.Log(service)
		}
	default:
		return NewZeroLogger(service, logLevel...)
	}
}

func NewZeroLogger(service string, logLevel ...string) *ZLoggerWrapper {
	if service == "" {
		service = "ubsv"
	}

	var z *ZLoggerWrapper
	if gocore.Config().GetBool("PRETTY_LOGS", true) {
		z = prettyZeroLogger(service)
	} else {
		z = &ZLoggerWrapper{
			zerolog.New(os.Stdout).With().
				CallerWithSkipFrameCount(zerolog.CallerSkipFrameCount + 2).
				Timestamp().
				Logger(),
			service,
		}
	}

	if len(logLevel) > 0 {
		setZerologLogLevel(logLevel[0], z)
	}

	return z
}

func setZerologLogLevel(logLevel string, z *ZLoggerWrapper) {
	switch strings.ToUpper(logLevel) {
	case "DEBUG":
		z.Logger.Level(zerolog.DebugLevel)
	case "INFO":
		z.Logger.Level(zerolog.InfoLevel)
	case "WARN":
		z.Logger.Level(zerolog.WarnLevel)
	case "ERROR":
		z.Logger.Level(zerolog.ErrorLevel)
	case "FATAL":
		z.Logger.Level(zerolog.FatalLevel)
	case "PANIC":
		z.Logger.Level(zerolog.PanicLevel)
	default:
		z.Logger.Level(zerolog.InfoLevel)
	}
}

func prettyZeroLogger(service string) *ZLoggerWrapper {
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}

	output.FormatTimestamp = func(i interface{}) string {
		parse, _ := time.Parse(time.RFC3339, i.(string))
		return parse.Format("15:04:05")
	}

	output.FormatLevel = func(i interface{}) string {
		l := strings.ToUpper(fmt.Sprintf("%-6s", i))

		// colorize the levels in same way as gocore
		switch i {
		case "debug":
			l = colorize(l, colorBlue, false)
		case "info":
			l = colorize(l, colorGreen, false)
		case "warn":
			l = colorize(l, colorYellow, false)
		case "error":
			l = colorize(l, colorRed, false)
		case "fatal":
			l = colorize(l, colorRed, false)
		case "panic":
			l = colorize(l, colorRed, false)
		default:
			l = colorize(l, colorWhite, false)
		}

		return fmt.Sprintf("| %s|", l)
	}

	output.FormatMessage = func(i interface{}) string {
		return fmt.Sprintf("| %-6s| %s", service, i)
	}

	output.FormatFieldName = func(i interface{}) string {
		return fmt.Sprintf("%s:", i)
	}

	output.FormatFieldValue = func(i interface{}) string {
		return strings.ToUpper(fmt.Sprintf("%s", i))
	}

	output.FormatCaller = func(i interface{}) string {
		var c string
		if cc, ok := i.(string); ok {
			c = cc
		}
		if len(c) > 0 {
			if cwd, err := os.Getwd(); err == nil {
				if rel, err := filepath.Rel(cwd, c); err == nil {
					c = rel
				}
			}

			split := strings.Split(c, "/")
			currentElement := len(split) - 1
			c = split[currentElement]
			currentElement--
			for {
				if currentElement < 0 {
					break
				}
				if len(c)+len(split[currentElement])+1 > 32 {
					break
				} else {
					c = split[currentElement] + "/" + c
					currentElement--
				}
			}

			c = colorize(fmt.Sprintf("%-32s", c), colorBold, false)
		}
		return c
	}

	return &ZLoggerWrapper{
		zerolog.New(output).With().
			CallerWithSkipFrameCount(zerolog.CallerSkipFrameCount + 1).
			Timestamp().
			Logger(),
		service,
	}
}

func (z *ZLoggerWrapper) LogLevel() int {
	switch z.Logger.GetLevel() {
	case zerolog.DebugLevel:
		return int(gocore.DEBUG)
	case zerolog.InfoLevel:
		return int(gocore.INFO)
	case zerolog.WarnLevel:
		return int(gocore.WARN)
	case zerolog.ErrorLevel:
		return int(gocore.ERROR)
	case zerolog.FatalLevel:
		return int(gocore.FATAL)
	default:
		return int(gocore.INFO)
	}
}

func (z *ZLoggerWrapper) Debugf(format string, args ...interface{}) {
	z.Logger.Debug().Msgf(format, args...)
}

func (z *ZLoggerWrapper) Infof(format string, args ...interface{}) {
	z.Logger.Info().Msgf(format, args...)
}

func (z *ZLoggerWrapper) Warnf(format string, args ...interface{}) {
	z.Logger.Warn().Msgf(format, args...)
}

func (z *ZLoggerWrapper) Errorf(format string, args ...interface{}) {
	z.Logger.Error().Msgf(format, args...)
}

func (z *ZLoggerWrapper) Fatalf(format string, args ...interface{}) {
	z.Logger.Fatal().Msgf(format, args...)
}

// Output duplicates the current logger and sets w as its output.
func (z *ZLoggerWrapper) Output(w io.Writer) *ZLoggerWrapper {
	return &ZLoggerWrapper{z.Logger.Output(w), z.service}
}

// With creates a child logger with the field added to its context.
func (z *ZLoggerWrapper) With() zerolog.Context {
	return z.Logger.With()
}

// UpdateContext updates the internal logger's context.
//
// Caution: This method is not concurrency safe.
// Use the With method to create a child logger before modifying the context from concurrent goroutines.
func (z *ZLoggerWrapper) UpdateContext(update func(c zerolog.Context) zerolog.Context) {
	z.Logger.UpdateContext(update)
}

// Level creates a child logger with the minimum accepted level set to level.
func (z *ZLoggerWrapper) Level(lvl zerolog.Level) zerolog.Logger {
	return z.Logger.Level(lvl)
}

// GetLevel returns the current Level of l.
func (z *ZLoggerWrapper) GetLevel() zerolog.Level {
	return z.Logger.GetLevel()
}

// Sample returns a logger with the s sampler.
func (z *ZLoggerWrapper) Sample(s zerolog.Sampler) zerolog.Logger {
	return z.Logger.Sample(s)
}

// Hook returns a logger with the h Hook.
func (z *ZLoggerWrapper) Hook(h zerolog.Hook) zerolog.Logger {
	return z.Logger.Hook(h)
}

// Trace starts a new message with trace level.
//
// You must call Msg on the returned event in order to send the event.
func (z *ZLoggerWrapper) Trace() *zerolog.Event {
	return z.Logger.Trace()
}

// Debug starts a new message with debug level.
//
// You must call Msg on the returned event in order to send the event.
func (z *ZLoggerWrapper) Debug() *zerolog.Event {
	return z.Logger.Debug()
}

// Info starts a new message with info level.
//
// You must call Msg on the returned event in order to send the event.
func (z *ZLoggerWrapper) Info() *zerolog.Event {
	return z.Logger.Info()
}

// Warn starts a new message with warn level.
//
// You must call Msg on the returned event in order to send the event.
func (z *ZLoggerWrapper) Warn() *zerolog.Event {
	return z.Logger.Warn()
}

// Error starts a new message with error level.
//
// You must call Msg on the returned event in order to send the event.
func (z *ZLoggerWrapper) Error() *zerolog.Event {
	return z.Logger.Error()
}

// Err starts a new message with error level with err as a field if not nil or
// with info level if err is nil.
//
// You must call Msg on the returned event in order to send the event.
func (z *ZLoggerWrapper) Err(err error) *zerolog.Event {
	return z.Logger.Err(err)
}

// Fatal starts a new message with fatal level. The os.Exit(1) function
// is called by the Msg method, which terminates the program immediately.
//
// You must call Msg on the returned event in order to send the event.
func (z *ZLoggerWrapper) Fatal() *zerolog.Event {
	return z.Logger.Fatal()
}

// Panic starts a new message with panic level. The panic() function
// is called by the Msg method, which stops the ordinary flow of a goroutine.
//
// You must call Msg on the returned event in order to send the event.
func (z *ZLoggerWrapper) Panic() *zerolog.Event {
	return z.Logger.Panic()
}

// WithLevel starts a new message with level. Unlike Fatal and Panic
// methods, WithLevel does not terminate the program or stop the ordinary
// flow of a goroutine when used with their respective levels.
//
// You must call Msg on the returned event in order to send the event.
func (z *ZLoggerWrapper) WithLevel(level zerolog.Level) *zerolog.Event {
	return z.Logger.WithLevel(level)
}

// Log starts a new message with no level. Setting GlobalLevel to Disabled
// will still disable events produced by this method.
//
// You must call Msg on the returned event in order to send the event.
func (z *ZLoggerWrapper) Log() *zerolog.Event {
	return z.Logger.Log()
}

// Print sends a log event using debug level and no extra field.
// Arguments are handled in the manner of fmt.Print.
func (z *ZLoggerWrapper) Print(v ...interface{}) {
	z.Logger.Print(v...)
}

// Printf sends a log event using debug level and no extra field.
// Arguments are handled in the manner of fmt.Printf.
func (z *ZLoggerWrapper) Printf(format string, v ...interface{}) {
	z.Logger.Printf(format, v...)
}

// Write implements the io.Writer interface. This is useful to set as a writer
// for the standard library log.
func (z *ZLoggerWrapper) Write(p []byte) (n int, err error) {
	return z.Logger.Write(p)
}

// colorize returns the string s wrapped in ANSI code c, unless disabled is true or c is 0.
func colorize(s interface{}, c int, disabled bool) string {
	e := os.Getenv("NO_COLOR")
	if e != "" || c == 0 {
		disabled = true
	}

	if disabled {
		return fmt.Sprintf("%s", s)
	}
	return fmt.Sprintf("\x1b[%dm%v\x1b[0m", c, s)
}
