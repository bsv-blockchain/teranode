package main

import (
	_ "net/http/pprof" //nolint:gosec // Import for pprof, only enabled via CLI flag
	"os"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/ordishs/gocore"
	"github.com/rs/zerolog"
	"golang.org/x/term"
)

// Name used by build script for the binaries. (Please keep on single line)
const progname = "teranode"

// Version & commit strings injected at build with -ldflags -X...
var version string
var commit string

func init() {
	gocore.SetInfo(progname, version, commit)

	// Call the gocore.Log function to initialize the logger and start the Unix domain socket that allows us to configure settings at runtime.
	gocore.Log(progname)

	gocore.AddAppPayloadFn("CONFIG", func() interface{} {
		return gocore.Config().GetAll()
	})
}

func main() {
	tSettings := settings.NewSettings()

	logger := initLogger(progname, tSettings)

	stats := gocore.Config().Stats()
	logger.Infof("STATS\n%s\nVERSION\n-------\n%s (%s)\n\n", stats, version, commit)

	daemon.New().Start(logger, os.Args[1:], tSettings)
}

func initLogger(progname string, tSettings *settings.Settings) ulogger.Logger {
	logLevel := tSettings.LogLevel
	logOptions := []ulogger.Option{
		ulogger.WithLevel(logLevel),
	}

	isTerminal := term.IsTerminal(int(os.Stdout.Fd()))

	output := zerolog.ConsoleWriter{
		Out:     os.Stdout,
		NoColor: !isTerminal, // Disable color if output is not a terminal
	}

	logOptions = append(logOptions, ulogger.WithWriter(output))

	useLogger := tSettings.Logger
	if useLogger != "" {
		logOptions = append(logOptions, ulogger.WithLoggerType(useLogger))
	}

	logger := ulogger.New(progname, logOptions...)

	return logger
}
