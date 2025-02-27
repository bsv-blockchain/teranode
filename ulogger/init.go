package ulogger

import (
	"os"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/rs/zerolog"
	"golang.org/x/term"
)

func InitLogger(progname string, tSettings *settings.Settings) Logger {
	logLevel := tSettings.LogLevel
	logOptions := []Option{
		WithLevel(logLevel),
	}

	isTerminal := term.IsTerminal(int(os.Stdout.Fd()))

	output := zerolog.ConsoleWriter{
		Out:     os.Stdout,
		NoColor: !isTerminal, // Disable color if output is not a terminal
	}

	logOptions = append(logOptions, WithWriter(output))

	useLogger := tSettings.Logger
	if useLogger != "" {
		logOptions = append(logOptions, WithLoggerType(useLogger))
	}

	logger := New(progname, logOptions...)

	return logger
}
