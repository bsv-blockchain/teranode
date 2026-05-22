package ulogger

import (
	"os"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/rs/zerolog"
)

func InitLogger(progname string, tSettings *settings.Settings) Logger {
	logLevel := tSettings.LogLevel
	logOptions := []Option{
		WithLevel(logLevel),
	}

	output := zerolog.ConsoleWriter{
		Out:     os.Stdout,
		NoColor: !isStdoutTerminal(), // Disable color if output is not a terminal
	}

	logOptions = append(logOptions, WithWriter(output))

	useLogger := tSettings.Logger
	if useLogger != "" {
		logOptions = append(logOptions, WithLoggerType(useLogger))
	}

	logger := New(progname, logOptions...)

	return logger
}
