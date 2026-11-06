package ulogger

import (
	"io"
	"testing"

	"github.com/ordishs/gocore"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

var duplicateBenchmarkSink Logger

func TestZeroLoggerDuplicatePreservesOptions(t *testing.T) {
	parent := &ZLoggerWrapper{
		Logger:    zerolog.New(io.Discard).Level(zerolog.WarnLevel),
		service:   "test",
		w:         io.Discard,
		skipFrame: 3,
	}

	tests := []struct {
		name    string
		options []Option
		level   zerolog.Level
		skip    int
	}{
		{name: "inherit", level: zerolog.WarnLevel, skip: 3},
		{name: "level", options: []Option{WithLevel("ERROR")}, level: zerolog.ErrorLevel, skip: 3},
		{name: "default level inherits", options: []Option{WithLevel("INFO")}, level: zerolog.WarnLevel, skip: 3},
		{name: "skip", options: []Option{WithSkipFrame(2)}, level: zerolog.WarnLevel, skip: 2},
		{name: "default skip inherits", options: []Option{WithSkipFrame(0)}, level: zerolog.WarnLevel, skip: 3},
		{name: "increment", options: []Option{WithSkipFrameIncrement(2)}, level: zerolog.WarnLevel, skip: 5},
		{name: "negative increment ignored", options: []Option{WithSkipFrameIncrement(-1)}, level: zerolog.WarnLevel, skip: 3},
		{name: "combined", options: []Option{WithSkipFrame(2), WithSkipFrameIncrement(3), WithLevel("debug")}, level: zerolog.DebugLevel, skip: 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			duplicate, ok := parent.Duplicate(tt.options...).(*ZLoggerWrapper)
			require.True(t, ok)
			require.NotSame(t, parent, duplicate)
			require.Equal(t, tt.level, duplicate.GetLevel())
			require.Equal(t, tt.skip, duplicate.skipFrame)
			require.Equal(t, parent.service, duplicate.service)
			require.Equal(t, parent.w, duplicate.w)
			require.Equal(t, zerolog.WarnLevel, parent.GetLevel())
			require.Equal(t, 3, parent.skipFrame)
		})
	}
}

func TestZeroLoggerDuplicateAllocations(t *testing.T) {
	parent := &ZLoggerWrapper{Logger: zerolog.New(io.Discard)}
	option := WithSkipFrameIncrement(2)
	allocs := testing.AllocsPerRun(1000, func() {
		duplicateBenchmarkSink = parent.Duplicate(option)
	})
	require.LessOrEqual(t, allocs, float64(2), "duplication should allocate only the wrapper and applied options")
}

func TestDuplicatePreservesBackendState(t *testing.T) {
	t.Run("gocore", func(t *testing.T) {
		// Construct the wrapper directly: the gocore constructor starts a Unix
		// socket listener, which is unrelated to copying an existing logger.
		parent := &GoCoreLogger{Logger: &gocore.Logger{}, skipFrame: 3}
		for _, options := range [][]Option{nil, {WithSkipFrame(1), WithSkipFrameIncrement(2), WithLevel("ERROR")}} {
			duplicate, ok := parent.Duplicate(options...).(*GoCoreLogger)
			require.True(t, ok)
			require.NotSame(t, parent, duplicate)
			require.Same(t, parent.Logger, duplicate.Logger)
			require.Equal(t, 3, duplicate.skipFrame)
		}
	})

	t.Run("file", func(t *testing.T) {
		parent := &FileLogger{service: "test", logLevel: LogLevelWarning, writer: io.Discard, skipFrame: 3}
		duplicate, ok := parent.Duplicate(WithSkipFrame(1), WithSkipFrameIncrement(2), WithLevel("ERROR")).(*FileLogger)
		require.True(t, ok)
		require.NotSame(t, parent, duplicate)
		require.Equal(t, LogLevelError, duplicate.logLevel)
		require.Equal(t, LogLevelWarning, parent.logLevel)
		require.Equal(t, 3, duplicate.skipFrame)
		require.Equal(t, parent.service, duplicate.service)
		require.Equal(t, parent.writer, duplicate.writer)
	})
}

func TestZeroLoggerDebugEnabled(t *testing.T) {
	original := zerolog.GlobalLevel()
	t.Cleanup(func() { zerolog.SetGlobalLevel(original) })

	for _, tt := range []struct {
		name      string
		primary   zerolog.Level
		secondary *zerolog.Level
		global    zerolog.Level
		enabled   bool
	}{
		{name: "info", primary: zerolog.InfoLevel, global: zerolog.TraceLevel},
		{name: "debug", primary: zerolog.DebugLevel, global: zerolog.TraceLevel, enabled: true},
		{name: "trace", primary: zerolog.TraceLevel, global: zerolog.TraceLevel, enabled: true},
		{name: "global threshold", primary: zerolog.DebugLevel, global: zerolog.ErrorLevel},
		{name: "secondary debug", primary: zerolog.InfoLevel, secondary: levelPointer(zerolog.DebugLevel), global: zerolog.TraceLevel, enabled: true},
		{name: "secondary respects global threshold", primary: zerolog.InfoLevel, secondary: levelPointer(zerolog.DebugLevel), global: zerolog.ErrorLevel},
	} {
		t.Run(tt.name, func(t *testing.T) {
			zerolog.SetGlobalLevel(tt.global)
			logger := &ZLoggerWrapper{Logger: zerolog.New(io.Discard).Level(tt.primary)}
			if tt.secondary != nil {
				secondary := zerolog.New(io.Discard).Level(*tt.secondary)
				logger.jsonLogger = &secondary
			}
			require.Equal(t, tt.enabled, logger.DebugEnabled())
		})
	}
}

func levelPointer(level zerolog.Level) *zerolog.Level { return &level }

func BenchmarkZeroLoggerDuplicate(b *testing.B) {
	parent := &ZLoggerWrapper{Logger: zerolog.New(io.Discard)}
	for _, tt := range []struct {
		name    string
		options []Option
	}{
		{name: "NoOptions"},
		{name: "SkipFrame", options: []Option{WithSkipFrame(1)}},
		{name: "SkipIncrement", options: []Option{WithSkipFrameIncrement(2)}},
	} {
		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				duplicateBenchmarkSink = parent.Duplicate(tt.options...)
			}
		})
	}
}
