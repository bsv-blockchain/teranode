package tracing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type traceLogRecord struct {
	level   string
	message string
}

type recordingTraceLogger struct {
	ulogger.Logger
	level      int
	duplicates int
	contexts   int
	records    []traceLogRecord
}

func (l *recordingTraceLogger) LogLevel() int { return l.level }

func (l *recordingTraceLogger) Duplicate(...ulogger.Option) ulogger.Logger {
	l.duplicates++
	return l
}

func (l *recordingTraceLogger) WithTraceContext(context.Context) ulogger.Logger {
	l.contexts++
	return l
}

func (l *recordingTraceLogger) record(level int, name, format string, args ...interface{}) {
	if level >= l.level {
		l.records = append(l.records, traceLogRecord{level: name, message: fmt.Sprintf(format, args...)})
	}
}

func (l *recordingTraceLogger) Debugf(format string, args ...interface{}) {
	l.record(ulogger.LogLevelDebug, "DEBUG", format, args...)
}

func (l *recordingTraceLogger) Infof(format string, args ...interface{}) {
	l.record(ulogger.LogLevelInfo, "INFO", format, args...)
}

func (l *recordingTraceLogger) Warnf(format string, args ...interface{}) {
	l.record(ulogger.LogLevelWarning, "WARN", format, args...)
}

func (l *recordingTraceLogger) Errorf(format string, args ...interface{}) {
	l.record(ulogger.LogLevelError, "ERROR", format, args...)
}

func TestStart_SuppressedDebugAvoidsLoggerWork(t *testing.T) {
	original := IsTracingEnabled()
	t.Cleanup(func() { SetTracingEnabled(original) })
	SetTracingEnabled(false)

	logger := &recordingTraceLogger{level: ulogger.LogLevelInfo}
	_, _, end := Tracer("test").Start(context.Background(), "op", WithDebugLogMessage(logger, "debug"))
	end(errors.NewProcessingError("failed"))

	require.Empty(t, logger.records)
	require.Zero(t, logger.duplicates, "suppressed debug logs must not duplicate the logger")
	require.Zero(t, logger.contexts, "suppressed debug logs must not enrich the logger")
}

func TestStart_LogLevelsAndErrorPromotion(t *testing.T) {
	original := IsTracingEnabled()
	t.Cleanup(func() { SetTracingEnabled(original) })
	SetTracingEnabled(false)

	tests := []struct {
		name       string
		level      int
		option     func(ulogger.Logger, string, ...interface{}) Options
		err        error
		wantLevels []string
	}{
		{name: "debug", level: ulogger.LogLevelDebug, option: WithDebugLogMessage, wantLevels: []string{"DEBUG", "DEBUG"}},
		{name: "debug error", level: ulogger.LogLevelDebug, option: WithDebugLogMessage, err: errors.NewProcessingError("failed"), wantLevels: []string{"DEBUG", "ERROR"}},
		{name: "suppressed debug error", level: ulogger.LogLevelInfo, option: WithDebugLogMessage, err: errors.NewProcessingError("failed")},
		{name: "info", level: ulogger.LogLevelInfo, option: WithLogMessage, wantLevels: []string{"INFO", "INFO"}},
		{name: "info error at error level", level: ulogger.LogLevelError, option: WithLogMessage, err: errors.NewProcessingError("failed"), wantLevels: []string{"ERROR"}},
		{name: "warning error", level: ulogger.LogLevelWarning, option: WithWarnLogMessage, err: errors.NewProcessingError("failed"), wantLevels: []string{"WARN", "ERROR"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := &recordingTraceLogger{level: tt.level}
			_, _, end := Tracer("test").Start(context.Background(), "op", tt.option(logger, "work %s", "item"))
			end(tt.err)

			var levels []string
			for _, record := range logger.records {
				levels = append(levels, record.level)
				require.Contains(t, record.message, "work item")
			}
			require.Equal(t, tt.wantLevels, levels)
			if len(logger.records) > 0 {
				require.Contains(t, logger.records[len(logger.records)-1].message, "DONE in")
				if tt.err != nil {
					require.Contains(t, logger.records[len(logger.records)-1].message, tt.err.Error())
				}
			}
		})
	}
}

func TestStart_DebugErrorWithGlobalThreshold(t *testing.T) {
	original := IsTracingEnabled()
	originalLevel := zerolog.GlobalLevel()
	t.Cleanup(func() {
		SetTracingEnabled(original)
		zerolog.SetGlobalLevel(originalLevel)
	})
	SetTracingEnabled(false)
	zerolog.SetGlobalLevel(zerolog.ErrorLevel)

	var output bytes.Buffer
	logger := &ulogger.ZLoggerWrapper{Logger: zerolog.New(&output).Level(zerolog.DebugLevel)}
	_, _, end := Tracer("test").Start(context.Background(), "op", WithDebugLogMessage(logger, "debug"))
	end(errors.NewProcessingError("failed"))

	var record map[string]interface{}
	require.NoError(t, json.Unmarshal(output.Bytes(), &record))
	require.Equal(t, "error", record["level"])
	require.Contains(t, record["message"], "debug DONE in")
}

func TestStart_DebugLoggingKeepsTraceContext(t *testing.T) {
	original := IsTracingEnabled()
	t.Cleanup(func() { SetTracingEnabled(original) })
	SetTracingEnabled(true)

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	tracer := &UTracer{name: "test", tracer: provider.Tracer("test")}
	var output bytes.Buffer
	logger := &ulogger.ZLoggerWrapper{Logger: zerolog.New(&output).Level(zerolog.DebugLevel)}
	_, span, end := tracer.Start(context.Background(), "op", WithDebugLogMessage(logger, "work"))
	end(errors.NewProcessingError("failed"))

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	require.Len(t, lines, 2)
	for _, line := range lines {
		var record map[string]interface{}
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		require.Equal(t, span.SpanContext().TraceID().String(), record["traceId"])
		require.Equal(t, span.SpanContext().SpanID().String(), record["spanId"])
	}
	require.Len(t, recorder.Ended(), 1)
	require.Equal(t, codes.Error, recorder.Ended()[0].Status().Code)
}

func TestStart_SuppressedDebugStillRecordsMetrics(t *testing.T) {
	original := IsTracingEnabled()
	t.Cleanup(func() { SetTracingEnabled(original) })
	SetTracingEnabled(false)

	logger := &recordingTraceLogger{level: ulogger.LogLevelInfo}
	counter := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_suppressed_debug", Help: "test"})
	_, _, end := Tracer("test").Start(context.Background(), "op", WithDebugLogMessage(logger, "debug"), WithCounter(counter))
	end()
	end()

	require.Equal(t, float64(1), counterValue(t, counter))
	require.Zero(t, logger.duplicates)
}

func TestStart_MixedLogMessagesKeepFirstLogger(t *testing.T) {
	original := IsTracingEnabled()
	t.Cleanup(func() { SetTracingEnabled(original) })
	SetTracingEnabled(false)

	first := &recordingTraceLogger{level: ulogger.LogLevelInfo}
	second := &recordingTraceLogger{level: ulogger.LogLevelDebug}
	_, _, end := Tracer("test").Start(context.Background(), "op",
		WithDebugLogMessage(first, "suppressed"),
		WithLogMessage(second, "visible"),
	)
	end()

	require.Len(t, first.records, 2)
	require.Equal(t, "visible", first.records[0].message)
	require.Contains(t, first.records[1].message, "visible DONE in")
	require.Empty(t, second.records)
}

func BenchmarkStart_DebugLogging(b *testing.B) {
	original := IsTracingEnabled()
	b.Cleanup(func() { SetTracingEnabled(original) })
	SetTracingEnabled(false)

	for _, level := range []zerolog.Level{zerolog.InfoLevel, zerolog.DebugLevel} {
		for _, metrics := range []bool{false, true} {
			b.Run(fmt.Sprintf("Level=%s/Metrics=%t", level, metrics), func(b *testing.B) {
				logger := &ulogger.ZLoggerWrapper{Logger: zerolog.New(io.Discard).Level(level)}
				histogram := prometheus.NewHistogram(prometheus.HistogramOpts{Name: "bench_debug_histogram", Help: "bench"})
				tracer := Tracer("bench")
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					var end func(...error)
					if metrics {
						_, _, end = tracer.Start(ctx, "op", WithDebugLogMessage(logger, "[ValidateTransaction] called"), WithHistogram(histogram))
					} else {
						_, _, end = tracer.Start(ctx, "op", WithDebugLogMessage(logger, "[ValidateTransaction] called"))
					}
					end()
				}
			})
		}
	}
}
