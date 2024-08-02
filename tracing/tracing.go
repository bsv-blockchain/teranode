package tracing

import (
	"context"
	"fmt"
	"time"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
)

type Options func(s *TraceOptions)

type TraceOptions struct {
	ParentStat *gocore.Stat
	Histogram  prometheus.Histogram
	Counter    prometheus.Counter
	Logger     ulogger.Logger
	LogMessage string
	LogArgs    []interface{}
}

func WithParentStat(stat *gocore.Stat) Options {
	return func(s *TraceOptions) {
		s.ParentStat = stat
	}
}

// WithHistogram sets the prometheus histogram to be observed when the span is finished.
func WithHistogram(histogram prometheus.Histogram) Options {
	return func(s *TraceOptions) {
		s.Histogram = histogram
	}
}

// WithCounter sets the prometheus counter to be incremented when the span is finished.
func WithCounter(counter prometheus.Counter) Options {
	return func(s *TraceOptions) {
		s.Counter = counter
	}
}

// WithLogMessage sets the logger and log message to be used when starting the span and when the span is finished.
// The log message is formatted with fmt.Sprintf and all arguments are passed to the logger.
// The log message is logged at the INFO level. This should only be used in grpc / http calls and not internal functions.
func WithLogMessage(logger ulogger.Logger, format string, args ...interface{}) Options {
	return func(s *TraceOptions) {
		s.Logger = logger
		s.LogMessage = format
		s.LogArgs = args
	}
}

// StartTracing starts a new span with the given name and returns a context with the span and a function to finish the span.
func StartTracing(ctx context.Context, name string, setOptions ...Options) (context.Context, *gocore.Stat, func()) {
	span, spanCtx := opentracing.StartSpanFromContext(ctx, name)

	// process the options
	options := &TraceOptions{}
	for _, opt := range setOptions {
		opt(options)
	}

	var start time.Time
	var stat *gocore.Stat
	if options.ParentStat != nil {
		start, stat, ctx = NewStatFromContext(spanCtx, name, options.ParentStat)
	} else {
		start, stat, ctx = StartStatFromContext(spanCtx, name)
	}

	if options.Logger != nil && options.LogMessage != "" {
		options.Logger.Infof(options.LogMessage, options.LogArgs...)
	}

	return ctx, stat, func() {
		span.Finish()
		stat.AddTime(start)

		if options.Histogram != nil {
			options.Histogram.Observe(float64(time.Since(start).Microseconds()) / 1_000_000)
		}

		if options.Counter != nil {
			options.Counter.Inc()
		}

		if options.Logger != nil && options.LogMessage != "" {
			done := fmt.Sprintf(" DONE in %s", time.Since(start))
			options.Logger.Infof(options.LogMessage+done, options.LogArgs...)
		}
	}
}
