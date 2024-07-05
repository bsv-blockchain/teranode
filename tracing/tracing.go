package tracing

import (
	"context"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"time"

	"github.com/bitcoin-sv/ubsv/util"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
)

type Options func(s *TraceOptions)

type TraceOptions struct {
	ParentStat *gocore.Stat
	Histogram  prometheus.Histogram
	Logger     ulogger.Logger
	LogMessage string
	LogArgs    []interface{}
}

func WithParentStat(stat *gocore.Stat) Options {
	return func(s *TraceOptions) {
		s.ParentStat = stat
	}
}

func WithHistogram(histogram prometheus.Histogram) Options {
	return func(s *TraceOptions) {
		s.Histogram = histogram
	}
}

func WithLogMessage(logger ulogger.Logger, format string, args ...interface{}) Options {
	return func(s *TraceOptions) {
		s.Logger = logger
		s.LogMessage = format
		s.LogArgs = args
	}
}

// StartTracing starts a new span with the given name and returns a context with the span and a function to finish the span.
func StartTracing(ctx context.Context, name string, setOptions ...Options) (context.Context, func()) {
	span, spanCtx := opentracing.StartSpanFromContext(ctx, name)

	// process the options
	options := &TraceOptions{}
	for _, opt := range setOptions {
		opt(options)
	}

	var start time.Time
	var stat *gocore.Stat
	if options.ParentStat != nil {
		start, stat, ctx = util.NewStatFromContext(spanCtx, name, options.ParentStat)
	} else {
		start, stat, ctx = util.StartStatFromContext(spanCtx, name)
	}

	if options.Logger != nil && options.LogMessage != "" {
		options.Logger.Infof(options.LogMessage, options.LogArgs)
	}

	return ctx, func() {
		span.Finish()
		stat.AddTime(start)

		if options.Histogram != nil {
			options.Histogram.Observe(float64(time.Since(start).Microseconds()) / 1_000_000)
		}

		if options.Logger != nil && options.LogMessage != "" {
			options.Logger.Infof(options.LogMessage+" DONE", options.LogArgs)
		}
	}
}
