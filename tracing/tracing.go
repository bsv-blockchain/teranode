package tracing

import (
	"context"
	"fmt"
	"time"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
)

type Options func(s *TraceOptions)

type logMessage struct {
	message string
	args    []interface{}
	level   string
}

type TraceOptions struct {
	ParentStat  *gocore.Stat
	Histogram   prometheus.Histogram
	Counter     prometheus.Counter
	Logger      ulogger.Logger
	LogMessages []logMessage
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
func WithLogMessage(logger ulogger.Logger, message string, args ...interface{}) Options {
	return func(s *TraceOptions) {
		s.addLogMessage(logger, message, "INFO", args)
	}
}

func WithWarnLogMessage(logger ulogger.Logger, message string, args ...interface{}) Options {
	return func(s *TraceOptions) {
		s.addLogMessage(logger, message, "WARN", args)
	}
}

func WithDebugLogMessage(logger ulogger.Logger, message string, args ...interface{}) Options {
	return func(s *TraceOptions) {
		s.addLogMessage(logger, message, "DEBUG", args)
	}
}

func (s *TraceOptions) addLogMessage(logger ulogger.Logger, message, level string, args []interface{}) {
	if s.Logger == nil {
		s.Logger = logger
	}
	if s.LogMessages == nil {
		s.LogMessages = []logMessage{{message: message, args: args, level: level}}
	} else {
		s.LogMessages = append(s.LogMessages, logMessage{message: message, args: args, level: level})
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
		start, stat, ctx = NewStatFromContext(spanCtx, name, defaultStat)
	}

	if options.Logger != nil && len(options.LogMessages) > 0 {
		for _, l := range options.LogMessages {
			switch {
			case l.level == "WARN":
				options.Logger.Warnf(l.message, l.args...)
			case l.level == "DEBUG":
				options.Logger.Debugf(l.message, l.args...)
			default:
				options.Logger.Infof(l.message, l.args...)
			}
		}
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

		if options.Logger != nil && len(options.LogMessages) > 0 {
			done := fmt.Sprintf(" DONE in %s", time.Since(start))
			for _, l := range options.LogMessages {
				switch {
				case l.level == "WARN":
					options.Logger.Warnf(l.message+done, l.args...)
				case l.level == "DEBUG":
					options.Logger.Debugf(l.message+done, l.args...)
				default:
					options.Logger.Infof(l.message+done, l.args...)
				}
			}
		}
	}
}

func DecoupleTracingSpan(ctx context.Context, name string) Span {
	callerSpan := opentracing.SpanFromContext(ctx)
	setCtx := opentracing.ContextWithSpan(context.Background(), callerSpan)
	setCtx = CopyStatFromContext(ctx, setCtx)
	setSpan := Start(setCtx, name)

	return setSpan
}

func SetGlobalMockTracer() {
	opentracing.SetGlobalTracer(mocktracer.New())
}

func RegisterGRPCClientTracer(unaryClientInterceptors []grpc.UnaryClientInterceptor, streamClientInterceptors []grpc.StreamClientInterceptor) ([]grpc.UnaryClientInterceptor, []grpc.StreamClientInterceptor) {
	tracer := opentracing.GlobalTracer()

	unaryClientInterceptors = append(unaryClientInterceptors, otgrpc.OpenTracingClientInterceptor(tracer))
	streamClientInterceptors = append(streamClientInterceptors, otgrpc.OpenTracingStreamClientInterceptor(tracer))

	return unaryClientInterceptors, streamClientInterceptors
}

func RegisterGRPCServerTracer(unaryInterceptors []grpc.UnaryServerInterceptor, streamInterceptors []grpc.StreamServerInterceptor) ([]grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor) {
	tracer := opentracing.GlobalTracer()

	unaryInterceptors = append(unaryInterceptors, otgrpc.OpenTracingServerInterceptor(tracer))
	streamInterceptors = append(streamInterceptors, otgrpc.OpenTracingStreamServerInterceptor(tracer))

	return unaryInterceptors, streamInterceptors
}

func GetGRPCClientTracerOptions(opts []grpc.DialOption, unaryClientInterceptors []grpc.UnaryClientInterceptor, streamClientInterceptors []grpc.StreamClientInterceptor) ([]grpc.DialOption, []grpc.UnaryClientInterceptor, []grpc.StreamClientInterceptor) {
	//if connectionOptions.OpenTelemetry {
	//	opts = append(opts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	//}

	if opentracing.IsGlobalTracerRegistered() {
		unaryClientInterceptors, streamClientInterceptors = RegisterGRPCClientTracer(unaryClientInterceptors, streamClientInterceptors)
	}

	return opts, unaryClientInterceptors, streamClientInterceptors
}

func GetGRPCServerTracerOptions(opts []grpc.ServerOption, unaryInterceptors []grpc.UnaryServerInterceptor, streamInterceptors []grpc.StreamServerInterceptor) ([]grpc.ServerOption, []grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor) {
	//if connectionOptions.OpenTelemetry {
	//	opts = append(opts, grpc.StatsHandler(otelgrpc.NewServerHandler()))
	//}

	if opentracing.IsGlobalTracerRegistered() {
		unaryInterceptors, streamInterceptors = RegisterGRPCServerTracer(unaryInterceptors, streamInterceptors)
	}

	return opts, unaryInterceptors, streamInterceptors
}
