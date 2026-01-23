package tracing

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// contextKey is a custom type for context keys to avoid collisions
type contextKey string

const (
	StartTime contextKey = "startTime"
)

var (
	once           sync.Once
	initErr        error
	tp             *sdktrace.TracerProvider
	mu             sync.Mutex
	tracingEnabled atomic.Bool // Global flag to completely disable tracing overhead
)

// Options func represents a functional option for configuring tracing
type Options func(s *TraceOptions)

// logMessage represents a log message with its level and arguments
type logMessage struct {
	message string
	args    []interface{}
	level   string
}

// tracingTag represents a key-value tag for tracing
type tracingTag struct {
	key   string
	value string
}

// TraceOptions contains all options for configuring a trace span
type TraceOptions struct {
	SpanStartOptions []trace.SpanStartOption // options passed to the OpenTelemetry span
	ParentStat       *gocore.Stat            // parent gocore.Stat
	Tags             []tracingTag            // tags to be added to the span
	Histogram        prometheus.Histogram    // histogram to be observed when the span is finished
	Counter          prometheus.Counter      // counter to be incremented when the span is finished
	Logger           ulogger.Logger          // logger to be used when starting the span and when the span is finished
	LogMessages      []logMessage            // log messages to be added to the span
	Timeout          time.Duration           // timeout for the span, if set
}

// addLogMessage adds a log message to the trace options
func (s *TraceOptions) addLogMessage(logger ulogger.Logger, message, level string, args []interface{}) {
	if s.Logger == nil && logger != nil {
		// duplicate the logger so that the skip frame is correct
		s.Logger = logger.Duplicate(ulogger.WithSkipFrame(1))
	}

	if s.LogMessages == nil {
		s.LogMessages = []logMessage{{message: message, args: args, level: level}}
	} else {
		s.LogMessages = append(s.LogMessages, logMessage{message: message, args: args, level: level})
	}
}

// IsTracingEnabled returns whether tracing is currently enabled.
func IsTracingEnabled() bool {
	return tracingEnabled.Load()
}

// SetTracingEnabled sets the global tracing enabled flag.
// This should be called during initialization based on settings.TracingEnabled.
// When false, all tracing operations become no-ops with minimal overhead.
func SetTracingEnabled(enabled bool) {
	tracingEnabled.Store(enabled)
}

// InitTracer initializes the global tracer. Safe to call multiple times.
// Only the first call will actually initialize the tracer.
// Returns an error if initialization fails.
func InitTracer(appSettings *settings.Settings) error {
	once.Do(func() {
		// Create OTLP exporter
		var (
			exporter *otlptrace.Exporter

			opts []otlptracehttp.Option
		)

		opts = append(opts, otlptracehttp.WithEndpoint(appSettings.TracingCollectorURL.Host))
		if appSettings.TracingCollectorURL.Scheme == "http" {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		exporter, initErr = otlptracehttp.New(
			context.Background(),
			opts...,
		)
		if initErr != nil {
			initErr = errors.NewProcessingError("failed to create OTLP exporter", initErr)
			return
		}

		// Create resource with service information
		var res *resource.Resource

		res, initErr = resource.New(
			context.Background(),
			resource.WithAttributes(
				semconv.ServiceNameKey.String(appSettings.ServiceName),
				semconv.ServiceVersionKey.String(appSettings.Version),
				attribute.String("commit", appSettings.Commit),
			),
		)
		if initErr != nil {
			initErr = errors.NewProcessingError("failed to create resource", initErr)
			return
		}

		mu.Lock()
		defer mu.Unlock()

		// Create trace provider with the exporter
		tp = sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(time.Second)), // Send batches every second
			sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(appSettings.TracingSampleRate))),
			sdktrace.WithResource(res),
		)

		// Set the global trace provider only after validation succeeds
		otel.SetTracerProvider(tp)

		// Set up propagation (for distributed tracing)
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))

		// Enable tracing globally now that initialization succeeded
		SetTracingEnabled(true)
	})

	return initErr
}

// ShutdownTracer shuts down the global tracer provider.
// Safe to call multiple times - subsequent calls are no-ops.
func ShutdownTracer(ctx context.Context) error {
	mu.Lock()
	defer mu.Unlock()

	if tp != nil {
		// Force flush to ensure spans are sent to Jaeger BEFORE stopping daemon
		if err := tp.ForceFlush(ctx); err != nil {
			if strings.Contains(err.Error(), "connection refused") {
				log.Error().Err(err).Msg("failed to flush spans")
				return nil
			}

			return errors.NewProcessingError("failed to flush spans", err)
		}

		if err := tp.Shutdown(ctx); err != nil {
			return errors.NewProcessingError("failed to shutdown tracer", err)
		}

		tp = nil
	}

	return nil
}

func WithSpanStartOptions(options ...trace.SpanStartOption) Options {
	return func(s *TraceOptions) {
		s.SpanStartOptions = options
	}
}

// WithContextTimeout sets the parent context timeout for the trace.
func WithContextTimeout(timeout time.Duration) Options {
	return func(s *TraceOptions) {
		s.Timeout = timeout
	}
}

// WithParentStat sets the parent gocore.Stat for the trace
func WithParentStat(stat *gocore.Stat) Options {
	return func(s *TraceOptions) {
		s.ParentStat = stat
	}
}

// WithTag adds a key-value tag to the trace
func WithTag(key, value string) Options {
	return func(s *TraceOptions) {
		if s.Tags == nil {
			s.Tags = make([]tracingTag, 0)
		}

		s.Tags = append(s.Tags, tracingTag{key: key, value: value})
	}
}

// WithHistogram sets the prometheus histogram to be observed when the span is finished
func WithHistogram(histogram prometheus.Histogram) Options {
	return func(s *TraceOptions) {
		s.Histogram = histogram
	}
}

// WithCounter sets the prometheus counter to be incremented when the span is finished
func WithCounter(counter prometheus.Counter) Options {
	return func(s *TraceOptions) {
		s.Counter = counter
	}
}

// WithLogMessage sets the logger and log message to be used when starting the span and when the span is finished
func WithLogMessage(logger ulogger.Logger, message string, args ...interface{}) Options {
	return func(s *TraceOptions) {
		s.addLogMessage(logger, message, "INFO", args)
	}
}

// WithWarnLogMessage sets a warning log message
func WithWarnLogMessage(logger ulogger.Logger, message string, args ...interface{}) Options {
	return func(s *TraceOptions) {
		s.addLogMessage(logger, message, "WARN", args)
	}
}

// WithDebugLogMessage sets a debug log message
func WithDebugLogMessage(logger ulogger.Logger, message string, args ...interface{}) Options {
	return func(s *TraceOptions) {
		s.addLogMessage(logger, message, "DEBUG", args)
	}
}

// WithNewRoot creates a new root span for the trace.
func WithNewRoot() Options {
	return func(s *TraceOptions) {
		s.SpanStartOptions = append(s.SpanStartOptions, trace.WithNewRoot())
	}
}

// UTracer provides a unified tracing interface that combines OpenTelemetry spans
// with gocore.Stat for consistent tracing and performance monitoring.
type UTracer struct {
	name   string
	tracer trace.Tracer
}

// USpan represents an active tracing span with associated statistics
type USpan struct {
	stat *gocore.Stat
	ctx  context.Context
}

var (
	// noopTracerProvider is a singleton no-op tracer provider used when tracing is disabled
	noopTracerProvider = noop.NewTracerProvider()

	// noopTracer is a singleton no-op tracer returned when tracing is disabled
	// This eliminates allocation overhead from creating new UTracer instances
	noopTracer = &UTracer{
		name:   "noop",
		tracer: noopTracerProvider.Tracer("noop"),
	}
)

// Tracer creates a new unified tracer with the given name.
// The name typically represents the service or component being traced.
//
// Parameters:
//   - name: The name of the service or component
//   - otelOpts: OpenTelemetry tracer options passed directly to otel.Tracer
func Tracer(name string, otelOpts ...trace.TracerOption) *UTracer {
	// Fast path: return singleton no-op tracer when tracing is disabled
	// This eliminates the overhead of:
	// - Global otel.Tracer lookup (~expensive)
	// - UTracer allocation (~700ms/3.5% CPU in profiles)
	// - Option processing
	if !IsTracingEnabled() {
		return noopTracer
	}

	// Filter out nil options to prevent panic in OpenTelemetry
	var validOpts []trace.TracerOption

	for _, opt := range otelOpts {
		if opt != nil {
			validOpts = append(validOpts, opt)
		}
	}

	// Create OpenTelemetry tracer with valid options
	tracer := otel.Tracer(name, validOpts...)

	return &UTracer{
		name:   name,
		tracer: tracer,
	}
}

// Start begins a new trace span with the given operation name and options.
// It returns:
//   - context.Context: Updated context containing the span
//   - *USpan: The unified span object that must be ended with End()
//
// Example usage:
//
//	ctx, span := tracer.Start(ctx, "ProcessTransaction",
//	    WithParentStat(parentStat),
//	    WithTag("txid", txID),
//	    WithLogMessage(logger, "Processing transaction %s", txID),
//	)
//	defer span.End()
func (u *UTracer) Start(ctx context.Context, spanName string, opts ...Options) (context.Context, trace.Span, func(...error)) {
	tracingEnabled := IsTracingEnabled()

	// Process options
	options := &TraceOptions{}

	for _, opt := range opts {
		opt(options)
	}

	// check whether the context has a timeout set
	var cancelFunc context.CancelFunc

	if options.Timeout > 0 {
		ctx, cancelFunc = context.WithTimeout(ctx, options.Timeout)
	}

	// Create gocore.Stat (only if enabled)
	var (
		start time.Time
		stat  *gocore.Stat
	)

	if options.ParentStat != nil {
		start, stat, ctx = NewStatFromContext(ctx, spanName, options.ParentStat)
	} else {
		start, stat, ctx = NewStatFromContext(ctx, spanName, defaultStat)
	}

	// add the start time to the context
	ctx = context.WithValue(ctx, StartTime, start)

	// Log start messages (only if logging is enabled)
	if options.Logger != nil && len(options.LogMessages) > 0 {
		for _, l := range options.LogMessages {
			switch l.level {
			case "WARN":
				options.Logger.Warnf(l.message, l.args...)
			case "DEBUG":
				options.Logger.Debugf(l.message, l.args...)
			default:
				options.Logger.Infof(l.message, l.args...)
			}
		}
	}

	var span trace.Span

	if tracingEnabled {
		// Add any options.Tags to the span options...
		for _, tag := range options.Tags {
			options.SpanStartOptions = append(options.SpanStartOptions, trace.WithAttributes(attribute.String(tag.key, tag.value)))
		}

		// Start OpenTelemetry span
		ctx, span = u.tracer.Start(ctx, spanName, options.SpanStartOptions...)

		// Set span attributes from tags
		if len(options.Tags) > 0 {
			attrs := make([]attribute.KeyValue, 0, len(options.Tags))
			for _, tag := range options.Tags {
				attrs = append(attrs, attribute.String(tag.key, tag.value))
			}

			span.SetAttributes(attrs...)
		}
	} else {
		span = trace.SpanFromContext(ctx)
	}

	endFn := func(optionalError ...error) {
		var err error

		if tracingEnabled {
			if len(optionalError) > 0 && optionalError[0] != nil {
				err = optionalError[0]
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}

			span.End()
		}

		if stat != nil {
			stat.AddTime(start)
		}

		u.recordMetrics(options, start)
		u.logEndMessage(options, start, err)

		// Ensure the cancelCtx function is called when the span ends
		if cancelFunc != nil {
			cancelFunc()
		}
	}

	return ctx, span, endFn
}

// Context returns the context associated with this span.
// This context should be passed to child operations to maintain the trace.
func (span *USpan) Context() context.Context {
	if span == nil {
		return context.Background()
	}

	return span.ctx
}

// Stat returns the gocore.Stat associated with this span.
// This can be used as a parent stat for child operations.
func (span *USpan) Stat() *gocore.Stat {
	if span == nil {
		return nil
	}

	return span.stat
}

// DecoupleTracingSpan creates a new context with the current span for decoupled tracing
func DecoupleTracingSpan(ctx context.Context, name string, spanName string) (context.Context, trace.Span, func(...error)) {
	// Fast path: if tracing is disabled, return immediately
	if !IsTracingEnabled() {
		noopSpan := trace.SpanFromContext(ctx)
		return ctx, noopSpan, func(...error) {}
	}

	// Extract the current span from context
	currentSpan := trace.SpanFromContext(ctx)

	// Create a new context with the current span
	newCtx := trace.ContextWithSpan(context.Background(), currentSpan)

	// Copy stats from the original context
	newCtx = CopyStatFromContext(ctx, newCtx)

	// Start a new span
	return Tracer(name).Start(newCtx, spanName)
}

// logEndMessage logs the completion message for a span
func (u *UTracer) logEndMessage(options *TraceOptions, start time.Time, err error) {
	if options.Logger == nil || len(options.LogMessages) == 0 {
		return
	}

	// Duplicate the logger to ensure the skip frame is correct, since we are calling this from
	// a closure and we want to skip the frame of this function
	logger := options.Logger.Duplicate(ulogger.WithSkipFrameIncrement(1))

	var done string
	if err != nil {
		done = fmt.Sprintf(" DONE in %s with error: %v", time.Since(start), err)
	} else {
		done = fmt.Sprintf(" DONE in %s", time.Since(start))
	}

	for _, l := range options.LogMessages {
		switch l.level {
		case "WARN":
			if err != nil && logger.LogLevel() == ulogger.LogLevelWarning {
				logger.Errorf(l.message+done, l.args...)
			} else {
				logger.Warnf(l.message+done, l.args...)
			}
		case "DEBUG":
			if err != nil && logger.LogLevel() == ulogger.LogLevelDebug {
				logger.Errorf(l.message+done, l.args...)
			} else {
				logger.Debugf(l.message+done, l.args...)
			}
		default:
			if err != nil {
				logger.Errorf(l.message+done, l.args...)
			} else {
				logger.Infof(l.message+done, l.args...)
			}
		}
	}
}

// recordMetrics records histogram and counter metrics
func (u *UTracer) recordMetrics(options *TraceOptions, start time.Time) {
	if options.Histogram != nil {
		duration := time.Since(start)
		options.Histogram.Observe(duration.Seconds())
	}

	if options.Counter != nil {
		options.Counter.Inc()
	}
}

// SetupMockTracer sets up a mock tracer for testing
func SetupMockTracer() {
	// OpenTelemetry doesn't have a direct equivalent to OpenTracing's mocktracer
	// For testing, you would typically use the SDK's trace.NewTracerProvider with
	// an in-memory exporter or a testing exporter
	// This is a placeholder - in a real implementation you'd set up a test provider
}
