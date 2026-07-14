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
	SampleRate       *float64                // per-span sample rate override (nil = use default)
	InjectStartTime  bool                    // inject the span start time into the context under StartTime
}

// traceOptionsPool recycles TraceOptions across spans. On the hot path (millions
// of spans per block during big-block catchup) allocating a fresh TraceOptions
// per Start — together with the end-function closure — was the dominant remaining
// per-span cost after #1099 gated the gocore.Stat/tag/StartTime work when tracing
// is disabled. Objects are taken in Start and returned either immediately (the
// no-op fast path) or when the span's end function runs.
var traceOptionsPool = sync.Pool{
	New: func() any { return &TraceOptions{} },
}

// reset clears a pooled TraceOptions for reuse. Slices that are only ever
// appended to internally (Tags, LogMessages) keep their backing array for reuse;
// SpanStartOptions is set to nil because WithSpanStartOptions replaces the field
// with a caller-owned slice, which must not be aliased across spans.
func (s *TraceOptions) reset() {
	s.SpanStartOptions = nil
	s.ParentStat = nil
	s.Tags = s.Tags[:0]
	s.Histogram = nil
	s.Counter = nil
	s.Logger = nil
	s.LogMessages = s.LogMessages[:0]
	s.Timeout = 0
	s.SampleRate = nil
	s.InjectStartTime = false
}

// noopEndFn is the shared, allocation-free end function returned by Start for
// spans that have no finalisation work (tracing disabled and no metrics, log
// messages, context timeout or gocore.Stat). Returning a single package-level
// function avoids allocating a closure per span on the hot path.
func noopEndFn(...error) {}

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
			sdktrace.WithSampler(newOverrideSampler(
				sdktrace.ParentBased(sdktrace.TraceIDRatioBased(appSettings.TracingSampleRate)),
			)),
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

// WithStartTime injects the span's start time into the returned context under the
// StartTime key, so callers can read it back via ctx.Value(tracing.StartTime).
//
// This is opt-in because injecting it costs a context.WithValue (and boxing the
// time.Time) on every span, and only a few call sites read it. The hot validation
// path opens many spans per transaction and does not need it, so it must not pay
// the cost by default.
func WithStartTime() Options {
	return func(s *TraceOptions) {
		s.InjectStartTime = true
	}
}

// WithTag adds a key-value tag to the trace
func WithTag(key, value string) Options {
	return func(s *TraceOptions) {
		// Tags are only consumed when building OpenTelemetry span attributes, which
		// is skipped entirely when tracing is disabled. Avoid the slice allocation
		// (and append growth) on the disabled hot path.
		if !tracingEnabled.Load() {
			return
		}

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

// WithSampleRate overrides the global sample rate for this specific span.
// rate is clamped to [0.0, 1.0]. When rate >= 1.0, the span is always sampled
// regardless of the global rate or parent sampling decision.
func WithSampleRate(rate float64) Options {
	return func(s *TraceOptions) {
		if rate < 0 {
			rate = 0
		}
		if rate > 1 {
			rate = 1
		}
		s.SampleRate = &rate
	}
}

// WithAlwaysSample forces this span to always be sampled, regardless of
// the global sample rate or parent sampling decision.
func WithAlwaysSample() Options {
	return func(s *TraceOptions) {
		rate := 1.0
		s.SampleRate = &rate
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

// OTelTracer returns the underlying OpenTelemetry tracer. Useful when
// integrating with libraries that accept a trace.Tracer directly (e.g.
// go-batcher v2's WithTracer option).
func (u *UTracer) OTelTracer() trace.Tracer {
	return u.tracer
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

	// Process options into a pooled TraceOptions to avoid a per-span heap
	// allocation; recycled in the no-op fast path below or when the span ends.
	// If an option closure or the OTel SDK panics before either Put, the object is
	// simply dropped (GC reclaims it) — sync.Pool does not require balanced
	// Get/Put, so this is a benign reuse miss, not a leak. We deliberately avoid a
	// defer-Put here to keep the hot path allocation- and defer-free.
	options := traceOptionsPool.Get().(*TraceOptions)
	options.reset()

	for _, opt := range opts {
		opt(options)
	}

	// check whether the context has a timeout set
	var cancelFunc context.CancelFunc

	if options.Timeout > 0 {
		ctx, cancelFunc = context.WithTimeout(ctx, options.Timeout)
	}

	// Create gocore.Stat (only when tracing is enabled).
	//
	// The gocore.Stat tree is consumed by the /stats perf endpoint via
	// WithParentStat. Creating it allocates a Stat (each with its own RWMutex and
	// child sync.Map) and takes the parent's child-map lock on every span. On the
	// hot sync path the validator opens several spans per transaction across
	// millions of transactions per block, so this allocation + lock is a dominant,
	// contended cost. When tracing is disabled (the documented "zero overhead"
	// state) skip it; start is still computed for metrics/log durations and for the
	// StartTime context value when a caller opts in via WithStartTime.
	var (
		start time.Time
		stat  *gocore.Stat
	)

	if tracingEnabled {
		if options.ParentStat != nil {
			start, stat, ctx = NewStatFromContext(ctx, spanName, options.ParentStat)
		} else {
			start, stat, ctx = NewStatFromContext(ctx, spanName, defaultStat)
		}
	} else {
		start = gocore.CurrentTime()
	}

	// add the start time to the context only when a caller opts in (read downstream,
	// e.g. blockvalidation catchup). Skipped by default to avoid the context.WithValue
	// boxing on the hot path, where many spans are opened per transaction.
	if options.InjectStartTime {
		ctx = context.WithValue(ctx, StartTime, start)
	}

	// inject sample rate override into context for the custom sampler
	if options.SampleRate != nil {
		ctx = context.WithValue(ctx, sampleRateOverrideKey{}, options.SampleRate)
	}

	var (
		span           trace.Span
		shortCircuited bool
	)

	if tracingEnabled {
		// Fast path: skip the entire OTel SDK when the parent span is unsampled
		// and there's no per-span override that could force sampling.
		// This avoids context traversal, sampler evaluation, and span allocation
		// for the 99%+ of child spans whose parent was already dropped.
		// At ~2M tx/s with multiple spans per tx, this eliminates millions of
		// unnecessary sampler evaluations, context lookups, and span allocations
		// per second on the unsampled path.
		if options.SampleRate == nil && canShortCircuit(options.SpanStartOptions) {
			parentSpan := trace.SpanFromContext(ctx)
			if parentSpan.SpanContext().IsValid() && !parentSpan.SpanContext().IsSampled() {
				span = parentSpan
				shortCircuited = true
			}
		}

		if !shortCircuited {
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
		}
	} else {
		span = trace.SpanFromContext(ctx)
	}

	// Log start messages (only if logging is enabled)
	// This is done AFTER starting the span so that WithTraceContext can extract
	// traceId/spanId from the context for log-trace correlation.
	if options.Logger != nil && len(options.LogMessages) > 0 {
		ctxLogger := options.Logger.WithTraceContext(ctx)
		for _, l := range options.LogMessages {
			switch l.level {
			case "WARN":
				ctxLogger.Warnf(l.message, l.args...)
			case "DEBUG":
				ctxLogger.Debugf(l.message, l.args...)
			default:
				ctxLogger.Infof(l.message, l.args...)
			}
		}
	}

	// Fast path: when the end function would do nothing — tracing disabled and no
	// gocore.Stat, metrics, log messages or context timeout to finalise — return a
	// shared no-op and recycle the options immediately. This eliminates both the
	// per-span end-closure allocation and (via the pool) the TraceOptions
	// allocation for the common zero-finalisation disabled span (e.g.
	// aerospike:Create, which passes no options at all).
	if !tracingEnabled && cancelFunc == nil && stat == nil &&
		options.Histogram == nil && options.Counter == nil &&
		!(options.Logger != nil && len(options.LogMessages) > 0) {
		traceOptionsPool.Put(options)

		return ctx, span, noopEndFn
	}

	// ended is a per-span guard captured by the end closure — deliberately NOT a
	// field on the pooled TraceOptions, which reset() clears on reissue and would
	// thus re-open the guard for a stale end call landing after the object had been
	// recycled into another span (corrupting that span's metrics/logs and
	// double-Putting). Declared after the fast-path return above, so the common
	// allocation-free path is untouched; on the slow path it rides in the end
	// closure. Atomic so a repeated or concurrent call is a safe no-op.
	var ended atomic.Bool

	endFn := func(optionalError ...error) {
		// Finalise exactly once: only the call that wins the compare-and-swap runs
		// finalisation and returns options to the pool. A later or concurrent call
		// returns before touching options, which by then may have been recycled into
		// an unrelated span. Winning the CAS is what licenses every read below.
		if !ended.CompareAndSwap(false, true) {
			return
		}

		var err error
		if len(optionalError) > 0 {
			err = optionalError[0]
		}

		// Only interact with the OTel span if we own it (not short-circuited).
		// When short-circuited, span points to the parent — we must not End() it.
		if tracingEnabled && !shortCircuited {
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}

			span.End()
		}

		if stat != nil {
			stat.AddTime(start)
		}

		u.recordMetrics(options, start)
		u.logEndMessage(ctx, options, start, err)

		// Ensure the cancelCtx function is called when the span ends
		if cancelFunc != nil {
			cancelFunc()
		}

		traceOptionsPool.Put(options)
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
		return ctx, noopSpan, func(...error) {
			// no-op cleanup: tracing is disabled
		}
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

// logEndMessage logs the completion message for a span with trace context correlation
func (u *UTracer) logEndMessage(ctx context.Context, options *TraceOptions, start time.Time, err error) {
	if options.Logger == nil || len(options.LogMessages) == 0 {
		return
	}

	// Duplicate the logger to ensure the skip frame is correct, since we are calling this from
	// a closure and we want to skip the frame of this function.
	// Then enrich with trace context for log-trace correlation.
	logger := options.Logger.Duplicate(ulogger.WithSkipFrameIncrement(2)).WithTraceContext(ctx)

	var done string
	if err != nil {
		done = fmt.Sprintf(" DONE in %s with error: %v", time.Since(start), err)
	} else {
		done = fmt.Sprintf(" DONE in %s", time.Since(start))
	}

	for _, l := range options.LogMessages {
		logTraceMessage(logger, l, done, err)
	}
}

// logTraceMessage logs a single trace message at the appropriate level.
func logTraceMessage(logger ulogger.Logger, l logMessage, done string, err error) {
	msg := l.message + done
	switch l.level {
	case "WARN":
		if err != nil && logger.LogLevel() == ulogger.LogLevelWarning {
			logger.Errorf(msg, l.args...)
		} else {
			logger.Warnf(msg, l.args...)
		}
	case "DEBUG":
		if err != nil && logger.LogLevel() == ulogger.LogLevelDebug {
			logger.Errorf(msg, l.args...)
		} else {
			logger.Debugf(msg, l.args...)
		}
	default:
		if err != nil {
			logger.Errorf(msg, l.args...)
		} else {
			logger.Infof(msg, l.args...)
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

// canShortCircuit reports whether the span creation can skip the OTel SDK
// based on the parent's sampling decision. It returns false when any
// SpanStartOption is present that could change the effective parent
// (e.g. WithNewRoot), because in that case we cannot infer the sampling
// outcome from the current context's parent span.
func canShortCircuit(spanOpts []trace.SpanStartOption) bool {
	// If there are span start options, one of them might be WithNewRoot or
	// WithLinks that could alter the sampling decision. To keep this check
	// allocation-free and simple, we conservatively fall through to the
	// OTel SDK whenever any SpanStartOption is provided.
	return len(spanOpts) == 0
}

// SetupMockTracer sets up a mock tracer for testing
func SetupMockTracer() {
	// OpenTelemetry doesn't have a direct equivalent to OpenTracing's mocktracer
	// For testing, you would typically use the SDK's trace.NewTracerProvider with
	// an in-memory exporter or a testing exporter
	// This is a placeholder - in a real implementation you'd set up a test provider
}
