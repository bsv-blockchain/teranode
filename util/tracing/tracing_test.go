package tracing

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/test/mocklogger"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
)

// initTestTracer initializes a test tracer that doesn't require external connections
func initTestTracer() error {
	// Enable tracing for tests
	SetTracingEnabled(true)

	// Create a no-op exporter for tests
	exporter := tracetest.NewNoopExporter()

	// Create resource with service information
	res, err := resource.New(
		context.Background(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String("test-service"),
			semconv.ServiceVersionKey.String("test"),
		),
	)
	if err != nil {
		return err
	}

	// Create trace provider with the no-op exporter
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(10*time.Millisecond)), // Very short timeout for tests
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
	)

	// Set the global trace provider
	otel.SetTracerProvider(tp)

	// Store the provider in the package global variable (accessing from test)
	// This is a bit hacky but allows us to use the existing ShutdownTracer
	setTestTracerProvider(tp)

	return nil
}

// initTestTracerWithSampler initializes a test tracer with the override sampler wrapping a custom base
func initTestTracerWithSampler(baseSampler sdktrace.Sampler) error {
	SetTracingEnabled(true)

	exporter := tracetest.NewNoopExporter()

	res, err := resource.New(
		context.Background(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String("test-service"),
			semconv.ServiceVersionKey.String("test"),
		),
	)
	if err != nil {
		return err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(10*time.Millisecond)),
		sdktrace.WithSampler(newOverrideSampler(baseSampler)),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	setTestTracerProvider(tp)

	return nil
}

func TestUTracer_WithError(t *testing.T) {
	gocore.SetInfo("name", "v0.1.2b", "76b9cdd7e5ff85b62f6fec6cc20cfe02b4a12c17")

	// Use a no-op tracer for tests to avoid connection attempts
	err := initTestTracer()
	require.NoError(t, err)

	defer func() {
		_ = ShutdownTracer(context.Background())
	}()

	logger := newLineLogger()

	tracer := Tracer("test-service")

	// Start span
	_, _, endFn := tracer.Start(context.Background(), "TestOperationWithError",
		WithLogMessage(logger, "Processing operation"),
	)

	// Simulate error
	testErr := errors.NewProcessingError("test error occurred")

	// End with error
	endFn(testErr)

	// Verify error logging
	assert.Contains(t, logger.lastLog, "Processing operation DONE in")
	assert.Contains(t, logger.lastLog, "with error: PROCESSING (4): test error occurred")
}

func TestUTracer_ChildSpans(t *testing.T) {
	gocore.SetInfo("name", "v0.1.2b", "76b9cdd7e5ff85b62f6fec6cc20cfe02b4a12c17")

	// Use a no-op tracer for tests to avoid connection attempts
	err := initTestTracer()
	require.NoError(t, err)

	defer func() {
		_ = ShutdownTracer(context.Background())
	}()

	tracer := Tracer("test-service", nil)

	// Start parent span
	ctx, parentSpan, endParent := tracer.Start(
		context.Background(),
		"ParentOperation",
		WithTag("TXID", "d286fcdf58754b59691528cf857850d47ed529608b0a6fd8da5317303beffe8b"),
	)

	// Start child span
	_, childSpan, endChild1 := tracer.Start(ctx, "ChildOperation",
		WithTag("child.id", "child-1"),
	)

	// Verify child has parent's stat as parent
	assert.NotNil(t, childSpan)
	assert.NotNil(t, parentSpan)

	// End child
	endChild1()

	// Start another child
	_, _, endChild2 := tracer.Start(ctx, "ChildOperation2")
	endChild2()

	// End parent
	endParent()
}

func TestSimpleTracing(t *testing.T) {
	// Initialize tracer
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.TracingSampleRate = 1.0

	err := InitTracer(tSettings)
	require.NoError(t, err)

	defer func() {
		_ = ShutdownTracer(context.Background())
	}()

	logger := ulogger.NewVerboseTestLogger(t)

	tracer := Tracer("test-service")

	ctx, span, endFn := tracer.Start(
		context.Background(),
		"operation 1",
		WithTag("foo", "bar"),
		WithLogMessage(logger, "Starting operation 1"),
	)

	_, _, childEndFn := tracer.Start(ctx, "operation 2")

	childEndFn()

	span.AddEvent("bang", trace.WithAttributes(attribute.String("foo", "bar")))

	endFn()
}

type lineLogger struct {
	lastLog string
}

func newLineLogger() *lineLogger {
	return &lineLogger{}
}

func (l *lineLogger) New(service string, options ...ulogger.Option) ulogger.Logger {
	return nil
}
func (l *lineLogger) Duplicate(options ...ulogger.Option) ulogger.Logger { return l }

func (l *lineLogger) LogLevel() int {
	return 0
}
func (l *lineLogger) SetLogLevel(level string) {}

func (l *lineLogger) Debugf(format string, args ...interface{}) {
	l.log("DEBUG", format, args...)
}

func (l *lineLogger) Infof(format string, args ...interface{}) {
	l.log("INFO", format, args...)
}

func (l *lineLogger) Warnf(format string, args ...interface{}) {
	l.log("WARN", format, args...)
}

func (l *lineLogger) Errorf(format string, args ...interface{}) {
	l.log("ERROR", format, args...)
}

func (l *lineLogger) Fatalf(format string, args ...interface{}) {
	l.log("FATAL", format, args...)
}

func (l *lineLogger) log(_ string, format string, args ...interface{}) {
	l.lastLog = fmt.Sprintf(format, args...)
}

func (l *lineLogger) WithTraceContext(_ context.Context) ulogger.Logger {
	return l
}

func setTestTracerProvider(provider *sdktrace.TracerProvider) {
	mu.Lock()
	defer mu.Unlock()
	tp = provider
}

// TestTracingEnabled tests the SetTracingEnabled and IsTracingEnabled functions
func TestTracingEnabled(t *testing.T) {
	// Save current state and restore at end to avoid affecting other tests
	originalState := IsTracingEnabled()
	defer SetTracingEnabled(originalState)

	// Test enabling tracing
	SetTracingEnabled(true)
	assert.True(t, IsTracingEnabled(), "tracing should be enabled after SetTracingEnabled(true)")

	// Test disabling tracing
	SetTracingEnabled(false)
	assert.False(t, IsTracingEnabled(), "tracing should be disabled after SetTracingEnabled(false)")

	// Test multiple toggles
	SetTracingEnabled(true)
	assert.True(t, IsTracingEnabled())
	SetTracingEnabled(true)
	assert.True(t, IsTracingEnabled())
	SetTracingEnabled(false)
	assert.False(t, IsTracingEnabled())
}

// TestTracer_Disabled verifies that Tracer() returns a singleton no-op tracer when disabled
func TestTracer_Disabled(t *testing.T) {
	// Save and restore state
	originalState := IsTracingEnabled()
	defer SetTracingEnabled(originalState)

	// Ensure tracing is disabled
	SetTracingEnabled(false)

	// Get multiple tracers
	tracer1 := Tracer("service1")
	tracer2 := Tracer("service2")
	tracer3 := Tracer("service1") // Same name as tracer1

	// All should return the same singleton instance
	assert.Same(t, tracer1, tracer2, "should return same singleton no-op tracer")
	assert.Same(t, tracer1, tracer3, "should return same singleton no-op tracer")
	assert.Same(t, tracer1, noopTracer, "should return the global noopTracer singleton")

	// Verify it's the no-op tracer
	assert.Equal(t, "noop", tracer1.name, "should be no-op tracer")
}

// TestTracer_Enabled verifies that Tracer() returns different instances when enabled
func TestTracer_Enabled(t *testing.T) {
	// Save and restore state
	originalState := IsTracingEnabled()
	defer SetTracingEnabled(originalState)

	// Initialize test tracer
	err := initTestTracer()
	require.NoError(t, err)
	defer func() {
		_ = ShutdownTracer(context.Background())
	}()

	// Enable tracing
	SetTracingEnabled(true)

	// Get multiple tracers
	tracer1 := Tracer("service1")
	tracer2 := Tracer("service2")

	// Should return different instances (not singleton)
	assert.NotSame(t, tracer1, tracer2, "should return different tracer instances when enabled")
	assert.NotSame(t, tracer1, noopTracer, "should not return no-op tracer when enabled")

	// Names should match
	assert.Equal(t, "service1", tracer1.name)
	assert.Equal(t, "service2", tracer2.name)
}

// TestStart_Disabled verifies that Start() returns no-op span when tracing is disabled
func TestStart_Disabled(t *testing.T) {
	// Save and restore state
	originalState := IsTracingEnabled()
	defer SetTracingEnabled(originalState)

	// Ensure tracing is disabled
	SetTracingEnabled(false)

	tracer := Tracer("test-service")
	ctx := context.Background()

	// Start a span
	newCtx, span, endFn := tracer.Start(ctx, "test-operation",
		WithTag("key", "value"),
	)

	// Verify no-op behavior
	assert.NotNil(t, newCtx, "context should not be nil")
	assert.NotNil(t, span, "span should not be nil")
	assert.NotNil(t, endFn, "end function should not be nil")

	// The span should be a no-op span (not recording)
	assert.False(t, span.IsRecording(), "span should not be recording when tracing disabled")

	// End function should not panic
	endFn()
	endFn(errors.NewProcessingError("test error"))
}

// TestStart_DisabledWithLoggingAndMetrics verifies that logging and metrics still work when tracing is disabled
func TestStart_DisabledWithLoggingAndMetrics(t *testing.T) {
	originalState := IsTracingEnabled()
	defer SetTracingEnabled(originalState)

	SetTracingEnabled(false)

	logger := mocklogger.NewTestLogger()

	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "test_counter",
		Help: "Test counter for tracing disabled test",
	})

	histogram := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "test_histogram",
		Help: "Test histogram for tracing disabled test",
	})

	tracer := Tracer("test-service")
	ctx := context.Background()

	parentStat := gocore.NewStat("parent")

	newCtx, span, endFn := tracer.Start(ctx, "test-operation",
		WithLogMessage(logger, "Test message: %s", "hello"),
		WithParentStat(parentStat),
		WithCounter(counter),
		WithHistogram(histogram),
	)

	require.NotNil(t, newCtx)
	require.NotNil(t, span)
	require.NotNil(t, endFn)
	require.False(t, span.IsRecording(), "span should not be recording when tracing disabled")

	time.Sleep(10 * time.Millisecond)
	endFn()

	logger.AssertNumberOfCalls(t, "Infof", 2)

	metric := &dto.Metric{}
	err := counter.Write(metric)
	require.NoError(t, err)
	require.Equal(t, float64(1), metric.Counter.GetValue(), "counter should be incremented even when tracing is disabled")

	err = histogram.Write(metric)
	require.NoError(t, err)
	require.Equal(t, uint64(1), metric.Histogram.GetSampleCount(), "histogram should be observed even when tracing is disabled")
}

// TestStart_Enabled verifies that Start() returns real span when tracing is enabled
func TestStart_Enabled(t *testing.T) {
	// Save and restore state
	originalState := IsTracingEnabled()
	defer SetTracingEnabled(originalState)

	// Initialize test tracer
	err := initTestTracer()
	require.NoError(t, err)
	defer func() {
		_ = ShutdownTracer(context.Background())
	}()

	// Enable tracing
	SetTracingEnabled(true)

	tracer := Tracer("test-service")
	ctx := context.Background()

	// Start a span
	newCtx, span, endFn := tracer.Start(ctx, "test-operation")

	// Verify real span behavior
	assert.NotNil(t, newCtx)
	assert.NotNil(t, span)
	assert.NotNil(t, endFn)

	// The span should be recording when tracing is enabled
	assert.True(t, span.IsRecording(), "span should be recording when tracing enabled")

	// Cleanup
	endFn()
}

// TestDecoupleTracingSpan_Disabled verifies DecoupleTracingSpan returns no-op when disabled
func TestDecoupleTracingSpan_Disabled(t *testing.T) {
	// Save and restore state
	originalState := IsTracingEnabled()
	defer SetTracingEnabled(originalState)

	// Ensure tracing is disabled
	SetTracingEnabled(false)

	ctx := context.Background()

	// Call DecoupleTracingSpan
	newCtx, span, endFn := DecoupleTracingSpan(ctx, "test-service", "decoupled-operation")

	// Verify no-op behavior
	assert.NotNil(t, newCtx)
	assert.NotNil(t, span)
	assert.NotNil(t, endFn)

	// The span should be a no-op span
	assert.False(t, span.IsRecording(), "span should not be recording when tracing disabled")

	// End function should not panic
	endFn()
	endFn(errors.NewProcessingError("test error"))
}

// TestDecoupleTracingSpan_Enabled verifies DecoupleTracingSpan returns real span when enabled
func TestDecoupleTracingSpan_Enabled(t *testing.T) {
	// Save and restore state
	originalState := IsTracingEnabled()
	defer SetTracingEnabled(originalState)

	// Initialize test tracer
	err := initTestTracer()
	require.NoError(t, err)
	defer func() {
		_ = ShutdownTracer(context.Background())
	}()

	// Enable tracing
	SetTracingEnabled(true)

	// Create parent span
	tracer := Tracer("test-service")
	ctx, parentSpan, endParent := tracer.Start(context.Background(), "parent-operation")

	// Verify parent is recording
	require.True(t, parentSpan.IsRecording())

	// Call DecoupleTracingSpan
	newCtx, span, endFn := DecoupleTracingSpan(ctx, "test-service", "decoupled-operation")

	// Verify real span behavior
	assert.NotNil(t, newCtx)
	assert.NotNil(t, span)
	assert.NotNil(t, endFn)

	// The span should be recording
	assert.True(t, span.IsRecording(), "span should be recording when tracing enabled")

	// Cleanup
	endFn()
	endParent()
}

// TestTracingDisabled_NoAllocation verifies that disabled tracing returns singleton without allocation
func TestTracingDisabled_NoAllocation(t *testing.T) {
	// Save and restore state
	originalState := IsTracingEnabled()
	defer SetTracingEnabled(originalState)

	// Ensure tracing is disabled
	SetTracingEnabled(false)

	// This test verifies the optimization works, but we can't easily test allocations
	// in a unit test without benchmarks. We'll just verify behavior is correct.

	// Get tracer multiple times
	tracer1 := Tracer("service1")
	tracer2 := Tracer("service2")
	tracer3 := Tracer("service3")

	// All should be the exact same instance
	assert.Same(t, tracer1, tracer2)
	assert.Same(t, tracer2, tracer3)

	// Start spans multiple times
	ctx := context.Background()
	_, span1, end1 := tracer1.Start(ctx, "op1")
	_, span2, end2 := tracer1.Start(ctx, "op2")

	// Both spans should not be recording
	assert.False(t, span1.IsRecording())
	assert.False(t, span2.IsRecording())

	// End functions should work
	end1()
	end2()
}

// TestWithSampleRate_AlwaysSample verifies WithSampleRate(1.0) forces sampling
func TestWithSampleRate_AlwaysSample(t *testing.T) {
	originalState := IsTracingEnabled()
	defer SetTracingEnabled(originalState)

	// Use NeverSample as base — without override, spans would be dropped
	err := initTestTracerWithSampler(sdktrace.NeverSample())
	require.NoError(t, err)
	defer func() {
		_ = ShutdownTracer(context.Background())
	}()

	tracer := Tracer("test-service")
	_, span, endFn := tracer.Start(context.Background(), "test-operation",
		WithSampleRate(1.0),
	)
	defer endFn()

	assert.True(t, span.IsRecording(), "span should be recording with WithSampleRate(1.0)")
}

// TestWithAlwaysSample verifies the convenience function
func TestWithAlwaysSample(t *testing.T) {
	originalState := IsTracingEnabled()
	defer SetTracingEnabled(originalState)

	err := initTestTracerWithSampler(sdktrace.NeverSample())
	require.NoError(t, err)
	defer func() {
		_ = ShutdownTracer(context.Background())
	}()

	tracer := Tracer("test-service")
	_, span, endFn := tracer.Start(context.Background(), "test-operation",
		WithAlwaysSample(),
	)
	defer endFn()

	assert.True(t, span.IsRecording(), "span should be recording with WithAlwaysSample()")
}

// TestWithSampleRate_NeverSample verifies WithSampleRate(0.0) suppresses sampling
func TestWithSampleRate_NeverSample(t *testing.T) {
	originalState := IsTracingEnabled()
	defer SetTracingEnabled(originalState)

	// Use AlwaysSample as base — without override, spans would be sampled
	err := initTestTracerWithSampler(sdktrace.AlwaysSample())
	require.NoError(t, err)
	defer func() {
		_ = ShutdownTracer(context.Background())
	}()

	tracer := Tracer("test-service")
	_, span, endFn := tracer.Start(context.Background(), "test-operation",
		WithSampleRate(0.0),
	)
	defer endFn()

	assert.False(t, span.IsRecording(), "span should not be recording with WithSampleRate(0.0)")
}

// TestWithSampleRate_Clamping verifies out-of-range values are clamped
func TestWithSampleRate_Clamping(t *testing.T) {
	opts := &TraceOptions{}

	WithSampleRate(-0.5)(opts)
	require.NotNil(t, opts.SampleRate)
	assert.Equal(t, 0.0, *opts.SampleRate, "negative rate should be clamped to 0")

	WithSampleRate(1.5)(opts)
	require.NotNil(t, opts.SampleRate)
	assert.Equal(t, 1.0, *opts.SampleRate, "rate > 1 should be clamped to 1")
}

// TestWithSampleRate_NoOverheadWhenNotUsed verifies no context injection without the option
func TestWithSampleRate_NoOverheadWhenNotUsed(t *testing.T) {
	originalState := IsTracingEnabled()
	defer SetTracingEnabled(originalState)

	err := initTestTracer()
	require.NoError(t, err)
	defer func() {
		_ = ShutdownTracer(context.Background())
	}()

	tracer := Tracer("test-service")
	ctx, _, endFn := tracer.Start(context.Background(), "test-operation")
	defer endFn()

	// Context should NOT contain the override key
	val := ctx.Value(sampleRateOverrideKey{})
	assert.Nil(t, val, "context should not contain sample rate override when option not used")
}

// TestShortCircuit_UnsampledParentSkipsOTelSDK verifies that child spans of unsampled
// parents are short-circuited and don't enter the OTel SDK.
func TestShortCircuit_UnsampledParentSkipsOTelSDK(t *testing.T) {
	originalState := IsTracingEnabled()
	defer SetTracingEnabled(originalState)

	// Use NeverSample so root spans are dropped (unsampled)
	err := initTestTracerWithSampler(sdktrace.NeverSample())
	require.NoError(t, err)
	defer func() {
		_ = ShutdownTracer(context.Background())
	}()

	tracer := Tracer("test-service")

	// Start a root span — NeverSample means it won't be sampled
	ctx, rootSpan, endRoot := tracer.Start(context.Background(), "root-operation")
	defer endRoot()

	// Root span is not recording (unsampled)
	require.False(t, rootSpan.IsRecording(), "root span should not be recording with NeverSample")
	require.True(t, rootSpan.SpanContext().IsValid(), "root span should have a valid SpanContext")
	require.False(t, rootSpan.SpanContext().IsSampled(), "root span should not be sampled")

	// Start child span — should be short-circuited since parent is unsampled
	_, childSpan, endChild := tracer.Start(ctx, "child-operation")
	defer endChild()

	// Child span should also not be recording
	assert.False(t, childSpan.IsRecording(), "child span should not be recording (short-circuited)")

	// The child span should be the same as the parent span (short-circuit reuses parent)
	assert.Equal(t, rootSpan.SpanContext(), childSpan.SpanContext(),
		"short-circuited child should reuse the parent span context")

	// Ending the child should not panic (it should be a no-op for the OTel span)
	endChild()
}

// TestShortCircuit_WithSampleRateOverrideBypasses verifies that WithSampleRate
// prevents short-circuiting even when the parent is unsampled.
func TestShortCircuit_WithSampleRateOverrideBypasses(t *testing.T) {
	originalState := IsTracingEnabled()
	defer SetTracingEnabled(originalState)

	// Use NeverSample so root spans are dropped
	err := initTestTracerWithSampler(sdktrace.NeverSample())
	require.NoError(t, err)
	defer func() {
		_ = ShutdownTracer(context.Background())
	}()

	tracer := Tracer("test-service")

	// Start unsampled root
	ctx, rootSpan, endRoot := tracer.Start(context.Background(), "root-operation")
	defer endRoot()
	require.False(t, rootSpan.IsRecording())

	// Start child with forced sampling — should NOT be short-circuited
	_, childSpan, endChild := tracer.Start(ctx, "child-operation",
		WithAlwaysSample(),
	)
	defer endChild()

	// Child should be recording despite unsampled parent
	assert.True(t, childSpan.IsRecording(),
		"child with WithAlwaysSample should be recording even with unsampled parent")
}

// TestShortCircuit_MetricsAndLoggingStillWork verifies that stats, metrics,
// and logging still function correctly on the short-circuited path.
func TestShortCircuit_MetricsAndLoggingStillWork(t *testing.T) {
	originalState := IsTracingEnabled()
	defer SetTracingEnabled(originalState)

	err := initTestTracerWithSampler(sdktrace.NeverSample())
	require.NoError(t, err)
	defer func() {
		_ = ShutdownTracer(context.Background())
	}()

	logger := newLineLogger()
	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "test_shortcircuit_counter",
		Help: "Test counter",
	})

	tracer := Tracer("test-service")

	// Start unsampled root
	ctx, _, endRoot := tracer.Start(context.Background(), "root-operation")
	defer endRoot()

	// Start short-circuited child with metrics and logging
	parentStat := gocore.NewStat("test-parent")
	_, _, endChild := tracer.Start(ctx, "child-operation",
		WithLogMessage(logger, "Processing child"),
		WithParentStat(parentStat),
		WithCounter(counter),
	)

	time.Sleep(5 * time.Millisecond)
	endChild()

	// Logging should still work
	assert.Contains(t, logger.lastLog, "Processing child DONE in")

	// Counter should still be incremented
	metric := &dto.Metric{}
	err = counter.Write(metric)
	require.NoError(t, err)
	assert.Equal(t, float64(1), metric.Counter.GetValue(),
		"counter should be incremented even on short-circuited path")
}

// TestWithSampleRate_ChildSpanInheritsOverride verifies children of force-sampled spans are also sampled
func TestWithSampleRate_ChildSpanInheritsOverride(t *testing.T) {
	originalState := IsTracingEnabled()
	defer SetTracingEnabled(originalState)

	err := initTestTracerWithSampler(sdktrace.NeverSample())
	require.NoError(t, err)
	defer func() {
		_ = ShutdownTracer(context.Background())
	}()

	tracer := Tracer("test-service")

	// Parent with forced sampling
	ctx, parentSpan, endParent := tracer.Start(context.Background(), "parent",
		WithAlwaysSample(),
	)
	defer endParent()
	require.True(t, parentSpan.IsRecording(), "parent should be recording")

	// Child without explicit sample rate — should inherit via context
	_, childSpan, endChild := tracer.Start(ctx, "child")
	defer endChild()
	assert.True(t, childSpan.IsRecording(), "child should be recording due to inherited context override")
}

// TestStart_DisabledSkipsStatCreation verifies that when tracing is disabled the
// per-span gocore.Stat is not created/injected (it is the dominant unconditional
// cost on the hot sync path), and that StartTime is not injected by default
// (it is opt-in via WithStartTime; see TestStart_StartTimeOptIn).
func TestStart_DisabledSkipsStatCreation(t *testing.T) {
	originalState := IsTracingEnabled()
	defer SetTracingEnabled(originalState)

	SetTracingEnabled(false)

	tracer := Tracer("test-service")
	ctx := context.Background()

	newCtx, _, endFn := tracer.Start(ctx, "op",
		WithTag("key", "value"),
		WithParentStat(gocore.NewStat("parent")),
	)
	defer endFn()

	// No gocore.Stat should be injected when tracing is disabled.
	require.Nil(t, newCtx.Value(statsKey{}), "no gocore.Stat should be injected when tracing disabled")

	// StartTime is opt-in (WithStartTime) and must not be injected by default.
	require.Nil(t, newCtx.Value(StartTime), "StartTime should not be injected without WithStartTime")
}

// TestStart_StartTimeOptIn verifies StartTime is injected into the context only
// when WithStartTime is supplied (blockvalidation catchup reads it), and is absent
// otherwise — avoiding the per-span context.WithValue boxing on the hot path.
func TestStart_StartTimeOptIn(t *testing.T) {
	originalState := IsTracingEnabled()
	defer SetTracingEnabled(originalState)

	SetTracingEnabled(false)

	tracer := Tracer("test-service")

	// Without WithStartTime: absent.
	ctxNo, _, endNo := tracer.Start(context.Background(), "no-opt")
	require.Nil(t, ctxNo.Value(StartTime), "StartTime should be absent without WithStartTime")

	endNo()

	// With WithStartTime: present and a time.Time.
	ctxYes, _, endYes := tracer.Start(context.Background(), "opt", WithStartTime())
	st := ctxYes.Value(StartTime)
	require.NotNil(t, st, "StartTime should be injected with WithStartTime")

	_, ok := st.(time.Time)
	require.True(t, ok, "StartTime should be a time.Time")

	endYes()
}

// BenchmarkStart_Disabled measures per-span allocation on the disabled hot path
// (the legacy sync path creates millions of these per block).
func BenchmarkStart_Disabled(b *testing.B) {
	originalState := IsTracingEnabled()
	defer SetTracingEnabled(originalState)

	SetTracingEnabled(false)

	tracer := Tracer("bench")
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, endFn := tracer.Start(ctx, "op",
			WithTag("txid", "abc123"),
			WithTag("height", "875800"),
		)
		endFn()
	}
}

// BenchmarkStart_Disabled_NoOptions mirrors the hottest disabled-path span in the
// codebase (e.g. aerospike:Create, which passes no options). With the pooled
// TraceOptions and the shared no-op end function this should report 0 allocs/op.
func BenchmarkStart_Disabled_NoOptions(b *testing.B) {
	originalState := IsTracingEnabled()
	defer SetTracingEnabled(originalState)

	SetTracingEnabled(false)

	tracer := Tracer("bench")
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, endFn := tracer.Start(ctx, "op")
		endFn()
	}
}

// BenchmarkStart_Disabled_Histogram mirrors the validator's per-transaction spans
// (WithHistogram), which take the slow path because the prometheus metric must be
// observed even when tracing is disabled. The pool removes the TraceOptions
// allocation; only the end-closure allocation remains.
func BenchmarkStart_Disabled_Histogram(b *testing.B) {
	originalState := IsTracingEnabled()
	defer SetTracingEnabled(originalState)

	SetTracingEnabled(false)

	histogram := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "bench_histogram_disabled",
		Help: "bench",
	})

	tracer := Tracer("bench")
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, endFn := tracer.Start(ctx, "op", WithHistogram(histogram))
		endFn()
	}
}

// TestStart_PooledOptionsNoStateBleed guards the sync.Pool reset: a span that sets
// a counter must not leave that counter set on the recycled TraceOptions, or a
// later option-less span would spuriously increment it.
func TestStart_PooledOptionsNoStateBleed(t *testing.T) {
	originalState := IsTracingEnabled()
	defer SetTracingEnabled(originalState)

	SetTracingEnabled(false)

	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "test_pool_bleed_counter",
		Help: "bench",
	})

	tracer := Tracer("svc")
	ctx := context.Background()

	// Span A carries a counter (slow path) and returns its options to the pool.
	_, _, endA := tracer.Start(ctx, "A", WithCounter(counter))
	endA()

	require.Equal(t, float64(1), counterValue(t, counter))

	// Span B carries no options and (in the same goroutine) reuses A's pooled
	// TraceOptions. If reset() failed to clear Counter, this would increment it.
	_, _, endB := tracer.Start(ctx, "B")
	endB()

	require.Equal(t, float64(1), counterValue(t, counter), "recycled options must not retain the previous span's counter")
}

// TestStart_DisabledTimeoutStillCancels verifies WithContextTimeout is honoured on
// the disabled path: the context carries the deadline and the span's end function
// cancels it.
func TestStart_DisabledTimeoutStillCancels(t *testing.T) {
	originalState := IsTracingEnabled()
	defer SetTracingEnabled(originalState)

	SetTracingEnabled(false)

	tracer := Tracer("svc")

	ctx, _, endFn := tracer.Start(context.Background(), "op", WithContextTimeout(time.Minute))

	_, hasDeadline := ctx.Deadline()
	require.True(t, hasDeadline, "context timeout must be applied even when tracing is disabled")

	endFn()
	require.Error(t, ctx.Err(), "end function must cancel the timeout context")
}

// TestStart_DisabledDoubleEndIsSafe verifies the end function finalises exactly
// once: a second call does not panic, does not double-count the metric, and does
// not corrupt the pooled object for a subsequently issued span.
func TestStart_DisabledDoubleEndIsSafe(t *testing.T) {
	originalState := IsTracingEnabled()
	defer SetTracingEnabled(originalState)

	SetTracingEnabled(false)

	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "test_double_end_counter",
		Help: "bench",
	})

	tracer := Tracer("svc")
	ctx := context.Background()

	_, _, endFn := tracer.Start(ctx, "op", WithCounter(counter))
	require.NotPanics(t, func() {
		endFn()
		endFn()
	})

	// Finalisation is once-only: the second call must be a no-op, not a second
	// increment (and must not touch the recycled object).
	require.Equal(t, float64(1), counterValue(t, counter), "double end must not double-count the counter")

	// A subsequent span must still behave correctly (pool not corrupted by the
	// repeated hand-back guard).
	_, _, endNext := tracer.Start(ctx, "next")
	require.NotPanics(t, func() { endNext() })
}

// TestStart_ConcurrentEndFinalisesOnce verifies the per-span atomic guard: the end
// function may be called from multiple goroutines at once and finalisation still
// runs exactly once (the counter is incremented once, not once per goroutine) with
// no data race. Run under -race.
func TestStart_ConcurrentEndFinalisesOnce(t *testing.T) {
	originalState := IsTracingEnabled()
	defer SetTracingEnabled(originalState)

	SetTracingEnabled(false)

	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "test_concurrent_end_counter",
		Help: "bench",
	})

	tracer := Tracer("svc")
	_, _, endFn := tracer.Start(context.Background(), "op", WithCounter(counter))

	const goroutines = 16

	var wg sync.WaitGroup

	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			endFn()
		}()
	}

	wg.Wait()

	require.Equal(t, float64(1), counterValue(t, counter), "concurrent end calls must finalise exactly once")
}

// TestStart_StaleEndAfterRecycleDoesNotCorrupt is the regression test for the
// pool-recycle hazard: a second (stale) call to a span's end function, landing
// after that span's pooled TraceOptions was returned to the pool and reissued to
// a later span, must not run finalisation against the later span's options.
//
// This is exactly the case a guard stored ON the pooled object could not catch
// (reset() clears such a flag on reissue, re-opening it for the stale call). The
// guard is a per-span closure local instead, so once S1 has ended, end1() is a
// permanent no-op regardless of what happens to the recycled object.
func TestStart_StaleEndAfterRecycleDoesNotCorrupt(t *testing.T) {
	originalState := IsTracingEnabled()
	defer SetTracingEnabled(originalState)

	SetTracingEnabled(false)

	c1 := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_stale_end_c1", Help: "bench"})
	c2 := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_stale_end_c2", Help: "bench"})

	tracer := Tracer("svc")

	// S1 ends once — its options are returned to the pool.
	_, _, end1 := tracer.Start(context.Background(), "s1", WithCounter(c1))
	end1()
	require.Equal(t, float64(1), counterValue(t, c1))

	// S2 starts (same goroutine → very likely reuses S1's recycled object) and is
	// configured with a different counter.
	_, _, end2 := tracer.Start(context.Background(), "s2", WithCounter(c2))

	// Stale second end of S1 must be a permanent no-op: it must neither double-count
	// c1 nor observe c2 (which it would, had the guard lived on the recycled object).
	end1()

	require.Equal(t, float64(1), counterValue(t, c1), "stale re-end must not double-count S1")
	require.Equal(t, float64(0), counterValue(t, c2), "stale re-end of S1 must not touch the recycled span S2")

	end2()
	require.Equal(t, float64(1), counterValue(t, c2))
}

// counterValue reads the current value of a prometheus counter.
func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()

	metric := &dto.Metric{}
	require.NoError(t, c.Write(metric))

	return metric.Counter.GetValue()
}
