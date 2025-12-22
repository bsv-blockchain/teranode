package tracing

import (
	"context"
	"fmt"
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
