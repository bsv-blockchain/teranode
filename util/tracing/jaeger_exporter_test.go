package tracing

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
)

// TestJaegerExporter demonstrates how to set up a tracer that exports spans to Jaeger.
//
// To run this test and see the results:
//
//  1. Start Jaeger using Docker:
//     docker run -d --name jaeger \
//     -e COLLECTOR_OTLP_ENABLED=true \
//     -p 16686:16686 \
//     -p 4317:4317 \
//     -p 4318:4318 \
//     jaegertracing/all-in-one:latest
//
//  2. Run this test:
//     go test -v ./util/tracing -run TestJaegerExporter
//
//  3. Open Jaeger UI in your browser:
//     http://localhost:16686
//
// 4. Look for traces from the "standalone-otel-example" service
func TestJaegerExporter(t *testing.T) {
	// Skip if running in CI or if ENABLE_JAEGER_TEST is not set
	if testing.Short() {
		t.Skip("Skipping Jaeger exporter test in short mode")
	}
	// Set up the tracer provider with Jaeger exporter
	shutdown, err := initJaegerTracer()
	require.NoError(t, err)

	// Make sure to shut down the tracer provider at the end
	defer func() {
		err := shutdown(context.Background())
		require.NoError(t, err)
	}()

	// Create a root span
	ctx, rootSpan := otel.Tracer("standalone-otel-example").Start(
		context.Background(),
		"root-span",
		trace.WithAttributes(attribute.String("test.key", "test.value")),
	)

	// Create a child span
	_, childSpan := otel.Tracer("standalone-otel-example").Start(
		ctx,
		"child-span",
		trace.WithAttributes(attribute.Int64("child.count", 1)),
	)

	// Simulate some work
	time.Sleep(10 * time.Millisecond)

	// End the child span
	childSpan.End()

	// Simulate more work in the parent span
	time.Sleep(10 * time.Millisecond)

	// End the root span
	rootSpan.End()

	// Wait a bit to ensure spans are exported before shutdown
	time.Sleep(50 * time.Millisecond)
}

// initJaegerTracer sets up a tracer provider with a Jaeger exporter
func initJaegerTracer() (func(context.Context) error, error) {
	// Create OTLP exporter with shorter timeout for tests
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	exporter, err := otlptracehttp.New(
		ctx,
		otlptracehttp.WithEndpoint("localhost:4318"),    // Jaeger OTLP HTTP endpoint
		otlptracehttp.WithInsecure(),                    // Use this for HTTP, remove for HTTPS
		otlptracehttp.WithTimeout(100*time.Millisecond), // Short timeout for tests
	)
	if err != nil {
		return nil, err
	}

	// Create resource with service information
	res, err := resource.New(
		context.Background(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String("standalone-otel-example"),
			semconv.ServiceVersionKey.String("1.0.0"),
		),
	)
	if err != nil {
		log.Fatalf("Failed to create resource: %v", err)
	}

	// Create trace provider with the exporter
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(100*time.Millisecond)), // Send batches every 100ms for tests
		sdktrace.WithSampler(sdktrace.AlwaysSample()), // Sample all traces
		sdktrace.WithResource(res),
	)

	// Set the global trace provider
	otel.SetTracerProvider(tp)

	// Set up propagation (for distributed tracing)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// Return a shutdown function that will flush and close the exporter
	return tp.Shutdown, nil
}
