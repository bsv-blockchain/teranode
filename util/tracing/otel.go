package tracing

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"go.opentelemetry.io/otel"
	attribute "go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

var (
	once    sync.Once
	initErr error
	tp      *sdktrace.TracerProvider
	mu      sync.Mutex
)

// InitTracer initializes the global tracer. Safe to call multiple times.
// Only the first call will actually initialize the tracer.
// Returns an error if initialization fails.
func InitTracer(appSettings *settings.Settings) error {
	once.Do(func() {
		// Create OTLP exporter
		var exporter *otlptrace.Exporter

		exporter, initErr = otlptracehttp.New(
			context.Background(),
			otlptracehttp.WithEndpoint(appSettings.TracingCollectorURL.String()), // Jaeger OTLP HTTP endpoint
			otlptracehttp.WithInsecure(),                                         // Use this for HTTP, remove for HTTPS
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
			sdktrace.WithSampler(sdktrace.TraceIDRatioBased(appSettings.TracingSampleRate)),
			sdktrace.WithResource(res),
		)

		// Set the global trace provider only after validation succeeds
		otel.SetTracerProvider(tp)

		// Set up propagation (for distributed tracing)
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))
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
				log.Printf("ERROR: failed to flush spans: %v", err)
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
