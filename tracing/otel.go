package tracing

import (
	"context"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/ordishs/gocore"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/trace"
	"io"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

// InitOtelTracer initializes the OpenTelemetry tracer
// serviceName: the name of the service
// samplingRate: the rate at which to sample traces (0.0 - 1.0)
func InitOtelTracer(serviceName string, samplingRate float64) (io.Closer, error) {
	tracerURL, err, found := gocore.Config().GetURL("tracing_collector_url")
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.NewConfigurationError("tracing_collector_url not found in config", nil)
	}

	headers := map[string]string{
		"content-type": "application/json",
	}

	exporter, err := otlptrace.New(
		context.Background(),
		otlptracehttp.NewClient(
			otlptracehttp.WithEndpoint(tracerURL.String()),
			otlptracehttp.WithHeaders(headers),
			otlptracehttp.WithInsecure(),
		),
	)
	if err != nil {
		return nil, err
	}

	tracerProvider := trace.NewTracerProvider(
		trace.WithSampler(trace.TraceIDRatioBased(samplingRate)),
		trace.WithBatcher(
			exporter,
			trace.WithMaxExportBatchSize(trace.DefaultMaxExportBatchSize),
			trace.WithBatchTimeout(trace.DefaultScheduleDelay*time.Millisecond),
			trace.WithMaxExportBatchSize(trace.DefaultMaxExportBatchSize),
		),
		trace.WithResource(
			resource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceNameKey.String(serviceName),
			),
		),
	)

	otel.SetTracerProvider(tracerProvider)

	return nil, nil
}
