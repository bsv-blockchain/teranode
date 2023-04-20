package tracing

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

func InitOtelTracer() func() {
	// set the tracer provider
	// TODO get from config
	exp, err := jaeger.New(
		jaeger.WithAgentEndpoint(jaeger.WithAgentPort("6831")),
	)
	if err != nil {
		panic(err)
	}

	// TODO get from K8s
	service := "ubsv"
	environment := "dev"
	pod := "pod-1"

	// setup a jaeger trace provider
	tp := tracesdk.NewTracerProvider(
		// Always be sure to batch in production.
		tracesdk.WithBatcher(exp),
		// Record information about this application in a Resource.
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(service),
			attribute.String("environment", environment),
			attribute.String("pod", pod),
		)),
	)

	otel.SetTracerProvider(tp)

	return func() {
		if err = tp.Shutdown(context.Background()); err != nil {
			panic(err)
		}
	}
}
