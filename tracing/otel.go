package tracing

import (
	"context"
	"os"

	// "go.opentelemetry.io/otel/exporters/jaeger"
	"github.com/ordishs/gocore"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

// InitOtelTracer initializes the OpenTelemetry tracer
// TODO this is not used anymore since the Jaeger exporter was deprecated
func InitOtelTracer() func() {
	// set the tracer provider
	found := false
	//tracerURL, err, found := gocore.Config().GetURL("tracing_collector_url")
	//if err != nil {
	//	panic(err)
	//}

	if found {
		//var exp *jaeger.Exporter
		//switch tracerURL.Scheme {
		//case "jaeger":
		//	exp, err = jaeger.New(
		//		jaeger.WithAgentEndpoint(
		//			jaeger.WithAgentHost(tracerURL.Hostname()),
		//			jaeger.WithAgentPort(tracerURL.Port()),
		//		),
		//	)
		//	if err != nil {
		//		panic(err)
		//	}
		//case "http":
		//	exp, err = jaeger.New(
		//		jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(tracerURL.String())),
		//	)
		//	if err != nil {
		//		panic(err)
		//	}
		//}

		service := os.Getenv("SERVICE_NAME")
		if service == "" {
			service = "ubsv"
		}
		environment := gocore.Config().GetContext()
		pod := os.Getenv("HOSTNAME")
		if pod == "" {
			pod = "unknown"
		}

		// setup a jaeger trace provider
		tp := tracesdk.NewTracerProvider(
			// Always be sure to batch in production.
			//tracesdk.WithBatcher(exp),
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
			if err := tp.Shutdown(context.Background()); err != nil {
				panic(err)
			}
		}
	}

	return nil
}
