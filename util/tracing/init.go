package tracing

import (
	"fmt"
	"io"
	"net/url"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
)

var (
	UseOpenTracing bool
)

// InitTracer creates a new instance of a tracer.
func InitTracer(serviceName string, samplingRate float64, useOpenTracing bool, otelTracerURL *url.URL) (io.Closer, error) {
	if !useOpenTracing {
		UseOpenTracing = useOpenTracing
		return InitOpenTracer(serviceName, samplingRate)
	}

	if otelTracerURL != nil {
		return InitOtelTracer(serviceName, samplingRate, otelTracerURL)
	}

	return nil, nil
}

// InitOpenTracer initializes the Jaeger tracer using opentracing
// serviceName: the name of the service
// samplingRate: the rate at which to sample traces (0.0 - 1.0)
func InitOpenTracer(serviceName string, samplingRate float64) (io.Closer, error) {
	if !UseOpenTracing {
		return nil, nil
	}

	cfg, err := config.FromEnv()
	if err != nil {
		return nil, fmt.Errorf("cannot parse jaeger environment variables: %s", err)
	}

	cfg.ServiceName = serviceName
	cfg.Sampler.Type = jaeger.SamplerTypeProbabilistic
	cfg.Sampler.Param = samplingRate

	var tracer opentracing.Tracer

	var closer io.Closer

	tracer, closer, err = cfg.NewTracer()
	if err != nil {
		return nil, fmt.Errorf("cannot initialize jaeger tracer: %s", err)
	}

	opentracing.SetGlobalTracer(tracer)

	return closer, nil
}
