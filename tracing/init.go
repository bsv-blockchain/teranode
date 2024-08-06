package tracing

import (
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/gocore"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
	"io"
)

// InitTracer creates a new instance of a ubsv tracer.
func InitTracer(serviceName string, samplingRate float64) (io.Closer, error) {
	useTracing := gocore.Config().GetBool("use_open_tracing", true)
	if !useTracing {
		return InitOpenTracer(serviceName, samplingRate)
	}
	useOtelTracing := gocore.Config().GetBool("use_otel_tracing", false)
	if useOtelTracing {
		return InitOtelTracer(serviceName, samplingRate)
	}

	return nil, nil
}

// InitOpenTracer initializes the Jaeger tracer using opentracing
// serviceName: the name of the service
// samplingRate: the rate at which to sample traces (0.0 - 1.0)
func InitOpenTracer(serviceName string, samplingRate float64) (io.Closer, error) {
	useTracing := gocore.Config().GetBool("use_open_tracing", true)
	if !useTracing {
		return nil, nil
	}

	cfg, err := config.FromEnv()
	if err != nil {
		return nil, errors.NewConfigurationError("cannot parse jaeger environment variables", err)
	}

	cfg.ServiceName = serviceName
	cfg.Sampler.Type = jaeger.SamplerTypeProbabilistic
	cfg.Sampler.Param = samplingRate

	var tracer opentracing.Tracer
	var closer io.Closer
	tracer, closer, err = cfg.NewTracer()
	if err != nil {
		return nil, errors.NewConfigurationError("cannot initialize jaeger tracer", err)
	}

	opentracing.SetGlobalTracer(tracer)

	return closer, nil
}
