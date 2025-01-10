package tracing

import (
	"io"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
)

// InitTracer creates a new instance of a teranode tracer.
func InitTracer(serviceName string, samplingRate float64, tSettings *settings.Settings) (io.Closer, error) {
	if !tSettings.UseOpenTracing {
		return InitOpenTracer(serviceName, samplingRate, tSettings)
	}

	if tSettings.UseOtelTracing {
		return InitOtelTracer(serviceName, samplingRate)
	}

	return nil, nil
}

// InitOpenTracer initializes the Jaeger tracer using opentracing
// serviceName: the name of the service
// samplingRate: the rate at which to sample traces (0.0 - 1.0)
func InitOpenTracer(serviceName string, samplingRate float64, tSettings *settings.Settings) (io.Closer, error) {
	if !tSettings.UseOpenTracing {
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
