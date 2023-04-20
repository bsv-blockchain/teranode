package tracing

import (
	"io"

	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/go-utils"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
)

// InitTracer creates a new instance of Jaeger tracer.
func InitTracer(logger utils.Logger, serviceName string) (opentracing.Tracer, io.Closer) {
	cfg, err := config.FromEnv()
	if err != nil {
		logger.Errorf("cannot parse jaeger env vars: %v\n", err.Error())
		return nil, nil
	}

	//cfg.Reporter.CollectorEndpoint = "http://jaeger:14268/api/traces"
	cfg.Reporter.CollectorEndpoint = "http://jaeger-cluster-collector.jaeger.svc.cluster.local:14250/api/traces"

	cfg.ServiceName = serviceName
	cfg.Sampler.Type = jaeger.SamplerTypeConst
	cfg.Sampler.Param = 100

	var tracer opentracing.Tracer
	var closer io.Closer
	tracer, closer, err = cfg.NewTracer()
	if err != nil {
		logger.Errorf("cannot initialize jaeger tracer: %v\n", err.Error())
		return nil, nil
	}

	return tracer, closer
}
