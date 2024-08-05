package tracing

import (
	"io"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/ordishs/gocore"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
)

// InitTracer creates a new instance of Jaeger tracer.
func InitTracer(logger ulogger.Logger, serviceName string) (opentracing.Tracer, io.Closer) {
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

func InitGlobalTracer(serviceName string, samplingRate float64) (opentracing.Tracer, io.Closer, error) {
	// TODO ipfs/go-log registers a tracer in its init() function() :-S
	// if opentracing.IsGlobalTracerRegistered() {
	//      so we cannot check this here and must overwrite it
	//return nil, nil, errors.New("global tracer already registered")
	// }

	useTracing := gocore.Config().GetBool("use_open_tracing", true)
	if !useTracing {
		return &mocktracer.MockTracer{}, nil, nil
	}

	cfg, err := config.FromEnv()
	if err != nil {
		return nil, nil, errors.NewConfigurationError("cannot parse jaeger environment variables", err)
	}

	cfg.ServiceName = serviceName
	cfg.Sampler.Type = jaeger.SamplerTypeProbabilistic
	cfg.Sampler.Param = samplingRate

	var tracer opentracing.Tracer
	var closer io.Closer
	tracer, closer, err = cfg.NewTracer()
	if err != nil {
		return nil, nil, errors.NewConfigurationError("cannot initialize jaeger tracer", err)
	}

	opentracing.SetGlobalTracer(tracer)

	return tracer, closer, nil
}
