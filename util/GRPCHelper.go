package util

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc/keepalive"

	"github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/gocore"
	prometheus_golang "github.com/prometheus/client_golang/prometheus"

	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
)

const (
	ONE_GIGABYTE = 1024 * 1024 * 1024
)

// PasswordCredentials -----------------------------------------------
// The PasswordCredentials type and the receivers it implements, allow
// us to use the grpc.WithPerRPCCredentials() dial option to pass
// credentials to downstream middleware
type PasswordCredentials map[string]string

func NewPassCredentials(m map[string]string) PasswordCredentials {
	return PasswordCredentials(m)
}

func (pc PasswordCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return pc, nil
}

func (PasswordCredentials) RequireTransportSecurity() bool {
	return false
}

// ---------------------------------------------------------------------

type ConnectionOptions struct {
	MaxMessageSize   int                 // Max message size in bytes
	SecurityLevel    int                 // 0 = insecure, 1 = secure, 2 = secure with client cert
	OpenTelemetry    bool                // Enable OpenTelemetry tracing
	OpenTracing      bool                // Enable OpenTelemetry tracing
	Prometheus       bool                // Enable Prometheus metrics
	CertFile         string              // CA cert file if SecurityLevel > 0
	CaCertFile       string              // CA cert file if SecurityLevel > 0
	KeyFile          string              // Client key file if SecurityLevel > 1
	MaxRetries       int                 // Max number of retries for transient errors
	RetryBackoff     time.Duration       // Backoff between retries
	Credentials      PasswordCredentials // Credentials to pass to downstream middleware (optional)
	MaxConnectionAge time.Duration       // The maximum amount of time a connection may exist before it will be closed by sending a GoAway
}

// ---------------------------------------------------------------------

func init() {
	// The secret sauce
	resolver.SetDefaultScheme("dns")
}

func InitGlobalTracer(serviceName string, samplingRate float64) (opentracing.Tracer, io.Closer, error) {
	// TODO ipfs/go-log registers a tracer in its init() function() :-S
	// if opentracing.IsGlobalTracerRegistered() {
	//      so we cannot check this here and must overwrite it
	//return nil, nil, errors.New("global tracer already registered")
	// }

	cfg, err := config.FromEnv()
	if err != nil {
		return nil, nil, fmt.Errorf("cannot parse jaeger environment variables: %v", err.Error())
	}

	cfg.ServiceName = serviceName
	cfg.Sampler.Type = jaeger.SamplerTypeProbabilistic
	cfg.Sampler.Param = samplingRate

	var tracer opentracing.Tracer
	var closer io.Closer
	tracer, closer, err = cfg.NewTracer()
	if err != nil {
		return nil, nil, fmt.Errorf("cannot initialize jaeger tracer: %v", err.Error())
	}

	opentracing.SetGlobalTracer(tracer)

	return tracer, closer, nil
}

func GetGRPCClient(ctx context.Context, address string, connectionOptions *ConnectionOptions) (*grpc.ClientConn, error) {
	if address == "" {
		return nil, errors.New("address is required")
	}

	if connectionOptions.MaxMessageSize == 0 {
		connectionOptions.MaxMessageSize = ONE_GIGABYTE
	}

	opts := []grpc.DialOption{
		// grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin":{}}]}`),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(connectionOptions.MaxMessageSize),
			grpc.MaxCallRecvMsgSize(connectionOptions.MaxMessageSize),
		),
		grpc.WithDisableServiceConfig(),
	}

	if connectionOptions.SecurityLevel == 0 {
		securityLevel, _ := gocore.Config().GetInt("securityLevelGRPC", 0)
		connectionOptions.SecurityLevel = securityLevel
	}

	tlsCredentials, err := loadTLSCredentials(connectionOptions, false)
	if err != nil {
		return nil, err
	}

	opts = append(opts, grpc.WithTransportCredentials(tlsCredentials))

	if connectionOptions.OpenTelemetry {
		opts = append(opts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	}

	unaryClientInterceptors := make([]grpc.UnaryClientInterceptor, 0)
	streamClientInterceptors := make([]grpc.StreamClientInterceptor, 0)

	if connectionOptions.OpenTracing {
		if opentracing.IsGlobalTracerRegistered() {
			tracer := opentracing.GlobalTracer()

			unaryClientInterceptors = append(unaryClientInterceptors, otgrpc.OpenTracingClientInterceptor(tracer))
			streamClientInterceptors = append(streamClientInterceptors, otgrpc.OpenTracingStreamClientInterceptor(tracer))
		} else {
			return nil, errors.New("no global tracer set")
		}
	}

	if connectionOptions.Prometheus {
		prometheusClientMetrics := prometheus.NewClientMetrics(
			prometheus.WithClientStreamSendHistogram(),
			prometheus.WithClientStreamRecvHistogram(),
			prometheus.WithClientHandlingTimeHistogram(),
		)

		unaryClientInterceptors = append(unaryClientInterceptors, prometheusClientMetrics.UnaryClientInterceptor())
		streamClientInterceptors = append(streamClientInterceptors, prometheusClientMetrics.StreamClientInterceptor())

		prometheusRegisterClientOnce.Do(func() {
			prometheus_golang.MustRegister(prometheusClientMetrics)
		})
	}

	if len(unaryClientInterceptors) > 0 {
		opts = append(opts, grpc.WithChainUnaryInterceptor(unaryClientInterceptors...))
	}

	if len(streamClientInterceptors) > 0 {
		opts = append(opts, grpc.WithChainStreamInterceptor(streamClientInterceptors...))
	}

	if connectionOptions.Credentials != nil {
		opts = append(opts, grpc.WithPerRPCCredentials(connectionOptions.Credentials))
	}

	// Retry interceptor...
	if connectionOptions.MaxRetries > 0 {
		if connectionOptions.RetryBackoff == 0 {
			connectionOptions.RetryBackoff = 100 * time.Millisecond
		}

		opts = append(opts, grpc.WithChainUnaryInterceptor(retryInterceptor(connectionOptions.MaxRetries, connectionOptions.RetryBackoff)))
	}

	conn, err := grpc.DialContext(
		ctx,
		address,
		opts...,
	)
	if err != nil {
		return nil, fmt.Errorf("error dialling grpc service at %s: %v", address, err)
	}

	return conn, nil
}

var prometheusRegisterServerOnce sync.Once
var prometheusRegisterClientOnce sync.Once

var prometheusMetrics = prometheus.NewServerMetrics(
	prometheus.WithServerHandlingTimeHistogram(),
)

func getGRPCServer(connectionOptions *ConnectionOptions) (*grpc.Server, error) {
	var opts []grpc.ServerOption

	if connectionOptions.MaxMessageSize == 0 {
		connectionOptions.MaxMessageSize = ONE_GIGABYTE
	}

	opts = append(opts,
		grpc.MaxSendMsgSize(connectionOptions.MaxMessageSize),
		grpc.MaxRecvMsgSize(connectionOptions.MaxMessageSize),
	)

	if connectionOptions.MaxConnectionAge > 0 {
		opts = append(opts, grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionAge: connectionOptions.MaxConnectionAge,
		}))
	}

	if connectionOptions.OpenTelemetry {
		opts = append(opts, grpc.StatsHandler(otelgrpc.NewServerHandler()))
	}

	// Interceptors.  The order may be important here.
	unaryInterceptors := make([]grpc.UnaryServerInterceptor, 0)
	streamInterceptors := make([]grpc.StreamServerInterceptor, 0)

	if connectionOptions.OpenTracing {
		if opentracing.IsGlobalTracerRegistered() {
			tracer := opentracing.GlobalTracer()
			unaryInterceptors = append(unaryInterceptors, otgrpc.OpenTracingServerInterceptor(tracer))
			streamInterceptors = append(streamInterceptors, otgrpc.OpenTracingStreamServerInterceptor(tracer))
		} else {
			return nil, errors.New("no global tracer set")
		}
	}

	if connectionOptions.Prometheus {
		unaryInterceptors = append(unaryInterceptors, prometheusMetrics.UnaryServerInterceptor())
		streamInterceptors = append(streamInterceptors, prometheusMetrics.StreamServerInterceptor())
	}

	if len(unaryInterceptors) > 0 {
		opts = append(opts, grpc.ChainUnaryInterceptor(unaryInterceptors...))
	}

	if len(streamInterceptors) > 0 {
		opts = append(opts, grpc.ChainStreamInterceptor(streamInterceptors...))
	}

	tlsCredentials, err := loadTLSCredentials(connectionOptions, true)
	if err != nil {
		return nil, err
	}

	opts = append(opts, grpc.Creds(tlsCredentials))

	server := grpc.NewServer(opts...)

	if connectionOptions.Prometheus && prometheusMetrics != nil {
		prometheusMetrics.InitializeMetrics(server)
	}

	return server, nil
}

func RegisterPrometheusMetrics() {
	prometheusRegisterServerOnce.Do(func() {
		prometheus_golang.MustRegister(prometheusMetrics)
	})
}

func retryInterceptor(maxRetries int, retryBackoff time.Duration) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		var err error

		for i := 0; i < maxRetries; i++ {
			err = invoker(ctx, method, req, reply, cc, opts...)
			if err == nil {
				return nil
			}

			// Check if we can retry (e.g., codes.Unavailable, codes.DeadlineExceeded)
			if status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded {
				break
			}

			//log.Printf("Retry attempt %d for request: %s\n", i+1, method)
			time.Sleep(retryBackoff)
		}

		return err
	}
}

func loadTLSCredentials(connectionData *ConnectionOptions, isServer bool) (credentials.TransportCredentials, error) {
	switch connectionData.SecurityLevel {
	case 0:
		// No security
		return insecure.NewCredentials(), nil

	case 1:
		// No client cert
		if isServer {
			cert, err := tls.LoadX509KeyPair(connectionData.CertFile, connectionData.KeyFile)
			if err != nil {
				return nil, fmt.Errorf("failed to read key pair: %w", err)
			}
			return credentials.NewTLS(&tls.Config{
				Certificates: []tls.Certificate{cert},
				ClientAuth:   tls.NoClientCert,
				MinVersion:   tls.VersionTLS12,
			}), nil
		} else {
			return credentials.NewTLS(&tls.Config{
				//nolint:gosec // G402: TLS InsecureSkipVerify set true. (gosec)
				InsecureSkipVerify: true,
			}), nil
		}

	case 2:
		// Any client cert
		if isServer {
			cert, err := tls.LoadX509KeyPair(connectionData.CertFile, connectionData.KeyFile)
			if err != nil {
				return nil, fmt.Errorf("failed to read key pair: %w", err)
			}
			return credentials.NewTLS(&tls.Config{
				Certificates:       []tls.Certificate{cert},
				InsecureSkipVerify: true,
				ClientAuth:         tls.RequireAnyClientCert,
			}), nil

		} else {
			// Load the server's CA certificate from disk
			caCert, err := os.ReadFile(connectionData.CaCertFile)
			if err != nil {
				return nil, fmt.Errorf("failed to read ca cert file: %w", err)
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)

			cert, err := tls.LoadX509KeyPair(connectionData.CertFile, connectionData.KeyFile)
			if err != nil {
				return nil, fmt.Errorf("failed to read key pair: %w", err)
			}
			return credentials.NewTLS(&tls.Config{
				Certificates:       []tls.Certificate{cert},
				InsecureSkipVerify: true,
				RootCAs:            caCertPool,
			}), nil

		}

	case 3:
		// Require client cert
		if isServer {
			// Load the server's CA certificate from disk
			caCert, err := os.ReadFile(connectionData.CaCertFile)
			if err != nil {
				return nil, fmt.Errorf("failed to read ca cert file: %w", err)
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)

			cert, err := tls.LoadX509KeyPair(connectionData.CertFile, connectionData.KeyFile)
			if err != nil {
				return nil, fmt.Errorf("failed to read key pair: %w", err)
			}
			return credentials.NewTLS(&tls.Config{
				Certificates:       []tls.Certificate{cert},
				InsecureSkipVerify: true,
				ClientAuth:         tls.RequireAndVerifyClientCert,
				ClientCAs:          caCertPool,
			}), nil

		} else {
			// Load the server's CA certificate from disk
			caCert, err := os.ReadFile(connectionData.CaCertFile)
			if err != nil {
				return nil, fmt.Errorf("failed to read ca cert file: %w", err)
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)

			cert, err := tls.LoadX509KeyPair(connectionData.CertFile, connectionData.KeyFile)
			if err != nil {
				return nil, fmt.Errorf("failed to read key pair: %w", err)
			}
			return credentials.NewTLS(&tls.Config{
				Certificates:       []tls.Certificate{cert},
				InsecureSkipVerify: true,
				RootCAs:            caCertPool,
			}), nil
		}
	}

	return nil, errors.New("securityLevel must be 0, 1, 2 or 3")
}
