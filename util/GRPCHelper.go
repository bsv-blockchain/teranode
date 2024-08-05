package util

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/ordishs/gocore"
	prometheus_golang "github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
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

func GetGRPCClient(ctx context.Context, address string, connectionOptions *ConnectionOptions) (*grpc.ClientConn, error) {
	if address == "" {
		return nil, errors.NewInvalidArgumentError("address is required")
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

	unaryClientInterceptors := make([]grpc.UnaryClientInterceptor, 0)
	streamClientInterceptors := make([]grpc.StreamClientInterceptor, 0)

	// add tracing, which is configured and enabled in the config
	opts, unaryClientInterceptors, streamClientInterceptors = tracing.GetGRPCClientTracerOptions(
		opts,
		unaryClientInterceptors,
		streamClientInterceptors,
	)

	prometheusGRPCMetrics := gocore.Config().GetBool("use_prometheus_grpc_metrics", true)
	if prometheusGRPCMetrics {
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
		return nil, errors.NewServiceError("error dialing grpc service at %s", address, err)
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

	// Interceptors.  The order may be important here.
	unaryInterceptors := make([]grpc.UnaryServerInterceptor, 0)
	streamInterceptors := make([]grpc.StreamServerInterceptor, 0)

	// add tracing, which is configured and enabled in the config
	opts, unaryInterceptors, streamInterceptors = tracing.GetGRPCServerTracerOptions(
		opts,
		unaryInterceptors,
		streamInterceptors,
	)

	prometheusGRPCMetrics := gocore.Config().GetBool("use_prometheus_grpc_metrics", true)
	if prometheusGRPCMetrics {
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

	if prometheusGRPCMetrics && prometheusMetrics != nil {
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
				return nil, errors.NewConfigurationError("failed to read key pair", err)
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
				return nil, errors.NewConfigurationError("failed to read key pair", err)
			}
			return credentials.NewTLS(&tls.Config{
				Certificates: []tls.Certificate{cert},
				//nolint:gosec //  G402: TLS InsecureSkipVerify set true. (gosec)
				InsecureSkipVerify: true,
				ClientAuth:         tls.RequireAnyClientCert,
			}), nil

		} else {
			// Load the server's CA certificate from disk
			caCert, err := os.ReadFile(connectionData.CaCertFile)
			if err != nil {
				return nil, errors.NewConfigurationError("failed to read ca cert file", err)
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)

			cert, err := tls.LoadX509KeyPair(connectionData.CertFile, connectionData.KeyFile)
			if err != nil {
				return nil, errors.NewConfigurationError("failed to read key pair", err)
			}
			return credentials.NewTLS(&tls.Config{
				Certificates: []tls.Certificate{cert},
				//nolint:gosec //  G402: TLS InsecureSkipVerify set true. (gosec)
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
				return nil, errors.NewConfigurationError("failed to read ca cert file", err)
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)

			cert, err := tls.LoadX509KeyPair(connectionData.CertFile, connectionData.KeyFile)
			if err != nil {
				return nil, errors.NewConfigurationError("failed to read key pair", err)
			}
			return credentials.NewTLS(&tls.Config{
				Certificates: []tls.Certificate{cert},
				//nolint:gosec //  G402: TLS InsecureSkipVerify set true. (gosec)
				InsecureSkipVerify: true,
				ClientAuth:         tls.RequireAndVerifyClientCert,
				ClientCAs:          caCertPool,
			}), nil

		} else {
			// Load the server's CA certificate from disk
			caCert, err := os.ReadFile(connectionData.CaCertFile)
			if err != nil {
				return nil, errors.NewConfigurationError("failed to read ca cert file", err)
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)

			cert, err := tls.LoadX509KeyPair(connectionData.CertFile, connectionData.KeyFile)
			if err != nil {
				return nil, errors.NewConfigurationError("failed to read key pair", err)
			}
			return credentials.NewTLS(&tls.Config{
				Certificates: []tls.Certificate{cert},
				//nolint:gosec //  G402: TLS InsecureSkipVerify set true. (gosec)
				InsecureSkipVerify: true,
				RootCAs:            caCertPool,
			}), nil
		}
	}

	return nil, errors.NewConfigurationError("securityLevel must be 0, 1, 2 or 3")
}
