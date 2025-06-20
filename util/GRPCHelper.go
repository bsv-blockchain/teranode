package util

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/pkg/k8sresolver"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	prometheus_golang "github.com/prometheus/client_golang/prometheus"
	"github.com/sercand/kuberesolver/v5"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
)

const (
	oneGigabyte = 1024 * 1024 * 1024
	// Define context keys
	authenticatedKey contextKey = "authenticated"

	apiKeyHeader = "x-api-key"
)

// contextKey is a custom type for context keys to avoid collisions
type contextKey string

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
	CertFile         string              // CA cert file if SecurityLevel > 0
	CaCertFile       string              // CA cert file if SecurityLevel > 0
	KeyFile          string              // Client key file if SecurityLevel > 1
	MaxRetries       int                 // Max number of retries for transient errors
	RetryBackoff     time.Duration       // Backoff between retries
	Credentials      PasswordCredentials // Credentials to pass to downstream middleware (optional)
	MaxConnectionAge time.Duration       // The maximum amount of time a connection may exist before it will be closed by sending a GoAway
	APIKey           string              // API key for authentication
}

// ---------------------------------------------------------------------

func init() {
	// The secret sauce
	resolver.SetDefaultScheme("dns")
}

func GetGRPCClient(ctx context.Context, address string, connectionOptions *ConnectionOptions, tSettings *settings.Settings) (*grpc.ClientConn, error) {
	if address == "" {
		return nil, errors.NewInvalidArgumentError("address is required")
	}

	if connectionOptions.MaxMessageSize == 0 {
		connectionOptions.MaxMessageSize = oneGigabyte
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
		securityLevel := tSettings.SecurityLevelGRPC
		connectionOptions.SecurityLevel = securityLevel
	}

	tlsCredentials, err := loadTLSCredentials(connectionOptions, false)
	if err != nil {
		return nil, err
	}

	opts = append(opts, grpc.WithTransportCredentials(tlsCredentials))

	unaryClientInterceptors := make([]grpc.UnaryClientInterceptor, 0)
	streamClientInterceptors := make([]grpc.StreamClientInterceptor, 0)

	if connectionOptions.APIKey != "" {
		unaryClientInterceptors = append(unaryClientInterceptors,
			func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn,
				invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				ctx = metadata.AppendToOutgoingContext(ctx, apiKeyHeader, connectionOptions.APIKey)
				return invoker(ctx, method, req, reply, cc, opts...)
			})

		streamClientInterceptors = append(streamClientInterceptors,
			func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
				method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				ctx = metadata.AppendToOutgoingContext(ctx, apiKeyHeader, connectionOptions.APIKey)
				return streamer(ctx, desc, cc, method, opts...)
			})
	}

	// add tracing, which is configured and enabled in the config
	if tSettings.TracingEnabled {
		opts = append(opts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	}

	prometheusGRPCMetrics := tSettings.UsePrometheusGRPCMetrics
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

	conn, err := grpc.NewClient(address, opts...)
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

func getGRPCServer(connectionOptions *ConnectionOptions, opts []grpc.ServerOption, tSettings *settings.Settings) (*grpc.Server, error) {
	if connectionOptions.MaxMessageSize == 0 {
		connectionOptions.MaxMessageSize = oneGigabyte
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

	if tSettings.TracingEnabled {
		opts = append(opts, grpc.StatsHandler(otelgrpc.NewServerHandler()))
	}

	prometheusGRPCMetrics := tSettings.UsePrometheusGRPCMetrics
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

			// log.Printf("Retry attempt %d for request: %s\n", i+1, method)
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

// InitGRPCResolver configures the gRPC resolver based on the environment.
// It supports different resolver configurations:
//   - k8s: Kubernetes resolver using default scheme
//   - kubernetes: In-cluster Kubernetes resolver
//   - default: Standard gRPC resolver
//
// Parameters:
//   - logger: Logger for resolver configuration messages
func InitGRPCResolver(logger ulogger.Logger, grpcResolver string) {
	switch grpcResolver {
	case "k8s":
		logger.Infof("[VALIDATOR] Using k8s resolver for clients")
		resolver.Register(k8sresolver.NewBuilder(logger))
		resolver.SetDefaultScheme("k8s")
	case "kubernetes":
		logger.Infof("[VALIDATOR] Using kubernetes resolver for clients")
		resolver.Register(k8sresolver.NewBuilder(logger))
		kuberesolver.RegisterInClusterWithSchema("k8s")
	default:
		logger.Infof("[VALIDATOR] Using default resolver for clients")
	}
}

// CreateAuthInterceptor creates a gRPC interceptor that handles authentication for protected methods
func CreateAuthInterceptor(apiKey string, protectedMethods map[string]bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Skip authentication for non-protected methods
		if !protectedMethods[info.FullMethod] {
			return handler(ctx, req)
		}

		// Extract metadata from context
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}

		// Get API key from metadata
		keys := md.Get(apiKeyHeader)
		if len(keys) == 0 {
			return nil, status.Error(codes.Unauthenticated, "missing API key")
		}

		// Validate API key
		if keys[0] != apiKey {
			return nil, status.Error(codes.Unauthenticated, "invalid API key")
		}

		// Add authentication info to context for logging
		newCtx := context.WithValue(ctx, authenticatedKey, true)

		// Proceed with the handler
		return handler(newCtx, req)
	}
}
