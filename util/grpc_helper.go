package util

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/k8sresolver"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	prometheusgolang "github.com/prometheus/client_golang/prometheus"
	"github.com/sercand/kuberesolver/v6"
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

	// Default retry configuration
	defaultRetryBackoff = 100 * time.Millisecond
)

// contextKey is a custom type for context keys to avoid collisions.
// Using a custom type prevents accidental key collisions when storing values in contexts.
type contextKey string

// PasswordCredentials -----------------------------------------------
// The PasswordCredentials type and the receivers it implements, allow
// us to use the grpc.WithPerRPCCredentials() dial option to pass
// credentials to downstream middleware
type PasswordCredentials map[string]string

// NewPassCredentials creates a new PasswordCredentials instance from a map of key-value pairs.
// These credentials are passed as metadata with each gRPC request for downstream authentication.
func NewPassCredentials(m map[string]string) PasswordCredentials {
	return m
}

// GetRequestMetadata retrieves the request metadata for the gRPC call.
func (pc PasswordCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return pc, nil
}

// RequireTransportSecurity indicates whether the credentials require transport security.
func (PasswordCredentials) RequireTransportSecurity() bool {
	return false
}

// ---------------------------------------------------------------------

// ConnectionOptions contains configuration parameters for establishing gRPC client connections,
// including security settings, authentication, retries, and message size limits.
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

// GetGRPCClient creates a new gRPC client connection with the specified options.
// Handles TLS configuration, authentication, retries, metrics, and tracing based on settings.
// Returns a connection that should be closed by the caller when no longer needed.
func GetGRPCClient(_ context.Context, address string, connectionOptions *ConnectionOptions, tSettings *settings.Settings) (*grpc.ClientConn, error) {
	if address == "" {
		return nil, errors.NewInvalidArgumentError("address is required")
	}

	if connectionOptions.MaxMessageSize == 0 {
		connectionOptions.MaxMessageSize = oneGigabyte
	}

	// Get GRPC tuning parameters from settings (respects HighThroughputMode)
	grpcSettings := &tSettings.GRPC

	opts := []grpc.DialOption{
		// grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin":{}}]}`),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(connectionOptions.MaxMessageSize),
			grpc.MaxCallRecvMsgSize(connectionOptions.MaxMessageSize),
		),
		grpc.WithDisableServiceConfig(),

		// HTTP/2 flow control windows - larger values allow more data in-flight
		// Values are determined by grpc_high_throughput_mode setting
		grpc.WithInitialWindowSize(grpcSettings.GetInitialWindowSize()),
		grpc.WithInitialConnWindowSize(grpcSettings.GetInitialConnWindowSize()),

		// Read/write buffer sizes - larger values reduce syscall overhead
		grpc.WithReadBufferSize(grpcSettings.GetReadBufferSize()),
		grpc.WithWriteBufferSize(grpcSettings.GetWriteBufferSize()),
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

	// Add client-side keepalive for faster detection of dead connections
	// This complements application-level heartbeat by detecting transport-level issues
	// Note: Time must be >= server's MinTime enforcement policy
	opts = append(opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                time.Duration(grpcSettings.KeepaliveTime) * time.Second,
		Timeout:             time.Duration(grpcSettings.KeepaliveTimeout) * time.Second,
		PermitWithoutStream: grpcSettings.PermitWithoutStream,
	}))

	// Preallocate interceptor slices with reasonable capacity
	unaryClientInterceptors := make([]grpc.UnaryClientInterceptor, 0, 3)
	streamClientInterceptors := make([]grpc.StreamClientInterceptor, 0, 3)

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
			prometheusgolang.MustRegister(prometheusClientMetrics)
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
			connectionOptions.RetryBackoff = defaultRetryBackoff
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

// getGRPCServer creates a new gRPC server with the specified options.
// Configures message size limits, keepalive parameters, TLS credentials, and interceptors
// for metrics and tracing based on settings. This is a private helper function used internally.
//
// Parameters:
//   - connectionOptions: Configuration for the server connection
//   - opts: Additional gRPC server options to apply
//   - tSettings: Settings for enabling features like tracing and metrics
//
// Returns:
//   - *grpc.Server: Configured gRPC server instance
//   - error: Configuration error if TLS setup or other initialization fails
func getGRPCServer(connectionOptions *ConnectionOptions, opts []grpc.ServerOption, tSettings *settings.Settings) (*grpc.Server, error) {
	if connectionOptions.MaxMessageSize == 0 {
		connectionOptions.MaxMessageSize = oneGigabyte
	}

	// Get GRPC tuning parameters from settings (respects HighThroughputMode)
	grpcSettings := &tSettings.GRPC

	opts = append(opts,
		grpc.MaxSendMsgSize(connectionOptions.MaxMessageSize),
		grpc.MaxRecvMsgSize(connectionOptions.MaxMessageSize),

		// HTTP/2 flow control and buffer settings - values determined by grpc_high_throughput_mode
		grpc.InitialWindowSize(grpcSettings.GetInitialWindowSize()),
		grpc.InitialConnWindowSize(grpcSettings.GetInitialConnWindowSize()),
		grpc.ReadBufferSize(grpcSettings.GetReadBufferSize()),
		grpc.WriteBufferSize(grpcSettings.GetWriteBufferSize()),
		grpc.MaxConcurrentStreams(grpcSettings.GetMaxConcurrentStreams()),
	)

	// Server keepalive parameters for connection management
	serverParams := keepalive.ServerParameters{
		MaxConnectionIdle:     time.Duration(grpcSettings.MaxConnectionIdleSeconds) * time.Second,
		MaxConnectionAgeGrace: 30 * time.Second, // Allow 30 seconds for pending RPCs to complete after MaxConnectionAge
		Time:                  60 * time.Second, // Server pings client every 60 seconds if no activity
		Timeout:               time.Duration(grpcSettings.KeepaliveTimeout) * time.Second,
	}
	if connectionOptions.MaxConnectionAge > 0 {
		serverParams.MaxConnectionAge = connectionOptions.MaxConnectionAge
	}
	opts = append(opts, grpc.KeepaliveParams(serverParams))

	// Enforcement policy allows clients to send pings at the configured rate
	// This prevents ENHANCE_YOUR_CALM / "too_many_pings" errors from client keepalives
	opts = append(opts, grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             time.Duration(grpcSettings.ServerMinPingTime) * time.Second,
		PermitWithoutStream: grpcSettings.PermitWithoutStream,
	}))

	// Interceptors.  The order may be important here.
	unaryInterceptors := make([]grpc.UnaryServerInterceptor, 0, 2)
	streamInterceptors := make([]grpc.StreamServerInterceptor, 0, 2)

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

// RegisterPrometheusMetrics registers the gRPC server metrics with the global Prometheus registry.
// This should be called once during application startup to enable gRPC metrics collection.
func RegisterPrometheusMetrics() {
	prometheusRegisterServerOnce.Do(func() {
		prometheusgolang.MustRegister(prometheusMetrics)
	})
}

// retryInterceptor creates a gRPC unary client interceptor that implements retry logic
// for transient errors. It retries requests that fail with codes.Unavailable or
// codes.DeadlineExceeded up to the specified maximum number of times, with a fixed
// backoff delay between attempts.
//
// Parameters:
//   - maxRetries: Maximum number of retry attempts
//   - retryBackoff: Time to wait between retry attempts
//
// Returns:
//   - grpc.UnaryClientInterceptor: Interceptor function that wraps requests with retry logic
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

// loadTLSCredentials configures TLS transport credentials based on the specified security level.
// Supports four security levels:
//   - Level 0: No security (insecure connection)
//   - Level 1: TLS with no client certificate required
//   - Level 2: TLS with any client certificate accepted
//   - Level 3: TLS with client certificate required and verified
//
// The function behaves differently for client vs server configurations, loading appropriate
// certificates and CA trust stores as needed.
//
// Parameters:
//   - connectionData: Connection options containing security level and certificate paths
//   - isServer: True if configuring server credentials, false for client credentials
//
// Returns:
//   - credentials.TransportCredentials: Configured TLS credentials
//   - error: Configuration error if certificate loading or validation fails
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
//   - grpcResolver: The resolver type to configure
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

// CreateAuthInterceptor creates a gRPC interceptor that handles authentication for protected methods.
// It validates API keys in request metadata and only applies authentication to specified methods.
// Non-protected methods bypass authentication entirely.
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
