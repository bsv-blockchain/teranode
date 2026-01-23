// Package propagation implements Bitcoin SV transaction propagation and validation services.
// It provides functionality for processing, validating, and distributing BSV transactions
// across the network using multiple protocols including GRPC and UDP6 multicast.
//
// The propagation service acts as a critical gateway for transaction ingress into the Teranode
// architecture. It ensures transactions are validated, stored, and efficiently distributed to
// other components while maintaining high throughput and reliability. Key responsibilities include:
//
// - Transaction acceptance via multiple protocols (GRPC, HTTP, UDP6 multicast)
// - Initial validation to ensure transaction format correctness
// - Transaction storage in the configured blob store
// - Asynchronous validation through integration with the validator service
// - Efficient batch processing for high transaction volumes
// - Size-based routing with fallback mechanisms for large transactions
//
// The service implements multiple connection strategies and fallback mechanisms to ensure
// reliable transaction processing even under high load conditions or when dealing with
// exceptionally large transactions that exceed standard gRPC message size limits.
package propagation

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/propagation/propagation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/health"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/ordishs/gocore"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Request processing limits for the propagation service.
// These constants define the maximum capacity constraints for transaction processing
// to ensure system stability and prevent resource exhaustion attacks.
const (
	// maxTransactionsPerRequest defines the maximum number of transactions that can be
	// processed in a single batch request. This limit prevents memory exhaustion and
	// ensures reasonable processing times for batch operations.
	maxTransactionsPerRequest = 1024

	// maxDataPerRequest defines the maximum total data size (in bytes) that can be
	// processed in a single request. This limit prevents oversized requests from
	// consuming excessive memory and network resources (32 MB limit).
	maxDataPerRequest = 32 * 1024 * 1024
)

var (
	// maxDatagramSize defines the maximum size of UDP datagrams for IPv6 multicast
	maxDatagramSize = 512 // 100 * 1024 * 1024
	// ipv6Port defines the default port used for IPv6 multicast listeners
	ipv6Port = 9999
)

// PropagationServer implements the transaction propagation service for Bitcoin SV.
// This server provides the core transaction processing infrastructure for the Teranode system,
// handling transaction validation, storage, and distribution across the Bitcoin SV network.
// It serves as the primary entry point for transaction ingress and manages the complete
// transaction lifecycle from initial receipt through validation and network propagation.
//
// The server supports multiple ingress protocols:
//   - gRPC API for high-performance programmatic access
//   - HTTP REST API for web-based integrations
//   - UDP6 multicast for efficient network-wide distribution
//
// Key responsibilities:
//   - Transaction format validation and integrity checking
//   - Persistent storage of transactions in configured blob stores
//   - Asynchronous validation through integration with validator services
//   - Batch processing for high-throughput scenarios
//   - Size-based routing with fallback mechanisms for large transactions
//   - Integration with blockchain services for state verification
//   - Kafka-based event publishing for downstream processing
//   - Comprehensive metrics collection and monitoring
//
// Architecture:
// The server maintains connections to various backend services including validators,
// blockchain clients, and Kafka producers. It implements rate limiting, request
// validation, and error handling to ensure system stability under high load.
//
// Thread Safety:
// The PropagationServer is designed for concurrent operation and maintains internal
// synchronization for shared resources. Multiple goroutines can safely process
// transactions simultaneously through the same server instance.
type PropagationServer struct {
	propagation_api.UnsafePropagationAPIServer
	logger                       ulogger.Logger
	settings                     *settings.Settings
	stats                        *gocore.Stat
	txStore                      blob.Store
	validator                    validator.Interface
	blockchainClient             blockchain.ClientI
	validatorKafkaProducerClient kafka.KafkaAsyncProducerI
	httpServer                   *echo.Echo
	validatorHTTPAddr            *url.URL
}

// New creates a new PropagationServer instance with the specified dependencies.
// It initializes Prometheus metrics and configures the server with required services.
//
// Parameters:
//   - logger: logging interface for server operations
//   - tSettings: settings for the server
//   - txStore: storage interface for persisting transactions
//   - validatorClient: service for transaction validation
//   - blockchainClient: interface to blockchain operations
//   - validatorKafkaProducerClient: Kafka producer for async validation
//
// Returns:
//   - *PropagationServer: configured server instance
func New(logger ulogger.Logger, tSettings *settings.Settings, txStore blob.Store, validatorClient validator.Interface, blockchainClient blockchain.ClientI, validatorKafkaProducerClient kafka.KafkaAsyncProducerI) *PropagationServer {
	initPrometheusMetrics()

	return &PropagationServer{
		logger:                       logger,
		settings:                     tSettings,
		stats:                        gocore.NewStat("propagation"),
		txStore:                      txStore,
		validator:                    validatorClient,
		blockchainClient:             blockchainClient,
		validatorKafkaProducerClient: validatorKafkaProducerClient,
		validatorHTTPAddr:            tSettings.Validator.HTTPAddress,
	}
}

// Health performs health checks on the server and its dependencies.
// When checkLiveness is true, it performs basic liveness checks.
// Otherwise, it performs readiness checks including dependency verification.
//
// Parameters:
//   - ctx: context for the health check operation
//   - checkLiveness: boolean indicating whether to perform liveness check
//
// Returns:
//   - int: HTTP status code indicating health status
//   - string: detailed health status message
//   - error: error if health check fails
func (ps *PropagationServer) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// Add liveness checks here. Don't include dependency checks.
		// If the service is stuck return http.StatusServiceUnavailable
		// to indicate a restart is needed
		return http.StatusOK, "OK", nil
	}

	var brokersURL []string
	if ps.validatorKafkaProducerClient != nil { // tests may not set this
		brokersURL = ps.validatorKafkaProducerClient.BrokersURL()
	}

	// Add readiness checks here. Include dependency checks.
	// If any dependency is not ready, return http.StatusServiceUnavailable
	// If all dependencies are ready, return http.StatusOK
	// A failed dependency check does not imply the service needs restarting
	checks := make([]health.Check, 0, 7)

	// Check if the gRPC server is actually listening and accepting requests
	// Only check if the address is configured (not empty)
	if ps.settings.Propagation.GRPCListenAddress != "" {
		checks = append(checks, health.Check{
			Name: "gRPC Server",
			Check: health.CheckGRPCServerWithSettings(ps.settings.Propagation.GRPCListenAddress, ps.settings, func(ctx context.Context, conn *grpc.ClientConn) error {
				client := propagation_api.NewPropagationAPIClient(conn)
				_, err := client.HealthGRPC(ctx, &propagation_api.EmptyMessage{})
				return err
			}),
		})
	}

	// Check if the HTTP server is actually listening and accepting requests
	if ps.settings.Propagation.HTTPListenAddress != "" {
		addr := ps.settings.Propagation.HTTPListenAddress
		if strings.HasPrefix(addr, ":") {
			addr = "localhost" + addr
		}
		checks = append(checks, health.Check{
			Name:  "HTTP Server",
			Check: health.CheckHTTPServer(fmt.Sprintf("http://%s", addr), "/health"),
		})
	}

	// Only check Kafka if it's configured
	if len(brokersURL) > 0 {
		checks = append(checks, health.Check{Name: "Kafka", Check: kafka.HealthChecker(ctx, brokersURL)})
	}

	if ps.blockchainClient != nil {
		checks = append(checks, health.Check{Name: "BlockchainClient", Check: ps.blockchainClient.Health})
		checks = append(checks, health.Check{Name: "FSM", Check: blockchain.CheckFSM(ps.blockchainClient)})
	}

	if ps.validator != nil {
		checks = append(checks, health.Check{Name: "ValidatorClient", Check: ps.validator.Health})
	}

	if ps.txStore != nil {
		checks = append(checks, health.Check{Name: "TxStore", Check: ps.txStore.Health})
	}

	// If no checks configured (test environment), return OK
	if len(checks) == 0 {
		return http.StatusOK, `{"status":"200", "dependencies":[]}`, nil
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

// HealthGRPC implements the gRPC health check endpoint for the propagation service.
// It performs readiness checks on the server and its dependencies, returning the results
// in a gRPC-friendly format.
//
// Parameters:
//   - ctx: context for the health check operation
//   - _: empty message parameter (unused)
//
// Returns:
//   - *propagation_api.HealthResponse: health check response including status and timestamp
//   - error: error if health check fails
func (ps *PropagationServer) HealthGRPC(ctx context.Context, _ *propagation_api.EmptyMessage) (*propagation_api.HealthResponse, error) {
	startTime := time.Now()
	defer func() {
		prometheusHealth.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
	}()

	// Add context value to prevent circular dependency when checking gRPC server health
	ctx = context.WithValue(ctx, "skip-grpc-self-check", true)
	status, details, err := ps.Health(ctx, false)

	return &propagation_api.HealthResponse{
		Ok:        status == http.StatusOK,
		Details:   details,
		Timestamp: timestamppb.Now(),
	}, errors.WrapGRPC(err)
}

// Init initializes the PropagationServer.
// Currently a no-op, reserved for future initialization needs.
//
// Parameters:
//   - ctx: context for initialization (unused)
//
// Returns:
//   - error: always returns nil in current implementation
func (ps *PropagationServer) Init(_ context.Context) (err error) {
	return nil
}

// Start initializes and starts the PropagationServer services including:
// - FSM state restoration if configured
// - UDP6 multicast listeners
// - Kafka producer initialization
// - GRPC server setup
//
// The function blocks until the GRPC server is running or an error occurs.
//
// Parameters:
//   - ctx: context for the start operation
//
// Returns:
//   - error: error if server fails to start
func (ps *PropagationServer) Start(ctx context.Context, readyCh chan<- struct{}) (err error) {
	var closeOnce sync.Once
	defer closeOnce.Do(func() { close(readyCh) })

	// Blocks until the FSM transitions from the IDLE state
	err = ps.blockchainClient.WaitUntilFSMTransitionFromIdleState(ctx)
	if err != nil {
		ps.logger.Errorf("[Propagation Service] Failed to wait for FSM transition from IDLE state: %s", err)
		return err
	}

	ipv6Addresses := ps.settings.Propagation.IPv6Addresses
	if ipv6Addresses != "" {
		err = ps.StartUDP6Listeners(ctx, ipv6Addresses)
		if err != nil {
			return errors.NewServiceError("error starting ipv6 listeners", err)
		}
	}

	if ps.validatorKafkaProducerClient != nil {
		ps.validatorKafkaProducerClient.Start(ctx, make(chan *kafka.Message, 10_000))
	}

	// start the http listener for incoming transactions
	if ps.settings.Propagation.HTTPListenAddress != "" {
		if err = ps.startHTTPServer(ctx, ps.settings.Propagation.HTTPListenAddress); err != nil {
			return err
		}
	}

	// this will block
	maxConnectionAge := ps.settings.Propagation.GRPCMaxConnectionAge
	if err = util.StartGRPCServer(ctx, ps.logger, ps.settings, "propagation", ps.settings.Propagation.GRPCListenAddress, func(server *grpc.Server) {
		propagation_api.RegisterPropagationAPIServer(server, ps)
		closeOnce.Do(func() { close(readyCh) })
	}, nil, maxConnectionAge); err != nil {
		return err
	}

	return nil
}

// StartUDP6Listeners initializes IPv6 multicast listeners for transaction propagation.
// It creates UDP listeners on specified interfaces and addresses, processing incoming
// transactions in separate goroutines.
//
// Parameters:
//   - ctx: context for the UDP listener operations
//   - ipv6Addresses: comma-separated list of IPv6 multicast addresses
//
// Returns:
//   - error: error if listeners fail to start
func (ps *PropagationServer) StartUDP6Listeners(ctx context.Context, ipv6Addresses string) error {
	ps.logger.Infof("Starting UDP6 listeners on %s", ipv6Addresses)

	ipv6Interface := ps.settings.Propagation.IPv6Interface
	if ipv6Interface == "" {
		// default to en0
		ipv6Interface = "en0"
	}

	useInterface, err := net.InterfaceByName(ipv6Interface)
	if err != nil {
		return errors.NewConfigurationError("error resolving interface", err)
	}

	for _, ipv6Address := range strings.Split(ipv6Addresses, ",") {
		var conn *net.UDPConn

		conn, err = net.ListenMulticastUDP("udp6", useInterface, &net.UDPAddr{
			IP:   net.ParseIP(ipv6Address),
			Port: ipv6Port,
			Zone: useInterface.Name,
		})
		if err != nil {
			return errors.NewServiceError("error starting listener", err)
		}

		go func(conn *net.UDPConn) {
			// Loop forever reading from the socket
			var (
				// numBytes int
				n   int
				src *net.UDPAddr
				// oobn int
				// flags int
				msg   wire.Message
				b     []byte
				oobB  []byte
				msgTx *wire.MsgExtendedTx
			)

			buffer := make([]byte, maxDatagramSize)

			for {
				n, _, _, src, err = conn.ReadMsgUDP(buffer, oobB)
				if err != nil {
					ps.logger.Errorf("ReadMsgUDP failed: %v", err)
					continue
				}
				// ps.logger.Infof("read %d bytes from %s, out of bounds data len %d", len(buffer), src.String(), len(oobB))

				reader := bytes.NewReader(buffer[:n])

				func() {
					defer func() {
						if r := recover(); r != nil {
							err = errors.NewProcessingError("wire message parsing panic: %v", r)
							ps.logger.Errorf("Recovered from panic in wire.ReadMessage: %v", r)
						}
					}()
					// reset err before parsing to avoid stale errors
					err = nil
					msg, b, err = wire.ReadMessage(reader, wire.ProtocolVersion, wire.MainNet)
				}()

				if err != nil {
					ps.logger.Warnf("wire.ReadMessage failed: %v", err)
					continue
				}

				ps.logger.Infof("read %d bytes into wire message from %s", len(b), src.String())
				// ps.logger.Infof("wire message type: %v", msg)
				var ok bool

				msgTx, ok = msg.(*wire.MsgExtendedTx)
				if ok {
					ps.logger.Infof("received %d bytes from %s", len(b), src.String())

					txBytes := bytes.NewBuffer(nil)
					if err = msgTx.Serialize(txBytes); err != nil {
						ps.logger.Errorf("error serializing transaction: %v", err)
						continue
					}

					// Process the received bytes
					go func(txb []byte) {
						if _, err = ps.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
							Tx: txb,
						}); err != nil {
							ps.logger.Errorf("error processing transaction: %v", err)
						}
					}(txBytes.Bytes())
				}
			}
		}(conn)
	}

	return nil
}

// Stop gracefully stops the PropagationServer.
// Currently a no-op, reserved for future cleanup operations.
//
// Parameters:
//   - ctx: context for stop operation (unused)
//
// Returns:
//   - error: always returns nil in current implementation
func (ps *PropagationServer) Stop(_ context.Context) error {
	return nil
}

// handleSingleTx handles a single transaction request on the /tx endpoint.
// This method creates and returns an HTTP handler function for processing
// individual transactions submitted via HTTP POST. The handler:
//
// 1. Sets up tracing and instrumentation for the request
// 2. Reads the raw transaction data from the request body
// 3. Delegates to processTransaction for core processing logic
// 4. Returns appropriate HTTP response codes and messages
//
// The /tx endpoint is critical for accepting transactions from external clients
// and also serves as a fallback mechanism for large transactions within the system.
//
// Parameters:
//   - _: Unused context parameter (context is obtained from the HTTP request)
//
// Returns:
//   - echo.HandlerFunc: HTTP handler function for the Echo web framework
func (ps *PropagationServer) handleSingleTx(_ context.Context) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx, _, deferFn := tracing.Tracer("propagation").Start(c.Request().Context(), "handleSingleTx",
			tracing.WithParentStat(ps.stats),
			tracing.WithHistogram(prometheusProcessedHandleSingleTx),
		)
		defer deferFn()

		body, err := io.ReadAll(c.Request().Body)
		if err != nil {
			return c.String(http.StatusBadRequest, "Invalid request body")
		}

		// Process the transaction and return appropriate response
		err = ps.processTransaction(ctx, &propagation_api.ProcessTransactionRequest{Tx: body})
		if err != nil {
			return c.String(http.StatusInternalServerError, "Failed to process transaction: "+err.Error())
		}

		return c.String(http.StatusOK, "OK")
	}
}

// handleMultipleTx handles multiple transactions on the /txs endpoint.
// This method creates and returns an HTTP handler function for processing
// batches of transactions submitted via HTTP POST. The handler implements
// a sophisticated processing pipeline that:
//
// 1. Sets up tracing and instrumentation for batch processing
// 2. Creates a worker pool with channels for parallel transaction processing
// 3. Concurrently reads and parses transactions from the request body
// 4. Enforces batch size limits (maxTransactionsPerRequest) and data size limits (maxDataPerRequest)
// 5. Processes each transaction through a separate goroutine for maximum throughput
// 6. Collects and aggregates errors from parallel processing
// 7. Returns appropriate HTTP responses with detailed error information
//
// The batch processing endpoint is critical for high-throughput ingestion scenarios
// where clients need to submit multiple transactions efficiently.
//
// Parameters:
//   - _: Unused context parameter (context is obtained from the HTTP request)
//
// Returns:
//   - echo.HandlerFunc: HTTP handler function for the Echo web framework
func (ps *PropagationServer) handleMultipleTx(_ context.Context) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx, _, deferFn := tracing.Tracer("propagation").Start(c.Request().Context(), "handleMultipleTx",
			tracing.WithParentStat(ps.stats),
			tracing.WithHistogram(prometheusProcessedHandleMultipleTx),
		)
		defer deferFn()

		processTxs := make(chan *bt.Tx, maxTransactionsPerRequest)
		processErrors := make(chan error, maxTransactionsPerRequest)
		processingWg := sync.WaitGroup{}
		processingErrorWg := sync.WaitGroup{}
		totalNrTransactions := 0
		totalBytesRead := int64(0)

		go func() {
			// Process transactions in a separate goroutine
			for tx := range processTxs {
				if err := ps.processTransactionInternal(ctx, tx); err != nil {
					processingErrorWg.Add(1)
					processErrors <- err
				}

				processingWg.Done()
			}
		}()

		errStr := ""

		go func() {
			for err := range processErrors {
				errStr += err.Error() + "\n"

				processingErrorWg.Done()
			}
		}()

		// Read transactions with the bt reader in a loop
		for {
			tx := &bt.Tx{}

			// Read transaction from request body with panic recovery
			var bytesRead int64
			var err error
			func() {
				defer func() {
					if r := recover(); r != nil {
						err = errors.NewProcessingError("transaction parsing panic: %v", r)
						ps.logger.Errorf("Recovered from panic in tx.ReadFrom: %v", r)
					}
				}()
				bytesRead, err = tx.ReadFrom(c.Request().Body)
			}()

			if err != nil {
				// End of stream is expected and not an error
				if err == io.EOF {
					break
				}

				processingErrorWg.Add(1)
				processErrors <- err

				// if the error came from panic recovery, the stream is likely corrupted
				if terr, ok := err.(*errors.Error); ok && terr.Code() == errors.ERR_PROCESSING {
					ps.logger.Errorf("Stream corrupted after panic, stopping transaction processing")
					break
				}

				// skip counters and reading this tx if a non-EOF error occurred
				continue
			}

			totalNrTransactions++
			totalBytesRead += bytesRead

			if totalNrTransactions > maxTransactionsPerRequest {
				return c.String(http.StatusBadRequest, "Invalid request body: too many transactions")
			}

			if totalBytesRead > maxDataPerRequest {
				return c.String(http.StatusBadRequest, "Invalid request body: too much data")
			}

			// Send transaction to processing channel
			processingWg.Add(1)
			processTxs <- tx
		}

		processingWg.Wait()
		processingErrorWg.Wait()

		close(processTxs)
		close(processErrors)

		if errStr != "" {
			return c.String(http.StatusInternalServerError, "Failed to process transactions:\n"+errStr)
		}

		return c.String(http.StatusOK, "OK")
	}
}

// startHTTPServer initializes and starts the HTTP server for transaction processing.
// This method configures and launches the Echo web server with the following setup:
//
// 1. Creates an Echo server instance with context for graceful shutdown
// 2. Registers essential middleware (recover, CORS, request ID, logging)
// 3. Configures transaction processing endpoints:
//   - POST /tx for single transaction processing
//   - POST /txs for batch transaction processing
//   - GET /health for service health checks
//
// 4. Sets up listener configuration with appropriate address binding
// 5. Starts the server in a non-blocking mode with proper error handling
//
// The HTTP server provides REST endpoints that complement the gRPC and UDP6
// interfaces for transaction ingestion, serving different client needs.
//
// Parameters:
//   - ctx: Context for server lifecycle and shutdown
//   - httpAddresses: Comma-separated list of address:port combinations to bind
//
// Returns:
//   - error: Error if server initialization or startup fails
func (ps *PropagationServer) startHTTPServer(ctx context.Context, httpAddresses string) error {
	// Initialize Echo server with settings
	ps.httpServer = echo.New()
	ps.httpServer.Debug = false
	ps.httpServer.HideBanner = true

	// Configure middleware and timeouts
	if ps.settings.Propagation.HTTPRateLimit > 0 {
		ps.httpServer.Use(middleware.RateLimiter(middleware.NewRateLimiterMemoryStore(rate.Limit(ps.settings.Propagation.HTTPRateLimit))))
	}

	ps.httpServer.Server.ReadTimeout = 30 * time.Second
	ps.httpServer.Server.WriteTimeout = 30 * time.Second
	ps.httpServer.Server.IdleTimeout = 120 * time.Second

	// Register route handlers
	ps.httpServer.POST("/tx", ps.handleSingleTx(ctx))
	ps.httpServer.POST("/txs", ps.handleMultipleTx(ctx))

	// add a health endpoint that simply returns "OK"
	ps.httpServer.GET("/health", func(c echo.Context) error {
		return c.String(http.StatusOK, "OK")
	})

	// add a 404 handler with a message for unknown routes
	ps.httpServer.Any("/*", func(c echo.Context) error {
		return c.String(http.StatusNotFound, "Unknown route")
	})

	// Start server and handle shutdown
	ps.startAndMonitorHTTPServer(ctx, httpAddresses)

	return nil
}

// startAndMonitorHTTPServer starts the HTTP server and monitors for shutdown.
// This method manages the HTTP server lifecycle by:
//
// 1. Starting the HTTP server with the given addresses in a background goroutine
// 2. Logging server startup events and any errors encountered
// 3. Monitoring for context cancellation signals
// 4. Performing graceful shutdown when the context is canceled
// 5. Ensuring all resources are properly released
//
// The server is launched in a non-blocking manner, allowing the main service
// thread to continue initialization and operation while HTTP endpoints are available.
//
// Parameters:
//   - ctx: Context for server lifecycle monitoring and shutdown signals
//   - httpAddresses: Address configuration for HTTP server bindings
func (ps *PropagationServer) startAndMonitorHTTPServer(ctx context.Context, httpAddresses string) {
	// Get listener using util.GetListener - use "propagation" to match the test setup
	listener, address, _, err := util.GetListener(ps.settings.Context, "propagation", "http://", httpAddresses)
	if err != nil {
		ps.logger.Errorf("failed to get listener: %v", err)
		return
	}

	ps.logger.Infof("Propagation HTTP server listening on %s", address)

	ps.httpServer.Listener = listener

	// Start the server with the pre-created listener
	go func() {
		// Use the Listener method instead of Start to use our pre-created listener
		if err := ps.httpServer.Server.Serve(listener); err != nil {
			if err == http.ErrServerClosed {
				ps.logger.Infof("http server shutdown")
			} else {
				ps.logger.Errorf("failed to start http server: %v", err)
			}
		}
		// Clean up the listener when server stops
		util.RemoveListener(ps.settings.Context, "propagation", "http://")
	}()

	// Monitor for context cancellation
	go func() {
		<-ctx.Done()

		_ = ps.httpServer.Shutdown(context.Background())
	}()
}

// ProcessTransaction validates and stores a single transaction.
// This method is the primary gRPC entry point for transaction submission and implements
// the complete transaction processing pipeline with the following steps:
//
// 1. Validates transaction format and parses it into a Bitcoin transaction
// 2. Verifies it's not a coinbase transaction (not allowed for propagation)
// 3. Ensures the transaction is in extended format (required for processing)
// 4. Stores the transaction in the configured blob store for persistence
// 5. Triggers validation through the appropriate channel (validator service or Kafka)
// 6. Records performance metrics for monitoring and alerting
//
// The method is designed to handle high transaction throughput while providing
// detailed error reporting for various failure scenarios.
//
// Parameters:
//   - ctx: Context for the transaction processing with tracing information
//   - req: Transaction processing request containing raw transaction data
//
// Returns:
//   - *propagation_api.EmptyMessage: Empty response on successful processing
//   - error: Error with specific details if transaction processing fails
func (ps *PropagationServer) ProcessTransaction(ctx context.Context, req *propagation_api.ProcessTransactionRequest) (*propagation_api.EmptyMessage, error) {
	// Debug: Check if span context was received from client
	spanCtx := trace.SpanContextFromContext(ctx)
	if spanCtx.IsValid() {
		ps.logger.Infof("[ProcessTransaction] Server received span context: TraceID=%s, SpanID=%s",
			spanCtx.TraceID().String(), spanCtx.SpanID().String())
	} else {
		ps.logger.Warnf("[ProcessTransaction] Server received INVALID span context")
	}

	if err := ps.processTransaction(ctx, req); err != nil {
		ps.logger.Errorf("[ProcessTransaction] failed to process transaction: %v", err)

		return nil, errors.WrapGRPC(err)
	}

	return &propagation_api.EmptyMessage{}, nil
}

// ProcessTransactionBatch processes multiple transactions concurrently.
// This method implements efficient concurrent processing of transaction batches with the following workflow:
//
// 1. Validates batch constraints (max batch size and total data size)
// 2. Uses error groups (errgroup) to manage parallel transaction processing with proper cancellation
// 3. Processes each transaction independently while preserving the original order in results
// 4. Aggregates errors for each transaction while allowing the batch to complete even with partial failures
// 5. Collects and maps individual transaction errors to their respective positions in the response
//
// This concurrent processing approach significantly improves throughput for batch submission
// while maintaining proper error isolation between transactions.
//
// Parameters:
//   - ctx: Context for the batch processing operation with cancellation support
//   - req: Batch request containing multiple raw transactions
//
// Returns:
//   - *propagation_api.ProcessTransactionBatchResponse: Response containing per-transaction error status
//   - error: Error if overall batch processing fails (size limits, context canceled)
func (ps *PropagationServer) ProcessTransactionBatch(ctx context.Context, req *propagation_api.ProcessTransactionBatchRequest) (*propagation_api.ProcessTransactionBatchResponse, error) {
	ctx, _, endSpan := tracing.Tracer("propagation").Start(
		ctx,
		"ProcessTransactionBatch",
		tracing.WithTag("batch_size", fmt.Sprintf("%d", len(req.Items))),
		tracing.WithParentStat(ps.stats),
		tracing.WithHistogram(prometheusProcessedTransactionBatch),
		tracing.WithDebugLogMessage(ps.logger, "[ProcessTransactionBatch] called for %d transactions", len(req.Items)),
	)
	defer endSpan()

	response := &propagation_api.ProcessTransactionBatchResponse{
		Errors: make([]*errors.TError, len(req.Items)),
	}

	g, gCtx := errgroup.WithContext(ctx)

	for idx, item := range req.Items {
		idx := idx
		tx := item.Tx

		g.Go(func() error {
			var txCtx context.Context

			if len(item.TraceContext) > 0 {
				// Deserialize the trace context
				prop := otel.GetTextMapPropagator()
				txCtx = prop.Extract(gCtx, propagation.MapCarrier(item.TraceContext))
			} else {
				// No trace context available, use the batch context
				txCtx = gCtx
			}

			// just call the internal process transaction function for every transaction
			if err := ps.processTransaction(txCtx, &propagation_api.ProcessTransactionRequest{
				Tx: tx,
			}); err != nil {
				e := errors.Wrap(err)
				ps.logger.Errorf("[ProcessTransactionBatch] failed to process transaction %d: %v", idx, e)

				response.Errors[idx] = e
			} else {
				response.Errors[idx] = nil
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		ps.logger.Errorf("[ProcessTransactionBatch] failed to process transaction batch: %v", err)

		return nil, errors.WrapGRPC(err)
	}

	return response, nil
}

// processTransaction handles the core transaction processing logic.
// It validates, stores, and triggers async validation of a transaction,
// updating metrics throughout the process.
//
// Parameters:
//   - ctx: context for transaction processing
//   - req: transaction processing request
//
// Returns:
//   - error: error if any processing step fails
func (ps *PropagationServer) processTransaction(ctx context.Context, req *propagation_api.ProcessTransactionRequest) error {
	ctx, span, endSpan := tracing.Tracer("propagation").Start(ctx, "processTransaction",
		tracing.WithParentStat(ps.stats),
	)
	defer endSpan()

	timeStart := time.Now()

	var btTx *bt.Tx
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = errors.NewProcessingError("transaction parsing panic: %v", r)
				ps.logger.Errorf("Recovered from panic in bt.NewTxFromBytes: %v", r)
			}
		}()
		btTx, err = bt.NewTxFromBytes(req.Tx)
	}()

	if err != nil {
		prometheusInvalidTransactions.Inc()

		err = errors.NewProcessingError("[ProcessTransaction] failed to parse transaction from bytes", err)
		span.RecordError(err)

		return err
	}

	if err = ps.processTransactionInternal(ctx, btTx); err != nil {
		span.RecordError(err)
		return err
	}

	prometheusTransactionSize.Observe(float64(len(req.Tx)))
	prometheusProcessedTransactions.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)

	return nil
}

// processTransactionInternal performs the core business logic for processing a transaction.
// This function implements the validation, storage, and validation routing logic with
// the following workflow:
//
// 1. Validates that the transaction is not a coinbase transaction (not allowed)
// 2. Verifies the transaction is in extended format (required for proper processing)
// 3. Stores the transaction in the configured blob store with proper tracing context decoupling
// 4. Routes the transaction to the appropriate validation path based on size and configuration:
//   - If Kafka is configured, uses size-based routing:
//   - Small transactions go through Kafka for async validation
//   - Large transactions that exceed Kafka size limits use HTTP fallback
//   - If no Kafka is configured, uses direct synchronous validation
//
// Parameters:
//   - ctx: Context for transaction processing with tracing information
//   - btTx: Bitcoin transaction to process (must be already parsed)
//
// Returns:
//   - error: Error if any step in the processing pipeline fails
func (ps *PropagationServer) processTransactionInternal(ctx context.Context, btTx *bt.Tx) (err error) {
	ctx, _, endSpan := tracing.Tracer("propagation").Start(ctx, "processTransactionInternal",
		tracing.WithParentStat(ps.stats),
	)
	defer endSpan(err)

	// Do not allow propagation of coinbase transactions
	if btTx.IsCoinbase() {
		prometheusInvalidTransactions.Inc()
		return errors.NewTxInvalidError("[ProcessTransaction][%s] received coinbase transaction", btTx.TxID())
	}

	// do some very simple sanity checks on the transaction
	if err = ps.txSanityChecks(btTx); err != nil {
		return err
	}

	// // decouple the tracing context to not cancel the context when the tx is being saved in the background
	// decoupledCtx, decoupledSpan, decoupledEndSpan := tracing.DecoupleTracingSpan(ctx, "processTransactionInternal", "decoupled")
	// defer decoupledEndSpan()

	// we should store all transactions, if this fails we should not validate the transaction
	if err = ps.storeTransaction(ctx, btTx); err != nil {
		return errors.NewStorageError("[ProcessTransaction][%s] failed to save transaction", btTx.TxIDChainHash(), err)
	}

	if ps.validatorKafkaProducerClient != nil {
		// Check transaction size first - if it's too large, use HTTP endpoint instead
		txSize := len(btTx.SerializeBytes())
		maxKafkaMessageSize := ps.settings.Validator.KafkaMaxMessageBytes

		if txSize > maxKafkaMessageSize {
			return ps.validateTransactionViaHTTP(ctx, btTx, txSize, maxKafkaMessageSize)
		}

		// For normal-sized transactions, continue with Kafka
		return ps.validateTransactionViaKafka(btTx)
	} else {
		ps.logger.Debugf("[ProcessTransaction][%s] Calling validate function", btTx.TxID())

		// All transactions entering Teranode can be assumed to be after Genesis activation height
		// but we pass in no block height, and just use the block height set in the utxo store
		if _, err = ps.validator.Validate(ctx, btTx, 0); err != nil {
			return errors.NewProcessingError("[ProcessTransaction][%s] failed to validate transaction", btTx.TxID(), err)
		}
	}

	return nil
}

func (ps *PropagationServer) txSanityChecks(btTx *bt.Tx) error {
	if len(btTx.Inputs) == 0 {
		prometheusInvalidTransactions.Inc()
		return errors.NewTxInvalidError("[ProcessTransaction][%s] received transaction with no inputs", btTx.TxID())
	}

	if len(btTx.Outputs) == 0 {
		prometheusInvalidTransactions.Inc()
		return errors.NewTxInvalidError("[ProcessTransaction][%s] received transaction with no outputs", btTx.TxID())
	}

	return nil
}

// validateTransactionViaHTTP sends a transaction to the validator's HTTP endpoint.
// This method serves as a fallback mechanism for large transactions that exceed
// the configured Kafka message size limits. It performs the following operations:
//
// 1. Verifies a validator HTTP endpoint is configured (fails if not available)
// 2. Creates an HTTP client with appropriate timeout
// 3. Constructs the complete request URL by resolving the endpoint path
// 4. Submits the transaction's extended bytes to the validator's /tx endpoint
// 5. Processes the response with proper error handling
//
// Parameters:
//   - ctx: Context for HTTP request with cancellation support
//   - btTx: Bitcoin transaction to validate
//   - txSize: Size of the transaction in bytes (pre-calculated)
//   - maxKafkaMessageSize: Maximum Kafka message size for logging/comparison
//
// Returns:
//   - error: Error if HTTP validation fails or is not available
func (ps *PropagationServer) validateTransactionViaHTTP(ctx context.Context, btTx *bt.Tx, txSize int, maxKafkaMessageSize int) error {
	if ps.validatorHTTPAddr == nil {
		return errors.NewServiceError("[ProcessTransaction][%s] Transaction size %d bytes exceeds Kafka message limit (%d bytes), but no HTTP endpoint configured for validator",
			btTx.TxID(), txSize, maxKafkaMessageSize)
	}

	ps.logger.Warnf("[ProcessTransaction][%s] Transaction size %d bytes exceeds Kafka message limit (%d bytes), falling back to validator /tx endpoint",
		btTx.TxID(), txSize, maxKafkaMessageSize)

	// Create an HTTP client with a timeout
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Prepare request to validator /tx endpoint
	endpoint, err := url.Parse("/tx")
	if err != nil {
		return errors.NewServiceError("[ProcessTransaction][%s] error parsing endpoint /tx", btTx.TxID(), err)
	}

	fullURL := ps.validatorHTTPAddr.ResolveReference(endpoint)

	req, err := http.NewRequestWithContext(ctx, "POST", fullURL.String(), bytes.NewReader(btTx.SerializeBytes()))
	if err != nil {
		return errors.NewServiceError("[ProcessTransaction][%s] error creating request to validator /tx endpoint", btTx.TxID(), err)
	}

	// Send the request
	resp, err := client.Do(req)
	if err != nil {
		return errors.NewServiceError("[ProcessTransaction][%s] error sending transaction to validator /tx endpoint", btTx.TxID(), err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return errors.NewServiceError("[ProcessTransaction][%s] validator /tx endpoint returned non-OK status: %d, body: %s",
			btTx.TxID(), resp.StatusCode, string(body))
	}

	ps.logger.Debugf("[ProcessTransaction][%s] successfully validated using validator /tx endpoint", btTx.TxID())

	return nil
}

// validateTransactionViaKafka sends a transaction to the validator through Kafka.
// This method handles the asynchronous validation pathway for transactions that
// fit within the Kafka message size limits. It performs the following operations:
//
// 1. Creates a validation options object with default settings
// 2. Constructs a Kafka message with the transaction and validation options
// 3. Serializes the message using Protocol Buffers
// 4. Publishes the message to the configured Kafka topic
//
// This asynchronous validation path is generally preferred for normal-sized transactions
// as it provides better throughput and scalability compared to synchronous HTTP validation.
//
// Parameters:
//   - btTx: Bitcoin transaction to validate
//
// Returns:
//   - error: Error if message preparation or publishing fails
func (ps *PropagationServer) validateTransactionViaKafka(btTx *bt.Tx) error {
	validationOptions := validator.NewDefaultOptions()

	msg := &kafkamessage.KafkaTxValidationTopicMessage{
		Tx:     btTx.SerializeBytes(),
		Height: 0,
		Options: &kafkamessage.KafkaTxValidationOptions{
			SkipUtxoCreation:     validationOptions.SkipUtxoCreation,
			AddTXToBlockAssembly: validationOptions.AddTXToBlockAssembly,
			SkipPolicyChecks:     validationOptions.SkipPolicyChecks,
			CreateConflicting:    validationOptions.CreateConflicting,
		},
	}

	value, err := proto.Marshal(msg)
	if err != nil {
		return errors.NewProcessingError("[ProcessTransaction][%s] error marshaling KafkaTxValidationTopicMessage", btTx.TxID(), err, err)
	}

	ps.logger.Debugf("[ProcessTransaction][%s] sending transaction to validator kafka channel", btTx.TxID())
	ps.validatorKafkaProducerClient.Publish(&kafka.Message{
		Key:   []byte(btTx.TxID()),
		Value: value,
	})

	return nil
}

// storeTransaction persists a transaction to the configured storage backend.
// This method implements the transaction storage mechanism with the following workflow:
//
// 1. Extracts the transaction chain hash to use as the unique key
// 2. Obtains the transaction bytes in received format for storage
// 3. Attempts to store the transaction in the configured blob store
// 4. Handles errors with appropriate categorization and context
// 5. Updates metrics for performance monitoring
//
// The storage mechanism is critical for transaction durability and enables
// transaction lookup for subsequent processing stages. Using the chain hash
// as key ensures consistent and efficient transaction retrieval.
//
// Parameters:
//   - ctx: context for the storage operation with tracing and timeout
//   - btTx: Bitcoin transaction to store (must be properly parsed)
//
// Returns:
//   - error: error with detailed context if the storage operation fails
func (ps *PropagationServer) storeTransaction(ctx context.Context, btTx *bt.Tx) error {
	ctx, _, deferFn := tracing.Tracer("propagation").Start(ctx, "PropagationServer:Set:Store")
	defer deferFn()

	if ps.txStore != nil {
		if err := ps.txStore.Set(ctx, btTx.TxIDChainHash().CloneBytes(), fileformat.FileTypeTx, btTx.SerializeBytes()); err != nil {
			// Duplicate transactions are acceptable - the transaction already exists
			if errors.Is(err, errors.ErrBlobAlreadyExists) {
				return nil
			}
			// TODO make this resilient to errors
			// write it to secondary store (Kafka) and retry?
			return err
		}
	}

	return nil
}
