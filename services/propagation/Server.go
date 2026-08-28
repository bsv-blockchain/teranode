// Package propagation implements BSV Blockchain transaction propagation and validation services.
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
	"github.com/bsv-blockchain/teranode/internal/banlist"
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
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// PropagationServer implements the transaction propagation service for BSV Blockchain.
// This server provides the core transaction processing infrastructure for the Teranode system,
// handling transaction validation, storage, and distribution across the BSV Blockchain network.
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
// udpWorkerPoolSize limits concurrent goroutines for UDP transaction processing
// to prevent resource exhaustion from high-volume UDP traffic.
const udpWorkerPoolSize = 100

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
	banList                      banlist.Interface
	udpWorkerPool                chan struct{} // Semaphore for limiting UDP processing goroutines
	batchWorkerPool              chan struct{} // Server-wide semaphore limiting concurrent tx-processing goroutines across all ProcessTransactionBatch calls
	batchHandlerPool             chan struct{} // Non-blocking admission control for in-flight batch/tx handlers; nil when disabled
	udpConns                     []*net.UDPConn
	udpConnsMu                   sync.Mutex
	// udpWg tracks the UDP reader goroutines and the per-transaction worker
	// goroutines they spawn (which call ProcessTransaction). Stop() waits on it
	// after closing the conns so no in-flight ProcessTransaction can still publish
	// to the validator Kafka producer after that producer's channel is closed.
	udpWg sync.WaitGroup
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
func New(logger ulogger.Logger, tSettings *settings.Settings, txStore blob.Store, validatorClient validator.Interface, blockchainClient blockchain.ClientI, validatorKafkaProducerClient kafka.KafkaAsyncProducerI, banList banlist.Interface) *PropagationServer {
	initPrometheusMetrics()

	var batchPool chan struct{}
	if limit := tSettings.Propagation.BatchConcurrencyLimit; limit > 0 {
		batchPool = make(chan struct{}, limit)
	}

	var batchHandlerPool chan struct{}
	if limit := tSettings.Propagation.BatchHandlerLimit; limit > 0 {
		batchHandlerPool = make(chan struct{}, limit)
	}

	return &PropagationServer{
		logger:                       logger,
		settings:                     tSettings,
		stats:                        gocore.NewStat("propagation"),
		txStore:                      txStore,
		validator:                    validatorClient,
		blockchainClient:             blockchainClient,
		validatorKafkaProducerClient: validatorKafkaProducerClient,
		validatorHTTPAddr:            tSettings.Validator.HTTPAddress,
		banList:                      banList,
		udpWorkerPool:                make(chan struct{}, udpWorkerPoolSize),
		batchWorkerPool:              batchPool,
		batchHandlerPool:             batchHandlerPool,
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
		if errors.IsContextError(err) {
			ps.logger.Infof("[Propagation Service] Shutting down during FSM wait")
			return err
		}
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

	// Build auth options with ban list interceptor if available
	var authOptions *util.AuthOptions
	if ps.banList != nil {
		authOptions = &util.AuthOptions{
			ExtraUnaryInterceptors: []grpc.UnaryServerInterceptor{
				banlist.CreateGRPCUnaryInterceptor(ps.banList),
			},
		}
	}

	// this will block
	maxConnectionAge := ps.settings.Propagation.GRPCMaxConnectionAge
	if err = util.StartGRPCServer(ctx, ps.logger, ps.settings, "propagation", ps.settings.Propagation.GRPCListenAddress, func(server *grpc.Server) {
		propagation_api.RegisterPropagationAPIServer(server, ps)
		closeOnce.Do(func() { close(readyCh) })
	}, authOptions, maxConnectionAge); err != nil {
		return err
	}

	return nil
}

// parseAllowedSources parses a list of IP addresses and CIDR ranges into net.IPNet structures.
func parseAllowedSources(sources []string) ([]*net.IPNet, error) {
	nets := make([]*net.IPNet, 0, len(sources))
	for _, src := range sources {
		src = strings.TrimSpace(src)
		if src == "" {
			continue
		}

		// Try parsing as CIDR first
		_, ipNet, err := net.ParseCIDR(src)
		if err == nil {
			nets = append(nets, ipNet)
			continue
		}

		// If not CIDR, treat as single IP
		ip := net.ParseIP(src)
		if ip == nil {
			return nil, errors.NewConfigurationError("invalid IP or CIDR in allowed sources: %s", src)
		}

		// Normalize IPv4-mapped IPv6 addresses to IPv4 for consistent matching
		// in dual-stack environments
		if ip4 := ip.To4(); ip4 != nil {
			ip = ip4
			nets = append(nets, &net.IPNet{IP: ip, Mask: net.CIDRMask(32, 32)})
		} else {
			nets = append(nets, &net.IPNet{IP: ip, Mask: net.CIDRMask(128, 128)})
		}
	}
	return nets, nil
}

// isIPAllowed checks if an IP address is allowed based on the allowlist.
// Returns true if the allowlist is empty (allow all) or if the IP matches any entry.
func isIPAllowed(ip net.IP, allowedNets []*net.IPNet) bool {
	// Empty allowlist means allow all
	if len(allowedNets) == 0 {
		return true
	}
	for _, ipNet := range allowedNets {
		if ipNet.Contains(ip) {
			return true
		}
	}
	return false
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

	// Parse allowed source IPs/CIDRs
	allowedSources := ps.settings.Propagation.IPv6AllowedSources
	allowedNets, err := parseAllowedSources(allowedSources)
	if err != nil {
		return err
	}
	if len(allowedNets) > 0 {
		ps.logger.Infof("UDP6 source allowlist configured with %d entries", len(allowedNets))
	} else {
		ps.logger.Infof("UDP6 source allowlist not configured, accepting from all sources")
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

		// Track connection for cleanup on shutdown
		ps.udpConnsMu.Lock()
		ps.udpConns = append(ps.udpConns, conn)
		ps.udpConnsMu.Unlock()

		ps.udpWg.Add(1)

		go func(conn *net.UDPConn, allowedNets []*net.IPNet) {
			defer ps.udpWg.Done()

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
					if errors.Is(err, net.ErrClosed) {
						ps.logger.Infof("UDP listener shutting down")
						return
					}
					ps.logger.Errorf("ReadMsgUDP failed: %v", err)
					continue
				}

				// Check if source IP is in allowlist
				if !isIPAllowed(src.IP, allowedNets) {
					ps.logger.Warnf("Dropping UDP packet from unauthorized source: %s", src.IP.String())
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

				ps.logger.Debugf("read %d bytes into wire message from %s", len(b), src.String())
				// ps.logger.Infof("wire message type: %v", msg)
				var ok bool

				msgTx, ok = msg.(*wire.MsgExtendedTx)
				if ok {
					ps.logger.Debugf("received %d bytes from %s", len(b), src.String())

					txBytes := bytes.NewBuffer(nil)
					if err = msgTx.Serialize(txBytes); err != nil {
						ps.logger.Errorf("error serializing transaction: %v", err)
						continue
					}

					// Process the received bytes using worker pool to limit concurrency.
					// Add to udpWg before spawning (while this reader still holds its
					// own udpWg slot, so the counter is never observed at zero here)
					// so Stop() can join in-flight ProcessTransaction calls.
					select {
					case ps.udpWorkerPool <- struct{}{}:
						ps.udpWg.Add(1)

						go func(txb []byte) {
							defer ps.udpWg.Done()
							defer func() { <-ps.udpWorkerPool }()
							if _, err := ps.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
								Tx: txb,
							}); err != nil {
								ps.logger.Errorf("error processing transaction: %v", err)
							}
						}(txBytes.Bytes())
					default:
						ps.logger.Warnf("UDP worker pool full, dropping transaction from %s", src.String())
					}
				}
			}
		}(conn, allowedNets)
	}

	return nil
}

// Stop gracefully stops the PropagationServer, closing UDP listeners.
//
// Parameters:
//   - ctx: bounds the shutdown. The wait for UDP/tx workers and the validator
//     producer stop are each raced against ctx so a wedged worker or broker
//     cannot block Stop past the service-manager's per-service budget.
//
// Returns:
//   - error: ctx.Err() — nil on a clean stop, the ctx error if the budget was hit
//     (cleanup is still attempted on a best-effort, bounded basis before returning).
func (ps *PropagationServer) Stop(ctx context.Context) error {
	// Ordering: the validator Kafka producer is stopped LAST, after every
	// transaction-ingress path that can call ProcessTransaction (which publishes to
	// the producer) is quiesced. A late publish during the producer stop drops
	// safely — Publish guards under channelMu and util.SafeSend recovers — so
	// stopping last is not about avoiding a panic; it simply maximises the final
	// flush by letting the workers finish first. So: close UDP conns + join their
	// workers, drain the HTTP server, THEN stop the producer. (gRPC ingress is
	// already drained before Stop() runs: StartGRPCServer GracefulStops on
	// ctx-cancel and Start() — hence the service-manager's Wait() — returns only
	// after that.)

	// 1. Close UDP listeners so the reader goroutines exit, then wait for them and
	//    any in-flight per-transaction worker goroutines to finish — bounded by ctx
	//    so a wedged ProcessTransaction (e.g. a publish blocked on a dead broker)
	//    cannot stall the whole shutdown.
	ps.udpConnsMu.Lock()
	for _, conn := range ps.udpConns {
		if err := conn.Close(); err != nil {
			ps.logger.Errorf("Error closing UDP connection: %v", err)
		}
	}
	ps.udpConns = nil
	ps.udpConnsMu.Unlock()

	udpDone := make(chan struct{})
	go func() {
		ps.udpWg.Wait()
		close(udpDone)
	}()

	select {
	case <-udpDone:
	case <-ctx.Done():
		ps.logger.Errorf("[Propagation] timed out waiting for UDP workers, proceeding with shutdown: %v", ctx.Err())
	}

	// 2. Drain the HTTP server so in-flight /tx and /txs handlers finish. Already
	//    ctx-bounded; safe regardless of worker state.
	if ps.httpServer != nil {
		if err := ps.httpServer.Shutdown(ctx); err != nil {
			ps.logger.Errorf("[Propagation] error shutting down http server: %v", err)
		}
	}

	// 3. Stop the async validator producer so its final flush completes inside the
	//    bounded Stop() window. Bounded against the remaining ctx so a wedged broker
	//    Flush can't re-hang shutdown: when the budget is already spent (the timeout
	//    path above) we stop WAITING here and return, leaving the outstanding Stop()
	//    (and/or the producer's own ctx-cancel self-close) to complete the flush
	//    later if it can — it is not guaranteed to finish, but shutdown no longer
	//    blocks on it. A worker still publishing here drops safely (channelMu +
	//    SafeSend). Guarded and non-fatal.
	if ps.validatorKafkaProducerClient != nil {
		stopDone := make(chan struct{})
		go func() {
			defer close(stopDone)
			if err := ps.validatorKafkaProducerClient.Stop(); err != nil {
				ps.logger.Errorf("[Propagation] failed to stop validator kafka producer gracefully: %v", err)
			}
		}()

		select {
		case <-stopDone:
		case <-ctx.Done():
			ps.logger.Errorf("[Propagation] validator producer stop exceeded stop budget; relying on async self-close")
		}
	}

	return ctx.Err()
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

		// Process the transaction and return appropriate response. The parsed
		// transaction comes back with the error so the failure can name it
		// without parsing the body again: the only parse is the one inside
		// processTransaction, which is guarded against a parser panic on
		// adversarial input. It is nil exactly when there was nothing to name.
		failedTx, err := ps.processTransaction(ctx, &propagation_api.ProcessTransactionRequest{Tx: body})
		if err != nil {
			status := httpStatusForTxError(err)
			if status >= 200 && status < 300 {
				return c.String(status, "OK")
			}
			return c.String(status, "Failed to process transaction: "+failureLine(failedTx, err))
		}

		return c.String(http.StatusOK, "OK")
	}
}

// failureLine renders one failed transaction for a client-facing response
// body: the public message, always naming the transaction it is about.
//
// The txid is not otherwise guaranteed to be there. The public error boundary
// (errors.PublicError, and errors.UserMessage over it) surfaces the innermost
// allowlisted cause and discards everything outside it — including the
// "[ProcessTransaction][<txid>]" wrapper this package adds — so precisely the
// failures with the most useful messages arrive anonymous:
// "bad-txns-in-belowout", "insufficient-fee", "Script evaluated without error
// but finished with a false/empty top stack element". A client submitting a
// batch is then told that something failed, without being told what.
//
// The txid goes into the public message, and the line is then rendered by the
// errors package itself, so the "CODE (n): " prefix stays at the head where
// every existing consumer looks for it — including on errors.PublicError's own
// fallback, which yields the bare literal "internal error" and would otherwise
// produce a line with no code on it at all.
//
// tx may be nil (a parse failure, or a slot taken by context cancellation);
// there is no transaction to name then, and the message is returned as-is.
//
// The txid is derived here, while rendering the line, rather than being
// captured for every submission: a batch of N transactions with F failures
// pays for F lookups rather than N. processTransactionInternal caches the
// hash on the transaction, so each of those is a lookup and a hex encoding
// rather than a re-serialization and a double-SHA.
func failureLine(tx *bt.Tx, err error) string {
	publicErr := errors.PublicError(err)
	if publicErr == nil {
		return ""
	}

	if tx == nil {
		return publicErr.Error()
	}

	txid := tx.TxID()
	if strings.Contains(publicErr.Message(), txid) {
		// Already named — the outermost wrapper survived, or the cause names it
		// itself. Don't say it twice.
		return publicErr.Error()
	}

	// Rendered through errors.New so the code+message formatting has one owner.
	// publicErr.Message() is passed as an argument, never as the format string.
	return errors.New(publicErr.Code(), "[ProcessTransaction][%s] %s", txid, publicErr.Message()).Error()
}

// httpStatusForTxError maps a transaction processing error to the appropriate
// HTTP status code so clients can distinguish tx rejections from system
// failures. Walks the error chain via errors.Is so wrapped inner errors are
// classified by their actual cause.
func httpStatusForTxError(err error) int {
	switch {
	case errors.Is(err, errors.ErrTxExists):
		// Duplicate submission of a tx Teranode has already accepted. The
		// resource is already in the desired state — surface as success so
		// clients don't treat idempotent resubmits as failures.
		return http.StatusOK
	case errors.Is(err, errors.ErrFrozen):
		return http.StatusForbidden
	case errors.Is(err, errors.ErrTxInvalidDoubleSpend),
		errors.Is(err, errors.ErrTxConflicting),
		errors.Is(err, errors.ErrSpent),
		errors.Is(err, errors.ErrTxLocked),
		errors.Is(err, errors.ErrTxCreating):
		return http.StatusConflict
	case errors.Is(err, errors.ErrTxMissingParent):
		return http.StatusUnprocessableEntity
	case errors.Is(err, errors.ErrInvalidArgument),
		errors.Is(err, errors.ErrTxInvalid),
		errors.Is(err, errors.ErrTxLockTime),
		errors.Is(err, errors.ErrNonFinal),
		errors.Is(err, errors.ErrTxPolicy),
		errors.Is(err, errors.ErrTxCoinbaseImmature),
		errors.Is(err, errors.ErrUtxoInvalidSize):
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
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

		// Errors are reported in stream submission order. Each parse attempt and
		// each dispatched transaction is assigned a monotonically increasing
		// submission index; workers write into a pre-allocated slot at that
		// index. After all workers finish, the slots are walked in order to
		// produce a deterministic ordered error list — independent of the order
		// in which concurrent workers complete.
		//
		// errSlots has len == cap and is never resized, so the slice header is
		// never written by the producer after workers start. Workers write to
		// distinct indices, the producer writes to its own indices, and reads
		// happen only after processingWg.Wait() establishes happens-before.
		const maxSubmissions = maxTransactionsPerRequest + 1 // +1 for ctx-cancel slot
		errSlots := make([]error, maxSubmissions)
		// txSlots holds the transaction that occupies each submission slot, so a
		// failure can name it. Only a pointer is stored; the txid is derived
		// later, in failureLine, and only for the slots that actually failed.
		// Slots used by parse errors and the ctx-cancel path stay nil — there is
		// no transaction to name.
		txSlots := make([]*bt.Tx, maxSubmissions)
		nextSlot := 0
		processingWg := sync.WaitGroup{}
		totalNrTransactions := 0
		totalBytesRead := int64(0)

		// Caller contract: a batch must NOT contain both a parent and any of
		// its children. The server does not enforce this — violating it will
		// surface as missing-parent errors because txs in a batch are processed
		// concurrently here with no in-batch ordering. See ProcessTransactionBatch
		// for the gRPC equivalent of this pattern.
		processOne := func(tx *bt.Tx, slot int) {
			defer processingWg.Done()

			if ps.batchWorkerPool != nil {
				defer func() { <-ps.batchWorkerPool }()
			}

			defer func() {
				if r := recover(); r != nil {
					ps.logger.Errorf("Recovered from panic in processTransactionInternal: %v", r)
					errSlots[slot] = errors.NewProcessingError("transaction processing panic: %v", r)
				}
			}()

			if err := ps.processTransactionInternal(ctx, tx); err != nil {
				errSlots[slot] = err
			}
		}

		// Track early-exit error to return after cleanup
		var earlyExitMsg string

		// Read transactions with the bt reader in a loop
		for {
			// Check limits BEFORE reading the next transaction to prevent bypass attacks
			if totalNrTransactions >= maxTransactionsPerRequest {
				earlyExitMsg = "Invalid request body: too many transactions"
				break
			}

			if totalBytesRead >= maxDataPerRequest {
				earlyExitMsg = "Invalid request body: too much data"
				break
			}

			// All submission slots consumed (parse errors + successful txs).
			// Cut off the stream to prevent unbounded parse-error accumulation
			// from outgrowing the pre-allocated slot budget.
			if nextSlot >= maxSubmissions {
				earlyExitMsg = "Invalid request body: too many submissions"
				break
			}

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

				// Record the parse error in submission order.
				errSlots[nextSlot] = err
				nextSlot++

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

			// Acquire the server-wide batch semaphore before spawning a goroutine,
			// so total concurrent tx-processing goroutines stay bounded across all
			// HTTP and gRPC batch calls. If the limit is disabled (nil), spawn
			// immediately. Respect context cancellation so a disconnected client
			// can drain the handler.
			cancelled := false

			if ps.batchWorkerPool != nil {
				select {
				case ps.batchWorkerPool <- struct{}{}:
				case <-ctx.Done():
					errSlots[nextSlot] = errors.WrapPublic(ctx.Err())
					nextSlot++
					earlyExitMsg = "request context cancelled"
					cancelled = true
				}
			}

			if cancelled {
				break
			}

			// Reserve a submission slot for this tx and dispatch the worker.
			slot := nextSlot
			nextSlot++
			txSlots[slot] = tx
			processingWg.Add(1)

			go processOne(tx, slot)
		}

		// Wait for all worker goroutines to finish writing their slots before
		// reading errSlots. The Done/Wait pair establishes the happens-before
		// edge for the writes.
		processingWg.Wait()

		var errMsgs []string

		// Derive the aggregate HTTP status from the per-tx errors using the same
		// classifier as the single-tx path (httpStatusForTxError). A per-tx
		// outcome that classifies as success (e.g. a duplicate submission ->
		// StatusOK) is not a failure: skip it from both the body and the status,
		// mirroring handleSingleTx which returns OK for those. Among the genuine
		// failures the precedence is: a server fault (5xx, e.g. a storage error)
		// dominates and forces 500 — the client cannot fix it by resubmitting;
		// otherwise the first client-error (4xx) status in submission order wins,
		// so a batch of pure tx rejections is a client error (e.g. 400) rather
		// than a misleading 500.
		aggStatus := http.StatusOK

		for i, err := range errSlots[:nextSlot] {
			if err == nil {
				continue
			}

			txStatus := httpStatusForTxError(err)
			if txStatus < http.StatusBadRequest {
				// Success-classified outcome (e.g. duplicate submission): not a
				// failure, so it does not enter the body or raise the status.
				continue
			}

			errMsgs = append(errMsgs, failureLine(txSlots[i], err))

			switch {
			case txStatus >= http.StatusInternalServerError:
				aggStatus = http.StatusInternalServerError
			case aggStatus < http.StatusBadRequest:
				aggStatus = txStatus
			}
		}

		if earlyExitMsg != "" {
			return c.String(http.StatusBadRequest, earlyExitMsg)
		}

		// errMsgs only holds genuine failures now, so aggStatus is always >= 400 here.
		if len(errMsgs) > 0 {
			return c.String(aggStatus, "Failed to process transactions:\n"+strings.Join(errMsgs, "\n")+"\n")
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

	// Ban list middleware - reject requests from banned IPs early
	if ps.banList != nil {
		ps.httpServer.Use(banlist.CreateEchoMiddleware(ps.banList))
	}

	// Configure middleware and timeouts
	if ps.settings.Propagation.HTTPRateLimit > 0 {
		ps.httpServer.Use(middleware.RateLimiter(middleware.NewRateLimiterMemoryStore(rate.Limit(ps.settings.Propagation.HTTPRateLimit))))
	}

	if ps.settings.Propagation.HTTPBodyLimit != "" {
		ps.httpServer.Use(middleware.BodyLimit(ps.settings.Propagation.HTTPBodyLimit))
	}

	ps.httpServer.Server.ReadTimeout = 30 * time.Second
	ps.httpServer.Server.ReadHeaderTimeout = 10 * time.Second
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
	// Non-blocking admission control: reject immediately if too many handlers are in-flight
	if ps.batchHandlerPool != nil {
		select {
		case ps.batchHandlerPool <- struct{}{}:
			defer func() { <-ps.batchHandlerPool }()
		default:
			prometheusBatchHandlerRejections.Inc()
			return nil, status.Error(codes.Unavailable, "server at capacity")
		}
	}

	// Use context-aware logger for automatic trace correlation
	ctxLogger := ps.logger.WithTraceContext(ctx)

	ctxLogger.Debugf("[ProcessTransaction] processing transaction request")

	if _, err := ps.processTransaction(ctx, req); err != nil {
		ctxLogger.Errorf("[ProcessTransaction] failed to process transaction: %v", err)

		return nil, errors.WrapGRPCPublic(err)
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
	// Non-blocking admission control: reject immediately if too many handlers are in-flight
	if ps.batchHandlerPool != nil {
		select {
		case ps.batchHandlerPool <- struct{}{}:
			defer func() { <-ps.batchHandlerPool }()
		default:
			prometheusBatchHandlerRejections.Inc()
			return nil, status.Error(codes.Unavailable, "server at capacity")
		}
	}

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

		// Acquire server-wide semaphore before spawning goroutine to limit
		// total concurrent tx-processing goroutines across all batch calls.
		if ps.batchWorkerPool != nil {
			select {
			case ps.batchWorkerPool <- struct{}{}:
			case <-ctx.Done():
				response.Errors[idx] = errors.WrapPublic(ctx.Err())
				continue
			}
		}

		g.Go(func() error {
			if ps.batchWorkerPool != nil {
				defer func() { <-ps.batchWorkerPool }()
			}

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
			if _, err := ps.processTransaction(txCtx, &propagation_api.ProcessTransactionRequest{
				Tx: tx,
			}); err != nil {
				// Use context-aware logger for trace correlation
				ps.logger.WithTraceContext(txCtx).Errorf("[ProcessTransactionBatch] failed to process transaction %d: %v", idx, err)

				response.Errors[idx] = errors.WrapPublic(err)
			} else {
				response.Errors[idx] = nil
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		ps.logger.WithTraceContext(ctx).Errorf("[ProcessTransactionBatch] failed to process transaction batch: %v", err)

		return nil, errors.WrapGRPCPublic(err)
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
//   - *bt.Tx: the parsed transaction, once parsing has succeeded, so a caller
//     can name it in a client-facing failure without parsing the body a second
//     time. nil when the body could not be parsed (or was rejected before
//     parsing, on size), which is exactly when there is no transaction to name.
//   - error: error if any processing step fails
func (ps *PropagationServer) processTransaction(ctx context.Context, req *propagation_api.ProcessTransactionRequest) (*bt.Tx, error) {
	ctx, span, endSpan := tracing.Tracer("propagation").Start(ctx, "processTransaction",
		tracing.WithParentStat(ps.stats),
	)
	defer endSpan()

	timeStart := time.Now()
	txSize := len(req.Tx)

	// Check transaction size BEFORE parsing to avoid wasting CPU on oversized transactions
	if ps.settings != nil && ps.settings.Policy != nil {
		maxTxSize := ps.settings.Policy.GetMaxTxSizePolicy()
		if maxTxSize > 0 && txSize > maxTxSize {
			prometheusInvalidTransactions.Inc()
			err := errors.NewTxInvalidError("[ProcessTransaction] transaction size %d exceeds maximum allowed size %d", txSize, maxTxSize)
			span.RecordError(err)
			return nil, err
		}
	}

	var btTx *bt.Tx
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = errors.NewProcessingError("transaction parsing panic: %v", r)
				ps.logger.WithTraceContext(ctx).Errorf("Recovered from panic in bt.NewTxFromBytes: %v", r)
			}
		}()
		btTx, err = bt.NewTxFromBytes(req.Tx)
	}()

	if err != nil {
		prometheusInvalidTransactions.Inc()

		err = errors.NewProcessingError("[ProcessTransaction] failed to parse transaction from bytes", err)
		span.RecordError(err)

		return nil, err
	}

	if err = ps.processTransactionInternal(ctx, btTx); err != nil {
		span.RecordError(err)
		return btTx, err
	}

	prometheusTransactionSize.Observe(float64(txSize))
	prometheusProcessedTransactions.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)

	return btTx, nil
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

	// Cache the hash on the transaction. bt.Tx.TxIDChainHash re-serializes and
	// double-hashes on every call unless SetTxHash has been used, and every
	// ingest path asks for the txid repeatedly: the coinbase check below, the
	// blob key, the Kafka message key, a Debugf argument that is evaluated
	// whether or not debug logging is on, and every error message. Caching here
	// covers HTTP /tx, gRPC, and HTTP /txs (which parses with ReadFrom and
	// calls this directly), so one hash replaces all of them.
	btTx.SetTxHash(btTx.TxIDChainHash())

	// Do not allow propagation of coinbase transactions
	if btTx.IsCoinbase() {
		prometheusInvalidTransactions.Inc()
		return errors.NewTxInvalidError("[ProcessTransaction][%s] received coinbase transaction", btTx.TxID())
	}

	// do some very simple sanity checks on the transaction
	if err = ps.txSanityChecks(btTx); err != nil {
		return err
	}

	// Serialize once and reuse everywhere downstream to avoid redundant allocations
	txBytes := btTx.SerializeBytes()

	// we should store all transactions, if this fails we should not validate the transaction
	if err = ps.storeTransaction(ctx, btTx, txBytes); err != nil {
		return errors.NewStorageError("[ProcessTransaction][%s] failed to save transaction", btTx.TxIDChainHash(), err)
	}

	// This branch decides whether the submitter is ever told the verdict.
	//
	// With Kafka configured — kafka_validatortxsConfig is populated for the
	// .operator context, so this is the production shape — a normal-sized
	// transaction is published and this function returns nil, so /tx and /txs
	// answer 200 OK before the validator has looked at it. Any rejection is
	// discovered asynchronously and reaches the client through some other
	// channel, if at all. Only two paths validate synchronously and can report a
	// reason: the > KafkaMaxMessageBytes HTTP fallback just below, and the
	// Kafka-less else-branch. Everything the surfaces above do with a rejection
	// reason applies to those two and nothing else.
	if ps.validatorKafkaProducerClient != nil {
		txSize := len(txBytes)
		maxKafkaMessageSize := ps.settings.Validator.KafkaMaxMessageBytes

		if txSize > maxKafkaMessageSize {
			return ps.validateTransactionViaHTTP(ctx, btTx, txBytes, txSize, maxKafkaMessageSize)
		}

		// For normal-sized transactions, continue with Kafka
		return ps.validateTransactionViaKafka(btTx, txBytes)
	} else {
		ps.logger.WithTraceContext(ctx).Debugf("[ProcessTransaction][%s] Calling validate function", btTx.TxID())

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

	// Check for duplicate inputs (same prevTxID and vout)
	if err := ps.checkDuplicateInputs(btTx); err != nil {
		return err
	}

	return nil
}

// checkDuplicateInputs verifies that a transaction doesn't spend the same output twice.
// Optimized to avoid allocations for the common case of no duplicates.
func (ps *PropagationServer) checkDuplicateInputs(btTx *bt.Tx) error {
	numInputs := len(btTx.Inputs)

	// Fast path: single input can't have duplicates
	if numInputs <= 1 {
		return nil
	}

	// Use a map with pre-allocated capacity
	// Key format: 32 bytes prevTxID + 4 bytes vout = 36 bytes, use [36]byte as key to avoid string alloc
	type inputKey struct {
		prevTxID [32]byte
		vout     uint32
	}

	seen := make(map[inputKey]struct{}, numInputs)
	for _, input := range btTx.Inputs {
		var key inputKey
		key.prevTxID = *input.PreviousTxIDChainHash()
		key.vout = input.PreviousTxOutIndex

		if _, exists := seen[key]; exists {
			prometheusInvalidTransactions.Inc()
			return errors.NewTxInvalidError("[ProcessTransaction][%s] duplicate input found: %x:%d", btTx.TxID(), key.prevTxID, key.vout)
		}
		seen[key] = struct{}{}
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
//   - txBytes: pre-serialized transaction bytes to avoid redundant serialization
//   - txSize: Size of the transaction in bytes (pre-calculated)
//   - maxKafkaMessageSize: Maximum Kafka message size for logging/comparison
//
// Returns:
//   - error: Error if HTTP validation fails or is not available
func (ps *PropagationServer) validateTransactionViaHTTP(ctx context.Context, btTx *bt.Tx, txBytes []byte, txSize int, maxKafkaMessageSize int) error {
	if ps.validatorHTTPAddr == nil {
		return errors.NewServiceError("[ProcessTransaction][%s] Transaction size %d bytes exceeds Kafka message limit (%d bytes), but no HTTP endpoint configured for validator",
			btTx.TxID(), txSize, maxKafkaMessageSize)
	}

	ps.logger.WithTraceContext(ctx).Warnf("[ProcessTransaction][%s] Transaction size %d bytes exceeds Kafka message limit (%d bytes), falling back to validator /tx endpoint",
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

	req, err := http.NewRequestWithContext(ctx, "POST", fullURL.String(), bytes.NewReader(txBytes))
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
		return ps.validatorRejection(ctx, btTx, resp, body)
	}

	ps.logger.WithTraceContext(ctx).Debugf("[ProcessTransaction][%s] successfully validated using validator /tx endpoint", btTx.TxID())

	return nil
}

// validatorRejection turns a non-OK response from the validator's /tx endpoint
// into an error that says what actually happened to the transaction.
//
// This is the only path on which a Kafka-wired node answers a submitter with a
// real verdict — every normal-sized transaction is published and answered 200 OK
// before validation runs — so getting it wrong is not a corner case. It was
// wrong: the whole response was wrapped as a SERVICE_ERROR, which is off
// publicCauseCodes, so httpStatusForTxError classified a permanently invalid
// transaction as a retryable 500, and the validator's rendered error chain —
// file, line and function included — was spliced verbatim into a client-facing
// body that errors.UserMessage could no longer strip, because by then it was
// plain text inside one error's message.
//
// The validator attaches its public code and message as a header
// (errors.AttachHTTPError). When it is there, that verdict is what the client is
// told, under this node's [ProcessTransaction][<txid>] context, and the response
// body goes to the log rather than into the returned error. When it is not —
// an older validator, or a proxy answering on its behalf — the previous wrapping
// stands (status and body included), so this degrades to the old behaviour
// rather than losing the failure.
func (ps *PropagationServer) validatorRejection(ctx context.Context, btTx *bt.Tx, resp *http.Response, body []byte) error {
	verdict := errors.HTTPErrorFrom(resp.Header)
	if verdict == nil {
		return errors.NewServiceError("[ProcessTransaction][%s] validator /tx endpoint returned non-OK status: %d, body: %s",
			btTx.TxID(), resp.StatusCode, string(body))
	}

	ps.logger.WithTraceContext(ctx).Warnf("[ProcessTransaction][%s] validator /tx endpoint rejected transaction: status=%d body=%s",
		btTx.TxID(), resp.StatusCode, string(body))

	return errors.New(verdict.Code(), "[ProcessTransaction][%s] %s", btTx.TxID(), verdict.Message())
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
//   - txBytes: pre-serialized transaction bytes to avoid redundant serialization
//
// Returns:
//   - error: Error if message preparation or publishing fails
func (ps *PropagationServer) validateTransactionViaKafka(btTx *bt.Tx, txBytes []byte) error {
	validationOptions := validator.NewDefaultOptions()

	msg := &kafkamessage.KafkaTxValidationTopicMessage{
		Tx:     txBytes,
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
//   - txBytes: pre-serialized transaction bytes to avoid redundant serialization
//
// Returns:
//   - error: error with detailed context if the storage operation fails
func (ps *PropagationServer) storeTransaction(ctx context.Context, btTx *bt.Tx, txBytes []byte) error {
	ctx, _, deferFn := tracing.Tracer("propagation").Start(ctx, "PropagationServer:Set:Store")
	defer deferFn()

	if ps.txStore != nil {
		if err := ps.txStore.Set(ctx, btTx.TxIDChainHash().CloneBytes(), fileformat.FileTypeTx, txBytes); err != nil {
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
