// Package propagation implements Bitcoin SV transaction propagation and validation services.
// It provides functionality for processing, validating, and distributing BSV transactions
// across the network using multiple protocols including GRPC and UDP6 multicast.
package propagation

import (
	"bytes"
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/legacy/wire"
	"github.com/bitcoin-sv/teranode/services/propagation/propagation_api"
	"github.com/bitcoin-sv/teranode/services/validator"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/health"
	"github.com/bitcoin-sv/teranode/util/kafka"
	kafkamessage "github.com/bitcoin-sv/teranode/util/kafka/kafka_message"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	maxTransactionsPerRequest = 1024
	maxDataPerRequest         = 32 * 1024 * 1024
)

var (
	// maxDatagramSize defines the maximum size of UDP datagrams for IPv6 multicast
	maxDatagramSize = 512 // 100 * 1024 * 1024
	// ipv6Port defines the default port used for IPv6 multicast listeners
	ipv6Port = 9999
)

// PropagationServer implements the transaction propagation service for Bitcoin SV.
// It handles transaction validation, storage, and distribution across the network
// while managing connections to various backend services including validators,
// blockchain clients, and Kafka producers.
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
	checks := make([]health.Check, 0, 5)
	checks = append(checks, health.Check{Name: "Kafka", Check: kafka.HealthChecker(ctx, brokersURL)})

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
				_, _, _, src, err = conn.ReadMsgUDP(buffer, oobB)
				if err != nil {
					log.Fatal("ReadFromUDP failed:", err)
				}
				// ps.logger.Infof("read %d bytes from %s, out of bounds data len %d", len(buffer), src.String(), len(oobB))

				reader := bytes.NewReader(buffer)

				msg, b, err = wire.ReadMessage(reader, wire.ProtocolVersion, wire.MainNet)
				if err != nil {
					ps.logger.Errorf("wire.ReadMessage failed: %v", err)
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

// handleSingleTx handles a single transaction request on the /tx endpoint
func (ps *PropagationServer) handleSingleTx(_ context.Context) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx, _, deferFn := tracing.StartTracing(c.Request().Context(), "handleSingleTx",
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

// handleMultipleTx handles multiple transactions on the /txs endpoint
func (ps *PropagationServer) handleMultipleTx(_ context.Context) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx, _, deferFn := tracing.StartTracing(c.Request().Context(), "handleMultipleTx",
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

			// Read transaction from request body
			bytesRead, err := tx.ReadFrom(c.Request().Body)
			if err != nil {
				// End of stream is expected and not an error
				if err == io.EOF {
					break
				}

				processingErrorWg.Add(1)
				processErrors <- err
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

// startHTTPServer initializes and starts the HTTP server for transaction processing
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

// startAndMonitorHTTPServer starts the HTTP server and monitors for shutdown
func (ps *PropagationServer) startAndMonitorHTTPServer(ctx context.Context, httpAddresses string) {
	// Start the server
	go func() {
		if err := ps.httpServer.Start(httpAddresses); err != nil {
			if err == http.ErrServerClosed {
				ps.logger.Infof("http server shutdown")
			} else {
				ps.logger.Errorf("failed to start http server: %v", err)
			}
		}
	}()

	// Monitor for context cancellation
	go func() {
		<-ctx.Done()

		_ = ps.httpServer.Shutdown(context.Background())
	}()
}

// ProcessTransaction validates and stores a single transaction.
// It performs the following steps:
// 1. Validates transaction format
// 2. Verifies it's not a coinbase transaction
// 3. Ensures transaction is extended
// 4. Stores the transaction
// 5. Triggers validation through validator service or Kafka
//
// Parameters:
//   - ctx: context for the transaction processing
//   - req: transaction processing request containing raw transaction data
//
// Returns:
//   - *propagation_api.EmptyMessage: empty response on success
//   - error: error if transaction processing fails
func (ps *PropagationServer) ProcessTransaction(ctx context.Context, req *propagation_api.ProcessTransactionRequest) (*propagation_api.EmptyMessage, error) {
	if err := ps.processTransaction(ctx, req); err != nil {
		ps.logger.Errorf("[ProcessTransaction] failed to process transaction: %v", err)

		return nil, errors.WrapGRPC(err)
	}

	return &propagation_api.EmptyMessage{}, nil
}

// ProcessTransactionBatch processes multiple transactions concurrently.
// It uses error groups to handle parallel processing while maintaining
// proper error handling and context cancellation.
//
// Parameters:
//   - ctx: context for the batch processing operation
//   - req: batch request containing multiple transactions
//
// Returns:
//   - *propagation_api.ProcessTransactionBatchResponse: response containing error status for each transaction
//   - error: error if batch processing fails
func (ps *PropagationServer) ProcessTransactionBatch(ctx context.Context, req *propagation_api.ProcessTransactionBatchRequest) (*propagation_api.ProcessTransactionBatchResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "ProcessTransactionBatch",
		tracing.WithParentStat(ps.stats),
		tracing.WithHistogram(prometheusProcessedTransactionBatch),
		tracing.WithDebugLogMessage(ps.logger, "[ProcessTransactionBatch] called for %d transactions", len(req.Tx)),
	)
	defer deferFn()

	response := &propagation_api.ProcessTransactionBatchResponse{
		Errors: make([]*errors.TError, len(req.Tx)),
	}

	g, gCtx := errgroup.WithContext(ctx)

	for idx, tx := range req.Tx {
		idx := idx
		tx := tx

		g.Go(func() error {
			// just call the internal process transaction function for every transaction
			if err := ps.processTransaction(gCtx, &propagation_api.ProcessTransactionRequest{
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
	_, _, deferFn := tracing.StartTracing(ctx, "processTransaction",
		tracing.WithParentStat(ps.stats),
	)
	defer deferFn()

	timeStart := time.Now()

	btTx, err := bt.NewTxFromBytes(req.Tx)
	if err != nil {
		prometheusInvalidTransactions.Inc()
		return errors.NewProcessingError("[ProcessTransaction] failed to parse transaction from bytes", err)
	}

	if err = ps.processTransactionInternal(ctx, btTx); err != nil {
		return err
	}

	prometheusTransactionSize.Observe(float64(len(req.Tx)))
	prometheusProcessedTransactions.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)

	return nil
}

func (ps *PropagationServer) processTransactionInternal(ctx context.Context, btTx *bt.Tx) error {
	// Do not allow propagation of coinbase transactions
	if btTx.IsCoinbase() {
		prometheusInvalidTransactions.Inc()
		return errors.NewTxInvalidError("[ProcessTransaction][%s] received coinbase transaction", btTx.TxID())
	}

	if !btTx.IsExtended() {
		return errors.NewTxInvalidError("[ProcessTransaction][%s] transaction is not extended", btTx.TxID())
	}

	// decouple the tracing context to not cancel the context when the tx is being saved in the background
	callerSpan := tracing.DecoupleTracingSpan(ctx, "decoupleStoreTransaction")
	defer callerSpan.Finish()

	// we should store all transactions, if this fails we should not validate the transaction
	if err := ps.storeTransaction(callerSpan.Ctx, btTx); err != nil {
		return errors.NewStorageError("[ProcessTransaction][%s] failed to save transaction", btTx.TxIDChainHash(), err)
	}

	if ps.validatorKafkaProducerClient != nil {
		// Check transaction size first - if it's too large, use HTTP endpoint instead
		txSize := len(btTx.ExtendedBytes())
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
		if _, err := ps.validator.Validate(ctx, btTx, 0); err != nil {
			return errors.NewProcessingError("[ProcessTransaction][%s] failed to validate transaction", btTx.TxID(), err)
		}
	}

	return nil
}

// validateTransactionViaHTTP sends a transaction to the validator's HTTP endpoint
// This is used as a fallback when Kafka message size limits are exceeded
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

	req, err := http.NewRequestWithContext(ctx, "POST", fullURL.String(), bytes.NewReader(btTx.ExtendedBytes()))
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

// validateTransactionViaKafka sends a transaction to the validator through Kafka
func (ps *PropagationServer) validateTransactionViaKafka(btTx *bt.Tx) error {
	validationOptions := validator.NewDefaultOptions()

	msg := &kafkamessage.KafkaTxValidationTopicMessage{
		Tx:     btTx.ExtendedBytes(),
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
		Value: value,
	})

	return nil
}

// storeTransaction persists a transaction to the configured storage backend.
// It stores the transaction using its chain hash as the key and extended
// bytes as the value.
//
// Parameters:
//   - ctx: context for the storage operation
//   - btTx: Bitcoin transaction to store
//
// Returns:
//   - error: error if storage operation fails
func (ps *PropagationServer) storeTransaction(ctx context.Context, btTx *bt.Tx) error {
	ctx, _, deferFn := tracing.StartTracing(ctx, "PropagationServer:Set:Store")
	defer deferFn()

	if ps.txStore != nil {
		if err := ps.txStore.Set(ctx, btTx.TxIDChainHash().CloneBytes(), btTx.ExtendedBytes()); err != nil {
			// TODO make this resilient to errors
			// write it to secondary store (Kafka) and retry?
			return err
		}
	}

	return nil
}
