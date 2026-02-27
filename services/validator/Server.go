/*
Package validator implements Bitcoin SV transaction validation functionality.

The validator package is a critical component of the Teranode architecture that handles
transaction validation according to Bitcoin SV consensus rules. It enforces transaction
rules, manages UTXO state transitions, and ensures that only valid transactions are
accepted into the mempool and blocks.

Key features of the validator package include:
- Comprehensive transaction validation against Bitcoin SV consensus rules
- Multiple script execution engines (GoBDK, GoSDK, GoBT) for script verification
- Integration with UTXO store for input/output tracking and double-spend prevention
- Batch processing capability for efficient validation of transaction groups
- Support for both synchronous (RPC) and asynchronous (Kafka) validation paths
- Integration with block assembly for transaction inclusion in mining templates

This file implements the validator server component, providing gRPC endpoints
for transaction validation services and managing the interaction between
different validation components. The server handles validation requests, manages
health checking, and coordinates with other services like blockchain and UTXO store.
*/
package validator

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/validator/validator_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/health"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Server implements the validator gRPC service and manages validation operations.
// It acts as the primary coordinator for transaction validation requests, integrating
// with multiple components (UTXO store, blockchain, Kafka) to provide a complete
// validation service. The Server handles both synchronous API requests and asynchronous
// message processing through Kafka.
type Server struct {
	// UnsafeValidatorAPIServer embeds the base gRPC server implementation for the validator API,
	// providing the foundational gRPC service methods that can be overridden by this Server implementation.
	// This embedding pattern allows the Server to implement the validator API interface while maintaining
	// flexibility for custom method implementations and middleware integration.
	validator_api.UnsafeValidatorAPIServer

	// validator is the core validation engine that implements the Interface contract for transaction validation.
	// This component handles all validation logic including consensus rule enforcement, script execution,
	// and UTXO verification. It serves as the primary business logic layer for the validation service.
	validator Interface

	// logger provides structured logging capabilities for the validator server, enabling comprehensive
	// monitoring and debugging of validation operations. All server activities, errors, and performance
	// metrics are logged through this component for operational visibility.
	logger ulogger.Logger

	// settings contains the complete configuration for the validator service, including validation parameters,
	// network settings, database connections, and operational thresholds. These settings control the
	// behavior of all validation operations and service integrations.
	settings *settings.Settings

	// utxoStore provides direct access to the UTXO database for transaction input validation and
	// double-spend prevention. This store is used to verify transaction inputs, check UTXO availability,
	// and maintain the current state of unspent transaction outputs across the blockchain.
	utxoStore utxo.Store

	// kafkaSignal is a channel used to coordinate graceful shutdown of Kafka-related components.
	// When a shutdown signal is received, this channel notifies all Kafka consumers and producers
	// to complete their current operations and terminate cleanly.
	kafkaSignal chan os.Signal

	// stats collects performance metrics for the validator service, including request counts,
	// processing times, and error rates. These metrics are used for monitoring and optimization.
	stats *gocore.Stat

	// ctx is the server's main context used for operation management and cancellation.
	// This context is used to manage the lifecycle of the server and its components.
	ctx context.Context

	// blockchainClient connects to the blockchain service for block-related operations,
	// including block height retrieval, chain state verification, and FSM synchronization.
	// This client is used to ensure the validator service remains synchronized with the blockchain.
	blockchainClient blockchain.ClientI

	// consumerClient receives validation requests via Kafka, providing an asynchronous
	// validation path for high-throughput processing. This client is used to consume
	// Kafka messages and trigger validation operations.
	consumerClient kafka.KafkaConsumerGroupI

	// txMetaKafkaProducerClient publishes transaction metadata to Kafka, enabling
	// downstream processing and analytics. This producer is used to send transaction
	// metadata to Kafka topics for further processing.
	txMetaKafkaProducerClient kafka.KafkaAsyncProducerI

	// rejectedTxKafkaProducerClient publishes rejected transaction information to Kafka,
	// providing visibility into validation failures and error conditions. This producer
	// is used to send rejected transaction data to Kafka topics for monitoring and analysis.
	rejectedTxKafkaProducerClient kafka.KafkaAsyncProducerI

	// blockAssemblyClient connects to the block assembly service for mining integration,
	// enabling the validator service to participate in block template generation and
	// transaction inclusion in mining operations. This client is used to interact with
	// the block assembly service and coordinate transaction inclusion.
	blockAssemblyClient blockassembly.ClientI

	// httpServer handles HTTP API requests for transaction validation, providing a
	// synchronous validation path for clients. This server is used to process HTTP
	// requests and return validation results.
	httpServer *echo.Echo
}

// NewServer creates and initializes a new validator server instance with the specified components.
// This function initializes Prometheus metrics and configures the server with all required
// dependencies for transaction validation. It does not start any background processes
// or establish connections - that happens in the Init and Start methods.
//
// Parameters:
//   - logger: Logger instance for server operations and error reporting
//   - tSettings: Configuration settings for the validator service
//   - utxoStore: UTXO database interface for transaction input validation
//   - blockchainClient: Interface to blockchain operations for block height and chain state
//   - consumerClient: Kafka consumer client for receiving validation requests
//   - txMetaKafkaProducerClient: Kafka producer for publishing transaction metadata
//   - rejectedTxKafkaProducerClient: Kafka producer for publishing rejected transactions
//   - blockAssemblyClient: Client for block assembly service integration
//
// Returns:
//   - *Server: Initialized server instance ready for Init() and Start() calls
func NewServer(logger ulogger.Logger, tSettings *settings.Settings, utxoStore utxo.Store,
	blockchainClient blockchain.ClientI, consumerClient kafka.KafkaConsumerGroupI,
	txMetaKafkaProducerClient kafka.KafkaAsyncProducerI, rejectedTxKafkaProducerClient kafka.KafkaAsyncProducerI,
	blockAssemblyClient blockassembly.ClientI) *Server {
	initPrometheusMetrics()

	return &Server{
		logger:                        logger,
		settings:                      tSettings,
		utxoStore:                     utxoStore,
		stats:                         gocore.NewStat("validator"),
		blockchainClient:              blockchainClient,
		consumerClient:                consumerClient,
		txMetaKafkaProducerClient:     txMetaKafkaProducerClient,
		rejectedTxKafkaProducerClient: rejectedTxKafkaProducerClient,
		blockAssemblyClient:           blockAssemblyClient,
	}
}

// Health performs health checks on the validator service and its dependencies.
// This function serves as the foundation for both liveness and readiness checks
// used in Kubernetes and other container orchestration systems. When used as a
// liveness check (checkLiveness=true), it only verifies basic service operation.
// When used as a readiness check (checkLiveness=false), it performs comprehensive
// dependency checks to ensure the service can properly handle validation requests.
//
// Parameters:
//   - ctx: Context for the health check operation, used for cancellation and timeouts
//   - checkLiveness: If true, performs only basic service liveness checks without dependency validation
//     If false, performs full readiness check including all service dependencies
//
// Returns:
//   - int: HTTP status code indicating health status (200=healthy, other codes=unhealthy)
//   - string: Detailed health status message including component-specific information
//   - error: Any errors encountered during health check, nil if healthy
func (v *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// Add liveness checks here. Don't include dependency checks.
		// If the service is stuck return http.StatusServiceUnavailable
		// to indicate a restart is needed
		return http.StatusOK, "OK", nil
	}

	var brokersURL []string
	if v.consumerClient != nil { // tests may not set this
		brokersURL = v.consumerClient.BrokersURL()
	}

	// Add readiness checks here. Include dependency checks.
	// If any dependency is not ready, return http.StatusServiceUnavailable
	// If all dependencies are ready, return http.StatusOK
	// A failed dependency check does not imply the service needs restarting
	checks := make([]health.Check, 0, 7)

	// Check if the gRPC server is actually listening and accepting requests
	// Only check if the address is configured (not empty)
	if v.settings.Validator.GRPCListenAddress != "" {
		checks = append(checks, health.Check{
			Name: "gRPC Server",
			Check: health.CheckGRPCServerWithSettings(v.settings.Validator.GRPCListenAddress, v.settings, func(ctx context.Context, conn *grpc.ClientConn) error {
				client := validator_api.NewValidatorAPIClient(conn)
				_, err := client.HealthGRPC(ctx, &validator_api.EmptyMessage{})
				return err
			}),
		})
	}

	// Check if the HTTP server is actually listening and accepting requests
	if v.settings.Validator.HTTPListenAddress != "" {
		addr := v.settings.Validator.HTTPListenAddress
		if strings.HasPrefix(addr, ":") {
			addr = "localhost" + addr
		}
		checks = append(checks, health.Check{
			Name:  "HTTP Server",
			Check: health.CheckHTTPServer(fmt.Sprintf("http://%s", addr), "/health"),
		})
	}

	checks = append(checks, health.Check{Name: "Kafka", Check: kafka.HealthChecker(ctx, brokersURL)})

	if v.blockchainClient != nil {
		checks = append(checks, health.Check{Name: "BlockchainClient", Check: v.blockchainClient.Health})
		checks = append(checks, health.Check{Name: "FSM", Check: blockchain.CheckFSM(v.blockchainClient)})
	}

	if v.utxoStore != nil {
		checks = append(checks, health.Check{Name: "UTXOStore", Check: v.utxoStore.Health})
	}

	if v.validator != nil {
		checks = append(checks, health.Check{Name: "Validator", Check: v.validator.Health})
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

// HealthGRPC implements the gRPC health check endpoint for the validator service.
// This method provides the gRPC interface for health checking and is part of the
// validator_api.ValidatorAPIServer interface implementation. It records metrics
// for monitoring purposes and delegates the actual health check to the Health method.
// This endpoint supports Kubernetes readiness probes and service mesh health checking.
//
// Parameters:
//   - ctx: Context for the health check operation, used for cancellation and timeouts
//   - _: Empty message parameter (unused), required by the gRPC interface contract
//
// Returns:
//   - *validator_api.HealthResponse: Health check response containing status and details
//     The Ok field indicates overall health (true=healthy)
//   - error: Any errors encountered during health check, wrapped appropriately for gRPC
func (v *Server) HealthGRPC(ctx context.Context, _ *validator_api.EmptyMessage) (*validator_api.HealthResponse, error) {
	prometheusHealth.Add(1)

	// Add context value to prevent circular dependency when checking gRPC server health
	ctx = context.WithValue(ctx, "skip-grpc-self-check", true)
	status, details, err := v.Health(ctx, false)

	return &validator_api.HealthResponse{
		Ok:      status == http.StatusOK,
		Details: details,
	}, errors.WrapGRPC(err)
}

// Init initializes the validator server and sets up the core validation engine.
// This method must be called before Start() and after NewServer(). It creates the
// core validator component and performs necessary setup operations. The method
// validates configuration requirements, particularly ensuring the blockassembly
// client is properly configured if block assembly is enabled.
//
// Parameters:
//   - ctx: Context for initialization, stored for the server's lifetime and used for cancellation
//
// Returns:
//   - error: Any initialization errors, including configuration issues or dependency failures
//     Returns ServiceError if blockassembly is enabled but the client is nil
func (v *Server) Init(ctx context.Context) (err error) {
	v.ctx = ctx

	if v.blockAssemblyClient == nil && !v.settings.BlockAssembly.Disabled {
		return errors.NewServiceError("[Init] blockassembly client is nil while enabled in the validator", nil)
	}

	v.validator, err = New(ctx, v.logger, v.settings, v.utxoStore, v.txMetaKafkaProducerClient, v.rejectedTxKafkaProducerClient, v.blockAssemblyClient, v.blockchainClient)
	if err != nil {
		return errors.NewServiceError("[Init] could not create validator", err)
	}

	return nil
}

// Start begins the validator server operation and registers handlers for validation requests.
// This method initiates all background processing, including Kafka consumer setup, HTTP API servers,
// and synchronization with the blockchain FSM. It waits for the blockchain FSM to transition
// from IDLE state before becoming fully operational, ensuring chain state consistency.
//
// Start is typically called after Init() and runs until the context is cancelled or
// an unrecoverable error occurs. It signals readiness through the readyCh channel
// once all components are ready to accept validation requests.
//
// Parameters:
//   - ctx: Context for server operation, used to manage the server lifecycle and cancellation
//   - readyCh: Channel used to signal when the server is ready to accept requests
//
// Returns:
//   - error: Any startup errors, including FSM transition failures, Kafka setup issues,
//     or HTTP server initialization problems
func (v *Server) Start(ctx context.Context, readyCh chan<- struct{}) error {
	var closeOnce sync.Once
	defer closeOnce.Do(func() { close(readyCh) })

	// Blocks until the FSM transitions from the IDLE state
	err := v.blockchainClient.WaitUntilFSMTransitionFromIdleState(ctx)
	if err != nil {
		v.logger.Errorf("[Validator] Failed to wait for FSM transition from IDLE state: %s", err)

		return err
	}

	kafkaMessageHandler := func(msg *kafka.KafkaMessage) error {
		var kafkaMsg kafkamessage.KafkaTxValidationTopicMessage
		if err := proto.Unmarshal(msg.Value, &kafkaMsg); err != nil {
			v.logger.Errorf("Failed to unmarshal kafka message: %v", err)

			return err
		}

		tx, err := bt.NewTxFromBytes(kafkaMsg.Tx)
		if err != nil {
			prometheusInvalidTransactions.Inc()
			v.logger.Errorf("[Validator] failed to parse transaction from bytes: %v", err)

			return err
		}

		height := kafkaMsg.Height

		options := &Options{
			SkipUtxoCreation:     kafkaMsg.Options.SkipUtxoCreation,
			AddTXToBlockAssembly: kafkaMsg.Options.AddTXToBlockAssembly,
			SkipPolicyChecks:     kafkaMsg.Options.SkipPolicyChecks,
			CreateConflicting:    kafkaMsg.Options.CreateConflicting,
		}

		// should not pass in a height when validating from Kafka, should just be current utxo store height
		if _, err = v.validator.ValidateWithOptions(ctx, tx, height, options); err != nil {
			prometheusInvalidTransactions.Inc()
			v.logger.Errorf("[Validator] Invalid tx: %s", err)

			return err
		}

		return nil
	}

	if v.consumerClient != nil {
		v.consumerClient.Start(ctx, kafkaMessageHandler, kafka.WithLogErrorAndMoveOn())
	}

	if err = v.startHTTPServer(ctx, v.settings.Validator.HTTPListenAddress); err != nil {
		return err
	}

	//  Start gRPC server - this will block
	if err := util.StartGRPCServer(ctx, v.logger, v.settings, "validator", v.settings.Validator.GRPCListenAddress, func(server *grpc.Server) {
		validator_api.RegisterValidatorAPIServer(server, v)
		closeOnce.Do(func() { close(readyCh) })
	}, nil); err != nil {
		return err
	}

	return nil
}

// Stop gracefully shuts down the validator server and all associated components.
// This method performs an orderly shutdown of all server resources, including Kafka
// producers/consumers and any background tasks. It sends termination signals to Kafka
// components and ensures proper cleanup of all connections.
//
// The method should be called when the service is being terminated to ensure proper
// cleanup and prevent resource leaks. It is typically called as part of service
// shutdown orchestration.
//
// Parameters:
//   - ctx: Context for shutdown operation (currently unused but maintained for interface consistency)
//
// Returns:
//   - error: Any shutdown errors encountered during the cleanup process
//     Returns nil if shutdown is successful or if no cleanup was necessary
func (v *Server) Stop(_ context.Context) error {
	if v.kafkaSignal != nil {
		v.kafkaSignal <- syscall.SIGTERM
	}

	if v.consumerClient != nil {
		// close the kafka consumer gracefully
		if err := v.consumerClient.Close(); err != nil {
			v.logger.Errorf("[BlockValidation] failed to close kafka consumer gracefully: %v", err)
		}
	}

	return nil
}

// ValidateTransaction implements the gRPC endpoint for validating a single Bitcoin transaction.
// This method is part of the validator_api.ValidatorAPIServer interface and serves as the
// public API entry point for transaction validation requests. It delegates the actual
// validation logic to validateTransaction and wraps any returned errors in gRPC format.
//
// Parameters:
//   - ctx: Context for the validation operation, used for cancellation and tracing
//   - req: ValidateTransactionRequest containing the transaction data and validation options
//
// Returns:
//   - *validator_api.ValidateTransactionResponse: Validation results including success status
//   - error: Any validation errors wrapped appropriately for gRPC transmission
func (v *Server) ValidateTransaction(ctx context.Context, req *validator_api.ValidateTransactionRequest) (*validator_api.ValidateTransactionResponse, error) {
	response, err := v.validateTransaction(ctx, req)
	return response, errors.WrapGRPC(err)
}

// validateTransaction performs the internal validation logic for a single transaction.
// This method handles the core transaction validation workflow, including performance
// monitoring, transaction parsing, and interaction with the validator component.
// It supports validation with configurable options and handles various error conditions.
//
// Parameters:
//   - ctx: Context for the validation operation, used for tracing and cancellation
//   - req: ValidateTransactionRequest containing transaction data and validation options
//
// Returns:
//   - *validator_api.ValidateTransactionResponse: Validation results with status and details
//   - error: Detailed validation error if validation fails, nil on success
func (v *Server) validateTransaction(ctx context.Context, req *validator_api.ValidateTransactionRequest) (*validator_api.ValidateTransactionResponse, error) {
	ctx, _, deferFn := tracing.Tracer("validator").Start(ctx, "ValidateTransaction",
		tracing.WithParentStat(v.stats),
		tracing.WithHistogram(prometheusValidateTransaction),
		tracing.WithDebugLogMessage(v.logger, "[ValidateTransaction] called"),
	)
	defer deferFn()

	transactionData := req.GetTransactionData()

	tx, err := bt.NewTxFromBytes(transactionData)
	if err != nil {
		prometheusInvalidTransactions.Inc()

		return &validator_api.ValidateTransactionResponse{
			Valid: false,
		}, errors.NewTxError("error reading transaction data", err)
	}

	// set the tx hash, so it doesn't have to be recalculated
	tx.SetTxHash(tx.TxIDChainHash())

	validationOptions := NewDefaultOptions()
	if req.SkipUtxoCreation != nil {
		validationOptions.SkipUtxoCreation = *req.SkipUtxoCreation
	}

	if req.AddTxToBlockAssembly != nil {
		validationOptions.AddTXToBlockAssembly = *req.AddTxToBlockAssembly
	}

	if req.SkipPolicyChecks != nil {
		validationOptions.SkipPolicyChecks = *req.SkipPolicyChecks
	}

	if req.CreateConflicting != nil {
		validationOptions.CreateConflicting = *req.CreateConflicting
	}

	txMetaData, err := v.validator.ValidateWithOptions(ctx, tx, req.BlockHeight, validationOptions)
	if err != nil {
		prometheusInvalidTransactions.Inc()

		return &validator_api.ValidateTransactionResponse{
			Valid: false,
			Txid:  tx.TxIDChainHash().CloneBytes(),
		}, err
	}

	prometheusTransactionSize.Observe(float64(len(transactionData)))

	txMetaBytes, err := txMetaData.Bytes()
	if err != nil {
		return &validator_api.ValidateTransactionResponse{
			Valid: false,
			Txid:  tx.TxIDChainHash().CloneBytes(),
		}, errors.NewProcessingError("failed to serialize transaction metadata", err)
	}

	return &validator_api.ValidateTransactionResponse{
		Valid:    true,
		Txid:     tx.TxIDChainHash().CloneBytes(),
		Metadata: txMetaBytes,
	}, nil
}

// ValidateTransactionBatch implements the gRPC endpoint for batch validation of multiple Bitcoin transactions.
// This method provides significant performance optimization over individual validation by processing
// multiple transactions in parallel using Go's errgroup. It maintains transaction order in the
// response to match the request order, allowing clients to correlate results with submissions.
//
// The method creates a goroutine for each transaction to be validated, collecting metadata and
// error information for each. Unlike single transaction validation, this method always returns
// a success status at the batch level - individual transaction validation results are included
// in the response arrays.
//
// Parameters:
//   - ctx: Context for the batch validation operation, used for cancellation and tracing
//   - req: ValidateTransactionBatchRequest containing multiple transactions to validate
//
// Returns:
//   - *validator_api.ValidateTransactionBatchResponse: Batch validation results including:
//   - Individual transaction metadata
//   - Error details for each transaction
//   - error: Batch-level errors (rare, typically nil as transaction-specific errors are in the response)
func (v *Server) ValidateTransactionBatch(ctx context.Context, req *validator_api.ValidateTransactionBatchRequest) (*validator_api.ValidateTransactionBatchResponse, error) {
	ctx, _, deferFn := tracing.Tracer("validator").Start(ctx, "ValidateTransactionBatch",
		tracing.WithParentStat(v.stats),
		tracing.WithHistogram(prometheusTransactionValidateBatch),
		tracing.WithDebugLogMessage(v.logger, "[ValidateTransactionBatch] called for %d transactions", len(req.GetTransactions())),
	)
	defer deferFn()

	g, gCtx := errgroup.WithContext(ctx)

	// we create a slice for all transactions we just batched, in the same order as we got them
	metaData := make([][]byte, len(req.GetTransactions()))
	errReasons := make([]*errors.TError, len(req.GetTransactions()))

	for idx, reqItem := range req.GetTransactions() {
		idx, reqItem := idx, reqItem

		g.Go(func() error {
			validatorResponse, err := v.validateTransaction(gCtx, reqItem)
			metaData[idx] = validatorResponse.Metadata
			errReasons[idx] = errors.Wrap(err)

			return nil
		})
	}

	// wait for all transactions to be validated, never returns error
	_ = g.Wait()

	return &validator_api.ValidateTransactionBatchResponse{
		Valid:    true,
		Errors:   errReasons,
		Metadata: metaData,
	}, nil
}

// GetBlockHeight implements the gRPC endpoint for retrieving the current blockchain height.
// This method provides a critical service for clients needing to know the current chain state,
// which is essential for transaction validation, block template generation, and determining
// transaction finality. The block height is retrieved from the validator component, which
// maintains synchronization with the blockchain service.
//
// If the block height cannot be retrieved or is zero (indicating potential initialization issues),
// the method returns an appropriate error. This helps prevent validation against an outdated
// or uninitialized chain state.
//
// Parameters:
//   - ctx: Context for the operation, used for tracing and cancellation
//   - _: Empty message parameter (unused), required by the gRPC interface contract
//
// Returns:
//   - *validator_api.GetBlockHeightResponse: Response containing the current block height
//   - error: Returns Internal error if block height is 0 or cannot be retrieved
func (v *Server) GetBlockHeight(ctx context.Context, _ *validator_api.EmptyMessage) (*validator_api.GetBlockHeightResponse, error) {
	_, _, deferFn := tracing.Tracer("validator").Start(ctx, "GetBlockHeight",
		tracing.WithParentStat(v.stats),
		tracing.WithDebugLogMessage(v.logger, "[GetBlockHeight] called"),
	)
	defer deferFn()

	blockHeight := v.validator.GetBlockHeight()
	if blockHeight == 0 {
		return nil, status.Errorf(codes.Internal, "cannot get block height: %d", blockHeight)
	}

	return &validator_api.GetBlockHeightResponse{
		Height: blockHeight,
	}, nil
}

// GetMedianBlockTime implements the gRPC endpoint for retrieving the median time of recent blocks.
// This method provides access to the median timestamp of the last several blocks, which is
// a critical value for time-based transaction features like nLockTime. Using median time
// instead of current time helps prevent timestamp manipulation and provides a more stable
// reference for time-based script conditions.
//
// The median time is calculated based on the timestamps of the last 11 blocks (Bitcoin consensus
// rule). If the median time cannot be retrieved or is zero (indicating potential initialization
// issues), the method returns an appropriate error.
//
// Parameters:
//   - ctx: Context for the operation, used for tracing and cancellation
//   - _: Empty message parameter (unused), required by the gRPC interface contract
//
// Returns:
//   - *validator_api.GetMedianBlockTimeResponse: Response containing the median block time as Unix timestamp
//   - error: Returns Internal error if median time is 0 or cannot be retrieved
func (v *Server) GetMedianBlockTime(ctx context.Context, _ *validator_api.EmptyMessage) (*validator_api.GetMedianBlockTimeResponse, error) {
	_, _, deferFn := tracing.Tracer("validator").Start(ctx, "GetMedianBlockTime",
		tracing.WithParentStat(v.stats),
		tracing.WithDebugLogMessage(v.logger, "[GetMedianBlockTime] called"),
	)
	defer deferFn()

	medianTime := v.validator.GetMedianBlockTime()
	if medianTime == 0 {
		return nil, status.Errorf(codes.Internal, "cannot get median block time: %d", medianTime)
	}

	return &validator_api.GetMedianBlockTimeResponse{
		MedianTime: medianTime,
	}, nil
}

// extractValidationParams extracts validation parameters from HTTP query string parameters.
// This utility function parses and converts various query parameters into validation options
// for transaction processing. It handles both numeric parameters (like blockHeight) and
// boolean flags (like skipUtxoCreation, addTxToBlockAssembly) that control validation behavior.
//
// The function recognizes boolean values as either 'true' or '1' strings in the query parameters.
// If parameters are not provided or cannot be parsed, default values are used.
//
// Parameters:
//   - c: Echo context containing the HTTP request and query parameters
//
// Returns:
//   - uint32: Block height to use for validation context (0 if not specified or invalid)
//   - *Options: Configured validation options with all extracted parameters applied
func extractValidationParams(c echo.Context) (uint32, *Options) {
	const trueString = "true"

	var (
		blockHeight uint32
	)

	options := NewDefaultOptions()

	// Extract block height
	if blockHeightStr := c.QueryParam("blockHeight"); blockHeightStr != "" {
		height, err := strconv.ParseUint(blockHeightStr, 10, 32)
		if err == nil {
			blockHeight = uint32(height)
		}
	}

	// Extract boolean parameters
	if skipUtxoCreationStr := c.QueryParam("skipUtxoCreation"); skipUtxoCreationStr != "" {
		boolVal := skipUtxoCreationStr == trueString || skipUtxoCreationStr == "1"
		options.SkipUtxoCreation = boolVal
	}

	if addTxToBlockAssemblyStr := c.QueryParam("addTxToBlockAssembly"); addTxToBlockAssemblyStr != "" {
		boolVal := addTxToBlockAssemblyStr == trueString || addTxToBlockAssemblyStr == "1"
		options.AddTXToBlockAssembly = boolVal
	}

	if skipPolicyChecksStr := c.QueryParam("skipPolicyChecks"); skipPolicyChecksStr != "" {
		boolVal := skipPolicyChecksStr == trueString || skipPolicyChecksStr == "1"
		options.SkipPolicyChecks = boolVal
	}

	if createConflictingStr := c.QueryParam("createConflicting"); createConflictingStr != "" {
		boolVal := createConflictingStr == trueString || createConflictingStr == "1"
		options.CreateConflicting = boolVal
	}

	return blockHeight, options
}

// handleSingleTx handles a single transaction request on the /tx endpoint.
// This method implements an HTTP handler for validating a single Bitcoin transaction
// submitted via POST request. It reads the raw transaction bytes from the request body,
// extracts validation parameters from the query string, and delegates to the core
// validation logic via validateTransaction.
//
// The handler supports several validation options through query parameters:
// - blockHeight: The blockchain height to validate against
// - skipUtxoCreation: Whether to skip UTXO creation (useful for testing/dry-runs)
// - addTxToBlockAssembly: Whether to include the transaction in block templates
// - skipPolicyChecks: Whether to skip non-consensus policy validation checks
// - createConflicting: Whether to allow creating conflicting UTXOs
//
// Parameters:
//   - ctx: Context for the handler operation, passed through to validation
//
// Returns:
//   - echo.HandlerFunc: HTTP handler function that processes transaction validation requests
//     and returns appropriate status codes and responses:
//   - 200 OK: Transaction is valid
//   - 400 Bad Request: Invalid request body
//   - 500 Internal Server Error: Validation failed with specific reason
func (v *Server) handleSingleTx(ctx context.Context) echo.HandlerFunc {
	return func(c echo.Context) error {
		body, err := io.ReadAll(c.Request().Body)
		if err != nil {
			return c.String(http.StatusBadRequest, "[handleSingleTx] Invalid request body")
		}

		// Extract validation parameters from query string
		blockHeight, options := extractValidationParams(c)

		// Create the request with transaction data and parameters
		req := &validator_api.ValidateTransactionRequest{
			TransactionData:      body,
			BlockHeight:          blockHeight,
			SkipUtxoCreation:     &options.SkipUtxoCreation,
			AddTxToBlockAssembly: &options.AddTXToBlockAssembly,
			SkipPolicyChecks:     &options.SkipPolicyChecks,
			CreateConflicting:    &options.CreateConflicting,
		}

		// Process the transaction and return appropriate response
		response, err := v.validateTransaction(ctx, req)
		if err != nil {
			return c.String(http.StatusInternalServerError, "[handleSingleTx] Failed to process transaction: "+err.Error())
		}

		if !response.Valid {
			return c.String(http.StatusInternalServerError, "[handleSingleTx] Failed to process transaction: "+response.Reason)
		}

		return c.String(http.StatusOK, "OK")
	}
}

// handleMultipleTx handles multiple transactions on the /txs endpoint.
// This method implements an HTTP handler for validating a stream of Bitcoin transactions
// submitted via POST request. It reads transactions sequentially from the request body
// until EOF, validating each one in order. This allows clients to submit transaction
// batches in a single HTTP request for more efficient processing.
//
// The handler supports proper transaction ordering by processing transactions in sequence,
// which is important when later transactions depend on outputs from earlier ones in the
// same batch. It uses the same validation options as handleSingleTx, supporting the
// same query parameters.
//
// Unlike the gRPC batch validation endpoint, this HTTP handler processes transactions
// sequentially and will stop at the first validation failure, returning an error response.
// This fail-fast behavior helps identify issues in transaction chains quickly.
//
// Parameters:
//   - ctx: Context for the handler operation, passed through to validation
//
// Returns:
//   - echo.HandlerFunc: HTTP handler function that processes transaction validation requests
//     and returns appropriate status codes and responses:
//   - 200 OK: All transactions are valid
//   - 400 Bad Request: Invalid request body or transaction format
//   - 500 Internal Server Error: Validation failed with specific reason
func (v *Server) handleMultipleTx(ctx context.Context) echo.HandlerFunc {
	return func(c echo.Context) error {
		// Extract validation parameters from query string
		blockHeight, options := extractValidationParams(c)

		// Read transactions with the bt reader in a loop
		for {
			tx := &bt.Tx{}

			// Read transaction from request body
			_, err := tx.ReadFrom(c.Request().Body)
			if err != nil {
				// End of stream is expected and not an error
				if err == io.EOF {
					break
				}

				return c.String(http.StatusBadRequest, "[handleMultipleTx] Invalid request body: "+err.Error())
			}

			// Process the transaction
			req := &validator_api.ValidateTransactionRequest{
				TransactionData:      tx.SerializeBytes(),
				BlockHeight:          blockHeight,
				SkipUtxoCreation:     &options.SkipUtxoCreation,
				AddTxToBlockAssembly: &options.AddTXToBlockAssembly,
				SkipPolicyChecks:     &options.SkipPolicyChecks,
				CreateConflicting:    &options.CreateConflicting,
			}

			response, err := v.validateTransaction(ctx, req)
			if err != nil {
				return c.String(http.StatusInternalServerError, "[handleMultipleTx] Failed to process transaction: "+err.Error())
			}

			if !response.Valid {
				return c.String(http.StatusInternalServerError, "[handleMultipleTx] Failed to process transaction: "+response.Reason)
			}
		}

		return c.String(http.StatusOK, "OK")
	}
}

// startHTTPServer initializes and starts the HTTP server for transaction processing.
// This method configures and launches an Echo web server that provides HTTP REST endpoints
// for transaction validation. The server supports both single transaction validation and
// batch transaction processing through dedicated endpoints, with configurable rate limiting
// and connection timeouts for production reliability.
//
// The server implements several endpoints:
// - POST /tx: Single transaction validation endpoint
// - POST /txs: Batch transaction validation endpoint
// - GET /health: Simple health check endpoint
// - Any other path: Returns 404 Not Found
//
// Rate limiting and timeout configuration is applied based on the validator settings,
// allowing customization for different deployment scenarios. The actual server start
// operation is delegated to startAndMonitorHTTPServer for background operation.
//
// Parameters:
//   - ctx: Context for the server operation, used for cancellation and lifecycle management
//   - httpAddresses: Comma-separated list of address:port combinations to listen on
//
// Returns:
//   - error: Any server initialization errors, nil on success
func (v *Server) startHTTPServer(ctx context.Context, httpAddresses string) error {
	// Initialize Echo server with settings
	v.httpServer = echo.New()
	v.httpServer.Debug = false
	v.httpServer.HideBanner = true

	// Configure middleware and timeouts
	v.httpServer.Use(middleware.RateLimiter(middleware.NewRateLimiterMemoryStore(rate.Limit(v.settings.Validator.HTTPRateLimit))))
	v.httpServer.Server.ReadTimeout = 5 * time.Second
	v.httpServer.Server.WriteTimeout = 10 * time.Second
	v.httpServer.Server.IdleTimeout = 120 * time.Second

	// Register route handlers
	v.httpServer.POST("/tx", v.handleSingleTx(ctx))
	v.httpServer.POST("/txs", v.handleMultipleTx(ctx))

	// add a health endpoint that simply returns "OK"
	v.httpServer.GET("/health", func(c echo.Context) error {
		return c.String(http.StatusOK, "OK")
	})

	// add a 404 handler with a message for unknown routes
	v.httpServer.Any("/*", func(c echo.Context) error {
		return c.String(http.StatusNotFound, "Unknown route")
	})

	// Start server and handle shutdown
	v.startAndMonitorHTTPServer(ctx, httpAddresses)

	return nil
}

// startAndMonitorHTTPServer starts the HTTP server and monitors for shutdown.
// This method launches the HTTP server in a non-blocking manner using goroutines,
// allowing the main server thread to continue execution. It implements a graceful
// shutdown mechanism that monitors the provided context for cancellation signals
// and performs proper cleanup when shutdown is requested.
//
// The implementation uses two separate goroutines:
// 1. The first goroutine starts the HTTP server and handles any startup errors
// 2. The second goroutine monitors the context for cancellation and triggers shutdown
//
// This approach ensures that the validator service remains responsive during startup
// and can handle both normal shutdown scenarios and error conditions properly.
//
// Parameters:
//   - ctx: Context for server lifecycle management, cancellation signals shutdown
//   - httpAddresses: Comma-separated list of address:port combinations to listen on
func (v *Server) startAndMonitorHTTPServer(ctx context.Context, httpAddresses string) {
	// Get listener using util.GetListener
	listener, address, _, err := util.GetListener(v.settings.Context, "validator", "http://", httpAddresses)
	if err != nil {
		v.logger.Errorf("failed to get listener: %v", err)
		return
	}

	v.logger.Infof("[Validator] HTTP server listening on %s", address)
	v.httpServer.Listener = listener

	// Start the server
	go func() {
		if err := v.httpServer.Server.Serve(listener); err != nil {
			if err == http.ErrServerClosed {
				v.logger.Infof("http server shutdown")
			} else {
				v.logger.Errorf("failed to start http server: %v", err)
			}
		}
		// Clean up the listener when server stops
		util.RemoveListener(v.settings.Context, "validator", "http://")
	}()

	// Monitor for context cancellation
	go func() {
		<-ctx.Done()

		_ = v.httpServer.Shutdown(context.Background())
	}()
}
