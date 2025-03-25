/*
Package validator implements Bitcoin SV transaction validation functionality.

This file implements the validator server component, providing gRPC endpoints
for transaction validation services and managing the interaction between
different validation components.
*/
package validator

import (
	"context"
	"net/http"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/validator/validator_api"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/health"
	"github.com/bitcoin-sv/teranode/util/kafka"
	kafkamessage "github.com/bitcoin-sv/teranode/util/kafka/kafka_message"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Server implements the validator gRPC service and manages validation operations
type Server struct {
	validator_api.UnsafeValidatorAPIServer
	validator                     Interface
	logger                        ulogger.Logger
	settings                      *settings.Settings
	utxoStore                     utxo.Store
	kafkaSignal                   chan os.Signal
	stats                         *gocore.Stat
	ctx                           context.Context
	blockchainClient              blockchain.ClientI
	consumerClient                kafka.KafkaConsumerGroupI
	txMetaKafkaProducerClient     kafka.KafkaAsyncProducerI
	rejectedTxKafkaProducerClient kafka.KafkaAsyncProducerI
}

// NewServer creates and initializes a new validator server instance
// Parameters:
//   - logger: Logger instance for server operations
//   - utxoStore: UTXO database interface
//   - blockchainClient: Interface to blockchain operations
//   - consumerClient: Kafka consumer client
//   - txMetaKafkaProducerClient: Kafka producer for transaction metadata
//   - rejectedTxKafkaProducerClient: Kafka producer for rejected transactions
//
// Returns:
//   - *Server: Initialized server instance
func NewServer(logger ulogger.Logger, tSettings *settings.Settings, utxoStore utxo.Store, blockchainClient blockchain.ClientI, consumerClient kafka.KafkaConsumerGroupI, txMetaKafkaProducerClient kafka.KafkaAsyncProducerI, rejectedTxKafkaProducerClient kafka.KafkaAsyncProducerI) *Server {
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
	}
}

// Health performs health checks on the validator service
// Parameters:
//   - ctx: Context for the health check operation
//   - checkLiveness: If true, performs only liveness checks
//
// Returns:
//   - int: HTTP status code indicating health status
//   - string: Detailed health status message
//   - error: Any errors encountered during health check
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
	checks := make([]health.Check, 0, 5)
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

// HealthGRPC implements the gRPC health check endpoint
// Parameters:
//   - ctx: Context for the health check operation
//   - _: Empty message parameter (unused)
//
// Returns:
//   - *validator_api.HealthResponse: Health check response
//   - error: Any errors encountered
func (v *Server) HealthGRPC(ctx context.Context, _ *validator_api.EmptyMessage) (*validator_api.HealthResponse, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "HealthGRPC",
		tracing.WithParentStat(v.stats),
		tracing.WithCounter(prometheusHealth),
		tracing.WithDebugLogMessage(v.logger, "[HealthGRPC] called"),
	)
	defer deferFn()

	status, details, err := v.Health(ctx, false)

	return &validator_api.HealthResponse{
		Ok:      status == http.StatusOK,
		Details: details,
	}, errors.WrapGRPC(err)
}

// Init initializes the validator server
// Parameters:
//   - ctx: Context for initialization
//
// Returns:
//   - error: Any initialization errors
func (v *Server) Init(ctx context.Context) (err error) {
	v.ctx = ctx

	v.validator, err = New(ctx, v.logger, v.settings, v.utxoStore, v.txMetaKafkaProducerClient, v.rejectedTxKafkaProducerClient)
	if err != nil {
		return errors.NewServiceError("could not create validator", err)
	}

	return nil
}

// Start begins the validator server operation
// Parameters:
//   - ctx: Context for server operation
//
// Returns:
//   - error: Any startup errors
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
			v.logger.Errorf("[Validator] failed to parse transaction from bytes: %w", err)

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
		v.consumerClient.Start(ctx, kafkaMessageHandler, kafka.WithRetryAndMoveOn(0, 1, time.Second))
	}

	//  Start gRPC server - this will block
	if err := util.StartGRPCServer(ctx, v.logger, v.settings, "validator", v.settings.Validator.GRPCListenAddress, func(server *grpc.Server) {
		validator_api.RegisterValidatorAPIServer(server, v)
		closeOnce.Do(func() { close(readyCh) })
	}); err != nil {
		return err
	}

	return nil
}

// Stop gracefully shuts down the validator server
// Parameters:
//   - ctx: Context for shutdown operation
//
// Returns:
//   - error: Any shutdown errors
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

func (v *Server) ValidateTransaction(ctx context.Context, req *validator_api.ValidateTransactionRequest) (*validator_api.ValidateTransactionResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "ValidateTransaction",
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
		}, errors.WrapGRPC(errors.NewTxError("error reading transaction data", err))
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
		}, errors.WrapGRPC(err)
	}

	prometheusTransactionSize.Observe(float64(len(transactionData)))

	return &validator_api.ValidateTransactionResponse{
		Valid:    true,
		Txid:     tx.TxIDChainHash().CloneBytes(),
		Metadata: txMetaData.Bytes(),
	}, nil
}

func (v *Server) ValidateTransactionBatch(ctx context.Context, req *validator_api.ValidateTransactionBatchRequest) (*validator_api.ValidateTransactionBatchResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "ValidateTransactionBatch",
		tracing.WithParentStat(v.stats),
		tracing.WithHistogram(prometheusTransactionValidateBatch),
		tracing.WithDebugLogMessage(v.logger, "[ValidateTransactionBatch] called for %d transactions", len(req.GetTransactions())),
	)
	defer deferFn()

	g, gCtx := errgroup.WithContext(ctx)

	// we create a slice for all transactions we just batched, in the same order as we got them
	errReasons := make([]*errors.TError, len(req.GetTransactions()))
	metaData := make([][]byte, len(req.GetTransactions()))

	for idx, reqItem := range req.GetTransactions() {
		idx, reqItem := idx, reqItem

		g.Go(func() error {
			validatorResponse, err := v.ValidateTransaction(gCtx, reqItem)
			if err != nil {
				errReasons[idx] = errors.Wrap(err)
			} else {
				errReasons[idx] = nil
			}

			if validatorResponse.Metadata != nil {
				metaData[idx] = validatorResponse.Metadata
			}

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

func (v *Server) GetBlockHeight(ctx context.Context, _ *validator_api.EmptyMessage) (*validator_api.GetBlockHeightResponse, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "GetBlockHeight",
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

func (v *Server) GetMedianBlockTime(ctx context.Context, _ *validator_api.EmptyMessage) (*validator_api.GetMedianBlockTimeResponse, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "GetMedianBlockTime",
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
