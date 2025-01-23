/*
Package validator implements Bitcoin SV transaction validation functionality.

This file implements the validator server component, providing gRPC endpoints
for transaction validation services and managing the interaction between
different validation components.
*/
package validator

import (
	"bytes"
	"context"
	"io"
	"log"
	"net/http"
	"os"
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
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
func (v *Server) Start(ctx context.Context) error {
	kafkaMessageHandler := func(msg *kafka.KafkaMessage) error {
		// currentState, err := v.blockchainClient.GetFSMCurrentState(ctx)
		// if err != nil {
		// 	v.logger.Errorf("[Validator] Failed to get current state: %s", err)
		// 	return err
		// }
		// for currentState != nil && *currentState == blockchain.FSMStateCATCHINGTXS {
		// 	v.logger.Debugf("[Validator] Waiting for FSM to finish catching txs")
		// 	time.Sleep(1 * time.Second) // Wait and check again in 1 second
		// 	currentState, err = v.blockchainClient.GetFSMCurrentState(ctx)
		// 	if err != nil {
		// 		v.logger.Errorf("[Validator] Failed to get current state: %s", err)
		// 		return err
		// 	}
		// }
		data, err := NewTxValidationDataFromBytes(msg.Value)
		if err != nil {
			prometheusInvalidTransactions.Inc()
			v.logger.Errorf("[Validator] Failed to decode kafka message: %s", err)

			return err
		}

		tx, err := bt.NewTxFromBytes(data.Tx)
		if err != nil {
			prometheusInvalidTransactions.Inc()
			v.logger.Errorf("[Validator] failed to parse transaction from bytes: %w", err)

			return err
		}

		// should not pass in a height when validating from Kafka, should just be current utxo store height
		if _, err = v.validator.ValidateWithOptions(ctx, tx, 0, data.Options); err != nil {
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

// ValidateTransactionStream implements streaming transaction validation
// Parameters:
//   - stream: gRPC stream for transaction data
//
// Returns:
//   - error: Any validation errors
func (v *Server) ValidateTransactionStream(stream validator_api.ValidatorAPI_ValidateTransactionStreamServer) error {
	_, _, deferFn := tracing.StartTracing(v.ctx, "ValidateTransactionStream",
		tracing.WithParentStat(v.stats),
		tracing.WithHistogram(prometheusValidateTransaction),
	)
	defer deferFn()

	transactionData := bytes.Buffer{}

	for {
		log.Print("waiting to receive more data")

		req, err := stream.Recv()
		if err == io.EOF {
			log.Print("no more data")
			break
		}

		if err != nil {
			prometheusInvalidTransactions.Inc()
			return status.Errorf(codes.Unknown, "cannot receive chunk data: %v", err)
		}

		chunk := req.GetTransactionData()

		_, err = transactionData.Write(chunk)
		if err != nil {
			prometheusInvalidTransactions.Inc()
			return status.Errorf(codes.Internal, "cannot write chunk data: %v", err)
		}
	}

	var tx bt.Tx
	if _, err := tx.ReadFrom(bytes.NewReader(transactionData.Bytes())); err != nil {
		prometheusInvalidTransactions.Inc()
		return status.Errorf(codes.Internal, "cannot read transaction data: %v", err)
	}

	return stream.SendAndClose(&validator_api.ValidateTransactionResponse{
		Valid: true,
	})
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
		}, status.Errorf(codes.Internal, "cannot read transaction data: %v", err)
	}

	// set the tx hash, so it doesn't have to be recalculated
	tx.SetTxHash(tx.TxIDChainHash())

	validationOptions := NewDefaultOptions()
	if req.SkipUtxoCreation != nil {
		validationOptions.skipUtxoCreation = *req.SkipUtxoCreation
	}

	if req.AddTxToBlockAssembly != nil {
		validationOptions.addTXToBlockAssembly = *req.AddTxToBlockAssembly
	}

	if req.SkipPolicyChecks != nil {
		validationOptions.skipPolicyChecks = *req.SkipPolicyChecks
	}

	if req.CreateConflicting != nil {
		validationOptions.createConflicting = *req.CreateConflicting
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
	errReasons := make([]string, len(req.GetTransactions()))
	metaData := make([][]byte, len(req.GetTransactions()))

	for idx, reqItem := range req.GetTransactions() {
		idx, reqItem := idx, reqItem

		g.Go(func() error {
			validatorResponse, err := v.ValidateTransaction(gCtx, reqItem)
			if err != nil {
				errReasons[idx] = err.Error()
			} else {
				errReasons[idx] = ""
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
