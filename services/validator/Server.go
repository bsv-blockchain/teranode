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

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/validator/validator_api"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/health"
	"github.com/bitcoin-sv/ubsv/util/kafka"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server type carries the logger within it
type Server struct {
	validator_api.UnsafeValidatorAPIServer
	validator        Interface
	logger           ulogger.Logger
	utxoStore        utxo.Store
	kafkaSignal      chan os.Signal
	stats            *gocore.Stat
	ctx              context.Context
	blockchainClient blockchain.ClientI
	consumerClient   *kafka.KafkaConsumerGroup
}

// NewServer will return a server instance with the logger stored within it
func NewServer(logger ulogger.Logger, utxoStore utxo.Store, blockchainClient blockchain.ClientI) *Server {
	initPrometheusMetrics()

	return &Server{
		logger:           logger,
		utxoStore:        utxoStore,
		stats:            gocore.NewStat("validator"),
		blockchainClient: blockchainClient,
	}
}

func (v *Server) Health(ctx context.Context) (int, string, error) {
	checks := []health.Check{
		{Name: "BlockchainClient", Check: v.blockchainClient.Health},
		{Name: "UTXOStore", Check: v.utxoStore.Health},
		{Name: "Validator", Check: v.validator.Health},
		{Name: "FSM", Check: blockchain.CheckFSM(v.blockchainClient)},
		{Name: "Kafka", Check: v.consumerClient.CheckKafkaHealth},
	}

	return health.CheckAll(ctx, checks)
}

func (v *Server) HealthGRPC(ctx context.Context, _ *validator_api.EmptyMessage) (*validator_api.HealthResponse, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "HealthGRPC",
		tracing.WithParentStat(v.stats),
		tracing.WithCounter(prometheusHealth),
		tracing.WithLogMessage(v.logger, "[HealthGRPC] called"),
	)
	defer deferFn()

	status, details, err := v.Health(ctx)

	return &validator_api.HealthResponse{
		Ok:      status == http.StatusOK,
		Details: details,
	}, errors.WrapGRPC(err)
}

func (v *Server) Init(ctx context.Context) (err error) {
	v.ctx = ctx

	v.validator, err = New(ctx, v.logger, v.utxoStore)
	if err != nil {
		return errors.NewServiceError("could not create validator", err)
	}

	kafkaURL, err, ok := gocore.Config().GetURL("kafka_validatortxsConfig")
	if err == nil && ok {
		v.logger.Debugf("[Validator] Kafka listener starting in URL: %s", kafkaURL.String())

		workers, _ := gocore.Config().GetInt("validator_kafkaWorkers", 100)
		if workers < 1 {
			// no workers, nothing to do
			return nil
		}
		v.logger.Infof("[Validator Server] starting Kafka listener")

		consumerRatio := util.GetQueryParamInt(kafkaURL, "consumer_ratio", 8)
		if consumerRatio < 1 {
			consumerRatio = 1
		}

		partitions := util.GetQueryParamInt(kafkaURL, "partitions", 1)

		consumerCount := partitions / consumerRatio
		if consumerCount < 0 {
			consumerCount = 1
		}

		v.logger.Infof("[Validator] starting Kafka on address: %s, with %d consumers and %d workers\n", kafkaURL.String(), consumerCount, workers)

		kafkaMessageHandler := func(msg kafka.KafkaMessage) error {
			currentState, err := v.blockchainClient.GetFSMCurrentState(ctx)
			if err != nil {
				v.logger.Errorf("[Validator] Failed to get current state: %s", err)
				return err
			}
			for currentState != nil && *currentState == blockchain.FSMStateCATCHINGTXS {
				v.logger.Debugf("[Validator] Waiting for FSM to finish catching txs")
				time.Sleep(1 * time.Second) // Wait and check again in 1 second
			}

			data, err := NewTxValidationDataFromBytes(msg.Message.Value)
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

			if err = v.validator.Validate(ctx, tx, uint32(data.Height)); err != nil {
				prometheusInvalidTransactions.Inc()
				v.logger.Errorf("[Validator] Invalid tx: %s", err)
				return err
			}

			return nil
		}
		v.consumerClient, err = kafka.NewKafkaGroupListener(ctx, kafka.KafkaListenerConfig{
			Logger:            v.logger,
			URL:               kafkaURL,
			GroupID:           "blockassembly",
			ConsumerCount:     consumerCount,
			AutoCommitEnabled: true,
			ConsumerFn:        kafkaMessageHandler,
		})
		if err != nil {
			return errors.NewConfigurationError("failed to create new Kafka listener for %s: %v", kafkaURL.String(), err)
		}
	}

	return nil
}

// Start function
func (v *Server) Start(ctx context.Context) error {
	// Check if we need to Restore. If so, move FSM to the Restore state
	// Restore will block and wait for RUN event to be manually sent
	// TODO: think if we can automate transition to RUN state after restore is complete.
	fsmStateRestore := gocore.Config().GetBool("fsm_state_restore", false)
	if fsmStateRestore {
		// Send Restore event to FSM
		err := v.blockchainClient.Restore(ctx)
		if err != nil {
			v.logger.Errorf("[Validator] failed to send Restore event [%v], this should not happen, FSM will continue without Restoring", err)
		}

		// Wait for node to finish Restoring.
		// this means FSM got a RUN event and transitioned to RUN state
		// this will block
		v.logger.Infof("[Validator] Node is restoring, waiting for FSM to transition to Running state")
		_ = v.blockchainClient.WaitForFSMtoTransitionToGivenState(ctx, blockchain.FSMStateRUNNING)
		v.logger.Infof("[Validator] Node finished restoring and has transitioned to Running state, continuing to start Transaction Validator service")
	}

	go v.consumerClient.Start(ctx)

	// this will block
	if err := util.StartGRPCServer(ctx, v.logger, "validator", func(server *grpc.Server) {
		validator_api.RegisterValidatorAPIServer(server, v)
	}); err != nil {
		return err
	}

	return nil
}

func (v *Server) Stop(_ context.Context) error {
	if v.kafkaSignal != nil {
		v.kafkaSignal <- syscall.SIGTERM
	}

	return nil
}

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

	err = v.validator.Validate(ctx, tx, req.BlockHeight)
	if err != nil {
		prometheusInvalidTransactions.Inc()

		return &validator_api.ValidateTransactionResponse{
			Valid: false,
			Txid:  tx.TxIDChainHash().CloneBytes(),
		}, status.Errorf(codes.Internal, "transaction %s is invalid: %v", tx.TxID(), err)
	}

	prometheusTransactionSize.Observe(float64(len(transactionData)))

	return &validator_api.ValidateTransactionResponse{
		Valid: true,
		Txid:  tx.TxIDChainHash().CloneBytes(),
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

	for idx, reqItem := range req.GetTransactions() {
		idx, reqItem := idx, reqItem

		g.Go(func() error {
			_, err := v.ValidateTransaction(gCtx, reqItem)
			if err != nil {
				errReasons[idx] = err.Error()
			} else {
				errReasons[idx] = ""
			}

			return nil
		})
	}

	// wait for all transactions to be validated, never returns error
	_ = g.Wait()

	return &validator_api.ValidateTransactionBatchResponse{
		Valid:  true,
		Errors: errReasons,
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
