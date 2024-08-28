package validator

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	"golang.org/x/sync/errgroup"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/validator/validator_api"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/gocore"
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
	return 0, "", nil
}

func (v *Server) Init(ctx context.Context) (err error) {
	v.ctx = ctx

	v.validator, err = New(ctx, v.logger, v.utxoStore)
	if err != nil {
		return errors.NewServiceError("could not create validator", err)
	}

	return nil
}

// Start function
func (v *Server) Start(ctx context.Context) error {

	kafkaURL, err, ok := gocore.Config().GetURL("kafka_validatortxsConfig")
	if err == nil && ok {
		v.logger.Debugf("[Validator] Kafka listener starting in URL: %s", kafkaURL.String())
		go v.startKafkaListener(ctx, kafkaURL)
	}

	// this will block
	if err := util.StartGRPCServer(ctx, v.logger, "validator", func(server *grpc.Server) {
		validator_api.RegisterValidatorAPIServer(server, v)
	}); err != nil {
		return err
	}

	return nil
}

func (v *Server) startKafkaListener(ctx context.Context, kafkaURL *url.URL) {
	workers, _ := gocore.Config().GetInt("validator_kafkaWorkers", 100)
	if workers < 1 {
		// no workers, nothing to do
		return
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

	if err := util.StartKafkaGroupListener(ctx, v.logger, kafkaURL, "blockassembly", nil, consumerCount, true, func(msg util.KafkaMessage) error {
		//startTime := time.Now()
		currentState, err := v.blockchainClient.GetFSMCurrentState(ctx)
		if err != nil {
			v.logger.Errorf("[BlockAssembly] Failed to get current state: %s", err)
			// TODO: how to handle it gracefully?
		}
		for *currentState == blockchain_api.FSMStateType_CATCHINGTXS {
			v.logger.Debugf("[BlockAssembly] Waiting for FSM to finish catching txs")
			time.Sleep(1 * time.Second) // Wait and check again in 1 second
		}

		data, err := NewTxValidationDataFromBytes(msg.Message.Value)
		if err != nil {
			prometheusInvalidTransactions.Inc()
			v.logger.Errorf("[BlockAssembly] Failed to decode kafka message: %s", err)
			return err
		}

		tx, err := bt.NewTxFromBytes(data.Tx)
		if err != nil {
			prometheusInvalidTransactions.Inc()
			v.logger.Errorf("[ProcessTransaction] failed to parse transaction from bytes: %w", err)
			return err
		}

		if err = v.validator.Validate(ctx, tx, uint32(data.Height)); err != nil {
			prometheusInvalidTransactions.Inc()
			v.logger.Errorf("[BlockAssembly] Invalid tx: %s", err)
			return err
		}

		//prometheusProcessedTransactions.Inc()
		//prometheusTransactionDuration.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
		return nil
	}); err != nil {
		v.logger.Errorf("[BlockAssembly] failed to start Kafka listener: %s", err)
	}
}

func (v *Server) Stop(_ context.Context) error {
	if v.kafkaSignal != nil {
		v.kafkaSignal <- syscall.SIGTERM
	}

	return nil
}

func (v *Server) HealthGRPC(_ context.Context, _ *validator_api.EmptyMessage) (*validator_api.HealthResponse, error) {
	start := gocore.CurrentTime()
	defer func() {
		prometheusHealth.Inc()
		v.stats.NewStat("Health", true).AddTime(start)
	}()

	var sb strings.Builder
	errs := make([]error, 0)

	blockHeight := v.validator.GetBlockHeight()
	if blockHeight == 0 {
		err := errors.NewProcessingError("error getting blockHeight from validator: 0")
		errs = append(errs, err)
		_, _ = sb.WriteString(fmt.Sprintf("BlockHeight: BAD: %v\n", err))
	} else {
		_, _ = sb.WriteString(fmt.Sprintf("BlockHeight: GOOD: %d\n", blockHeight))
	}

	if blockHeight <= 0 {
		errs = append(errs, errors.NewProcessingError("blockHeight <= 0"))
		_, _ = sb.WriteString(fmt.Sprintf("BlockHeight: BAD: %d\n", blockHeight))
	} else {
		_, _ = sb.WriteString(fmt.Sprintf("BlockHeight: GOOD: %d\n", blockHeight))
	}

	if len(errs) > 0 {
		return &validator_api.HealthResponse{
			Ok:        false,
			Details:   sb.String(),
			Timestamp: uint32(time.Now().Unix()),
		}, errs[0]
	}

	return &validator_api.HealthResponse{
		Ok:        true,
		Timestamp: uint32(time.Now().Unix()),
	}, nil
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
