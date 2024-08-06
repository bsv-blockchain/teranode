package validator

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"syscall"
	"time"

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
	validator   Interface
	logger      ulogger.Logger
	utxoStore   utxo.Store
	kafkaSignal chan os.Signal
	stats       *gocore.Stat
	ctx         context.Context
}

// NewServer will return a server instance with the logger stored within it
func NewServer(logger ulogger.Logger, utxoStore utxo.Store) *Server {
	initPrometheusMetrics()

	return &Server{
		logger:    logger,
		utxoStore: utxoStore,
		stats:     gocore.NewStat("validator"),
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

func (v *Server) HealthGRPC(_ context.Context, _ *validator_api.EmptyMessage) (*validator_api.HealthResponse, error) {
	start := gocore.CurrentTime()
	defer func() {
		prometheusHealth.Inc()
		v.stats.NewStat("Health", true).AddTime(start)
	}()

	var sb strings.Builder
	errs := make([]error, 0)

	blockHeight, err := v.validator.GetBlockHeight()
	if err != nil {
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
		}, err
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

	// v.logger.Debugf("[ValidateTransactions][%s] validating transaction (pq:)", tx.TxID())
	err = v.validator.Validate(ctx, tx, req.BlockHeight)
	if err != nil {
		prometheusInvalidTransactions.Inc()
		return &validator_api.ValidateTransactionResponse{
			Valid: false,
		}, status.Errorf(codes.Internal, "transaction %s is invalid: %v", tx.TxID(), err)
	}

	prometheusTransactionSize.Observe(float64(len(transactionData)))

	return &validator_api.ValidateTransactionResponse{
		Valid: true,
	}, nil
}

func (v *Server) ValidateTransactionBatch(ctx context.Context, req *validator_api.ValidateTransactionBatchRequest) (*validator_api.ValidateTransactionBatchResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "ValidateTransactionBatch",
		tracing.WithParentStat(v.stats),
		tracing.WithHistogram(prometheusTransactionValidateBatch),
		tracing.WithDebugLogMessage(v.logger, "[ValidateTransactionBatch] called for %d transactions", len(req.GetTransactions())),
	)
	defer deferFn()

	errReasons := make([]*validator_api.ValidateTransactionError, 0, len(req.GetTransactions()))
	for _, reqItem := range req.GetTransactions() {
		tx, err := v.ValidateTransaction(ctx, reqItem)
		if err != nil {
			if tx != nil {
				errReasons = append(errReasons, &validator_api.ValidateTransactionError{
					TxId:   tx.String(),
					Reason: tx.Reason,
				})
			}
		}
	}

	return &validator_api.ValidateTransactionBatchResponse{
		Valid:   true,
		Reasons: errReasons,
	}, nil
}

func (v *Server) GetBlockHeight(ctx context.Context, _ *validator_api.EmptyMessage) (*validator_api.GetBlockHeightResponse, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "GetBlockHeight",
		tracing.WithParentStat(v.stats),
		tracing.WithDebugLogMessage(v.logger, "[GetBlockHeight] called"),
	)
	defer deferFn()

	blockHeight, err := v.validator.GetBlockHeight()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cannot get block height: %v", err)
	}

	return &validator_api.GetBlockHeightResponse{
		Height: blockHeight,
	}, nil
}
