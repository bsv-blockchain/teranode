package validator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/TAAL-GmbH/ubsv/services/validator/validator_api"
	"github.com/TAAL-GmbH/ubsv/tracing"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Server type carries the logger within it
type Server struct {
	validator_api.UnsafeValidatorAPIServer
	validator  Interface
	logger     utils.Logger
	grpcServer *grpc.Server
}

var (
	prometheusProcessedTransactions prometheus.Counter
	prometheusInvalidTransactions   prometheus.Counter
	prometheusTransactionDuration   prometheus.Histogram
	prometheusTransactionSize       prometheus.Histogram
)

func init() {
	prometheusProcessedTransactions = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "validator_processed_transactions",
			Help: "Number of transactions processed by the validator service",
		},
	)
	prometheusInvalidTransactions = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "validator_invalid_transactions",
			Help: "Number of transactions found invalid by the validator service",
		},
	)
	prometheusTransactionDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name: "validator_transactions_duration",
			Help: "Duration of transaction processing by the validator service",
		},
	)
	prometheusTransactionSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name: "validator_transactions_size",
			Help: "Size of transactions processed by the validator service",
		},
	)
}

func Enabled() bool {
	_, found := gocore.Config().Get("validator_grpcAddress")
	return found
}

// NewServer will return a server instance with the logger stored within it
func NewServer(logger utils.Logger) *Server {
	utxostoreURL, err, found := gocore.Config().GetURL("utxostore")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("no utxostore setting found")
	}

	s, err := NewUTXOStore(logger, utxostoreURL)
	if err != nil {
		panic(err)
	}

	validator := New(s)

	return &Server{
		logger:    logger,
		validator: validator,
	}
}

// Start function
func (v *Server) Start() error {

	address, ok := gocore.Config().Get("validator_grpcAddress")
	if !ok {
		return errors.New("no validator_grpcAddress setting found")
	}

	var err error
	v.grpcServer, err = utils.GetGRPCServer(&utils.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
	})
	if err != nil {
		return fmt.Errorf("could not create GRPC server [%w]", err)
	}

	gocore.SetAddress(address)

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("GRPC server failed to listen [%w]", err)
	}

	validator_api.RegisterValidatorAPIServer(v.grpcServer, v)

	// Register reflection service on gRPC server.
	reflection.Register(v.grpcServer)

	v.logger.Infof("GRPC server listening on %s", address)

	if err = v.grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("GRPC server failed [%w]", err)
	}

	return nil
}

func (v *Server) Stop(ctx context.Context) {
	_, cancel := context.WithCancel(ctx)
	defer cancel()

	v.grpcServer.GracefulStop()
}

func (v *Server) Health(_ context.Context, _ *emptypb.Empty) (*validator_api.HealthResponse, error) {
	return &validator_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (v *Server) ValidateTransactionStream(stream validator_api.ValidatorAPI_ValidateTransactionStreamServer) error {
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
			return v.logError(status.Errorf(codes.Unknown, "cannot receive chunk data: %v", err))
		}

		chunk := req.GetTransactionData()

		_, err = transactionData.Write(chunk)
		if err != nil {
			prometheusInvalidTransactions.Inc()
			return v.logError(status.Errorf(codes.Internal, "cannot write chunk data: %v", err))
		}
	}

	var tx bt.Tx
	if _, err := tx.ReadFrom(bytes.NewReader(transactionData.Bytes())); err != nil {
		prometheusInvalidTransactions.Inc()
		return v.logError(status.Errorf(codes.Internal, "cannot read transaction data: %v", err))
	}

	// increment prometheus counter
	prometheusProcessedTransactions.Inc()

	return stream.SendAndClose(&validator_api.ValidateTransactionResponse{
		Valid: true,
	})
}

func (v *Server) ValidateTransaction(ctx context.Context, req *validator_api.ValidateTransactionRequest) (*validator_api.ValidateTransactionResponse, error) {
	timeStart := time.Now()
	traceSpan := tracing.Start(ctx, "Validator:ValidateTransaction")
	defer traceSpan.Finish()

	tx, err := bt.NewTxFromBytes(req.TransactionData)
	if err != nil {
		prometheusInvalidTransactions.Inc()
		traceSpan.RecordError(err)
		return nil, v.logError(status.Errorf(codes.Internal, "cannot read transaction data: %v", err))
	}

	err = v.validator.Validate(traceSpan.Ctx, tx)
	if err != nil {
		prometheusInvalidTransactions.Inc()
		traceSpan.RecordError(err)
		return &validator_api.ValidateTransactionResponse{
			Valid:  false,
			Reason: fmt.Sprintf("transaction %s is invalid: %v", tx.TxID(), err),
		}, nil
	}

	// increment prometheus counter
	prometheusProcessedTransactions.Inc()
	prometheusTransactionSize.Observe(float64(len(req.TransactionData)))
	prometheusTransactionDuration.Observe(float64(time.Since(timeStart).Microseconds()))

	return &validator_api.ValidateTransactionResponse{
		Valid: true,
	}, nil
}

func (v *Server) logError(err error) error {
	if err != nil {
		v.logger.Errorf("%v", err)
	}
	return err
}
