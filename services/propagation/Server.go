package propagation

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/TAAL-GmbH/ubsv/services/propagation/propagation_api"
	"github.com/TAAL-GmbH/ubsv/services/propagation/store"
	"github.com/TAAL-GmbH/ubsv/services/validator"
	"github.com/TAAL-GmbH/ubsv/tracing"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	prometheusProcessedTransactions prometheus.Counter
	prometheusInvalidTransactions   prometheus.Counter
	prometheusTransactionDuration   prometheus.Histogram
	prometheusTransactionSize       prometheus.Histogram
)

func init() {
	prometheusProcessedTransactions = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "propagation_processed_transactions",
			Help: "Number of transactions processed by the propagation service",
		},
	)
	prometheusInvalidTransactions = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "propagation_invalid_transactions",
			Help: "Number of transactions found invalid by the propagation service",
		},
	)
	prometheusTransactionDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name: "propagation_transactions_duration",
			Help: "Duration of transaction processing by the propagation service",
		},
	)
	prometheusTransactionSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name: "propagation_transactions_size",
			Help: "Size of transactions processed by the propagation service",
		},
	)
}

// PropagationServer type carries the logger within it
type PropagationServer struct {
	propagation_api.UnsafePropagationAPIServer
	logger     utils.Logger
	grpcServer *grpc.Server
	txStore    store.TransactionStore
	validator  validator.Interface
}

func Enabled() bool {
	_, found := gocore.Config().Get("utxostore_grpcAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger utils.Logger, txStore store.TransactionStore, validatorClient *validator.Client) (*PropagationServer, error) {
	return &PropagationServer{
		logger:    logger,
		txStore:   txStore,
		validator: validatorClient,
	}, nil
}

// Start function
func (u *PropagationServer) Start() error {

	address, ok := gocore.Config().Get("propagation_grpcAddress") //, "localhost:8001")
	if !ok {
		return errors.New("no propagation_grpcAddress setting found")
	}

	// LEVEL 0 - no security / no encryption
	var opts []grpc.ServerOption
	_, prometheusOn := gocore.Config().Get("prometheusEndpoint")
	if prometheusOn {
		opts = append(opts,
			grpc.ChainStreamInterceptor(grpc_prometheus.StreamServerInterceptor),
			grpc.ChainUnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
		)
	}

	u.grpcServer = grpc.NewServer(tracing.AddGRPCServerOptions(opts)...)

	gocore.SetAddress(address)

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("GRPC server failed to listen [%w]", err)
	}

	propagation_api.RegisterPropagationAPIServer(u.grpcServer, u)

	// Register reflection service on gRPC server.
	reflection.Register(u.grpcServer)

	u.logger.Infof("GRPC server listening on %s", address)

	if err = u.grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("GRPC server failed [%w]", err)
	}

	return nil
}

func (u *PropagationServer) Stop(ctx context.Context) {
	_, cancel := context.WithCancel(ctx)
	defer cancel()

	u.grpcServer.GracefulStop()
}

func (u *PropagationServer) Health(_ context.Context, _ *emptypb.Empty) (*propagation_api.HealthResponse, error) {
	return &propagation_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (u *PropagationServer) Get(_ context.Context, req *propagation_api.GetRequest) (*propagation_api.GetResponse, error) {
	tx, err := u.txStore.Get(context.Background(), req.Txid)
	if err != nil {
		return nil, err
	}

	return &propagation_api.GetResponse{
		Tx: tx,
	}, nil
}

func (u *PropagationServer) Set(_ context.Context, req *propagation_api.SetRequest) (*emptypb.Empty, error) {
	timeStart := time.Now()
	btTx, err := bt.NewTxFromBytes(req.Tx)
	if err != nil {
		prometheusInvalidTransactions.Inc()
		return &emptypb.Empty{}, err
	}

	// u.logger.Debugf("received transaction on propagation GRPC server: %s", btTx.TxID())

	// Do not allow propagation of coinbase transactions
	if btTx.IsCoinbase() {
		prometheusInvalidTransactions.Inc()
		return &emptypb.Empty{}, fmt.Errorf("received coinbase transaction: %s", btTx.TxID())
	}

	err = ExtendTransaction(btTx, u.txStore)
	if err != nil {
		prometheusInvalidTransactions.Inc()
		return &emptypb.Empty{}, err
	}

	if err = u.validator.Validate(btTx); err != nil {
		// send REJECT message to peer if invalid tx
		u.logger.Errorf("received invalid transaction: %s", err.Error())
		prometheusInvalidTransactions.Inc()
		return &emptypb.Empty{}, err
	}

	if err = u.txStore.Set(context.Background(), bt.ReverseBytes(btTx.TxIDBytes()), btTx.Bytes()); err != nil {
		prometheusInvalidTransactions.Inc()
		return &emptypb.Empty{}, err
	}

	prometheusProcessedTransactions.Inc()
	prometheusTransactionSize.Observe(float64(len(req.Tx)))
	prometheusTransactionDuration.Observe(float64(time.Since(timeStart).Microseconds()))

	return &emptypb.Empty{}, nil
}
