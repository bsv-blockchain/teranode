package propagation

import (
	"context"
	"time"

	"github.com/bitcoin-sv/ubsv/services/propagation/propagation_api"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// PropagationServer type carries the logger within it
type DumbPropagationServer struct {
	logger utils.Logger
	propagation_api.UnsafePropagationAPIServer
}

// New will return a server instance with the logger stored within it
func NewDumbPropagationServer() *DumbPropagationServer {
	initPrometheusMetrics()

	logger := gocore.Log("dumbPS")

	logger.Warnf("Using DumbPropagationServer (for testing only)")

	return &DumbPropagationServer{
		logger: logger,
	}
}

func (ps *DumbPropagationServer) Init(_ context.Context) (err error) {
	return nil
}

// Start function
func (ps *DumbPropagationServer) Start(ctx context.Context) (err error) {
	// this will block
	if err = util.StartGRPCServer(ctx, ps.logger, "propagation", func(server *grpc.Server) {
		propagation_api.RegisterPropagationAPIServer(server, ps)
	}); err != nil {
		return err
	}

	return nil
}

func (ps *DumbPropagationServer) Stop(_ context.Context) error {
	return nil
}

func (ps *DumbPropagationServer) Health(_ context.Context, _ *emptypb.Empty) (*propagation_api.HealthResponse, error) {
	prometheusHealth.Inc()
	return &propagation_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (ps *DumbPropagationServer) ProcessTransaction(ctx context.Context, req *propagation_api.ProcessTransactionRequest) (*emptypb.Empty, error) {
	prometheusProcessedTransactions.Inc()

	return &emptypb.Empty{}, nil
}
