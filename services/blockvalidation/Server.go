package blockvalidation

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	blockvalidation_api "github.com/TAAL-GmbH/ubsv/services/blockvalidation/blockvalidation_api"
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
	prometheusBlockValidationBlockFound prometheus.Counter
)

func init() {
	prometheusBlockValidationBlockFound = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "blockvalidation_block_found",
			Help: "Number of blocks found",
		},
	)
}

// BlockValidation type carries the logger within it
type BlockValidation struct {
	blockvalidation_api.UnimplementedBlockValidationAPIServer
	logger     utils.Logger
	grpcServer *grpc.Server
}

func Enabled() bool {
	_, found := gocore.Config().Get("blockvalidation_grpcAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger utils.Logger) *BlockValidation {
	// utxostoreURL, err, found := gocore.Config().GetURL("utxostore")
	// if err != nil {
	// 	panic(err)
	// }
	// if !found {
	// 	panic("no utxostore setting found")
	// }

	// s, err := utxo.NewStore(logger, utxostoreURL)
	// if err != nil {
	// 	panic(err)
	// }

	bVal := &BlockValidation{
		// utxoStore: s,
		logger: logger,
	}

	return bVal
}

// Start function
func (u *BlockValidation) Start() error {

	address, ok := gocore.Config().Get("blockvalidation_grpcAddress")
	if !ok {
		return errors.New("no blockvalidation_grpcAddress setting found")
	}

	var err error
	u.grpcServer, err = utils.GetGRPCServer(&utils.ConnectionOptions{
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

	blockvalidation_api.RegisterBlockValidationAPIServer(u.grpcServer, u)

	// Register reflection service on gRPC server.
	reflection.Register(u.grpcServer)

	u.logger.Infof("GRPC server listening on %s", address)

	if err = u.grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("GRPC server failed [%w]", err)
	}

	return nil
}

func (u *BlockValidation) Stop(ctx context.Context) {
	_, cancel := context.WithCancel(ctx)
	defer cancel()

	u.grpcServer.GracefulStop()
}

func (u *BlockValidation) Health(_ context.Context, _ *emptypb.Empty) (*blockvalidation_api.HealthResponse, error) {
	return &blockvalidation_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (u *BlockValidation) BlockFound(ctx context.Context, req *blockvalidation_api.BlockFoundRequest) (*emptypb.Empty, error) {
	prometheusBlockValidationBlockFound.Inc()

	return &emptypb.Empty{}, nil
}
