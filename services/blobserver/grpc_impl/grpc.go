package grpc_impl

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/TAAL-GmbH/ubsv/services/blobserver"
	blobserver_api "github.com/TAAL-GmbH/ubsv/services/blobserver/blobserver_api"
	"github.com/TAAL-GmbH/ubsv/services/seeder/seeder_api"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	prometheusBlobServerGRPCGetTransaction *prometheus.CounterVec
	prometheusBlobServerGRPCGetSubtree     *prometheus.CounterVec
	prometheusBlobServerGRPCGetBlockHeader *prometheus.CounterVec
	prometheusBlobServerGRPCGetBlock       *prometheus.CounterVec
	prometheusBlobServerGRPCGetUTXO        *prometheus.CounterVec
)

type GRPC struct {
	blobserver_api.UnimplementedBlobServerAPIServer
	db         *blobserver.DAO
	logger     *utils.Logger
	grpcServer *grpc.Server
}

func New(db *blobserver.DAO) (*GRPC, error) {
	grpcServer, err := utils.GetGRPCServer(&utils.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
	})
	if err != nil {
		return nil, fmt.Errorf("could not create GRPC server [%w]", err)
	}

	blobserver_api.RegisterBlobServerAPIServer(grpcServer, v)

	// Register reflection service on gRPC server
	reflection.Register(v.grpcServer)

	return &GRPC{
		db:         db,
		grpcServer: grpcServer,
	}, nil

}

func (grpc *GRPC) Start() error {
	lis, err := net.Listen("tcp", grpcAddress)
	if err != nil {
		return fmt.Errorf("GRPC server failed to listen [%w]", err)
	}

	v.logger.Infof("BlobServer GRPC service listening on %s", grpcAddress)

	if err = grpc.grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("GRPC server failed [%w]", err)
	}
}

func (grpc *GRPC) Stop() {
	grpc.grpcServer.GracefulStop()
}

func (v *GRPC) Health(_ context.Context, _ *emptypb.Empty) (*seeder_api.HealthResponse, error) {
	return &seeder_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (v *GRPC) GetTransaction(ctx context.Context, req *blobserver_api.Hash) (*blobserver_api.Blob, error) {
	tx, err := v.db.GetTransaction(req.GetHash())
	if err != nil {
		return nil, err
	}

	prometheusBlobServerGRPCGetTransaction.WithLabelValues("OK").Inc()

	return &blobserver_api.GetTransactionResponse{
		Transaction: tx,
	}, nil
}
