package grpc_impl

import (
	"context"
	"fmt"
	"net"
	"time"

	blobserver_api "github.com/TAAL-GmbH/ubsv/services/blobserver/blobserver_api"
	"github.com/TAAL-GmbH/ubsv/services/blobserver/dao"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
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
	prometheusBlobServerGRPCGetTransaction *prometheus.CounterVec
	prometheusBlobServerGRPCGetSubtree     *prometheus.CounterVec
	prometheusBlobServerGRPCGetBlockHeader *prometheus.CounterVec
	prometheusBlobServerGRPCGetBlock       *prometheus.CounterVec
	prometheusBlobServerGRPCGetUTXO        *prometheus.CounterVec
)

type GRPC struct {
	blobserver_api.UnimplementedBlobServerAPIServer
	logger     utils.Logger
	db         *dao.DAO
	grpcServer *grpc.Server
}

func New(db *dao.DAO) (*GRPC, error) {
	logger := gocore.Log("b_grpc")

	grpcServer, err := utils.GetGRPCServer(&utils.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
	})
	if err != nil {
		return nil, fmt.Errorf("could not create GRPC server [%w]", err)
	}

	v := &GRPC{
		logger:     logger,
		db:         db,
		grpcServer: grpcServer,
	}

	blobserver_api.RegisterBlobServerAPIServer(grpcServer, v)

	// Register reflection service on gRPC server
	reflection.Register(v.grpcServer)

	return v, nil
}

func (grpc *GRPC) Start(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("GRPC server failed to listen [%w]", err)
	}

	grpc.logger.Infof("BlobServer GRPC service listening on %s", addr)

	go func() {
		grpc.grpcServer.Serve(lis)
	}()

	return nil
}

func (grpc *GRPC) Stop(ctx context.Context) {
	grpc.grpcServer.GracefulStop()
}

func (v *GRPC) Health(_ context.Context, _ *emptypb.Empty) (*blobserver_api.HealthResponse, error) {
	return &blobserver_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (v *GRPC) GetTransaction(ctx context.Context, req *blobserver_api.Hash) (*blobserver_api.Blob, error) {
	hash, err := chainhash.NewHash(req.Hash)
	if err != nil {
		return nil, err
	}

	tx, err := v.db.GetTransaction(ctx, hash)
	if err != nil {
		return nil, err
	}

	prometheusBlobServerGRPCGetTransaction.WithLabelValues("OK").Inc()

	return &blobserver_api.Blob{
		Blob: tx,
	}, nil
}

func (v *GRPC) GetSubtree(ctx context.Context, req *blobserver_api.Hash) (*blobserver_api.Blob, error) {
	hash, err := chainhash.NewHash(req.Hash)
	if err != nil {
		return nil, err
	}

	tx, err := v.db.GetSubtree(ctx, hash)
	if err != nil {
		return nil, err
	}

	prometheusBlobServerGRPCGetSubtree.WithLabelValues("OK").Inc()

	return &blobserver_api.Blob{
		Blob: tx,
	}, nil
}

func (v *GRPC) GetHeader(ctx context.Context, req *blobserver_api.HashOrHeight) (*blobserver_api.Blob, error) {
	hash, err := chainhash.NewHash(req.GetHash())
	if err != nil {
		return nil, err
	}

	tx, err := v.db.GetBlockHeaderByHash(ctx, hash)
	if err != nil {
		return nil, err
	}

	prometheusBlobServerGRPCGetBlockHeader.WithLabelValues("OK").Inc()

	return &blobserver_api.Blob{
		Blob: tx,
	}, nil
}

func (v *GRPC) GetBlock(ctx context.Context, req *blobserver_api.HashOrHeight) (*blobserver_api.Blob, error) {
	hash, err := chainhash.NewHash(req.GetHash())
	if err != nil {
		return nil, err
	}

	tx, err := v.db.GetBlockByHash(ctx, hash)
	if err != nil {
		return nil, err
	}

	prometheusBlobServerGRPCGetBlock.WithLabelValues("OK").Inc()

	return &blobserver_api.Blob{
		Blob: tx,
	}, nil
}

func (v *GRPC) GetUTXO(ctx context.Context, req *blobserver_api.Hash) (*blobserver_api.Blob, error) {
	hash, err := chainhash.NewHash(req.GetHash())
	if err != nil {
		return nil, err
	}

	tx, err := v.db.GetUtxo(ctx, hash)
	if err != nil {
		return nil, err
	}

	prometheusBlobServerGRPCGetUTXO.WithLabelValues("OK").Inc()

	return &blobserver_api.Blob{
		Blob: tx,
	}, nil
}

func init() {
	prometheusBlobServerGRPCGetTransaction = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "blobserver_grpc_get_transaction",
			Help: "Number of Get transactions ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusBlobServerGRPCGetSubtree = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "blobserver_grpc_get_subtree",
			Help: "Number of Get subtree ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusBlobServerGRPCGetBlockHeader = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "blobserver_grpc_get_block_header",
			Help: "Number of Get block header ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusBlobServerGRPCGetBlock = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "blobserver_grpc_get_block",
			Help: "Number of Get block ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusBlobServerGRPCGetUTXO = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "blobserver_grpc_get_utxo",
			Help: "Number of Get UTXO ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)

}
