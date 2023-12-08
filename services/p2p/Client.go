package p2p

import (
	"context"
	"fmt"

	"github.com/bitcoin-sv/ubsv/services/p2p/p2p_api"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/ordishs/gocore"
	"google.golang.org/protobuf/types/known/emptypb"
)

type ClientI interface {
	Health(context.Context) (*p2p_api.HealthResponse, error)
}

type Client struct {
	client p2p_api.P2PAPIClient
	logger ulogger.Logger
	// running bool
	// conn *grpc.ClientConn
}

func NewClient(ctx context.Context, logger ulogger.Logger) (ClientI, error) {
	logger = logger.New("blkcC")

	address, ok := gocore.Config().Get("p2p_grpcAddress")
	if !ok {
		return nil, fmt.Errorf("no p2p_grpcAddress setting found")
	}

	return NewClientWithAddress(ctx, logger, address)
}

func NewClientWithAddress(ctx context.Context, logger ulogger.Logger, address string) (ClientI, error) {
	conn, err := util.GetGRPCClient(ctx, address, &util.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
		MaxRetries:  3,
	})
	if err != nil {
		return nil, err
	}

	return &Client{
		client: p2p_api.NewP2PAPIClient(conn),
		logger: logger,
	}, nil
}

func (c Client) Health(ctx context.Context) (*p2p_api.HealthResponse, error) {
	return c.client.Health(ctx, &emptypb.Empty{})
}
