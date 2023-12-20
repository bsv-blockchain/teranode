package status

import (
	"context"
	"fmt"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/status/status_api"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/ordishs/gocore"
	"google.golang.org/protobuf/types/known/emptypb"
)

type ClientI interface {
	Health(context.Context) (*status_api.HealthResponse, error)
	AnnounceStatus(context.Context, *model.AnnounceStatusRequest)
}

type Client struct {
	client status_api.StatusAPIClient
	logger ulogger.Logger
}

func NewClient(ctx context.Context, logger ulogger.Logger) (ClientI, error) {
	logger = logger.New("statusC")

	address, ok := gocore.Config().Get("status_grpcAddress")
	if !ok {
		return nil, fmt.Errorf("no status_grpcAddress setting found")
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
		client: status_api.NewStatusAPIClient(conn),
		logger: logger,
	}, nil
}

func (c Client) Health(ctx context.Context) (*status_api.HealthResponse, error) {
	return c.client.HealthGRPC(ctx, &emptypb.Empty{})
}

func (c Client) AnnounceStatus(ctx context.Context, status *model.AnnounceStatusRequest) {
	if _, err := c.client.AnnounceStatus(ctx, status); err != nil {
		c.logger.Errorf("failed to announce status: %v", err)
	}
}
