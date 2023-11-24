package coinbase

import (
	"context"
	"fmt"

	"github.com/bitcoin-sv/ubsv/services/coinbase/coinbase_api"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	client  coinbase_api.CoinbaseAPIClient
	logger  utils.Logger
	running bool
	conn    *grpc.ClientConn
}

func NewClient(ctx context.Context) (*Client, error) {
	coinbaseGrpcAddress, ok := gocore.Config().Get("coinbase_grpcAddress")
	if !ok {
		return nil, fmt.Errorf("no coinbase_grpcAddress setting found")
	}
	baConn, err := util.GetGRPCClient(ctx, coinbaseGrpcAddress, &util.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
		MaxRetries:  3,
	})
	if err != nil {
		return nil, err
	}

	return &Client{
		client:  coinbase_api.NewCoinbaseAPIClient(baConn),
		logger:  util.NewLogger("coinb"),
		running: true,
		conn:    baConn,
	}, nil
}

func NewClientWithAddress(ctx context.Context, logger utils.Logger, address string) (ClientI, error) {
	baConn, err := util.GetGRPCClient(ctx, address, &util.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
		MaxRetries:  3,
	})
	if err != nil {
		return nil, err
	}

	return &Client{
		client: coinbase_api.NewCoinbaseAPIClient(baConn),
		logger: logger,
	}, nil
}

func (c *Client) Health(ctx context.Context) (*coinbase_api.HealthResponse, error) {
	return c.client.Health(ctx, &emptypb.Empty{})
}

// RequestFunds implements ClientI.
func (c *Client) RequestFunds(ctx context.Context, address string, disableDistribute bool) (*bt.Tx, error) {
	res, err := c.client.RequestFunds(ctx, &coinbase_api.RequestFundsRequest{
		Address:           address,
		DisableDistribute: disableDistribute,
	})

	if err != nil {
		return nil, err
	}

	return bt.NewTxFromBytes(res.Tx)
}
