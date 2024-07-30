package coinbase

import (
	"context"
	"github.com/bitcoin-sv/ubsv/errors"

	"github.com/bitcoin-sv/ubsv/services/coinbase/coinbase_api"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	client  coinbase_api.CoinbaseAPIClient
	logger  ulogger.Logger
	running bool
	conn    *grpc.ClientConn
}

func NewClient(ctx context.Context, logger ulogger.Logger) (*Client, error) {
	coinbaseGrpcAddress, ok := gocore.Config().Get("coinbase_grpcAddress")
	if !ok {
		return nil, errors.NewConfigurationError("no coinbase_grpcAddress setting found")
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
		logger:  logger.New("coinb"),
		running: true,
		conn:    baConn,
	}, nil
}

func NewClientWithAddress(ctx context.Context, logger ulogger.Logger, address string) (*Client, error) {
	baConn, err := util.GetGRPCClient(ctx, address, &util.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
		MaxRetries:  3,
	})
	if err != nil {
		return nil, err
	}

	return &Client{
		client:  coinbase_api.NewCoinbaseAPIClient(baConn),
		logger:  logger,
		running: true,
		conn:    baConn,
	}, nil
}

func (c *Client) Health(ctx context.Context) (*coinbase_api.HealthResponse, error) {
	return c.client.HealthGRPC(ctx, &emptypb.Empty{})
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

func (c *Client) GetBalance(ctx context.Context) (uint64, uint64, error) {
	res, err := c.client.GetBalance(ctx, &emptypb.Empty{})
	if err != nil {
		return 0, 0, err
	}

	return res.NumberOfUtxos, res.TotalSatoshis, nil
}
