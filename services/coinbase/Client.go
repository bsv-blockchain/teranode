package coinbase

import (
	"context"
	"net/http"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/coinbase/coinbase_api"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	client  coinbase_api.CoinbaseAPIClient
	logger  ulogger.Logger
	running bool
	conn    *grpc.ClientConn
}

func NewClient(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings) (*Client, error) {
	coinbaseGrpcAddress := tSettings.Coinbase.GRPCAddress
	if coinbaseGrpcAddress == "" {
		return nil, errors.NewConfigurationError("no coinbase_grpcAddress setting found")
	}

	baConn, err := util.GetGRPCClient(ctx, coinbaseGrpcAddress, &util.ConnectionOptions{
		MaxRetries: 3,
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
		MaxRetries: 3,
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

func (c *Client) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// Add liveness checks here. Don't include dependency checks.
		// If the service is stuck return http.StatusServiceUnavailable
		// to indicate a restart is needed
		return http.StatusOK, "OK", nil
	}

	// Add readiness checks here. Include dependency checks.
	// If any dependency is not ready, return http.StatusServiceUnavailable
	// If all dependencies are ready, return http.StatusOK
	// A failed dependency check does not imply the service needs restarting
	res, err := c.client.HealthGRPC(ctx, &emptypb.Empty{})
	if res == nil || !res.Ok || err != nil {
		details := "failed health check with no details"
		if res != nil {
			details = res.Details
		}

		return http.StatusFailedDependency, details, errors.UnwrapGRPC(err)
	}

	return http.StatusOK, res.Details, nil
}

// RequestFunds implements ClientI.
func (c *Client) RequestFunds(ctx context.Context, address string, disableDistribute bool) (*bt.Tx, error) {
	res, err := c.client.RequestFunds(ctx, &coinbase_api.RequestFundsRequest{
		Address:           address,
		DisableDistribute: disableDistribute,
	})

	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	return bt.NewTxFromBytes(res.Tx)
}

func (c *Client) GetBalance(ctx context.Context) (uint64, uint64, error) {
	res, err := c.client.GetBalance(ctx, &emptypb.Empty{})
	if err != nil {
		return 0, 0, errors.UnwrapGRPC(err)
	}

	return res.NumberOfUtxos, res.TotalSatoshis, nil
}
