package p2p

import (
	"context"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/p2p/p2p_api"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	client p2p_api.PeerServiceClient
	logger ulogger.Logger
}

func NewClient(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings) (ClientI, error) {
	logger = logger.New("blkcC")

	p2pGrpcAddress := tSettings.P2P.GRPCAddress
	if p2pGrpcAddress == "" {
		return nil, errors.NewConfigurationError("no p2p_grpcAddress setting found")
	}

	return NewClientWithAddress(ctx, logger, p2pGrpcAddress)
}

func NewClientWithAddress(ctx context.Context, logger ulogger.Logger, address string) (ClientI, error) {
	baConn, err := util.GetGRPCClient(ctx, address, &util.ConnectionOptions{
		MaxRetries: 3,
	})
	if err != nil {
		return nil, errors.NewServiceError("failed to init p2p service connection ", err)
	}

	c := &Client{
		client: p2p_api.NewPeerServiceClient(baConn),
		logger: logger,
	}

	return c, nil
}

func (c *Client) GetPeers(ctx context.Context) (*p2p_api.GetPeersResponse, error) {
	return c.client.GetPeers(ctx, &emptypb.Empty{})
}
