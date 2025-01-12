package peer

import (
	"context"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/legacy/peer_api"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	client peer_api.PeerServiceClient
	logger ulogger.Logger
}

func NewClient(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings) (ClientI, error) {
	logger = logger.New("blkcC")

	legacyGrpcAddress := tSettings.Legacy.GRPCAddress
	if legacyGrpcAddress == "" {
		return nil, errors.NewConfigurationError("no legacy_grpcAddress setting found")
	}

	return NewClientWithAddress(ctx, logger, tSettings, legacyGrpcAddress)
}

func NewClientWithAddress(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, address string) (ClientI, error) {
	baConn, err := util.GetGRPCClient(ctx, address, &util.ConnectionOptions{
		MaxRetries: 3,
	}, tSettings)
	if err != nil {
		return nil, errors.NewServiceError("failed to init peer service connection ", err)
	}

	c := &Client{
		client: peer_api.NewPeerServiceClient(baConn),
		logger: logger,
	}

	return c, nil
}

func (c *Client) GetPeers(ctx context.Context) (*peer_api.GetPeersResponse, error) {
	return c.client.GetPeers(ctx, &emptypb.Empty{})
}
