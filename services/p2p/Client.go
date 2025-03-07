// Package p2p provides peer-to-peer networking functionality for the Teranode system.
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

// Client implements the ClientI interface and provides P2P client functionality.
type Client struct {
	client p2p_api.PeerServiceClient // gRPC client for peer service communication
	logger ulogger.Logger            // Logger instance for the client
}

// NewClient creates a new P2P client instance using the provided configuration.
// Parameters:
//   - ctx: Context for the operation
//   - logger: Logger instance for client operations
//   - tSettings: Configuration settings for the client
//
// Returns:
//   - ClientI: A new client instance
//   - error: Any error encountered during client creation
func NewClient(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings) (ClientI, error) {
	logger = logger.New("blkcC")

	p2pGrpcAddress := tSettings.P2P.GRPCAddress
	if p2pGrpcAddress == "" {
		return nil, errors.NewConfigurationError("no p2p_grpcAddress setting found")
	}

	return NewClientWithAddress(ctx, logger, p2pGrpcAddress, tSettings)
}

// NewClientWithAddress creates a new P2P client instance with a specific address.
// Parameters:
//   - ctx: Context for the operation
//   - logger: Logger instance for client operations
//   - address: The address to connect to
// 	 - tSettings: The Teranode settings
//
// Returns:
//   - ClientI: A new client instance
//   - error: Any error encountered during client creation

func NewClientWithAddress(ctx context.Context, logger ulogger.Logger, address string, tSettings *settings.Settings) (ClientI, error) {
	baConn, err := util.GetGRPCClient(ctx, address, &util.ConnectionOptions{
		MaxRetries: 3,
	}, tSettings)
	if err != nil {
		return nil, errors.NewServiceError("failed to init p2p service connection ", err)
	}

	c := &Client{
		client: p2p_api.NewPeerServiceClient(baConn),
		logger: logger,
	}

	return c, nil
}

// GetPeers implements the ClientI interface method to retrieve connected peers.
// Parameters:
//   - ctx: Context for the operation
//
// Returns:
//   - *p2p_api.GetPeersResponse: Response containing peer information
//   - error: Any error encountered during the operation
func (c *Client) GetPeers(ctx context.Context) (*p2p_api.GetPeersResponse, error) {
	return c.client.GetPeers(ctx, &emptypb.Empty{})
}
