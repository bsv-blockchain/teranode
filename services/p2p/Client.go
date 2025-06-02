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
	// Include the admin API key in the connection options
	apiKey := tSettings.GRPCAdminAPIKey
	if apiKey != "" {
		logger.Infof("[Legacy Client] Using API key for authentication")
	}

	baConn, err := util.GetGRPCClient(ctx, address, &util.ConnectionOptions{
		MaxRetries:   tSettings.GRPCMaxRetries,
		RetryBackoff: tSettings.GRPCRetryBackoff,
		APIKey:       apiKey, // Add the API key to the connection options
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

func (c *Client) BanPeer(ctx context.Context, peer *p2p_api.BanPeerRequest) (*p2p_api.BanPeerResponse, error) {
	return c.client.BanPeer(ctx, peer)
}

func (c *Client) UnbanPeer(ctx context.Context, peer *p2p_api.UnbanPeerRequest) (*p2p_api.UnbanPeerResponse, error) {
	return c.client.UnbanPeer(ctx, peer)
}

func (c *Client) IsBanned(ctx context.Context, peer *p2p_api.IsBannedRequest) (*p2p_api.IsBannedResponse, error) {
	return c.client.IsBanned(ctx, peer)
}

func (c *Client) ListBanned(ctx context.Context, _ *emptypb.Empty) (*p2p_api.ListBannedResponse, error) {
	return c.client.ListBanned(ctx, &emptypb.Empty{})
}

func (c *Client) ClearBanned(ctx context.Context, _ *emptypb.Empty) (*p2p_api.ClearBannedResponse, error) {
	return c.client.ClearBanned(ctx, &emptypb.Empty{})
}

// AddBanScore adds to a peer's ban score with the specified reason.
// Parameters:
//   - ctx: Context for the operation
//   - req: AddBanScoreRequest containing peer ID and reason
//
// Returns:
//   - *p2p_api.AddBanScoreResponse: Response indicating success
//   - error: Any error encountered during the operation
func (c *Client) AddBanScore(ctx context.Context, req *p2p_api.AddBanScoreRequest) (*p2p_api.AddBanScoreResponse, error) {
	return c.client.AddBanScore(ctx, req)
}
