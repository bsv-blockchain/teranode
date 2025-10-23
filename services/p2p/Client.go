// Package p2p provides peer-to-peer networking functionality for the Teranode system.
package p2p

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
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

// BanPeer implements the ClientI interface method to ban a peer.
// This method forwards the ban request to the P2P service via gRPC, allowing
// clients to remotely manage peer bans through the service API.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and timeout control
//   - peer: BanPeerRequest containing the peer address and ban duration
//
// Returns:
//   - BanPeerResponse confirming the ban operation
//   - Error if the gRPC call fails or the peer cannot be banned
func (c *Client) BanPeer(ctx context.Context, peer *p2p_api.BanPeerRequest) (*p2p_api.BanPeerResponse, error) {
	return c.client.BanPeer(ctx, peer)
}

// UnbanPeer implements the ClientI interface method to unban a peer.
// This method forwards the unban request to the P2P service via gRPC, allowing
// clients to remotely remove peer bans through the service API.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and timeout control
//   - peer: UnbanPeerRequest containing the peer address to unban
//
// Returns:
//   - UnbanPeerResponse confirming the unban operation
//   - Error if the gRPC call fails or the peer cannot be unbanned
func (c *Client) UnbanPeer(ctx context.Context, peer *p2p_api.UnbanPeerRequest) (*p2p_api.UnbanPeerResponse, error) {
	return c.client.UnbanPeer(ctx, peer)
}

// IsBanned implements the ClientI interface method to check if a peer is banned.
// This method queries the P2P service via gRPC to determine the ban status
// of a specific peer address, allowing clients to verify ban states remotely.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and timeout control
//   - peer: IsBannedRequest containing the peer address to check
//
// Returns:
//   - IsBannedResponse with the ban status (true if banned, false otherwise)
//   - Error if the gRPC call fails
func (c *Client) IsBanned(ctx context.Context, peer *p2p_api.IsBannedRequest) (*p2p_api.IsBannedResponse, error) {
	return c.client.IsBanned(ctx, peer)
}

// ListBanned implements the ClientI interface method to retrieve all banned peers.
// This method queries the P2P service via gRPC to get a complete list of currently
// banned peer addresses, providing clients with visibility into the ban list.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and timeout control
//   - _: Empty request (no parameters required)
//
// Returns:
//   - ListBannedResponse containing an array of banned peer addresses
//   - Error if the gRPC call fails
func (c *Client) ListBanned(ctx context.Context, _ *emptypb.Empty) (*p2p_api.ListBannedResponse, error) {
	return c.client.ListBanned(ctx, &emptypb.Empty{})
}

// ClearBanned implements the ClientI interface method to clear all peer bans.
// This method forwards the clear request to the P2P service via gRPC, allowing
// clients to remotely reset the entire ban list through the service API.
//
// This is a destructive operation that removes all ban entries permanently.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and timeout control
//   - _: Empty request (no parameters required)
//
// Returns:
//   - ClearBannedResponse confirming the clear operation
//   - Error if the gRPC call fails
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

// ConnectPeer connects to a specific peer using the provided multiaddr.
// Parameters:
//   - ctx: Context for the operation
//   - peerAddr: The peer address in multiaddr format
//
// Returns:
//   - error: Any error encountered during the operation
func (c *Client) ConnectPeer(ctx context.Context, peerAddr string) error {
	req := &p2p_api.ConnectPeerRequest{
		PeerAddress: peerAddr,
	}

	var (
		resp *p2p_api.ConnectPeerResponse
		err  error
	)

	if resp, err = c.client.ConnectPeer(ctx, req); err != nil {
		return err
	}

	if resp != nil && !resp.Success {
		return errors.NewServiceError("failed to connect to peer: %s", resp.Error)
	}

	return nil
}

// DisconnectPeer disconnects from a specific peer using their peer ID.
// Parameters:
//   - ctx: Context for the operation
//   - peerID: The peer ID to disconnect from
//
// Returns:
//   - error: Any error encountered during the operation
func (c *Client) DisconnectPeer(ctx context.Context, peerID string) error {
	req := &p2p_api.DisconnectPeerRequest{
		PeerId: peerID,
	}

	var (
		resp *p2p_api.DisconnectPeerResponse
		err  error
	)

	if resp, err = c.client.DisconnectPeer(ctx, req); err != nil {
		return err
	}

	if resp != nil && !resp.Success {
		return errors.NewServiceError("failed to disconnect from peer: %s", resp.Error)
	}

	return nil
}

// RecordCatchupAttempt records that a catchup attempt was made to a peer.
// Parameters:
//   - ctx: Context for the operation
//   - peerID: The peer ID to record the attempt for
//
// Returns:
//   - error: Any error encountered during the operation
func (c *Client) RecordCatchupAttempt(ctx context.Context, peerID string) error {
	req := &p2p_api.RecordCatchupAttemptRequest{
		PeerId: peerID,
	}

	resp, err := c.client.RecordCatchupAttempt(ctx, req)
	if err != nil {
		return err
	}

	if resp != nil && !resp.Ok {
		return errors.NewServiceError("failed to record catchup attempt")
	}

	return nil
}

// RecordCatchupSuccess records a successful catchup from a peer.
// Parameters:
//   - ctx: Context for the operation
//   - peerID: The peer ID to record the success for
//   - durationMs: Duration of the catchup operation in milliseconds
//
// Returns:
//   - error: Any error encountered during the operation
func (c *Client) RecordCatchupSuccess(ctx context.Context, peerID string, durationMs int64) error {
	req := &p2p_api.RecordCatchupSuccessRequest{
		PeerId:     peerID,
		DurationMs: durationMs,
	}

	resp, err := c.client.RecordCatchupSuccess(ctx, req)
	if err != nil {
		return err
	}

	if resp != nil && !resp.Ok {
		return errors.NewServiceError("failed to record catchup success")
	}

	return nil
}

// RecordCatchupFailure records a failed catchup attempt from a peer.
// Parameters:
//   - ctx: Context for the operation
//   - peerID: The peer ID to record the failure for
//
// Returns:
//   - error: Any error encountered during the operation
func (c *Client) RecordCatchupFailure(ctx context.Context, peerID string) error {
	req := &p2p_api.RecordCatchupFailureRequest{
		PeerId: peerID,
	}

	resp, err := c.client.RecordCatchupFailure(ctx, req)
	if err != nil {
		return err
	}

	if resp != nil && !resp.Ok {
		return errors.NewServiceError("failed to record catchup failure")
	}

	return nil
}

// RecordCatchupMalicious records malicious behavior detected during catchup.
// Parameters:
//   - ctx: Context for the operation
//   - peerID: The peer ID to record malicious behavior for
//
// Returns:
//   - error: Any error encountered during the operation
func (c *Client) RecordCatchupMalicious(ctx context.Context, peerID string) error {
	req := &p2p_api.RecordCatchupMaliciousRequest{
		PeerId: peerID,
	}

	resp, err := c.client.RecordCatchupMalicious(ctx, req)
	if err != nil {
		return err
	}

	if resp != nil && !resp.Ok {
		return errors.NewServiceError("failed to record catchup malicious behavior")
	}

	return nil
}

// UpdateCatchupReputation updates the reputation score for a peer.
// Parameters:
//   - ctx: Context for the operation
//   - peerID: The peer ID to update reputation for
//   - score: Reputation score between 0 and 100
//
// Returns:
//   - error: Any error encountered during the operation
func (c *Client) UpdateCatchupReputation(ctx context.Context, peerID string, score float64) error {
	req := &p2p_api.UpdateCatchupReputationRequest{
		PeerId: peerID,
		Score:  score,
	}

	resp, err := c.client.UpdateCatchupReputation(ctx, req)
	if err != nil {
		return err
	}

	if resp != nil && !resp.Ok {
		return errors.NewServiceError("failed to update catchup reputation")
	}

	return nil
}

// GetPeersForCatchup returns peers suitable for catchup operations.
// Parameters:
//   - ctx: Context for the operation
//
// Returns:
//   - *p2p_api.GetPeersForCatchupResponse: Response containing peer information
//   - error: Any error encountered during the operation
func (c *Client) GetPeersForCatchup(ctx context.Context) (*p2p_api.GetPeersForCatchupResponse, error) {
	req := &p2p_api.GetPeersForCatchupRequest{}
	return c.client.GetPeersForCatchup(ctx, req)
}

// ReportValidSubtree reports that a subtree was successfully fetched and validated from a peer.
// Parameters:
//   - ctx: Context for the operation
//   - peerID: Peer ID that provided the subtree
//   - subtreeHash: Hash of the validated subtree
//
// Returns:
//   - error: Any error encountered during the operation
func (c *Client) ReportValidSubtree(ctx context.Context, peerID string, subtreeHash string) error {
	req := &p2p_api.ReportValidSubtreeRequest{
		PeerId:      peerID,
		SubtreeHash: subtreeHash,
	}

	resp, err := c.client.ReportValidSubtree(ctx, req)
	if err != nil {
		return err
	}

	if resp != nil && !resp.Success {
		return errors.NewServiceError("failed to report valid subtree: %s", resp.Message)
	}

	return nil
}

// ReportValidBlock reports that a block was successfully received and validated from a peer.
// Parameters:
//   - ctx: Context for the operation
//   - peerID: Peer ID that provided the block
//   - blockHash: Hash of the validated block
//
// Returns:
//   - error: Any error encountered during the operation
func (c *Client) ReportValidBlock(ctx context.Context, peerID string, blockHash string) error {
	req := &p2p_api.ReportValidBlockRequest{
		PeerId:    peerID,
		BlockHash: blockHash,
	}

	resp, err := c.client.ReportValidBlock(ctx, req)
	if err != nil {
		return err
	}

	if resp != nil && !resp.Success {
		return errors.NewServiceError("failed to report valid block: %s", resp.Message)
	}

	return nil
}
