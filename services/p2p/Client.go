// Package p2p provides peer-to-peer networking functionality for the Teranode system.
package p2p

import (
	"context"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/libp2p/go-libp2p/core/peer"
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
//   - []*PeerInfo: Slice of peer information
//   - error: Any error encountered during the operation
func (c *Client) GetPeers(ctx context.Context) ([]*PeerInfo, error) {
	_, err := c.client.GetPeers(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}

	// Convert p2p_api response to native PeerInfo slice
	// Note: The p2p_api.GetPeersResponse contains legacy SVNode format
	// For now, return empty slice as the actual implementation uses GetPeerRegistry
	return []*PeerInfo{}, nil
}

// BanPeer implements the ClientI interface method to ban a peer.
// This method forwards the ban request to the P2P service via gRPC, allowing
// clients to remotely manage peer bans through the service API.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and timeout control
//   - addr: Peer address (IP or subnet) to ban
//   - until: Unix timestamp when the ban expires
//
// Returns:
//   - Error if the gRPC call fails or the peer cannot be banned
func (c *Client) BanPeer(ctx context.Context, addr string, until int64) error {
	req := &p2p_api.BanPeerRequest{
		Addr:  addr,
		Until: until,
	}

	resp, err := c.client.BanPeer(ctx, req)
	if err != nil {
		return err
	}

	if resp != nil && !resp.Ok {
		return errors.NewServiceError("failed to ban peer")
	}

	return nil
}

// UnbanPeer implements the ClientI interface method to unban a peer.
// This method forwards the unban request to the P2P service via gRPC, allowing
// clients to remotely remove peer bans through the service API.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and timeout control
//   - addr: Peer address (IP or subnet) to unban
//
// Returns:
//   - Error if the gRPC call fails or the peer cannot be unbanned
func (c *Client) UnbanPeer(ctx context.Context, addr string) error {
	req := &p2p_api.UnbanPeerRequest{
		Addr: addr,
	}

	resp, err := c.client.UnbanPeer(ctx, req)
	if err != nil {
		return err
	}

	if resp != nil && !resp.Ok {
		return errors.NewServiceError("failed to unban peer")
	}

	return nil
}

// IsBanned implements the ClientI interface method to check if a peer is banned.
// This method queries the P2P service via gRPC to determine the ban status
// of a specific peer address, allowing clients to verify ban states remotely.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and timeout control
//   - ipOrSubnet: IP address or subnet to check
//
// Returns:
//   - bool: True if banned, false otherwise
//   - Error if the gRPC call fails
func (c *Client) IsBanned(ctx context.Context, ipOrSubnet string) (bool, error) {
	req := &p2p_api.IsBannedRequest{
		IpOrSubnet: ipOrSubnet,
	}

	resp, err := c.client.IsBanned(ctx, req)
	if err != nil {
		return false, err
	}

	return resp.IsBanned, nil
}

// ListBanned implements the ClientI interface method to retrieve all banned peers.
// This method queries the P2P service via gRPC to get a complete list of currently
// banned peer addresses, providing clients with visibility into the ban list.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and timeout control
//
// Returns:
//   - []string: Array of banned peer IDs
//   - Error if the gRPC call fails
func (c *Client) ListBanned(ctx context.Context) ([]string, error) {
	resp, err := c.client.ListBanned(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}

	return resp.Banned, nil
}

// ClearBanned implements the ClientI interface method to clear all peer bans.
// This method forwards the clear request to the P2P service via gRPC, allowing
// clients to remotely reset the entire ban list through the service API.
//
// This is a destructive operation that removes all ban entries permanently.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and timeout control
//
// Returns:
//   - Error if the gRPC call fails
func (c *Client) ClearBanned(ctx context.Context) error {
	resp, err := c.client.ClearBanned(ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}

	if resp != nil && !resp.Ok {
		return errors.NewServiceError("failed to clear banned peers")
	}

	return nil
}

// AddBanScore adds to a peer's ban score with the specified reason.
// Parameters:
//   - ctx: Context for the operation
//   - peerID: Peer ID to add ban score to
//   - reason: Reason for adding ban score
//
// Returns:
//   - error: Any error encountered during the operation
func (c *Client) AddBanScore(ctx context.Context, peerID string, reason string) error {
	req := &p2p_api.AddBanScoreRequest{
		PeerId: peerID,
		Reason: reason,
	}

	resp, err := c.client.AddBanScore(ctx, req)
	if err != nil {
		return err
	}

	if resp != nil && !resp.Ok {
		return errors.NewServiceError("failed to add ban score")
	}

	return nil
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

// UpdateCatchupError stores the last catchup error for a peer.
// Parameters:
//   - ctx: Context for the operation
//   - peerID: The peer ID to update error for
//   - errorMsg: The error message to store
//
// Returns:
//   - error: Any error encountered during the operation
func (c *Client) UpdateCatchupError(ctx context.Context, peerID string, errorMsg string) error {
	req := &p2p_api.UpdateCatchupErrorRequest{
		PeerId:   peerID,
		ErrorMsg: errorMsg,
	}

	resp, err := c.client.UpdateCatchupError(ctx, req)
	if err != nil {
		return err
	}

	if resp != nil && !resp.Ok {
		return errors.NewServiceError("failed to update catchup error")
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
//   - []*PeerInfo: Slice of peer information sorted by reputation
//   - error: Any error encountered during the operation
func (c *Client) GetPeersForCatchup(ctx context.Context) ([]*PeerInfo, error) {
	req := &p2p_api.GetPeersForCatchupRequest{}
	resp, err := c.client.GetPeersForCatchup(ctx, req)
	if err != nil {
		return nil, err
	}

	// Convert p2p_api peer info to native PeerInfo
	peers := make([]*PeerInfo, 0, len(resp.Peers))
	for _, apiPeer := range resp.Peers {
		peerInfo := convertFromAPIPeerInfo(apiPeer)
		peers = append(peers, peerInfo)
	}

	return peers, nil
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

// IsPeerMalicious checks if a peer is considered malicious.
//
// Parameters:
//   - ctx: Context for the operation
//   - peerID: The P2P peer identifier to check
//
// Returns:
//   - bool: True if the peer is considered malicious
//   - string: Reason why the peer is considered malicious (if applicable)
//   - error: Any error encountered during the operation
func (c *Client) IsPeerMalicious(ctx context.Context, peerID string) (bool, string, error) {
	req := &p2p_api.IsPeerMaliciousRequest{
		PeerId: peerID,
	}

	resp, err := c.client.IsPeerMalicious(ctx, req)
	if err != nil {
		return false, "", err
	}

	return resp.IsMalicious, resp.Reason, nil
}

// IsPeerUnhealthy checks if a peer is considered unhealthy.
//
// Parameters:
//   - ctx: Context for the operation
//   - peerID: The P2P peer identifier to check
//
// Returns:
//   - bool: True if the peer is considered unhealthy
//   - string: Reason why the peer is considered unhealthy (if applicable)
//   - float32: The peer's current reputation score
//   - error: Any error encountered during the operation
func (c *Client) IsPeerUnhealthy(ctx context.Context, peerID string) (bool, string, float32, error) {
	req := &p2p_api.IsPeerUnhealthyRequest{
		PeerId: peerID,
	}

	resp, err := c.client.IsPeerUnhealthy(ctx, req)
	if err != nil {
		return false, "", 0, err
	}

	return resp.IsUnhealthy, resp.Reason, resp.ReputationScore, nil
}

// GetPeerRegistry retrieves the comprehensive peer registry data from the P2P service.
func (c *Client) GetPeerRegistry(ctx context.Context) ([]*PeerInfo, error) {
	resp, err := c.client.GetPeerRegistry(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}

	// Convert p2p_api peer registry info to native PeerInfo
	peers := make([]*PeerInfo, 0, len(resp.Peers))
	for _, apiPeer := range resp.Peers {
		peers = append(peers, convertFromAPIPeerInfo(apiPeer))
	}

	return peers, nil
}

// RecordBytesDownloaded records the number of bytes downloaded via HTTP from a peer.
// Parameters:
//   - ctx: Context for the operation
//   - peerID: Peer ID string that provided the data
//   - bytesDownloaded: Number of bytes downloaded in this operation
//
// Returns:
//   - error: Any error encountered during the operation
func (c *Client) RecordBytesDownloaded(ctx context.Context, peerID string, bytesDownloaded uint64) error {
	req := &p2p_api.RecordBytesDownloadedRequest{
		PeerId:          peerID,
		BytesDownloaded: bytesDownloaded,
	}

	resp, err := c.client.RecordBytesDownloaded(ctx, req)
	if err != nil {
		return err
	}

	if resp != nil && !resp.Ok {
		return errors.NewServiceError("failed to record bytes downloaded")
	}

	return nil
}

// GetPeer retrieves information about a specific peer from the P2P service.
// Returns nil if the peer is not found in the registry.
func (c *Client) GetPeer(ctx context.Context, peerID string) (*PeerInfo, error) {
	resp, err := c.client.GetPeer(ctx, &p2p_api.GetPeerRequest{
		PeerId: peerID,
	})
	if err != nil {
		return nil, err
	}

	// Check if peer was found
	if !resp.Found || resp.Peer == nil {
		return nil, nil
	}

	// Convert from protobuf to native PeerInfo
	return convertFromAPIPeerInfo(resp.Peer), nil
}

// convertFromAPIPeerInfo converts a p2p_api peer info (either PeerInfoForCatchup or PeerRegistryInfo) to native PeerInfo
func convertFromAPIPeerInfo(apiPeer interface{}) *PeerInfo {
	// Handle both PeerInfoForCatchup and PeerRegistryInfo types
	switch p := apiPeer.(type) {
	case *p2p_api.PeerInfoForCatchup:
		peerID, _ := peer.Decode(p.Id)
		return &PeerInfo{
			ID:                   peerID,
			Height:               p.Height,
			BlockHash:            p.BlockHash,
			DataHubURL:           p.DataHubUrl,
			ReputationScore:      p.CatchupReputationScore,
			InteractionAttempts:  p.CatchupAttempts,
			InteractionSuccesses: p.CatchupSuccesses,
			InteractionFailures:  p.CatchupFailures,
		}
	case *p2p_api.PeerRegistryInfo:
		peerID, _ := peer.Decode(p.Id)
		return &PeerInfo{
			ID:                     peerID,
			ClientName:             p.ClientName,
			Height:                 p.Height,
			BlockHash:              p.BlockHash,
			DataHubURL:             p.DataHubUrl,
			BanScore:               int(p.BanScore),
			IsBanned:               p.IsBanned,
			IsConnected:            p.IsConnected,
			ConnectedAt:            time.Unix(p.ConnectedAt, 0),
			BytesReceived:          p.BytesReceived,
			LastBlockTime:          time.Unix(p.LastBlockTime, 0),
			LastMessageTime:        time.Unix(p.LastMessageTime, 0),
			URLResponsive:          p.UrlResponsive,
			LastURLCheck:           time.Unix(p.LastUrlCheck, 0),
			Storage:                p.Storage,
			InteractionAttempts:    p.InteractionAttempts,
			InteractionSuccesses:   p.InteractionSuccesses,
			InteractionFailures:    p.InteractionFailures,
			LastInteractionAttempt: time.Unix(p.LastInteractionAttempt, 0),
			LastInteractionSuccess: time.Unix(p.LastInteractionSuccess, 0),
			LastInteractionFailure: time.Unix(p.LastInteractionFailure, 0),
			ReputationScore:        p.ReputationScore,
			MaliciousCount:         p.MaliciousCount,
			AvgResponseTime:        time.Duration(p.AvgResponseTimeMs) * time.Millisecond,
			LastCatchupError:       p.LastCatchupError,
			LastCatchupErrorTime:   time.Unix(p.LastCatchupErrorTime, 0),
		}
	default:
		// Return empty PeerInfo for unknown types
		return &PeerInfo{}
	}
}
