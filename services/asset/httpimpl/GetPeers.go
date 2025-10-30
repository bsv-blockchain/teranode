package httpimpl

import (
	"context"
	"net/http"
	"time"

	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/labstack/echo/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

// PeerInfoResponse represents the JSON response for a single peer
// Matches the structure from P2P service's HandlePeers.go
type PeerInfoResponse struct {
	ID              string `json:"id"`
	Height          int32  `json:"height"`
	BlockHash       string `json:"block_hash"`
	DataHubURL      string `json:"data_hub_url"`
	IsHealthy       bool   `json:"is_healthy"`
	HealthDuration  int64  `json:"health_duration_ms"`
	LastHealthCheck int64  `json:"last_health_check"`
	BanScore        int    `json:"ban_score"`
	IsBanned        bool   `json:"is_banned"`
	IsConnected     bool   `json:"is_connected"`
	ConnectedAt     int64  `json:"connected_at"`
	BytesReceived   uint64 `json:"bytes_received"`
	LastBlockTime   int64  `json:"last_block_time"`
	LastMessageTime int64  `json:"last_message_time"`
	URLResponsive   bool   `json:"url_responsive"`
	LastURLCheck    int64  `json:"last_url_check"`

	// Catchup metrics
	CatchupAttempts        int64   `json:"catchup_attempts"`
	CatchupSuccesses       int64   `json:"catchup_successes"`
	CatchupFailures        int64   `json:"catchup_failures"`
	CatchupLastAttempt     int64   `json:"catchup_last_attempt"`
	CatchupLastSuccess     int64   `json:"catchup_last_success"`
	CatchupLastFailure     int64   `json:"catchup_last_failure"`
	CatchupReputationScore float64 `json:"catchup_reputation_score"`
	CatchupMaliciousCount  int64   `json:"catchup_malicious_count"`
	CatchupAvgResponseTime int64   `json:"catchup_avg_response_ms"`
}

// PeersResponse represents the JSON response containing all peers
type PeersResponse struct {
	Peers []PeerInfoResponse `json:"peers"`
	Count int                `json:"count"`
}

// GetPeers returns the current peer registry data from the P2P service
func (h *HTTP) GetPeers(c echo.Context) error {
	ctx, cancel := context.WithTimeout(c.Request().Context(), 5*time.Second)
	defer cancel()

	// Connect to P2P gRPC service using the configured GRPCAddress
	p2pAddr := h.settings.P2P.GRPCAddress
	if p2pAddr == "" {
		h.logger.Errorf("[GetPeers] P2P gRPC address not configured (p2p_grpcAddress)")
		return c.JSON(http.StatusServiceUnavailable, PeersResponse{
			Peers: []PeerInfoResponse{},
			Count: 0,
		})
	}

	conn, err := grpc.DialContext(ctx, p2pAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		h.logger.Errorf("[GetPeers] Failed to connect to P2P service: %v", err)
		return c.JSON(http.StatusServiceUnavailable, PeersResponse{
			Peers: []PeerInfoResponse{},
			Count: 0,
		})
	}
	defer conn.Close()

	// Create P2P service client
	client := p2p_api.NewPeerServiceClient(conn)

	// Get comprehensive peer registry data
	registryResp, err := client.GetPeerRegistry(ctx, &emptypb.Empty{})
	if err != nil {
		h.logger.Errorf("[GetPeers] Failed to get peer registry: %v", err)
		return c.JSON(http.StatusInternalServerError, PeersResponse{
			Peers: []PeerInfoResponse{},
			Count: 0,
		})
	}

	// Convert gRPC response to JSON response
	peerResponses := make([]PeerInfoResponse, 0, len(registryResp.Peers))
	for _, peer := range registryResp.Peers {
		peerResponses = append(peerResponses, PeerInfoResponse{
			ID:              peer.Id,
			Height:          peer.Height,
			BlockHash:       peer.BlockHash,
			DataHubURL:      peer.DataHubUrl,
			IsHealthy:       peer.IsHealthy,
			HealthDuration:  peer.HealthDurationMs,
			LastHealthCheck: peer.LastHealthCheck,
			BanScore:        int(peer.BanScore),
			IsBanned:        peer.IsBanned,
			IsConnected:     peer.IsConnected,
			ConnectedAt:     peer.ConnectedAt,
			BytesReceived:   peer.BytesReceived,
			LastBlockTime:   peer.LastBlockTime,
			LastMessageTime: peer.LastMessageTime,
			URLResponsive:   peer.UrlResponsive,
			LastURLCheck:    peer.LastUrlCheck,

			// Interaction/catchup metrics (using the original field names for backward compatibility)
			CatchupAttempts:        peer.InteractionAttempts,
			CatchupSuccesses:       peer.InteractionSuccesses,
			CatchupFailures:        peer.InteractionFailures,
			CatchupLastAttempt:     peer.LastInteractionAttempt,
			CatchupLastSuccess:     peer.LastInteractionSuccess,
			CatchupLastFailure:     peer.LastInteractionFailure,
			CatchupReputationScore: peer.ReputationScore,
			CatchupMaliciousCount:  peer.MaliciousCount,
			CatchupAvgResponseTime: peer.AvgResponseTimeMs,
		})
	}

	response := PeersResponse{
		Peers: peerResponses,
		Count: len(peerResponses),
	}

	return c.JSON(http.StatusOK, response)
}
