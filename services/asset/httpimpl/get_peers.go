package httpimpl

import (
	"context"
	"net/http"
	"time"

	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/labstack/echo/v4"
)

// PeerInfoResponse represents the JSON response for a single peer
// Matches the structure from P2P service's HandlePeers.go
type PeerInfoResponse struct {
	ID              string `json:"id"`
	ClientName      string `json:"client_name"`
	Height          int32  `json:"height"`
	BlockHash       string `json:"block_hash"`
	DataHubURL      string `json:"data_hub_url"`
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
	LastCatchupError       string  `json:"last_catchup_error"`
	LastCatchupErrorTime   int64   `json:"last_catchup_error_time"`
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

	p2pClient := h.repository.GetP2PClient()

	// Check if P2P client connection is available
	if p2pClient == nil {
		h.logger.Errorf("[GetPeers] P2P client not available")
		return c.JSON(http.StatusServiceUnavailable, PeersResponse{
			Peers: []PeerInfoResponse{},
			Count: 0,
		})
	}

	// Get comprehensive peer registry data using the p2p.ClientI interface
	// Returns []*p2p.PeerInfo
	peers, err := p2pClient.GetPeerRegistry(ctx)
	if err != nil {
		h.logger.Errorf("[GetPeers] Failed to get peer registry: %v", err)
		return c.JSON(http.StatusInternalServerError, PeersResponse{
			Peers: []PeerInfoResponse{},
			Count: 0,
		})
	}

	// Convert native PeerInfo to JSON response
	peerResponses := make([]PeerInfoResponse, 0, len(peers))
	for _, peerPtr := range peers {
		peer := (*p2p.PeerInfo)(peerPtr) // Explicit type assertion to satisfy import checker
		peerResponses = append(peerResponses, PeerInfoResponse{
			ID:              peer.ID.String(),
			ClientName:      peer.ClientName,
			Height:          peer.Height,
			BlockHash:       peer.BlockHash,
			DataHubURL:      peer.DataHubURL,
			BanScore:        peer.BanScore,
			IsBanned:        peer.IsBanned,
			IsConnected:     peer.IsConnected,
			ConnectedAt:     peer.ConnectedAt.Unix(),
			BytesReceived:   peer.BytesReceived,
			LastBlockTime:   peer.LastBlockTime.Unix(),
			LastMessageTime: peer.LastMessageTime.Unix(),
			URLResponsive:   peer.URLResponsive,
			LastURLCheck:    peer.LastURLCheck.Unix(),

			// Interaction/catchup metrics (using the original field names for backward compatibility)
			CatchupAttempts:        peer.InteractionAttempts,
			CatchupSuccesses:       peer.InteractionSuccesses,
			CatchupFailures:        peer.InteractionFailures,
			CatchupLastAttempt:     peer.LastInteractionAttempt.Unix(),
			CatchupLastSuccess:     peer.LastInteractionSuccess.Unix(),
			CatchupLastFailure:     peer.LastInteractionFailure.Unix(),
			CatchupReputationScore: peer.ReputationScore,
			CatchupMaliciousCount:  peer.MaliciousCount,
			CatchupAvgResponseTime: peer.AvgResponseTime.Milliseconds(),
			LastCatchupError:       peer.LastCatchupError,
			LastCatchupErrorTime:   peer.LastCatchupErrorTime.Unix(),
		})
	}

	response := PeersResponse{
		Peers: peerResponses,
		Count: len(peerResponses),
	}

	return c.JSON(http.StatusOK, response)
}
