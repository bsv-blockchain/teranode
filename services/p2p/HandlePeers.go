package p2p

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

// PeerInfoResponse represents the JSON response for a single peer
type PeerInfoResponse struct {
	ID              string  `json:"id"`
	Height          int32   `json:"height"`
	BlockHash       string  `json:"block_hash"`
	DataHubURL      string  `json:"data_hub_url"`
	IsHealthy       bool    `json:"is_healthy"`
	HealthDuration  int64   `json:"health_duration_ms"` // Duration in milliseconds
	LastHealthCheck int64   `json:"last_health_check"`  // Unix timestamp
	BanScore        int     `json:"ban_score"`
	IsBanned        bool    `json:"is_banned"`
	IsConnected     bool    `json:"is_connected"`
	ConnectedAt     int64   `json:"connected_at"`          // Unix timestamp
	BytesReceived   uint64  `json:"bytes_received"`
	LastBlockTime   int64   `json:"last_block_time"`       // Unix timestamp
	LastMessageTime int64   `json:"last_message_time"`     // Unix timestamp
	URLResponsive   bool    `json:"url_responsive"`
	LastURLCheck    int64   `json:"last_url_check"`        // Unix timestamp
}

// PeersResponse represents the JSON response containing all peers
type PeersResponse struct {
	Peers []PeerInfoResponse `json:"peers"`
	Count int                `json:"count"`
}

// HandleGetPeers returns an HTTP handler that serves peer registry data as JSON
func (s *Server) HandleGetPeers() echo.HandlerFunc {
	return func(c echo.Context) error {
		if s.peerRegistry == nil {
			return c.JSON(http.StatusOK, PeersResponse{
				Peers: []PeerInfoResponse{},
				Count: 0,
			})
		}

		// Get all peers from the registry
		allPeers := s.peerRegistry.GetAllPeers()

		// Convert to response format
		peerResponses := make([]PeerInfoResponse, 0, len(allPeers))
		for _, peer := range allPeers {
			peerResponses = append(peerResponses, PeerInfoResponse{
				ID:              peer.ID.String(),
				Height:          peer.Height,
				BlockHash:       peer.BlockHash,
				DataHubURL:      peer.DataHubURL,
				IsHealthy:       peer.IsHealthy,
				HealthDuration:  peer.HealthDuration.Milliseconds(),
				LastHealthCheck: peer.LastHealthCheck.Unix(),
				BanScore:        peer.BanScore,
				IsBanned:        peer.IsBanned,
				IsConnected:     peer.IsConnected,
				ConnectedAt:     peer.ConnectedAt.Unix(),
				BytesReceived:   peer.BytesReceived,
				LastBlockTime:   peer.LastBlockTime.Unix(),
				LastMessageTime: peer.LastMessageTime.Unix(),
				URLResponsive:   peer.URLResponsive,
				LastURLCheck:    peer.LastURLCheck.Unix(),
			})
		}

		response := PeersResponse{
			Peers: peerResponses,
			Count: len(peerResponses),
		}

		return c.JSON(http.StatusOK, response)
	}
}
