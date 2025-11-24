package httpimpl

import (
	"context"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
)

// ResetReputationRequest represents the JSON request body
type ResetReputationRequest struct {
	PeerID string `json:"peer_id"` // Empty string means reset all peers
}

// ResetReputationResponse represents the JSON response
type ResetReputationResponse struct {
	OK         bool  `json:"ok"`
	PeersReset int32 `json:"peers_reset"`
}

// ResetReputation resets reputation metrics for a peer or all peers
func (h *HTTP) ResetReputation(c echo.Context) error {
	ctx, cancel := context.WithTimeout(c.Request().Context(), 10*time.Second)
	defer cancel()

	// Parse request body
	var req ResetReputationRequest
	if err := c.Bind(&req); err != nil {
		h.logger.Errorf("[ResetReputation] Failed to parse request: %v", err)
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "Invalid request body",
		})
	}

	p2pClient := h.repository.GetP2PClient()

	// Check if P2P client connection is available
	if p2pClient == nil {
		h.logger.Errorf("[ResetReputation] P2P client not available")
		return c.JSON(http.StatusServiceUnavailable, map[string]string{
			"error": "P2P service not available",
		})
	}

	// Call P2P service ResetReputation method
	peersReset, err := p2pClient.ResetReputation(ctx, req.PeerID)
	if err != nil {
		h.logger.Errorf("[ResetReputation] Failed to reset reputation: %v", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error":   "Failed to reset reputation",
			"details": err.Error(),
		})
	}

	if req.PeerID == "" {
		h.logger.Infof("[ResetReputation] Reset reputation for all peers. Count: %d", peersReset)
	} else {
		h.logger.Infof("[ResetReputation] Reset reputation for peer %s", req.PeerID)
	}

	response := ResetReputationResponse{
		OK:         true,
		PeersReset: int32(peersReset),
	}

	return c.JSON(http.StatusOK, response)
}
