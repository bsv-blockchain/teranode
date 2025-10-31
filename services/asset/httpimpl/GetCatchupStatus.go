package httpimpl

import (
	"context"
	"net/http"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockvalidation/blockvalidation_api"
	"github.com/labstack/echo/v4"
)

// GetCatchupStatus returns the current catchup status from the BlockValidation service
func (h *HTTP) GetCatchupStatus(c echo.Context) error {
	ctx, cancel := context.WithTimeout(c.Request().Context(), 5*time.Second)
	defer cancel()

	// Initialize persistent gRPC clients if not already done
	h.initGRPCClients(ctx)

	// Check if BlockValidation client connection is available
	if h.blockvalClientConn == nil {
		h.logger.Errorf("[GetCatchupStatus] BlockValidation gRPC client not available: %v", h.blockvalClientInitErr)
		return c.JSON(http.StatusServiceUnavailable, map[string]interface{}{
			"error":          "BlockValidation service not available",
			"is_catching_up": false,
		})
	}

	// Create BlockValidation API client from persistent connection
	client := blockvalidation_api.NewBlockValidationAPIClient(h.blockvalClientConn)
	resp, err := client.GetCatchupStatus(ctx, &blockvalidation_api.EmptyMessage{})
	if err != nil {
		h.logger.Errorf("[GetCatchupStatus] Failed to get catchup status: %v", err)
		return c.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error":          "Failed to get catchup status",
			"is_catching_up": false,
		})
	}

	// Convert gRPC response to JSON
	jsonResp := map[string]interface{}{
		"is_catching_up":         resp.IsCatchingUp,
		"peer_id":                resp.PeerId,
		"peer_url":               resp.PeerUrl,
		"target_block_hash":      resp.TargetBlockHash,
		"target_block_height":    resp.TargetBlockHeight,
		"current_height":         resp.CurrentHeight,
		"total_blocks":           resp.TotalBlocks,
		"blocks_fetched":         resp.BlocksFetched,
		"blocks_validated":       resp.BlocksValidated,
		"start_time":             resp.StartTime,
		"duration_ms":            resp.DurationMs,
		"fork_depth":             resp.ForkDepth,
		"common_ancestor_hash":   resp.CommonAncestorHash,
		"common_ancestor_height": resp.CommonAncestorHeight,
	}

	// Add previous attempt if available
	if resp.PreviousAttempt != nil {
		jsonResp["previous_attempt"] = map[string]interface{}{
			"peer_id":             resp.PreviousAttempt.PeerId,
			"peer_url":            resp.PreviousAttempt.PeerUrl,
			"target_block_hash":   resp.PreviousAttempt.TargetBlockHash,
			"target_block_height": resp.PreviousAttempt.TargetBlockHeight,
			"error_message":       resp.PreviousAttempt.ErrorMessage,
			"error_type":          resp.PreviousAttempt.ErrorType,
			"attempt_time":        resp.PreviousAttempt.AttemptTime,
			"duration_ms":         resp.PreviousAttempt.DurationMs,
			"blocks_validated":    resp.PreviousAttempt.BlocksValidated,
		}
	}

	return c.JSON(http.StatusOK, jsonResp)
}
