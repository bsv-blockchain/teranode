package httpimpl

import (
	"context"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
)

// GetCatchupStatus returns the current catchup status from the BlockValidation service
func (h *HTTP) GetCatchupStatus(c echo.Context) error {
	ctx, cancel := context.WithTimeout(c.Request().Context(), 5*time.Second)
	defer cancel()

	// Get BlockValidation client from repository
	blockValidationClient := h.repository.GetBlockvalidationClient()
	if blockValidationClient == nil {
		h.logger.Errorf("[GetCatchupStatus] BlockValidation client not available")
		return c.JSON(http.StatusServiceUnavailable, map[string]interface{}{
			"error":          "BlockValidation service not available",
			"is_catching_up": false,
		})
	}

	// Get catchup status
	status, err := blockValidationClient.GetCatchupStatus(ctx)
	if err != nil {
		h.logger.Errorf("[GetCatchupStatus] Failed to get catchup status: %v", err)
		return c.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error":          "Failed to get catchup status",
			"is_catching_up": false,
		})
	}

	// Convert to JSON response
	jsonResp := map[string]interface{}{
		"is_catching_up":         status.IsCatchingUp,
		"peer_id":                status.PeerID,
		"peer_url":               status.PeerURL,
		"target_block_hash":      status.TargetBlockHash,
		"target_block_height":    status.TargetBlockHeight,
		"current_height":         status.CurrentHeight,
		"total_blocks":           status.TotalBlocks,
		"blocks_fetched":         status.BlocksFetched,
		"blocks_validated":       status.BlocksValidated,
		"start_time":             status.StartTime,
		"duration_ms":            status.DurationMs,
		"fork_depth":             status.ForkDepth,
		"common_ancestor_hash":   status.CommonAncestorHash,
		"common_ancestor_height": status.CommonAncestorHeight,
	}

	// Add previous attempt if available
	if status.PreviousAttempt != nil {
		jsonResp["previous_attempt"] = map[string]interface{}{
			"peer_id":             status.PreviousAttempt.PeerID,
			"peer_url":            status.PreviousAttempt.PeerURL,
			"target_block_hash":   status.PreviousAttempt.TargetBlockHash,
			"target_block_height": status.PreviousAttempt.TargetBlockHeight,
			"error_message":       status.PreviousAttempt.ErrorMessage,
			"error_type":          status.PreviousAttempt.ErrorType,
			"attempt_time":        status.PreviousAttempt.AttemptTime,
			"duration_ms":         status.PreviousAttempt.DurationMs,
			"blocks_validated":    status.PreviousAttempt.BlocksValidated,
		}
	}

	return c.JSON(http.StatusOK, jsonResp)
}
